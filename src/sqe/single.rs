use crate::context::with_context_mut;
use crate::sqe::{Completable, CompletionHandler, RawSqe, Sqe, Submittable};
use anyhow::Result;
use io_uring::squeue::Entry;
use std::io;
use std::task::{Poll, Waker};

#[derive(Debug)]
pub struct SqeSingle {
    // Index to RawSqe in thread-local Slab
    idx: usize,
}

impl SqeSingle {
    pub fn try_new(entry: Entry) -> Result<Self> {
        let idx = with_context_mut(|ctx| -> Result<usize> {
            let (idx, _) = ctx
                .slab
                .insert(RawSqe::new(entry, CompletionHandler::new_single()))?;
            Ok(idx)
        })?;

        Ok(Self { idx })
    }

    pub fn get_idx(&self) -> usize {
        self.idx
    }
}

impl Submittable for SqeSingle {
    fn submit(&self) -> io::Result<i32> {
        with_context_mut(|ctx| ctx.push_sqes(&[self.idx]))
    }
}

impl Completable for SqeSingle {
    type Output = Result<(Entry, io::Result<i32>)>;

    fn poll_complete(&self, waker: &Waker) -> Poll<Self::Output> {
        with_context_mut(|ctx| -> Poll<Self::Output> {
            let raw_sqe = match ctx.slab.get_mut(self.idx) {
                Ok(sqe) => sqe,
                Err(e) => return Poll::Ready(Err(e)),
            };

            if raw_sqe.is_ready() {
                Poll::Ready(raw_sqe.take_final_result())
            } else {
                raw_sqe.set_waker(waker);
                Poll::Pending
            }
        })
    }
}

// RAII: free RawSqe from slab.
impl Drop for SqeSingle {
    fn drop(&mut self) {
        with_context_mut(|ctx| {
            if ctx.slab.try_remove(self.idx).is_none() {
                eprintln!("Warning: SQE {} not found in slab during drop", self.idx);
            }
        });
    }
}

impl Into<Sqe<SqeSingle>> for SqeSingle {
    fn into(self) -> Sqe<SqeSingle> {
        Sqe::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{init_context, with_context_mut};
    use crate::test_utils::*;
    use std::pin::pin;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll};

    #[test]
    fn test_submit_and_complete_single_sqe() -> Result<()> {
        init_context(64);

        let sqe = SqeSingle::try_new(nop())?;
        let idx = sqe.get_idx();
        let mut sqe_fut = pin!(Sqe::new(sqe));

        let (waker, waker_data) = mock_waker();
        let mut ctx = Context::from_waker(&waker);

        // Polling once will submit the SQE and set the waker.
        // Polling N more times after this won't change the state.
        for _ in 0..10 {
            assert!(matches!(sqe_fut.as_mut().poll(&mut ctx), Poll::Pending));
            assert_eq!(waker_data.load(Ordering::Relaxed), 0);

            with_context_mut(|ctx| {
                assert_eq!(ctx.ring.submission().len(), 1);

                let res = ctx.slab.get(idx).and_then(|sqe| {
                    assert!(!sqe.is_ready());
                    assert!(sqe.has_waker());
                    Ok(())
                });

                assert!(res.is_ok());
            });
        }

        with_context_mut(|ctx| {
            // Submit SQEs and wait for CQEs :: `io_uring_enter`
            assert!(matches!(ctx.submit_and_wait(1, None), Ok(1)));
            assert_eq!(waker_data.load(Ordering::Relaxed), 0);

            // Process CQEs :: wakes up Waker
            assert!(matches!(ctx.process_cqes(None), Ok(1)));
            assert_eq!(waker_data.load(Ordering::Relaxed), 1);
        });

        if let Poll::Ready(Ok((entry, result))) = sqe_fut.as_mut().poll(&mut ctx) {
            assert!(matches!(result, Ok(0)));
            assert_eq!(entry.get_user_data(), idx as u64);
        } else {
            assert!(false, "Expected Poll::Ready(Ok((entry, result)))");
        }

        Ok(())
    }
}
