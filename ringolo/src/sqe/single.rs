use crate::context::{with_core_mut, with_slab_mut};
use crate::sqe::{
    Completable, CompletionHandler, IoError, RawSqe, Sqe, Submittable, increment_pending_io,
};
use io_uring::squeue::Entry;
use std::io::{self, Error};
use std::mem;
use std::task::{Poll, Waker};

#[derive(Debug)]
pub(crate) enum SqeSingleState {
    Preparing { entry: Entry },
    Indexed { idx: usize },
}

#[derive(Debug)]
pub struct SqeSingle {
    state: SqeSingleState,
}

impl SqeSingle {
    pub fn new(entry: Entry) -> Self {
        Self {
            state: SqeSingleState::Preparing { entry },
        }
    }

    pub fn get_idx(&self) -> io::Result<usize> {
        match self.state {
            SqeSingleState::Indexed { idx } => Ok(idx),
            _ => Err(Error::other("sqe single state is not indexed")),
        }
    }
}

impl Submittable for SqeSingle {
    fn submit(&mut self, waker: &Waker) -> Result<i32, IoError> {
        let idx = match &mut self.state {
            // Submit can be retried for example when the ring is full. We need to make sure we only count this IO once,
            // and only insert the entry in the slab once.
            SqeSingleState::Preparing { entry } => {
                // We run `pre_push_validation` to make the submission *atomic*. This is
                // because we must ensure that entries are added to both the ring and the slab
                // to avoid corrupted state.
                with_core_mut(|core| core.pre_push_validation(1))?;

                // Important: clone entry so we can retry if Slab is full.
                let entry = entry.clone();
                let idx = with_slab_mut(|slab| {
                    slab.insert(RawSqe::new(entry, CompletionHandler::new_single()))
                        .map(|(idx, _)| idx)
                })?;

                _ = mem::replace(&mut self.state, SqeSingleState::Indexed { idx });
                increment_pending_io(waker);

                idx
            }
            SqeSingleState::Indexed { idx } => *idx,
        };

        with_core_mut(|core| core.push_sqes([idx].iter()))
    }
}

impl Completable for SqeSingle {
    type Output = Result<i32, IoError>;

    fn poll_complete(&self, waker: &Waker) -> Poll<Self::Output> {
        let idx = self.get_idx()?;

        with_slab_mut(|slab| -> Poll<Self::Output> {
            let raw_sqe = match slab.get_mut(idx) {
                Ok(sqe) => sqe,
                Err(e) => return Poll::Ready(Err(e.into())),
            };

            if raw_sqe.is_ready() {
                Poll::Ready(raw_sqe.take_final_result().map_err(|e| e.into()))
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
        if let Err(e) = self.get_idx().map(|idx| {
            with_slab_mut(|slab| {
                if slab.try_remove(idx).is_none() {
                    eprintln!("Warning: SQE {} not found in slab during drop", idx);
                }
            });
        }) {
            eprintln!("{:?}", e);
        }
    }
}

impl From<SqeSingle> for Sqe<SqeSingle> {
    fn from(val: SqeSingle) -> Self {
        Sqe::new(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::with_slab_and_ring_mut;
    use crate::test_utils::*;
    use anyhow::Result;
    use std::pin::pin;
    use std::task::{Context, Poll};

    #[test]
    fn test_submit_and_complete_single_sqe() -> Result<()> {
        init_local_runtime_and_context(None)?;

        let mut idx = 0;

        let sqe = SqeSingle::new(nop());
        let mut sqe_fut = pin!(Sqe::new(sqe));

        let (waker, waker_data) = mock_waker();
        let mut ctx = Context::from_waker(&waker);

        // Polling once will submit the SQE and set the waker.
        // Polling N more times after this won't change the state.
        for _ in 0..10 {
            assert!(matches!(sqe_fut.as_mut().poll(&mut ctx), Poll::Pending));
            idx = sqe_fut.get().get_idx()?;

            assert_eq!(waker_data.get_count(), 0);
            assert_eq!(waker_data.get_pending_io(), 1);

            with_slab_and_ring_mut(|slab, ring| {
                assert_eq!(ring.sq().len(), 1);

                let res = slab.get(idx).and_then(|sqe| {
                    assert!(!sqe.is_ready());
                    assert!(sqe.has_waker());
                    Ok(())
                });

                assert!(res.is_ok());
            });
        }

        with_core_mut(|core| {
            // Submit SQEs and wait for CQEs :: `io_uring_enter`
            assert!(matches!(core.submit_and_wait(1, None), Ok(1)));
            assert_eq!(waker_data.get_count(), 0);

            // Process CQEs :: wakes up Waker
            assert!(matches!(core.process_cqes(None), Ok(1)));
            assert_eq!(waker_data.get_count(), 1);
            assert_eq!(waker_data.get_pending_io(), 0);
        });

        if let Poll::Ready(Ok(result)) = sqe_fut.as_mut().poll(&mut ctx) {
            assert_eq!(result, 0);
            // TODO: rawsqe has userdata?
            // assert_eq!(entry.get_user_data(), idx as u64);
        } else {
            assert!(false, "Expected Poll::Ready(Ok((entry, result)))");
        }

        Ok(())
    }
}
