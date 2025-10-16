use crate::context::{with_slab_and_ring_mut, with_slab_mut};
use crate::sqe::{
    Completable, CompletionHandler, IoError, RawSqe, Sqe, Submittable, increment_pending_io,
};
use anyhow::anyhow;
use io_uring::squeue::Entry;
use std::io::{self, Error};
use std::pin::Pin;
use std::task::{Poll, Waker};

#[derive(Debug)]
pub(crate) enum SqeSingleState {
    Unsubmitted { entry: Entry },
    Submitted { idx: usize },
}

#[derive(Debug)]
pub(crate) struct SqeSingle {
    state: SqeSingleState,
}

impl SqeSingle {
    pub(crate) fn new(entry: Entry) -> Self {
        Self {
            state: SqeSingleState::Unsubmitted { entry },
        }
    }

    pub(crate) fn get_idx(&self) -> io::Result<usize> {
        match self.state {
            SqeSingleState::Submitted { idx } => Ok(idx),
            _ => Err(Error::other("sqe single state is not indexed")),
        }
    }
}

impl Submittable for SqeSingle {
    /// Submit is a two-step process to avoid corrupted state. Slab and Ring are
    /// interconnected so we use a reservation API on the slab and only commit
    /// once we have successfully pushed to the ring. This enables safe retries
    /// and avoids corrupted state.
    fn submit(&mut self, waker: &Waker) -> Result<(), IoError> {
        match &mut self.state {
            SqeSingleState::Submitted { .. } => {
                return Err(anyhow!("SqeSingle already submitted").into());
            }
            SqeSingleState::Unsubmitted { entry } => with_slab_and_ring_mut(|slab, ring| {
                let reserved = slab.reserve_entry()?;
                let idx = reserved.key();

                entry.set_user_data(idx as u64);
                ring.push(entry)?;

                reserved.commit(RawSqe::new(CompletionHandler::new_single()));

                Ok(idx)
            }),
        }
        // On successful push, we can transition the state and increment the
        // number of local pending IOs for this task.
        .map(|idx| {
            self.state = SqeSingleState::Submitted { idx };
            increment_pending_io(waker);
        })
    }
}

impl Completable for SqeSingle {
    type Output = Result<i32, IoError>;

    fn poll_complete(&mut self, waker: &Waker) -> Poll<Self::Output> {
        let idx = self.get_idx()?;

        with_slab_mut(|slab| -> Poll<Self::Output> {
            let raw = slab.get_mut(idx)?;

            if raw.is_ready() {
                Poll::Ready(raw.take_final_result().map_err(|e| e.into()))
            } else {
                raw.set_waker(waker);
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
    use static_assertions::assert_impl_one;
    use std::pin::pin;
    use std::task::{Context, Poll};

    #[test]
    fn test_submit_and_complete_single_sqe() -> Result<()> {
        init_local_runtime_and_context(None)?;

        let sqe = SqeSingle::new(nop());
        let mut sqe_fut = pin!(Sqe::new(sqe));

        let (waker, waker_data) = mock_waker();
        let mut ctx = Context::from_waker(&waker);

        // Polling once will submit the SQE and set the waker.
        // Polling N more times after this won't change the state.
        for _ in 0..10 {
            assert!(matches!(sqe_fut.as_mut().poll(&mut ctx), Poll::Pending));
            let idx = sqe_fut.get().get_idx()?;

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

        with_slab_and_ring_mut(|slab, ring| {
            // Submit SQEs and wait for CQEs :: `io_uring_enter`
            assert!(matches!(ring.submit_and_wait(1, None), Ok(1)));
            assert_eq!(waker_data.get_count(), 0);

            // Process CQEs :: wakes up Waker
            assert!(matches!(ring.process_cqes(slab, None), Ok(1)));
            assert_eq!(waker_data.get_count(), 1);
            assert_eq!(waker_data.get_pending_io(), 0);
        });

        assert!(matches!(
            sqe_fut.as_mut().poll(&mut ctx),
            Poll::Ready(Ok(_))
        ));

        Ok(())
    }
}
