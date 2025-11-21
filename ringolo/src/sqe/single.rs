use crate::context;
use crate::sqe::{Completable, CompletionHandler, IoError, RawSqe, Sqe, Submittable};
use io_uring::squeue::Entry;
use std::task::{Poll, Waker};

#[derive(Debug)]
pub(crate) enum SqeSingleState {
    Unsubmitted { entry: Entry },
    Submitted { idx: usize },
    Completed,
}

/// The simplest SQE backend, representing a single standard `io_uring` operation.
///
/// Maps 1:1 with a standard syscall replacement (e.g., `fsync`, `read`, `write`).
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

    pub(crate) fn get_idx(&self) -> Result<usize, IoError> {
        match self.state {
            SqeSingleState::Submitted { idx } => Ok(idx),
            _ => Err(IoError::SqeBackendInvalidState),
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
            SqeSingleState::Submitted { .. } | SqeSingleState::Completed => {
                return Err(IoError::SqeBackendInvalidState);
            }
            SqeSingleState::Unsubmitted { entry } => {
                context::with_slab_and_ring_mut(|slab, ring| {
                    let reserved = slab.reserve_entry()?;
                    let idx = reserved.key();

                    entry.set_user_data(idx as u64);
                    ring.push(entry)?;

                    reserved.commit(RawSqe::new(CompletionHandler::new_single()));

                    Ok(idx)
                })
            }
        }
        .map(|idx| {
            // On successful push, we can transition the state and increment the
            // number of pending IOs at both the thread-level and task-level.
            self.state = SqeSingleState::Submitted { idx };
            context::with_core(|core| {
                core.increment_pending_ios();
                core.increment_task_pending_ios(waker);
            });
        })
    }
}

impl Completable for SqeSingle {
    type Output = Result<i32, IoError>;

    fn poll_complete(&mut self, waker: &Waker) -> Poll<Self::Output> {
        let idx = self.get_idx()?;

        context::with_slab_mut(|slab| -> Poll<Self::Output> {
            let raw = slab.get_mut(idx)?;

            if raw.is_ready() {
                let res = raw.take_final_result().map_err(Into::into);

                // # Important
                // We need to decrement task `pending_ios` *after* dropping the RawSqe
                // to prevent making the task stealable while it still needs to drop this
                // RawSqe from thread-local slab.
                debug_assert!(slab.try_remove(idx).is_some(), "RawSqe not found in slab.");
                context::with_core(|c| c.decrement_task_pending_ios(waker));

                self.state = SqeSingleState::Completed;
                Poll::Ready(res)
            } else {
                raw.set_waker(waker);
                Poll::Pending
            }
        })
    }
}

impl Drop for SqeSingle {
    fn drop(&mut self) {
        if let SqeSingleState::Submitted { idx } = self.state {
            // If we get here, it means *nothing* will decrement the task pending_ios.
            // The consequence is the task will *not be stealable*. We can live with that
            // and there is no way to get access to the underlying task anyways because
            // the Waker is lost.
            context::with_slab_mut(|slab| {
                if slab.try_remove(idx).is_none() {
                    eprintln!("[SqeSingle]: SQE {} not found in slab during drop.", idx,);
                }
            });
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
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

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
            context::with_core(|core| assert_eq!(core.get_pending_ios(), 1));
            assert_eq!(waker_data.get_pending_ios(), 1);

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
            context::with_core(|core| assert_eq!(core.get_pending_ios(), 0));
            assert_eq!(waker_data.get_pending_ios(), 1);
        });

        assert!(matches!(
            sqe_fut.as_mut().poll(&mut ctx),
            Poll::Ready(Ok(_))
        ));

        assert_eq!(waker_data.get_pending_ios(), 0);
        Ok(())
    }
}
