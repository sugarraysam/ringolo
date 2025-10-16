use crate::context::with_slab_mut;
use crate::future::opcode::{Op, OpPayload};
use crate::runtime::{PanicReason, SchedulerPanic};
use crate::sqe::IoError;
use std::pin::pin;

/// Traits for cancellation operations.
pub(crate) type CancelOutputT = Result<i32, IoError>;

pub(crate) trait OpCancelPayload:
    OpPayload<Output = CancelOutputT> + Clone + Send + 'static
{
}

impl<T> OpCancelPayload for T where T: OpPayload<Output = CancelOutputT> + Clone + Send + 'static {}

#[derive(Debug)]
pub(crate) struct CancelTaskBuilder<T: OpCancelPayload> {
    cancel_op: T,
    user_data: usize,
    on_error: OnCancelError,
}

impl<T: OpCancelPayload> CancelTaskBuilder<T> {
    pub(crate) fn new(cancel_op: T, user_data: usize) -> Self {
        Self {
            cancel_op,
            user_data,
            // Defaults to retrying a few times.
            on_error: OnCancelError::default(),
        }
    }

    pub(crate) fn on_error(mut self, on_error: OnCancelError) -> Self {
        self.on_error = on_error;
        self
    }

    pub(crate) fn build(self) -> CancelTask<T> {
        CancelTask::new(Op::new(self.cancel_op), self.user_data, self.on_error)
    }
}

const DEFAULT_NUM_RETRIES: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnCancelError {
    Ignore,
    Panic,
    Retry(usize),
}

impl Default for OnCancelError {
    fn default() -> Self {
        OnCancelError::Retry(DEFAULT_NUM_RETRIES)
    }
}

/// A task dedicated to performing an asynchronous cancellation.
///
/// This struct encapsulates all the logic for building the cancellation future,
/// handling its result, and cleaning up associated resources.
#[derive(Debug)]
pub(crate) struct CancelTask<T: OpCancelPayload> {
    cancel_op: Op<T>,
    user_data: usize,
    on_error: OnCancelError,
}

impl<T: OpCancelPayload> CancelTask<T> {
    fn new(cancel_op: Op<T>, user_data: usize, on_error: OnCancelError) -> Self {
        Self {
            cancel_op,
            user_data,
            on_error,
        }
    }

    /// Converts the `CancelTask` into a future that performs the cancellation.
    /// Can panic if we exhausted retries or OnCancelError::Panic is set.
    pub(crate) async fn into_future(mut self) {
        loop {
            let res = pin!(self.cancel_op.clone()).await;

            match res {
                Ok(_) => break,
                Err(err) => {
                    match (err.is_retryable(), &mut self.on_error) {
                        (_, OnCancelError::Ignore) => break,
                        (true, OnCancelError::Retry(retries_left)) if *retries_left > 0 => {
                            *retries_left -= 1;
                            continue; // Continue to the next iteration of the loop to retry.
                        }

                        (_, OnCancelError::Panic | OnCancelError::Retry(_)) => {
                            std::panic::panic_any(SchedulerPanic::new(
                                PanicReason::CancelTask,
                                format!(
                                    "CancelTask failed. OnCancelError: {:?}, err: {:?}",
                                    self.on_error, err
                                ),
                            ));
                        }
                    }
                }
            }
        } // cancel loop /w retries

        // The cancellation task is now responsible for removing the RawSqe from the slab.
        // We do this at the very end to avoid race condition on the slab, because as soon
        // as we release the entry, it can be reused by another async operation.
        with_slab_mut(|slab| {
            if slab.try_remove(self.user_data).is_none()
                && !matches!(self.on_error, OnCancelError::Ignore)
            {
                std::panic::panic_any(SchedulerPanic::new(
                    PanicReason::SlabInvalidState,
                    "Failed to remove entry from slab in CancelTask".to_string(),
                ));
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::with_slab;
    use crate::future::opcode::single::AsyncCancelOp;
    use crate::runtime::Builder;
    use crate::test_utils::init_local_runtime_and_context;
    use crate::utils::scheduler::{Call, Method};
    use crate::{
        self as ringolo,
        future::opcode::{Multishot, TimeoutMultishot},
    };
    use anyhow::Result;
    use futures::StreamExt;
    use std::pin::pin;
    use std::time::Duration;

    #[ringolo::test]
    async fn test_cancel_timeout_multishot() -> Result<()> {
        let interval = Duration::from_millis(1);
        let mut timeout = pin!(Multishot::new(TimeoutMultishot::new(interval, 0, None)));

        let mut n = 5;
        while let Some(res) = timeout.next().await
            && n > 0
        {
            n -= 1;
            assert!(matches!(res, Ok(())));
        }

        // Cancel once
        let cancel_handle = timeout.as_mut().cancel();
        assert!(cancel_handle.is_some());
        assert!(cancel_handle.unwrap().await.is_ok());

        crate::with_scheduler!(|s| {
            let spawn_calls = s.tracker.get_calls(&Method::Spawn);
            assert_eq!(spawn_calls.len(), 1);

            let unhandled_panic_calls = s.tracker.get_calls(&Method::UnhandledPanic);
            assert!(unhandled_panic_calls.is_empty());
        });

        // Cancel twice is no-op
        assert!(matches!(timeout.next().await, None));
        assert!(timeout.cancel().is_none());

        // Make sure all entries were cleared from the slab.
        with_slab(|slab| {
            assert!(slab.is_empty());
        });

        Ok(())
    }

    #[test]
    fn test_on_cancel_error_ignore() -> Result<()> {
        let builder = Builder::new_local().on_cancel_error(OnCancelError::Ignore);
        init_local_runtime_and_context(Some(builder))?;

        let user_data = 42;

        let builder = io_uring::types::CancelBuilder::user_data(user_data as u64);
        let task = CancelTaskBuilder::new(AsyncCancelOp::new(builder), user_data);

        let handle = crate::runtime::spawn_cancel(task);

        ringolo::block_on(async {
            assert!(handle.await.is_ok());
        });

        // Make sure there was no panic
        crate::with_scheduler!(|s| {
            let spawn_calls = s.tracker.get_calls(&Method::Spawn);
            assert_eq!(spawn_calls.len(), 1);

            let unhandled_panic_calls = s.tracker.get_calls(&Method::UnhandledPanic);
            assert!(unhandled_panic_calls.is_empty());
        });

        Ok(())
    }

    #[test]
    fn test_on_cancel_error_panic() -> Result<()> {
        let builder = Builder::new_local().on_cancel_error(OnCancelError::Panic);
        init_local_runtime_and_context(Some(builder))?;

        let user_data = 42;

        let builder = io_uring::types::CancelBuilder::user_data(user_data as u64);
        let task = CancelTaskBuilder::new(AsyncCancelOp::new(builder), user_data);

        let handle = crate::runtime::spawn_cancel(task);

        let root_res = std::panic::catch_unwind(|| {
            ringolo::block_on(async {
                // We dont await the handle as the root future otherwise root future would
                // panic. We want to check the panic triggered in a spawned task vs. root future.
                assert!(true);
            });
        });

        // Scheduler panics and we stored JoinHandle output before.
        assert!(root_res.is_err());
        let res = handle.get_result();
        assert!(res.is_err());
        assert!(res.unwrap_err().is_panic());

        crate::with_scheduler!(|s| {
            let spawn_calls = s.tracker.get_calls(&Method::Spawn);
            assert_eq!(spawn_calls.len(), 1);

            let unhandled_panic_calls = s.tracker.get_calls(&Method::UnhandledPanic);
            assert_eq!(unhandled_panic_calls.len(), 1);
            assert_eq!(
                unhandled_panic_calls.first(),
                Some(&Call::UnhandledPanic {
                    reason: PanicReason::CancelTask
                })
            );
        });

        Ok(())
    }
}
