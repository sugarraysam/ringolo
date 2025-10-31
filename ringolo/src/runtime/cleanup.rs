use crate::context;
use crate::future::lib::{Op, OpPayload};
use crate::runtime::{PanicReason, SchedulerPanic};
use std::pin::pin;

pub(crate) trait OpCleanupPayload: OpPayload<Output = i32> + Clone + Send + 'static {}

impl<T> OpCleanupPayload for T where T: OpPayload<Output = i32> + Clone + Send + 'static {}

#[derive(Debug)]
pub(crate) struct CleanupTaskBuilder<T: OpCleanupPayload> {
    cleanup: T,
    user_data: Option<usize>,
    on_error: OnCleanupError,
}

impl<T: OpCleanupPayload> CleanupTaskBuilder<T> {
    pub(crate) fn new(cleanup: T) -> Self {
        Self {
            cleanup,
            user_data: None,
            // Defaults to retrying a few times.
            on_error: OnCleanupError::default(),
        }
    }

    /// Associates the cleanup task with a `RawSqe` entry in the slab.
    /// The entry will be removed from the slab after the cleanup operation completes.
    pub(crate) fn with_slab_entry(mut self, user_data: usize) -> Self {
        self.user_data = Some(user_data);
        self
    }

    /// Sets the error handling policy for the cleanup task.
    pub(crate) fn on_error(mut self, on_error: OnCleanupError) -> Self {
        self.on_error = on_error;
        self
    }

    pub(crate) fn build(self) -> CleanupTask<T> {
        CleanupTask::new(Op::new(self.cleanup), self.user_data, self.on_error)
    }
}

const DEFAULT_NUM_RETRIES: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnCleanupError {
    Ignore,
    Panic,
    Retry(usize),
}

impl Default for OnCleanupError {
    fn default() -> Self {
        OnCleanupError::Retry(DEFAULT_NUM_RETRIES)
    }
}

/// A task dedicated to performing an asynchronous cancellation.
///
/// This struct encapsulates all the logic for building the cancellation future,
/// handling its result, and cleaning up associated resources.
#[derive(Debug)]
pub(crate) struct CleanupTask<T: OpCleanupPayload> {
    op: Op<T>,
    user_data: Option<usize>,
    on_error: OnCleanupError,
}

// # TODO:
//    A much better cancellation model would integrate tightly within the Task. Here we
//    spawn a new task to perform cancellation, and use a new RawSqe. A much better
//    model would re-use the same Task and re-use the same RawSqe. We need to
//    expand the state of both of these structs to account for this state, and
//    somehow wrap every single regular task to allow for cancellation.
//    How to wrap the user-provided future with this logic?
impl<T: OpCleanupPayload> CleanupTask<T> {
    fn new(op: Op<T>, user_data: Option<usize>, on_error: OnCleanupError) -> Self {
        Self {
            op,
            user_data,
            on_error,
        }
    }

    /// Converts the `CleanupTask` into a future that performs the cleanup.
    /// Can panic if we exhausted retries or OnCleanupError::Panic is set.
    pub(crate) async fn into_future(mut self) {
        loop {
            let res = pin!(self.op.clone()).await;
            match res {
                Ok(_) => break,
                Err(err) => {
                    match (err.is_retryable(), &mut self.on_error) {
                        (_, OnCleanupError::Ignore) => break,

                        (true, OnCleanupError::Retry(retries_left)) if *retries_left > 0 => {
                            *retries_left -= 1;
                            continue; // Continue to the next iteration of the loop to retry.
                        }

                        (_, OnCleanupError::Panic | OnCleanupError::Retry(_)) => {
                            std::panic::panic_any(SchedulerPanic::new(
                                PanicReason::CleanupTask,
                                format!(
                                    "CleanupTask failed. OnCleanupError: {:?}, err: {:?}",
                                    self.on_error, err
                                ),
                            ));
                        }
                    }
                }
            }
        } // cancel loop /w retries

        if let Some(user_data) = self.user_data {
            // The cleanup task is now responsible for removing the RawSqe from the slab.
            // We do this at the very end to avoid race condition on the slab, because as soon
            // as we release the entry, it can be reused by another async operation.
            context::with_slab_mut(|slab| {
                if slab.try_remove(user_data).is_none()
                    && !matches!(self.on_error, OnCleanupError::Ignore)
                {
                    std::panic::panic_any(SchedulerPanic::new(
                        PanicReason::SlabInvalidState,
                        "Failed to remove entry from slab in CleanupTask".to_string(),
                    ));
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::with_slab;
    use crate::future::lib::AsyncCancel;
    use crate::runtime::{Builder, spawn_cleanup};
    use crate::test_utils::init_local_runtime_and_context;
    use crate::utils::scheduler::{Call, Method};
    use crate::{
        self as ringolo,
        future::lib::{Multishot, TimeoutMultishot},
    };
    use anyhow::{Context, Result};
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

        // Cleanup once
        let cancel_handle = timeout.as_mut().cancel();
        assert!(cancel_handle.is_some());
        cancel_handle.unwrap().await.context("cancel failed")?;

        crate::with_scheduler!(|s| {
            let spawn_calls = s.tracker.get_calls(&Method::Spawn);
            assert_eq!(spawn_calls.len(), 1);

            let unhandled_panic_calls = s.tracker.get_calls(&Method::UnhandledPanic);
            assert!(unhandled_panic_calls.is_empty());
        });

        // Cleanup twice is no-op
        assert!(matches!(timeout.next().await, None));
        assert!(timeout.cancel().is_none());

        // Make sure all entries were cleared from the slab.
        with_slab(|slab| {
            assert!(slab.is_empty());
        });

        Ok(())
    }

    #[test]
    fn test_on_cleanup_error_ignore() -> Result<()> {
        let builder = Builder::new_local().on_cleanup_error(OnCleanupError::Ignore);
        let (_runtime, scheduler) = init_local_runtime_and_context(Some(builder))?;

        let user_data = 42;

        let builder = io_uring::types::CancelBuilder::user_data(user_data as u64);
        let task = CleanupTaskBuilder::new(AsyncCancel::new(builder)).with_slab_entry(user_data);

        let handle = spawn_cleanup(task);

        ringolo::block_on(async {
            assert!(handle.await.is_ok());
        });

        // Make sure there was no panic
        let spawn_calls = scheduler.tracker.get_calls(&Method::Spawn);
        assert_eq!(spawn_calls.len(), 1);

        let unhandled_panic_calls = scheduler.tracker.get_calls(&Method::UnhandledPanic);
        assert!(unhandled_panic_calls.is_empty());

        Ok(())
    }

    #[test]
    fn test_on_cleanup_error_panic() -> Result<()> {
        let builder = Builder::new_local().on_cleanup_error(OnCleanupError::Panic);
        let (_runtime, scheduler) = init_local_runtime_and_context(Some(builder))?;

        let user_data = 333;

        let builder = io_uring::types::CancelBuilder::user_data(user_data as u64);
        let task = CleanupTaskBuilder::new(AsyncCancel::new(builder)).with_slab_entry(user_data);

        let handle = spawn_cleanup(task);

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

        let spawn_calls = scheduler.tracker.get_calls(&Method::Spawn);
        assert_eq!(spawn_calls.len(), 1);

        let unhandled_panic_calls = scheduler.tracker.get_calls(&Method::UnhandledPanic);
        assert_eq!(unhandled_panic_calls.len(), 1);
        assert_eq!(
            unhandled_panic_calls.first(),
            Some(&Call::UnhandledPanic {
                reason: PanicReason::CleanupTask
            })
        );

        Ok(())
    }
}
