//! Provides asynchronous, thread-local `io_uring` resource cleanup.
//!
//! ## The "Why": Thread-Local `io_uring` Resources
//!
//! `Ringolo` is built on `io_uring`, where many resources are **thread-local**—they
//! are registered with a specific ring and are only valid on that thread.
//!
//! This presents two major challenges:
//!
//! 1.  **Leaky Multishot Operations:** `io_uring` multishot operations (like
//!     [`TimeoutMultishot`]) are "leaky" by design. Once submitted, they will
//!     fire indefinitely until the program exits *or* they are explicitly
//!     cancelled. This cancellation **must** happen on the same thread (and ring)
//!     that submitted it.
//!
//! 2.  **Registered Resources:** Resources like direct descriptors or provided
//!     buffers are registered with the thread-local ring. They must be explicitly
//!     closed/un-registered to avoid leaking kernel resources.
//!
//! ## The "How": The Asynchronous Maintenance Task
//!
//! This module provides a mechanism to safely clean up these resources
//! asynchronously. All cleanup operations (like closing a file or cancelling
//! a timeout) are handled by a special **maintenance task** that exists on
//! *each worker thread* from program start to finish.
//!
//! When a future (like [`TimeoutMultishot`]) is dropped, it doesn't block
//! to clean itself up. Instead, it calls a function from this module
//! (e.g., [async_timeout_remove]), which enqueues a `CleanupOp` on the
//! current thread's maintenance queue.
//!
//! The maintenance task wakes up periodically, drains its queue, batches
//! all cleanup ops into a single `io_uring` submission, and executes them. This
//! guarantees that resources submitted on a thread are also cleaned up *on that
//! same thread*.
//!
//! [`TimeoutMultishot`]: crate::future::lib::TimeoutMultishot
//!
use std::cell::RefCell;

use crate::context;

use crate::context::maintenance::queue::AsyncLocalQueue;
use crate::future::lib::list::{AnyOp, OpList};
use crate::future::lib::ops::{AsyncCancel, Close, TimeoutRemove};
use crate::runtime::{OnCleanupError, PanicReason, SchedulerPanic};
use crate::sqe::IoError;
use anyhow::Result;
use either::Either;
use io_uring::types::{CancelBuilder, Fd, Fixed};

/// Enqueues an `io_uring` async cancellation operation.
///
/// This is primarily used to stop "leaky" multishot operations that are
/// not `TIMEOUT_REMOVE`. The `user_data` typically corresponds to the
/// slab entry of the I/O operation being cancelled.
pub(crate) fn async_cancel(builder: CancelBuilder, user_data: usize) {
    let op = CleanupOp::new_cancel(builder, user_data);
    context::with_core(|core| core.maintenance_task.add_cleanup_op(op));
}

/// Enqueues an asynchronous `close` operation for a file descriptor.
pub(crate) fn async_close(fd: Either<Fd, Fixed>) {
    let op = CleanupOp::new_close(fd);
    context::with_core(|core| core.maintenance_task.add_cleanup_op(op));
}

/// Enqueues an `io_uring` `TIMEOUT_REMOVE` operation.
///
/// This is the correct way to cancel a `TimeoutMultishot`, as it
/// both removes the timer from `io_uring`'s internal data structures
/// and (via `user_data`) releases its slab entry.
pub(crate) fn async_timeout_remove(user_data: usize) {
    let op = CleanupOp::new_timeout_remove(user_data as u64);
    context::with_core(|core| core.maintenance_task.add_cleanup_op(op));
}

#[derive(Debug, Clone)]
enum CleanupOpcode {
    AsyncCancel(AsyncCancel),
    Close(Close),
    TimeoutRemove(TimeoutRemove),
}

/// A pending cleanup operation, awaiting execution by the maintenance task.
#[derive(Debug)]
pub(crate) struct CleanupOp {
    /// One of the supported cleanup operation.
    op: CleanupOpcode,

    /// Counter to keep track of retries.
    num_retries: usize,

    /// If we should also remove a slab entry, this is its index.
    user_data: Option<usize>,
}

impl CleanupOp {
    fn new_cancel(builder: CancelBuilder, user_data: usize) -> Self {
        Self {
            op: CleanupOpcode::AsyncCancel(AsyncCancel::new(builder)),
            num_retries: 0,
            user_data: Some(user_data),
        }
    }

    fn new_close(fd: Either<Fd, Fixed>) -> Self {
        Self {
            op: CleanupOpcode::Close(Close::new(fd)),
            num_retries: 0,
            user_data: None,
        }
    }

    fn new_timeout_remove(user_data: u64) -> Self {
        Self {
            op: CleanupOpcode::TimeoutRemove(TimeoutRemove::new(user_data)),
            num_retries: 0,
            user_data: Some(user_data as usize),
        }
    }
}

/// The core handler, owned by the maintenance task, that batches and
/// executes asynchronous cleanup operations.
#[derive(Debug)]
pub(crate) struct CleanupHandler {
    /// The SPSC queue for all pending ops on this thread.
    queue: AsyncLocalQueue<CleanupOp>,

    /// What to do if a cleanup op fails.
    policy: OnCleanupError,

    /// A count of all pending operations.
    inflight: RefCell<usize>,
}

unsafe impl Sync for CleanupHandler {}
unsafe impl Send for CleanupHandler {}

impl CleanupHandler {
    pub(crate) fn new(policy: OnCleanupError) -> Self {
        Self {
            queue: AsyncLocalQueue::new(),
            policy,
            inflight: RefCell::new(0),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        *self.inflight.borrow() == 0
    }

    pub(crate) fn len(&self) -> usize {
        *self.inflight.borrow()
    }

    pub(crate) fn push(&self, op: CleanupOp) {
        *self.inflight.borrow_mut() += 1;
        self.queue.push(op);
    }

    /// Drains the queue and submits a batch of cleanup operations.
    ///
    /// This is the main "work" function for the maintenance task.
    /// It handles retries and error policies.
    pub(crate) async fn cleanup(&self) -> Result<(), IoError> {
        // Batch all cleanup op in a single submit call for efficiency. Creating
        // the OpList does not invalidate our CleanupOp so we can re-use it for
        // retries.
        let ops = self.queue.drain().await;
        let results = OpList::new_batch(cvt(&ops)).await?;

        for (res, mut op) in results.into_iter().zip(ops.into_iter()) {
            // CleanupHandler is thread-local, safe to decrement counter here.
            *self.inflight.borrow_mut() -= 1;

            match res {
                // Success
                Ok(_) => {}

                // Did not find what we want to cancel. Assume it is ok.
                Err(err) if err.raw_os_error() == Some(libc::ENOENT) => {}

                // Other cases
                Err(err) => {
                    match (err.is_retryable(), self.policy) {
                        (_, OnCleanupError::Ignore) => { /* do nothing */ }

                        (true, OnCleanupError::Retry(allowed_retries))
                            if allowed_retries > op.num_retries =>
                        {
                            // This op failed but we have retries left. Bump counter and re-enqueue.
                            op.num_retries += 1;
                            self.queue.push(op);
                            continue; // skip removing slab entry
                        }

                        (_, OnCleanupError::Panic | OnCleanupError::Retry(_)) => self.panic(
                            PanicReason::FailedCleanup,
                            format!(
                                "Cleanup failed for {:?}. OnCleanupError: {:?}, err: {:?}",
                                op, self.policy, err
                            ),
                        ),
                    }
                }
            }

            // The `CleanupOp` is responsible for removing the RawSqe from the slab in certain
            // cases. We do this at the very end to avoid race condition on the slab, because
            // as soon as we release the entry, it can be reused by another async operation.
            self.remove_slab_entry(op);
        }

        Ok(())
    }

    fn remove_slab_entry(&self, op: CleanupOp) {
        if let Some(user_data) = op.user_data {
            context::with_slab_mut(|slab| {
                if slab.try_remove(user_data).is_none()
                    && !matches!(self.policy, OnCleanupError::Ignore)
                {
                    self.panic(
                        PanicReason::SlabInvalidState,
                        "Failed to remove entry from slab in CleanupTask".to_string(),
                    );
                }
            });
        }
    }

    #[track_caller]
    #[cold]
    fn panic(&self, reason: PanicReason, msg: String) -> ! {
        std::panic::panic_any(SchedulerPanic::new(reason, msg));
    }
}

fn cvt(ops: &[CleanupOp]) -> Vec<AnyOp<'_>> {
    ops.iter().map(|op| op.into()).collect()
}

impl<'a> From<&CleanupOp> for AnyOp<'a> {
    fn from(val: &CleanupOp) -> Self {
        match val.op.clone() {
            CleanupOpcode::AsyncCancel(op) => op.into(),
            CleanupOpcode::Close(op) => op.into(),
            CleanupOpcode::TimeoutRemove(op) => op.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as ringolo;
    use crate::context::with_slab;
    use crate::future::lib::Multishot;
    use crate::future::lib::ops::TimeoutMultishot;
    use crate::runtime::Builder;
    use crate::test_utils::*;
    use crate::utils::scheduler::{Call, Method};
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

        // Cleanup once
        let user_data = timeout.as_mut().cancel();
        assert!(user_data.is_some());

        assert_inflight_cleanup(1);
        wait_for_cleanup().await;

        // Cleanup twice is no-op
        assert!(matches!(timeout.next().await, None));
        assert!(timeout.cancel().is_none());

        // No scheduler panic
        crate::with_scheduler!(|s| {
            let unhandled_panic_calls = s.tracker.get_calls(&Method::UnhandledPanic);
            assert!(unhandled_panic_calls.is_empty());
        });

        // Make sure all entries were cleared from the slab.
        with_slab(|slab| {
            assert_eq!(slab.len(), 0);
        });

        Ok(())
    }

    #[test]
    fn test_on_cleanup_error_ignore() -> Result<()> {
        let builder = Builder::new_local().on_cleanup_error(OnCleanupError::Ignore);
        let (runtime, scheduler) = init_local_runtime_and_context(Some(builder))?;

        runtime.block_on(async {
            let user_data = 42;

            let builder = CancelBuilder::user_data(user_data as u64);
            ringolo::async_cancel(builder, user_data);

            wait_for_cleanup().await;
        });

        // Make sure there was no panic
        let unhandled_panic_calls = scheduler.tracker.get_calls(&Method::UnhandledPanic);
        assert!(unhandled_panic_calls.is_empty());

        Ok(())
    }

    #[test]
    fn test_on_cleanup_error_panic() -> Result<()> {
        let builder = Builder::new_local().on_cleanup_error(OnCleanupError::Panic);
        let (_runtime, scheduler) = init_local_runtime_and_context(Some(builder))?;

        let root_res = std::panic::catch_unwind(|| {
            ringolo::block_on(async {
                let invalid_fd = -424242;
                ringolo::async_close(Either::Left(Fd(invalid_fd)));

                wait_for_cleanup().await;
            });
        });

        assert!(root_res.is_err());

        let unhandled_panic_calls = scheduler.tracker.get_calls(&Method::UnhandledPanic);
        assert_eq!(unhandled_panic_calls.len(), 1);
        assert_eq!(
            unhandled_panic_calls.first(),
            Some(&Call::UnhandledPanic {
                reason: PanicReason::FailedCleanup
            })
        );

        Ok(())
    }
}
