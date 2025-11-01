#![allow(unsafe_op_in_unsafe_fn)]

use crate::context::maintenance::task::MaintenanceTask;
use crate::context::{RawSqeSlab, Shared};
use crate::runtime::RuntimeConfig;
use crate::task::{Header, TaskNode};
use anyhow::Result;
use std::cell::RefCell;
use std::os::unix::io::RawFd;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;
use std::thread;

use crate::context::ring::SingleIssuerRing;

/// Core Thread-local context required by all flavors of schedulers. Requires
/// interior mutability on ALL fields for best interface. Will allow borrowing
/// multiple fields as mut in the same context. Otherwise we will run in borrow
/// checker issues.
pub(crate) struct Core {
    // Tracks pending I/O operations at two distinct levels:
    //
    // 1. Per-Thread (via `self.pending_ios`):
    //    This is a signal for the *scheduler*. It indicates that this worker
    //    thread has outstanding I/O operations and is expecting completions.
    //    The scheduler uses this count to determine if the thread has more
    //    work to do and should avoid parking.
    //
    // 2. Per-Task (via `Header::pending_ios`):
    //    We also track pending I/O for each individual task. This is our
    //    signal to determine if a task can be safely stolen by another
    //    thread (a task with I/O in-flight might be tied to this thread's
    //    driver, like io_uring). It also influences scheduling logic,
    //    (e.g., placing the task on a non-stealable queue).
    //
    // CAVEAT: The "root future" polled by the scheduler is special. It uses
    // a Waker implementation where the `data` pointer is the `Scheduler`
    // itself, not a task `Header`. For this reason, we do not track pending_ios
    // on the root future, which makes sense as it would not be stealable anyways.
    pub(crate) pending_ios: Arc<AtomicUsize>,

    /// Slab to manage RawSqe allocations for this thread.
    pub(crate) slab: RefCell<RawSqeSlab>,

    /// IoUring to submit and complete IO operations.
    pub(crate) ring_fd: RawFd,
    pub(crate) ring: RefCell<SingleIssuerRing>,

    /// Task that is currently executing on this thread. This field is used to
    /// determine the parent of newly spawned tasks.
    pub(crate) current_task: RefCell<Option<Arc<TaskNode>>>,

    pub(crate) maintenance_task: Arc<MaintenanceTask>,
}

impl Core {
    pub(crate) fn try_new(cfg: &RuntimeConfig, shared: &Arc<Shared>) -> Result<Self> {
        let ring = SingleIssuerRing::try_new(cfg)?;

        let mut core = Core {
            pending_ios: Arc::new(AtomicUsize::new(0)),
            slab: RefCell::new(RawSqeSlab::new(
                cfg.sq_ring_size * cfg.cq_ring_size_multiplier,
            )),
            ring_fd: ring.as_raw_fd(),
            ring: RefCell::new(ring),
            current_task: RefCell::new(None),
            maintenance_task: Arc::new(MaintenanceTask::new(cfg, shared.shutdown.clone())),
        };

        // We share an atomic counter for pending_ios on this thread. The
        // *hot path* will access the atomic on Core directly. The *cold path*
        // will use the shared context. This is required to support cross-thread
        // cancellation.
        core.pending_ios = shared.register_worker(thread::current().id(), &core);

        Ok(core)
    }

    pub(crate) fn spawn_maintenance_task(&self) {
        let clone = self.maintenance_task.clone();
        clone.spawn();
    }

    pub(crate) fn is_polling_root(&self) -> bool {
        self.current_task
            .borrow()
            .as_ref()
            .is_some_and(|task| task.id.is_root())
    }

    pub(crate) fn get_pending_ios(&self) -> usize {
        self.pending_ios.load(Ordering::Acquire)
    }

    pub(crate) fn increment_pending_ios(&self) {
        self.pending_ios.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn decrement_pending_ios(&self) {
        self.pending_ios.fetch_sub(1, Ordering::Release);
    }

    pub(crate) fn increment_task_pending_ios(&self, waker: &Waker) {
        self.modify_task_pending_ios(waker, 1);
    }

    pub(crate) fn decrement_task_pending_ios(&self, waker: &Waker) {
        self.modify_task_pending_ios(waker, -1);
    }

    fn modify_task_pending_ios(&self, waker: &Waker, delta: i32) {
        // Don't track pending_ios for the root_future.
        if !self.is_polling_root() {
            unsafe {
                let ptr = NonNull::new_unchecked(waker.data() as *mut Header);
                Header::modify_pending_ios(ptr, delta);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate as ringolo;
    use crate::context::{self, PendingIoOp};
    use anyhow::Result;

    #[ringolo::test]
    async fn test_pending_io_on_core_and_shared() -> Result<()> {
        let thread_id = std::thread::current().id();

        context::with_core(|core| {
            core.increment_pending_ios();
            core.decrement_pending_ios();
            core.increment_pending_ios();

            assert_eq!(core.get_pending_ios(), 1);
        });

        context::with_shared(|shared| {
            assert_eq!(shared.get_pending_ios(&thread_id), 1);

            shared.modify_pending_ios(&thread_id, PendingIoOp::Increment, 1);
            shared.modify_pending_ios(&thread_id, PendingIoOp::Increment, 1);
            shared.modify_pending_ios(&thread_id, PendingIoOp::Decrement, 1);
        });

        context::with_core(|core| {
            assert_eq!(core.get_pending_ios(), 2);

            // Need to manually set to zero otherwise worker does not exit.
            core.decrement_pending_ios();
            core.decrement_pending_ios();
        });

        Ok(())
    }
}
