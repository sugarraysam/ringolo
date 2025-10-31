#![allow(unsafe_op_in_unsafe_fn)]

use crate::context::{PendingIoOp, RawSqeSlab, Shared};
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
    pub(crate) thread_id: thread::ThreadId,

    /// Track number of locally pending IOs. This is used by the scheduler to
    /// determine if there is more pending work.
    pub(crate) pending_ios: Arc<AtomicUsize>,

    /// Slab to manage RawSqe allocations for this thread.
    pub(crate) slab: RefCell<RawSqeSlab>,

    /// IoUring to submit and complete IO operations.
    pub(crate) ring_fd: RawFd,
    pub(crate) ring: RefCell<SingleIssuerRing>,

    /// Task that is currently executing on this thread. This field is used to
    /// determine the parent of newly spawned tasks.
    pub(crate) current_task: RefCell<Option<Arc<TaskNode>>>,
}

impl Core {
    pub(crate) fn try_new(cfg: &RuntimeConfig, shared: &Arc<Shared>) -> Result<Self> {
        let ring = SingleIssuerRing::try_new(cfg)?;

        let mut core = Core {
            thread_id: thread::current().id(),
            pending_ios: Arc::new(AtomicUsize::new(0)),
            slab: RefCell::new(RawSqeSlab::new(
                cfg.sq_ring_size * cfg.cq_ring_size_multiplier,
            )),
            ring_fd: ring.as_raw_fd(),
            ring: RefCell::new(ring),
            current_task: RefCell::new(None),
        };

        // We share an atomic counter for pending_ios on this thread. The
        // *hot path* will access the atomic on Core directly. The *cold path*
        // will use the shared context. This is required to support cross-thread
        // cancellation.
        let pending_ios = shared.register_worker(&core);
        core.pending_ios = pending_ios;

        Ok(core)
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

    // Incrementing pending ios is done as part of polling SQE backends. For this
    // reason we can rely on `self.is_polling_root()` indicator.
    pub(crate) fn increment_pending_ios(&self, waker: &Waker) {
        self.modify_pending_ios(self.is_polling_root(), PendingIoOp::Increment, Some(waker));
    }

    // Decrementing pending ios is done as part of processing CQEs which means
    // there is no polling context. We rely on caller to inform us if the
    // underlying IO operation is owned by the root future.
    pub(crate) fn decrement_pending_ios(&self, owned_by_root: bool, waker: &Waker) {
        self.modify_pending_ios(owned_by_root, PendingIoOp::Decrement, Some(waker));
    }

    // Tracks pending I/O operations at two distinct levels:
    //
    // 1. Per-Thread (via `self.pending_ios`):
    //    This is a signal for the *scheduler*. It indicates that this worker
    //    thread has outstanding I/O operations and is expecting completions.
    //    The scheduler uses this count to determine if the thread has more
    //    work to do and should avoid parking.
    //
    // 2. Per-Task (via `Header::increment_pending_ios`):
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
    fn modify_pending_ios(&self, owned_by_root: bool, op: PendingIoOp, waker: Option<&Waker>) {
        let delta: i32 = match op {
            PendingIoOp::Increment => {
                self.pending_ios.fetch_add(1, Ordering::Release);
                1
            }
            PendingIoOp::Decrement => {
                self.pending_ios.fetch_sub(1, Ordering::Release);
                -1
            }
        };

        if !owned_by_root && let Some(waker) = waker {
            unsafe {
                let ptr = NonNull::new_unchecked(waker.data() as *mut Header);
                Header::modify_pending_ios(ptr, delta);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use crate as ringolo;
    use crate::context::{self, PendingIoOp};
    use anyhow::Result;

    #[ringolo::test]
    async fn test_pending_io_on_core_and_shared() -> Result<()> {
        let thread_id = context::with_core(|core| {
            core.modify_pending_ios(false, PendingIoOp::Increment, None);
            core.modify_pending_ios(false, PendingIoOp::Increment, None);
            core.modify_pending_ios(false, PendingIoOp::Decrement, None);

            assert_eq!(core.get_pending_ios(), 1);
            core.thread_id
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
            core.modify_pending_ios(false, PendingIoOp::Decrement, None);
            core.modify_pending_ios(false, PendingIoOp::Decrement, None);
        });

        Ok(())
    }
}
