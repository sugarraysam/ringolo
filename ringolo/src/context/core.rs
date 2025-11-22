#![allow(unsafe_op_in_unsafe_fn)]

use crate::context::maintenance::task::MaintenanceTask;
use crate::context::{RawSqeSlab, Shared};
use crate::runtime::RuntimeConfig;
use crate::task::ThreadId;
use crate::task::{Header, TaskNode};
use anyhow::Result;
use std::cell::{Cell, RefCell};
use std::os::unix::io::RawFd;
use std::ptr::NonNull;
use std::sync::Arc;
use std::task::Waker;

use crate::context::ring::SingleIssuerRing;

/// The thread-local heart of a runtime worker.
///
/// `Core` allows a specific worker thread to perform I/O and manage tasks without
/// locking shared resources. It is designed to be used via `RefCell` to allow
/// holding mutable references to specific fields (like the `ring` or `slab`)
/// while simultaneously borrowing the context.
pub(crate) struct Core {
    /// The unique identifier of the worker thread owning this core.
    pub(crate) thread_id: ThreadId,

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
    pub(crate) pending_ios: Cell<usize>,

    /// High-performance slab allocator for `io_uring` Submission Queue Entries (SQEs).
    /// Pre-allocates memory to avoid allocation during the hot path.
    pub(crate) slab: RefCell<RawSqeSlab>,

    /// The raw file descriptor of the `io_uring` instance, cached for quick lookup.
    pub(crate) ring_fd: RawFd,

    /// The underlying `io_uring` interface configured for Single Issuer mode.
    pub(crate) ring: RefCell<SingleIssuerRing>,

    /// The task currently being polled by this worker. Used to establish
    /// parent-child relationships when spawning new tasks.
    pub(crate) current_task: RefCell<Option<Arc<TaskNode>>>,

    /// A background task responsible for periodic cleanup or driving
    /// non-blocking maintenance operations.
    pub(crate) maintenance_task: Arc<MaintenanceTask>,
}

impl Core {
    pub(crate) fn try_new(
        thread_id: ThreadId,
        cfg: &RuntimeConfig,
        shared: &Arc<Shared>,
    ) -> Result<Self> {
        let ring = SingleIssuerRing::try_new(cfg)?;

        let core = Core {
            thread_id,
            pending_ios: Cell::new(0),
            slab: RefCell::new(RawSqeSlab::new(
                cfg.sq_ring_size * cfg.cq_ring_size_multiplier,
            )),
            ring_fd: ring.as_raw_fd(),
            ring: RefCell::new(ring),
            current_task: RefCell::new(None),
            maintenance_task: Arc::new(MaintenanceTask::new(cfg, shared.shutdown.clone())),
        };

        shared.register_worker(&core);
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

    pub(crate) fn is_pre_block_on(&self, root_thread_id: &ThreadId) -> bool {
        self.current_task.borrow().is_none() && self.thread_id == *root_thread_id
    }

    pub(crate) fn get_pending_ios(&self) -> usize {
        self.pending_ios.get()
    }

    pub(crate) fn increment_pending_ios(&self) {
        self.pending_ios.update(|x| x + 1);
    }

    pub(crate) fn decrement_pending_ios(&self) {
        self.pending_ios
            .update(|x| x.checked_add_signed(-1).expect("underflow"));
    }

    pub(crate) fn increment_task_owned_resources(&self, waker: &Waker) -> Option<NonNull<Header>> {
        // Don't track owned_resources for the root_future. The reason is because it
        // *is not backed* by a regular task and is never stealable by other threads.
        if !self.is_polling_root() {
            unsafe {
                let ptr = NonNull::new_unchecked(waker.data() as *mut Header);
                Header::increment_owned_resources(ptr);
                Some(ptr)
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::context;
    use crate::test_utils::init_stealing_runtime_and_context;
    use anyhow::Result;
    use rstest::rstest;

    #[rstest]
    #[case::two(2)]
    #[case::four(4)]
    fn test_core_registers_worker_in_shared(#[case] num_workers: usize) -> Result<()> {
        let (runtime, _scheduler) = init_stealing_runtime_and_context(num_workers, None)?;
        runtime.block_on(async {
            assert_eq!(
                context::with_shared(|shared| shared.worker_slots.len()),
                num_workers + 1 // root_worker
            );
        });

        Ok(())
    }
}
