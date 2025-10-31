use crate::context::slots::WorkerSlots;
use crate::context::{Core, PendingIoOp};
use crate::runtime::RuntimeConfig;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::ThreadId;

#[derive(Debug)]
pub struct Shared {
    pub(crate) shutdown: Arc<AtomicBool>,

    /// Used to store per-worker data. We panic on any errors coming from the
    /// slots as it would mean the runtime is in an unexpected and unrecoverable
    /// state.
    pub(crate) worker_slots: WorkerSlots,
}

impl Shared {
    pub(crate) fn new(cfg: &RuntimeConfig) -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),

            worker_slots: WorkerSlots::new(cfg.worker_threads),
        }
    }

    #[track_caller]
    pub(super) fn register_worker(&self, core: &Core) -> Arc<AtomicUsize> {
        self.worker_slots
            .register(core)
            .map(|data| Arc::clone(&data.pending_ios))
            .expect("failed to register worker")
    }

    #[track_caller]
    pub(crate) fn unregister_worker(&self, core: &Core) {
        self.worker_slots
            .unregister(core)
            .expect("failed to unregister worker");
    }

    // Slow path to decrement pending_ios on a specific thread. See note on
    // `task::harness::Harness::cancel_task`.
    #[track_caller]
    pub(crate) fn modify_pending_ios(&self, thread_id: &ThreadId, op: PendingIoOp, delta: usize) {
        self.worker_slots
            .with_data(thread_id, |data| {
                match op {
                    PendingIoOp::Increment => data.pending_ios.fetch_add(delta, Ordering::Relaxed),
                    PendingIoOp::Decrement => data.pending_ios.fetch_sub(delta, Ordering::Relaxed),
                };
            })
            .expect("failed to modify worker data");
    }

    #[track_caller]
    pub(crate) fn get_pending_ios(&self, thread_id: &ThreadId) -> usize {
        self.worker_slots
            .with_data(thread_id, |data| data.pending_ios.load(Ordering::Relaxed))
            .expect("failed to get worker data")
    }
}
