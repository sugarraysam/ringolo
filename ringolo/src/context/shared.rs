use crate::context::{Core, PendingIoOp};
use crate::runtime::RuntimeConfig;
use anyhow::{Result, anyhow};
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::ThreadId;

use dashmap::DashMap;

// TODO:
// - error handling dont use anyhow

#[derive(Debug)]
pub struct Shared {
    pub(crate) shutdown: Arc<AtomicBool>,

    pub(crate) worker_data: DashMap<ThreadId, WorkerData>,
}

impl Shared {
    pub(crate) fn new(cfg: &RuntimeConfig) -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),

            // TODO: revise this data struct, not optimal, we can have empty shards
            // depending on how thread_id's are hashed.
            // There is no need to have more shards then the number of workers.
            worker_data: DashMap::with_shard_amount(cfg.worker_threads.next_power_of_two()),
        }
    }

    pub(super) fn register_worker(&self, core: &Core) -> Arc<AtomicUsize> {
        let worker_data = WorkerData::from_core(core);
        let pending_ios = Arc::clone(&worker_data.pending_ios);

        self.worker_data.insert(core.thread_id, worker_data);
        pending_ios
    }

    pub(crate) fn unregister_worker(&self, core: &Core) -> Result<()> {
        self.worker_data
            .remove(&core.thread_id)
            .map(|_| {})
            .ok_or_else(|| anyhow!("Thread ID {:?} not found", &core.thread_id))
    }

    // Slow path to decrement pending_ios on a specific thread. See note on
    // `task::harness::Harness::cancel_task`.
    pub(crate) fn modify_pending_ios(&self, thread_id: ThreadId, op: PendingIoOp, delta: usize) {
        let modified = self.worker_data.get(&thread_id).map(|data| {
            match op {
                PendingIoOp::Increment => data.pending_ios.fetch_add(delta, Ordering::Relaxed),
                PendingIoOp::Decrement => data.pending_ios.fetch_sub(delta, Ordering::Relaxed),
            };
        });
        debug_assert!(modified.is_some());
    }
}

#[derive(Debug)]
pub(crate) struct WorkerData {
    pub(crate) pending_ios: Arc<AtomicUsize>,
    pub(crate) ring_fd: RawFd,
}

impl WorkerData {
    fn from_core(core: &Core) -> Self {
        Self {
            pending_ios: Arc::new(AtomicUsize::new(0)),
            ring_fd: core.ring_fd,
        }
    }
}
