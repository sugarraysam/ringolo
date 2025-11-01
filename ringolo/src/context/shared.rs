use crate::context::slots::WorkerSlots;
use crate::context::{Core, PendingIoOp};
use crate::runtime::RuntimeConfig;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{self, Thread, ThreadId};

#[derive(Debug)]
pub struct Shared {
    pub(crate) cfg: RuntimeConfig,

    pub(crate) shutdown: Arc<AtomicBool>,

    /// Used to store per-worker data. We panic on any errors coming from the
    /// slots as it would mean the runtime is in an unexpected and unrecoverable
    /// state.
    pub(crate) worker_slots: WorkerSlots,

    /// LIFO collection of parked threads. We unpark thread in LIFO order as the
    /// latest parked thread is the one where CPU cache will be the hotest.
    pub(crate) parked_threads: RwLock<VecDeque<Thread>>,
}

impl Shared {
    pub(crate) fn new(cfg: &RuntimeConfig) -> Self {
        Self {
            cfg: cfg.clone(),
            shutdown: Arc::new(AtomicBool::new(false)),
            // We have N + 1 worker slots to account for the `root_worker`
            worker_slots: WorkerSlots::new(cfg.worker_threads + 1),
            parked_threads: RwLock::new(VecDeque::with_capacity(cfg.worker_threads)),
        }
    }

    #[track_caller]
    pub(super) fn register_worker(&self, thread_id: ThreadId, core: &Core) -> Arc<AtomicUsize> {
        self.worker_slots
            .register(thread_id, core)
            .map(|data| Arc::clone(&data.pending_ios))
            .expect("failed to register worker")
    }

    // # Unused
    // Could be useful if we want to madvise inactive worker threads but
    // the scheduler does not do this today.
    //
    // # Warning
    // There could be a race condition between tasks getting cancelled and
    // trying to decrement pending_ios.
    #[allow(unused)]
    #[track_caller]
    pub(crate) fn unregister_worker(&self, thread_id: ThreadId) {
        self.worker_slots
            .unregister(&thread_id)
            .expect("failed to unregister worker");
    }

    // Slow path to decrement pending_ios on a specific thread. See note on
    // `task::harness::Harness::cancel_task`.
    #[track_caller]
    pub(crate) fn modify_pending_ios(&self, thread_id: &ThreadId, op: PendingIoOp, delta: usize) {
        self.worker_slots.with_data(thread_id, |data| {
            match op {
                PendingIoOp::Increment => data.pending_ios.fetch_add(delta, Ordering::Relaxed),
                PendingIoOp::Decrement => data.pending_ios.fetch_sub(delta, Ordering::Relaxed),
            };
        });
    }

    #[track_caller]
    pub(crate) fn get_pending_ios(&self, thread_id: &ThreadId) -> usize {
        self.worker_slots
            .with_data(thread_id, |data| data.pending_ios.load(Ordering::Relaxed))
    }

    /// This method parks the current thread, adding the thread to the LIFO
    /// `parked_threads` list. It will stay parked until the scheduler unparks
    /// it when it judges there is sufficient work to resume the event_loop on
    /// this worker.
    #[track_caller]
    pub(crate) fn park_current_thread<T>(&self, injector: &Arc<crossbeam_deque::Injector<T>>) {
        let should_unpark = {
            let mut parked_threads = self.parked_threads.write();

            // We need to check these signals *while holding the lock* to avoid race
            // conditions. These are used right before we call unpark.
            if !injector.is_empty() || self.shutdown.load(Ordering::Acquire) {
                return;
            }

            let thread = thread::current();
            let should_unpark = self.worker_slots.with_data(&thread.id(), |data| {
                data.should_unpark.store(false, Ordering::Release);
                Arc::clone(&data.should_unpark)
            });

            parked_threads.push_back(thread);
            should_unpark
        };

        // Release lock and spin until it is time to unpark. We use this loop to
        // account for spurious wakeups as per docs.
        while !should_unpark.load(Ordering::Acquire) {
            thread::park();
        }
    }

    #[track_caller]
    pub(crate) fn unpark_one_thread(&self) -> bool {
        if let Some(thread) = self.parked_threads.write().pop_back() {
            self.worker_slots.with_data(&thread.id(), |data| {
                data.should_unpark.store(true, Ordering::Release);
            });
            thread.unpark();
            true
        } else {
            false
        }
    }

    #[track_caller]
    pub(crate) fn unpark_all_threads(&self) -> usize {
        let mut num_unparked = 0;
        let mut parked_threads = self.parked_threads.write();

        while let Some(thread) = parked_threads.pop_back() {
            num_unparked += 1;
            self.worker_slots.with_data(&thread.id(), |data| {
                data.should_unpark.store(true, Ordering::Release);
            });

            thread.unpark();
        }

        num_unparked
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::assert_impl_all;

    assert_impl_all!(Shared: Send, Sync);
}
