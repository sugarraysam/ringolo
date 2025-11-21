use crate::context::slots::WorkerSlots;
use crate::context::{Core, PendingIoOp};
use crate::runtime::RuntimeConfig;
use crate::task::ThreadId;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{self, Thread};

/// Global state shared across all worker threads.
///
/// `Shared` handles the coordination aspects of the runtime. Unlike [`Core`],
/// all fields here must be thread-safe (Sync). This context is primarily used
/// for work-stealing, parking logic, and shutdown coordination.
#[derive(Debug)]
pub(crate) struct Shared {
    /// Configuration for the runtime (ring sizes, worker counts, etc.).
    pub(crate) cfg: RuntimeConfig,

    /// A global signal indicating if the runtime is in the process of shutting down.
    pub(crate) shutdown: Arc<AtomicBool>,

    /// A registry of all active workers, mapping `ThreadId` to their specific atomic states.
    /// Used for cross-thread lookups (e.g., "Does thread X have pending I/O?").
    pub(crate) worker_slots: WorkerSlots,

    /// LIFO (Last-In, First-Out) queue of parked threads.
    ///
    /// **Why LIFO?** The thread that parked most recently is the most likely to still
    /// have its working set in the CPU cache (hot), making it the best candidate
    /// to wake up next.
    pub(crate) parked_threads: RwLock<VecDeque<(Thread, ThreadId)>>,
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
    pub(super) fn register_worker(&self, core: &Core) -> Arc<AtomicUsize> {
        self.worker_slots
            .register(core)
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
    pub(crate) fn park_current_thread<T>(
        &self,
        thread_id: &ThreadId,
        injector: &Arc<crossbeam_deque::Injector<T>>,
    ) {
        let should_unpark = {
            let mut parked_threads = self.parked_threads.write();

            // We need to check these signals *while holding the lock* to avoid race
            // conditions. These are used right before we call unpark.
            if !injector.is_empty() || self.shutdown.load(Ordering::Acquire) {
                return;
            }

            let thread = thread::current();
            let should_unpark = self.worker_slots.with_data(thread_id, |data| {
                data.should_unpark.store(false, Ordering::Release);
                Arc::clone(&data.should_unpark)
            });

            parked_threads.push_back((thread, *thread_id));
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
        if let Some((thread, thread_id)) = self.parked_threads.write().pop_back() {
            self.worker_slots.with_data(&thread_id, |data| {
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

        while let Some((thread, thread_id)) = parked_threads.pop_back() {
            num_unparked += 1;
            self.worker_slots.with_data(&thread_id, |data| {
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
