use crate::runtime::stealing::scheduler::StealableTask;
use crate::runtime::{AddMode, EventLoop, RuntimeConfig};
use anyhow::Result;
use crossbeam_deque::{Injector, Stealer, Worker as CbWorker};
use std::iter;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct Worker {
    /// Global injector queue where new tasks are pushed.
    global: Arc<Injector<StealableTask>>,

    /// Local pollable queue for tasks that are not stealable by other threads.
    /// This means that these tasks have locally pending IO on this thread's
    /// iouring.
    pollable: CbWorker<StealableTask>,

    /// Local stealable queue for tasks that don't have any locally pending IO.
    /// By definition they are also pollable.
    stealable: CbWorker<StealableTask>,

    /// Handle to all of the other worker's stealable queues. If there are N
    /// workers we will have N-1 queues to steal from.
    stealers: Vec<Stealer<StealableTask>>,
}

impl Worker {
    pub(crate) fn new(
        _cfg: RuntimeConfig,
        global: Arc<Injector<StealableTask>>,
        stealable: CbWorker<StealableTask>,
        stealers: Vec<Stealer<StealableTask>>,
    ) -> Self {
        Self {
            global,
            pollable: CbWorker::new_lifo(),
            stealable,
            stealers,
        }
    }
}

impl EventLoop for Worker {
    type Task = StealableTask;

    fn add_task(&self, task: Self::Task, mode: AddMode) {
        if task.is_stealable() {
            self.stealable.push(task);
        } else {
            self.pollable.push(task);
        }
    }

    /// Always start popping from pollable as these tasks were just scheduled so
    /// we expect the CPU cache to be hot. This is because we always push and pop
    /// from the head of the queue (LIFO).
    ///
    /// Another reason why this is a good choice is that it gives more time for
    /// other threads to find tasks in our stealable queue and try to load balance.
    ///
    /// Always steal from back and pop from front to reduce contention.
    fn find_task(&self) -> Option<Self::Task> {
        // 1. Look in our pollable queue
        self.pollable.pop().or_else(|| {
            // 2. Look in our stealable queue
            self.stealable.pop().or_else(|| {
                // 3. No local work, repeatedly try the global injector and other
                //    workers stealable queues.
                iter::repeat_with(|| {
                    self.global
                        // Work from the injector is by definition stealable as it has not
                        // scheduled any IO on any thread.
                        .steal_batch_and_pop(&self.stealable)
                        .or_else(|| self.stealers.iter().map(|s| s.steal()).collect())
                })
                .find(|s| !s.is_retry())
                .and_then(|s| s.success())
            })
        })
    }

    fn event_loop<F: Future>(&self, _root_future: Option<F>) -> Result<F::Output> {
        unimplemented!("TODO");
    }
}
