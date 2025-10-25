use crate::context::Shared;
use crate::runtime::stealing::worker::Worker;
use crate::runtime::{
    AddMode, OwnedTasks, PanicReason, RuntimeConfig, Schedule, SchedulerPanic, TaskMetadata,
    TaskOpts, TaskRegistry, YieldReason,
};
use crate::sqe::IoError;
use crate::task::{Id, JoinHandle, Notified, Task};
use crate::utils::scheduler::{Call, Method, Tracker};
use crossbeam_deque::{Injector, Worker as CbWorker};
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use std::task::Waker;

pub(crate) type StealableTask = Notified<Handle>;

#[derive(Debug)]
pub struct Scheduler {
    /// Runtime confguration to be injected in context and worker
    pub(crate) cfg: RuntimeConfig,

    pub(crate) tasks: Arc<OwnedTasks<Handle>>,

    /// The global injector queue for new tasks.
    pub(crate) injector: Arc<Injector<StealableTask>>,

    /// Shared context between workers, to be injected in every worker thread.
    pub(crate) shared: Arc<Shared>,

    #[cfg(test)]
    pub(crate) tracker: Tracker,
}

impl Scheduler {
    pub(crate) fn new(cfg: RuntimeConfig) -> Self {
        let shared = Arc::new(Shared::new(&cfg));

        let injector = Arc::new(Injector::new());
        let mut local_queues = Vec::with_capacity(cfg.worker_threads);
        let mut stealers = Vec::with_capacity(cfg.worker_threads);

        for _ in 0..cfg.worker_threads {
            let w = CbWorker::new_lifo();
            stealers.push(w.stealer());
            local_queues.push(w);
        }

        let workers = local_queues
            .into_iter()
            .enumerate()
            .map(|(i, local_queue)| {
                // Exclude this worker's stealer. Stealer queue is cheap to clone
                // inner value is behind an Arc so we just increment the ref count.
                let other_stealers = stealers
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(_, s)| s.clone())
                    .collect::<Vec<_>>();

                Worker::new(cfg.clone(), injector.clone(), local_queue, other_stealers)
            })
            .collect::<Vec<_>>();

        // TODO: spawn workers, do something with handles
        let _handles = workers
            .into_iter()
            .map(|w| {
                let _shared_clone = Arc::clone(&shared);

                std::thread::spawn(move || {
                    let _worker = w;
                    // init_stealing_context(shared_clone, &worker);
                    // worker.event_loop();
                })
            })
            .collect::<Vec<_>>();

        Self {
            cfg: cfg.clone(),
            tasks: OwnedTasks::new(cfg.sq_ring_size),
            injector,
            shared,

            #[cfg(test)]
            tracker: Tracker::new(),
        }
    }

    pub(crate) fn into_handle(self) -> Handle {
        Handle(Arc::new(self))
    }

    // TODO: spawn threads function
}

#[derive(Debug, Clone)]
pub struct Handle(Arc<Scheduler>);

impl Schedule for Handle {
    fn schedule(&self, is_new: bool, task: StealableTask) {
        if is_new {
            // New tasks go to the global queue to be picked up by any worker.
            self.0.injector.push(task);
        } else {
            // TODO:
            // - get_worker from thread_id (workers map in Shared)
            // with_shared(|shared| {
            //     let mode = ...;
            //     shared.get_worker().add_task(task, mode);
            // });
        }
    }

    fn yield_now(&self, waker: &Waker, reason: YieldReason, mode: Option<AddMode>) {
        unimplemented!("todo");
    }

    fn release(&self, _task: &Task<Self>) -> Option<Task<Self>> {
        unimplemented!("todo");
    }

    /// Polling the task resulted in a panic.
    fn unhandled_panic(&self, payload: SchedulerPanic) {
        unimplemented!("todo");
    }

    fn task_registry(&self) -> Arc<dyn TaskRegistry> {
        self.tasks.clone()
    }
}

impl Handle {
    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        unimplemented!("TODO");
    }

    pub(crate) fn spawn<F>(
        &self,
        future: F,
        opts: Option<TaskOpts>,
        metadata: Option<TaskMetadata>,
    ) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let id = crate::task::Id::next();

        let (task, notified, join_handle) =
            crate::task::new_task(future, opts, metadata, self.clone());

        // TODO: insert task take ownership
        // debug_assert!(self.tasks.insert(task).is_none());

        self.schedule(true, notified);

        join_handle
    }
}

impl Deref for Handle {
    type Target = Arc<Scheduler>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::assert_impl_all;

    assert_impl_all!(Scheduler: Send, Sync);
    assert_impl_all!(Handle: Send, Sync, Schedule);
}
