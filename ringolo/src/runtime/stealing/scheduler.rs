use crate::context::{ThreadId, current_thread_id};
use crate::runtime::EventLoop;
use crate::runtime::Schedule;
use crate::runtime::runtime::{RuntimeConfig, ThreadNameFn};
use crate::runtime::stealing::context::Shared;
use crate::runtime::stealing::worker::Worker;
use crate::task::{Notified, Task};
use std::collections::HashMap;
use std::ops::Deref;

use crossbeam_deque::{Injector, Worker as CbWorker};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) type StealableTask = Notified<Handle>;

#[derive(Debug)]
pub struct Scheduler {
    /// Runtime confguration to be injected in context and worker
    cfg: RuntimeConfig,

    /// The global injector queue for new tasks.
    injector: Arc<Injector<StealableTask>>,

    /// Shared context between workers, to be injected in every worker thread.
    shared: Arc<Shared>,
}

impl Scheduler {
    pub(crate) fn new(cfg: RuntimeConfig) -> Self {
        let shared = Arc::new(Shared::new());

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
                let shared_clone = Arc::clone(&shared);

                std::thread::spawn(move || {
                    let worker = w;
                    // init_stealing_context(shared_clone, &worker);
                    worker.event_loop();
                })
            })
            .collect::<Vec<_>>();

        Self {
            cfg,
            injector,
            shared,
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
            // - expect_stealing_context
            // - get_worker
            // - worker stores itself (a RefCell in context)
            // let ctx = expect_stealing_context();
            // ctx.get_worker().add_task(task);
        }
    }

    fn release(&self, _task: &Task<Self>) -> Option<Task<Self>> {
        unimplemented!("todo");
    }
}

impl Handle {
    pub fn spawn<F: Future>(f: F) {
        unimplemented!("TODO");
    }

    pub fn block_on<F: Future>(f: F) {
        unimplemented!("TODO");
    }
}

impl Deref for Handle {
    type Target = Scheduler;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
