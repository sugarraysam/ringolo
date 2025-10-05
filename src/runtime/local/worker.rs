use crate::runtime::EventLoop;
use crate::runtime::local::scheduler::LocalTask;
use crate::runtime::runtime::RuntimeConfig;
use crossbeam_deque::Worker as CbWorker;
use std::rc::Rc;

#[derive(Debug)]
pub(crate) struct Worker {
    /// Handle to single local queue.
    pollable: crossbeam_deque::Worker<LocalTask>,
}

impl Worker {
    pub(super) fn new(_cfg: &RuntimeConfig) -> Self {
        Self {
            pollable: CbWorker::new_lifo(),
        }
    }
}

impl EventLoop for Worker {
    type Task = LocalTask;

    fn add_task(&self, task: Self::Task) {
        self.pollable.push(task);
    }

    fn find_task(&self) -> Option<Self::Task> {
        self.pollable.pop()
    }

    fn event_loop(&self) {
        unimplemented!("TODO");
    }
}
