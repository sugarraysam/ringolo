use crate::context::init_local_context;
use crate::runtime::local::context::Context;
use crate::runtime::local::worker::Worker;
use crate::runtime::runtime::RuntimeConfig;
use crate::runtime::{EventLoop, Schedule};
use crate::task::{Notified, Task};
use anyhow::{Result, anyhow};
use crossbeam_deque::Worker as CbWorker;
use std::cell::Cell;
use std::ops::Deref;
use std::rc::Rc;
use std::thread;

// Use a thread_local variable to track if a runtime is already active on this thread.
thread_local! {
    static IS_RUNTIME_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

pub(crate) type LocalTask = Notified<Handle>;

#[derive(Debug)]
pub struct Scheduler {
    pub(crate) cfg: RuntimeConfig,

    worker: Rc<Worker>,
}

impl Scheduler {
    pub(crate) fn try_new(cfg: RuntimeConfig) -> Result<Self> {
        IS_RUNTIME_ACTIVE.with(|is_active| -> Result<()> {
            if is_active.get() {
                Err(anyhow!(
                    "Cannot create a new LocalRuntime: a runtime is already active on this thread."
                ))
            } else {
                is_active.set(true);
                Ok(())
            }
        })?;

        let worker = Worker::new(&cfg);
        Ok(Self {
            cfg,
            worker: Rc::new(worker),
        })
    }

    pub(crate) fn get_worker(&self) -> &Rc<Worker> {
        &self.worker
    }

    pub(crate) fn into_handle(self) -> Handle {
        Handle(Rc::new(self))
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        IS_RUNTIME_ACTIVE.with(|is_active| is_active.set(false));
    }
}

#[derive(Debug, Clone)]
pub struct Handle(Rc<Scheduler>);

// Safety: local::Scheduler will only be accessed by the Local thread.
unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}

impl Schedule for Handle {
    fn schedule(&self, _is_new: bool, task: LocalTask) {
        self.get_worker().add_task(task)
    }

    fn release(&self, _task: &Task<Self>) -> Option<Task<Self>> {
        unimplemented!("todo");
    }
}

impl Handle {
    // TODO:
    // - how to return value to user?
    // - make it NOT safe to call `init_local_context` more than once
    pub fn block_on<F: Future>(&self, f: F) -> Result<()> {
        let worker = Rc::new(self.get_worker());

        init_local_context(&self.cfg, self.clone(), Rc::clone(&worker));
        worker.event_loop();

        Ok(())
    }
}

impl Deref for Handle {
    type Target = Scheduler;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
