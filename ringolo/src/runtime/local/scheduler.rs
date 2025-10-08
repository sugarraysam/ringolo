use crate::context::init_local_context;
use crate::runtime::local::worker::Worker;
use crate::runtime::runtime::RuntimeConfig;
use crate::runtime::waker::Wake;
use crate::runtime::{EventLoop, Schedule};
use crate::task::{Notified, Task};
use anyhow::{Result, anyhow};
use std::cell::Cell;
use std::cell::RefCell;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

// Use a thread_local variable to track if a runtime is already active on this thread.
thread_local! {
    static IS_RUNTIME_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

pub(crate) type LocalTask = Notified<Handle>;

#[derive(Debug)]
pub struct Scheduler {
    pub(crate) cfg: RuntimeConfig,

    worker: Rc<Worker>,

    pub(crate) root_woken: RefCell<bool>,
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

            // We should always poll the root future on first iteration of the
            // loop. Afterwards, this will only be set to true after the waker
            // is awakened.
            root_woken: RefCell::new(true),
        })
    }

    pub(crate) fn get_worker(&self) -> &Rc<Worker> {
        &self.worker
    }

    pub(crate) fn into_handle(self) -> Handle {
        Handle(Arc::new(self))
    }

    pub(crate) fn set_root_woken(&self) {
        self.root_woken.replace(true);
    }

    pub(crate) fn reset_root_woken(&self) -> bool {
        self.root_woken.replace(false)
    }
}

// Safety: local Scheduler is only ever used from context of a single thread. We
// don't want to slow down performance and use thread-safe data structures so
// let's lie to the compiler instead :)
unsafe impl Send for Scheduler {}
unsafe impl Sync for Scheduler {}

impl Wake for Scheduler {
    fn wake(arc_self: Arc<Self>) {
        Wake::wake_by_ref(&arc_self);
    }

    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.root_woken.replace(true);
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        IS_RUNTIME_ACTIVE.with(|is_active| is_active.set(false));
    }
}

#[derive(Debug, Clone)]
pub struct Handle(Arc<Scheduler>);

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
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let worker = Rc::new(self.get_worker());

        if let Err(e) = init_local_context(&self.cfg, self.clone()) {
            panic!("Failed to initialize local context: {:?}", e);
        }

        match worker.event_loop(Some(future)) {
            Ok(res) => res,
            Err(e) => panic!("Failed to drive future to completion: {:?}", e),
        }
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

    assert_impl_all!(Scheduler: Send, Sync, Wake);
    assert_impl_all!(Handle: Send, Sync, Schedule);
}
