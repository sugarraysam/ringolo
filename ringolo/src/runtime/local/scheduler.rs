use crate::context::{init_local_context, with_core, with_core_mut};
use crate::runtime::local::worker::Worker;
use crate::runtime::runtime::{BOX_FUTURE_THRESHOLD, RuntimeConfig};
use crate::runtime::waker::Wake;
use crate::runtime::{AddMode, EventLoop, Schedule, TaskOpts, YieldReason};
use crate::task::{JoinHandle, Notified, Task};
use anyhow::{Result, anyhow};
use std::cell::Cell;
use std::cell::RefCell;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Waker;

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
    /// Schedule a task to run next (i.e.: front of queue).
    fn schedule(&self, _is_new: bool, task: LocalTask) {
        self.get_worker().add_task(task, AddMode::Lifo);
    }

    /// Schedule a task to run soon but not next (i.e.: back of queue).
    #[track_caller]
    fn yield_now(&self, waker: &Waker, reason: YieldReason) {
        match reason {
            YieldReason::SqRingFull => {
                if let Err(e) = with_core_mut(|core| core.submit_and_wait(1, None)) {
                    panic!(
                        "FATAL: scheduler error. Unable to submit SQEs and retrying to submit failed again: {:?}",
                        e
                    );
                }
            }
            YieldReason::SlabFull => {
                if let Err(e) = with_core_mut(|core| -> Result<usize> {
                    core.submit_and_wait(1, None)?;
                    core.process_cqes(None)
                }) {
                    panic!(
                        "FATAL: scheduler error. SlabFull and could not submit or process cqes: {:?}",
                        e
                    );
                }
            }
            YieldReason::NoTaskBudget => { /* nothing to do */ }
        }

        // If the root_future is yielding, we need to handle it differently.
        if with_core(|c| c.is_polling_root()) {
            self.set_root_woken();
        } else {
            // Safety: there is two flavors of Waker in the codebase, one for
            // tasks and one for the root_future. We just checked that we are not
            // currently polling the root future.
            let task = unsafe { Notified::from_waker(waker) };
            self.get_worker().add_task(task, AddMode::Fifo);
        }
    }

    fn release(&self, _task: &Task<Self>) -> Option<Task<Self>> {
        unimplemented!("todo");
    }
}

impl Handle {
    #[track_caller]
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

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let fut_size = std::mem::size_of::<F>();
        if fut_size > BOX_FUTURE_THRESHOLD {
            self.spawn_inner(Box::pin(future))
        } else {
            self.spawn_inner(future)
        }
    }

    fn spawn_inner<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let id = crate::task::Id::next();

        // TODO: where to store `task` ?
        let (_task, notified, join_handle) =
            crate::task::new_task(future, Some(TaskOpts::STICKY), self.clone(), id);

        self.schedule(true /* is_new */, notified);

        join_handle
    }
}

impl Deref for Handle {
    type Target = Arc<Scheduler>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
