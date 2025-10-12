use crate::context::{with_core, with_core_mut};
use crate::runtime::local::worker::Worker;
use crate::runtime::runtime::RuntimeConfig;
use crate::runtime::waker::Wake;
use crate::runtime::{
    AddMode, EventLoop, OwnedTasks, PanicReason, Schedule, TaskOpts, YieldReason,
};
use crate::task::{Id, JoinHandle, Notified, Task};
#[allow(unused)]
use crate::utils::scheduler::{Call, Method, Tracker};
use anyhow::Result;
use std::cell::RefCell;
use std::ops::Deref;
use std::sync::Arc;
use std::task::Waker;

pub(crate) type LocalTask = Notified<Handle>;

#[derive(Debug)]
pub struct Scheduler {
    #[allow(unused)]
    pub(crate) cfg: RuntimeConfig,

    pub(crate) worker: Worker,

    pub(crate) tasks: OwnedTasks<Handle>,

    pub(crate) root_woken: RefCell<bool>,

    #[cfg(test)]
    pub(crate) tracker: Tracker,
}

impl Scheduler {
    pub(crate) fn new(cfg: &RuntimeConfig) -> Self {
        Self {
            cfg: cfg.clone(),
            worker: Worker::new(cfg),
            tasks: OwnedTasks::new(cfg.sq_ring_size),
            root_woken: RefCell::new(true),

            #[cfg(test)]
            tracker: Tracker::new(),
        }
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

    // Small price to pay to get introspection on all scheduler calls during
    // testing. No op in release builds.
    #[allow(unused)]
    #[inline(always)]
    fn track(&self, method: Method, call: Call) {
        #[cfg(test)]
        self.tracker.record(method, call);
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

#[derive(Debug, Clone)]
pub struct Handle(Arc<Scheduler>);

// Safety: local::Scheduler will only be accessed by the Local thread.
unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}

impl Schedule for Handle {
    /// Schedule a task to run next (i.e.: front of queue).
    fn schedule(&self, is_new: bool, task: LocalTask) {
        self.track(
            Method::Schedule,
            Call::Schedule {
                is_new,
                id: task.id(),
            },
        );
        self.worker.add_task(task, AddMode::Lifo);
    }

    /// Schedule a task to run soon but not next (i.e.: back of queue).
    #[track_caller]
    fn yield_now(&self, waker: &Waker, reason: YieldReason) {
        self.track(Method::YieldNow, Call::YieldNow { reason });
        match reason {
            YieldReason::SqRingFull | YieldReason::SlabFull => {
                if let Err(e) = with_core_mut(|core| -> Result<usize> {
                    core.submit_and_wait(1, None)?;
                    core.process_cqes(None)
                }) {
                    panic!(
                        "FATAL: scheduler error: {:?}. Unable to submit or process cqes: {:?}",
                        reason, e
                    );
                }
            }
            YieldReason::NoTaskBudget | YieldReason::Unknown => { /* nothing to do */ }
        }

        // If the root_future is yielding, we need to handle it differently.
        if with_core(|c| c.is_polling_root()) {
            self.set_root_woken();
        } else {
            // Safety: there is two flavors of Waker in the codebase, one for
            // tasks and one for the root_future. We just checked that we are not
            // currently polling the root future.
            let task = unsafe { Notified::from_waker(waker) };

            // Lifo to minimize scheduler latency.
            self.worker.add_task(task, AddMode::Lifo);
        }
    }

    fn release(&self, task: &Task<Self>) -> Option<Task<Self>> {
        self.track(Method::Release, Call::Release { id: task.id() });
        self.tasks.remove(&task.id())
    }

    fn unhandled_panic(&self, reason: PanicReason) {
        self.track(Method::UnhandledPanic, Call::UnhandledPanic { reason });

        // TODO: how to handle? What does tokio do? For now just crash.
        panic!(
            "FATAL: scheduler error. Unhandled panic in task {:?}.",
            reason
        );

        // By default, we shutdown the runtime.
        // self.tasks.shutdown_all();
    }
}

impl Handle {
    #[track_caller]
    pub(crate) fn block_on<F: Future>(&self, root_fut: F) -> F::Output {
        match self.worker.event_loop(Some(root_fut)) {
            Ok(res) => res,
            Err(e) => panic!("Failed to drive future to completion: {:?}", e),
        }
    }

    pub(crate) fn spawn<F>(&self, future: F, task_opts: Option<TaskOpts>) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.track(Method::Spawn, Call::Spawn);

        // All tasks are sticky on local scheduler.
        let task_opts = task_opts.map(|opt| opt | TaskOpts::STICKY);

        let (task, notified, join_handle) =
            crate::task::new_task(future, task_opts, self.clone(), Id::next());

        let existed = self.tasks.insert(task);
        debug_assert!(existed.is_none());

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
