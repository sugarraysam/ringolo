use crate::context;
use crate::runtime::local::worker::Worker;
use crate::runtime::runtime::RuntimeConfig;
use crate::runtime::waker::Wake;
use crate::runtime::{
    AddMode, EventLoop, OwnedTasks, Schedule, SchedulerPanic, TaskOpts, YieldReason,
};
use crate::task::{JoinHandle, Notified, Task};
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

    pub(crate) tasks: OwnedTasks<Handle>,

    pub(crate) worker: Worker,

    pub(crate) root_woken: RefCell<bool>,

    #[cfg(test)]
    pub(crate) tracker: Tracker,
}

impl Scheduler {
    pub(crate) fn new(cfg: &RuntimeConfig) -> Self {
        Self {
            cfg: cfg.clone(),
            tasks: OwnedTasks::new(cfg.sq_ring_size),
            worker: Worker::new(cfg),
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

        // We use LIFO because most of the time schedule is called when a task
        // is woken up as part of the scheduler completion/polling routine. We
        // want these *hot tasks* to run next.
        self.worker.add_task(task, AddMode::Lifo);
    }

    /// Schedule a task to run soon.
    #[track_caller]
    fn yield_now(&self, waker: &Waker, reason: YieldReason, mode: Option<AddMode>) {
        self.track(Method::YieldNow, Call::YieldNow { reason, mode });

        // Default to adding the task to the back of the queue.
        let mut mode = mode.unwrap_or(AddMode::Fifo);

        match reason {
            YieldReason::SqRingFull | YieldReason::SlabFull => {
                // Reduce scheduler latency, event outside control of task add to
                // front of queue in this case.
                mode = AddMode::Lifo;

                if let Err(e) = context::with_slab_and_ring_mut(|slab, ring| -> Result<usize> {
                    ring.submit_and_wait(1, None)?;
                    ring.process_cqes(slab, None)
                }) {
                    panic!(
                        "FATAL: scheduler error: {:?}. Unable to submit or process cqes: {:?}",
                        reason, e
                    );
                }
            }

            // Nothing to do, keep FIFO order as task should run soon but not next.
            YieldReason::NoTaskBudget | YieldReason::Unknown | YieldReason::SelfYielded => { /* */ }
        }

        // If the root_future is yielding, we need to handle it differently.
        if context::with_core(|c| c.is_polling_root()) {
            self.set_root_woken();
        } else {
            // Safety: there is two flavors of Waker in the codebase, one for
            // tasks and one for the root_future. We just checked that we are not
            // currently polling the root future.
            let task = unsafe { Notified::from_waker(waker) };
            self.worker.add_task(task, mode);
        }
    }

    fn release(&self, task: &Task<Self>) -> Option<Task<Self>> {
        self.track(Method::Release, Call::Release { id: task.id() });
        self.tasks.remove(&task.id())
    }

    fn unhandled_panic(&self, payload: SchedulerPanic) {
        self.track(
            Method::UnhandledPanic,
            Call::UnhandledPanic {
                reason: payload.reason,
            },
        );

        // TODO:
        // - tokio uses Panic handlers
        // - cleanup? drain? clean shutdown?

        panic!(
            "FATAL: scheduler panic. Reason: {:?}, msg: {:?}",
            payload.reason, payload.msg
        );
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
        self.track(
            Method::Spawn,
            Call::Spawn {
                opts: task_opts.unwrap_or_default(),
            },
        );

        // All tasks are sticky on local scheduler.
        let task_opts = task_opts.map(|opt| opt | TaskOpts::STICKY);

        let (task, notified, join_handle) = crate::task::new_task(future, task_opts, self.clone());

        let existed = self.tasks.insert(task, context::current_task_id());
        debug_assert!(!existed);

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
