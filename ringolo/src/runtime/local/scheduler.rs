use crate::context::{self, Shared};
use crate::runtime::local::worker::Worker;
use crate::runtime::registry::InsertResult;
use crate::runtime::waker::Wake;
use crate::runtime::{
    AddMode, EventLoop, OwnedTasks, PanicReason, RuntimeConfig, Schedule, SchedulerPanic,
    TaskMetadata, TaskOpts, TaskRegistry, YieldReason,
};
use crate::task::{JoinHandle, Notified, Task};
#[allow(unused)]
use crate::utils::scheduler::{Call, Method, Tracker};
use anyhow::Result;
use std::cell::RefCell;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Waker;

pub(crate) type LocalTask = Notified<Handle>;

#[derive(Debug)]
pub struct Scheduler {
    pub(crate) cfg: RuntimeConfig,

    pub(crate) default_task_opts: TaskOpts,

    pub(crate) tasks: Arc<OwnedTasks<Handle>>,

    pub(crate) worker: Worker,

    pub(crate) shared: Arc<Shared>,

    pub(crate) root_woken: RefCell<bool>,

    #[cfg(test)]
    pub(crate) tracker: Tracker,
}

impl Scheduler {
    pub(crate) fn new(cfg: &RuntimeConfig) -> Self {
        let shared = Arc::new(Shared::new(cfg));

        Self {
            cfg: cfg.clone(),
            default_task_opts: cfg.default_task_opts(),
            tasks: OwnedTasks::new(cfg),
            worker: Worker::new(cfg, Arc::clone(&shared)),
            shared,
            root_woken: RefCell::new(true),

            #[cfg(test)]
            tracker: Tracker::new(),
        }
    }

    pub(crate) fn into_handle(self) -> Handle {
        Handle(Arc::new(self))
    }

    pub(super) fn set_root_woken(&self) {
        self.root_woken.replace(true);
    }

    pub(super) fn reset_root_woken(&self) -> bool {
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
    fn schedule(&self, task: LocalTask) {
        #[cfg(test)]
        self.track(Method::Schedule, Call::Schedule { id: task.id() });

        // We use LIFO because most of the time schedule is called when a task
        // is woken up as part of the scheduler completion/polling routine. We
        // want these *hot tasks* to run next.
        self.worker.add_task(task, AddMode::Lifo);
    }

    /// Schedule a task to run soon.
    #[track_caller]
    fn yield_now(&self, waker: &Waker, reason: YieldReason, mode: Option<AddMode>) {
        #[cfg(test)]
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
                    self.panic_on_scheduler_error(
                        reason.into(),
                        format!("Unable to submit or process cqes: {:?}", &e),
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
            // Safety: there are two flavors of Waker in the codebase, one for
            // tasks and one for the root_future. We just checked that we are not
            // currently polling the root future.
            if let Some(task) = unsafe { Notified::from_waker(waker) } {
                self.worker.add_task(task, mode);
            }
        }
    }

    fn release(&self, task: &Task<Self>) -> Option<Task<Self>> {
        #[cfg(test)]
        self.track(Method::Release, Call::Release { id: task.id() });
        self.tasks.remove(&task.id())
    }

    fn unhandled_panic(&self, payload: SchedulerPanic) {
        #[cfg(test)]
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

    fn task_registry(&self) -> Arc<dyn TaskRegistry> {
        self.tasks.clone()
    }
}

impl Handle {
    pub(crate) fn block_on<F: Future>(&self, root_fut: F) -> F::Output {
        match self.worker.event_loop(Some(root_fut)) {
            Ok(Some(res)) => res,

            Ok(None) => self.panic_on_scheduler_error(
                PanicReason::PollingFuture,
                "Event loop returned 'None' without an error.",
            ),

            Err(e) => self.panic_on_scheduler_error(
                PanicReason::PollingFuture,
                format!("Failed to drive future to completion: {:?}", &e),
            ),
        }
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
        let opts = opts.map(|opt| opt | self.default_task_opts);

        #[cfg(test)]
        self.track(
            Method::Spawn,
            Call::Spawn {
                opts: opts.clone().unwrap_or_default(),
                metadata: metadata.clone(),
            },
        );

        let (task, notified, join_handle) =
            crate::task::new_task(future, opts, metadata, self.clone());

        match self.tasks.insert(task) {
            InsertResult::Ok => self.schedule(notified),
            InsertResult::Shutdown => {
                // TODO: tracing :: warn but safe to continue
                eprintln!("Tried to spawn a task after shutdown...");
            }
            InsertResult::Duplicate(id) => self.unhandled_panic(SchedulerPanic::new(
                PanicReason::DuplicateTaskId,
                format!("Tried to spawn task with duplicate id: {}", id),
            )),
        }

        join_handle
    }

    pub(crate) fn shutdown(&self) {
        // This is not necessary for single-threaded runtime but we guarantee
        // thread-safe oneshot shutdown.
        if !self.shared.shutdown.swap(true, Ordering::AcqRel) {
            self.tasks.close_and_partition();
            self.tasks.shutdown_all_partitions();
        }
    }

    #[cold]
    #[track_caller]
    fn panic_on_scheduler_error<M>(&self, reason: PanicReason, msg: M) -> !
    where
        M: std::fmt::Display,
    {
        self.unhandled_panic(SchedulerPanic::new(reason, msg));
        unreachable!("Scheduler panic");
    }
}

impl Deref for Handle {
    type Target = Arc<Scheduler>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
