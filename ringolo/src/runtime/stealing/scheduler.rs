use crate::context::{self, Shared};
use crate::runtime::registry::InsertResult;
use crate::runtime::stealing::pool::{ThreadPool, WorkerRef};
use crate::runtime::waker::Wake;
use crate::runtime::{
    AddMode, EventLoop, OwnedTasks, PanicReason, RuntimeConfig, Schedule, SchedulerPanic,
    TaskMetadata, TaskOpts, TaskRegistry, YieldReason,
};
use crate::task::ThreadId;
use crate::task::{JoinHandle, Notified, Task};
#[allow(unused)]
use crate::utils::scheduler::{Call, Method, Tracker};
use anyhow::Result;
use crossbeam_deque::Injector;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, OnceLock};
use std::task::Waker;

pub(crate) type StealableTask = Notified<Handle>;

#[derive(Debug)]
pub struct Scheduler {
    pub(crate) cfg: RuntimeConfig,

    pub(crate) default_task_opts: TaskOpts,

    /// The Waker associated with the scheduler can be used to await a JoinHandle.
    /// This means we need a thread-safe way to interact with `root_woken`.
    pub(super) root_woken: AtomicBool,

    /// Shared context between workers, to be injected in every worker thread.
    pub(crate) shared: Arc<Shared>,

    pub(crate) tasks: Arc<OwnedTasks<Handle>>,

    /// The global injector queue for new tasks.
    pub(super) injector: Arc<Injector<StealableTask>>,

    /// ThreadPool of workers.
    pub(super) pool: OnceLock<ThreadPool>,

    /// We use this barrier to coordinate startup of maintenance tasks across
    /// all workers, including the root worker.
    pub(super) maintenance_task_barrier: Arc<Barrier>,

    #[cfg(test)]
    pub(crate) tracker: Tracker,
}

impl Scheduler {
    pub(crate) fn new(cfg: &RuntimeConfig) -> Self {
        Self {
            cfg: cfg.clone(),
            default_task_opts: cfg.default_task_opts(),
            shared: Arc::new(Shared::new(cfg)),
            tasks: OwnedTasks::new(cfg),
            injector: Arc::new(Injector::new()),
            root_woken: AtomicBool::new(true),
            pool: OnceLock::new(),
            maintenance_task_barrier: Arc::new(Barrier::new(cfg.worker_threads + 1)),

            #[cfg(test)]
            tracker: Tracker::new(),
        }
    }

    pub(crate) fn into_handle(self) -> Handle {
        Handle(Arc::new(self))
    }

    pub(super) fn set_root_woken(&self) {
        self.root_woken.store(true, Ordering::Release);
    }

    pub(super) fn reset_root_woken(&self) -> bool {
        self.root_woken.swap(false, Ordering::AcqRel)
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

// Safety: Only reason we need this is because we refuse to use an atomic for
// `root_woken` which is fine as this is the responsibility of a single worker.
unsafe impl Send for Scheduler {}
unsafe impl Sync for Scheduler {}

impl Wake for Scheduler {
    fn wake(arc_self: Arc<Self>) {
        Wake::wake_by_ref(&arc_self);
    }

    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.root_woken.store(true, Ordering::Release);
    }
}

#[derive(Debug, Clone)]
pub struct Handle(Arc<Scheduler>);

impl Schedule for Handle {
    #[track_caller]
    fn schedule(&self, task: StealableTask, mode: Option<AddMode>) {
        #[cfg(test)]
        self.track(
            Method::Schedule,
            Call::Schedule {
                id: task.id(),
                is_stealable: task.is_stealable(),
                opts: task.get_opts(),
                mode,
            },
        );

        // Maintenance tasks never go on the injector. Otherwise, tasks spawned
        // while polling `root_future` OR spawned before the user calls `block_on`
        // are added to the injector.
        if !task.is_maintenance_task()
            && context::with_core(|core| {
                core.is_polling_root() || core.is_pre_block_on(&self.root_thread_id())
            })
        {
            self.0.injector.push(task);
            let _ = self.shared.unpark_one_thread();
        } else {
            // Otherwise, add task to the local worker.
            self.add_task_to_current_worker(task, mode.unwrap_or(AddMode::Lifo));
        }
    }

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
            YieldReason::SelfYielded | YieldReason::NoTaskBudget | YieldReason::Unknown => { /* */ }
        }

        // If the root_future is yielding, we need to handle it differently.
        if context::with_core(|c| c.is_polling_root()) {
            self.set_root_woken();
        } else {
            // Safety: there is two flavors of Waker in the codebase, one for
            // tasks and one for the root_future. We just checked that we are not
            // currently polling the root future.
            if let Some(task) = unsafe { Notified::from_waker(waker) } {
                self.schedule(task, Some(mode));
            }
        }
    }

    fn release(&self, task: &Task<Self>) -> Option<Task<Self>> {
        #[cfg(test)]
        self.track(Method::Release, Call::Release { id: task.id() });
        self.tasks.remove(&task.id())
    }

    /// Polling the task resulted in a panic.
    #[track_caller]
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

    fn task_registry(&self) -> std::sync::Arc<dyn TaskRegistry> {
        self.tasks.clone()
    }
}

impl Handle {
    pub(crate) fn spawn_workers(&self) {
        self.pool.get_or_init(|| ThreadPool::new(self));
    }

    pub(super) fn wait_for_thread_pool(&self) {
        let _ = self.pool.wait();
    }

    pub(super) fn wait_for_all_maintenance_tasks(&self) {
        let _ = self.maintenance_task_barrier.wait();
    }

    #[track_caller]
    pub(crate) fn block_on<F: Future>(&self, root_fut: F) -> F::Output {
        match self.get_pool().root_worker.event_loop(Some(root_fut)) {
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
            InsertResult::Ok => {
                self.schedule(notified, opts.and_then(|o| o.initial_spawn_add_mode()))
            }
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

    /// Shutdown the stealing scheduler.
    pub(crate) fn shutdown(&self) -> Result<()> {
        // Set global shutdown signal - ensure we shutdown once
        if !self.shared.shutdown.swap(true, Ordering::AcqRel) {
            // Close the registry and partition tasks by thread_id - this is because
            // dropping futures has to happen on the thread that owns the task, as
            // SQE backends use thread-local context in Drop impl.
            self.tasks.close_and_partition();

            // Unpark all workers - important to do *after* partitioning tasks.
            context::with_shared(|shared| shared.unpark_all_threads());

            // Wait for workers to cancel their tasks.
            let res = self
                .pool
                .get()
                .expect("thread pool not initialized")
                .join_all();

            // Cancel all remaining tasks.
            self.tasks.shutdown_all_partitions();

            res
        } else {
            Ok(())
        }
    }

    #[track_caller]
    fn add_task_to_current_worker(&self, task: StealableTask, mode: AddMode) {
        let thread_id = context::with_core(|core| core.thread_id);

        self.get_pool()
            .with_worker(&thread_id, |worker| match worker {
                WorkerRef::NonRoot(w) => w.add_task(task, mode),

                // We are only allowed to schedule the maintenance task on the
                // root_worker. Everything else goes in the pool.
                WorkerRef::Root(w) => {
                    debug_assert!(
                        task.is_maintenance_task(),
                        "Can only schedule maintenance task on root_worker"
                    );
                    w.add_task(task, mode)
                }
            });
    }

    #[track_caller]
    pub(super) fn get_pool(&self) -> &ThreadPool {
        self.pool.get().expect("thread pool not initialized")
    }

    fn root_thread_id(&self) -> ThreadId {
        self.get_pool().root_worker.thread_id
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

#[doc(hidden)]
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
