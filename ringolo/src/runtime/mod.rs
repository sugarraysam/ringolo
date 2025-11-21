use crate::task::{Notified, Task};
use anyhow::Result;
use std::fmt;
use std::sync::Arc;
use std::task::Waker;

// Public APIs
/// Cancellation APIs for tasks built on top of the global task tree.
pub mod cancel;
#[doc(inline)]
pub use cancel::{
    recursive_cancel_all, recursive_cancel_all_leaves, recursive_cancel_all_metadata,
    recursive_cancel_all_orphans, recursive_cancel_any_metadata,
};

/// Builder API to configure and create a runtime.
pub mod runtime;
#[doc(inline)]
pub use runtime::{Builder, OnCleanupError, OrphanPolicy, Runtime};

/// Utilities to spawn new tasks.
pub mod spawn;
#[doc(inline)]
pub use spawn::{TaskMetadata, TaskOpts, spawn, spawn_builder};

// Exports
#[doc(hidden)]
pub mod local;

#[doc(hidden)]
mod registry;
pub(crate) use registry::{OwnedTasks, TaskRegistry, get_orphan_root, get_root};

#[doc(hidden)]
pub(crate) use runtime::{RuntimeConfig, SPILL_TO_HEAP_THRESHOLD};

#[doc(hidden)]
pub mod stealing;

#[doc(hidden)]
mod ticker;
use ticker::{Ticker, TickerData, TickerEvents};

#[doc(hidden)]
mod waker;

#[doc(hidden)]
pub(crate) trait Schedule: Sync + Sized + 'static + std::fmt::Debug {
    /// Schedule a task to run soon.
    fn schedule(&self, task: Notified<Self>, mode: Option<AddMode>);

    /// Mechanism through which a task can suspend itself, with the intention
    /// of running again soon without being woken up. Very useful if a task was
    /// unable to register the waker for example, can fallback to yielding and
    /// try to register the waker again.
    ///
    /// By default, the scheduler will figure out the optimal AddMode and decide
    /// if task should be run again next (Lifo :: front of queue) or run again
    /// soon (Fifo :: back of queue). Only override if you have special insights.
    //
    // We pass the Waker as this is how we are able to reconstruct the appropriate
    // task. The Waker data ptr carries the task information if we are not polling
    // the root future.
    fn yield_now(&self, waker: &Waker, reason: YieldReason, mode: Option<AddMode>);

    /// The task has completed work and is ready to be released. The scheduler
    /// should release it immediately and return it. The task module will batch
    /// the ref-dec with setting other options.
    ///
    /// If the scheduler has already released the task, then None is returned.
    fn release(&self, task: &Task<Self>) -> Option<Task<Self>>;

    /// Polling the task resulted in a panic. Let the scheduler handle it according
    /// to runtime config and policies.
    fn unhandled_panic(&self, payload: SchedulerPanic);

    /// Returns TaskRegistry for this scheduler.
    fn task_registry(&self) -> Arc<dyn TaskRegistry>;
}

#[doc(hidden)]
#[derive(Debug)]
pub(crate) enum Scheduler {
    Local(local::Handle),
    Stealing(stealing::Handle),
}

#[allow(unused)]
#[doc(hidden)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum YieldReason {
    SlabFull,
    SqRingFull,
    NoTaskBudget,
    SelfYielded,
    Unknown,
}

impl From<YieldReason> for PanicReason {
    fn from(val: YieldReason) -> Self {
        match val {
            YieldReason::SlabFull => PanicReason::SlabInvalidState,
            YieldReason::SqRingFull => PanicReason::SqRingInvalidState,
            _ => PanicReason::Unknown,
        }
    }
}

#[doc(hidden)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum PanicReason {
    DuplicateTaskId,
    FailedCleanup,
    PollingFuture,
    SlabInvalidState,
    SqBatchTooLarge,
    SqRingInvalidState,
    StoringTaskOutput,
    Unknown,
}

#[doc(hidden)]
#[derive(Debug, Clone)]
pub(crate) struct SchedulerPanic {
    pub(crate) reason: PanicReason,
    pub(crate) msg: String,
}

impl SchedulerPanic {
    pub(crate) fn new(reason: PanicReason, msg: impl fmt::Display) -> Self {
        Self {
            reason,
            msg: msg.to_string(),
        }
    }
}

/// Instructs the scheduler if a task should be added at front or back of the
/// run queue.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub enum AddMode {
    /// Adds the task to the **front** of the queue (Last-In, First-Out).
    ///
    /// This is the default behavior. It prioritizes the most recently scheduled
    /// task, which is often beneficial for **CPU cache locality** as the task's
    /// state is likely still "hot" in the cache.
    #[default]
    Lifo,

    /// Adds the task to the **back** of the queue (First-In, First-Out).
    ///
    /// This provides a more "fair" scheduling approach, ensuring tasks are
    /// polled in the order they were yielded, but may be less cache-efficient
    /// than `Lifo`.
    Fifo,
}

#[doc(hidden)]
/// Abstraction of everything needed to build an event loop.
pub(crate) trait EventLoop {
    type Task;

    fn add_task(&self, task: Self::Task, mode: AddMode);

    fn find_task(&self) -> Option<Self::Task>;

    /// Event loop to drive work to completion. One of the worker will be given
    /// the "root_future", which corresponds to the entry point of the runtime,
    /// what the passes to the `block_on` function.
    //
    // Can't do &mut because scheduler needs access to the worker to schedule
    // tasks. Worker needs interior mutability.
    fn event_loop<F: Future>(&self, root_future: Option<F>) -> Result<Option<F::Output>>;
}

#[doc(hidden)]
/// Boundary value to prevent stack overflow caused by a large-sized
/// Future being placed in the stack.
pub(crate) const BOX_ROOT_FUTURE_THRESHOLD: usize = 16384;

/// We `block_on` on a special future that we refer to as the `root_future`. It
/// is guaranteed to be polled on the current thread, and is central in deciding
/// how and when the runtime returns. This is why it has looser bounds (!Send and !Sync).
///
/// It can stay on the stack if it is small enough, otherwise it gets heap
/// allocated.
pub fn block_on<F: Future>(root_fut: F) -> F::Output {
    crate::with_scheduler!(|s| {
        let fut_size = std::mem::size_of::<F>();

        if fut_size > BOX_ROOT_FUTURE_THRESHOLD {
            s.block_on(Box::pin(root_fut))
        } else {
            s.block_on(root_fut)
        }
    })
}
