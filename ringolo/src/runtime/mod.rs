use crate::{
    runtime::cleanup::OpCleanupPayload,
    task::{JoinHandle, Notified, Task},
    with_scheduler,
};
use anyhow::Result;
use bitflags::bitflags;
use std::fmt;
use std::task::Waker;

// Public API
pub mod runtime;
pub use runtime::Builder;

// Exports
pub(crate) mod cleanup;
pub(crate) use cleanup::CleanupTaskBuilder;

pub(crate) mod local;

pub(crate) mod registry;
pub(crate) use registry::OwnedTasks;

pub(crate) use runtime::{RuntimeConfig, SPILL_TO_HEAP_THRESHOLD};

pub(crate) mod stealing;

mod ticker;
use ticker::{Ticker, TickerData, TickerEvents};

mod waker;

/// Scheduler trait
pub(crate) trait Schedule: Sync + Sized + 'static {
    /// Schedule a task to run soon.
    fn schedule(&self, is_new: bool, task: Notified<Self>);

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
}

#[derive(Debug)]
pub(crate) enum Scheduler {
    Local(local::Handle),
    Stealing(stealing::Handle),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum YieldReason {
    SlabFull,
    SqRingFull,
    NoTaskBudget,
    SelfYielded,
    Unknown,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum PanicReason {
    SqBatchTooLarge,
    SqRingInvalidState,
    SlabInvalidState,
    PollingFuture,
    StoringTaskOutput,
    CleanupTask,
    Unknown,
}

#[derive(Debug, Clone)]
pub(crate) struct SchedulerPanic {
    pub(crate) reason: PanicReason,
    pub(crate) msg: String,
}

impl SchedulerPanic {
    pub fn new(reason: PanicReason, msg: impl fmt::Display) -> Self {
        Self {
            reason,
            msg: msg.to_string(),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum AddMode {
    Fifo,
    Lifo,
}

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
    fn event_loop<F: Future>(&self, root_future: Option<F>) -> Result<F::Output>;
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
    pub(crate) struct TaskOpts: u32 {
        /// Task will stick to the thread onto which it is created.
        const STICKY = 1;
    }
}

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
    with_scheduler!(|s| {
        let fut_size = std::mem::size_of::<F>();

        if fut_size > BOX_ROOT_FUTURE_THRESHOLD {
            s.block_on(Box::pin(root_fut))
        } else {
            s.block_on(root_fut)
        }
    })
}

// Future gets boxed in `task::layout::TaskLayout::new`, so don't box it
// twice like the `root_future`.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    with_scheduler!(|s| { s.spawn(future, None) })
}

pub(crate) fn spawn_cleanup<T: OpCleanupPayload>(builder: CleanupTaskBuilder<T>) -> JoinHandle<()> {
    with_scheduler!(|s| {
        // Copy the runtime OnCleanupError policy into the builder.
        let task = builder.on_error(s.cfg.on_cleanup_error).build();

        // Cleanup tasks need to be sticky to the local thread. It does not make sense
        // to cancel an io_uring operation from another thread.
        s.spawn(task.into_future(), Some(TaskOpts::STICKY))
    })
}
