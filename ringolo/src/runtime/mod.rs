use crate::task::{Notified, Task};
use anyhow::Result;
use bitflags::bitflags;
use std::task::Waker;

// Public API
pub mod runtime;
pub use runtime::{Builder, Runtime};

// Exports
pub(crate) mod local;

pub(crate) use runtime::RuntimeConfig;

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
    //
    // We pass the Waker as this is how we are able to reconstruct the appropriate
    // task. The Waker data ptr carries the task information if we are not polling
    // the root future.
    fn yield_now(&self, waker: &Waker, reason: YieldReason);

    /// The task has completed work and is ready to be released. The scheduler
    /// should release it immediately and return it. The task module will batch
    /// the ref-dec with setting other options.
    ///
    /// If the scheduler has already released the task, then None is returned.
    fn release(&self, task: &Task<Self>) -> Option<Task<Self>>;

    /// Polling the task resulted in a panic. Should the runtime shutdown?
    fn unhandled_panic(&self) {
        // By default, do nothing.
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum YieldReason {
    SlabFull,
    SqRingFull,
    NoTaskBudget,
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
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(crate) struct TaskOpts: u32 {
        /// Task will stick to the thread onto which it is created.
        const STICKY = 1;
    }
}

#[derive(Debug)]
pub(crate) enum Scheduler {
    Local(local::Handle),
    Stealing(stealing::Handle),
}
