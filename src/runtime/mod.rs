use crate::task::{Notified, Task};
use std::sync::Arc;

// Public API
pub mod runtime;
pub use runtime::{Builder, Runtime};

// Exports
pub(crate) mod local;

pub(crate) use runtime::RuntimeConfig;

pub(crate) mod stealing;

#[derive(Debug)]
pub(crate) enum Scheduler {
    Local(local::Handle),
    Stealing(stealing::Handle),
}

/// Scheduler trait
pub(crate) trait Schedule: Sync + Sized + 'static {
    fn schedule(&self, is_new: bool, task: Notified<Self>);

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

/// Abstraction of everything needed to build an event loop.
pub(crate) trait EventLoop {
    type Task;

    fn add_task(&self, task: Self::Task);

    fn find_task(&self) -> Option<Self::Task>;

    fn event_loop(&self);
}
