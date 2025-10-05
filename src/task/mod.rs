// Public API
pub mod abort;
pub use self::abort::AbortHandle;

mod error;
pub use self::error::JoinError;

mod join;
pub use self::join::JoinHandle;

// Re-exports
pub mod id;
pub(crate) use self::id::Id;

mod harness;

mod header;
pub(crate) use self::header::Header;

mod layout;

pub mod raw;
pub(crate) use self::raw::RawTask;

mod state;

// Used in `test_utils/**` module to mock the waker and header.
#[allow(unused_imports)]
pub(crate) use self::state::State;

pub(crate) mod task;
pub(crate) use self::task::{Notified, Task};

mod trailer;

pub(crate) mod vtable;

mod waker;

/// Task result sent back.
pub(crate) type Result<T> = std::result::Result<T, JoinError>;

/// Scheduler trait
pub(crate) trait Schedule: Sync + Sized + 'static {
    fn schedule(&self, task: Notified<Self>);

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

/// This is the constructor for a new task. Three references to the task are
/// created. The first task reference is usually put into an `OwnedTasks`
/// immediately. The Notified is sent to the scheduler as an ordinary
/// notification.
fn new_task<T, S>(task: T, scheduler: S, id: Id) -> (Task<S>, Notified<S>, JoinHandle<T::Output>)
where
    S: Schedule,
    T: Future + 'static,
    T::Output: 'static,
{
    let raw = RawTask::new::<T, S>(task, scheduler, id);
    let task = Task::new(raw);
    let notified = Notified::new(Task::new(raw));
    let join = JoinHandle::new(raw);

    (task, notified, join)
}
