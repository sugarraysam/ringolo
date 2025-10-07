use crate::runtime::Schedule;

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

pub mod layout;

pub mod raw;
pub(crate) use self::raw::RawTask;

mod state;

// Used in `test_utils/**` module to mock the waker and header.
#[allow(unused_imports)]
pub(crate) use self::state::State;

pub(crate) mod task;
pub(crate) use self::task::{Notified, Task};

mod trailer;

mod waker;

/// Task result sent back.
pub(crate) type Result<T> = std::result::Result<T, JoinError>;

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
