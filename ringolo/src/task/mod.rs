//! # Task System
//!
//! The unit of execution in the runtime.
//!
//! ## Origin & Evolution
//!
//! This module was originally derived from the `tokio` task system. It shares the
//! same high-performance atomic state management and lock-free notification logic.
//! However, it has been significantly evolved to support the specific requirements
//! of a `io_uring`-based, thread-per-core runtime.
//!
//! ## Key Architecture Differences
//!
//! ### 1. Thread-Local I/O & Resource Accounting
//!
//! Unlike epoll-based runtimes where file descriptors can often be polled from any
//! thread, `io_uring` instances are strictly thread-local. This runtime introduces
//! fields in the task [`Header`] to track:
//!
//! * **Pending I/Os:** We track the number of in-flight operations on the ring.
//! * **Owned Resources:** We track thread-local resources (like registered buffers
//!     or direct descriptors) owned by the task.
//!
//! **Why?** This is critical for **Safe Work Stealing**. A task cannot be stolen
//! by another worker thread if it has pending completion events on the current thread's
//! ring, or if it holds resources tied to the current thread's driver. The scheduler
//! uses these counters to enforce this invariant.
//!
//! ### 2. Structured Concurrency & The Global Task Tree
//!
//! This runtime implements full Structured Concurrency. Unlike a flat list of tasks,
//! every task exists within a global hierarchy tracked by [`TaskNode`].
//!
//! * **Parent/Child Relationships:** Every spawned task (unless explicitly detached
//!     via [`TaskOpts`]) is a child of the task that spawned it.
//! * **Cascading Cancellation:** When a parent task is cancelled or aborts, the
//!     runtime can walk the [`TaskNode`] tree and cancel all descendants efficiently.
//! * **Orphan Policy:** The system tracks "orphans" (tasks whose parents have died)
//!     to ensure resources are eventually reclaimed or policies are enforced. The
//!     default policy is [`OrphanPolicy::Enforced`] which prohibits the creation
//!     of orphaned tasks by cancelling all children when a task finishes.
//!
//! [`Header`]: crate::task::header::Header
//! [`TaskNode`]: crate::task::node::TaskNode
//! [`TaskOpts`]: crate::TaskOpts
//! [`OrphanPolicy::Enforced`]: crate::runtime::OrphanPolicy

use crate::{
    runtime::{Schedule, TaskOpts},
    spawn::TaskMetadata,
};

// Public API
mod abort;
pub use self::abort::AbortHandle;

mod error;
pub use self::error::JoinError;

mod join;
pub use self::join::JoinHandle;

// Re-exports
pub(crate) mod id;
pub(crate) use self::id::{Id, ThreadId};

mod harness;

mod header;
pub(crate) use self::header::Header;

pub(crate) mod layout;

pub(crate) mod node;
pub(crate) use node::{CancellationStats, TaskNode, TaskNodeGuard};

pub(crate) mod raw;
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
pub(crate) fn new_task<T, S>(
    task: T,
    opts: Option<TaskOpts>,
    metadata: Option<TaskMetadata>,
    scheduler: S,
) -> (Task<S>, Notified<S>, JoinHandle<T::Output>)
where
    S: Schedule,
    T: Future + 'static,
    T::Output: 'static,
{
    let raw = RawTask::new::<T, S>(task, opts, metadata, scheduler);
    let task = Task::new(raw);
    let notified = Notified::new(Task::new(raw));
    let join = JoinHandle::new(raw);

    (task, notified, join)
}
