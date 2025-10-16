use crate::context::{current_task_id, set_current_task_id};
use std::sync::atomic::{AtomicU64, Ordering};

use std::{fmt, num::NonZeroU64};

/// An opaque ID that uniquely identifies a task relative to all other currently
/// running tasks.
///
/// A task's ID may be re-used for another task only once *both* of the
/// following happen:
/// 1. The task itself exits.
/// 2. There is no active [`JoinHandle`] associated with this task.
///
/// # Notes
///
/// - Task IDs are *not* sequential, and do not indicate the order in which
///   tasks are spawned.
/// - The task ID of the currently running task can be obtained from inside the
///   task via the [`task::try_id()`](crate::task::try_id()) and
///   [`task::id()`](crate::task::id()) functions and from outside the task via
///   the [`JoinHandle::id()`](crate::task::JoinHandle::id()) function.
///
/// [`JoinHandle`]: crate::task::JoinHandle
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct Id(pub(crate) NonZeroU64);

/// Returns the [`Id`] of the currently running task.
///
/// # Panics
///
/// This function panics if called from outside a task. Please note that calls
/// to `block_on` do not have task IDs, so the method will panic if called from
/// within a call to `block_on`. For a version of this function that doesn't
/// panic, see [`task::try_id()`](crate::runtime::task::try_id()).
///
/// [task ID]: crate::task::Id
pub fn id() -> Id {
    current_task_id().expect("Can't get a task id when not inside a task")
}

/// Returns the [`Id`] of the currently running task, or `None` if called outside
/// of a task.
///
/// This function is similar to  [`task::id()`](crate::runtime::task::id()), except
/// that it returns `None` rather than panicking if called outside of a task
/// context.
///
/// [task ID]: crate::task::Id
pub fn try_id() -> Option<Id> {
    current_task_id()
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Id {
    pub(crate) fn next() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);

        // Safety: this number is unimaginably large, even if the runtime was
        // creating 1 billion task/sec, it would take 584 years to wrap around.
        loop {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            if let Some(id) = NonZeroU64::new(id) {
                return Self(id);
            }
        }
    }

    pub(crate) fn as_u64(&self) -> u64 {
        self.0.get()
    }

    /// Get a unique task tracing Id to be used with tracing library.
    pub(crate) fn as_tracing_id(&self) -> tracing::Id {
        tracing::Id::from_non_zero_u64(self.0)
    }
}

/// Set and clear the task id in the context when the future is executed or
/// dropped, or when the output produced by the future is dropped.
pub(super) struct TaskIdGuard {
    parent_task_id: Option<Id>,
}

impl TaskIdGuard {
    pub(super) fn enter(id: Id) -> Self {
        TaskIdGuard {
            parent_task_id: set_current_task_id(Some(id)),
        }
    }
}

impl Drop for TaskIdGuard {
    fn drop(&mut self) {
        set_current_task_id(self.parent_task_id);
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::*;
    use std::collections::HashSet;

    #[test]
    fn test_new_task_id_unique() {
        let n = 13;
        let scheduler = DummyScheduler::default();

        let mut all_ids = HashSet::with_capacity(n);

        for _ in 1..=n {
            let (task, notified, join) = crate::task::new_task(async { 42 }, None, scheduler);

            assert_eq!(task.id(), notified.id());
            assert_eq!(task.id(), join.id());

            all_ids.insert(task.id());
        }

        assert_eq!(all_ids.len(), n);
    }
}
