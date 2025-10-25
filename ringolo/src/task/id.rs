use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use std::{fmt, num::NonZeroU32, num::NonZeroU64};

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

impl Id {
    pub(crate) fn is_root(&self) -> bool {
        self == &ROOT_ID
    }

    pub(crate) fn is_orphan_root(&self) -> bool {
        self == &ORPHAN_ROOT_ID
    }
}

const ROOT_ID_VAL: u64 = 1;
pub static ROOT_ID: Id = Id(NonZeroU64::new(ROOT_ID_VAL).unwrap());

const ORPHAN_ROOT_ID_VAL: u64 = 2;
pub static ORPHAN_ROOT_ID: Id = Id(NonZeroU64::new(ORPHAN_ROOT_ID_VAL).unwrap());

const TASK_ID_START_VAL: u64 = 3;

// Compile time check
const _: () = assert!(
    TASK_ID_START_VAL > ROOT_ID_VAL && TASK_ID_START_VAL > ORPHAN_ROOT_ID_VAL,
    "TASK_ID_START_VAL must be greater than ROOT_ID_VAL and ORPHAN_ROOT_ID_VAL"
);

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Id {
    pub(crate) fn next() -> Id {
        // Reserve ID == 1 for the root future.
        // Reserve ID == 2 for the orphan root.
        static COUNTER: AtomicU64 = AtomicU64::new(3);

        let id = COUNTER.fetch_add(1, Ordering::Relaxed);

        // Safety: this number is unimaginably large, even if the runtime was
        // creating 1 billion task/sec, it would take 584 years to wrap around.
        Id(NonZeroU64::new(id).expect("failed to generate unique task ID: bitspace exhausted"))
    }

    pub(crate) fn as_u64(&self) -> u64 {
        self.0.get()
    }

    /// Get a unique task tracing Id to be used with tracing library.
    pub(crate) fn as_tracing_id(&self) -> tracing::Id {
        tracing::Id::from_non_zero_u64(self.0)
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Hash, Debug)]
pub(crate) struct ThreadId(NonZeroU32);

impl ThreadId {
    pub(crate) fn next() -> ThreadId {
        static COUNTER: AtomicU32 = AtomicU32::new(1);

        let id = COUNTER.load(Ordering::Relaxed);

        // Safety: We use U32 which means we can create 4 Billion threads, which
        // should be more than enough. The reason why we don't use `std::thread::ThreadId`
        // is explained in `task::header::Header`.
        ThreadId(
            NonZeroU32::new(id).expect("failed to generate unique thread ID: bitspace exhausted"),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::*;
    use anyhow::Result;
    use std::collections::HashSet;

    #[test]
    fn test_new_task_id_unique() -> Result<()> {
        init_local_runtime_and_context(None)?;

        let n = 13;
        let scheduler = DummyScheduler::default();

        let mut all_ids = HashSet::with_capacity(n);

        for _ in 1..=n {
            let (task, notified, join) = crate::task::new_task(async { 42 }, None, None, scheduler);

            assert_eq!(task.id(), notified.id());
            assert_eq!(task.id(), join.id());

            all_ids.insert(task.id());
        }

        assert_eq!(all_ids.len(), n);
        Ok(())
    }
}
