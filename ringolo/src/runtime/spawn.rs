use std::collections::HashSet;
use std::ops::{Deref, DerefMut};

use crate::runtime::AddMode;
use crate::task::JoinHandle;
use bitflags::bitflags;

// Future gets boxed in `task::layout::TaskLayout::new`, so don't box it
// twice like the `root_future`.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    crate::with_scheduler!(|s| { s.spawn(future, None, None) })
}

pub fn spawn_builder() -> SpawnBuilder {
    SpawnBuilder::default()
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
    pub struct TaskOpts: u16 {
        /// Task will stick to the thread onto which it is created.
        const STICKY = 1;

        /// The task parent is set to the `ROOT_NODE` instead of the task that spawned
        /// it. It allows this task to outlive it's parent. The task can still be
        /// cancelled if invoking one of the cancellation APIs from the `ROOT_NODE`.
        const BACKGROUND_TASK = 1 << 1;

        /// Recursively cancel all children of this task when it exits. Prevents
        /// any children from outliving the parent.
        const CANCEL_ALL_CHILDREN_ON_EXIT = 1 << 2;

        /// Force scheduler to add at front or back of the queue in certain cases.
        /// This is not enforced in all cases, and is scheduler specific. But in
        /// general, the scheduler will try to respect this option. This only
        /// applies when the task is initially spawned, subsequent call to
        /// schedule will use the scheduler default (most likely LIFO).
        const HINT_SPAWN_FIFO = 1 << 3;
        const HINT_SPAWN_LIFO = 1 << 4;
    }

    // == Not available publicly ==
    // # Important
    // The bitspace is *shared* between TaskOpts and TaskOptsInternal as we will
    // treat TaskOptsInternal as TaskOpts within the codebase. This is simply to
    // make sure users are unable to use these flags.
    // - `TaskOpts` => defines flags starting from right-most bit.
    // - `TaskOptsInternal` => defines flags starting from left-most bit.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
    pub(crate) struct TaskOptsInternal: u16 {
        /// Special task that is spawned on startup and stays alive until the
        /// end of the program. Used for cleanup, cancellation and other stuff.
        const MAINTENANCE_TASK = 1 << 15;
    }
}

impl From<TaskOptsInternal> for TaskOpts {
    fn from(val: TaskOptsInternal) -> Self {
        TaskOpts::from_bits_retain(val.bits())
    }
}

impl TaskOpts {
    pub(crate) fn contains_internal(&self, other: TaskOptsInternal) -> bool {
        self.contains(other.into())
    }

    pub(crate) fn initial_spawn_add_mode(&self) -> Option<AddMode> {
        self.contains(TaskOpts::HINT_SPAWN_LIFO)
            .then_some(AddMode::Lifo)
            .or_else(|| {
                self.contains(TaskOpts::HINT_SPAWN_FIFO)
                    .then_some(AddMode::Fifo)
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TaskMetadata(HashSet<&'static str>);

impl TaskMetadata {
    pub fn new() -> Self {
        Self(HashSet::new())
    }
}

impl Deref for TaskMetadata {
    type Target = HashSet<&'static str>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TaskMetadata {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<const N: usize> From<[&'static str; N]> for TaskMetadata {
    fn from(arr: [&'static str; N]) -> Self {
        Self(HashSet::from_iter(arr))
    }
}

#[derive(Debug, Default)]
pub struct SpawnBuilder {
    opts: Option<TaskOpts>,

    metadata: Option<TaskMetadata>,
}

impl SpawnBuilder {
    pub fn new() -> Self {
        Self {
            opts: None,
            metadata: None,
        }
    }

    pub fn with_opts(mut self, opts: TaskOpts) -> Self {
        self.opts = Some(opts);
        self
    }

    pub fn with_metadata(mut self, metadata: TaskMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn spawn<F>(self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        crate::with_scheduler!(|s| { s.spawn(future, self.opts, self.metadata) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_opts_internal() {
        let internal = TaskOptsInternal::MAINTENANCE_TASK;
        let public: TaskOpts = internal.into();
        assert_eq!(internal.bits(), public.bits());
        assert!(public.contains_internal(internal));

        let mut combined = TaskOpts::STICKY | TaskOpts::BACKGROUND_TASK;
        assert!(!combined.contains_internal(internal));

        combined.insert(internal.into());
        assert!(combined.contains_internal(internal));
    }

    #[test]
    fn test_task_opts_bitspace_no_overlap() {
        let all_public = TaskOpts::all();
        let all_internal = TaskOptsInternal::all().into();

        let overlap = all_public & all_internal;

        assert_eq!(
            overlap,
            TaskOpts::empty(),
            "Overlap detected between public and internal task options! Overlapping bits: {:#018b}",
            overlap.bits()
        );
    }
}
