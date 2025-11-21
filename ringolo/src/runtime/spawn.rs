//! Provides functions and types for spawning new tasks onto the runtime.
//!
//! Tasks can be spawned using the simple [`spawn()`] function for default
//! behavior, or configured using the [`SpawnBuilder`] for more control.
//!
//! The [`SpawnBuilder`] allows you to set [`TaskOpts`] (like stickiness or
//! structured concurrency behavior) and [`TaskMetadata`] (for cancellation).
//!
//! [`SpawnBuilder`]: crate::runtime::SpawnBuilder
//! [`TaskOpts`]: crate::runtime::TaskOpts
//! [`TaskMetadata`]: crate::runtime::TaskMetadata
use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::sync::LazyLock;

use crate::runtime::AddMode;
use crate::task::JoinHandle;
use bitflags::bitflags;

/// Spawns a new asynchronous task with default options.
///
/// This is a convenience function for [spawn_builder()].
///
/// By default, the spawned task is attached to the current task in the task tree
/// and will be cancelled if its parent exits, respecting the runtime's
/// [OrphanPolicy].
///
/// [OrphanPolicy]: crate::runtime::OrphanPolicy
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Future gets boxed in `task::layout::TaskLayout::new`, so don't box it
    // twice like the `root_future`.
    crate::with_scheduler!(|s| { s.spawn(future, None, None) })
}

/// Creates a new [SpawnBuilder] for configuring and spawning a task.
///
/// This is the entry point for setting custom [TaskOpts] or [TaskMetadata].
///
/// # Example
///
/// ```no_run
/// use ringolo::{TaskMetadata, TaskOpts};
///
/// ringolo::spawn_builder()
///     .with_opts(TaskOpts::STICKY | TaskOpts::BACKGROUND_TASK)
///     .with_metadata(TaskMetadata::from(["my-task"]))
///     .spawn(async {
///         // ...
///     });
/// ```
pub fn spawn_builder() -> SpawnBuilder {
    SpawnBuilder::default()
}

pub(crate) static MAINTENANCE_TASK_OPTS: LazyLock<TaskOpts> = LazyLock::new(|| {
    TaskOpts::STICKY | TaskOpts::BACKGROUND_TASK | TaskOptsInternal::MAINTENANCE_TASK.into()
});

bitflags! {
    /// Configuration options for a new task.
    ///
    /// Passed to the runtime via [SpawnBuilder::with_opts].
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
    pub struct TaskOpts: u16 {
        /// Task will stick to the thread onto which it is created.
        ///
        /// This prevents the work-stealing scheduler from migrating it.
        /// Useful for tasks managing thread-local `io_uring` resources.
        const STICKY = 1;

        /// The task's parent is set to the `ROOT_NODE` instead of the task that
        /// spawned it. This creates a "detached" task.
        ///
        /// This allows the task to outlive its parent, bypassing the default
        /// structured concurrency behavior.
        ///
        /// The task can still be cancelled if invoking one of the cancellation
        /// APIs from the `ROOT_NODE`
        const BACKGROUND_TASK = 1 << 1;

        /// Recursively cancel all children of this task when it exits.
        ///
        /// This enforces structured concurrency for this specific task and its subtree,
        /// ensuring no children are orphaned when this task completes.
        ///
        /// This is primarily useful when the runtime is configured with
        /// [`OrphanPolicy::Permissive`], as it allows you to enforce
        /// structured concurrency on a per-task basis.
        ///
        /// If the runtime is already using [`OrphanPolicy::Enforced`] (the default),
        /// this flag is redundant as this behavior is already guaranteed.
        ///
        /// [OrphanPolicy::Permissive]: crate::runtime::OrphanPolicy::Permissive
        /// [OrphanPolicy::Enforced]: crate::runtime::OrphanPolicy::Enforced
        const CANCEL_ALL_CHILDREN_ON_EXIT = 1 << 2;

        // --- Scheduler Hints ---
        //
        /// These flags provide a hint to the scheduler for the *initial*
        /// placement of the task (LIFO or FIFO). This is not enforced in all
        /// cases and is scheduler-specific. Subsequent polls and
        /// re-schedules will use the scheduler's default logic.

        /// **Hint** to spawn this task in FIFO (back of queue) order.
        const HINT_SPAWN_FIFO = 1 << 3;

        /// **Hint** to spawn this task in LIFO (front of queue) order.
        const HINT_SPAWN_LIFO = 1 << 4;

        // TODO: impl
        // Use this option to instruct the scheduler that this task is blocking,
        // and will not yield or await futures.
        // const BLOCKING = 1 << 5;
    }

    // == Not available publicly ==
    //
    // # Important
    //
    // The bitspace is *shared* between TaskOpts and TaskOptsInternal as we will
    // treat TaskOptsInternal as TaskOpts within the codebase. This is simply to
    // make sure users are unable to use these flags.
    // - `TaskOpts` => defines flags starting from right-most bit.
    // - `TaskOptsInternal` => defines flags starting from left-most bit.
    #[doc(hidden)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
    struct TaskOptsInternal: u16 {
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
    pub(crate) fn is_sticky(&self) -> bool {
        self.contains(TaskOpts::STICKY)
    }

    pub(crate) fn cancel_all_on_exit(&self) -> bool {
        self.contains(TaskOpts::CANCEL_ALL_CHILDREN_ON_EXIT)
    }

    pub(crate) fn is_background_task(&self) -> bool {
        self.contains(TaskOpts::BACKGROUND_TASK)
    }

    pub(crate) fn is_maintenance_task(&self) -> bool {
        self.contains_internal(TaskOptsInternal::MAINTENANCE_TASK)
    }

    pub(crate) fn initial_spawn_add_mode(&self) -> Option<AddMode> {
        self.contains(TaskOpts::HINT_SPAWN_LIFO)
            .then_some(AddMode::Lifo)
            .or_else(|| {
                self.contains(TaskOpts::HINT_SPAWN_FIFO)
                    .then_some(AddMode::Fifo)
            })
    }

    fn contains_internal(&self, other: TaskOptsInternal) -> bool {
        self.contains(other.into())
    }
}

/// A set of string tags used to identify a task.
///
/// This metadata is used by the [cancellation APIs] to find and
/// cancel groups of tasks.
///
/// It is a simple wrapper around a `HashSet<&'static str>`, so it can be
/// created easily from a string array.
///
/// [cancellation APIs]: crate::runtime::cancel
///
/// # Example
///
/// ```
/// use ringolo::TaskMetadata;
/// let metadata = TaskMetadata::from(["group:db", "op:read"]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TaskMetadata(HashSet<&'static str>);

#[doc(hidden)]
impl Deref for TaskMetadata {
    type Target = HashSet<&'static str>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[doc(hidden)]
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

/// A builder for configuring and spawning a new task.
///
/// Created by calling [spawn_builder()].
#[derive(Debug, Default)]
pub struct SpawnBuilder {
    opts: Option<TaskOpts>,

    metadata: Option<TaskMetadata>,
}

impl SpawnBuilder {
    /// Sets the [TaskOpts] for the new task.
    pub fn with_opts(mut self, opts: TaskOpts) -> Self {
        self.opts = Some(opts);
        self
    }

    /// Sets the [TaskMetadata] for the new task.
    pub fn with_metadata(mut self, metadata: TaskMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Spawns the task with the configured options.
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
