use std::collections::HashSet;
use std::ops::{Deref, DerefMut};

use crate::runtime::{CleanupTaskBuilder, cleanup::OpCleanupPayload};
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

pub(crate) fn spawn_cleanup<T: OpCleanupPayload>(builder: CleanupTaskBuilder<T>) -> JoinHandle<()> {
    crate::with_scheduler!(|s| {
        // Copy the runtime OnCleanupError policy into the builder.
        let task = builder.on_error(s.cfg.on_cleanup_error).build();

        s.spawn(
            task.into_future(),
            // Cleanup tasks need to be sticky to the local thread. It does not make sense
            // to cancel an io_uring operation from another thread.
            Some(TaskOpts::STICKY | TaskOpts::BACKGROUND_TASK),
            None,
        )
    })
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
