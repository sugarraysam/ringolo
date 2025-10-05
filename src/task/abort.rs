use crate::task::{Header, Id, RawTask};
use std::fmt;
use std::panic::{RefUnwindSafe, UnwindSafe};

/// An owned permission to abort a spawned task, without awaiting its completion.
///
/// Unlike a [`JoinHandle`], an `AbortHandle` does *not* represent the
/// permission to await the task's completion, only to terminate it.
///
/// The task may be aborted by calling the [`AbortHandle::abort`] method.
/// Dropping an `AbortHandle` releases the permission to terminate the task
/// --- it does *not* abort the task.
///
/// [`JoinHandle`]: crate::task::JoinHandle
pub struct AbortHandle {
    raw: RawTask,
}

impl AbortHandle {
    pub(super) fn new(raw: RawTask) -> Self {
        Self { raw }
    }

    /// Abort the task associated with the handle.
    ///
    /// Awaiting a cancelled task might complete as usual if the task was
    /// already completed at the time it was cancelled, but most likely it
    /// will fail with a [cancelled] `JoinError`.
    ///
    /// If the task was already cancelled, such as by [`JoinHandle::abort`],
    /// this method will do nothing.
    ///
    /// See also [the module level docs] for more information on cancellation.
    ///
    /// [cancelled]: method@super::error::JoinError::is_cancelled
    pub fn abort(&self) {
        self.raw.remote_abort();
    }

    /// Checks if the task associated with this `AbortHandle` has finished.
    ///
    /// Please note that this method can return `false` even if `abort` has been
    /// called on the task. This is because the cancellation process may take
    /// some time, and this method does not return `true` until it has
    /// completed.
    pub fn is_finished(&self) -> bool {
        let state = self.raw.state().load();
        state.is_complete()
    }

    /// Returns a [task ID] that uniquely identifies this task relative to other
    /// currently spawned tasks.
    ///
    /// [task ID]: crate::task::Id
    pub fn id(&self) -> Id {
        // Safety: The header pointer is valid.
        unsafe { Header::get_id(self.raw.header_ptr()) }
    }
}

unsafe impl Send for AbortHandle {}
unsafe impl Sync for AbortHandle {}

impl UnwindSafe for AbortHandle {}
impl RefUnwindSafe for AbortHandle {}

impl fmt::Debug for AbortHandle {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Safety: The header pointer is valid.
        fmt.debug_struct("AbortHandle")
            .field("id", &self.id())
            .finish()
    }
}

impl Drop for AbortHandle {
    fn drop(&mut self) {
        self.raw.drop_abort_handle();
    }
}

impl Clone for AbortHandle {
    /// Returns a cloned `AbortHandle` that can be used to remotely abort this task.
    fn clone(&self) -> Self {
        self.raw.ref_inc();
        Self::new(self.raw)
    }
}
