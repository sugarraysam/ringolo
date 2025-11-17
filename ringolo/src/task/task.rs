#![allow(unsafe_op_in_unsafe_fn, unused)]

use crate::runtime::Schedule;
use crate::spawn::TaskOpts;
use crate::task::ThreadId;
use crate::task::state::TransitionToNotifiedByRef;
use crate::task::{Header, Id, RawTask};
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ptr::NonNull;
use std::task::Waker;

/// An owned handle to the task, tracked by ref count.
#[repr(transparent)]
pub(crate) struct Task<S: 'static> {
    raw: RawTask,
    _p: PhantomData<S>,
}

unsafe impl<S> Send for Task<S> {}
unsafe impl<S> Sync for Task<S> {}

impl<S: 'static> Task<S> {
    pub(crate) fn new(raw: RawTask) -> Task<S> {
        Task {
            raw,
            _p: PhantomData,
        }
    }

    pub(super) unsafe fn from_raw(ptr: NonNull<Header>) -> Task<S> {
        Task::new(RawTask::from_raw(ptr))
    }

    pub(crate) fn as_raw(&self) -> RawTask {
        self.raw
    }

    fn header(&self) -> &Header {
        self.raw.header()
    }

    fn header_ptr(&self) -> NonNull<Header> {
        self.raw.header_ptr()
    }

    /// Returns a [task ID] that uniquely identifies this task relative to other
    /// currently spawned tasks.
    ///
    /// [task ID]: crate::task::Id
    pub(crate) fn id(&self) -> Id {
        self.raw.task_node().id
    }

    /// Returns true if the task can safely be stolen by another thread. This is
    /// true if there are no pending IO operations on the current thread's
    /// iouring.
    ///
    // We keep track of inflight IO operations through the waker, and
    // the Submittable interface.
    pub(crate) fn is_stealable(&self) -> bool {
        self.header().is_stealable()
    }

    pub(crate) fn set_owner_id(&self, owner_id: ThreadId) {
        unsafe {
            Header::set_owner_id(self.raw.header_ptr(), owner_id);
        }
    }

    pub(crate) fn get_owner_id(&self) -> ThreadId {
        self.header().get_owner_id()
    }

    pub(crate) fn get_opts(&self) -> TaskOpts {
        self.header().get_opts()
    }

    pub(crate) fn is_maintenance_task(&self) -> bool {
        self.header().is_maintenance_task()
    }

    pub(crate) fn is_sticky(&self) -> bool {
        self.header().is_sticky()
    }
}

impl<S: Schedule> Task<S> {
    /// Preemptively cancels the task as part of the shutdown process.
    pub(crate) fn shutdown(self) {
        let raw = self.raw;
        mem::forget(self);
        raw.shutdown();
    }
}

impl<S: 'static> Drop for Task<S> {
    fn drop(&mut self) {
        // Decrement the ref count
        if self.header().state.ref_dec() {
            // Deallocate if this is the final ref count
            self.raw.dealloc();
        }
    }
}

impl<S> fmt::Debug for Task<S> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "Task({:p})", self.header())
    }
}

/// A task was notified.
#[repr(transparent)]
pub(crate) struct Notified<S: 'static>(Task<S>);

// safety: This type cannot be used to touch the task without first verifying
// that the value is on a thread where it is safe to poll the task.
unsafe impl<S: Schedule> Send for Notified<S> {}
unsafe impl<S: Schedule> Sync for Notified<S> {}

impl<S: 'static> Notified<S> {
    pub(super) fn new(task: Task<S>) -> Notified<S> {
        Notified(task)
    }

    // The waker carries our Header ptr unless we are currently polling the root
    // future. We can reconstruct a Notified task from this waker data ptr.
    pub(crate) unsafe fn from_waker(waker: &Waker) -> Option<Notified<S>> {
        let ptr = NonNull::new_unchecked(waker.data() as *mut Header);
        let raw = RawTask::from_raw(ptr);

        match raw.state().transition_to_notified_by_ref() {
            TransitionToNotifiedByRef::Submit => Some(Notified::from_raw(raw)),
            TransitionToNotifiedByRef::DoNothing => None,
        }
    }

    pub(crate) unsafe fn from_raw(ptr: RawTask) -> Notified<S> {
        Notified(Task::new(ptr))
    }

    pub(crate) fn into_raw(self) -> RawTask {
        let raw = self.0.raw;
        mem::forget(self);
        raw
    }

    /// Runs the task.
    pub(crate) fn run(self) {
        let raw = self.into_raw();
        raw.poll();
    }

    fn header(&self) -> &Header {
        self.0.header()
    }

    pub(crate) fn id(&self) -> Id {
        self.0.id()
    }

    pub(crate) fn is_stealable(&self) -> bool {
        self.0.is_stealable()
    }

    pub(crate) fn is_maintenance_task(&self) -> bool {
        self.0.is_maintenance_task()
    }

    pub(crate) fn set_owner_id(&self, owner_id: ThreadId) {
        self.0.set_owner_id(owner_id)
    }

    pub(crate) fn get_owner_id(&self) -> ThreadId {
        self.0.get_owner_id()
    }

    pub(crate) fn get_opts(&self) -> TaskOpts {
        self.0.get_opts()
    }
}

impl<S> fmt::Debug for Notified<S> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "task::Notified({:p})", self.0.header())
    }
}
