#![allow(unsafe_op_in_unsafe_fn, unused)]

use crate::runtime::{AddMode, Schedule, TaskMetadata, TaskOpts};
use crate::task::Header;
use crate::task::layout::TaskLayout;
use crate::task::node::TaskNode;
use crate::task::state::{Snapshot, State, TransitionToNotifiedByRef, TransitionToNotifiedByVal};
use crate::task::trailer::Trailer;
use std::future::Future;
use std::ptr::NonNull;
use std::sync::Arc;
use std::task::Waker;

/// Raw task handle
#[derive(Debug, Clone, Copy)]
pub(crate) struct RawTask {
    ptr: NonNull<Header>,
}

impl RawTask {
    pub(super) fn new<T, S>(
        task: T,
        opts: Option<TaskOpts>,
        metadata: Option<TaskMetadata>,
        scheduler: S,
    ) -> RawTask
    where
        T: Future + 'static,
        S: Schedule,
    {
        let ptr = Box::into_raw(TaskLayout::<_, S>::new(
            task,
            opts,
            metadata,
            scheduler,
            State::new(),
        ));
        let ptr = unsafe { NonNull::new_unchecked(ptr.cast()) };
        RawTask { ptr }
    }

    pub(super) unsafe fn from_raw(ptr: NonNull<Header>) -> RawTask {
        RawTask { ptr }
    }

    pub(super) fn header_ptr(&self) -> NonNull<Header> {
        self.ptr
    }

    pub(super) fn trailer_ptr(&self) -> NonNull<Trailer> {
        unsafe { Header::get_trailer(self.ptr) }
    }

    /// Returns a reference to the task's header.
    pub(super) fn header(&self) -> &Header {
        unsafe { self.ptr.as_ref() }
    }

    /// Returns a reference to the task's trailer.
    pub(super) fn trailer(&self) -> &Trailer {
        unsafe { &*self.trailer_ptr().as_ptr() }
    }

    /// Returns a reference to the task's state.
    pub(super) fn state(&self) -> &State {
        &self.header().state
    }

    pub(crate) fn task_node(&self) -> Arc<TaskNode> {
        unsafe { Header::get_task_node(self.ptr) }
    }

    /// Safety: mutual exclusion is required to call this function.
    pub(super) fn poll(self) {
        let vtable = self.header().vtable;
        unsafe { (vtable.poll)(self.ptr) }
    }

    pub(super) fn schedule(self, mode: AddMode) {
        let vtable = self.header().vtable;
        unsafe { (vtable.schedule)(self.ptr, mode) }
    }

    pub(super) fn dealloc(self) {
        let vtable = self.header().vtable;
        unsafe {
            (vtable.dealloc)(self.ptr);
        }
    }

    /// Safety: `dst` must be a `*mut Poll<Result<T::Output>>` where `T`
    /// is the future stored by the task.
    pub(super) unsafe fn try_read_output(self, dst: *mut (), waker: &Waker) {
        let vtable = self.header().vtable;
        (vtable.try_read_output)(self.ptr, dst, waker);
    }

    pub(super) fn drop_join_handle_slow(self) {
        let vtable = self.header().vtable;
        unsafe { (vtable.drop_join_handle_slow)(self.ptr) }
    }

    pub(super) fn drop_abort_handle(self) {
        let vtable = self.header().vtable;
        unsafe { (vtable.drop_abort_handle)(self.ptr) }
    }

    pub(super) fn shutdown(self) {
        // TODO: call through TaskNode, make sure OnceLock enforced, single shutdown
        let vtable = self.header().vtable;
        unsafe { (vtable.shutdown)(self.ptr) }
    }
}

/// Task operations that can be implemented without being generic over the
/// scheduler or task. Only one version of these methods should exist in the
/// final binary.
impl RawTask {
    /// Increment the task's reference count.
    ///
    /// Currently, this is used only when creating an `AbortHandle`.
    pub(super) fn ref_inc(self) {
        self.header().state.ref_inc();
    }

    pub(super) fn drop_reference(self) {
        if self.state().ref_dec() {
            self.dealloc();
        }
    }

    /// This call consumes a ref-count and notifies the task. This will create a
    /// new Notified and submit it if necessary.
    ///
    /// The caller does not need to hold a ref-count besides the one that was
    /// passed to this call.
    pub(super) fn wake_by_val(&self) {
        match self.state().transition_to_notified_by_val() {
            TransitionToNotifiedByVal::Submit => {
                // The caller has given us a ref-count, and the transition has
                // created a new ref-count, so we now hold two. We turn the new
                // ref-count Notified and pass it to the call to `schedule`.
                //
                // The old ref-count is retained for now to ensure that the task
                // is not dropped during the call to `schedule` if the call
                // drops the task it was given.
                self.schedule(AddMode::Lifo);

                // Now that we have completed the call to schedule, we can
                // release our ref-count.
                self.drop_reference();
            }
            TransitionToNotifiedByVal::Dealloc => {
                self.dealloc();
            }
            TransitionToNotifiedByVal::DoNothing => {}
        }
    }

    /// This call notifies the task. It will not consume any ref-counts, but the
    /// caller should hold a ref-count.  This will create a new Notified and
    /// submit it if necessary.
    pub(super) fn wake_by_ref(&self) {
        match self.state().transition_to_notified_by_ref() {
            TransitionToNotifiedByRef::Submit => {
                // The transition above incremented the ref-count for a new task
                // and the caller also holds a ref-count. The caller's ref-count
                // ensures that the task is not destroyed even if the new task
                // is dropped before `schedule` returns.
                self.schedule(AddMode::Lifo);
            }
            TransitionToNotifiedByRef::DoNothing => {}
        }
    }

    /// Remotely aborts the task.
    ///
    /// The caller should hold a ref-count, but we do not consume it.
    ///
    /// This is similar to `shutdown` except that it asks the runtime to perform
    /// the shutdown. This is necessary to avoid the shutdown happening in the
    /// wrong thread for non-Send tasks.
    pub(super) fn remote_abort(&self) {
        if self.state().transition_to_notified_and_cancel() {
            // The transition has created a new ref-count, which we turn into
            // a Notified and pass to the task.
            //
            // Since the caller holds a ref-count, the task cannot be destroyed
            // before the call to `schedule` returns even if the call drops the
            // `Notified` internally.
            self.schedule(AddMode::Cancel);
        }
    }

    /// Try to set the waker notified when the task is complete. Returns true if
    /// the task has already completed. If this call returns false, then the
    /// waker will not be notified.
    pub(super) fn try_set_join_waker(&self, waker: &Waker) -> bool {
        can_read_output(self.header(), self.trailer(), waker)
    }
}

pub(super) fn can_read_output(header: &Header, trailer: &Trailer, waker: &Waker) -> bool {
    // Load a snapshot of the current task state
    let snapshot = header.state.load();

    debug_assert!(snapshot.is_join_interested());

    if !snapshot.is_complete() {
        // If the task is not complete, try storing the provided waker in the
        // task's waker field.

        let res = if snapshot.is_join_waker_set() {
            // If JOIN_WAKER is set, then JoinHandle has previously stored a
            // waker in the waker field per step (iii) of rule 5 in task/mod.rs.

            // Optimization: if the stored waker and the provided waker wake the
            // same task, then return without touching the waker field. (Reading
            // the waker field below is safe per rule 3 in task/mod.rs.)
            if unsafe { trailer.will_wake(waker) } {
                return false;
            }

            // Otherwise swap the stored waker with the provided waker by
            // following the rule 5 in task/mod.rs.
            header
                .state
                .unset_waker()
                .and_then(|snapshot| set_join_waker(header, trailer, waker.clone(), snapshot))
        } else {
            // If JOIN_WAKER is unset, then JoinHandle has mutable access to the
            // waker field per rule 2 in task/mod.rs; therefore, skip step (i)
            // of rule 5 and try to store the provided waker in the waker field.
            set_join_waker(header, trailer, waker.clone(), snapshot)
        };

        match res {
            Ok(_) => return false,
            Err(snapshot) => {
                assert!(snapshot.is_complete());
            }
        }
    }
    true
}

pub(super) fn set_join_waker(
    header: &Header,
    trailer: &Trailer,
    waker: Waker,
    snapshot: Snapshot,
) -> Result<Snapshot, Snapshot> {
    debug_assert!(snapshot.is_join_interested());
    debug_assert!(!snapshot.is_join_waker_set());

    // Safety: Only the `JoinHandle` may set the `waker` field. When
    // `JOIN_INTEREST` is **not** set, nothing else will touch the field.
    unsafe {
        trailer.set_waker(Some(waker));
    }

    // Update the `JoinWaker` state accordingly
    let res = header.state.set_join_waker();

    // If the state could not be updated, then clear the join waker
    if res.is_err() {
        unsafe {
            trailer.set_waker(None);
        }
    }

    res
}
