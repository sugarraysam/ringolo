#![allow(unsafe_op_in_unsafe_fn)]

use crate::task::id::Id;
use crate::task::state::State;
use crate::task::trailer::Trailer;
use crate::task::vtable::Vtable;
use std::cell::Cell;
use std::ptr::NonNull;

#[repr(C)]
pub(crate) struct Header {
    /// Task state.
    pub(super) state: State,

    /// Table of function pointers for executing actions on the task.
    pub(super) vtable: &'static Vtable,

    /// We keep track of all locally scheduled IOs on iouring as a signal to
    /// determine if a task can safely be stolen by another thread. This is
    /// because completion will arrive on the thread that registered the
    /// submissions.
    pub(super) pending_io: Cell<i32>,
}

unsafe impl Send for Header {}
unsafe impl Sync for Header {}

impl Header {
    pub(crate) fn new(state: State, vtable: &'static Vtable) -> Header {
        Header {
            state,
            vtable,
            pending_io: Cell::new(0),
        }
    }

    /// Gets a pointer to the `Trailer` of the task containing this `Header`.
    ///
    /// # Safety
    ///
    /// The provided raw pointer must point at the header of a task.
    pub(super) unsafe fn get_trailer(me: NonNull<Header>) -> NonNull<Trailer> {
        let offset = me.as_ref().vtable.trailer_offset;
        let trailer = me.as_ptr().cast::<u8>().add(offset).cast::<Trailer>();
        NonNull::new_unchecked(trailer)
    }

    /// Gets a pointer to the scheduler of the task containing this `Header`.
    ///
    /// # Safety
    ///
    /// The provided raw pointer must point at the header of a task.
    ///
    /// The generic type S must be set to the correct scheduler type for this
    /// task.
    pub(super) unsafe fn get_scheduler<S>(me: NonNull<Header>) -> NonNull<S> {
        let offset = me.as_ref().vtable.scheduler_offset;
        let scheduler = me.as_ptr().cast::<u8>().add(offset).cast::<S>();
        NonNull::new_unchecked(scheduler)
    }

    /// Gets a pointer to the id of the task containing this `Header`.
    ///
    /// # Safety
    ///
    /// The provided raw pointer must point at the header of a task.
    pub(super) unsafe fn get_id_ptr(me: NonNull<Header>) -> NonNull<Id> {
        let offset = me.as_ref().vtable.id_offset;
        let id = me.as_ptr().cast::<u8>().add(offset).cast::<Id>();
        NonNull::new_unchecked(id)
    }

    /// Gets the id of the task containing this `Header`.
    ///
    /// # Safety
    ///
    /// The provided raw pointer must point at the header of a task.
    pub(super) unsafe fn get_id(me: NonNull<Header>) -> Id {
        let ptr = Header::get_id_ptr(me).as_ptr();
        *ptr
    }

    /// Increment pending io on local thread.
    pub(crate) unsafe fn get_pending_io(me: NonNull<Header>) -> i32 {
        me.as_ref().pending_io.get()
    }

    /// Increment pending io on local thread.
    pub(crate) unsafe fn increment_pending_io(me: NonNull<Header>) {
        me.as_ref().pending_io.update(|x| x + 1);
    }

    /// Decrement pending io on local thread.
    pub(crate) unsafe fn decrement_pending_io(me: NonNull<Header>) {
        me.as_ref().pending_io.update(|x| x - 1);
    }

    /// Determines if the task is safe to be stolen by another worker thread.
    ///
    /// A task is considered stealable only if it has no pending `io_uring`
    /// operations associated with it. This is a critical safety check because
    /// I/O completions are delivered to the ring of the original submitting thread.
    /// Stealing a task with pending I/O would cause its waker to be invoked on
    /// the wrong worker, leading to undefined behavior.
    pub(crate) unsafe fn is_stealable(me: NonNull<Header>) -> bool {
        me.as_ref().pending_io.get() == 0
    }

    // TODO: impl Id methods
}
