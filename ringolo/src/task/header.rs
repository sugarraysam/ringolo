#![allow(unsafe_op_in_unsafe_fn)]

use crate::runtime::TaskOpts;
use crate::task::layout::Vtable;
use crate::task::node::TaskNode;
use crate::task::state::State;
use crate::task::trailer::Trailer;
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::Arc;
use std::thread::ThreadId;

/// Headers are accessed all the time and represent thin-pointers to Task (i.e.: future).
/// We can only store "hot data" in this struct, and we must only add values that are accessed
/// by the current thread to prevent false sharing. We should also optimize the layout for maximum
/// CPU cache friendliness, which a CPU cache line is 64 to 128 bytes.
///
/// The structure now stands at 30 bytes:
/// - 8 bytes  :: state (atomic_usize)
/// - 8 bytes  :: vtable (ptr)
/// - 8 bytes  :: thread_id (u64)
/// - 4 bytes  :: pending_io (u32)
/// - 2 byte   :: task_opts (u16)
#[repr(C, align(32))]
pub(crate) struct Header {
    /// Task state.
    pub(super) state: State,

    /// Table of function pointers for executing actions on the task.
    pub(super) vtable: &'static Vtable,

    pub(super) owner_id: Cell<ThreadId>,

    /// We keep track of all locally scheduled IOs on io_uring as a signal to
    /// determine if a task can safely be stolen by another thread. This is
    /// because completion will arrive on the thread that registered the
    /// submissions.
    pub(super) pending_ios: Cell<u32>,

    /// Special task options to modify scheduling behaviour.
    pub(super) opts: TaskOpts,
}

unsafe impl Send for Header {}
unsafe impl Sync for Header {}

impl Header {
    pub(crate) fn new(state: State, vtable: &'static Vtable, opts: Option<TaskOpts>) -> Header {
        Header {
            state,
            vtable,
            owner_id: Cell::new(std::thread::current().id()),
            pending_ios: Cell::new(0),
            opts: opts.unwrap_or_default(),
        }
    }

    /// Gets a pointer to the `Trailer` of the task containing this `Header`.
    pub(super) unsafe fn get_trailer(me: NonNull<Header>) -> NonNull<Trailer> {
        let offset = me.as_ref().vtable.trailer_offset;
        let trailer = me.as_ptr().cast::<u8>().add(offset).cast::<Trailer>();
        NonNull::new_unchecked(trailer)
    }

    /// Gets a pointer to the scheduler of the task containing this `Header`.
    ///
    /// The generic type S must be set to the correct scheduler type for this
    /// task.
    pub(super) unsafe fn get_scheduler<S>(me: NonNull<Header>) -> NonNull<S> {
        let offset = me.as_ref().vtable.scheduler_offset;
        let scheduler = me.as_ptr().cast::<u8>().add(offset).cast::<S>();
        NonNull::new_unchecked(scheduler)
    }

    pub(super) unsafe fn get_task_node(me: NonNull<Header>) -> Arc<TaskNode> {
        let offset = me.as_ref().vtable.task_node_offset;
        let task_node = me.as_ptr().cast::<u8>().add(offset).cast::<Arc<TaskNode>>();
        (*task_node).clone()
    }

    /// Gets the owner_id of the task containing this `Header`.
    pub(crate) unsafe fn get_owner_id(me: NonNull<Header>) -> ThreadId {
        me.as_ref().owner_id.get()
    }

    pub(crate) unsafe fn set_owner_id(me: NonNull<Header>, owner_id: ThreadId) {
        me.as_ref().owner_id.set(owner_id);
    }

    /// Gets the options set on this Task.
    pub(crate) unsafe fn get_opts(me: NonNull<Header>) -> TaskOpts {
        me.as_ref().opts
    }

    /// Get pending ios on local thread.
    pub(crate) unsafe fn get_pending_ios(me: NonNull<Header>) -> u32 {
        me.as_ref().pending_ios.get()
    }

    /// Modify pending ios.
    pub(crate) unsafe fn modify_pending_ios(me: NonNull<Header>, delta: i32) {
        me.as_ref()
            .pending_ios
            .update(|x| x.checked_add_signed(delta).expect("underflow"));
    }

    /// Determines if the task is safe to be stolen by another worker thread.
    ///
    /// A task is considered stealable only if it has no pending `io_uring`
    /// operations associated with it. This is a critical safety check because
    /// I/O completions are delivered to the ring of the original submitting thread.
    /// Stealing a task with pending I/O would cause its waker to be invoked on
    /// the wrong worker, leading to undefined behavior.
    pub(crate) unsafe fn is_stealable(me: NonNull<Header>) -> bool {
        let ptr = me.as_ref();

        let is_sticky = ptr.opts.contains(TaskOpts::STICKY);
        let has_pending_ios = ptr.pending_ios.get() > 0;

        !is_sticky && !has_pending_ios
    }
}
