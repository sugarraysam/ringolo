#![allow(unsafe_op_in_unsafe_fn, unused)]

use crate::context;
use crate::runtime::TaskOpts;
use crate::task::ThreadId;
use crate::task::layout::Vtable;
use crate::task::node::TaskNode;
use crate::task::state::State;
use crate::task::trailer::Trailer;
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

/// Headers are accessed all the time and represent thin-pointers to Task (i.e.: future).
/// We can only store "hot data" in this struct, and we must only add values that are accessed
/// by the current thread to prevent false sharing. We should also optimize the layout for maximum
/// CPU cache friendliness, which a CPU cache line is 64 to 128 bytes.
///
/// The structure now stands at 30 bytes:
/// - 8 bytes  :: state (atomic_usize)
/// - 8 bytes  :: vtable (ptr)
/// - 4 bytes  :: owner_id (u32)
/// - 4 bytes  :: pending_io (u32)
/// - 4 bytes  :: owned_resources (u32)
/// - 2 bytes  :: task_opts (u16)
#[repr(C, align(32))]
pub(crate) struct Header {
    /// Task state (Lifecycle, Running, Complete, etc.).
    pub(super) state: State,

    /// Table of function pointers for executing actions on the task.
    pub(super) vtable: &'static Vtable,

    /// The ID of the worker thread that currently "owns" this task.
    /// Completions for this task must be processed by this thread.
    //
    // Atomic required to support concurrent access on shutdown and cancellation.
    pub(super) owner_id: AtomicU32,

    /// We keep track of all locally scheduled IOs on io_uring as a signal to
    /// determine if a task can safely be stolen by another thread. This is
    /// because completion will arrive on the thread that registered the
    /// submissions.
    pub(super) pending_ios: Cell<u32>,

    /// We keep track of all locally owned resources (direct descriptor, provided
    /// buffers, registered buffers) for this task. These resources have no meaning
    /// on other threads and we use this counter to determine when a task can
    /// safely be stolen by another worker on a separate thread.
    pub(super) owned_resources: Cell<u32>,

    /// Special task options to modify scheduling behaviour.
    pub(super) opts: TaskOpts,
}

unsafe impl Send for Header {}
unsafe impl Sync for Header {}

impl Header {
    pub(crate) fn new(state: State, vtable: &'static Vtable, opts: Option<TaskOpts>) -> Header {
        let tid = context::with_core(|core| core.thread_id).as_u32();
        Header {
            state,
            vtable,
            owner_id: AtomicU32::new(tid),
            pending_ios: Cell::new(0),
            owned_resources: Cell::new(0),
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

    pub(crate) unsafe fn set_owner_id(me: NonNull<Header>, owner_id: ThreadId) {
        me.as_ref()
            .owner_id
            .store(owner_id.as_u32(), Ordering::Release);
    }

    /// Increment pending ios resources.
    pub(crate) unsafe fn increment_pending_ios(me: NonNull<Header>) {
        me.as_ref().pending_ios.update(|x| x + 1);
    }

    /// Decrement pending ios resources.
    pub(crate) unsafe fn decrement_pending_ios(me: NonNull<Header>) {
        me.as_ref()
            .pending_ios
            .update(|x| x.checked_add_signed(-1).expect("underflow"));
    }

    /// Increment owned resources.
    pub(crate) unsafe fn increment_owned_resources(me: NonNull<Header>) {
        me.as_ref().owned_resources.update(|x| x + 1);
    }

    /// Decrement owned resources.
    pub(crate) unsafe fn decrement_owned_resources(me: NonNull<Header>) {
        me.as_ref()
            .owned_resources
            .update(|x| x.checked_add_signed(-1).expect("underflow"));
    }
}

// Non pointer read-only self methods.
impl Header {
    /// Determines if the task is safe to be stolen by another worker thread.
    ///
    /// A task is considered stealable only if it has no pending `io_uring`
    /// operations associated with it. This is a critical safety check because
    /// I/O completions are delivered to the ring of the original submitting thread.
    /// Stealing a task with pending I/O would cause its waker to be invoked on
    /// the wrong worker, leading to undefined behavior.
    ///
    /// Also, a task is not stealable if it owns `io_uring` resources that have no
    /// meaning outside of the current thread (e.g.: direct file descriptor,
    /// provided buffer, registered buffers). Trying to use such a resource on
    /// another thread would also lead to errors.
    pub(crate) fn is_stealable(&self) -> bool {
        !(self.is_sticky()
            || self.is_maintenance_task()
            || self.has_pending_ios()
            || self.owns_resources())
    }

    pub(crate) fn is_sticky(&self) -> bool {
        self.opts.is_sticky()
    }

    pub(crate) fn is_maintenance_task(&self) -> bool {
        self.opts.is_maintenance_task()
    }

    pub(crate) fn has_pending_ios(&self) -> bool {
        self.pending_ios.get() > 0
    }

    pub(crate) fn owns_resources(&self) -> bool {
        self.owned_resources.get() > 0
    }

    /// Get owned resources on local thread.
    pub(crate) fn get_owned_resources(&self) -> u32 {
        self.owned_resources.get()
    }

    /// Gets the options set on this Task.
    pub(crate) fn get_opts(&self) -> TaskOpts {
        self.opts
    }

    /// Get pending ios on local thread.
    pub(crate) fn get_pending_ios(&self) -> u32 {
        self.pending_ios.get()
    }

    /// Gets the owner_id of the task containing this `Header`.
    pub(crate) fn get_owner_id(&self) -> ThreadId {
        self.owner_id.load(Ordering::Acquire).into()
    }
}
