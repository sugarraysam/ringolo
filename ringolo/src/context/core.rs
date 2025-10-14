#![allow(unsafe_op_in_unsafe_fn)]

use crate::context::RawSqeSlab;
use crate::runtime::RuntimeConfig;
use crate::sqe::{CompletionEffect, IoError, RawSqeState};
use crate::task::Id;
use crate::utils::ScopeGuard;
use anyhow::Result;
use io_uring::squeue::Entry;
use std::cell::{Cell, RefCell};
use std::io;
use std::num::NonZeroU64;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::Duration;

use crate::context::ring::SingleIssuerRing;

/// Core Thread-local context required by all flavors of schedulers. Requires
/// interior mutability on ALL fields for best interface. Will allow borrowing
/// multiple fields as mut in the same context. Otherwise we will run in borrow
/// checker issues.
pub(crate) struct Core {
    pub(crate) thread_id: ThreadId,

    /// Slab to manage RawSqe allocations for this thread.
    pub(crate) slab: RefCell<RawSqeSlab>,

    /// IoUring to submit and complete IO operations.
    pub(crate) ring_fd: RawFd,
    pub(crate) ring: RefCell<SingleIssuerRing>,

    /// Task that is currently executing on this thread.
    pub(crate) current_task_id: Cell<Option<Id>>,

    /// Set to true if we are currently polling root future.
    pub(crate) polling_root_future: Cell<bool>,
}

impl Core {
    pub(crate) fn try_new(cfg: &RuntimeConfig) -> Result<Self> {
        let ring = SingleIssuerRing::try_new(cfg.sq_ring_size as u32)?;

        Ok(Self {
            thread_id: ThreadId::next(),
            slab: RefCell::new(RawSqeSlab::new(
                cfg.sq_ring_size * cfg.cq_ring_size_multiplier,
            )),
            ring_fd: ring.as_raw_fd(),
            ring: RefCell::new(ring),
            current_task_id: Cell::new(None),
            polling_root_future: Cell::new(false),
        })
    }

    pub(crate) fn is_polling_root(&self) -> bool {
        self.polling_root_future.get()
    }
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub(crate) struct ThreadId(pub(crate) NonZeroU64);

impl ThreadId {
    pub(crate) fn next() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);

        // Safety: this number is unimaginably large, even if the runtime was
        // creating 1 billion task/sec, it would take 584 years to wrap around.
        loop {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            if let Some(id) = NonZeroU64::new(id) {
                return Self(id);
            }
        }
    }

    pub(crate) fn as_u64(&self) -> u64 {
        self.0.get()
    }
}
