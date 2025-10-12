#![allow(unsafe_op_in_unsafe_fn)]

use crate::context::RawSqeSlab;
use crate::runtime::RuntimeConfig;
use crate::sqe::{CompletionEffect, IoError, RawSqeState};
use crate::task::Id;
use crate::utils::ScopeGuard;
use anyhow::Result;
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

    // Must call before trying to call `push_sqes`. This is because we need to make sure our
    // batch can fit in both the Slab and the SQ ring to avoid corrupted state.
    pub(crate) fn pre_push_validation(&mut self, n_sqes: usize) -> Result<(), IoError> {
        let slab = self.slab.borrow();
        let sq = self.ring.get_mut().sq();

        if n_sqes > sq.capacity() {
            Err(IoError::SqBatchTooLarge)
        } else if n_sqes + sq.len() > sq.capacity() {
            Err(IoError::SqRingFull)
        } else if n_sqes + slab.len() > slab.capacity() {
            Err(IoError::SlabFull)
        } else {
            Ok(())
        }
    }

    // Queues SQEs in the SQ ring, fetching them from the slab using the provided
    // indices. To submit the SQEs to the kernel, we need to call `q.sync()` and
    // make an `io_uring_enter` syscall unless kernel side SQ polling is enabled.
    pub(crate) fn push_sqes<'a>(
        &mut self,
        indices: impl ExactSizeIterator<Item = &'a usize>,
    ) -> Result<i32, IoError> {
        let mut sq = self.ring.get_mut().sq();
        let slab = self.slab.borrow();

        // If we fail to add an entry at this point, we might violate SQE primitive contracts.
        // This is unrecoverable and we return invalid state errors.
        for idx in indices {
            let entry = slab
                .get(*idx)
                .and_then(|sqe| sqe.get_entry())
                .map_err(|_| IoError::SlabInvalidState)?;

            unsafe { sq.push(entry) }.map_err(|_| IoError::SqRingInvalidState)?;
        }

        Ok(0)
    }

    pub(crate) fn submit_and_wait(
        &mut self,
        num_to_wait: usize,
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        self.ring.get_mut().submit_and_wait(num_to_wait, timeout)
    }

    pub(crate) fn submit_no_wait(&mut self) -> io::Result<usize> {
        self.ring.get_mut().submit_no_wait()
    }

    // Will busy loop until `num_to_complete` has been achieved. It is the caller's
    // responsibility to make sure the CQ will see that many completions, otherwise
    // this will result in an infinite loop.
    pub(crate) fn process_cqes(&mut self, num_to_complete: Option<usize>) -> Result<usize> {
        let mut num_completed = 0;
        let mut should_sync = false;

        let to_complete = num_to_complete.unwrap_or(self.ring.get_mut().cq().len());

        let slab = self.slab.get_mut();

        while num_completed < to_complete {
            let mut cq = self.ring.get_mut().cq();

            // Avoid syncing on first pass
            if should_sync {
                cq.sync();
            }

            for cqe in cq {
                let raw_sqe = match slab.get_mut(cqe.user_data() as usize) {
                    Err(e) => {
                        eprintln!("CQE user data not found in RawSqeSlab: {:?}", e);
                        continue;
                    }
                    Ok(sqe) => {
                        // Ignore unknown CQEs which might have valid index in
                        // the Slab. Can this even happen?
                        if !matches!(sqe.get_state(), RawSqeState::Pending | RawSqeState::Ready) {
                            continue;
                        }
                        sqe
                    }
                };

                let cqe_flags = match cqe.flags() {
                    0 => None,
                    flags => Some(flags),
                };

                num_completed += 1;

                for effect in raw_sqe.on_completion(cqe.result(), cqe_flags)? {
                    match effect {
                        CompletionEffect::DecrementPendingIo => slab.pending_ios -= 1,
                        CompletionEffect::WakeHead { head } => slab.get_mut(head)?.wake()?,
                    }
                }
            }

            should_sync = true;
        }

        Ok(num_completed)
    }

    pub(crate) fn num_unsubmitted_sqes(&self) -> usize {
        self.ring.borrow_mut().sq().len()
    }

    pub(crate) fn has_ready_cqes(&self) -> bool {
        self.ring.borrow_mut().has_ready_cqes()
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
