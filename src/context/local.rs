use crate::context::RawSqeSlab;
use anyhow::Result;
use either::Either;
use io_uring::types::{SubmitArgs, Timespec};
use std::io;
use std::os::unix::io::RawFd;
use std::time::Duration;

use crate::context::ring::SingleIssuerRing;
use crate::context::{GlobalContext, ThreadId};
use crate::protocol::message::{MAX_MSG_COUNTER_VALUE, MSG_COUNTER_BITS, MsgId};

pub struct LocalContext {
    // Global thread_id
    pub thread_id: ThreadId,

    // Slab to manage RawSqe allocations for this thread.
    pub slab: RawSqeSlab,

    // IoUring to submit and complete IO operations.
    pub ring_fd: RawFd,
    pub ring: SingleIssuerRing,

    // TODO: add to mailbox
    pub ring_msg_counter: u32,
}

impl LocalContext {
    pub fn try_new(sq_ring_size: usize) -> Result<Self> {
        let ring = SingleIssuerRing::try_new(sq_ring_size as u32)?;
        let ring_fd = ring.as_raw_fd();

        let thread_id = GlobalContext::instance().register_ring_fd(ring_fd)?;

        Ok(Self {
            thread_id,
            slab: RawSqeSlab::new(sq_ring_size),
            ring_fd: ring.as_raw_fd(),
            ring,
            ring_msg_counter: 0,
        })
    }

    // Queues SQEs in the SQ ring, fetching them from the slab using the provided
    // indices. To submit the SQEs to the kernel, we need to call `q.sync()` and
    // make an `io_uring_enter` syscall unless kernel side SQ polling is enabled.
    pub fn push_sqes<'a>(
        &mut self,
        indices: impl IntoIterator<Item = &'a usize>,
    ) -> io::Result<i32> {
        // Need to manually access the fields to avoid immutable vs. mutable
        // borrow issues or double borrow problem.
        let mut sq = self.ring.get_mut().submission();

        for idx in indices {
            let entry = self
                .slab
                .get(*idx)
                .and_then(|sqe| sqe.get_entry())
                .map_err(|_e| io::Error::new(io::ErrorKind::Other, "Can't find RawSqe in slab."))?;

            unsafe { sq.push(entry) }
                .map_err(|_e| io::Error::new(io::ErrorKind::ResourceBusy, "SQ ring full."))?;
        }

        Ok(0)
    }

    pub fn submit_and_wait_timeout(
        &mut self,
        num_to_wait: usize,
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        let ring = self.ring.get_mut();

        // Sync user space and kernel shared queue
        ring.submission().sync();

        if let Some(duration) = timeout {
            let ts = Timespec::from(duration);
            let args = SubmitArgs::new().timespec(&ts);

            return ring.submitter().submit_with_args(num_to_wait, &args);
        }

        ring.submitter().submit_and_wait(num_to_wait)
    }

    pub fn has_pending_cqes(&self) -> bool {
        self.ring.has_pending_cqes()
    }

    pub fn process_cqes(&mut self, num_to_complete: Option<usize>) -> Result<usize> {
        let mut num_completed = 0;

        {
            let cq = self.ring.get_mut().completion();

            // If `num_to_complete` is not provided, we complete as many CQEs as
            // possible.
            let q_iter = if let Some(n) = num_to_complete {
                Either::Right(cq.into_iter().take(n))
            } else {
                Either::Left(cq.into_iter())
            };

            for cqe in q_iter {
                let raw_sqe = match self.slab.get_mut(cqe.user_data() as usize) {
                    Err(e) => {
                        eprintln!("CQE user data not found in RawSqeSlab: {:?}", e);
                        continue;
                    }
                    Ok(sqe) => sqe,
                };

                let head_to_wake = raw_sqe.on_completion(cqe.result())?;
                num_completed += 1;

                // We have to handle waking up the head outside the scope of RawSqe
                // to get around Rust's double borrow rule on the slab.
                if let Some(head) = head_to_wake {
                    self.slab.get_mut(head)?.wake()?;
                }
            }
        }

        // Sync with kernel queue.
        let mut cq = self.ring.get_mut().completion();
        cq.sync();

        Ok(num_completed)
    }

    // Each thread is responsible for generating MsgId to be used in the
    // RingMessage protocol. The reasoning is we want to avoid collisions when
    // storing MsgIds in the Mailbox. We achieve this goal by including the
    // unique `thread_id` in the upper 8 bits of the MsgId.
    pub fn next_ring_msg_id(&mut self) -> MsgId {
        let prev_msg_counter = self.ring_msg_counter as i32;

        // We have 10 bits for ring_msg_counter, so we need to wrap around and
        // re-use previous ids when we reach the maximum.
        self.ring_msg_counter += 1;
        if self.ring_msg_counter >= MAX_MSG_COUNTER_VALUE {
            self.ring_msg_counter = 0;
        }

        let thread_id = (self.thread_id as i32) << MSG_COUNTER_BITS;

        MsgId::from(thread_id | prev_msg_counter)
    }
}

impl Drop for LocalContext {
    fn drop(&mut self) {
        if let Err(e) = GlobalContext::instance().release_thread_id_and_ring_fd(self.thread_id) {
            eprintln!("Failed to release thread_id: {:?}", e);
        }
    }
}
