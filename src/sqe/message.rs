use crate::context::with_context_mut;
use crate::sqe::{Completable, CompletionHandler, RawSqe, Sqe, Submittable};
use anyhow::Result;
use io_uring::squeue::Entry;
use std::io;
use std::task::{Poll, Waker};

// TODO: this is largely unusable as of now, need to re-implement the protocol
// we need to send multiple messages and support ACK to be truly useful.
#[derive(Debug)]
pub struct SqeRingMessage {
    // Index to RawSqe in thread-local Slab
    idx: usize,
    // id :: returned by Mailbox, will be able to retrieve answers
    // tx_ring_fd :: ring_fd of sender
    // rx_ring_fd :: ring_fd of receiver
}

impl SqeRingMessage {
    pub fn try_new(entry: Entry) -> Result<Self> {
        let idx = with_context_mut(|ctx| -> Result<usize> {
            let (idx, _) = ctx
                .slab
                .insert(RawSqe::new(entry, CompletionHandler::RingMessage))?;
            Ok(idx)
        })?;

        Ok(Self { idx })
    }
}

impl Submittable for SqeRingMessage {
    // TODO: impl ACK + mailbox
    fn submit(&self) -> io::Result<i32> {
        with_context_mut(|ctx| ctx.push_sqes(&[self.idx]))
    }
}

impl Completable for SqeRingMessage {
    type Output = Result<()>;

    // TODO: impl ACK + mailbox
    fn poll_complete(&self, _: &Waker) -> Poll<Self::Output> {
        Poll::Ready(Ok(()))
    }
}

// RAII: free RawSqe from slab.
impl Drop for SqeRingMessage {
    fn drop(&mut self) {
        with_context_mut(|ctx| {
            if ctx.slab.try_remove(self.idx).is_none() {
                eprintln!("Warning: SQE {} not found in slab during drop", self.idx);
            }
        });
    }
}

impl Into<Sqe<SqeRingMessage>> for SqeRingMessage {
    fn into(self) -> Sqe<SqeRingMessage> {
        Sqe::new(self)
    }
}
