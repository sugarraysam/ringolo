use crate::context::{with_context_mut, with_slab_mut};
use crate::sqe::{Completable, CompletionHandler, RawSqe, Sqe, Submittable};
use anyhow::Result;
use io_uring::squeue::Entry;
use std::io;
use std::task::{Poll, Waker};

#[derive(Debug)]
pub struct SqeSingle {
    // Index to RawSqe in thread-local Slab
    idx: usize,
}

impl SqeSingle {
    pub fn try_new(entry: Entry) -> Result<Self> {
        let idx = with_slab_mut(|slab| -> Result<usize> {
            let (idx, _) = slab.insert(RawSqe::new(entry, CompletionHandler::Single))?;
            Ok(idx)
        })?;

        Ok(Self { idx })
    }
}

impl Submittable for SqeSingle {
    fn submit(&self) -> io::Result<i32> {
        with_context_mut(|ctx| ctx.submit_sqes(&[self.idx]))
    }
}

impl Completable for SqeSingle {
    type Output = Result<(Entry, io::Result<i32>)>;

    fn poll_complete(&self, waker: &Waker) -> Poll<Self::Output> {
        with_slab_mut(|slab| -> Poll<Self::Output> {
            let raw_sqe = match slab.get_mut(self.idx) {
                Ok(sqe) => sqe,
                Err(e) => return Poll::Ready(Err(e)),
            };

            if raw_sqe.is_ready() {
                Poll::Ready(raw_sqe.get_result())
            } else {
                raw_sqe.set_waker(waker);
                Poll::Pending
            }
        })
    }
}

// RAII: free RawSqe from slab.
impl Drop for SqeSingle {
    fn drop(&mut self) {
        with_slab_mut(|slab| {
            if !slab.try_remove(self.idx) {
                eprintln!("Warning: SQE {} not found in slab during drop", self.idx);
            }
        });
    }
}

impl Into<Sqe<SqeSingle>> for SqeSingle {
    fn into(self) -> Sqe<SqeSingle> {
        Sqe::new(self)
    }
}
