// TODO: remove this
#![allow(dead_code)]

use crate::context::{with_context, with_slab_mut};
use crate::sqe::{Completable, CompletionHandler, RawSqe, Sqe, Submittable};
use anyhow::{Context, Result, anyhow};
use io_uring::squeue::{Entry, Flags};
use std::cell::RefCell;
use std::collections::LinkedList;
use std::io;
use std::rc::Rc;
use std::task::{Poll, Waker};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqeListKind {
    Batch,

    // Chain API:
    // - The next SQE will not be started until the previous one completes.
    // - chain of requests must be submitted together as one batch
    // - If any request in a chained sequence fails, the entire chain is aborted
    // - CQEs *not guaranteed to be atomic*, will come in order, but may be interleaved
    Chain,
}

// Batch and Chain APIs resolve to this type
pub struct SqeList {
    list: LinkedList<usize>,
    remaining: Rc<RefCell<usize>>,
    kind: SqeListKind,
}

impl SqeList {
    pub fn new(list: LinkedList<usize>, remaining: Rc<RefCell<usize>>, kind: SqeListKind) -> Self {
        Self {
            list,
            remaining,
            kind,
        }
    }

    fn set_waker(&self, waker: &Waker) -> Result<()> {
        let head = self
            .list
            .front()
            .with_context(|| anyhow!("Empty SQE list"))?;

        with_slab_mut(|slab| slab.get_mut(*head).map(|sqe| sqe.set_waker(waker)))
    }

    fn is_ready(&self) -> bool {
        *self.remaining.borrow() == 0
    }
}

impl Submittable for SqeList {
    fn submit(&self) -> io::Result<i32> {
        let _ = with_context(|ctx| -> Result<()> {
            for idx in self.list.iter() {
                let _entry = ctx.get_slab().get(*idx)?.get_entry()?;

                // TODO: push in iouring
            }

            Ok(())
        });

        Ok(0)
    }
}

impl Completable for SqeList {
    type Output = Result<Vec<(Entry, io::Result<i32>)>>;

    fn poll_complete(&self, waker: &Waker) -> Poll<Self::Output> {
        if !self.is_ready() {
            // In normal operations, the Completable is only woken up when it's last RawSqe
            // completes. We still want to account for spurious wakeups or complex future
            // interactions so let's update the waker every time.
            return match self.set_waker(waker) {
                Ok(_) => Poll::Pending,
                Err(e) => Poll::Ready(Err(e)),
            };
        }

        let res = with_slab_mut(|slab| -> Self::Output {
            self.list
                .iter()
                .map(|idx| -> Result<(Entry, io::Result<i32>)> { slab.get_mut(*idx)?.get_result() })
                .collect::<Result<Vec<_>>>()
        });

        Poll::Ready(res)
    }
}

// RAII: walk the SqeList and free every RawSqe from slab.
impl Drop for SqeList {
    fn drop(&mut self) {
        with_slab_mut(|slab| {
            self.list.iter().for_each(|idx| {
                if !slab.try_remove(*idx) {
                    eprintln!("Warning: SQE {} not found in slab during drop", idx);
                }
            });
        });
    }
}

impl Into<Sqe<SqeList>> for SqeList {
    fn into(self) -> Sqe<SqeList> {
        Sqe::new(self)
    }
}

// Contract on SqeBatch is:
// - all SQEs submitted in `io_uring` as part of the same `io_uring_enter` syscall
// - SQEs can complete in any order BUT result return will respect order of insertion
// - will only wake up Future when all SQEs have completed
pub struct SqeBatchBuilder {
    inner: SqeListBuilder,
}

impl SqeBatchBuilder {
    fn new() -> Self {
        Self {
            inner: SqeListBuilder::new(SqeListKind::Batch),
        }
    }

    fn add_entry(&mut self, entry: Entry) -> &mut Self {
        self.inner.add_entry(entry, None);
        self
    }

    fn try_build(self) -> Result<Sqe<SqeList>> {
        self.inner.try_build()
    }
}

// Contract for SqeChain is:
// - all SQEs submitted in `io_uring` as part of the same `io_uring_enter` syscall
// - SQEs execute serially, in the order they were appended, they are linked with
//   IO_LINK flag.
// - If one of the SQE fails, all of the remaining SQEs will complete with an error.
// - The returned results respect the order of insertion in the builder.
// - We only wake up the Future after all SQEs have completed
pub struct SqeChainBuilder {
    inner: SqeListBuilder,
}

impl SqeChainBuilder {
    fn new() -> Self {
        Self {
            inner: SqeListBuilder::new(SqeListKind::Chain),
        }
    }

    fn add_entry(&mut self, entry: Entry) -> &mut Self {
        self.inner.add_entry(entry, Some(Flags::IO_LINK));
        self
    }

    fn try_build(self) -> Result<Sqe<SqeList>> {
        self.inner.try_build()
    }
}

struct SqeListBuilder {
    list: LinkedList<Entry>,
    kind: SqeListKind,
}

impl SqeListBuilder {
    fn new(kind: SqeListKind) -> Self {
        Self {
            list: LinkedList::new(),
            kind,
        }
    }

    fn add_entry(&mut self, entry: Entry, flags: Option<Flags>) -> &mut Self {
        if let Some(f) = flags {
            self.list.push_back(entry.flags(f));
        } else {
            self.list.push_back(entry);
        }

        self
    }

    // Build the linked list of RawSqe for either SqeChain or SqeBatch abstractions.
    // This builder maintains the order of insertion, and the contract is that we will also
    // respect this order when returning results.
    fn try_build(self) -> Result<Sqe<SqeList>> {
        let n_sqes = self.list.len();
        if n_sqes < 2 {
            return Err(anyhow!(
                "SqeListBuilder requires at least 2 sqes, got {}",
                n_sqes
            ));
        }

        let mut entries = self.list.into_iter();
        let remaining = Rc::new(RefCell::new(n_sqes));

        let (head_idx, mut indices) =
            with_slab_mut(|slab| -> Result<(usize, LinkedList<usize>)> {
                let vacant = slab.vacant_entry()?;
                let head_idx = vacant.key();

                let handler = CompletionHandler::BatchOrChain {
                    head: head_idx,
                    remaining: remaining.clone(),
                };

                _ = vacant.insert(RawSqe::new(
                    // SAFETY: self.list.len() >= 2.
                    entries.next().unwrap(),
                    handler.clone(),
                ));

                let indices = entries
                    .map(|entry| {
                        let (idx, _) = slab.insert(RawSqe::new(entry, handler.clone()))?;
                        Ok(idx)
                    })
                    .collect::<Result<LinkedList<_>>>()?;

                Ok((head_idx, indices))
            })?;

        // Don't forget to add head index at the front.
        indices.push_front(head_idx);

        Ok(SqeList::new(indices, remaining, self.kind).into())
    }
}
