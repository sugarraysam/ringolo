use crate::context::with_context_mut;
use crate::sqe::{Completable, CompletionHandler, RawSqe, Sqe, Submittable};
use anyhow::{Result, anyhow};
use io_uring::squeue::{Entry, Flags};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Poll, Waker};

#[derive(Clone, Debug, Eq, PartialEq, Copy)]
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
//
// We use Arc<Atomic> for remaining counter as we need SqeList to be Sync + Send.
#[derive(Debug)]
pub struct SqeList {
    list: Vec<usize>,
    remaining: Arc<AtomicUsize>,

    #[allow(dead_code)]
    kind: SqeListKind,
}

impl SqeList {
    pub fn new(list: Vec<usize>, remaining: Arc<AtomicUsize>, kind: SqeListKind) -> Self {
        Self {
            list,
            remaining,
            kind,
        }
    }

    pub fn head_idx(&self) -> usize {
        self.list[0]
    }

    pub fn set_waker(&self, waker: &Waker) -> Result<()> {
        with_context_mut(|ctx| {
            ctx.slab
                .get_mut(self.list[0])
                .map(|sqe| sqe.set_waker(waker))
        })
    }

    pub fn is_ready(&self) -> bool {
        self.remaining.load(Ordering::Relaxed) == 0
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }
}

impl Submittable for SqeList {
    fn submit(&self) -> io::Result<i32> {
        with_context_mut(|ctx| ctx.push_sqes(&self.list))
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

        let res = with_context_mut(|ctx| -> Self::Output {
            self.list
                .iter()
                .map(|idx| -> Result<(Entry, io::Result<i32>)> {
                    ctx.slab.get_mut(*idx)?.take_final_result()
                })
                .collect::<Result<Vec<_>>>()
        });

        Poll::Ready(res)
    }
}

// RAII: walk the SqeList and free every RawSqe from slab.
impl Drop for SqeList {
    fn drop(&mut self) {
        with_context_mut(|ctx| {
            self.list.iter().for_each(|idx| {
                if ctx.slab.try_remove(*idx).is_none() {
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
    pub fn new() -> Self {
        Self {
            inner: SqeListBuilder::new(SqeListKind::Batch),
        }
    }

    pub fn add_entry(&mut self, entry: Entry, flags: Option<Flags>) -> &mut Self {
        self.inner.add_entry(entry, flags);
        self
    }

    pub fn try_build(self) -> Result<SqeList> {
        self.inner.try_build()
    }
}

// Contract for SqeChain is:
// - all SQEs submitted in `io_uring` as part of the same `io_uring_enter` syscall
// - SQEs execute serially, in the order they were appended, they are linked with
//   IO_LINK flag.
// - The tail of the chain is denoted by the first SQE that does not have this flag set.
// - If one of the SQE fails, all of the remaining SQEs will complete with an error.
// - The returned results respect the order of insertion in the builder.
// - We only wake up the Future after all SQEs have completed
pub struct SqeChainBuilder {
    inner: SqeListBuilder,
}

impl SqeChainBuilder {
    pub fn new() -> Self {
        Self {
            inner: SqeListBuilder::new(SqeListKind::Chain),
        }
    }

    pub fn add_entry(&mut self, entry: Entry, flags: Option<Flags>) -> &mut Self {
        self.inner.add_entry(entry, flags);
        self
    }

    pub fn try_build(self) -> Result<SqeList> {
        self.inner.try_build()
    }
}

struct SqeListBuilder {
    list: Vec<Entry>,
    kind: SqeListKind,
}

// Avoid some mem allocations
const DEFAULT_BUILDER_CAPACITY: usize = 8;

impl SqeListBuilder {
    fn new(kind: SqeListKind) -> Self {
        Self {
            list: Vec::with_capacity(DEFAULT_BUILDER_CAPACITY),
            kind,
        }
    }

    fn add_entry(&mut self, entry: Entry, flags: Option<Flags>) -> &mut Self {
        if let Some(f) = flags {
            self.list.push(entry.flags(f));
        } else {
            self.list.push(entry);
        }

        self
    }

    // Setup the SQE chain as per documentation: `$ man io_uring_enter`:
    // > The tail of the chain is denoted by the first SQE that does not
    // > have this flag set (i.e.: IOSQE_IO_LINK).
    fn into_chain(self) -> Vec<Entry> {
        let last_idx = self.list.len() - 1;

        self.list
            .into_iter()
            .enumerate()
            .map(|(idx, entry)| {
                if idx != last_idx {
                    entry.flags(Flags::IO_LINK)
                } else {
                    entry
                }
            })
            .collect()
    }

    // Build the linked list of RawSqe for either SqeChain or SqeBatch abstractions.
    // This builder maintains the order of insertion, and the contract is that we will also
    // respect this order when returning results.
    fn try_build(self) -> Result<SqeList> {
        let n_sqes = self.list.len();
        if n_sqes < 2 {
            return Err(anyhow!(
                "SqeListBuilder requires at least 2 sqes, got {}",
                n_sqes
            ));
        }

        let kind = self.kind;
        let mut entries = match kind {
            SqeListKind::Chain => self.into_chain(),
            SqeListKind::Batch => self.list,
        }
        .into_iter();

        let remaining = Arc::new(AtomicUsize::new(n_sqes));
        let mut indices = Vec::with_capacity(entries.len());

        with_context_mut(|ctx| -> Result<()> {
            let vacant = ctx.slab.vacant_entry()?;
            indices.push(vacant.key());

            let mut head = RawSqe::new(
                // SAFETY: self.list.len() >= 2.
                entries.next().unwrap(),
                CompletionHandler::new_batch_or_chain(indices[0], Arc::clone(&remaining)),
            );
            head.set_user_data(indices[0] as u64)?;
            _ = vacant.insert(head);

            for entry in entries {
                let (idx, _) = ctx.slab.insert(RawSqe::new(
                    entry,
                    CompletionHandler::new_batch_or_chain(indices[0], Arc::clone(&remaining)),
                ))?;
                indices.push(idx);
            }

            Ok(())
        })?;

        Ok(SqeList::new(indices, remaining, kind))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{init_context, with_context, with_context_mut};
    use crate::sqe::RawSqeState;
    use crate::test_utils::*;
    use either::Either;
    use rstest::rstest;
    use std::pin::pin;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll};

    #[rstest]
    #[case::batch(SqeListKind::Batch)]
    #[case::batch(SqeListKind::Chain)]
    fn test_list_builder(#[case] kind: SqeListKind) -> Result<()> {
        init_context(64);

        let n = 4;
        let list = match kind {
            SqeListKind::Batch => build_batch(n),
            SqeListKind::Chain => build_chain(n),
        }?;

        let (waker, _) = mock_waker();
        list.set_waker(&waker)?;
        let mut num_heads = 0;

        with_context(|ctx| -> Result<()> {
            for idx in list.list.iter() {
                let sqe = ctx.slab.get(*idx)?;
                assert_eq!(sqe.get_state(), RawSqeState::Pending);

                match &sqe.handler {
                    CompletionHandler::BatchOrChain {
                        head, remaining, ..
                    } => {
                        assert_eq!(remaining.load(Ordering::Relaxed), list.len());
                        if sqe.has_waker() {
                            assert_eq!(*idx, *head, "sqe with waker should be head node");
                            num_heads += 1;
                        }
                    }
                    _ => assert!(false, "unexpected completion handler"),
                }
            }

            assert_eq!(num_heads, 1, "expected one unique head node");
            Ok(())
        })
    }

    #[rstest]
    #[case::batch_5(5, SqeListKind::Batch)]
    #[case::batch_32(32, SqeListKind::Batch)]
    #[case::batch_64(64, SqeListKind::Batch)]
    #[case::chain_5(2, SqeListKind::Chain)]
    #[case::chain_32(4, SqeListKind::Chain)]
    #[case::chain_64(8, SqeListKind::Chain)]
    fn test_submit_and_complete_sqe_list(
        #[case] size: usize,
        #[case] kind: SqeListKind,
    ) -> Result<()> {
        init_context(64);

        let list = match kind {
            SqeListKind::Batch => build_batch(size),
            SqeListKind::Chain => build_chain(size),
        }?;
        let sqe_list = Sqe::new(list);

        let head_idx = sqe_list.get().head_idx();
        let expected_user_data = sqe_list.get().list.clone();

        let mut sqe_fut = pin!(sqe_list);

        let (waker, waker_data) = mock_waker();
        let mut ctx = Context::from_waker(&waker);

        // Polling once will submit the batch SQE and set the waker.
        // Polling N more times after this won't change the state.
        for _ in 0..10 {
            assert!(matches!(sqe_fut.as_mut().poll(&mut ctx), Poll::Pending));
            assert_eq!(waker_data.load(Ordering::Relaxed), 0);

            with_context_mut(|ctx| {
                assert_eq!(ctx.ring.submission().len(), size);

                // Waker only set on head in batch/chain setup
                let res = ctx.slab.get(head_idx).and_then(|sqe| {
                    assert!(!sqe.is_ready());
                    assert!(sqe.has_waker());
                    assert_eq!(sqe.get_state(), RawSqeState::Pending);
                    Ok(())
                });

                assert!(res.is_ok());
            });
        }

        with_context_mut(|ctx| -> Result<()> {
            // Submit SQEs and wait for CQEs :: `io_uring_enter`
            assert_eq!(ctx.submit_and_wait(size, None)?, size);
            assert_eq!(waker_data.load(Ordering::Relaxed), 0);

            // Process CQEs :: wakes up Waker
            assert_eq!(ctx.process_cqes(None)?, size);
            assert_eq!(waker_data.load(Ordering::Relaxed), 1);
            Ok(())
        })?;

        if let Poll::Ready(Ok(results)) = sqe_fut.as_mut().poll(&mut ctx) {
            assert_eq!(results.len(), size);

            // SqeList contract is the results order has to respect the
            // insertion order.
            for ((entry, io_result), user_data) in results.iter().zip(expected_user_data) {
                assert!(matches!(io_result, Ok(0)));
                assert_eq!(entry.get_user_data(), user_data as u64);
            }
        } else {
            assert!(false, "Expected Poll::Ready(Ok((entry, result)))");
        }

        Ok(())
    }

    #[rstest]
    #[case::batch_all_errors_complete(
        SqeListKind::Batch,
        vec![
            openat(-333, "dummy"),
            openat(-111, "invalid"),
        ],
        vec![
            Either::Right(libc::EBADF),
            Either::Right(libc::EBADF),
        ]
    )]
    #[case::batch_all_errors_complete(
        SqeListKind::Chain,
        vec![
            nop(),
            nop(),
            openat(-333, "dummy"),
            nop(),
            nop(),
        ],
        vec![
            Either::Left(0),
            Either::Left(0),
            Either::Right(libc::EBADF),
            Either::Right(libc::ECANCELED),
            Either::Right(libc::ECANCELED),
        ]
    )]
    fn test_sqe_list_edge_cases(
        #[case] kind: SqeListKind,
        #[case] entries: Vec<Entry>,
        #[case] expected_results: Vec<Either<i32, i32>>,
    ) -> Result<()> {
        assert_eq!(entries.len(), expected_results.len());

        init_context(64);

        let size = entries.len();
        let sqe_list = Sqe::new(build_list_with_entries(kind, entries)?);

        let head_idx = sqe_list.get().head_idx();
        let expected_user_data = sqe_list.get().list.clone();

        let mut sqe_fut = pin!(sqe_list);

        let (waker, waker_data) = mock_waker();
        let mut ctx = Context::from_waker(&waker);

        // Polling once will submit the batch SQE and set the waker.
        // Polling N more times after this won't change the state.
        for _ in 0..10 {
            assert!(matches!(sqe_fut.as_mut().poll(&mut ctx), Poll::Pending));
            assert_eq!(waker_data.load(Ordering::Relaxed), 0);

            with_context_mut(|ctx| {
                assert_eq!(ctx.ring.submission().len(), size);

                // Waker only set on head in batch/chain setup
                let res = ctx.slab.get(head_idx).and_then(|sqe| {
                    assert!(!sqe.is_ready());
                    assert!(sqe.has_waker());
                    Ok(())
                });

                assert!(res.is_ok());
            });
        }

        with_context_mut(|ctx| -> Result<()> {
            // Submit SQEs and wait for CQEs :: `io_uring_enter`
            assert_eq!(ctx.submit_and_wait(size, None)?, size);
            assert_eq!(waker_data.load(Ordering::Relaxed), 0);

            // Process CQEs :: wakes up Waker
            assert_eq!(ctx.process_cqes(None)?, size);
            assert_eq!(waker_data.load(Ordering::Relaxed), 1);
            Ok(())
        })?;

        if let Poll::Ready(Ok(results)) = sqe_fut.as_mut().poll(&mut ctx) {
            assert_eq!(results.len(), size);

            // SqeList contract is the results order has to respect the
            // insertion order.
            let expectations = expected_user_data.iter().zip(expected_results);

            for ((entry, io_result), (user_data, expected)) in results.iter().zip(expectations) {
                match (io_result, expected) {
                    (Err(e1), Either::Right(e2)) => assert_eq!(e1.raw_os_error().unwrap(), e2),
                    (Ok(r1), Either::Left(r2)) => assert_eq!(*r1, r2),
                    (left, right) => {
                        dbg!("left: {:?}, right: {:?}", left, right);
                        assert!(false,);
                    }
                }

                assert_eq!(entry.get_user_data(), *user_data as u64);
            }
        } else {
            assert!(false, "Expected Poll::Ready(Ok((entry, result)))");
        }

        Ok(())
    }

    #[test]
    fn test_overflow_slab() -> Result<()> {
        let slab_size = 32;
        let batch_size = slab_size * 2;

        init_context(slab_size);

        let mut builder = SqeBatchBuilder::new();
        for _ in 0..batch_size {
            builder.add_entry(nop(), None);
        }

        // In the current setup, we always overflow the Slab before we overflow
        // the ring. So we can detect this error first.
        assert!(matches!(builder.try_build(), Err(_)));
        Ok(())
    }
}
