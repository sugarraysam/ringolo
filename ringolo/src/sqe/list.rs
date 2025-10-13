use crate::context::{with_core_mut, with_slab_mut};
use crate::sqe::errors::IoError;
use crate::sqe::{Completable, CompletionHandler, RawSqe, Sqe, Submittable, increment_pending_io};
use anyhow::anyhow;
use io_uring::squeue::{Entry, Flags};
use std::io::{self, Error};
use std::mem;
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

pub(crate) enum SqeListState {
    Preparing {
        builder: SqeListBuilder,
    },
    Indexed {
        indices: Vec<usize>,
        remaining: Arc<AtomicUsize>,

        #[allow(unused)]
        kind: SqeListKind,
    },
}

// Batch and Chain APIs resolve to this type
//
// We use Arc<Atomic> for remaining counter as we need SqeList to be Sync + Send.
pub struct SqeList {
    state: SqeListState,
}

impl SqeList {
    fn new(builder: SqeListBuilder) -> Self {
        Self {
            state: SqeListState::Preparing { builder },
        }
    }

    fn with_indexed<F, R>(&self, f: F) -> io::Result<R>
    where
        F: FnOnce(&Vec<usize>, &Arc<AtomicUsize>) -> R,
    {
        match &self.state {
            SqeListState::Indexed {
                indices, remaining, ..
            } => Ok(f(indices, remaining)),
            _ => Err(Error::other("SqeList invalid state: expected Indexed")),
        }
    }

    pub fn head_idx(&self) -> io::Result<usize> {
        self.with_indexed(|indices, _| indices[0])
    }

    // We set the waker on the head of the list.
    pub fn set_waker(&self, waker: &Waker) -> io::Result<()> {
        self.with_indexed(|indices, _| {
            with_slab_mut(|slab| slab.get_mut(indices[0]).map(|sqe| sqe.set_waker(waker)))
        })?
    }

    pub fn is_ready(&self) -> io::Result<bool> {
        self.with_indexed(|_, remaining| remaining.load(Ordering::Relaxed) == 0)
    }

    pub fn len(&self) -> io::Result<usize> {
        self.with_indexed(|indices, _| indices.len())
    }
}

impl Submittable for SqeList {
    fn submit(&mut self, waker: &Waker) -> Result<i32, IoError> {
        if let SqeListState::Preparing { builder } = &self.state {
            // We run `pre_submit_validation` to make the submission *atomic*. This is
            // because we must ensure that entries are added to both the ring and the slab
            // to avoid corrupted state.
            with_core_mut(|core| core.pre_push_validation(builder.list.len()))?;

            // Clone entries so we can safely retry.
            let builder = builder.clone();

            _ = mem::replace(&mut self.state, builder.try_build()?);

            increment_pending_io(waker);
        }

        self.with_indexed(|indices, _| with_core_mut(|ctx| ctx.push_sqes(indices.iter())))?
    }
}

impl Completable for SqeList {
    type Output = Result<Vec<io::Result<i32>>, IoError>;

    fn poll_complete(&self, waker: &Waker) -> Poll<Self::Output> {
        if !self.is_ready()? {
            // In normal operations, the Completable is only woken up when it's last RawSqe
            // completes. We still want to account for spurious wakeups or complex future
            // interactions so let's update the waker every time.
            return match self.set_waker(waker) {
                Ok(_) => Poll::Pending,
                Err(e) => Poll::Ready(Err(e.into())),
            };
        }

        self.with_indexed(|indices, _| {
            let res = with_slab_mut(|slab| -> Self::Output {
                indices
                    .iter()
                    .map(|idx| -> Result<io::Result<i32>, IoError> {
                        let sqe = slab.get_mut(*idx).map_err(IoError::from)?;

                        Ok(sqe.take_final_result())
                    })
                    .collect::<Result<Vec<_>, IoError>>()
            });

            Poll::Ready(res)
        })?
    }
}

// RAII: walk the SqeList and free every RawSqe from slab.
impl Drop for SqeList {
    fn drop(&mut self) {
        if let Err(e) = self.with_indexed(|indices, _| {
            with_slab_mut(|slab| {
                indices.iter().for_each(|idx| {
                    if slab.try_remove(*idx).is_none() {
                        eprintln!("Warning: SQE {} not found in slab during drop", idx);
                    }
                });
            });
        }) {
            eprintln!("{:?}", e);
        }
    }
}

impl From<SqeList> for Sqe<SqeList> {
    fn from(val: SqeList) -> Self {
        Sqe::new(val)
    }
}

// Contract on SqeBatch is:
// - all SQEs submitted in `io_uring` as part of the same `io_uring_enter` syscall
// - SQEs can complete in any order BUT result return will respect order of insertion
// - will only wake up Future when all SQEs have completed
pub struct SqeBatchBuilder {
    inner: SqeListBuilder,
}

impl Default for SqeBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SqeBatchBuilder {
    pub fn new() -> Self {
        Self {
            inner: SqeListBuilder::new(SqeListKind::Batch),
        }
    }

    pub fn add_entry(mut self, entry: Entry, flags: Option<Flags>) -> Self {
        self.inner = self.inner.add_entry(entry, flags);
        self
    }

    pub fn build(self) -> SqeList {
        SqeList::new(self.inner)
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

impl Default for SqeChainBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SqeChainBuilder {
    pub fn new() -> Self {
        Self {
            inner: SqeListBuilder::new(SqeListKind::Chain),
        }
    }

    pub fn add_entry(mut self, entry: Entry, flags: Option<Flags>) -> Self {
        self.inner = self.inner.add_entry(entry, flags);
        self
    }

    pub fn build(self) -> SqeList {
        SqeList::new(self.inner)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SqeListBuilder {
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

    fn add_entry(mut self, entry: Entry, flags: Option<Flags>) -> Self {
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
    fn try_build(self) -> Result<SqeListState, IoError> {
        let n_sqes = self.list.len();
        if n_sqes < 2 {
            return Err(anyhow!("SqeListBuilder requires at least 2 sqes, got {}", n_sqes).into());
        }

        let kind = self.kind;
        let mut entries = match kind {
            SqeListKind::Chain => self.into_chain(),
            SqeListKind::Batch => self.list,
        }
        .into_iter();

        let remaining = Arc::new(AtomicUsize::new(n_sqes));
        let mut indices = Vec::with_capacity(entries.len());

        with_slab_mut(|slab| -> Result<(), IoError> {
            let vacant = slab.vacant_entry()?;
            indices.push(vacant.key());

            let mut head = RawSqe::new(
                // SAFETY: self.list.len() >= 2.
                entries.next().unwrap(),
                CompletionHandler::new_batch_or_chain(indices[0], Arc::clone(&remaining)),
            );
            head.set_user_data(indices[0] as u64)?;
            _ = vacant.insert(head);

            for entry in entries {
                let (idx, _) = slab.insert(RawSqe::new(
                    entry,
                    CompletionHandler::new_batch_or_chain(indices[0], Arc::clone(&remaining)),
                ))?;
                indices.push(idx);
            }

            Ok(())
        })
        // If we fail for any reason here, we will be in an inconsistent state, where
        // some of our batch entries are in the slab and others are not. Let's return
        // invalid state error as this is unrecoverable.
        .map_err(|_| IoError::SlabInvalidState)?;

        Ok(SqeListState::Indexed {
            indices,
            remaining,
            kind,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{with_slab, with_slab_and_ring_mut};
    use crate::runtime::Builder;
    use crate::sqe::RawSqeState;
    use crate::test_utils::*;
    use anyhow::Result;
    use either::Either;
    use rstest::rstest;
    use std::pin::pin;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll};

    #[rstest]
    #[case::batch(SqeListKind::Batch)]
    #[case::batch(SqeListKind::Chain)]
    fn test_list_builder(#[case] kind: SqeListKind) -> Result<()> {
        init_local_runtime_and_context(None)?;

        let n = 4;
        let mut list = match kind {
            SqeListKind::Batch => build_batch(n),
            SqeListKind::Chain => build_chain(n),
        };

        // Need to submit for slab to be populated.
        let (waker, _) = mock_waker();
        list.submit(&waker)?;
        list.set_waker(&waker)?;

        let mut num_heads = 0;

        with_slab(|slab| -> Result<()> {
            list.with_indexed(|indices, _| -> Result<()> {
                for idx in indices {
                    let sqe = slab.get(*idx)?;
                    assert_eq!(sqe.get_state(), RawSqeState::Pending);

                    match &sqe.handler {
                        CompletionHandler::BatchOrChain {
                            head, remaining, ..
                        } => {
                            assert_eq!(remaining.load(Ordering::Relaxed), indices.len());
                            if sqe.has_waker() {
                                assert_eq!(*idx, *head, "sqe with waker should be head node");
                                num_heads += 1;
                            }
                        }
                        _ => assert!(false, "unexpected completion handler"),
                    }
                }
                Ok(())
            })?
        })?;

        assert_eq!(num_heads, 1, "expected one unique head node");
        Ok(())
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
        init_local_runtime_and_context(None)?;

        let list = match kind {
            SqeListKind::Batch => build_batch(size),
            SqeListKind::Chain => build_chain(size),
        };

        let (waker, waker_data) = mock_waker();
        let mut ctx = Context::from_waker(&waker);

        let sqe_list = Sqe::new(list);
        let mut sqe_fut = pin!(sqe_list);

        // Polling once will populate the slab and SQ ring. It will also set the
        // waker. Polling N more times after this won't change the state.
        for _ in 0..10 {
            assert!(matches!(sqe_fut.as_mut().poll(&mut ctx), Poll::Pending));
            assert_eq!(waker_data.get_count(), 0);
            assert_eq!(waker_data.get_pending_io(), 1);
        }

        let expected_user_data = sqe_fut.get().with_indexed(|indices, _| indices.clone())?;
        let head_idx = expected_user_data[0];

        with_slab_and_ring_mut(|slab, ring| {
            assert_eq!(ring.sq().len(), size);

            // Waker only set on head in batch/chain setup
            let res = slab.get(head_idx).and_then(|sqe| {
                assert!(!sqe.is_ready());
                assert!(sqe.has_waker());
                assert_eq!(sqe.get_state(), RawSqeState::Pending);
                Ok(())
            });

            assert!(res.is_ok());
        });

        with_core_mut(|core| -> Result<()> {
            // Submit SQEs and wait for CQEs :: `io_uring_enter`
            assert_eq!(core.submit_and_wait(size, None)?, size);
            assert_eq!(waker_data.get_count(), 0);

            // Process CQEs :: wakes up Waker
            assert_eq!(core.process_cqes(None)?, size);
            assert_eq!(waker_data.get_count(), 1);
            assert_eq!(waker_data.get_pending_io(), 0);
            Ok(())
        })?;

        if let Poll::Ready(Ok(results)) = sqe_fut.as_mut().poll(&mut ctx) {
            assert_eq!(results.len(), size);

            // SqeList contract is the results order has to respect the
            // insertion order.
            for (io_result, _user_data) in results.iter().zip(expected_user_data) {
                assert!(matches!(io_result, Ok(0)));
                // TODO: raw sqe has user_data?
                // assert_eq!(entry.get_user_data(), user_data as u64);
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

        init_local_runtime_and_context(None)?;

        let size = entries.len();
        let sqe_list = Sqe::new(build_list_with_entries(kind, entries));

        let mut sqe_fut = pin!(sqe_list);

        let (waker, waker_data) = mock_waker();
        let mut ctx = Context::from_waker(&waker);

        // Polling once will submit the batch SQE and set the waker.
        // Polling N more times after this won't change the state.
        for _ in 0..10 {
            assert!(matches!(sqe_fut.as_mut().poll(&mut ctx), Poll::Pending));
            assert_eq!(waker_data.get_count(), 0);
            assert_eq!(waker_data.get_pending_io(), 1);
        }

        let expected_user_data = sqe_fut.get().with_indexed(|indices, _| indices.clone())?;
        let head_idx = expected_user_data[0];

        with_slab_and_ring_mut(|slab, ring| {
            assert_eq!(ring.sq().len(), size);

            // Waker only set on head in batch/chain setup
            let res = slab.get(head_idx).and_then(|sqe| {
                assert!(!sqe.is_ready());
                assert!(sqe.has_waker());
                Ok(())
            });

            assert!(res.is_ok());
        });

        with_core_mut(|core| -> Result<()> {
            // Submit SQEs and wait for CQEs :: `io_uring_enter`
            assert_eq!(core.submit_and_wait(size, None)?, size);
            assert_eq!(waker_data.get_count(), 0);

            // Process CQEs :: wakes up Waker
            assert_eq!(core.process_cqes(None)?, size);
            assert_eq!(waker_data.get_count(), 1);
            assert_eq!(waker_data.get_pending_io(), 0);
            Ok(())
        })?;

        if let Poll::Ready(Ok(results)) = sqe_fut.as_mut().poll(&mut ctx) {
            assert_eq!(results.len(), size);

            // SqeList contract is the results order has to respect the
            // insertion order.
            let expectations = expected_user_data.iter().zip(expected_results);

            for (io_result, (_user_data, expected)) in results.iter().zip(expectations) {
                match (io_result, expected) {
                    (Err(e1), Either::Right(e2)) => assert_eq!(e1.raw_os_error().unwrap(), e2),
                    (Ok(r1), Either::Left(r2)) => assert_eq!(*r1, r2),
                    (left, right) => {
                        dbg!("left: {:?}, right: {:?}", left, right);
                        assert!(false,);
                    }
                }

                // TODO: raw sqe has user_data?
                // assert_eq!(entry.get_user_data(), *user_data as u64);
            }
        } else {
            assert!(false, "Expected Poll::Ready(Ok((entry, result)))");
        }

        Ok(())
    }

    #[test]
    fn test_overflow_slab() -> Result<()> {
        let ring_size = 32;
        let batch_size = 16;

        let builder = Builder::new_local().sq_ring_size(ring_size);
        init_local_runtime_and_context(Some(builder))?;

        let (waker, _) = mock_waker();

        for i in 0..=(ring_size / batch_size) {
            let mut list = build_batch(batch_size);
            let res = list.submit(&waker);

            if i == (ring_size / batch_size) + 1 {
                // This last submission should fail as we have exhausted the slab.
                assert!(res.is_err());
                assert_eq!(res.unwrap_err(), IoError::SlabFull);
                break;
            }
        }

        Ok(())
    }
}
