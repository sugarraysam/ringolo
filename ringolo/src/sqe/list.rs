use crate::context;
use crate::context::slab::SlabReservedBatch;
use crate::runtime::SPILL_TO_HEAP_THRESHOLD;
use crate::sqe::errors::IoError;
use crate::sqe::{Completable, CompletionHandler, RawSqe, Sqe, Submittable};
use io_uring::squeue::{Entry, Flags};
use smallvec::SmallVec;
use std::io::{self};
use std::iter;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Poll, Waker};

// Batch and Chain APIs resolve to this type
//
// We use Arc<Atomic> for remaining counter as we need SqeList to be Sync + Send.
pub(crate) struct SqeList {
    state: SqeListState,
}

pub(crate) enum SqeListState {
    Unsubmitted {
        builder: SqeListBuilderInner,
    },
    Submitted {
        indices: SmallVec<[usize; SPILL_TO_HEAP_THRESHOLD]>,
        remaining: Arc<AtomicUsize>,
        kind: SqeListKind,
    },
    Completed,
}

#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub(crate) enum SqeListKind {
    Batch,

    // Chain API:
    // - The next SQE will not be started until the previous one completes.
    // - chain of requests must be submitted together as one batch
    // - If any request in a chained sequence fails, the entire chain is aborted
    // - CQEs will come in order, but may be interleaved
    Chain,
}

impl SqeList {
    fn new(builder: SqeListBuilderInner) -> Self {
        Self {
            state: SqeListState::Unsubmitted { builder },
        }
    }

    fn get_head_idx(&self) -> Result<usize, IoError> {
        match &self.state {
            SqeListState::Submitted { indices, .. } => Ok(indices[0]),
            _ => Err(IoError::SqeBackendInvalidState),
        }
    }

    // We set the waker on the head of the list.
    pub(crate) fn set_waker(&self, waker: &Waker) -> Result<(), IoError> {
        let head_idx = self.get_head_idx()?;

        context::with_slab_mut(|slab| {
            let head = slab.get_mut(head_idx)?;
            head.set_waker(waker);
            Ok(())
        })
    }

    pub(crate) fn is_ready(&self) -> bool {
        match &self.state {
            SqeListState::Unsubmitted { .. } => false,
            SqeListState::Submitted { remaining, .. } => remaining.load(Ordering::Relaxed) == 0,
            SqeListState::Completed => true,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match &self.state {
            SqeListState::Unsubmitted { builder } => builder.len(),
            SqeListState::Submitted { indices, .. } => indices.len(),
            SqeListState::Completed => 0,
        }
    }
}

impl Submittable for SqeList {
    fn submit(&mut self, waker: &Waker) -> Result<(), IoError> {
        match &mut self.state {
            SqeListState::Submitted { .. } | SqeListState::Completed => {
                return Err(IoError::SqeBackendInvalidState);
            }
            SqeListState::Unsubmitted { builder } => {
                context::with_slab_and_ring_mut(|slab, ring| {
                    let reserved_batch = slab.reserve_batch(builder.len())?;

                    let (raws, next_state) = builder.build(&reserved_batch);
                    ring.push_batch(builder.entries())?;

                    reserved_batch.commit(raws)?;
                    Ok(next_state)
                })
            }
        }
        .map(|next_state| {
            // On successful push, we can transition the state and increment the
            // number of pending IOs at both the thread-level and task-level.
            self.state = next_state;
            context::with_core(|core| {
                core.increment_pending_ios();
                core.increment_task_pending_ios(waker);
            });
        })
    }
}

impl Completable for SqeList {
    type Output = Result<Vec<io::Result<i32>>, IoError>;

    fn poll_complete(&mut self, waker: &Waker) -> Poll<Self::Output> {
        if !self.is_ready() {
            // In normal operations, the SqeList is only woken up when it's last RawSqe
            // completes. We still want to account for spurious wakeups or complex future
            // interactions so let's update the waker every time.
            return match self.set_waker(waker) {
                Ok(_) => Poll::Pending,
                Err(e) => Poll::Ready(Err(e)),
            };
        }

        let SqeListState::Submitted { indices, .. } = &self.state else {
            return Poll::Ready(Err(IoError::SqeBackendInvalidState));
        };

        let res = context::with_slab_mut(|slab| -> Self::Output {
            let mut n = 0;

            let res = indices
                .iter()
                .filter_map(|idx| slab.try_remove(*idx))
                .map(|mut sqe| Ok(sqe.take_final_result()))
                .inspect(|_| n += 1)
                .collect::<Self::Output>();

            debug_assert!(
                n == indices.len(),
                "SqeList completion logic error: unable to free all RawSqes"
            );

            // # Important
            // We need to decrement task `pending_ios` *after* dropping the RawSqe
            // to prevent making the task stealable while it still needs to drop this
            // RawSqe from thread-local slab.
            context::with_core(|c| c.decrement_task_pending_ios(waker));

            res
        });

        self.state = SqeListState::Completed;
        Poll::Ready(res)
    }
}

impl Drop for SqeList {
    fn drop(&mut self) {
        if let SqeListState::Submitted { indices, .. } = &self.state {
            // If we get here, it means *nothing* will decrement the task pending_ios.
            // The consequence is the task will *not be stealable*. We can live with that
            // and there is no way to get access to the underlying task anyways because
            // the Waker is lost.
            context::with_slab_mut(|slab| {
                indices.iter().for_each(|idx| {
                    if slab.try_remove(*idx).is_none() {
                        eprintln!("[SqeList]: RawSqe {} not found in slab during drop", idx);
                    }
                });
            });
        }
    }
}

impl From<SqeList> for Sqe<SqeList> {
    fn from(val: SqeList) -> Self {
        Sqe::new(val)
    }
}

// Trait so we can use either SqeBatchBuilder or SqeChainBuilder in future lib.
pub trait SqeListBuilder {
    fn add_entry(self, entry: Entry, flags: Option<Flags>) -> Self;

    fn build(self) -> SqeList;
}

// Contract on SqeBatch is:
// - all SQEs submitted in `io_uring` as part of the same `io_uring_enter` syscall
// - SQEs can complete in any order BUT result return will respect order of insertion
// - will only wake up Future when all SQEs have completed
#[derive(Debug)]
pub struct SqeBatchBuilder {
    inner: SqeListBuilderInner,
}

impl Default for SqeBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SqeBatchBuilder {
    pub fn new() -> Self {
        Self {
            inner: SqeListBuilderInner::new(SqeListKind::Batch),
        }
    }
}

impl SqeListBuilder for SqeBatchBuilder {
    fn add_entry(mut self, entry: Entry, flags: Option<Flags>) -> Self {
        self.inner = self.inner.add_entry(entry, flags);
        self
    }

    fn build(self) -> SqeList {
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
// - We only wake up the Future after all SQEs have complete
#[derive(Debug)]
pub struct SqeChainBuilder {
    inner: SqeListBuilderInner,
}

impl Default for SqeChainBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SqeChainBuilder {
    pub fn new() -> Self {
        Self {
            inner: SqeListBuilderInner::new(SqeListKind::Chain),
        }
    }
}

impl SqeListBuilder for SqeChainBuilder {
    fn add_entry(mut self, entry: Entry, flags: Option<Flags>) -> Self {
        self.inner = self.inner.add_entry(entry, flags);
        self
    }

    fn build(self) -> SqeList {
        SqeList::new(self.inner.into_chain())
    }
}

// This builder has to be private for a few reasons:
// - try_build is *repeatable* and it is safe to call it more than once, enabling
//   retries when submitting entries in SQ ring
// - `try_build` interacts with thread-local RawSqe which has to happen when a
//   task is being polled.
#[derive(Debug, Clone)]
pub(crate) struct SqeListBuilderInner {
    list: SmallVec<[Entry; SPILL_TO_HEAP_THRESHOLD]>,
    kind: SqeListKind,
}

impl SqeListBuilderInner {
    fn new(kind: SqeListKind) -> Self {
        Self {
            list: SmallVec::with_capacity(SPILL_TO_HEAP_THRESHOLD),
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
    fn into_chain(mut self) -> Self {
        let last = self.list.len();

        self.list = self
            .list
            .into_iter()
            .enumerate()
            .map(|(i, e)| {
                if i != last - 1 {
                    e.flags(Flags::IO_LINK)
                } else {
                    e
                }
            })
            .collect();

        self
    }

    fn len(&self) -> usize {
        self.list.len()
    }

    fn entries(&self) -> &SmallVec<[Entry; SPILL_TO_HEAP_THRESHOLD]> {
        &self.list
    }

    // Build the linked list of RawSqe for either SqeChain or SqeBatch abstractions.
    // This builder maintains the order of insertion, and the contract is that we will also
    // respect this order when returning results.
    fn build(
        &mut self,
        batch: &SlabReservedBatch<'_>,
    ) -> (SmallVec<[RawSqe; SPILL_TO_HEAP_THRESHOLD]>, SqeListState) {
        let n_sqes = self.list.len();
        let remaining = Arc::new(AtomicUsize::new(n_sqes));

        let indices = batch.keys();
        let head_idx = indices[0];

        let raws = iter::repeat_with(|| {
            RawSqe::new(CompletionHandler::new_batch_or_chain(
                head_idx,
                Arc::clone(&remaining),
            ))
        })
        .take(n_sqes)
        .collect();

        self.list
            .iter_mut()
            .zip(indices.iter())
            .for_each(|(entry, user_data)| {
                entry.set_user_data(*user_data as u64);
            });

        (
            raws,
            SqeListState::Submitted {
                indices,
                remaining,
                kind: self.kind,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context;
    use crate::runtime::Builder;
    use crate::sqe::RawSqeState;
    use crate::test_utils::*;
    use anyhow::{Result, anyhow};
    use either::Either;
    use rstest::rstest;
    use std::pin::pin;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll};

    #[rstest]
    #[case::batch(SqeListKind::Batch)]
    #[case::batch(SqeListKind::Chain)]
    fn test_list_builder(#[case] kind: SqeListKind) -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

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

        context::with_slab(|slab| -> Result<()> {
            let SqeListState::Submitted { indices, .. } = &list.state else {
                return Err(anyhow!("unexpected sqe list state"));
            };

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
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

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

            context::with_core(|core| assert_eq!(core.get_pending_ios(), 1));
            assert_eq!(waker_data.get_pending_ios(), 1);
        }

        let head_idx = sqe_fut.get().get_head_idx()?;

        context::with_slab_and_ring_mut(|slab, ring| {
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

        context::with_slab_and_ring_mut(|slab, ring| -> Result<()> {
            // Submit SQEs and wait for CQEs :: `io_uring_enter`
            assert_eq!(ring.submit_and_wait(size, None)?, size);
            assert_eq!(waker_data.get_count(), 0);

            // Process CQEs :: wakes up Waker
            assert_eq!(ring.process_cqes(slab, None)?, size);
            assert_eq!(waker_data.get_count(), 1);

            // Only core pending_ios is zero
            context::with_core(|core| assert_eq!(core.get_pending_ios(), 0));
            assert_eq!(waker_data.get_pending_ios(), 1);
            Ok(())
        })?;

        if let Poll::Ready(Ok(results)) = sqe_fut.as_mut().poll(&mut ctx) {
            assert_eq!(results.len(), size);

            // SqeList contract is the results order has to respect the
            // insertion order.
            for io_result in results {
                assert!(matches!(io_result, Ok(0)));
            }
        } else {
            assert!(false, "Expected Poll::Ready(Ok((entry, result)))");
        }

        // Task pending io reaches 0 after RawSqe is released
        assert_eq!(waker_data.get_pending_ios(), 0);
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

        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

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

            context::with_core(|core| assert_eq!(core.get_pending_ios(), 1));
            assert_eq!(waker_data.get_pending_ios(), 1);
        }

        let SqeListState::Submitted { indices, .. } = &sqe_fut.get().state else {
            return Err(anyhow!("unexpected sqe list state"));
        };

        let expected_user_data = indices.clone();
        let head_idx = expected_user_data[0];

        context::with_slab_and_ring_mut(|slab, ring| {
            assert_eq!(ring.sq().len(), size);

            // Waker only set on head in batch/chain setup
            let res = slab.get(head_idx).and_then(|sqe| {
                assert!(!sqe.is_ready());
                assert!(sqe.has_waker());
                Ok(())
            });

            assert!(res.is_ok());
        });

        context::with_slab_and_ring_mut(|slab, ring| -> Result<()> {
            // Submit SQEs and wait for CQEs :: `io_uring_enter`
            assert_eq!(ring.submit_and_wait(size, None)?, size);
            assert_eq!(waker_data.get_count(), 0);

            // Process CQEs :: wakes up Waker
            assert_eq!(ring.process_cqes(slab, None)?, size);
            assert_eq!(waker_data.get_count(), 1);

            context::with_core(|core| assert_eq!(core.get_pending_ios(), 0));
            assert_eq!(waker_data.get_pending_ios(), 1);
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
            }
        } else {
            assert!(false, "Expected Poll::Ready(Ok((entry, result)))");
        }

        assert_eq!(waker_data.get_pending_ios(), 0);
        Ok(())
    }

    #[test]
    fn test_overflow_slab() -> Result<()> {
        let ring_size = 32;
        let batch_size = 16;

        let builder = Builder::new_local().sq_ring_size(ring_size);
        let (_runtime, _scheduler) = init_local_runtime_and_context(Some(builder))?;

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
