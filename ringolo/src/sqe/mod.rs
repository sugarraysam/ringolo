#![allow(dead_code)]

use crate::context::with_core;
use crate::runtime::{Schedule, SchedulerPanic};
use crate::task::Header;
use crate::with_scheduler;
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll, Waker};

// Re-exports
pub(crate) mod errors;
pub(crate) use errors::IoError;

pub(crate) mod list;
#[allow(unused)]
pub(crate) use list::{SqeBatchBuilder, SqeChainBuilder, SqeList, SqeListKind};

pub(crate) mod raw;
pub(crate) use self::raw::RawSqe;
pub(crate) use self::raw::{CompletionEffect, CompletionHandler, RawSqeState};

pub(crate) mod single;
pub(crate) use self::single::SqeSingle;

pub(crate) mod stream;
pub(crate) use self::stream::SqeStream;

pub(crate) trait Submittable {
    /// We pas the waker so we get access to the RawTask Header.
    fn submit(&mut self, waker: &Waker) -> Result<(), IoError>;
}

pub(crate) trait Completable {
    type Output;

    /// We pas the waker so we get access to the RawTask Header but also because
    /// implementation of completable needs to guarantee the associated task will
    /// be woken up in the future.
    fn poll_complete(&mut self, waker: &Waker) -> Poll<Self::Output>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum State {
    Initial,
    Submitted,
    Completed,
}

#[derive(Debug)]
pub(crate) struct Sqe<T: Submittable + Completable + Unpin> {
    state: State,

    inner: T,
}

impl<T: Submittable + Completable + Unpin> Sqe<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self {
            state: State::Initial,
            inner,
        }
    }

    pub(crate) fn get(&self) -> &T {
        &self.inner
    }

    pub(crate) fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<E, T: Submittable + Completable<Output = Result<E, IoError>> + Unpin + Send> Future
    for Sqe<T>
{
    type Output = T::Output;

    #[track_caller]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match this.state {
                State::Initial => {
                    match this.inner.submit(cx.waker()) {
                        Ok(_) => {
                            // Continue the loop to immediately poll for completion. Although very unlikely
                            // as we will batch submit SQEs in SQ ring. Nevertheless, we want to call
                            // `poll_complete` to ensure we set the waker.
                            this.state = State::Submitted;
                            continue;
                        }
                        Err(e) if e.is_retryable() => {
                            // We were unable to register the waker, and submit our IO to local uring.
                            // Yield to scheduler so we can take corrective action and retry later.
                            with_scheduler!(|s| {
                                s.yield_now(cx.waker(), e.as_yield_reason(), None);
                            });

                            return Poll::Pending;
                        }
                        Err(e) => {
                            this.state = State::Completed;

                            if e.is_fatal() {
                                std::panic::panic_any(SchedulerPanic::new(
                                    e.as_panic_reason(),
                                    e.to_string(),
                                ));
                            }

                            return Poll::Ready(Err(e));
                        }
                    }
                }
                State::Submitted => match this.inner.poll_complete(cx.waker()) {
                    Poll::Ready(res) => {
                        this.state = State::Completed;
                        return Poll::Ready(res);
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
                State::Completed => {
                    panic!("Future polled after completion");
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct SqeCollection<T: Submittable + Completable + Unpin> {
    inner: Vec<(usize, Sqe<T>)>,

    // Store results in internal future state to persist across poll calls.
    // We store it behind Option<T> because this type impl default which allows
    // pre-allocating and indexing directly.
    results: Vec<Option<T::Output>>,
}

impl<T: Submittable + Completable + Unpin> SqeCollection<T> {
    pub(crate) fn new(inner: Vec<T>) -> Self {
        let results = (0..inner.len()).map(|_| None).collect();
        Self {
            inner: inner.into_iter().map(Sqe::new).enumerate().collect(),
            results,
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.inner.is_empty() && self.results.iter().all(Option::is_some)
    }
}

// Since we store E inside SqeCollection, it also needs to be Unpin.
impl<E: Unpin, T: Submittable + Completable<Output = Result<E, IoError>> + Unpin + Send> Future
    for SqeCollection<T>
{
    type Output = Vec<T::Output>;

    #[track_caller]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Delegate to all inner future implementations.
        this.inner
            .retain_mut(|(idx, fut)| match Pin::new(fut).poll(cx) {
                Poll::Ready(result) => {
                    this.results[*idx] = Some(result);
                    false
                }
                Poll::Pending => true,
            });

        if this.is_ready() {
            let results = std::mem::take(&mut this.results)
                .into_iter()
                // Safety: panics if any result is None but we just checked.
                .flatten()
                .collect();
            Poll::Ready(results)
        } else {
            Poll::Pending
        }
    }
}

// In our submit implementation, we keep track of each task's pending IO. This is
// our signal to determine if a task can be safely stolen by another thread. It
// will also influence the scheduling logic (e.g.: place on a stealable queue).
//
// BUT, the root future is different. It gets a Waker implementation where the
// data ptr is actually the Scheduler, and not a task Header. We use `thread_local`
// trick to detect if we are polling the root future, and avoid accessing invalid
// memory location.
pub(super) fn increment_pending_io(waker: &Waker) {
    unsafe {
        if !with_core(|core| core.is_polling_root()) {
            let ptr = NonNull::new_unchecked(waker.data() as *mut Header);
            Header::increment_pending_io(ptr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::list::{SqeList, SqeListKind};
    use super::*;
    use crate::context::{with_ring_mut, with_slab_and_ring_mut};
    use crate::test_utils::*;
    use anyhow::Result;
    use either::Either;
    use io_uring::squeue::Entry;
    use rstest::rstest;
    use static_assertions::assert_impl_one;
    use std::pin::pin;
    use std::task::{Context, Poll};

    // Make sure all our SQE backend impl Unpin.
    assert_impl_one!(SqeSingle: Unpin);
    assert_impl_one!(SqeList: Unpin);
    assert_impl_one!(SqeStream: Unpin);

    #[rstest]
    #[case::collection_two_batches(
        vec![
            SqeListKind::Batch,
            SqeListKind::Batch,
            SqeListKind::Batch,
        ],
        vec![
            vec![
                nop(),
                nop(),
            ],
            vec![
                openat(-22, "dummy"),
                openat(-33, "invalid"),
            ],
            vec![
                nop(),
                nop(),
            ],
        ],
        vec![
            vec![
                Either::Left(0),
                Either::Left(0),
            ],
            vec![
                Either::Right(libc::ENOENT),
                Either::Right(libc::ENOENT),
            ],
            vec![
                Either::Left(0),
                Either::Left(0),
            ],
        ]
    )]
    #[case::collection_three_chains(
        vec![
            SqeListKind::Chain,
            SqeListKind::Chain,
            SqeListKind::Chain,
        ],
        vec![
            vec![
                nop(),
                nop(),
            ],
            vec![
                openat(-333, "dummy"),
                openat(-111, "invalid"),
                nop(),
                openat(-22, "invalid"),
            ],
            vec![
                nop(),
                nop(),
                openat(-333, "dummy"),
                nop(),
                nop(),
            ],
        ],

        vec![
            vec![
                Either::Left(0),
                Either::Left(0),
            ],
            vec![
                Either::Right(libc::ENOENT),
                Either::Right(libc::ENOENT),
                Either::Right(libc::ECANCELED),
                Either::Right(libc::ENOENT),
            ],
            vec![
                Either::Left(0),
                Either::Left(0),
                Either::Right(libc::ENOENT),
                Either::Right(libc::ECANCELED),
                Either::Right(libc::ECANCELED),
            ],
        ]
    )]
    fn test_sqe_collection(
        #[case] kinds: Vec<SqeListKind>,
        #[case] entries: Vec<Vec<Entry>>,
        #[case] expected_results: Vec<Vec<Either<i32, i32>>>,
    ) -> Result<()> {
        assert_eq!(entries.len(), expected_results.len());

        init_local_runtime_and_context(None)?;

        let num_lists = entries.len();
        let n_sqes = entries.iter().map(Vec::len).sum();

        let collection = kinds
            .into_iter()
            .zip(entries.into_iter())
            .map(|(kind, entries)| build_list_with_entries(kind, entries))
            .collect::<Vec<_>>();

        let mut sqe_fut = pin!(SqeCollection::new(collection));

        let (waker, waker_data) = mock_waker();
        let mut ctx = Context::from_waker(&waker);

        // Polling once will submit the batch SQE and set the waker.
        // Polling N more times after this won't change the state.
        for _ in 0..10 {
            assert!(matches!(sqe_fut.as_mut().poll(&mut ctx), Poll::Pending));
            assert_eq!(waker_data.get_count(), 0);
            assert_eq!(waker_data.get_pending_io(), num_lists as i32);

            with_ring_mut(|ring| {
                assert_eq!(ring.sq().len(), n_sqes);
            });
        }

        with_slab_and_ring_mut(|slab, ring| -> Result<()> {
            // Submit SQEs and wait for CQEs :: `io_uring_enter`
            assert_eq!(ring.submit_and_wait(n_sqes, None)?, n_sqes);
            assert_eq!(waker_data.get_count(), 0);

            // Process CQEs :: wakes up Waker
            assert_eq!(ring.process_cqes(slab, None)?, n_sqes);
            assert_eq!(waker_data.get_count(), num_lists);
            assert_eq!(waker_data.get_pending_io(), 0);
            Ok(())
        })?;

        if let Poll::Ready(results) = sqe_fut.as_mut().poll(&mut ctx) {
            assert_eq!(results.len(), num_lists);
            assert!(results.iter().all(Result::is_ok));

            let results = results.into_iter().collect::<Result<Vec<_>, IoError>>()?;

            // SqeCollection contract is the results order has to respect the insertion order.
            for (got, expected) in results.iter().zip(expected_results) {
                assert_eq!(got.len(), expected.len());

                for (io_result, expected) in got.iter().zip(expected) {
                    match (io_result, expected) {
                        (Err(e1), Either::Right(e2)) => assert_eq!(e1.raw_os_error().unwrap(), e2),
                        (Ok(r1), Either::Left(r2)) => assert_eq!(*r1, r2),
                        (got, expected) => {
                            dbg!("got: {:?}, expected: {:?}", got, expected);
                            // assert!(false);
                        }
                    }
                }
            }
        } else {
            assert!(false, "Expected Poll::Ready(Ok((entry, result)))");
        }

        Ok(())
    }
}
