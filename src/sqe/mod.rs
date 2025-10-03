use anyhow::{Result, anyhow};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::{io, mem};

// Re-exports
pub mod list;
pub use list::{SqeBatchBuilder, SqeChainBuilder, SqeList, SqeListKind};
pub mod message;
pub use message::SqeRingMessage;
pub mod raw;
pub use raw::RawSqe;
pub use raw::{CompletionEffect, CompletionHandler, RawSqeState};
pub mod single;
pub use single::SqeSingle;
pub mod stream;
pub use stream::{SqeStream, SqeStreamError};

pub trait Submittable {
    /// We pas the waker so we get access to the RawTask Header.
    fn submit(&self, waker: &Waker) -> io::Result<i32>;
}

pub trait Completable {
    type Output;

    /// We pas the waker so we get access to the RawTask Header but also because
    /// implementation of completable needs to guarantee the associated task will
    /// be woken up in the future.
    fn poll_complete(&self, waker: &Waker) -> Poll<Self::Output>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Initial,
    Submitted,
    Completed,
}

pub struct Sqe<T: Submittable + Completable> {
    state: State,
    inner: T,
}

impl<T: Submittable + Completable> Sqe<T> {
    pub fn new(inner: T) -> Self {
        Self {
            state: State::Initial,
            inner,
        }
    }

    pub fn get(&self) -> &T {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: Submittable + Completable> Unpin for Sqe<T> {}

impl<E, T: Submittable + Completable<Output = Result<E>> + Send> Future for Sqe<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match this.state {
                State::Initial => {
                    if let Err(e) = this.inner.submit(cx.waker()) {
                        if e.kind() == io::ErrorKind::ResourceBusy {
                            // TODO: log error this is real bad, need to double SQ ring size
                            // Submission queue is full, yield and try again later.
                            //
                            // TODO BUG: will never be woken up?
                            eprintln!("Warning: Submission queue is full, double SQ ring size");
                            return Poll::Pending;
                        } else {
                            this.state = State::Completed;
                            return Poll::Ready(Err(anyhow!("failed to submit: {:?}", e)));
                        }
                    }

                    // Continue the loop to immediately poll for completion. Although very unlikely
                    // as we will batch submit SQEs in SQ ring. Nevertheless, we want to call
                    // `poll_complete` to ensure we set the waker.
                    this.state = State::Submitted;
                    continue;
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

pub struct SqeCollection<T: Submittable + Completable> {
    state: State,
    inner: Vec<(usize, T)>,

    // Store results in internal future state to persist across poll calls.
    // We store it behind Option<T> because this type impl default which allows
    // pre-allocating and indexing directly.
    results: Vec<Option<T::Output>>,
}

impl<T: Submittable + Completable> SqeCollection<T> {
    pub fn new(inner: Vec<T>) -> Self {
        let results = (0..inner.len()).map(|_| None).collect();
        Self {
            state: State::Initial,
            inner: inner.into_iter().enumerate().collect(),
            results,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T: Submittable + Completable> Unpin for SqeCollection<T> {}

impl<E, T: Submittable + Completable<Output = Result<E>> + Send> Future for SqeCollection<T> {
    type Output = Vec<T::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match this.state {
                State::Initial => {
                    // Submit implementation only pushes sqes in SQ ring so this
                    // is quite efficient and we maintain the guarantee that all
                    // operations are submitted as part of the same `io_uring_enter`
                    // syscall.
                    for (_, sqe) in &mut this.inner {
                        if let Err(e) = sqe.submit(cx.waker()) {
                            if e.kind() == io::ErrorKind::ResourceBusy {
                                // TODO: log error this is real bad, need to double SQ ring size
                                // Submission queue is full, yield and try again later.
                                eprintln!("Warning: Submission queue is full, double SQ ring size");
                                return Poll::Pending;
                            } else {
                                this.state = State::Completed;
                                return Poll::Ready(vec![Err(anyhow!(
                                    "failed to submit: {:?}",
                                    e
                                ))]);
                            }
                        }
                    }

                    // Continue the loop to immediately poll for completion. Although very unlikely
                    // as we will batch submit SQEs in SQ ring. Nevertheless, we want to call
                    // `poll_complete` to ensure we set the waker.
                    this.state = State::Submitted;
                    continue;
                }
                State::Submitted => {
                    this.inner.retain(|(idx, sqe)| {
                        match sqe.poll_complete(cx.waker()) {
                            Poll::Ready(res) => {
                                this.results[*idx] = Some(res);
                                false
                            }
                            Poll::Pending => true, // keep polling
                        }
                    });

                    if this.is_ready() {
                        this.state = State::Completed;

                        let results = mem::replace(&mut this.results, vec![])
                            .into_iter()
                            .filter_map(|res| res)
                            .collect();

                        return Poll::Ready(results);
                    } else {
                        return Poll::Pending;
                    }
                }

                State::Completed => {
                    panic!("Future polled after completion");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{init_context, with_context_mut};
    use crate::test_utils::*;
    use either::Either;
    use io_uring::squeue::Entry;
    use rstest::rstest;
    use std::pin::pin;
    use std::task::{Context, Poll};

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

        init_context(64);

        let num_lists = entries.len();
        let n_sqes = entries.iter().map(Vec::len).sum();

        let collection = kinds
            .into_iter()
            .zip(entries.into_iter())
            .map(|(kind, entries)| build_list_with_entries(kind, entries))
            .collect::<Result<Vec<_>>>()?;

        let mut sqe_fut = pin!(SqeCollection::new(collection));

        let (waker, waker_data) = mock_waker();
        let mut ctx = Context::from_waker(&waker);

        // Polling once will submit the batch SQE and set the waker.
        // Polling N more times after this won't change the state.
        for _ in 0..10 {
            assert!(matches!(sqe_fut.as_mut().poll(&mut ctx), Poll::Pending));
            assert_eq!(waker_data.get_count(), 0);
            assert_eq!(waker_data.get_pending_io(), num_lists as i32);

            with_context_mut(|ctx| {
                assert_eq!(ctx.ring.submission().len(), n_sqes);
            });
        }

        with_context_mut(|ctx| -> Result<()> {
            // Submit SQEs and wait for CQEs :: `io_uring_enter`
            assert_eq!(ctx.submit_and_wait(n_sqes, None)?, n_sqes);
            assert_eq!(waker_data.get_count(), 0);

            // Process CQEs :: wakes up Waker
            assert_eq!(ctx.process_cqes(None)?, n_sqes);
            assert_eq!(waker_data.get_count(), num_lists as u32);
            assert_eq!(waker_data.get_pending_io(), 0);
            Ok(())
        })?;

        if let Poll::Ready(results) = sqe_fut.as_mut().poll(&mut ctx) {
            assert_eq!(results.len(), num_lists);
            assert!(results.iter().all(Result::is_ok));

            let results = results.into_iter().collect::<Result<Vec<_>>>()?;

            // SqeCollection contract is the results order has to respect the insertion order.
            for (got, expected) in results.iter().zip(expected_results) {
                assert_eq!(got.len(), expected.len());

                for ((_, io_result), expected) in got.iter().zip(expected) {
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
