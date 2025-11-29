#![allow(unused)]

use crate::context;
use crate::runtime::{Schedule, SchedulerPanic};
use crate::sqe::{CompletionHandler, CqeRes, IoError, RawSqe, Submittable};
use futures::Stream;
use io_uring::squeue::Entry;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
pub(crate) enum SqeStreamState {
    Unsubmitted { entry: Entry, count: u32 },
    Submitted { idx: usize },
    Completed,
    Cancelled,
}

/// A backend for Multi-shot `io_uring` operations.
///
/// Unlike standard operations, a Multi-shot SQE (like `recv_multi` or `accept_multi`)
/// is submitted once but generates a stream of Completion Queue Entries (CQEs).
/// The kernel indicates more data is available via the `IORING_CQE_F_MORE` flag.
#[derive(Debug)]
pub(crate) struct SqeStream {
    state: SqeStreamState,
}

impl SqeStream {
    /// Create a new multi-shot stream SQE. It is the user responsibility to pass
    /// a properly constructed entry that does support multi-shot completions.
    /// If count is set, we will complete the stream after receiving `count` elements.
    /// Otherwise, we will keep producing results as long as completions are posted
    /// with the `IORING_CQE_F_MORE` flag.
    pub(crate) fn new(entry: Entry, count: u32) -> Self {
        Self {
            state: SqeStreamState::Unsubmitted { entry, count },
        }
    }

    pub(crate) fn get_idx(&self) -> Result<usize, IoError> {
        match self.state {
            SqeStreamState::Submitted { idx } => Ok(idx),
            _ => Err(IoError::SqeBackendInvalidState),
        }
    }

    // This API helps determine if the stream should be cleaned up. Important
    // otherwise we will keep posting CQE forever depending on the stream config.
    pub(crate) fn cancel(&mut self) -> Option<usize> {
        let old = mem::replace(&mut self.state, SqeStreamState::Cancelled);

        match old {
            SqeStreamState::Submitted { idx } => {
                context::with_slab_mut(|slab| slab.cancel(idx)).then_some(idx)
            }
            _ => None,
        }
    }
}

impl Submittable for SqeStream {
    /// Submit is a two-step process to avoid corrupted state. Slab and Ring are
    /// interconnected so we use a reservation API on the slab and only commit
    /// once we have successfully pushed to the ring. This enables safe retries
    /// and avoids corrupted state.
    fn submit(&mut self, waker: &Waker) -> Result<(), IoError> {
        match &mut self.state {
            SqeStreamState::Submitted { .. }
            | SqeStreamState::Cancelled
            | SqeStreamState::Completed => {
                return Err(IoError::SqeBackendInvalidState);
            }
            SqeStreamState::Unsubmitted { entry, count } => {
                context::with_slab_and_ring_mut(|slab, ring| {
                    let reserved = slab.reserve_entry()?;
                    let idx = reserved.key();

                    entry.set_user_data(idx as u64);
                    ring.push(entry)?;

                    reserved.commit(RawSqe::new(waker, CompletionHandler::new_stream(*count)));

                    Ok(idx)
                })
            }
        }
        // On successful push, transition state to submitted.
        .map(|idx| {
            self.state = SqeStreamState::Submitted { idx };
        })
    }
}

// SqeStream does not implement `Completable` trait, as it does not represent a one-off result.
// Instead, we implement `futures::Stream` interface, and will keep producing results to the
// user as they become available.
impl Stream for SqeStream {
    type Item = Result<CqeRes, IoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                SqeStreamState::Unsubmitted { .. } => {
                    match self.submit(cx.waker()) {
                        Ok(_) => {
                            debug_assert!(
                                matches!(self.state, SqeStreamState::Submitted { .. }),
                                "Submit should transition SqeStream state to Submitted."
                            );
                            continue;
                        }
                        Err(e) if e.is_retryable() => {
                            // We were unable to register the waker, and submit our IO to local uring.
                            // Yield to scheduler so we can take corrective action and retry later.
                            crate::with_scheduler!(|s| {
                                s.yield_now(cx.waker(), e.as_yield_reason(), None);
                            });

                            return Poll::Pending;
                        }
                        Err(e) => {
                            self.state = SqeStreamState::Completed;

                            if e.is_fatal() {
                                std::panic::panic_any(SchedulerPanic::new(
                                    e.as_panic_reason(),
                                    e.to_string(),
                                ));
                            }

                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                SqeStreamState::Submitted { idx } => {
                    return context::with_slab_mut(|slab| -> Poll<Option<Self::Item>> {
                        let raw_sqe = slab.get_mut(idx)?;

                        // It is the responsibility of the caller to cancel the
                        // stream if there is a bad result. We don't take responsibility
                        // for analyzing stream errors.
                        match raw_sqe.pop_next_result() {
                            Err(e) => Poll::Ready(Some(Err(e))),
                            Ok(Some(result)) => Poll::Ready(Some(Ok(result))),
                            Ok(None) => {
                                if raw_sqe.is_completed() {
                                    debug_assert!(
                                        slab.try_remove(idx).is_some(),
                                        "RawSqe not found in slab."
                                    );

                                    self.state = SqeStreamState::Completed;
                                    Poll::Ready(None)
                                } else {
                                    raw_sqe.set_waker(cx.waker());
                                    Poll::Pending
                                }
                            }
                        }
                    });
                }
                SqeStreamState::Completed | SqeStreamState::Cancelled => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl Drop for SqeStream {
    fn drop(&mut self) {
        if let SqeStreamState::Submitted { idx } = &self.state {
            context::with_slab_mut(|slab| {
                if slab.try_remove(*idx).is_none() {
                    eprintln!("[SqeStream]: SQE {} not found in slab during drop", idx);
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context;
    use crate::test_utils::*;
    use anyhow::Result;
    use io_uring::opcode::Timeout;
    use io_uring::types::{TimeoutFlags, Timespec};
    use rstest::rstest;
    use std::pin::pin;
    use std::task::{Context, Poll};

    #[rstest]
    #[case::single_timeout(1, 1_000)]
    #[case::ten_timeouts(10, 1_000)]
    #[case::two_timeouts_slow(2, 1_000_000)]
    fn test_sqe_stream_timeout_multishot(#[case] count: u32, #[case] nsecs: u32) -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        let timespec = Timespec::new().sec(0).nsec(nsecs);
        let timeout = Timeout::new(&timespec)
            .count(count)
            .flags(TimeoutFlags::MULTISHOT);

        let mut stream = pin!(SqeStream::new(timeout.build(), count));

        let (waker, waker_data) = mock_waker();
        let mut ctx = Context::from_waker(&waker);

        // Polling once will submit the SQE and set the waker.
        // Polling N more times after this won't change the state.
        for _ in 0..10 {
            let res = stream.as_mut().poll_next(&mut ctx);
            assert!(matches!(res, Poll::Pending));

            assert!(matches!(stream.as_mut().poll_next(&mut ctx), Poll::Pending));
            assert_eq!(waker_data.get_count(), 0);

            assert_eq!(context::with_core(|core| core.get_pending_ios()), 1);
            assert_eq!(waker_data.get_pending_ios(), 1);
        }

        let idx = stream.get_idx()?;

        context::with_slab_and_ring_mut(|slab, ring| -> Result<()> {
            assert_eq!(ring.sq().len(), 1);

            let res = slab.get(idx).and_then(|sqe| {
                assert!(!sqe.is_ready());
                assert!(sqe.waker.is_some());

                if let CompletionHandler::Stream { completion, .. } = &sqe.handler {
                    assert!(completion.has_more());
                }

                Ok(())
            });

            assert!(res.is_ok());

            // Because we use DEFER_TASKRUN, CQE are not automatically posted. Instead,
            // the kernel queues "task_work" everytime a timer fires. We have to
            // repeatedly enter the kernel with `io_uring_enter + GETEVENTS` to convert
            // the `task_work` into CQEs.
            let mut num_awaiting = count as usize;
            while num_awaiting > 0 {
                // Submit our single SQE and wait for CQEs :: `io_uring_enter`
                ring.submit_and_wait(num_awaiting, None)?;

                // Wait for our timeouts to have completed N times
                num_awaiting -= ring.process_cqes(slab, None)?;
            }

            assert_eq!(num_awaiting, 0);

            // We wake the task N time, and let scheduler handle the Notified
            // state and set the bit appropriately.
            assert_eq!(waker_data.get_count(), count as usize);

            if let CompletionHandler::Stream {
                results,
                completion,
            } = &slab.get(idx)?.handler
            {
                assert_eq!(results.len(), count as usize);
                assert!(!completion.has_more());
            }

            Ok(())
        })?;

        let mut fired = 0;
        while fired < count {
            match stream.as_mut().poll_next(&mut ctx) {
                Poll::Ready(Some(Err(IoError::Io(e)))) => {
                    assert_eq!(e.raw_os_error().unwrap(), libc::ETIME);
                    fired += 1;
                }
                Poll::Pending => continue,
                Poll::Ready(other) => {
                    dbg!("{:}", other);
                    assert!(false);
                }
            }
        }

        assert_eq!(fired, count);

        // Polling one more time stream should be completed, and we should drop
        // the RawSqe from the slab.
        assert!(matches!(
            stream.as_mut().poll_next(&mut ctx),
            Poll::Ready(None)
        ));
        assert_eq!(context::with_core(|core| core.get_pending_ios()), 0);
        assert_eq!(waker_data.get_pending_ios(), 0);
        Ok(())
    }
}
