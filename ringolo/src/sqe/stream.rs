use crate::context::{with_core_mut, with_slab_mut};
use crate::runtime::Schedule;
use crate::sqe::{
    CompletionHandler, IoError, RawSqe, RawSqeState, Submittable, increment_pending_io,
};
use crate::with_scheduler;
use anyhow::anyhow;
use futures::Stream;
use io_uring::squeue::Entry;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
pub(crate) enum SqeStreamState {
    Preparing { entry: Entry, count: Option<u32> },
    Indexed { idx: usize },
    Submitted { idx: usize },
    Completed,
    Cancelled,
}

#[derive(Debug)]
pub struct SqeStream {
    state: SqeStreamState,
}

impl SqeStream {
    // If count is set, we will complete the stream after receiving `count` elements.
    // Otherwise, we will keep producing results as long as completions are posted
    // with the `IORING_CQE_F_MORE` flag.
    pub fn try_new(entry: Entry, count: Option<u32>) -> io::Result<Self> {
        // TODO: add validation for opcode that can support `IORING_CQE_F_MORE`
        // waiting on PR :: opcode::ACCEPT | opcode::RECV | opcode::RECVMSG
        // entry.opcode()
        //
        // Which SQE flags to set?

        Ok(Self {
            state: SqeStreamState::Preparing { entry, count },
        })
    }

    pub(crate) fn get_idx(&self) -> Result<usize, IoError> {
        match self.state {
            SqeStreamState::Indexed { idx } | SqeStreamState::Submitted { idx } => Ok(idx),
            _ => Err(anyhow!("unexpected sqe stream state: {:?}", self.state).into()),
        }
    }

    pub(crate) fn cancel(mut self: Pin<&mut Self>) -> Option<usize> {
        let old = mem::replace(&mut self.state, SqeStreamState::Cancelled);
        match old {
            SqeStreamState::Submitted { idx } => Some(idx),
            _ => None,
        }
    }
}

impl Submittable for SqeStream {
    fn submit(&mut self, waker: &Waker) -> Result<i32, IoError> {
        // Submit can be retried for example when the ring is full. We need to make sure we only count this IO once,
        // and only insert the entry in the slab once.
        if let SqeStreamState::Preparing { entry, count } = &mut self.state {
            // We run `pre_push_validation` to make the submission *atomic*. This is
            // because we must ensure that entries are added to both the ring and the slab
            // to avoid corrupted state.
            with_core_mut(|core| core.pre_push_validation(1))?;

            // Important: clone entry + count so we can retry if Slab is full.
            let count = *count;
            let entry = entry.clone();
            let idx = with_slab_mut(|slab| {
                slab.insert(RawSqe::new(entry, CompletionHandler::new_stream(count)))
                    .map(|(idx, _)| idx)
            })?;

            let _ = mem::replace(&mut self.state, SqeStreamState::Indexed { idx });
            increment_pending_io(waker);
        }

        match &self.state {
            SqeStreamState::Indexed { idx } => with_core_mut(|core| core.push_sqes([*idx].iter())),
            _ => Err(anyhow!("SqeStream invalid state: expected state indexed").into()),
        }
    }
}

impl Unpin for SqeStream {}

// SqeStream does not implement `Completable` trait, as it does not represent a one-off result.
// Instead, we implement `futures::Stream` interface, and will keep producing results to the
// user as they become available.
impl Stream for SqeStream {
    type Item = Result<i32, IoError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match this.state {
                SqeStreamState::Preparing { .. } | SqeStreamState::Indexed { .. } => {
                    match this.submit(cx.waker()) {
                        Ok(_) => {
                            // Submission successful, advance state and immediately
                            // try to poll for a result.
                            dbg!("submitted");
                            this.state = SqeStreamState::Submitted {
                                idx: this.get_idx()?,
                            };
                            continue;
                        }
                        Err(e) if e.is_retryable() => {
                            // We were unable to register the waker, and submit our IO to local uring.
                            // Yield to scheduler so we can take corrective action and retry later.
                            with_scheduler!(|s| {
                                s.yield_now(cx.waker(), e.as_yield_reason());
                            });

                            return Poll::Pending;
                        }
                        Err(e) => {
                            this.state = SqeStreamState::Completed;

                            if e.is_fatal() {
                                with_scheduler!(|s| {
                                    s.unhandled_panic(e.as_panic_reason());
                                });
                                unreachable!("scheduler should panic");
                            }

                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                SqeStreamState::Submitted { idx } => {
                    return with_slab_mut(|slab| -> Poll<Option<Self::Item>> {
                        let raw_sqe = match slab.get_mut(idx) {
                            Ok(sqe) => sqe,
                            Err(e) => {
                                this.state = SqeStreamState::Completed;
                                return Poll::Ready(Some(Err(anyhow::anyhow!(
                                    "can't find sqe in slab: {:?}",
                                    e
                                )
                                .into())));
                            }
                        };

                        dbg!("found raw sqe looking for next result : {:?}", &raw_sqe);

                        // It is the responsibility of the caller to cancel the
                        // stream if there is a bad result. We don't take responsibility
                        // for analyzing stream errors.
                        match raw_sqe.pop_next_result() {
                            Ok(Some(result)) => {
                                dbg!("got result from stream: {}", result);
                                Poll::Ready(Some(Ok(result)))
                            }
                            Ok(None) => {
                                if matches!(raw_sqe.get_state(), RawSqeState::Completed) {
                                    dbg!("stream completed");
                                    this.state = SqeStreamState::Completed;
                                    Poll::Ready(None)
                                } else {
                                    dbg!("no result yet, pending");
                                    raw_sqe.set_waker(cx.waker());
                                    Poll::Pending
                                }
                            }
                            Err(e) => Poll::Ready(Some(Err(e))),
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

// RAII: free RawSqe from slab.
impl Drop for SqeStream {
    fn drop(&mut self) {
        if let SqeStreamState::Cancelled = self.state {
            // The cancellation task owns the RawSqe now.
            return;
        }

        if let Err(e) = self.get_idx().map(|idx| {
            with_slab_mut(|slab| {
                if slab.try_remove(idx).is_none() {
                    eprintln!("Warning: SQE {} not found in slab during drop", idx);
                }
            })
        }) {
            eprintln!("{:?}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::with_slab_and_ring_mut;
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
        init_local_runtime_and_context(None)?;

        let n = count as usize;

        let timespec = Timespec::new().sec(0).nsec(nsecs);
        let timeout = Timeout::new(&timespec)
            .count(count)
            .flags(TimeoutFlags::MULTISHOT);

        let mut stream = pin!(SqeStream::try_new(timeout.build(), Some(count))?);

        let (waker, waker_data) = mock_waker();
        let mut ctx = Context::from_waker(&waker);

        // Polling once will submit the SQE and set the waker.
        // Polling N more times after this won't change the state.
        for _ in 0..10 {
            let res = stream.as_mut().poll_next(&mut ctx);
            assert!(matches!(res, Poll::Pending));

            assert!(matches!(stream.as_mut().poll_next(&mut ctx), Poll::Pending));
            assert_eq!(waker_data.get_count(), 0);
            assert_eq!(waker_data.get_pending_io(), 1);
        }

        let idx = stream.get_idx()?;
        with_slab_and_ring_mut(|slab, ring| {
            assert_eq!(ring.sq().len(), 1);

            let res = slab.get(idx).and_then(|sqe| {
                assert!(!sqe.is_ready());
                assert!(sqe.has_waker());

                if let CompletionHandler::Stream { completion, .. } = &sqe.handler {
                    assert!(completion.has_more());
                }

                Ok(())
            });

            assert!(res.is_ok());
        });

        with_core_mut(|core| -> Result<()> {
            // Because we use DEFER_TASKRUN, CQE are not automatically posted. Instead,
            // the kernel queues "task_work" everytime a timer fires. We have to
            // repeatedly enter the kernel with `io_uring_enter + GETEVENTS` to convert
            // the `task_work` into CQEs.
            let mut num_awaiting = n;
            while num_awaiting > 0 {
                // Submit our single SQE and wait for CQEs :: `io_uring_enter`
                core.submit_and_wait(num_awaiting, None)?;

                // Wait for our timeouts to have completed N times
                num_awaiting -= core.process_cqes(None)?;
            }

            assert_eq!(num_awaiting, 0);

            // We wake the task N time, and let scheduler handle the Notified
            // state and set the bit appropriately.
            assert_eq!(waker_data.get_count(), count as usize);
            assert_eq!(waker_data.get_pending_io(), 0);

            if let CompletionHandler::Stream {
                results,
                completion,
            } = &core.slab.borrow().get(idx)?.handler
            {
                assert_eq!(results.len(), n);
                assert!(!completion.has_more());
            }

            Ok(())
        })?;

        let mut fired = 0;
        while fired < n {
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

        assert_eq!(fired, n);

        Ok(())
    }
}
