use crate::context::{with_core_mut, with_slab_mut};
use crate::sqe::{CompletionHandler, RawSqe, RawSqeState, State, Submittable};
use crate::task::Header;
use anyhow::{Result, anyhow};
use futures::Stream;
use io_uring::squeue::Entry;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
pub struct SqeStream {
    idx: usize,

    state: State,
}

impl SqeStream {
    // If count is set, we will complete the stream after receiving `count` elements.
    // Otherwise, we will keep producing results as long as completions are posted
    // with the `IORING_CQE_F_MORE` flag.
    pub fn try_new(entry: Entry, count: Option<usize>) -> Result<Self> {
        // TODO: add validation for opcode that can support `IORING_CQE_F_MORE`
        // waiting on PR :: opcode::ACCEPT | opcode::RECV | opcode::RECVMSG
        // entry.opcode()
        //
        // Which SQE flags to set?

        let idx = with_slab_mut(|slab| -> Result<usize> {
            let (idx, _) = slab.insert(RawSqe::new(entry, CompletionHandler::new_stream(count)))?;
            Ok(idx)
        })?;

        Ok(Self {
            idx,
            state: State::Initial,
        })
    }

    pub fn get_idx(&self) -> usize {
        self.idx
    }
}

impl Submittable for SqeStream {
    fn submit(&self, waker: &Waker) -> io::Result<i32> {
        unsafe {
            let ptr = NonNull::new_unchecked(waker.data() as *mut Header);
            Header::increment_pending_io(ptr);
        }
        with_core_mut(|core| core.push_sqes(&[self.idx]))
    }
}

// Distinguish between an IO error and an application error.
#[derive(Debug, thiserror::Error)]
pub enum SqeStreamError {
    #[error("An I/O operation failed: {0}")]
    Io(#[from] io::Error),

    #[error("Application error: {0}")]
    Fatal(#[from] anyhow::Error),
}

impl Unpin for SqeStream {}

// SqeStream does not implement Completable trait, as it represents a one-off result.
// Instead, we implement futures::Stream interface, to keep returning results to the
// user as they become available.
impl Stream for SqeStream {
    type Item = Result<i32, SqeStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match this.state {
                State::Initial => {
                    match this.submit(cx.waker()) {
                        Ok(_) => {
                            // Submission successful, advance state and immediately
                            // try to poll for a result.
                            this.state = State::Submitted;
                            continue;
                        }
                        Err(e) if e.kind() == ErrorKind::ResourceBusy => {
                            // TODO: log error this is real bad, need to double SQ ring size
                            // Submission queue is full, yield and try again later.
                            eprintln!("Warning: Submission queue is full, double SQ ring size");
                            return Poll::Pending;
                        }
                        Err(e) => {
                            this.state = State::Completed;
                            return Poll::Ready(Some(Err(
                                anyhow!("failed to submit: {:?}", e).into()
                            )));
                        }
                    }
                }
                State::Submitted => {
                    return with_slab_mut(|slab| -> Poll<Option<Self::Item>> {
                        let raw_sqe = match slab.get_mut(this.idx) {
                            Ok(sqe) => sqe,
                            Err(e) => {
                                this.state = State::Completed;
                                return Poll::Ready(Some(Err(anyhow!(
                                    "can't find sqe in slab: {:?}",
                                    e
                                )
                                .into())));
                            }
                        };

                        match raw_sqe.pop_next_result() {
                            Ok(Some(result)) => Poll::Ready(Some(Ok(result))),
                            Ok(None) => {
                                if matches!(raw_sqe.get_state(), RawSqeState::Completed) {
                                    this.state = State::Completed;
                                    Poll::Ready(None)
                                } else {
                                    raw_sqe.set_waker(cx.waker());
                                    Poll::Pending
                                }
                            }
                            Err(e) => {
                                if matches!(e, SqeStreamError::Fatal(_)) {
                                    this.state = State::Completed;
                                }
                                Poll::Ready(Some(Err(e)))
                            }
                        }
                    });
                }
                State::Completed => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

// RAII: free RawSqe from slab.
impl Drop for SqeStream {
    fn drop(&mut self) {
        with_slab_mut(|slab| {
            if slab.try_remove(self.idx).is_none() {
                eprintln!("Warning: SQE {} not found in slab during drop", self.idx);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::with_slab_and_ring_mut;
    use crate::test_utils::*;
    use io_uring::opcode::Timeout;
    use io_uring::types::{TimeoutFlags, Timespec};
    use rstest::rstest;
    use std::pin::pin;
    use std::task::{Context, Poll};

    #[ignore = "Broken since moving to DEFER TASK RUN - using timeout wrong?"]
    #[rstest]
    #[case::single_timeout(1, 1_000)]
    // #[case::ten_timeouts(10, 1_000)]
    // #[case::two_timeouts_slow(2, 1_000_000)]
    fn test_sqe_stream_timeout_multishot(#[case] count: u32, #[case] nsecs: u32) -> Result<()> {
        init_local_runtime_and_context(None)?;

        let n = count as usize;

        let timespec = Timespec::new().sec(0).nsec(nsecs);
        let timeout = Timeout::new(&timespec)
            .count(count)
            .flags(TimeoutFlags::ETIME_SUCCESS | TimeoutFlags::MULTISHOT);

        let mut stream = pin!(SqeStream::try_new(timeout.build(), Some(n))?);
        let idx = stream.get_idx();

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
        }

        with_core_mut(|core| -> Result<()> {
            // Submit SQEs and wait for CQEs :: `io_uring_enter`
            let n_ready = core.submit_and_wait(n, None)?;
            assert_eq!(n_ready, n);

            dbg!("process cqes");
            // Wait for our timeouts to have completed N times
            let num_completed = core.process_cqes(Some(n))?;
            assert_eq!(num_completed, n);

            dbg!("done process cqes");
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

        dbg!("consume stream");
        let mut fired = 0;
        while fired < n {
            match stream.as_mut().poll_next(&mut ctx) {
                Poll::Ready(Some(Err(SqeStreamError::Io(e)))) => {
                    assert_eq!(e.raw_os_error().unwrap(), 62);
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
