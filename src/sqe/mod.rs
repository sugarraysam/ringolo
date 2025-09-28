use anyhow::{Result, anyhow};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

// Re-exports
pub mod list;
pub use list::{SqeBatchBuilder, SqeChainBuilder, SqeList, SqeListKind};
pub mod message;
pub use message::SqeRingMessage;
pub mod raw;
pub use raw::CompletionHandler;
pub use raw::RawSqe;
pub mod single;
pub use single::SqeSingle;

pub trait Submittable {
    fn submit(&self) -> io::Result<i32>;
}

pub trait Completable {
    type Output;

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
}
impl<T: Submittable + Completable> Unpin for Sqe<T> {}

impl<E, T: Submittable + Completable<Output = Result<E>> + Send> Future for Sqe<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match this.state {
                State::Initial => {
                    if let Err(e) = this.inner.submit() {
                        if e.kind() == io::ErrorKind::ResourceBusy {
                            // TODO: log error this is real bad, need to double SQ ring size
                            // Submission queue is full, yield and try again later.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{init_context, with_context_mut};
    use crate::test_utils::mocks::mock_waker;
    use io_uring::opcode::Nop;
    use std::pin::pin;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll};

    #[test]
    fn test_submit_and_complete_single_sqe() -> Result<()> {
        init_context(64);

        let sqe = SqeSingle::try_new(Nop::new().build())?;
        let idx = sqe.get_idx();
        let mut sqe_fut = pin!(Sqe::new(sqe));

        let (waker, waker_data) = mock_waker();
        let mut ctx = Context::from_waker(&waker);

        // Polling once will submit the SQE and set the waker.
        // Polling N more times after this won't change the state.
        for _ in 0..10 {
            assert!(matches!(sqe_fut.as_mut().poll(&mut ctx), Poll::Pending));
            assert_eq!(waker_data.load(Ordering::Relaxed), 0);

            with_context_mut(|ctx| {
                assert_eq!(ctx.get_ring_mut().submission().len(), 1);

                let res = ctx.get_slab().get(idx).and_then(|sqe| {
                    assert!(!sqe.is_ready());
                    assert!(sqe.has_waker());
                    Ok(())
                });

                assert!(res.is_ok());
            });
        }

        with_context_mut(|ctx| {
            // Submit SQEs and wait for CQEs :: `io_uring_enter`
            assert!(matches!(ctx.submit_and_wait_timeout(1, None), Ok(1)));
            assert_eq!(waker_data.load(Ordering::Relaxed), 0);

            // Process CQEs :: wakes up Waker
            assert!(matches!(ctx.process_cqes(None), Ok(1)));
            assert_eq!(waker_data.load(Ordering::Relaxed), 1);
        });

        if let Poll::Ready(Ok((entry, result))) = sqe_fut.as_mut().poll(&mut ctx) {
            assert!(matches!(result, Ok(0)));
            assert_eq!(entry.get_user_data(), idx as u64);
        } else {
            assert!(false, "Expected Poll::Ready(Ok((entry, result)))");
        }

        Ok(())
    }
}
