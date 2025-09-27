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

    // TODO: need Pin<&mut Self> ?
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
