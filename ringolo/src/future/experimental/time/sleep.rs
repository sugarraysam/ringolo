use crate::future::lib::{Op, TimeoutOp};
use pin_project::pin_project;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

#[pin_project]
pub struct Sleep {
    #[pin]
    inner: Op<TimeoutOp>,
}

impl Sleep {
    pub fn new(when: Duration) -> Self {
        Self {
            inner: Op::new(TimeoutOp::new(when)),
        }
    }
}

impl Future for Sleep {
    type Output = io::Result<()>;

    #[track_caller]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match ready!(this.inner.poll(cx)) {
            Ok(_) => Poll::Ready(Ok(())),
            Err(e) if e.raw_os_error() == Some(libc::ETIME) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(e.into())),
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO: add tests
}
