use crate as ringolo;
use crate::sqe::{IoError, SqeStream};
use anyhow::Result;
use futures::Stream;
use io_uring::opcode::Timeout;
use io_uring::types::{CancelBuilder, TimeoutFlags, Timespec};
use pin_project::{pin_project, pinned_drop};
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

#[pin_project(PinnedDrop)]
pub struct Tick {
    #[pin]
    inner: SqeStream,

    // TODO: we need to keep the Timespec alive as we pass an addr to the kernel.
    // If we drop
    timespec: Box<Timespec>,
}

impl Tick {
    pub fn try_new(interval: Duration, count: Option<u32>) -> Result<Self> {
        let timespec = Box::new(Timespec::from(interval));

        let timeout = Timeout::new(&*timespec as *const Timespec)
            .count(count.unwrap_or(0))
            .flags(TimeoutFlags::MULTISHOT)
            .build();

        Ok(Self {
            inner: SqeStream::try_new(timeout, count)?,
            timespec,
        })
    }
}

impl Stream for Tick {
    type Item = ();

    #[track_caller]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let res = ready!(this.inner.poll_next(cx));

        // Stream successfully completed.
        if res.is_none() {
            return Poll::Ready(None);
        }

        // Safety: we just checked for none
        match res.unwrap() {
            Ok(_) => Poll::Ready(Some(())),

            // -62 ETIME is a success case as the kernel is telling us the
            // timeout occurred and triggered the completion event.
            Err(IoError::Io(e)) if e.raw_os_error() == Some(libc::ETIME) => Poll::Ready(Some(())),

            Err(e) => panic!("Unexpected tick error: {:?}", e),
        }
    }
}

// TODO:
// - if cancel fails, should provide a signal, cancellation is async in nature, but we want to know
//   if we are leaking resources. If many multishot fail to get cancelled its really bad.
#[pinned_drop]
impl PinnedDrop for Tick {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();

        let user_data = match this.inner.cancel() {
            Some(idx) => idx,
            None => {
                // Nothing to cancel.
                return;
            }
        };

        // TODO: OnCancelError::{Ignore, Panic} struct to choose from
        // - if fixed count we dont care just ignore
        // - if infinite then panic as we leak resources
        let builder = CancelBuilder::user_data(user_data as u64).all();
        ringolo::spawn_cancel(builder, user_data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as ringolo;
    use anyhow::Result;
    use futures::StreamExt;
    use std::time::Duration;

    // TODO:
    // - test Cancelling scenarios
    #[ringolo::test]
    async fn test_tick_with_count() -> Result<()> {
        let tick = Tick::try_new(Duration::from_nanos(1), Some(3))?;
        assert_eq!(tick.collect::<Vec<_>>().await.len(), 3);
        Ok(())
    }
}
