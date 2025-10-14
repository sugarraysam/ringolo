use crate as ringolo;
use crate::future::opcode::{Multishot, TimeoutMultishot};
use futures::Stream;
use io_uring::types::CancelBuilder;
use pin_project::{pin_project, pinned_drop};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

#[pin_project(PinnedDrop)]
pub struct Tick {
    #[pin]
    inner: Multishot<TimeoutMultishot>,
}

impl Tick {
    pub fn new(interval: Duration, count: u32) -> Self {
        Self {
            inner: Multishot::new(TimeoutMultishot::new(interval, count, None)),
        }
    }
}

impl Stream for Tick {
    type Item = io::Result<()>;

    #[track_caller]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let res = ready!(this.inner.poll_next(cx));

        match res {
            None => Poll::Ready(None),
            Some(r) => Poll::Ready(Some(r)),
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
    use rstest::rstest;
    use std::time::Duration;

    #[rstest]
    #[case::single(Duration::from_millis(2), 1)]
    #[case::three(Duration::from_millis(2), 3)]
    #[case::five(Duration::from_millis(2), 5)]
    #[ringolo::test]
    async fn test_tick_with_count(#[case] interval: Duration, #[case] count: u32) -> Result<()> {
        let tick = Tick::new(interval, count);
        let ticks = tick.collect::<Vec<_>>().await;

        assert!(ticks.iter().all(Result::is_ok));
        assert_eq!(ticks.len() as u32, count);
        Ok(())
    }

    // TODO:
    // - test Cancelling scenarios
}
