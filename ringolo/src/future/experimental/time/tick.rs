use crate::future::lib::{Multishot, TimeoutMultishot};
use crate::sqe::IoError;
use futures::Stream;
use pin_project::pin_project;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

#[pin_project]
pub struct Tick {
    #[pin]
    inner: Multishot<TimeoutMultishot>,
}

impl Tick {
    /// Create a new `Tick` that will wake up at set interval for the desired
    /// number of times. Very useful to implement background tasks that should
    /// run periodically. It is much more efficient than repeatedly sleeping as
    /// it leverages a `MultishotTimeout` timeout internally which corresponds
    /// to a single Task.
    /// - If `count` is `n`, the tick will fire n times.
    /// - If `count` is `0`, it will fire indefinitely.
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
            Some(r) => Poll::Ready(Some(r.map_err(IoError::into))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{self as ringolo, utils::scheduler::Method};
    use anyhow::Result;
    use futures::StreamExt;
    use rstest::rstest;
    use std::time::Duration;

    #[rstest]
    #[case::single(Duration::from_millis(1), 1)]
    #[case::three(Duration::from_millis(1), 3)]
    #[case::five(Duration::from_millis(1), 5)]
    #[ringolo::test]
    async fn test_tick_with_count(#[case] interval: Duration, #[case] count: u32) -> Result<()> {
        let tick = Tick::new(interval, count);
        let ticks = tick.collect::<Vec<_>>().await;

        assert!(ticks.iter().all(Result::is_ok));
        assert_eq!(ticks.len() as u32, count);
        Ok(())
    }

    #[ringolo::test]
    async fn test_infinite_tick_and_cancellation() -> Result<()> {
        {
            let take_n = 5;

            let tick = Tick::new(Duration::from_micros(100), 0 /* infinite */);
            let ticks = tick.take(take_n).collect::<Vec<_>>().await;

            assert!(ticks.iter().all(Result::is_ok));
            assert_eq!(ticks.len(), take_n);
        } // tick cancelled on drop

        crate::with_scheduler!(|s| {
            let spawn_calls = s.tracker.get_calls(&Method::Spawn);
            assert_eq!(spawn_calls.len(), 1);
        });

        Ok(())
    }
}
