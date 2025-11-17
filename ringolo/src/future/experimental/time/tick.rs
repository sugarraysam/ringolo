use crate::future::lib::Multishot;
use crate::future::lib::ops::TimeoutMultishot;
use crate::sqe::IoError;
use futures::Stream;
use pin_project::pin_project;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

/// A stream that yields events at a fixed interval.
///
/// This primitive creates a `Tick` that wakes up at a set interval for a desired
/// number of times. It is designed for implementing efficient background tasks
/// that need to run periodically.
///
/// # Efficiency via Multishot
///
/// `Tick` is significantly more efficient than a standard `loop { sleep().await; }` pattern.
/// Internally, it leverages `io_uring`'s **multishot timeout** feature.
///
/// Instead of submitting a new kernel request for every interval (which incurs
/// syscall overhead and context switching), `Tick` performs **one** initial submission.
/// The kernel then automatically posts completion events to the ring at the specified
/// cadence without further intervention from user space.
///
/// # Behavior
///
/// * If `count` is `n`, the tick will fire exactly `n` times.
/// * If `count` is `0`, the tick will fire indefinitely until dropped.
///
/// # Examples
///
/// ```
/// use ringolo::time::Tick;
/// use futures::StreamExt; // for .next()
/// use std::time::Duration;
///
/// # async fn doc() {
/// // Fire every 100ms, forever (count = 0)
/// let mut interval = Tick::new(Duration::from_millis(100), 0);
///
/// while let Some(_) = interval.next().await {
///     println!("Tick!");
/// }
/// # }
/// ```
#[derive(Debug)]
#[pin_project]
pub struct Tick {
    #[pin]
    inner: Multishot<TimeoutMultishot>,
}

impl Tick {
    /// Create a new `Tick` stream.
    ///
    /// * `interval`: The duration between ticks.
    /// * `count`: The number of ticks to generate. If `0`, the stream is infinite.
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
    use crate as ringolo;
    use crate::test_utils::*;
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

            crate::context::with_slab(|slab| assert_eq!(slab.len(), 1));
        } // tick cancelled on drop

        assert_inflight_cleanup(1);
        wait_for_cleanup().await;
        assert_inflight_cleanup(0);

        crate::context::with_slab(|slab| assert_eq!(slab.len(), 0));

        Ok(())
    }
}
