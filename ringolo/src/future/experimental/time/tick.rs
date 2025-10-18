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

    // TODO: test safe cancellation
}
