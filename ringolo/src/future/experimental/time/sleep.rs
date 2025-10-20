use crate::future::lib::{Op, OpcodeError, Timeout};
use pin_project::pin_project;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

#[pin_project]
pub struct Sleep {
    #[pin]
    inner: Op<Timeout>,
}

impl Sleep {
    pub fn try_new(when: Duration) -> Result<Self, OpcodeError> {
        if when.is_zero() {
            return Err(OpcodeError::SleepZeroDuration);
        }

        Ok(Self {
            inner: Op::new(Timeout::new(when, None)),
        })
    }
}

impl Future for Sleep {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match ready!(this.inner.poll(cx)) {
            Ok(_) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(e.into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as ringolo;
    use anyhow::{Context, Result};
    use rstest::rstest;
    use std::time::{Duration, Instant};

    #[rstest]
    #[case::one_hundred(Duration::from_micros(100))]
    #[case::two_hundred(Duration::from_micros(200))]
    #[case::three_hundred(Duration::from_micros(300))]
    #[ringolo::test]
    async fn test_sleep_duration_is_accurate(#[case] duration: Duration) -> Result<()> {
        let start = Instant::now();
        let sleep = Sleep::try_new(duration).context("cant create sleep")?;

        sleep.await.context("sleep failed")?;
        let elapsed = start.elapsed();

        // The sleep should last for *at least* the specified duration.
        // Due to scheduler latency, it might be slightly longer, but it should never be shorter.
        assert!(
            elapsed >= duration,
            "Sleep was shorter than expected. Elapsed: {:?}, Expected: >= {:?}",
            elapsed,
            duration
        );

        // It shouldn't be excessively long either. We add a generous margin
        // to prevent flaky tests on systems under heavy load.
        let upper_bound = duration + Duration::from_millis(5);
        assert!(
            elapsed < upper_bound,
            "Sleep was much longer than expected. Elapsed: {:?}, Expected: < {:?}",
            elapsed,
            upper_bound
        );

        Ok(())
    }

    #[ringolo::test]
    async fn test_sleep_with_zero_duration_errors() -> Result<()> {
        let res = Sleep::try_new(Duration::ZERO);
        assert!(matches!(res, Err(OpcodeError::SleepZeroDuration)));
        Ok(())
    }
}
