use crate::runtime::{AddMode, Schedule, YieldReason};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A future that yields execution back to the scheduler.
///
/// # Cooperative Multitasking
///
/// `ringolo` uses cooperative scheduling. If a task runs a long computation
/// without awaiting any I/O, it can starve other tasks on the same thread.
/// `YieldNow` allows a task to voluntarily suspend itself, giving the scheduler
/// a chance to run other pending tasks before returning to this one.
///
/// # Examples
///
/// ```
/// use ringolo::time::YieldNow;
///
/// # async fn doc() {
/// for i in 0..1_000_000 {
///     // Heavy computation...
///
///     if i % 100 == 0 {
///         // Let other tasks run every 100 iterations
///         let res = YieldNow::new(None).await;
///         assert!(res.is_ok());
///     }
/// }
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct YieldNow {
    awaiting_first_poll: bool,
    mode: Option<AddMode>,
}

impl YieldNow {
    /// Creates a new `YieldNow` future.
    ///
    /// * `mode`: Optionally specify where to place the task in the run queue.
    ///     * `None`: The scheduler decides (default).
    ///     * `Some(AddMode::Lifo)`: Reschedule at the front (run again ASAP).
    ///     * `Some(AddMode::Fifo)`: Reschedule at the back (run after others).
    pub fn new(mode: Option<AddMode>) -> Self {
        Self {
            awaiting_first_poll: true,
            mode,
        }
    }
}

impl Future for YieldNow {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Only yield on first poll. Next time task is scheduled we will return.
        if self.awaiting_first_poll {
            self.awaiting_first_poll = false;

            crate::with_scheduler!(|s| {
                s.yield_now(cx.waker(), YieldReason::SelfYielded, self.mode)
            });

            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as ringolo;
    use crate::utils::scheduler::{Call, Method};
    use anyhow::{Context, Result};
    use rstest::rstest;

    #[rstest]
    #[case::fifo(AddMode::Fifo)]
    #[case::lifo(AddMode::Lifo)]
    #[ringolo::test]
    async fn test_yield_now_with_mode(#[case] expected_mode: AddMode) -> Result<()> {
        YieldNow::new(Some(expected_mode))
            .await
            .context("failed to yield to scheduler")?;

        // Scheduler assertion
        crate::with_scheduler!(|s| {
            let yield_calls = s.tracker.get_calls(&Method::YieldNow);
            assert_eq!(yield_calls.len(), 1);
            assert!(matches!(
                yield_calls[0],
                Call::YieldNow {
                    reason: YieldReason::SelfYielded,
                    mode
                } if mode == Some(expected_mode)
            ));
        });

        Ok(())
    }
}
