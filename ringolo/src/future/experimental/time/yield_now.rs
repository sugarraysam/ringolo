use crate::runtime::{AddMode, Schedule, YieldReason};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug, Clone, Copy)]
pub struct YieldNow {
    awaiting_first_poll: bool,
    mode: Option<AddMode>,
}

impl YieldNow {
    /// Mechanism through which a task can suspend itself, with the intention
    /// of running again soon without being woken up. Very useful if a task was
    /// unable to register the waker for example, can fallback to yielding and
    /// try to register the waker again.
    ///
    /// By default, the scheduler will figure out the optimal AddMode and decide
    /// if task should be run again next (Lifo :: front of queue) or run again
    /// soon (Fifo :: back of queue). Only override if you have special insights.
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
