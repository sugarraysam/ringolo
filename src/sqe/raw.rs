use anyhow::{Context, Result, anyhow};
use io_uring::squeue::Entry;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::task::Waker;

#[derive(Debug, Clone)]
pub enum CompletionHandler {
    Single,
    BatchOrChain {
        // Waker only set on the head, so we store a pointer to the head SQE.
        head: usize,

        // Reference counted pointer to track how many SQEs are still pending.
        remaining: Rc<RefCell<usize>>,
    },
    RingMessage,
}

pub struct RawSqe {
    entry: Option<Entry>,

    // A callback to be invoked on completion.
    handler: CompletionHandler,

    result: Option<io::Result<i32>>,

    waker: Option<Waker>,
}

impl RawSqe {
    pub fn new(entry: Entry, handler: CompletionHandler) -> Self {
        Self {
            entry: Some(entry),
            handler,
            result: None,
            waker: None,
        }
    }

    pub fn get_entry(&self) -> Result<&Entry> {
        self.entry.as_ref().ok_or_else(|| anyhow!("Entry is None"))
    }

    pub fn has_waker(&self) -> bool {
        self.waker.is_some()
    }

    pub fn set_waker(&mut self, waker: &Waker) {
        if let Some(waker_ref) = self.waker.as_ref() {
            // No need to override waker if they are related.
            if waker_ref.will_wake(waker) {
                return;
            }
        }

        // Otherwise, clone the new waker and store it. This overwrites any existing one.
        self.waker = Some(waker.clone());
    }

    pub fn set_user_data(&mut self, user_data: u64) -> Result<()> {
        let old = self
            .entry
            .take()
            .with_context(|| anyhow!("Entry is None"))?;

        self.entry = Some(old.user_data(user_data));

        Ok(())
    }

    // Optionally returns a "head" to wake if we're working with BatchOrChain.
    pub fn on_completion(&mut self, res: i32) -> Result<Option<usize>> {
        self.result = if res >= 0 {
            Some(Ok(res))
        } else {
            Some(Err(io::Error::from_raw_os_error(-res)))
        };

        match &self.handler {
            CompletionHandler::Single => self.wake().map(|_| None),
            CompletionHandler::BatchOrChain { head, remaining } => {
                let mut count = remaining.borrow_mut();
                *count -= 1;

                // We completed the last SQE in the batch/chain, wake the head.
                if *count == 0 {
                    Ok(Some(*head))
                } else {
                    Ok(None)
                }
            }
            CompletionHandler::RingMessage => {
                unreachable!("RingMessage are not linked to RawSqe on the completion side.")
            }
        }
    }

    pub fn get_result(&mut self) -> Result<(Entry, io::Result<i32>)> {
        let entry = self.entry.take().context("No entry")?;
        let result = self.result.take().context("No result")?;

        Ok((entry, result))
    }

    pub fn is_ready(&self) -> bool {
        self.result.is_some()
    }

    pub fn wake(&mut self) -> Result<()> {
        Ok(self.waker.take().context("No waker to wake")?.wake())
    }

    // Wakes up the task without consuming the waker. Useful for `SqeMore` where
    // we get N cqes for a single SQE. Prefer using consuming `wake()` version
    // when possible as this has a performance cost.
    pub fn wake_by_ref(&self) -> Result<()> {
        Ok(self
            .waker
            .as_ref()
            .context("No waker to wake")?
            .wake_by_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{init_context, with_context_mut};
    use crate::test_utils::mocks::mock_waker;
    use io_uring::opcode::Nop;
    use rstest::rstest;
    use std::io::{self, ErrorKind};
    use std::sync::atomic::Ordering;

    #[test]
    fn test_raw_sqe_set_waker_logic() -> Result<()> {
        let mut sqe = RawSqe::new(Nop::new().build(), CompletionHandler::Single);

        let (waker1, waker1_data) = mock_waker();

        sqe.set_waker(&waker1);
        sqe.wake_by_ref()?;
        assert_eq!(waker1_data.load(Ordering::Relaxed), 1);

        // Set unrelated waker - should overwrite waker1
        let (waker2, waker2_data) = mock_waker();
        assert_eq!(waker1.will_wake(&waker2), false);

        sqe.set_waker(&waker2);
        sqe.wake_by_ref().unwrap();

        assert_eq!(waker1_data.load(Ordering::Relaxed), 1);
        assert_eq!(waker2_data.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[rstest]
    #[case::positive_result_success(123, Ok(123))]
    #[case::zero_result_success(0, Ok(0))]
    #[case::err_not_found(-2, Err(io::Error::new(ErrorKind::NotFound, "not found")))]
    #[case::err_would_block(-11, Err(io::Error::new(ErrorKind::WouldBlock, "would block")))]
    fn test_raw_sqe_single_completion(
        #[case] res: i32,
        #[case] expected: io::Result<i32>,
    ) -> Result<()> {
        init_context(64);
        let user_data = 42;

        let mut sqe = RawSqe::new(
            Nop::new().build().user_data(user_data),
            CompletionHandler::Single,
        );

        let (waker, waker_data) = mock_waker();
        sqe.set_waker(&waker);
        assert!(sqe.on_completion(res)?.is_none());

        assert!(sqe.is_ready());
        let (entry, got) = sqe.get_result()?;

        // Result and entry consumed
        assert_eq!(entry.get_user_data(), user_data);
        assert!(sqe.entry.is_none());
        assert!(sqe.result.is_none());

        // result matches
        match (got, expected) {
            (Ok(g), Ok(e)) => assert_eq!(g, e),
            (Err(g), Err(e)) => assert_eq!(g.kind(), e.kind()),
            _ => panic!("Result mismatch"),
        }

        // The waker was consumed and called
        assert_eq!(waker_data.load(Ordering::Relaxed), 1);
        assert!(sqe.waker.is_none());

        Ok(())
    }

    #[rstest]
    #[case::all_success_triggers_last(0, 5)]
    #[case::all_errors_triggers_last(-1, 5)]
    fn test_raw_sqe_batch_or_chain_completion(
        #[case] res: i32,
        #[case] n_sqes: usize,
    ) -> Result<()> {
        init_context(64);
        let remaining = Rc::new(RefCell::new(n_sqes));

        let handler = with_context_mut(|ctx| -> Result<CompletionHandler> {
            let vacant = ctx.slab.vacant_entry()?;
            let head_idx = vacant.key();

            let handler = CompletionHandler::BatchOrChain {
                head: head_idx,
                remaining: remaining.clone(),
            };

            let mut head = RawSqe::new(Nop::new().build(), handler.clone());

            // Not woken up yet, we have `n_sqes - 1` remaining
            assert!(head.on_completion(res)?.is_none());

            vacant.insert(head);

            Ok(handler)
        })?;

        while *remaining.borrow() > 0 {
            let mut sqe = RawSqe::new(Nop::new().build(), handler.clone());
            let head_to_wake = sqe.on_completion(res)?;

            // Last SQE to complete triggers completion
            if *remaining.borrow() == 0 {
                assert!(head_to_wake.is_some());
            } else {
                assert!(head_to_wake.is_none());
            }
        }

        Ok(())
    }
}
