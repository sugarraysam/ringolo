use anyhow::{Context, Result, anyhow};
use io_uring::squeue::Entry;
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;

use crate::sqe::stream::SqeStreamError;

#[derive(Debug)]
pub enum StreamCompletion {
    // The operation completes after a known number of events (e.g., `Timeout`).
    ByCount { remaining: Arc<AtomicUsize> },

    // The operation completes when a CQE arrives without the `IORING_CQE_F_MORE` flag.
    ByFlag { done: bool },
}

impl StreamCompletion {
    pub fn new(count: Option<usize>) -> Self {
        if let Some(count) = count {
            StreamCompletion::ByCount {
                remaining: Arc::new(AtomicUsize::new(count)),
            }
        } else {
            StreamCompletion::ByFlag { done: false }
        }
    }

    pub fn has_more(&self) -> bool {
        match self {
            StreamCompletion::ByCount { remaining } => remaining.load(Ordering::Relaxed) > 0,
            StreamCompletion::ByFlag { done } => !*done,
        }
    }
}

// Enum to hold the data that is different for each completion type. RawSqe is
// responsible to implement the logic.
#[derive(Debug)]
pub enum CompletionHandler {
    Single {
        result: Option<io::Result<i32>>,
    },
    BatchOrChain {
        result: Option<io::Result<i32>>,

        // Waker only set on the head, so we store a pointer to the head SQE.
        head: usize,

        // Reference counted counter to track how many SQEs are still pending.
        remaining: Arc<AtomicUsize>,
    },
    Stream {
        // We use the SqeStreamError to be able to distinguish between an IO
        // error or an application error.
        results: VecDeque<Result<i32, SqeStreamError>>,

        completion: StreamCompletion,
    },
    Message,
}

impl CompletionHandler {
    pub fn new_single() -> CompletionHandler {
        CompletionHandler::Single { result: None }
    }

    pub fn new_batch_or_chain(head: usize, remaining: Arc<AtomicUsize>) -> CompletionHandler {
        CompletionHandler::BatchOrChain {
            head,
            remaining,
            result: None,
        }
    }

    pub fn new_stream(count: Option<usize>) -> CompletionHandler {
        CompletionHandler::Stream {
            results: VecDeque::new(),
            completion: StreamCompletion::new(count),
        }
    }

    pub fn new_message() -> CompletionHandler {
        CompletionHandler::Message
    }
}

#[derive(Debug, Clone)]
pub enum CompletionEffect {
    None,
    WakeHead { head: usize },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RawSqeState {
    // Up for grabs
    Available,

    // Waiting to be submitted and completed
    Pending,

    // At least one result is ready to be consumed
    Ready,

    // Operation is completed and all results were consumed.
    Completed,
}

#[derive(Debug)]
pub struct RawSqe {
    pub entry: Option<Entry>,

    pub waker: Option<Waker>,

    pub state: RawSqeState,

    pub handler: CompletionHandler,
}

impl RawSqe {
    pub fn new(entry: Entry, handler: CompletionHandler) -> Self {
        Self {
            entry: Some(entry),
            handler,
            waker: None,
            state: RawSqeState::Available,
        }
    }

    pub fn get_entry(&self) -> Result<&Entry> {
        self.entry.as_ref().ok_or_else(|| anyhow!("Entry is None"))
    }

    pub fn set_available(&mut self) {
        self.state = RawSqeState::Available;
    }

    pub fn get_state(&self) -> RawSqeState {
        self.state
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
        if !matches!(self.state, RawSqeState::Available) {
            return Err(anyhow!("unexpected state {:?}", self.state));
        }

        let old = self
            .entry
            .take()
            .with_context(|| anyhow!("Entry is None"))?;

        self.state = RawSqeState::Pending;
        self.entry = Some(old.user_data(user_data));

        Ok(())
    }

    // Optionally returns a "head" to wake if we're working with BatchOrChain.
    pub fn on_completion(
        &mut self,
        cqe_res: i32,
        cqe_flags: Option<u32>,
    ) -> Result<CompletionEffect> {
        if !matches!(self.state, RawSqeState::Pending | RawSqeState::Ready) {
            return Err(anyhow!("unexpected state: {:?}", self.state));
        }

        let cqe_res: io::Result<i32> = if cqe_res >= 0 {
            Ok(cqe_res)
        } else {
            Err(io::Error::from_raw_os_error(-cqe_res))
        };

        let _prev_state = std::mem::replace(&mut self.state, RawSqeState::Ready);

        match &mut self.handler {
            CompletionHandler::Single { result } => {
                *result = Some(cqe_res);
                self.wake()?;

                Ok(CompletionEffect::None)
            }
            CompletionHandler::BatchOrChain {
                result,
                head,
                remaining,
            } => {
                *result = Some(cqe_res);
                let count = remaining.fetch_sub(1, Ordering::Relaxed) - 1;
                if count == 0 {
                    Ok(CompletionEffect::WakeHead { head: *head })
                } else {
                    Ok(CompletionEffect::None)
                }
            }
            CompletionHandler::Stream {
                results,
                completion,
            } => {
                results.push_back(cqe_res.map_err(SqeStreamError::Io));

                let done = match completion {
                    StreamCompletion::ByCount { remaining } => {
                        let prev = remaining.fetch_sub(1, Ordering::Relaxed) - 1;
                        prev == 0
                    }
                    StreamCompletion::ByFlag { done } => {
                        // If we have the `IORING_CQE_F_MORE` flags set, it means we are
                        // expecting more results, otherwise this was the final result.
                        *done = cqe_flags.map_or(false, io_uring::cqueue::more);
                        *done
                    }
                };

                // Important to distinguish between final wake and wake_by_ref.
                // This is because we will decrement the pending IO counter on the
                // task when invoking the consuming wake() call.
                if done {
                    self.wake()?;
                } else {
                    self.wake_by_ref()?;
                }

                Ok(CompletionEffect::None)
            }
            CompletionHandler::Message => {
                unimplemented!("RingMessage are not yet implemented.")
            }
        }
    }

    pub fn pop_next_result(&mut self) -> Result<Option<i32>, SqeStreamError> {
        if matches!(self.state, RawSqeState::Pending) {
            return Ok(None);
        }

        if let CompletionHandler::Stream {
            results,
            completion,
        } = &mut self.handler
        {
            let next = results.pop_front();

            self.state = match (results.is_empty(), completion.has_more()) {
                // Buffer is not empty, so there are more items ready. Stay in the Ready state
                // regardless of whether the stream is 'done' or not.
                (false, _) => RawSqeState::Ready,

                // Buffer is empty and we are not expecting more results.
                (true, false) => RawSqeState::Completed,

                // Buffer is now empty, but we are expecting more results.
                (true, true) => RawSqeState::Pending,
            };

            return next.transpose();
        }

        Err(anyhow!(
            "Misused API: pop_next_result called with non-stream handler: {:?}",
            self.handler
        )
        .into())
    }

    pub fn take_final_result(&mut self) -> Result<(Entry, io::Result<i32>)> {
        if !matches!(self.state, RawSqeState::Ready) {
            return Err(anyhow!("unexpected state: {:?}", self.state));
        }

        let entry = self.entry.take().context("No entry")?;
        let result = match &mut self.handler {
            CompletionHandler::Single { result } => result.take(),
            CompletionHandler::BatchOrChain { result, .. } => result.take(),
            _ => {
                return Err(anyhow!(
                    "Misused API: only call with single or batch handlers: {:?}",
                    self.handler
                ));
            }
        }
        .context("No result")?;

        self.state = RawSqeState::Completed;

        Ok((entry, result))
    }

    pub fn is_ready(&self) -> bool {
        matches!(self.state, RawSqeState::Ready)
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
    use crate::test_utils::*;
    use rstest::rstest;
    use std::io::{self, ErrorKind};
    use std::sync::atomic::Ordering;

    #[test]
    fn test_raw_sqe_set_waker_logic() -> Result<()> {
        let mut sqe = RawSqe::new(nop(), CompletionHandler::new_single());

        let (waker1, waker1_data) = mock_waker();

        sqe.set_waker(&waker1);
        sqe.wake_by_ref()?;
        assert_eq!(waker1_data.get_count(), 1);

        // Set unrelated waker - should overwrite waker1
        let (waker2, waker2_data) = mock_waker();
        assert_eq!(waker1.will_wake(&waker2), false);

        sqe.set_waker(&waker2);
        sqe.wake_by_ref().unwrap();

        assert_eq!(waker1_data.get_count(), 1);
        assert_eq!(waker2_data.get_count(), 1);
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

        let mut sqe = RawSqe::new(nop(), CompletionHandler::new_single());
        sqe.set_user_data(user_data)?;

        let (waker, waker_data) = mock_waker();
        sqe.set_waker(&waker);
        assert!(matches!(
            sqe.on_completion(res, None)?,
            CompletionEffect::None
        ));

        assert!(sqe.is_ready());
        let (entry, got) = sqe.take_final_result()?;

        // Result and entry consumed
        assert_eq!(entry.get_user_data(), user_data);
        assert!(sqe.entry.is_none());
        assert!(matches!(sqe.state, RawSqeState::Completed));

        // result matches
        match (got, expected) {
            (Ok(g), Ok(e)) => assert_eq!(g, e),
            (Err(g), Err(e)) => assert_eq!(g.kind(), e.kind()),
            _ => panic!("Result mismatch"),
        }

        // The waker was consumed and called
        assert_eq!(waker_data.get_count(), 1);
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
        let user_data = 12345;
        let remaining = Arc::new(AtomicUsize::new(n_sqes));

        let head_idx = with_context_mut(|ctx| -> Result<usize> {
            let vacant = ctx.slab.vacant_entry()?;
            let head_idx = vacant.key();

            let mut head = RawSqe::new(
                nop(),
                CompletionHandler::new_batch_or_chain(head_idx, Arc::clone(&remaining)),
            );
            head.set_user_data(user_data)?;

            // Not woken up yet, we have `n_sqes - 1` remaining
            assert!(matches!(
                head.on_completion(res, None)?,
                CompletionEffect::None
            ));

            vacant.insert(head);

            Ok(head_idx)
        })?;

        while remaining.load(Ordering::Relaxed) > 0 {
            let mut sqe = RawSqe::new(
                nop(),
                CompletionHandler::new_batch_or_chain(head_idx, Arc::clone(&remaining)),
            );
            sqe.set_user_data(user_data)?;

            let effect = sqe.on_completion(res, None)?;

            // Last SQE to complete triggers completion
            if remaining.load(Ordering::Relaxed) == 0 {
                assert!(matches!(effect, CompletionEffect::WakeHead { .. }));
            } else {
                assert!(matches!(effect, CompletionEffect::None));
            }
        }

        Ok(())
    }

    #[test]
    fn test_raw_sqe_lifecycle() -> Result<()> {
        init_context(64);

        let raw_sqe = RawSqe::new(nop(), CompletionHandler::new_single());
        assert_eq!(raw_sqe.get_state(), RawSqeState::Available);

        with_context_mut(|ctx| -> Result<()> {
            let (waker, waker_data) = mock_waker();

            let idx = ctx.slab.insert(raw_sqe).map(|(idx, inserted)| {
                assert_eq!(inserted.get_state(), RawSqeState::Pending);
                inserted.set_waker(&waker);

                assert!(inserted.on_completion(0, None).is_ok());
                assert_eq!(inserted.get_state(), RawSqeState::Ready);

                assert_eq!(waker_data.get_count(), 1);

                idx
            })?;

            let removed = ctx.slab.try_remove(idx);
            assert!(
                removed
                    .map(|sqe| assert_eq!(sqe.get_state(), RawSqeState::Available))
                    .is_some()
            );

            Ok(())
        })
    }
}
