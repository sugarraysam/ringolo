use io_uring::squeue::Entry;
use smallvec::{SmallVec, smallvec};
use std::collections::VecDeque;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::task::Waker;

use crate::sqe::IoError;

#[derive(Debug)]
pub(crate) enum StreamCompletion {
    // The operation completes after a known number of events (e.g., `Timeout`).
    ByCount { remaining: Arc<AtomicU32> },

    // The operation completes when a CQE arrives without the `IORING_CQE_F_MORE` flag.
    ByFlag { done: bool },
}

impl StreamCompletion {
    pub(crate) fn new(count: u32) -> Self {
        if count > 0 {
            StreamCompletion::ByCount {
                remaining: Arc::new(AtomicU32::new(count)),
            }
        } else {
            StreamCompletion::ByFlag { done: false }
        }
    }

    pub(crate) fn has_more(&self) -> bool {
        match self {
            StreamCompletion::ByCount { remaining } => remaining.load(Ordering::Relaxed) > 0,
            StreamCompletion::ByFlag { done } => !*done,
        }
    }
}

// Enum to hold the data that is different for each completion type. RawSqe is
// responsible to implement the logic.
#[derive(Debug)]
pub(crate) enum CompletionHandler {
    Single {
        result: Option<Result<i32>>,
    },
    BatchOrChain {
        result: Option<Result<i32>>,

        // Waker only set on the head, so we store a pointer to the head SQE.
        head: usize,

        // Reference counted counter to track how many SQEs are still pending.
        remaining: Arc<AtomicUsize>,
    },
    Stream {
        // We use the SqeStreamError to be able to distinguish between an IO
        // error or an application error.
        results: VecDeque<anyhow::Result<i32, IoError>>,

        completion: StreamCompletion,
    },
    Message,
}

impl CompletionHandler {
    pub(crate) fn new_single() -> CompletionHandler {
        CompletionHandler::Single { result: None }
    }

    pub(crate) fn new_batch_or_chain(
        head: usize,
        remaining: Arc<AtomicUsize>,
    ) -> CompletionHandler {
        CompletionHandler::BatchOrChain {
            head,
            remaining,
            result: None,
        }
    }

    pub(crate) fn new_stream(count: u32) -> CompletionHandler {
        CompletionHandler::Stream {
            results: VecDeque::new(),
            completion: StreamCompletion::new(count),
        }
    }

    pub(crate) fn new_message() -> CompletionHandler {
        CompletionHandler::Message
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CompletionEffect {
    // We use this fancy way to decrement pending IO on the slab mostly because
    // of MULTISHOT, where 1 SQE can generate N CQEs. We are only allowed to decrement
    // the pending IO counter after receiving the *last CQE* for this stream.
    DecrementPendingIo,
    WakeHead { head: usize },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum RawSqeState {
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
pub(crate) struct RawSqe {
    pub(crate) entry: Option<Entry>,

    pub(crate) waker: Option<Waker>,

    pub(crate) state: RawSqeState,

    pub(crate) handler: CompletionHandler,
}

impl RawSqe {
    pub(crate) fn new(entry: Entry, handler: CompletionHandler) -> Self {
        Self {
            entry: Some(entry),
            handler,
            waker: None,
            state: RawSqeState::Available,
        }
    }

    pub(crate) fn get_entry(&self) -> Result<&Entry> {
        self.entry
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "entry is none"))
    }

    pub(crate) fn take_entry(&mut self) -> Result<Entry> {
        self.entry
            .take()
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "entry is none"))
    }

    pub(crate) fn set_available(&mut self) {
        self.state = RawSqeState::Available;
    }

    pub(crate) fn get_state(&self) -> RawSqeState {
        self.state
    }

    pub(crate) fn has_waker(&self) -> bool {
        self.waker.is_some()
    }

    pub(crate) fn set_waker(&mut self, waker: &Waker) {
        if let Some(waker_ref) = self.waker.as_ref() {
            // No need to override waker if they are related.
            if waker_ref.will_wake(waker) {
                return;
            }
        }

        // Otherwise, clone the new waker and store it. This overwrites any existing one.
        self.waker = Some(waker.clone());
    }

    pub(crate) fn get_waker(&self) -> Result<&Waker> {
        self.waker
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "get_waker failed: waker is none"))
    }

    pub(crate) fn take_waker(&mut self) -> Result<Waker> {
        self.waker
            .take()
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "take_waker failed: waker is none"))
    }

    pub(crate) fn set_user_data(&mut self, user_data: u64) -> Result<()> {
        if !matches!(self.state, RawSqeState::Available) {
            return Err(Error::other(format!("unexpected state: {:?}", self.state)));
        }

        self.state = RawSqeState::Pending;
        self.entry = Some(self.take_entry()?.user_data(user_data));

        Ok(())
    }

    pub(crate) fn on_completion(
        &mut self,
        cqe_res: i32,
        cqe_flags: Option<u32>,
    ) -> Result<SmallVec<[CompletionEffect; 2]>> {
        if !matches!(self.state, RawSqeState::Pending | RawSqeState::Ready) {
            return Err(Error::other(format!("unexpected state: {:?}", self.state)));
        }

        let cqe_res: Result<i32> = if cqe_res >= 0 {
            Ok(cqe_res)
        } else {
            Err(Error::from_raw_os_error(-cqe_res))
        };

        let _prev_state = std::mem::replace(&mut self.state, RawSqeState::Ready);

        match &mut self.handler {
            CompletionHandler::Single { result } => {
                *result = Some(cqe_res);
                self.wake()?;

                Ok(smallvec![CompletionEffect::DecrementPendingIo])
            }
            CompletionHandler::BatchOrChain {
                result,
                head,
                remaining,
            } => {
                *result = Some(cqe_res);
                let count = remaining.fetch_sub(1, Ordering::Relaxed) - 1;

                if count == 0 {
                    Ok(smallvec![
                        CompletionEffect::DecrementPendingIo,
                        CompletionEffect::WakeHead { head: *head },
                    ])
                } else {
                    Ok(smallvec![CompletionEffect::DecrementPendingIo])
                }
            }
            CompletionHandler::Stream {
                results,
                completion,
            } => {
                results.push_back(cqe_res.map_err(IoError::from));

                let done = match completion {
                    StreamCompletion::ByCount { remaining } => {
                        let prev = remaining.fetch_sub(1, Ordering::Relaxed);
                        prev - 1 == 0
                    }
                    StreamCompletion::ByFlag { done } => {
                        // If we have the `IORING_CQE_F_MORE` flags set, it means we are
                        // expecting more results, otherwise this was the final result.
                        *done = cqe_flags.is_some_and(io_uring::cqueue::more);
                        *done
                    }
                };

                // Important to distinguish between final wake and wake_by_ref.
                // This is because we will decrement the pending IO counter on the
                // task when invoking the consuming wake() call.
                if done {
                    self.wake()?;
                    Ok(smallvec![CompletionEffect::DecrementPendingIo])
                } else {
                    self.wake_by_ref()?;
                    Ok(smallvec![])
                }
            }
            CompletionHandler::Message => {
                unimplemented!("RingMessage are not yet implemented.")
            }
        }
    }

    pub(crate) fn pop_next_result(&mut self) -> anyhow::Result<Option<i32>, IoError> {
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

        Err(anyhow::anyhow!(
            "Misused API: pop_next_result called with non-stream handler: {:?}",
            self.handler
        )
        .into())
    }

    pub(crate) fn take_final_result(&mut self) -> Result<i32> {
        if !matches!(self.state, RawSqeState::Ready) {
            return Err(Error::other(format!("unexpected state: {:?}", self.state)));
        }

        let _entry = self.take_entry()?;
        let result = match &mut self.handler {
            CompletionHandler::Single { result } => result.take(),
            CompletionHandler::BatchOrChain { result, .. } => result.take(),
            _ => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    format!(
                        "Misused API: only call with single or batch handlers: {:?}",
                        self.handler
                    ),
                ));
            }
        }
        .ok_or_else(|| Error::new(ErrorKind::NotFound, "no result"))?;

        self.state = RawSqeState::Completed;
        result
    }

    pub(crate) fn is_ready(&self) -> bool {
        matches!(self.state, RawSqeState::Ready)
    }

    pub(crate) fn wake(&mut self) -> Result<()> {
        self.take_waker()?.wake();
        Ok(())
    }

    // Wakes up the task without consuming the waker. Useful for `SqeMore` where
    // we get N cqes for a single SQE. Prefer using consuming `wake()` version
    // when possible as this has a performance cost.
    pub(crate) fn wake_by_ref(&self) -> Result<()> {
        self.get_waker()?.wake_by_ref();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::with_slab_mut;
    use crate::test_utils::*;
    use anyhow::Result;
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
        init_local_runtime_and_context(None)?;
        let user_data = 42;

        let mut sqe = RawSqe::new(nop(), CompletionHandler::new_single());
        sqe.set_user_data(user_data)?;

        let (waker, waker_data) = mock_waker();
        sqe.set_waker(&waker);
        assert_eq!(
            *sqe.on_completion(res, None)?,
            [CompletionEffect::DecrementPendingIo]
        );

        assert!(sqe.is_ready());
        let got = sqe.take_final_result();
        // TODO: rawsqe has userdata?
        // assert_eq!(entry.get_user_data(), user_data);

        // Result and entry consumed
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
        init_local_runtime_and_context(None)?;
        let user_data = 12345;
        let remaining = Arc::new(AtomicUsize::new(n_sqes));

        let head_idx = with_slab_mut(|slab| -> Result<usize> {
            let vacant = slab.vacant_entry()?;
            let head_idx = vacant.key();

            let mut head = RawSqe::new(
                nop(),
                CompletionHandler::new_batch_or_chain(head_idx, Arc::clone(&remaining)),
            );
            head.set_user_data(user_data)?;

            // Not woken up yet, we have `n_sqes - 1` remaining
            assert_eq!(
                *head.on_completion(res, None)?,
                [CompletionEffect::DecrementPendingIo]
            );

            vacant.insert(head);

            Ok(head_idx)
        })?;

        while remaining.load(Ordering::Relaxed) > 0 {
            let mut sqe = RawSqe::new(
                nop(),
                CompletionHandler::new_batch_or_chain(head_idx, Arc::clone(&remaining)),
            );
            sqe.set_user_data(user_data)?;

            let effects = sqe.on_completion(res, None)?;

            // Last SQE to complete triggers completion
            if remaining.load(Ordering::Relaxed) == 0 {
                assert!(matches!(effects[0], CompletionEffect::DecrementPendingIo));
                assert!(matches!(effects[1], CompletionEffect::WakeHead { .. }));
            } else {
                assert_eq!(*effects, [CompletionEffect::DecrementPendingIo]);
            }
        }

        Ok(())
    }

    #[test]
    fn test_raw_sqe_lifecycle() -> Result<()> {
        init_local_runtime_and_context(None)?;

        let raw_sqe = RawSqe::new(nop(), CompletionHandler::new_single());
        assert_eq!(raw_sqe.get_state(), RawSqeState::Available);

        with_slab_mut(|slab| -> Result<()> {
            let (waker, waker_data) = mock_waker();

            let idx = slab.insert(raw_sqe).map(|(idx, inserted)| {
                assert_eq!(inserted.get_state(), RawSqeState::Pending);
                inserted.set_waker(&waker);

                assert!(inserted.on_completion(0, None).is_ok());
                assert_eq!(inserted.get_state(), RawSqeState::Ready);

                assert_eq!(waker_data.get_count(), 1);

                idx
            })?;

            let removed = slab.try_remove(idx);
            assert!(
                removed
                    .map(|sqe| assert_eq!(sqe.get_state(), RawSqeState::Available))
                    .is_some()
            );

            Ok(())
        })
    }
}
