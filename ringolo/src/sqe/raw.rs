use crate::task::Header;
use crate::utils::io_uring::CompletionFlags;
use std::collections::VecDeque;
use std::io::{Error, ErrorKind, Result};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::task::Waker;

use crate::context;
use crate::sqe::IoError;

/// The raw state stored inside the `RawSqeSlab`.
///
/// This struct acts as the "glue" between the `io_uring` user_data pointer
/// and the Rust task system. When a CQE arrives, the `user_data` field is cast
/// to an index into the slab to find this struct.
///
/// It holds the `Waker` needed to notify the high-level `Future` that the
/// kernel operation has completed.
#[derive(Debug)]
pub(crate) struct RawSqe {
    pub(crate) waker: Option<Waker>,

    pub(crate) state: RawSqeState,

    pub(crate) handler: CompletionHandler,

    // If Some, this points to the specific task Header that owns this IO.
    // If None, this IO belongs to the root future.
    pub(crate) task_header: Option<NonNull<Header>>,
}

impl Default for RawSqe {
    fn default() -> Self {
        Self {
            waker: None,
            state: RawSqeState::Reserved,
            handler: CompletionHandler::new_single(),
            task_header: None,
        }
    }
}

impl RawSqe {
    pub(crate) fn new(waker: &Waker, handler: CompletionHandler) -> Self {
        let task_header = context::with_core(|core| {
            core.increment_pending_ios();

            if !core.is_polling_root() {
                // Safety: Don't track `pending_ios` for the root_future. The reason is because it
                // *is not backed* by a regular task and is never stealable by other threads.
                unsafe {
                    let ptr = NonNull::new_unchecked(waker.data() as *mut Header);
                    Header::increment_pending_ios(ptr);
                    Some(ptr)
                }
            } else {
                None
            }
        });

        Self {
            handler,
            waker: Some(waker.clone()),
            state: RawSqeState::Pending,
            task_header,
        }
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

    // Consumes the waker and wakes the task.
    // This corresponds to `Waker::wake(self)`.
    pub(crate) fn wake_by_val(&mut self) -> Result<()> {
        let waker = self
            .waker
            .take()
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "wake_by_val failed: waker is none"))?;
        waker.wake();
        Ok(())
    }

    // Wakes up the task without consuming the waker. Useful for `SqeMore` where
    // we get N cqes for a single SQE. Prefer using consuming `wake()` version
    // when possible as this has a performance cost.
    pub(crate) fn wake_by_ref(&self) -> Result<()> {
        self.waker
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "wake_by_ref failed: waker is none"))?
            .wake_by_ref();

        Ok(())
    }

    pub(crate) fn on_completion(
        &mut self,
        cqe_res: i32,
        cqe_flags: CompletionFlags,
    ) -> Result<Option<CompletionEffect>> {
        if !matches!(self.state, RawSqeState::Pending | RawSqeState::Ready) {
            return Err(Error::other(format!("unexpected state: {:?}", self.state)));
        }

        let cqe_res: Result<CqeRes> = if cqe_res >= 0 {
            Ok(CqeRes::new(cqe_res, cqe_flags))
        } else {
            Err(Error::from_raw_os_error(-cqe_res))
        };

        let _prev_state = std::mem::replace(&mut self.state, RawSqeState::Ready);

        match &mut self.handler {
            CompletionHandler::Single { result } => {
                *result = Some(cqe_res);
                self.wake_by_val()?;

                Ok(None)
            }
            CompletionHandler::BatchOrChain {
                result,
                head,
                remaining,
            } => {
                *result = Some(cqe_res);
                let count = remaining.fetch_sub(1, Ordering::Relaxed) - 1;

                if count == 0 {
                    Ok(Some(CompletionEffect::WakeHead { head: *head }))
                } else {
                    Ok(None)
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
                        let has_more = cqe_flags.contains(CompletionFlags::MORE);
                        *done = !has_more;
                        *done
                    }
                };

                if done {
                    self.wake_by_val()?;
                } else {
                    self.wake_by_ref()?;
                }

                Ok(None)
            }
        }
    }

    pub(crate) fn pop_next_result(&mut self) -> anyhow::Result<Option<CqeRes>, IoError> {
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

    pub(crate) fn take_final_result(&mut self) -> Result<CqeRes> {
        if !matches!(self.state, RawSqeState::Ready) {
            return Err(Error::other(format!("unexpected state: {:?}", self.state)));
        }

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

    pub(crate) fn is_completed(&self) -> bool {
        matches!(self.state, RawSqeState::Completed)
    }

    pub(crate) fn cancel(&mut self) -> bool {
        // Idempotency: Ignore if already processed or cancelled.
        if matches!(
            self.state,
            RawSqeState::Reserved | RawSqeState::Cancelled | RawSqeState::Completed
        ) {
            return false;
        }

        self.state = RawSqeState::Cancelled;

        // Release task reference immediately so the Task can complete/drop.
        if let Some(header_ptr) = self.task_header {
            unsafe {
                Header::decrement_pending_ios(header_ptr);
            }
        }

        true
    }
}

impl Drop for RawSqe {
    fn drop(&mut self) {
        if matches!(self.state, RawSqeState::Reserved) {
            // Do nothing - RawSqe was never promoted beyond reserved while using
            // reservation API of the slab.
            return;
        }

        // Decrement core IOs (ignore failure if TLS is tearing down during
        // thread exit).
        context::try_with_core(|core| {
            core.decrement_pending_ios();
        });

        // Decrement task pending IOs unless cancelled (cancelled IOs are
        // handled by the maintenance task).
        if !matches!(self.state, RawSqeState::Cancelled)
            && let Some(header_ptr) = self.task_header
        {
            // Safety: see comment in constructor.
            unsafe {
                Header::decrement_pending_ios(header_ptr);
            }
        }
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CqeRes {
    pub(crate) res: i32,
    pub(crate) flags: CompletionFlags,
}

impl CqeRes {
    pub fn new(res: i32, flags: CompletionFlags) -> Self {
        Self { res, flags }
    }

    pub fn with_res(mut self, res: i32) -> Self {
        self.res = res;
        self
    }

    pub fn with_flags(mut self, flags: CompletionFlags) -> Self {
        self.flags = flags;
        self
    }
}

impl Default for CqeRes {
    fn default() -> Self {
        Self {
            res: 0,
            flags: CompletionFlags::empty(),
        }
    }
}

// Enum to hold the data that is different for each completion type. RawSqe is
// responsible to implement the logic.
#[derive(Debug)]
pub(crate) enum CompletionHandler {
    Single {
        result: Option<Result<CqeRes>>,
    },
    BatchOrChain {
        result: Option<Result<CqeRes>>,

        // Waker only set on the head, so we store a pointer to the head SQE.
        head: usize,

        // Reference counted counter to track how many SQEs are still pending.
        remaining: Arc<AtomicUsize>,
    },
    Stream {
        // We use the SqeStreamError to be able to distinguish between an IO
        // error or an application error.
        results: VecDeque<anyhow::Result<CqeRes, IoError>>,

        completion: StreamCompletion,
    },
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
            // Make sure to store results on the heap. CompletionHandler enum is as large as
            // the largest variant so if we were to use a SmallVec here, the size of every
            // RawSqe in the slab would be bound by the size of CompletionHandler::Stream.
            results: VecDeque::new(),
            completion: StreamCompletion::new(count),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CompletionEffect {
    WakeHead { head: usize },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum RawSqeState {
    // Initial state for the reservation API.
    Reserved,

    // Waiting to be submitted and completed
    Pending,

    // At least one result is ready to be consumed
    Ready,

    // Operation is completed and all results were consumed.
    Completed,

    // Cancelled before it could complete. Used by infinite streams to prevent
    // future completions. Cancellation involves the maintenance task so we use
    // this signal in the Drop impl to prevent decrementing IO on the wrong thread.
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context;
    use crate::test_utils::*;
    use anyhow::Result;
    use rstest::rstest;
    use smallvec::SmallVec;
    use std::io::{self, ErrorKind};
    use std::sync::atomic::Ordering;

    #[rstest]
    #[case::positive_result_success(123, Ok(CqeRes::default().with_res(123)))]
    #[case::zero_result_success(0, Ok(CqeRes::default()))]
    #[case::err_not_found(-2, Err(io::Error::new(ErrorKind::NotFound, "not found")))]
    #[case::err_would_block(-11, Err(io::Error::new(ErrorKind::WouldBlock, "would block")))]
    fn test_raw_sqe_single_completion(
        #[case] res: i32,
        #[case] expected: io::Result<CqeRes>,
    ) -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;
        let (waker, waker_data) = mock_waker();

        let mut sqe = RawSqe::new(&waker, CompletionHandler::new_single());

        assert!(sqe.on_completion(res, CompletionFlags::empty())?.is_none());
        assert!(sqe.is_ready());
        assert_eq!(waker_data.get_count(), 1);
        let got = sqe.take_final_result();

        // Result and entry consumed
        assert!(matches!(sqe.state, RawSqeState::Completed));

        // result matches
        match (got, expected) {
            (Ok(g), Ok(e)) => assert_eq!(g, e),
            (Err(g), Err(e)) => assert_eq!(g.kind(), e.kind()),
            _ => panic!("Result mismatch"),
        }

        drop(sqe);
        assert_eq!(waker_data.get_pending_ios(), 0);
        assert_eq!(context::with_core(|core| core.get_pending_ios()), 0);

        Ok(())
    }

    #[rstest]
    #[case::all_success_triggers_last(0, 5)]
    #[case::all_errors_triggers_last(-1, 5)]
    fn test_raw_sqe_batch_or_chain_completion(
        #[case] res: i32,
        #[case] n_sqes: usize,
    ) -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;
        let (waker, waker_data) = mock_waker();

        let remaining = Arc::new(AtomicUsize::new(n_sqes));

        context::with_slab_mut(|slab| -> Result<()> {
            let batch = slab.reserve_batch(n_sqes)?;
            let indices = batch.keys();
            let head_idx = indices[0];

            let entries = (0..n_sqes)
                .map(|_| {
                    let raw = RawSqe::new(
                        &waker,
                        CompletionHandler::new_batch_or_chain(head_idx, Arc::clone(&remaining)),
                    );
                    Ok(raw)
                })
                .collect::<Result<SmallVec<_>>>()?;

            let _ = batch.commit(entries)?;

            assert_eq!(waker_data.get_pending_ios(), n_sqes as u32);
            assert_eq!(context::with_core(|c| c.get_pending_ios()), n_sqes);

            for i in 0..n_sqes {
                let raw = slab.get_mut(indices[i])?;
                let effect = raw.on_completion(res, CompletionFlags::empty())?;

                // Last SQE to complete triggers completion
                if i == n_sqes - 1 {
                    assert!(matches!(effect, Some(CompletionEffect::WakeHead { .. })));
                } else {
                    assert!(effect.is_none());
                }
            }

            assert_eq!(remaining.load(Ordering::Relaxed), 0);

            // Drop and decrement pending IOs
            indices.iter().for_each(|idx| {
                assert!(slab.try_remove(*idx).is_some());
            });
            assert_eq!(waker_data.get_pending_ios(), 0);
            assert_eq!(context::with_core(|c| c.get_pending_ios()), 0);

            Ok(())
        })
    }

    #[test]
    fn test_raw_sqe_stream_by_flag_completion() -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;
        let (waker, waker_data) = mock_waker();

        let n = 5;

        // count = 0 triggers ByFlag completion
        let mut sqe = RawSqe::new(&waker, CompletionHandler::new_stream(0));
        assert_eq!(sqe.state, RawSqeState::Pending);

        for i in 1..=n {
            assert!(sqe.on_completion(123, CompletionFlags::MORE)?.is_none());
            assert_eq!(waker_data.get_count(), i);

            assert!(sqe.waker.is_some(), "waker should NOT be consumed");
            assert_eq!(sqe.state, RawSqeState::Ready);

            assert_eq!(
                sqe.pop_next_result()?,
                Some(
                    CqeRes::default()
                        .with_res(123)
                        .with_flags(CompletionFlags::MORE)
                )
            );
        }

        assert!(sqe.on_completion(789, CompletionFlags::empty())?.is_none());
        assert_eq!(waker_data.get_count(), n + 1);

        assert_eq!(
            sqe.pop_next_result()?,
            Some(CqeRes::default().with_res(789))
        );
        assert_eq!(sqe.state, RawSqeState::Completed);

        assert!(sqe.pop_next_result()?.is_none());

        drop(sqe);
        assert_eq!(waker_data.get_pending_ios(), 0);
        assert_eq!(context::with_core(|core| core.get_pending_ios()), 0);

        Ok(())
    }

    #[test]
    fn test_raw_sqe_lifecycle() -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;
        let (waker, waker_data) = mock_waker();

        let raw = RawSqe::new(&waker, CompletionHandler::new_single());
        assert_eq!(raw.state, RawSqeState::Pending);

        context::with_slab_mut(|slab| -> Result<()> {
            let idx = {
                let reserved = slab.reserve_entry()?;
                let idx = reserved.key();
                reserved.commit(raw);
                idx
            };

            let inserted = slab.get_mut(idx).unwrap();
            assert_eq!(inserted.state, RawSqeState::Pending);

            assert!(inserted.on_completion(0, CompletionFlags::empty()).is_ok());
            assert_eq!(inserted.state, RawSqeState::Ready);

            assert_eq!(waker_data.get_count(), 1);
            assert!(slab.try_remove(idx).is_some());

            assert_eq!(context::with_core(|core| core.get_pending_ios()), 0);
            Ok(())
        })
    }

    #[test]
    fn test_cancel() -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;
        let (waker, waker_data) = mock_waker();

        let mut raw = RawSqe::new(&waker, CompletionHandler::new_single());
        assert_eq!(raw.state, RawSqeState::Pending);

        assert_eq!(waker_data.get_pending_ios(), 1);
        assert_eq!(context::with_core(|core| core.get_pending_ios()), 1);

        // Cancel N times test idempotency.
        for _ in 0..10 {
            raw.cancel();
            assert_eq!(raw.state, RawSqeState::Cancelled);

            assert_eq!(waker_data.get_pending_ios(), 0);
            assert_eq!(context::with_core(|core| core.get_pending_ios()), 1);
        }

        drop(raw);
        assert_eq!(context::with_core(|core| core.get_pending_ios()), 0);

        Ok(())
    }
}
