use anyhow::Result;
use io_uring::squeue::Entry;
use io_uring::types::{SubmitArgs, Timespec};
use io_uring::{CompletionQueue, EnterFlags, IoUring, SubmissionQueue};
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Duration;

use crate::context::RawSqeSlab;
use crate::runtime::RuntimeConfig;
use crate::sqe::{CompletionEffect, IoError, RawSqeState};

/// A wrapper around `io_uring` specifically configured for a Single Issuer.
///
/// This implementation leverages `IORING_SETUP_SINGLE_ISSUER` and `IORING_SETUP_DEFER_TASKRUN`.
/// This combination allows the kernel to defer processing completions until `io_uring_enter`
/// is explicitly called, minimizing interrupt overhead and context switches.
pub(crate) struct SingleIssuerRing {
    ring: IoUring,
}

impl SingleIssuerRing {
    pub(crate) fn try_new(cfg: &RuntimeConfig) -> Result<Self> {
        let ring = IoUring::builder()
            // Keep submitting requests even if we encounter error. This is
            // important to reduce the number of syscall. We also want to produce
            // errors (i.e.: CQEs) for all SQEs and put error handling
            // responsibilities in the hands of futures developers.
            .setup_submit_all()
            // Hint kernel that a single thread will submit requests
            .setup_single_issuer()
            // When IO operations are ready, `io_uring` places `task_work` onto
            // a queue. Next time ANY syscall is made, the application thread will
            // by default process all of these `task_work` callbacks. By setting
            // DEFER_TASKRUN, we tell `io_uring` that we will only process the
            // `task_work` queue when calling `io_uring_enter` with GETEVENTS.
            .setup_defer_taskrun()
            // Handle completions ourselves prevent unwanted interrupts
            .setup_coop_taskrun()
            // Setup IORING_SQ_TASKRUN flag on SQ ring to indicate if completions
            // are pending w/o a syscall to `io_uring_enter`
            .setup_taskrun_flag()
            .build(cfg.sq_ring_size as u32)?;

        // Check features and warn users.
        if !ring.params().is_feature_nodrop() {
            eprintln!(
                "Warning: IORING_FEAT_NODROP is not enabled for this kernel. The kernel will silently drop completions if the CQ ring is full."
            )
        }

        ring.submitter()
            .register_files_sparse(cfg.direct_fds_per_ring)?;

        Ok(SingleIssuerRing { ring })
    }

    pub(crate) fn as_raw_fd(&self) -> RawFd {
        self.ring.as_raw_fd()
    }

    pub(crate) fn sq(&mut self) -> SubmissionQueue<'_> {
        self.ring.submission()
    }

    pub(crate) fn cq(&mut self) -> CompletionQueue<'_> {
        self.ring.completion()
    }

    pub(crate) fn num_unsubmitted_sqes(&mut self) -> usize {
        self.sq().len()
    }

    pub(crate) fn push(&mut self, entry: &Entry) -> Result<(), IoError> {
        unsafe { self.sq().push(entry).map_err(IoError::from) }
    }

    pub(crate) fn push_batch(&mut self, entries: &[Entry]) -> Result<(), IoError> {
        if entries.len() > self.sq().capacity() {
            return Err(IoError::SqBatchTooLarge);
        }

        unsafe { self.sq().push_multiple(entries).map_err(IoError::from) }
    }

    pub(crate) fn submit_and_wait(
        &mut self,
        num_to_wait: usize,
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        // Sync user space and kernel shared queue
        self.ring.submission().sync();

        if let Some(duration) = timeout {
            let ts = Timespec::from(duration);
            let args = SubmitArgs::new().timespec(&ts);

            return match self.ring.submitter().submit_with_args(num_to_wait, &args) {
                Ok(n) => Ok(n),

                // To enable shutdown path, we need to timeout `io_uring_enter` syscall
                // to avoid blocking indefinitely. Treat timeouts as submit(0) and not
                // errors.
                Err(e) if e.raw_os_error() == Some(libc::ETIME) => Ok(0),

                Err(e) => Err(e),
            };
        }

        self.ring.submitter().submit_and_wait(num_to_wait)
    }

    /// Submit all pending SQ without the GETEVENTS flag. This is to prevent
    /// user/kernel transition where the application thread would have to process
    /// all of the queued `task_work` callbacks.
    pub(crate) fn submit_no_wait(&mut self) -> io::Result<usize> {
        let to_submit = {
            let mut sq = self.sq();
            let to_submit = sq.len();

            // Early return we have nothing to submit.
            if to_submit == 0 {
                return Ok(0);
            };

            sq.sync();
            to_submit
        };

        // Submit w/o the IORING_GETEVENTS flags. Since we use coop taskrun,
        // io_uring will process work asynchronously and set the taskrun flag on
        // SQ ring once we have pending CQEs.
        unsafe {
            self.ring.submitter().enter::<libc::sigset_t>(
                to_submit as u32,
                0,
                EnterFlags::empty().bits(),
                None,
            )
        }
    }

    // Because we set IORING_SQ_TASKRUN flag, we have a shortcut to check if
    // we have pending completions.
    pub(crate) fn has_ready_cqes(&mut self) -> bool {
        self.sq().taskrun()
    }

    // Will busy loop until `num_to_visit` CQE have been visited. It is the caller's
    // responsibility to make sure the CQ will see that many completions, otherwise
    // this will result in an infinite loop.
    pub(crate) fn process_cqes(
        &mut self,
        slab: &mut RawSqeSlab,
        num_to_visit: Option<usize>,
    ) -> Result<usize> {
        // We need to split `num_visited`/`num_completed` counters because if we
        // cancel an SQE inflight, the entry might be dropped from the slab while
        // still producing a CQE. In that case, the loop would only increment the
        // `num_visited` counter.
        let mut num_visited = 0;
        let mut num_completed = 0;

        let mut cq = self.cq();
        let to_visit = num_to_visit.unwrap_or(cq.len());

        let mut should_sync = false;

        while num_visited < to_visit {
            // Avoid syncing on first pass.
            if should_sync {
                // If we get here, we are spinning waiting for completions so
                // let's give the CPU a tiny breather.
                std::thread::yield_now();
                cq.sync();
            }

            for cqe in &mut cq {
                num_visited += 1;

                let raw_sqe = match slab.get_mut(cqe.user_data() as usize) {
                    Err(e) => {
                        eprintln!("CQE user data not found in RawSqeSlab: {:?}", e);
                        continue;
                    }
                    Ok(sqe) => {
                        if !matches!(sqe.state, RawSqeState::Pending | RawSqeState::Ready) {
                            // Ignore unknown CQEs which might have valid index in
                            // the Slab. Can this even happen?
                            eprintln!("SQE in unexpected state: {:?}", sqe.state);
                            continue;
                        }
                        sqe
                    }
                };

                num_completed += 1;

                if let Some(CompletionEffect::WakeHead { head }) =
                    raw_sqe.on_completion(cqe.result(), cqe.flags().into())?
                {
                    slab.get_mut(head)?.wake_by_val()?;
                }

                // Exit early if user provided an override for `to_visit`.
                if num_visited >= to_visit {
                    break;
                }
            }

            should_sync = true;
        }

        Ok(num_completed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context;
    use crate::future::lib::Op;
    use crate::future::lib::ops::Timeout;
    use crate::runtime::Builder;
    use crate::runtime::SPILL_TO_HEAP_THRESHOLD;
    use crate::sqe::{CompletionHandler, RawSqe};
    use crate::test_utils::*;
    use rstest::rstest;
    use smallvec::SmallVec;
    use std::pin::pin;
    use std::task::{Context, Poll};

    #[test]
    fn test_submit_timeout() -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        // Submit with empty ring should timeout - timeout is not an error
        context::with_ring_mut(|ring| {
            assert!(
                ring.submit_and_wait(1, Some(Duration::from_millis(5)))
                    .is_ok()
            );
        });

        Ok(())
    }

    #[test]
    fn test_taskrun_flag() -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        let mut sqe_fut = pin!(Op::new(Timeout::new(Duration::from_millis(1), None)));
        let (waker, _) = mock_waker();

        let mut ctx = Context::from_waker(&waker);
        assert!(matches!(sqe_fut.as_mut().poll(&mut ctx), Poll::Pending));

        context::with_slab_and_ring_mut(|slab, ring| -> Result<()> {
            assert!(!ring.has_ready_cqes());

            // Submit without GETEVENTS flag.
            ring.submit_no_wait()?;

            // Wait for first IO to be ready, and first `task_work` callback to
            // be queued.
            while !ring.has_ready_cqes() {}
            assert!(ring.has_ready_cqes());

            ring.submit_and_wait(1, None)?;
            assert_eq!(ring.process_cqes(slab, Some(1))?, 1);

            Ok(())
        })?;

        assert!(matches!(
            sqe_fut.as_mut().poll(&mut ctx),
            Poll::Ready(Ok(_))
        ));

        Ok(())
    }

    #[rstest]
    #[case::len_32_capacity_4(32, 4)]
    #[case::len_64_capacity_8(64, 8)]
    #[case::len_128_capacity_16(128, 16)]
    fn test_sq_len_and_capacity(#[case] sq_ring_size: usize, #[case] n: usize) -> Result<()> {
        let builder = Builder::new_local().sq_ring_size(sq_ring_size);
        let (_runtime, _scheduler) = init_local_runtime_and_context(Some(builder))?;

        let (waker, _) = mock_waker();

        context::with_slab_and_ring_mut(|slab, ring| -> Result<()> {
            {
                let sq = ring.sq();
                assert_eq!(sq.len(), 0);
                assert_eq!(sq.capacity(), sq_ring_size);
            }

            let mut nops: SmallVec<[Entry; SPILL_TO_HEAP_THRESHOLD]> = SmallVec::with_capacity(n);
            let mut raws: SmallVec<[RawSqe; SPILL_TO_HEAP_THRESHOLD]> = SmallVec::with_capacity(n);

            {
                let batch = slab.reserve_batch(n)?;
                let indices = batch.keys();

                (0..n).for_each(|i| {
                    nops.push(nop().user_data(indices[i] as u64));
                    raws.push(RawSqe::new(&waker, CompletionHandler::new_single()));
                });

                let _ = batch.commit(raws)?;
            };

            ring.push_batch(&nops)?;

            {
                let sq = ring.sq();
                assert_eq!(sq.len(), n);
                assert_eq!(sq.capacity(), sq_ring_size);
            }

            ring.submit_and_wait(n, None)?;

            {
                let sq = ring.sq();
                assert_eq!(sq.len(), 0);
                assert_eq!(sq.capacity(), sq_ring_size);
            }

            Ok(())
        })
    }

    #[rstest]
    #[case::capacity_32_len_4(32, 4)]
    #[case::capacity_64_len_8(64, 8)]
    #[case::capacity_128_len_16(128, 16)]
    fn test_cq_len_and_sync(#[case] sq_ring_size: usize, #[case] n: usize) -> Result<()> {
        let builder = Builder::new_local().sq_ring_size(sq_ring_size);
        let (_runtime, _scheduler) = init_local_runtime_and_context(Some(builder))?;

        context::with_slab_and_ring_mut(|slab, ring| -> Result<()> {
            {
                let cq = ring.cq();
                assert_eq!(cq.len(), 0);
                assert_eq!(cq.capacity(), sq_ring_size * 2);
            }

            let (waker, waker_data) = mock_waker();

            let mut nops: SmallVec<[Entry; SPILL_TO_HEAP_THRESHOLD]> = SmallVec::with_capacity(n);
            let mut raws: SmallVec<[RawSqe; SPILL_TO_HEAP_THRESHOLD]> = SmallVec::with_capacity(n);

            {
                let batch = slab.reserve_batch(n)?;
                let indices = batch.keys();

                (0..n).for_each(|i| {
                    let nop = nop().user_data(indices[i] as u64);

                    let raw = RawSqe::new(&waker, CompletionHandler::new_single());
                    nops.push(nop);
                    raws.push(raw);
                });

                let _ = batch.commit(raws)?;
            };

            ring.push_batch(&nops)?;
            ring.submit_no_wait()?;

            // Busy loop and ensure len of cq ring monotonically increases until n.
            // We want to guaranted there is no need to call "cq.sync()" to see
            // # of ready_cqes increase.
            {
                let cq = ring.cq();
                while cq.len() != n {}
                assert_eq!(cq.len(), n);
            }

            // Should process all ready cqes from iterator.
            ring.process_cqes(slab, None)?;
            assert_eq!(waker_data.get_count(), n);
            assert_eq!(ring.cq().len(), 0);

            // Waker goes out of scope *before* TLS is dropped causing heap-after-free
            // errors. This is test-specific so we can just clear the slab to fix.
            slab.clear();

            Ok(())
        })
    }
}
