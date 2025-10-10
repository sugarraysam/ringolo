use anyhow::Result;
use io_uring::types::{SubmitArgs, Timespec};
use io_uring::{CompletionQueue, EnterFlags, IoUring, SubmissionQueue};
use libc;
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Duration;

pub(crate) struct SingleIssuerRing {
    ring: IoUring,
}

impl SingleIssuerRing {
    pub(super) fn try_new(sq_ring_size: u32) -> Result<Self> {
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
            .build(sq_ring_size)?;

        // Check features and warn users.
        if !ring.params().is_feature_nodrop() {
            eprintln!(
                "Warning: IORING_FEAT_NODROP is not enabled for this kernel. The kernel will silently drop completions if the CQ ring is full."
            )
        }

        Ok(SingleIssuerRing { ring })
    }

    pub(super) fn as_raw_fd(&self) -> RawFd {
        self.ring.as_raw_fd()
    }

    pub(super) fn submit_and_wait(
        &mut self,
        num_to_wait: usize,
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        // Sync user space and kernel shared queue
        self.ring.submission().sync();

        if let Some(duration) = timeout {
            let ts = Timespec::from(duration);
            let args = SubmitArgs::new().timespec(&ts);

            return self.ring.submitter().submit_with_args(num_to_wait, &args);
        }

        self.ring.submitter().submit_and_wait(num_to_wait)
    }

    /// Submit all pending SQ without the GETEVENTS flag. This is to prevent
    /// user/kernel transition where the application thread would have to process
    /// all of the queued `task_work` callbacks.
    pub(super) fn submit_no_wait(&mut self) -> io::Result<usize> {
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
    pub(super) fn has_ready_cqes(&mut self) -> bool {
        self.sq().taskrun()
    }

    pub(crate) fn sq(&mut self) -> SubmissionQueue<'_> {
        self.ring.submission()
    }

    pub(crate) fn cq(&mut self) -> CompletionQueue<'_> {
        self.ring.completion()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::with_core_mut;
    use crate::future::opcodes::TimeoutBuilder;
    use crate::runtime::Builder;
    use crate::sqe::{CompletionHandler, IoError, RawSqe};
    use crate::test_utils::*;
    use rstest::rstest;
    use std::pin::pin;
    use std::task::{Context, Poll};

    #[test]
    fn test_taskrun_flag() -> Result<()> {
        init_local_runtime_and_context(None)?;

        let timespec = Timespec::from(Duration::from_millis(30));

        let mut sqe_fut = pin!(TimeoutBuilder::new(&timespec).build());
        let (waker, _) = mock_waker();

        let mut ctx = Context::from_waker(&waker);
        assert!(matches!(sqe_fut.as_mut().poll(&mut ctx), Poll::Pending));

        with_core_mut(|core| -> Result<()> {
            assert!(!core.has_ready_cqes());

            // Submit without GETEVENTS flag.
            core.submit_no_wait()?;

            // Wait for first IO to be ready, and first `task_work` callback to
            // be queued.
            while !core.has_ready_cqes() {}
            assert!(core.has_ready_cqes());

            core.submit_and_wait(1, None)?;
            assert_eq!(core.process_cqes(Some(1))?, 1);

            Ok(())
        })?;

        match sqe_fut.as_mut().poll(&mut ctx) {
            Poll::Ready(Ok((_, Err(e)))) => {
                assert_eq!(e.raw_os_error().unwrap(), libc::ETIME);
            }
            _ => assert!(false, "unexpected poll result"),
        }

        Ok(())
    }

    #[rstest]
    #[case::len_32_capacity_4(32, 4)]
    #[case::len_64_capacity_8(64, 8)]
    #[case::len_128_capacity_16(128, 16)]
    fn test_sq_len_and_capacity(#[case] sq_ring_size: usize, #[case] n: usize) -> Result<()> {
        let builder = Builder::new_local().sq_ring_size(sq_ring_size);
        init_local_runtime_and_context(Some(builder))?;

        with_core_mut(|core| -> Result<()> {
            {
                let sq = core.ring.get_mut().sq();
                assert_eq!(sq.len(), 0);
                assert_eq!(sq.capacity(), sq_ring_size);
            }

            let nops = {
                let mut slab = core.slab.borrow_mut();

                (0..n)
                    .map(|_| {
                        let raw_sqe = RawSqe::new(nop(), CompletionHandler::new_single());
                        slab.insert(raw_sqe)
                            .map(|(idx, _)| idx)
                            .map_err(IoError::into)
                    })
                    .collect::<Result<Vec<_>>>()
            }?;

            core.push_sqes(&nops)?;

            {
                let sq = core.ring.get_mut().sq();
                assert_eq!(sq.len(), n);
                assert_eq!(sq.capacity(), sq_ring_size);
            }

            core.submit_and_wait(n, None)?;

            {
                let sq = core.ring.get_mut().sq();
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
        init_local_runtime_and_context(Some(builder))?;

        with_core_mut(|core| -> Result<()> {
            {
                let cq = core.ring.get_mut().cq();
                assert_eq!(cq.len(), 0);
                assert_eq!(cq.capacity(), sq_ring_size * 2);
            }

            let (waker, waker_data) = mock_waker();

            let nops = {
                let mut slab = core.slab.borrow_mut();

                (0..n)
                    .map(|_| {
                        let mut raw_sqe = RawSqe::new(nop(), CompletionHandler::new_single());
                        raw_sqe.set_waker(&waker);
                        slab.insert(raw_sqe)
                            .map(|(idx, _)| idx)
                            .map_err(IoError::into)
                    })
                    .collect::<Result<Vec<_>>>()
            }?;

            core.push_sqes(&nops)?;
            core.submit_no_wait()?;

            // Busy loop and ensure len of cq ring monotonically increases until n.
            // We want to guaranted there is no need to call "cq.sync()" to see
            // # of ready_cqes increase.
            {
                let cq = core.ring.get_mut().cq();
                while cq.len() != n {}
                assert_eq!(cq.len(), n);
            }

            // Should process all ready cqes from iterator.
            core.process_cqes(None)?;
            assert_eq!(waker_data.get_count(), n);

            {
                let cq = core.ring.get_mut().cq();
                assert_eq!(cq.len(), 0);
            }

            Ok(())
        })
    }
}
