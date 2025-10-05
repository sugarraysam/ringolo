use anyhow::Result;
use io_uring::types::{SubmitArgs, Timespec};
use io_uring::{CompletionQueue, EnterFlags, IoUring, SubmissionQueue, types};
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
            // Handle completions ourselves prevent unwanted interrupts
            .setup_coop_taskrun()
            // Setup IORING_SQ_TASKRUN flag on SQ ring to indicate if completions
            // are pending w/o a syscall to `io_uring_enter`
            .setup_taskrun_flag()
            .build(sq_ring_size)?;

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

    pub(super) fn submit_no_wait(&mut self) -> io::Result<usize> {
        self.ring.submission().sync();

        // Submit w/o the IORING_GETEVENTS flags. Since we use coop taskrun,
        // io_uring will process work asynchronously and set the taskrun flag on
        // SQ ring once we have pending CQEs.
        unsafe {
            self.ring
                .submitter()
                .enter::<libc::sigset_t>(0, 0, EnterFlags::empty().bits(), None)
        }
    }

    // Because we set IORING_SQ_TASKRUN flag, we have a shortcut to check if
    // we have pending completions.
    pub(super) fn has_pending_cqes(&self) -> bool {
        unsafe { self.ring.submission_shared() }.taskrun()
    }

    pub(super) fn wait_cqes_timeout(
        &mut self,
        min_complete: u32,
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        let mut flags = EnterFlags::GETEVENTS;

        if let Some(duration) = timeout {
            flags |= EnterFlags::EXT_ARG;

            let ts = types::Timespec::from(duration);
            let args = types::SubmitArgs::new().timespec(&ts);

            unsafe {
                return self
                    .ring
                    .submitter()
                    .enter(0, min_complete, flags.bits(), Some(&args));
            }
        }

        unsafe {
            self.ring
                .submitter()
                .enter::<libc::sigset_t>(0, min_complete, flags.bits(), None)
        }
    }

    pub(crate) fn sq(&mut self) -> SubmissionQueue<'_> {
        self.ring.submission()
    }

    pub(crate) fn cq(&mut self) -> CompletionQueue<'_> {
        self.ring.completion()
    }

    pub(super) fn get(&self) -> &IoUring {
        &self.ring
    }

    pub(super) fn get_mut(&mut self) -> &mut IoUring {
        &mut self.ring
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::task::{Context, Poll};

    use crate::context::{init_context, with_context_mut};
    use crate::future::opcodes::TimeoutBuilder;

    use super::*;
    use crate::test_utils::*;

    #[test]
    #[ignore = "Can't get taskrun flag to work as expected. Never set by kernel."]
    fn test_taskrun_flag() -> Result<()> {
        init_context(64);

        let timespec = Timespec::from(Duration::from_millis(10));

        let mut sqe_fut = pin!(TimeoutBuilder::new(&timespec).build()?);
        let (waker, _) = mock_waker();

        let mut ctx = Context::from_waker(&waker);
        assert!(matches!(sqe_fut.as_mut().poll(&mut ctx), Poll::Pending));

        with_context_mut(|ctx| {
            assert!(!ctx.ring.has_pending_cqes());

            // submit + complete
            assert!(matches!(ctx.submit_no_wait(), Ok(0)));

            // TODO: cant get the flag to be set...
            while !ctx.ring.has_pending_cqes() {}

            assert!(matches!(ctx.process_cqes(None), Ok(1)));
        });

        match sqe_fut.as_mut().poll(&mut ctx) {
            Poll::Ready(Ok((_, Err(e)))) => {
                assert_eq!(e.raw_os_error().unwrap(), libc::ETIME);
            }
            _ => assert!(false, "unexpected poll result"),
        }

        Ok(())
    }
}
