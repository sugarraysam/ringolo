use anyhow::Result;
use io_uring::{CompletionQueue, EnterFlags, IoUring, SubmissionQueue, types};
use libc;
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Duration;

pub struct SingleIssuerRing {
    ring: IoUring,
}

impl SingleIssuerRing {
    pub fn try_new(sq_ring_size: u32) -> Result<Self> {
        let ring = IoUring::builder()
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

    pub fn as_raw_fd(&self) -> RawFd {
        self.ring.as_raw_fd()
    }

    // TODO: write unit test does not seem to work
    // Because we set IORING_SQ_TASKRUN flag, we have a shortcut to check if
    // we have pending completions.
    pub fn has_pending_cqes(&self) -> bool {
        unsafe { self.ring.submission_shared() }.taskrun()
    }

    pub fn wait_cqes_timeout(
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

    pub fn submission(&mut self) -> SubmissionQueue<'_> {
        self.ring.submission()
    }

    pub fn completion(&mut self) -> CompletionQueue<'_> {
        self.ring.completion()
    }

    pub fn get(&self) -> &IoUring {
        &self.ring
    }

    pub fn get_mut(&mut self) -> &mut IoUring {
        &mut self.ring
    }
}
