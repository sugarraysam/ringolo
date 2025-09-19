use anyhow::Result;
use io_uring::EnterFlags;
use io_uring::squeue::Entry;
use io_uring::{IoUring, types};
use libc;
use std::io;
use std::time::Duration;

use std::marker::PhantomData;

pub struct SingleIssuerRing {
    ring: IoUring,

    // Make !Send + !Sync with a marker type.
    _not_send_or_sync: PhantomData<*const ()>,
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

        Ok(SingleIssuerRing {
            ring,
            _not_send_or_sync: PhantomData,
        })
    }

    pub fn submit_and_wait_timeout(
        &mut self,
        sqes: &[Entry],
        num_to_wait: usize,
        timeout: Option<Duration>,
    ) -> anyhow::Result<usize> {
        unsafe {
            let mut sq = self.ring.submission();
            sq.push_multiple(sqes)?;
            sq.sync();
        }

        if let Some(duration) = timeout {
            let ts = types::Timespec::from(duration);
            let args = types::SubmitArgs::new().timespec(&ts);

            return Ok(self.ring.submitter().submit_with_args(num_to_wait, &args)?);
        }

        Ok(self.ring.submitter().submit_and_wait(num_to_wait)?)
    }

    // Because we set IORING_SQ_TASKRUN flag, we have a shortcut to check if
    // we have pending completions.
    pub fn has_pending_completions(&mut self) -> bool {
        self.ring.submission().taskrun()
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

        // TODO: missing `libc::sigset_t` ?
        unsafe {
            self.ring
                .submitter()
                .enter::<libc::sigset_t>(0, min_complete, flags.bits(), None)
        }
    }

    pub fn as_mut(&mut self) -> &mut IoUring {
        &mut self.ring
    }
}
