use anyhow::Result;
use io_uring::IoUring;
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
}
