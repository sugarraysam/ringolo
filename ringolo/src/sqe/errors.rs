use std::io;

/// A centralized error type for all scheduler and runtime operations.
#[derive(thiserror::Error, Debug)]
pub enum IoError {
    /// The submission queue ring for io_uring is full.
    /// This is a specific, recoverable state where the application
    /// should reap completions before submitting more I/O.
    #[error("Submission queue ring is full, cannot submit IO")]
    SqRingFull,

    #[error("FATAL: SQ ring is in an invalid state")]
    SqRingInvalidState,

    #[error("FATAL: SQ entry batch too large to fit in the SQ ring")]
    SqBatchTooLarge,

    /// A resource slab (e.g., for storing futures or tasks) is full.
    /// This indicates the system is at its configured capacity.
    #[error("Slab allocator is full, cannot allocate new resource")]
    SlabFull,

    #[error("FATAL: Slab allocator is in an invalid state")]
    SlabInvalidState,

    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// A catch-all for any other type of unexpected error.
    #[error("An unexpected error occurred: {0}")]
    Other(#[from] anyhow::Error),
}

impl PartialEq for IoError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::SqRingFull, Self::SqRingFull) => true,
            (Self::SlabFull, Self::SlabFull) => true,
            (Self::Io(a), Self::Io(b)) => a.kind() == b.kind(),
            _ => false,
        }
    }
}
