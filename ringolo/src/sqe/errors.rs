use crate::future::opcode::OpcodeError;
use crate::runtime::{PanicReason, YieldReason};
use io_uring::squeue::PushError;
use std::io::{self, Error};

/// A centralized error type for all scheduler and runtime operations.
#[derive(thiserror::Error, Debug)]
pub enum IoError {
    /// The submission queue ring for io_uring is full.
    /// This is a specific, recoverable state where the application
    /// should reap completions before submitting more I/O.
    #[error("Submission queue ring is full, cannot submit IO")]
    SqRingFull(#[from] PushError),

    #[error("FATAL: SQ entry batch too large to fit in the SQ ring")]
    SqBatchTooLarge,

    /// A resource slab (e.g., for storing futures or tasks) is full.
    /// This indicates the system is at its configured capacity.
    #[error("Slab allocator is full, cannot allocate new resource")]
    SlabFull,

    #[error("FATAL: Slab allocator is in an invalid state")]
    SlabInvalidState,

    #[error("Opcode error: {0}")]
    Opcode(#[from] OpcodeError),

    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// A catch-all for any other type of unexpected error.
    #[error("An unexpected error occurred: {0}")]
    Other(#[from] anyhow::Error),
}

impl IoError {
    pub fn is_retryable(&self) -> bool {
        match self {
            IoError::SlabFull => true,
            IoError::SqRingFull { .. } => true,
            IoError::Io(e) => {
                // Let's rely on kernel errors instead of `io::ErrorKind` for more accuracy and
                // direct mapping to io_uring documentation.
                e.raw_os_error()
                    // TODO: non-exhaustive add more as we identify them
                    .is_some_and(|errno| matches!(errno, libc::EAGAIN))
            }
            _ => false,
        }
    }

    pub fn is_fatal(&self) -> bool {
        matches!(self, IoError::SlabInvalidState)
    }

    pub(crate) fn as_yield_reason(&self) -> YieldReason {
        match self {
            IoError::SqRingFull { .. } => YieldReason::SqRingFull,
            IoError::SlabFull => YieldReason::SlabFull,
            _ => YieldReason::Unknown,
        }
    }

    pub(crate) fn as_panic_reason(&self) -> PanicReason {
        match self {
            IoError::SlabInvalidState => PanicReason::SlabInvalidState,
            _ => PanicReason::Unknown,
        }
    }

    pub(crate) fn raw_os_error(&self) -> Option<i32> {
        match self {
            IoError::Io(e) => e.raw_os_error(),
            _ => None,
        }
    }
}

impl PartialEq for IoError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::SqRingFull { .. }, Self::SqRingFull { .. }) => true,
            (Self::SlabFull, Self::SlabFull) => true,
            (Self::Io(a), Self::Io(b)) => a.kind() == b.kind(),
            (Self::Opcode(a), Self::Opcode(b)) => a == b,
            _ => false,
        }
    }
}

impl From<IoError> for io::Error {
    fn from(e: IoError) -> Self {
        match e {
            IoError::Io(io_err) => io_err,
            _ => Error::other(e.to_string()),
        }
    }
}
