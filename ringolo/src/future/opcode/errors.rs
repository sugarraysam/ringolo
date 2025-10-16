use std::io::{self};

use crate::future::opcode::UringFd;

/// A centralized error type for all scheduler and runtime operations.
#[derive(thiserror::Error, Debug)]
pub enum OpcodeError {
    #[error("Incorrect UringFd variant for the conversion")]
    IncorrectFdVariant(UringFd),

    #[error("Invalid fixed `io_uring` fd. Should be between 0 and u32::MAX - 2.")]
    InvalidFixedFd(u32),

    #[error("IO error occurred: {0}")]
    Io(#[from] io::Error),

    /// A catch-all for any other type of unexpected error.
    #[error("An unexpected error occurred: {0}")]
    Other(#[from] anyhow::Error),
}

impl OpcodeError {
    pub(crate) fn raw_os_error(&self) -> Option<i32> {
        match self {
            OpcodeError::Io(e) => e.raw_os_error(),
            _ => None,
        }
    }
}

impl PartialEq for OpcodeError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::IncorrectFdVariant(a), Self::IncorrectFdVariant(b)) => a == b,
            (Self::InvalidFixedFd(a), Self::InvalidFixedFd(b)) => a == b,
            (Self::Io(a), Self::Io(b)) => a.kind() == b.kind(),
            _ => false,
        }
    }
}
