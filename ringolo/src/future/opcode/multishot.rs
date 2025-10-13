use crate::future::opcode::{MultishotParams, MultishotPayload};
use crate::sqe::IoError;
use anyhow::Result;
use io_uring::types::{TimeoutFlags, Timespec};
use pin_project::pin_project;
use std::io;
use std::pin::Pin;
use std::time::Duration;

#[pin_project]
pub struct TimeoutMultishot {
    #[pin]
    timespec: Timespec,
    count: u32,
    flags: TimeoutFlags,
}

impl TimeoutMultishot {
    /// Create a new multishot timeout operation.
    /// - If `count` is `n`, the timeout will fire n times.
    /// - If `count` is `0`, it will fire indefinitely.
    pub fn new(interval: Duration, count: u32) -> Self {
        Self {
            timespec: Timespec::from(interval),
            count,
            flags: TimeoutFlags::MULTISHOT,
        }
    }

    pub fn flags(mut self, flags: TimeoutFlags) -> Self {
        self.flags |= flags;
        self
    }
}

impl MultishotPayload for TimeoutMultishot {
    type Item = io::Result<()>;

    fn create_params(self: Pin<&mut Self>) -> MultishotParams {
        let this = self.project();

        let timespec_addr = &*this.timespec as *const Timespec;

        let entry = io_uring::opcode::Timeout::new(timespec_addr)
            .count(*this.count)
            .flags(*this.flags)
            .build();

        MultishotParams::new(entry, *this.count)
    }

    fn into_next(self: Pin<&mut Self>, result: Result<i32, IoError>) -> Self::Item {
        match result {
            Ok(_) => Ok(()),
            Err(IoError::Io(e)) if e.raw_os_error() == Some(libc::ETIME) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}
