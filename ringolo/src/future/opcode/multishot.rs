use crate::future::opcode::{
    KernelFdMode, MultishotParams, MultishotPayload, OpcodeError, UringFd,
};
use crate::sqe::IoError;
use anyhow::Result;
use anyhow::anyhow;
use io_uring::types::{TimeoutFlags, Timespec};
use pin_project::pin_project;
use std::pin::Pin;
use std::time::Duration;

#[derive(Debug)]
pub struct AcceptMultishot {
    sockfd: UringFd,
    mode: KernelFdMode,
    flags: i32, // TODO: nix flags
}

impl AcceptMultishot {
    /// The `allocate_file_index` flag maps to the `IORING_FILE_INDEX_ALLOC` from the docs.
    /// The kernel will dynamically choose a direct descriptor index if this option is true.
    /// You need to have registered direct descriptors prior for this to work.
    pub fn try_new(sockfd: UringFd, mode: KernelFdMode, flags: Option<i32>) -> Result<Self> {
        if matches!(mode, KernelFdMode::Direct(_)) {
            Err(anyhow!("AcceptMultishot does not support fixed slots"))
        } else {
            Ok(Self {
                sockfd,
                mode,
                flags: flags.unwrap_or(0),
            })
        }
    }
}

impl MultishotPayload for AcceptMultishot {
    type Item = UringFd;

    fn create_params(self: Pin<&mut Self>) -> Result<MultishotParams, OpcodeError> {
        let mut entry = resolve_fd!(self.sockfd, |fd| io_uring::opcode::AcceptMulti::new(fd));

        if self.mode == KernelFdMode::DirectAuto {
            entry = entry.allocate_file_index(true);
        };

        Ok(MultishotParams::new(
            entry.flags(self.flags).build(),
            0, /* infinite */
        ))
    }

    fn into_next(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Item, IoError> {
        let fd = result?;
        Ok(UringFd::from_result(fd, self.mode))
    }
}

#[derive(Debug)]
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
    pub fn new(interval: Duration, count: u32, flags: Option<TimeoutFlags>) -> Self {
        Self {
            timespec: Timespec::from(interval),
            count,
            flags: TimeoutFlags::MULTISHOT | flags.unwrap_or(TimeoutFlags::empty()),
        }
    }
}

impl MultishotPayload for TimeoutMultishot {
    type Item = ();

    fn create_params(self: Pin<&mut Self>) -> Result<MultishotParams, OpcodeError> {
        let this = self.project();

        let timespec_addr = &*this.timespec as *const Timespec;

        let entry = io_uring::opcode::Timeout::new(timespec_addr)
            .count(*this.count)
            .flags(*this.flags)
            .build();

        Ok(MultishotParams::new(entry, *this.count))
    }

    fn into_next(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Item, IoError> {
        match result {
            Ok(_) => Ok(()),
            Err(IoError::Io(e)) if e.raw_os_error() == Some(libc::ETIME) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        os::fd::{AsRawFd, FromRawFd},
    };

    use super::*;
    use crate::{self as ringolo, future::opcode::Multishot};
    use crate::{future::opcode::common::UringFdKind, utils::scheduler::Method};
    use anyhow::Result;
    use futures::StreamExt;

    // #[ignore = "WOW completion bug. Now runtime blocks, parks thread waiting for completion. Needs to cancel itself when goes out of scope."]
    #[ringolo::test]
    async fn test_accept_multi() -> Result<()> {
        let n = 2;
        let hello = b"hello";

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let listen_addr = listener.local_addr()?;

        let handle = std::thread::spawn(move || -> Result<()> {
            for _ in 0..n {
                if let Ok(mut stream) = TcpStream::connect(listen_addr) {
                    let mut buf = Vec::with_capacity(hello.len());
                    stream.read_to_end(&mut buf)?;
                    assert_eq!(buf, hello);
                } else {
                    assert!(false, "failed to connect to server");
                }
            }

            Ok(())
        });

        {
            let mut stream = Multishot::new(AcceptMultishot::try_new(
                listener.as_raw_fd().into(),
                KernelFdMode::Legacy,
                None,
            )?)
            .take(n);

            let mut got = 0;
            while let Some(res) = stream.next().await
                && got < n
            {
                assert!(res.is_ok());

                let mut stream = match res.unwrap().kind() {
                    UringFdKind::Raw(raw) => unsafe { TcpStream::from_raw_fd(raw) },
                    UringFdKind::Fixed(_) => panic!("should not be fixed"),
                };

                stream.write_all(hello)?;
                got += 1;
            }

            handle.join().unwrap()?;
        } // stream dropped here, schedules async cancel task

        ringolo::with_scheduler!(|s| {
            let spawn_calls = s.tracker.get_calls(&Method::Spawn);
            assert_eq!(spawn_calls.len(), 1);

            let unhandled_panic_calls = s.tracker.get_calls(&Method::UnhandledPanic);
            assert!(unhandled_panic_calls.is_empty());
        });

        Ok(())
    }
}
