use crate::future::lib::{KernelFdMode, MultishotParams, MultishotPayload, OpcodeError, UringFd};
use crate::sqe::IoError;
use anyhow::Result;
use anyhow::anyhow;
use io_uring::types::{TimeoutFlags, Timespec};
use nix::sys::socket::SockFlag;
use pin_project::pin_project;
use std::pin::Pin;
use std::time::Duration;

#[derive(Debug)]
pub struct AcceptMultishot {
    sockfd: UringFd,
    mode: KernelFdMode,
    flags: SockFlag,
}

impl AcceptMultishot {
    /// The `allocate_file_index` flag maps to the `IORING_FILE_INDEX_ALLOC` from the docs.
    /// The kernel will dynamically choose a direct descriptor index if this option is true.
    /// You need to have registered direct descriptors prior for this to work.
    pub fn try_new(sockfd: UringFd, mode: KernelFdMode, flags: Option<SockFlag>) -> Result<Self> {
        if matches!(mode, KernelFdMode::Direct(_)) {
            Err(anyhow!("AcceptMultishot does not support fixed slots"))
        } else {
            Ok(Self {
                sockfd,
                mode,
                flags: SockFlag::SOCK_CLOEXEC | flags.unwrap_or(SockFlag::empty()),
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
            entry.flags(self.flags.bits()).build(),
            0, /* infinite */
        ))
    }

    fn into_next(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Item, IoError> {
        let fd = result?;
        Ok(UringFd::from_result_into_owned(fd, self.mode))
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

        let timespec_addr = std::ptr::from_ref(&*this.timespec).cast();

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
    use crate::future::lib::fd::UringFdKind;
    use crate::utils::scheduler::Method;
    use crate::{self as ringolo, future::lib::Multishot};
    use anyhow::{Context, Result};
    use futures::StreamExt;
    use rstest::rstest;

    #[rstest]
    #[case::ipv4("127.0.0.1:0")]
    #[case::ipv6("[::1]:0")]
    #[ringolo::test]
    async fn test_accept_multi(#[case] addr: &str) -> Result<()> {
        let n = 2;
        let hello = b"hello";

        let listener = TcpListener::bind(addr).context("creating tcp listener")?;
        let listen_addr = listener.local_addr().context("getting listener addr")?;

        let handle = std::thread::spawn(move || -> Result<()> {
            for _ in 0..n {
                if let Ok(mut stream) = TcpStream::connect(listen_addr) {
                    let mut buf = Vec::with_capacity(hello.len());
                    stream
                        .read_to_end(&mut buf)
                        .context("reading from stream")?;
                    assert_eq!(buf, hello);
                } else {
                    assert!(false, "failed to connect to server");
                }
            }

            Ok(())
        });

        {
            let mut stream = Multishot::new(AcceptMultishot::try_new(
                UringFd::new_raw_borrowed(listener.as_raw_fd()),
                KernelFdMode::Legacy,
                None,
            )?)
            .take(n);

            let mut got = 0;
            while let Some(res) = stream.next().await
                && got < n
            {
                let new_sockfd = res.context("stream socket")?;
                let mut stream = match new_sockfd.kind() {
                    UringFdKind::Raw => unsafe {
                        TcpStream::from_raw_fd(
                            // We pass ownership of the Fd to TcpStream to avoid double free.
                            new_sockfd.into_raw().context("cant pass ownership")?,
                        )
                    },
                    UringFdKind::Fixed => panic!("should not be fixed"),
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
