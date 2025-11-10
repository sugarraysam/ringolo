use crate::future::lib::{
    AsRawOrDirect, CancelHow, KernelFdMode, MultishotParams, MultishotPayload, OpcodeError,
    OwnedUringFd,
};
use crate::sqe::IoError;
use io_uring::types::{TimeoutFlags, Timespec};
use nix::sys::socket::SockFlag;
use pin_project::pin_project;
use std::io;
use std::pin::Pin;
use std::task::Waker;
use std::time::Duration;

#[derive(Debug)]
pub struct AcceptMultishot<T: AsRawOrDirect> {
    sockfd: T,
    mode: KernelFdMode,
    flags: SockFlag,
}

impl<T: AsRawOrDirect> AcceptMultishot<T> {
    pub fn try_new(sockfd: T, mode: KernelFdMode, flags: Option<SockFlag>) -> io::Result<Self> {
        let flags = match mode {
            KernelFdMode::Direct(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "AcceptMultishot only supports dynamically allocated fixed descriptors.",
                ));
            }
            KernelFdMode::Legacy => SockFlag::SOCK_CLOEXEC | flags.unwrap_or(SockFlag::empty()),
            // The ring itself is CLOEXEC, no need to set for direct descriptor. In fact it
            // triggers -EINVAL (22) invalid argument if you do.
            KernelFdMode::DirectAuto => flags.unwrap_or(SockFlag::empty()),
        };

        Ok(Self {
            sockfd,
            mode,
            flags,
        })
    }
}

impl<T: AsRawOrDirect> MultishotPayload for AcceptMultishot<T> {
    type Item = OwnedUringFd;

    fn cancel_how(&self) -> CancelHow {
        CancelHow::AsyncCancel
    }

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
        waker: &Waker,
        result: Result<i32, IoError>,
    ) -> Result<Self::Item, IoError> {
        let fd = result?;
        Ok(OwnedUringFd::from_result(fd, self.mode, waker))
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
    ///
    /// The `IORING_TIMEOUT_MULTISHOT` and `IORING_TIMEOUT_ETIME_SUCCESS` flags
    /// are added by default.
    pub fn new(interval: Duration, count: u32, flags: Option<TimeoutFlags>) -> Self {
        Self {
            timespec: Timespec::from(interval),
            count,
            flags: TimeoutFlags::MULTISHOT
                | TimeoutFlags::ETIME_SUCCESS
                | flags.unwrap_or(TimeoutFlags::empty()),
        }
    }
}

impl MultishotPayload for TimeoutMultishot {
    type Item = ();

    fn cancel_how(&self) -> CancelHow {
        CancelHow::TimeoutRemove
    }

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
        _waker: &Waker,
        result: Result<i32, IoError>,
    ) -> Result<Self::Item, IoError> {
        match result {
            Ok(_) => Ok(()),
            // Expired timeout is a success case.
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
    use crate::future::lib::{BorrowedUringFd, UringFdKind};
    use crate::test_utils::*;
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
                BorrowedUringFd::new_raw(listener.as_raw_fd()),
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
                            new_sockfd.leak_raw().context("cant leak raw fd")?,
                        )
                    },
                    UringFdKind::Fixed => panic!("should not be fixed"),
                };

                stream.write_all(hello)?;
                got += 1;
            }

            handle.join().unwrap()?;
        } // stream dropped here, schedules async cancel task

        assert_inflight_cleanup(1);
        wait_for_cleanup().await;
        assert_inflight_cleanup(0);

        Ok(())
    }
}
