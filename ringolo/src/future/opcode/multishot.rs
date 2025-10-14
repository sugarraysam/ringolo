use crate::future::opcode::{Fd, MultishotParams, MultishotPayload};
use crate::sqe::IoError;
use anyhow::Result;
use io_uring::opcode::Accept;
use io_uring::types::{TimeoutFlags, Timespec};
use pin_project::pin_project;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::time::Duration;

#[derive(Debug)]
pub struct AcceptMultishot {
    fd: RawFd,
    allocate_file_index: bool,
    flags: i32,
}

impl AcceptMultishot {
    /// The `allocate_file_index` flag maps to the `IORING_FILE_INDEX_ALLOC` from the docs.
    /// The kernel will dynamically choose a direct descriptor index if this option is true.
    /// You need to have registered direct descriptors prior for this to work.
    pub fn new(fd: &impl AsRawFd, allocate_file_index: bool, flags: Option<i32>) -> Self {
        Self {
            fd: fd.as_raw_fd(),
            allocate_file_index,
            flags: flags.unwrap_or(0),
        }
    }
}

impl MultishotPayload for AcceptMultishot {
    type Item = io::Result<Fd>;

    fn create_params(self: Pin<&mut Self>) -> MultishotParams {
        let entry = io_uring::opcode::AcceptMulti::new(io_uring::types::Fd(self.fd))
            .allocate_file_index(self.allocate_file_index)
            .flags(self.flags)
            .build();

        MultishotParams::new(entry, 0 /* infinite */)
    }

    fn into_next(self: Pin<&mut Self>, result: Result<i32, IoError>) -> Self::Item {
        match result {
            Ok(fd) => Ok(if self.allocate_file_index {
                Fd::Registered(fd as u32)
            } else {
                // TODO: lookup registered fd table for this thread, and convert
                // to a TcpStream directly.
                Fd::Unregistered(fd)
            }),
            Err(e) => Err(e.into()),
        }
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

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{IpAddr, Ipv4Addr, TcpListener, TcpStream},
        os::fd::FromRawFd,
        sync::{Arc, Barrier, atomic::AtomicBool},
    };

    use super::*;
    use crate::{self as ringolo, future::opcode::Multishot};
    use anyhow::Result;
    use futures::StreamExt;

    #[ignore = "test hanging, can only accept 1 ? client side fine"]
    #[ringolo::test]
    async fn test_accept_multi() -> Result<()> {
        let n = 2;
        let hello = b"hello";

        let barrier = Arc::new(Barrier::new(2));
        let clone_barrier = Arc::clone(&barrier);

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let listen_addr = listener.local_addr()?;

        let handle = std::thread::spawn(move || -> Result<()> {
            for _ in 0..n {
                if let Ok(mut stream) = TcpStream::connect(listen_addr) {
                    let mut buf = Vec::with_capacity(hello.len());
                    dbg!("waiting for server...");
                    stream.read_to_end(&mut buf)?;
                    assert_eq!(buf, hello);
                    dbg!("client done");
                } else {
                    assert!(false, "failed to connect to server");
                }
            }

            clone_barrier.wait();
            Ok(())
        });

        let mut stream = Multishot::new(AcceptMultishot::new(&listener, false, None)).take(n);

        let mut got = 0;
        while let Some(res) = stream.next().await
            && got < n
        {
            assert!(res.is_ok());

            let mut stream = match res.unwrap() {
                Fd::Unregistered(fd) => unsafe { TcpStream::from_raw_fd(fd.into()) },
                Fd::Registered(_) => panic!("should not be registered"),
            };

            dbg!("writing to client");
            stream.write_all(hello)?;
            got += 1;
            dbg!("server done");
        }

        dbg!("server barrier");
        barrier.wait();
        assert_eq!(got, n);

        handle.join().unwrap()?;
        Ok(())
    }
}
