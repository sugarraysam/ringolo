use crate::future::opcode::{OpParams, OpPayload, parse};
use crate::runtime::cancel::CancelOutputT;
use crate::sqe::IoError;
use anyhow::{Context, Result};
use io_uring::types::{Fd, TimeoutFlags, Timespec};
use pin_project::pin_project;
use std::io;
use std::mem::MaybeUninit;
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::fd::AsRawFd;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::time::Duration;

#[derive(Debug)]
#[pin_project]
pub struct AcceptOp {
    fd: RawFd,
    flags: i32,
    with_addr: bool,

    // These two fields are self-referential and address will remain stable until
    // the operation completes.
    #[pin]
    addr: MaybeUninit<libc::sockaddr_storage>, // Can fit Ipv4 or Ipv6

    #[pin]
    addrlen: MaybeUninit<libc::socklen_t>,
}

impl AcceptOp {
    pub fn new(fd: &impl AsRawFd, with_addr: bool) -> Self {
        let mut addrlen = MaybeUninit::uninit();
        if with_addr {
            addrlen.write(std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t);
        }

        Self {
            fd: fd.as_raw_fd(),
            addr: MaybeUninit::uninit(),
            addrlen,
            flags: libc::SOCK_CLOEXEC,
            with_addr,
        }
    }

    pub fn flags(mut self, flags: i32) -> Self {
        self.flags |= flags;
        self
    }
}

impl OpPayload for AcceptOp {
    type Output = io::Result<(RawFd, Option<SocketAddr>)>;

    fn create_params(self: Pin<&mut Self>) -> OpParams {
        let this = self.project();

        let (addr_ptr, addrlen_ptr) = if *this.with_addr {
            (
                this.addr.get_mut().as_mut_ptr() as *mut libc::sockaddr,
                this.addrlen.get_mut().as_mut_ptr() as *mut libc::socklen_t,
            )
        } else {
            (std::ptr::null_mut(), std::ptr::null_mut())
        };

        io_uring::opcode::Accept::new(Fd(*this.fd), addr_ptr, addrlen_ptr)
            .flags(*this.flags)
            .build()
            .into()
    }

    fn into_output(self: Pin<&mut Self>, result: Result<i32, IoError>) -> Self::Output {
        let this = self.project();

        // SAFETY(1): Will return *immediately* if there was an error.
        let fd = result?.as_raw_fd();

        let addr_info = if *this.with_addr {
            // SAFETY (2): We know the result was successful as we unwrapped above.
            // SAFETY (3): If the `accept` syscall was successful, we can safely assume
            // that the kernel has written valid data into our `addr` and `addrlen`
            // buffers. We can now treat them as initialized.
            unsafe {
                let addr = this.addr.assume_init_read();
                let addrlen = this.addrlen.assume_init_read();
                let addr = parse::sockaddr_storage_to_socket_addr(&addr, addrlen)?;
                Some(addr)
            }
        } else {
            None
        };

        Ok((fd, addr_info))
    }
}

// Safety: ok *to not implement* drop even though we have MaybeUninit fields because
// they are POD.

#[derive(Debug, Clone)]
pub(crate) struct AsyncCancelOp {
    entry: Option<io_uring::squeue::Entry>,
}

impl AsyncCancelOp {
    pub(crate) fn new(builder: io_uring::types::CancelBuilder) -> Self {
        Self {
            entry: Some(io_uring::opcode::AsyncCancel2::new(builder).build()),
        }
    }
}

impl OpPayload for AsyncCancelOp {
    type Output = CancelOutputT;

    fn create_params(mut self: Pin<&mut Self>) -> OpParams {
        self.entry.take().expect("only called once").into()
    }

    fn into_output(self: Pin<&mut Self>, result: Result<i32, IoError>) -> Self::Output {
        // Untouched, we want to rely on IoError `is_retryable` logic.
        result
    }
}

#[pin_project]
pub struct TimeoutOp {
    #[pin]
    timespec: Timespec,
    flags: TimeoutFlags,
}

// TODO: create separate for multishot
impl TimeoutOp {
    /// Create a new `TimeoutOp` that will complete after the specified duration.
    /// - If `count` is `None`, it will be a single-shot timeout.
    /// - If `count` is `Some(n)`, the timeout will be multishot and complete `n` times.
    /// - If `count` is `Some(0)`, it will be an infinite multishot timeout.
    pub fn new(when: Duration) -> Self {
        Self {
            timespec: Timespec::from(when),
            flags: TimeoutFlags::empty(),
        }
    }

    pub fn flags(mut self, flags: TimeoutFlags) -> Self {
        self.flags |= flags;
        self
    }
}

impl OpPayload for TimeoutOp {
    type Output = io::Result<()>;

    fn create_params(self: Pin<&mut Self>) -> OpParams {
        let this = self.project();

        let timespec_addr = &*this.timespec as *const Timespec;

        io_uring::opcode::Timeout::new(timespec_addr)
            .count(1)
            .flags(*this.flags)
            .build()
            .into()
    }

    fn into_output(self: Pin<&mut Self>, result: Result<i32, IoError>) -> Self::Output {
        match result {
            Ok(_) => Ok(()),
            Err(IoError::Io(e)) if e.raw_os_error() == Some(libc::ETIME) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

// TODO:
// - need call Socket + SetSockOpt
// - replace in accept test, need a receiving socket, need to create a new socket
// - create connect chain
#[pin_project]
pub struct ConnectOp {
    fd: RawFd,

    #[pin]
    addr: SocketAddr,
}

impl ConnectOp {
    pub fn new<A: ToSocketAddrs>(fd: RawFd, addr: A) -> Result<Self> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .context("no addresses found")?;

        Ok(Self { fd, addr })
    }
}

impl OpPayload for ConnectOp {
    type Output = io::Result<RawFd>;

    fn create_params(mut self: Pin<&mut Self>) -> OpParams {
        let this = self.as_mut().project();

        let addr_ptr = &*this.addr as *const _ as *const libc::sockaddr;
        let addr_len = std::mem::size_of_val(&*this.addr) as libc::socklen_t;

        io_uring::opcode::Connect::new(Fd(*this.fd), addr_ptr, addr_len)
            .build()
            .into()
    }

    fn into_output(self: Pin<&mut Self>, result: Result<i32, IoError>) -> Self::Output {
        // let this = self.project();
        // result.map(|_| *this.fd) // TODO
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, TcpListener, TcpStream};

    use super::*;
    use crate::{self as ringolo, future::opcode::Op};
    use anyhow::Result;

    #[ringolo::test]
    async fn test_accept() -> Result<()> {
        // (1) EBADF - test error does not trigger UB when reading addr
        let op = Op::new(AcceptOp::new(&42, true));
        let res = op.await;

        assert!(res.is_err());
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::EBADF));

        // (2) Success
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let listen_addr = listener.local_addr()?;
        let listener_fd = listener.as_raw_fd();

        let handle = std::thread::spawn(move || {
            let mut _stream = TcpStream::connect(listen_addr)?;
            Ok(())
        });

        let op = Op::new(AcceptOp::new(&listener_fd, true));
        let res = op.await;
        assert!(res.is_ok());

        let (fd, got_addr) = res.unwrap();
        assert!(fd > 0);
        assert!(got_addr.is_some());
        assert_eq!(got_addr.unwrap().ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));

        let res: Result<()> = handle.join().unwrap();
        assert!(res.is_ok());

        Ok(())
    }
}
