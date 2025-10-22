#![allow(dead_code)]
use crate::future::lib::{
    AnySockOpt, AsRawOrDirect, KernelFdMode, OpPayload, OpcodeError, OwnedUringFd, SetSockOptIf,
    parse,
};
use crate::sqe::IoError;
use either::Either;
use io_uring::squeue::Entry;
use io_uring::types::{Fd, Fixed, TimeoutFlags, Timespec};
use nix::sys::socket::{AddressFamily, SockFlag, SockProtocol, SockType};
use pin_project::pin_project;
use std::io;
use std::mem::MaybeUninit;
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::time::Duration;

///
/// === Accept ===
///
#[derive(Debug)]
#[pin_project]
pub struct Accept<T: AsRawOrDirect> {
    sockfd: T,
    flags: SockFlag,
    mode: KernelFdMode,
    with_addr: bool,

    // Safety: we have T from MaybeUninit<T> as POD so don't need manual drop.
    #[pin]
    addr: MaybeUninit<libc::sockaddr_storage>, // Can fit Ipv4 or Ipv6

    #[pin]
    addrlen: MaybeUninit<libc::socklen_t>,
}

impl<T: AsRawOrDirect> Accept<T> {
    pub fn new(sockfd: T, mode: KernelFdMode, with_addr: bool, flags: Option<SockFlag>) -> Self {
        let mut addrlen = MaybeUninit::uninit();
        if with_addr {
            addrlen.write(std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t);
        }

        let flags = match mode {
            KernelFdMode::Legacy => SockFlag::SOCK_CLOEXEC | flags.unwrap_or(SockFlag::empty()),
            // The ring itself is CLOEXEC, no need to set for direct descriptor. In fact it
            // triggers -EINVAL (22) invalid argument if you do.
            _ => flags.unwrap_or(SockFlag::empty()),
        };

        Self {
            sockfd,
            flags,
            with_addr,
            mode,
            addr: MaybeUninit::uninit(),
            addrlen,
        }
    }
}

impl<T: AsRawOrDirect> OpPayload for Accept<T> {
    type Output = (OwnedUringFd, Option<SocketAddr>);

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();

        let (addr_ptr, addrlen_ptr) = if *this.with_addr {
            (
                this.addr.get_mut().as_mut_ptr().cast(),
                this.addrlen.get_mut().as_mut_ptr().cast(),
            )
        } else {
            (std::ptr::null_mut(), std::ptr::null_mut())
        };

        let entry = resolve_fd!(this.sockfd, |fd| {
            io_uring::opcode::Accept::new(fd, addr_ptr, addrlen_ptr)
                .file_index(this.mode.try_into_slot()?)
                .flags(this.flags.bits())
        });

        Ok(entry.build().into())
    }

    fn into_output(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Output, IoError> {
        let this = self.project();

        // SAFETY(1): Will return *immediately* if there was an error.
        let fd = result?.as_raw_fd();

        let addr_info = if *this.with_addr {
            // SAFETY (2): We know the result was successful as we unwrapped above.
            // SAFETY (3): If the `accept` syscall was successful, we can safely assume
            // that the kernel has written valid data into our `addr` and `addrlen`
            // buffers. We can now treat them as initialized.
            unsafe {
                let addr = this.addr.as_ptr();
                let addrlen = this.addrlen.assume_init_read();
                let addr = parse::socket_addr_from_c(addr, addrlen as usize)?;
                Some(addr)
            }
        } else {
            None
        };

        Ok((OwnedUringFd::from_result(fd, *this.mode), addr_info))
    }
}

///
/// === AsyncCancel ===
///
#[derive(Debug, Clone)]
pub(crate) struct AsyncCancel {
    entry: Option<io_uring::squeue::Entry>,
}

impl AsyncCancel {
    pub(crate) fn new(builder: io_uring::types::CancelBuilder) -> Self {
        Self {
            entry: Some(io_uring::opcode::AsyncCancel2::new(builder).build()),
        }
    }
}

impl OpPayload for AsyncCancel {
    type Output = i32;

    fn create_entry(mut self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        Ok(self.entry.take().expect("only called once").into())
    }

    fn into_output(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res)
    }
}

///
/// === Bind ===
///
#[derive(Debug)]
#[pin_project]
pub struct Bind<T: AsRawOrDirect> {
    sockfd: T,

    #[pin]
    addr: parse::SocketAddrCRepr,
    addr_len: libc::socklen_t,
}

impl<T: AsRawOrDirect> Bind<T> {
    pub fn new(sockfd: T, addr: &SocketAddr) -> Self {
        let (addr, addr_len) = parse::socket_addr_to_c(&addr);

        Self {
            sockfd,
            addr,
            addr_len,
        }
    }
}

impl<T: AsRawOrDirect> OpPayload for Bind<T> {
    type Output = ();

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();

        let entry = resolve_fd!(this.sockfd, |fd| {
            io_uring::opcode::Bind::new(fd, this.addr.as_ptr(), *this.addr_len)
        });

        Ok(entry.build().into())
    }

    fn into_output(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Output, IoError> {
        let _ = result?;
        Ok(())
    }
}

///
/// === Close ===
///
#[derive(Debug, Clone)]
pub struct Close {
    fd: Either<Fd, Fixed>,
}

impl Close {
    /// Does not take an OwnedUringFd as we intend to use this in Drop impls to
    /// perform async cleanup.
    pub fn new(fd: Either<Fd, Fixed>) -> Self {
        Self { fd }
    }
}

impl OpPayload for Close {
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let entry = match self.fd {
            Either::Left(raw) => io_uring::opcode::Close::new(raw),
            Either::Right(fixed) => io_uring::opcode::Close::new(fixed),
        };

        Ok(entry.build().into())
    }

    fn into_output(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res)
    }
}

///
/// === Connect ===
///
#[derive(Debug)]
#[pin_project]
pub struct Connect<T: AsRawOrDirect> {
    sockfd: T,

    #[pin]
    addr: parse::SocketAddrCRepr,
    addr_len: libc::socklen_t,
}

impl<T: AsRawOrDirect> Connect<T> {
    pub fn new(sockfd: T, addr: &SocketAddr) -> Self {
        let (addr, addr_len) = parse::socket_addr_to_c(&addr);

        Self {
            sockfd,
            addr,
            addr_len,
        }
    }
}

impl<T: AsRawOrDirect> OpPayload for Connect<T> {
    type Output = ();

    fn create_entry(mut self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.as_mut().project();

        let entry = resolve_fd!(this.sockfd, |fd| {
            io_uring::opcode::Connect::new(fd, this.addr.as_ptr(), *this.addr_len)
        });

        Ok(entry.build().into())
    }

    fn into_output(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Output, IoError> {
        let _ = result?;
        Ok(())
    }
}

///
/// === Listen ===
///
#[derive(Debug)]
pub struct Listen<T: AsRawOrDirect> {
    sockfd: T,
    backlog: i32,
}

impl<T: AsRawOrDirect> Listen<T> {
    pub fn new(sockfd: T, backlog: i32) -> Self {
        Self { sockfd, backlog }
    }
}

impl<T: AsRawOrDirect> OpPayload for Listen<T> {
    type Output = ();

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let entry = resolve_fd!(self.sockfd, |fd| {
            io_uring::opcode::Listen::new(fd, self.backlog)
        });

        Ok(entry.build().into())
    }

    fn into_output(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Output, IoError> {
        let _ = result?;
        Ok(())
    }
}

///
/// === Nop ===
///
#[derive(Debug)]
pub struct Nop;

impl OpPayload for Nop {
    type Output = ();

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        Ok(io_uring::opcode::Nop::new().build().into())
    }

    fn into_output(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Output, IoError> {
        let _ = result?;
        Ok(())
    }
}

///
/// === Socket ===
///
#[derive(Debug)]
pub struct Socket {
    mode: KernelFdMode,
    addr_family: AddressFamily,
    sock_type: SockType,
    protocol: SockProtocol,
}

impl Socket {
    pub fn new(
        mode: KernelFdMode,
        addr_family: AddressFamily,
        sock_type: SockType,
        protocol: SockProtocol,
    ) -> Self {
        Self {
            mode,
            addr_family,
            sock_type,
            protocol,
        }
    }
}

impl OpPayload for Socket {
    type Output = OwnedUringFd;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        Ok(io_uring::opcode::Socket::new(
            self.addr_family as i32,
            self.sock_type as i32,
            self.protocol as i32,
        )
        .file_index(self.mode.try_into_slot()?)
        .build()
        .into())
    }

    fn into_output(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Output, IoError> {
        Ok(OwnedUringFd::from_result(result?, self.mode))
    }
}

///
/// === SetSockOpt ===
///
#[derive(Debug)]
#[pin_project]
pub struct SetSockOpt<T: AsRawOrDirect> {
    sockfd: T,

    /// The opt is a self-contained struct that generates stable c ptrs when
    /// we unpack it on first poll. We use it as an entry generator and it's
    /// role is to inject the appropriate arguments in our SQE.
    #[pin]
    opt: AnySockOpt,
}

impl<T: AsRawOrDirect> SetSockOpt<T> {
    pub fn new<O: Into<AnySockOpt>>(sockfd: T, opt: O) -> Self {
        Self {
            sockfd,
            opt: opt.into(),
        }
    }
}

impl<T: AsRawOrDirect> OpPayload for SetSockOpt<T> {
    type Output = ();

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();
        this.opt.create_entry(this.sockfd)
    }

    fn into_output(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Output, IoError> {
        let _ = result?;
        Ok(())
    }
}

///
/// === Timeout ===
///
#[derive(Debug)]
#[pin_project]
pub struct Timeout {
    #[pin]
    timespec: Timespec,
    flags: TimeoutFlags,
}

impl Timeout {
    /// Create a new `Timeout` that will complete after the specified duration.
    /// - If `count` is `None`, it will be a single-shot timeout.
    /// - If `count` is `Some(n)`, the timeout will be multishot and complete `n` times.
    /// - If `count` is `Some(0)`, it will be an infinite multishot timeout.
    ///
    /// The `IORING_TIMEOUT_ETIME_SUCCESS` flag is added by default as we consider
    /// an expired timeout as success.
    pub fn new(when: Duration, flags: Option<TimeoutFlags>) -> Self {
        Self {
            timespec: Timespec::from(when),
            flags: TimeoutFlags::ETIME_SUCCESS | flags.unwrap_or(TimeoutFlags::empty()),
        }
    }
}

impl OpPayload for Timeout {
    type Output = ();

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();

        let timespec_addr = std::ptr::from_ref(&*this.timespec).cast();

        Ok(io_uring::opcode::Timeout::new(timespec_addr)
            .count(1)
            .flags(*this.flags)
            .build()
            .into())
    }

    fn into_output(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Output, IoError> {
        match result {
            Ok(_) => Ok(()),
            // Expired timeout yield -ETIME but this is a success case.
            Err(IoError::Io(e)) if e.raw_os_error() == Some(libc::ETIME) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use super::*;
    use crate::future::lib::{BorrowedUringFd, ReuseAddr};
    use crate::test_utils::*;
    use crate::{self as ringolo, future::lib::Op, task::JoinHandle};
    use anyhow::{Context, Result};
    use rstest::rstest;

    #[ringolo::test]
    async fn test_accept_safe_unpacking() -> Result<()> {
        // (1) EBADF - test error does not trigger UB when reading addr
        let op = Op::new(Accept::new(
            BorrowedUringFd::new_raw(42),
            KernelFdMode::Legacy,
            true,
            None,
        ));
        let res = op.await;

        assert!(res.is_err());
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::EBADF));

        Ok(())
    }

    // TODO: impl `getsockname` on direct descriptor + use dynamic port allocation
    #[rstest]
    #[case::legacy_ipv4(KernelFdMode::Legacy, AddressFamily::Inet, LOCALHOST4, 9000)]
    #[case::legacy_ipv6(KernelFdMode::Legacy, AddressFamily::Inet6, LOCALHOST6, 9001)]
    #[case::auto_ipv4(KernelFdMode::DirectAuto, AddressFamily::Inet, LOCALHOST4, 9002)]
    #[case::auto_ipv6(KernelFdMode::DirectAuto, AddressFamily::Inet6, LOCALHOST6, 9003)]
    #[ringolo::test]
    async fn test_socket_bind_listen_accept_connect(
        #[case] mode: KernelFdMode,
        #[case] addr_family: AddressFamily,
        #[case] ip_addr: IpAddr,
        #[case] port: u16,
    ) -> Result<()> {
        let sock_addr = SocketAddr::new(ip_addr, port);

        // (1) Create listening socket + set SO_REUSEADDR
        let listener_fd = tcp_socket(mode, addr_family)
            .await
            .expect("server socket creation failed");
        let listener_ref = listener_fd.borrow();

        Op::new(SetSockOpt::new(listener_ref, ReuseAddr::new(true)))
            .await
            .context("failed to set SO_REUSEADDR")?;

        // (2) Bind to address
        let bind_op = Op::new(Bind::new(listener_ref, &sock_addr));
        bind_op.await.context("bind failed")?;

        // (3) Listen
        let listen_op = Op::new(Listen::new(listener_ref, 128));
        listen_op.await.context("listen failed")?;

        // (4) Spawn Connect
        let handle: JoinHandle<Result<()>> = ringolo::spawn(async move {
            let sockfd = tcp_socket(mode, addr_family)
                .await
                .expect("client socket creation failed");

            let connect_op = Op::new(Connect::new(sockfd, &sock_addr));
            connect_op.await.context("connect failed")?;

            Ok(())
        });

        // (5) Accept
        let op = Op::new(Accept::new(listener_ref, mode, true, None));
        let (_fd, got_addr) = op.await.context("accept failed")?;

        assert!(got_addr.is_some());
        assert_eq!(got_addr.unwrap().ip(), ip_addr);

        handle.await.context("client task failed")??;

        Ok(())
    }
}
