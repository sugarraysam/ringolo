#![allow(dead_code)]

use crate::future::opcode::{KernelFdMode, OpParams, OpPayload, OpcodeError, UringFd, parse};
use crate::runtime::cancel::CancelOutputT;
use crate::sqe::IoError;
use anyhow::{Context, Result};
use io_uring::types::{TimeoutFlags, Timespec};
use nix::sys::socket::{AddressFamily, SockFlag, SockProtocol, SockType};
use pin_project::pin_project;
use std::mem::MaybeUninit;
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::time::Duration;

// TODO:
// - setsockopt

///
/// === Accept ===
///
#[derive(Debug)]
#[pin_project]
pub struct AcceptOp {
    sockfd: UringFd,
    flags: SockFlag,
    mode: KernelFdMode,
    with_addr: bool,

    // Safety: we have T from MaybeUninit<T> as POD so don't need manual drop.
    #[pin]
    addr: MaybeUninit<libc::sockaddr_storage>, // Can fit Ipv4 or Ipv6

    #[pin]
    addrlen: MaybeUninit<libc::socklen_t>,
}

impl AcceptOp {
    pub fn new(
        sockfd: UringFd,
        mode: KernelFdMode,
        with_addr: bool,
        flags: Option<SockFlag>,
    ) -> Self {
        let mut addrlen = MaybeUninit::uninit();
        if with_addr {
            addrlen.write(std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t);
        }

        Self {
            sockfd,
            flags: SockFlag::SOCK_CLOEXEC | flags.unwrap_or(SockFlag::empty()),
            with_addr,
            mode,
            addr: MaybeUninit::uninit(),
            addrlen,
        }
    }
}

impl OpPayload for AcceptOp {
    type Output = (UringFd, Option<SocketAddr>);

    fn create_params(self: Pin<&mut Self>) -> Result<OpParams, OpcodeError> {
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

        Ok((UringFd::from_result(fd, *this.mode), addr_info))
    }
}

///
/// === AsyncCancelOp ===
///
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

    fn create_params(mut self: Pin<&mut Self>) -> Result<OpParams, OpcodeError> {
        Ok(self.entry.take().expect("only called once").into())
    }

    fn into_output(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Output, IoError> {
        // Untouched, we want to rely on IoError `is_retryable` logic.
        Ok(result)
    }
}

///
/// === Bind ===
///
// #[derive(Debug)]
#[pin_project]
pub struct BindOp {
    sockfd: UringFd,

    #[pin]
    addr: parse::SocketAddrCRepr,
    addr_len: libc::socklen_t,
}

impl BindOp {
    pub fn try_new(sockfd: UringFd, addr: &impl ToSocketAddrs) -> Result<Self> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .context("could not resolve to socket address")?;

        let (addr, addr_len) = parse::socket_addr_to_c(&addr);

        Ok(Self {
            sockfd,
            addr,
            addr_len,
        })
    }
}

impl OpPayload for BindOp {
    type Output = ();

    fn create_params(self: Pin<&mut Self>) -> Result<OpParams, OpcodeError> {
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
#[derive(Debug)]
pub struct CloseOp {
    fd: UringFd,
}

impl CloseOp {
    pub fn new(fd: UringFd) -> Self {
        Self { fd }
    }
}

impl OpPayload for CloseOp {
    type Output = ();

    fn create_params(self: Pin<&mut Self>) -> Result<OpParams, OpcodeError> {
        let entry = resolve_fd!(self.fd, |fd| io_uring::opcode::Close::new(fd));
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
/// === Connect ===
///
#[derive(Debug)]
#[pin_project]
pub struct ConnectOp {
    sockfd: UringFd,

    #[pin]
    addr: parse::SocketAddrCRepr,
    addr_len: libc::socklen_t,
}

impl ConnectOp {
    pub fn try_new(sockfd: UringFd, addr: &impl ToSocketAddrs) -> Result<Self> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .context("could not resolve to socket address")?;

        let (addr, addr_len) = parse::socket_addr_to_c(&addr);

        Ok(Self {
            sockfd,
            addr,
            addr_len,
        })
    }
}

impl OpPayload for ConnectOp {
    type Output = ();

    fn create_params(mut self: Pin<&mut Self>) -> Result<OpParams, OpcodeError> {
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
pub struct ListenOp {
    sockfd: UringFd,
    backlog: i32,
}

impl ListenOp {
    pub fn new(sockfd: UringFd, backlog: i32) -> Self {
        Self { sockfd, backlog }
    }
}

impl OpPayload for ListenOp {
    type Output = ();

    fn create_params(self: Pin<&mut Self>) -> Result<OpParams, OpcodeError> {
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
/// === Socket ===
///
#[derive(Debug)]
pub struct SocketOp {
    mode: KernelFdMode,
    addr_family: AddressFamily,
    sock_type: SockType,
    protocol: SockProtocol,
}

impl SocketOp {
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

impl OpPayload for SocketOp {
    type Output = UringFd;

    fn create_params(self: Pin<&mut Self>) -> Result<OpParams, OpcodeError> {
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
        Ok(UringFd::from_result(result?, self.mode))
    }
}

///
/// === Timeout ===
///
#[pin_project]
pub struct TimeoutOp {
    #[pin]
    timespec: Timespec,
    flags: TimeoutFlags,
}

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
    type Output = ();

    fn create_params(self: Pin<&mut Self>) -> Result<OpParams, OpcodeError> {
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
            Err(IoError::Io(e)) if e.raw_os_error() == Some(libc::ETIME) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use super::*;
    use crate::{self as ringolo, future::opcode::Op, task::JoinHandle};
    use anyhow::Result;
    use rstest::rstest;

    const LOCALHOST4: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    const LOCALHOST6: SocketAddr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);

    #[ringolo::test]
    async fn test_accept_safe_unpacking() -> Result<()> {
        // (1) EBADF - test error does not trigger UB when reading addr
        let op = Op::new(AcceptOp::new(42.into(), KernelFdMode::Legacy, true, None));
        let res = op.await;

        assert!(res.is_err());
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::EBADF));

        Ok(())
    }

    // TODO:
    // - KernelFdMod::DirectAuto, dont use `:0` dynamic port, pre-allocate port
    // - impl Close, need to pass Either, can't keep a ref count on UringFd
    #[rstest]
    #[case::legacy_ipv4(KernelFdMode::Legacy, AddressFamily::Inet, LOCALHOST4)]
    #[case::legacy_ipv6(KernelFdMode::Legacy, AddressFamily::Inet6, LOCALHOST6)]
    #[ringolo::test]
    async fn test_socket_bind_listen_accept_connect(
        #[case] mode: KernelFdMode,
        #[case] addr_family: AddressFamily,
        #[case] bind_addr: SocketAddr,
    ) -> Result<()> {
        // (1) Create listening socket
        let socket_op = Op::new(SocketOp::new(
            mode,
            addr_family,
            SockType::Stream,
            SockProtocol::Tcp,
        ));

        let listener_fd = socket_op.await.expect("server socket creation failed");

        // (2) Bind to address
        let bind_op = Op::new(BindOp::try_new(listener_fd.clone(), &bind_addr)?);
        assert!(bind_op.await.is_ok());

        // (3) Listen
        let listen_op = Op::new(ListenOp::new(listener_fd.clone(), 128));
        assert!(listen_op.await.is_ok());

        let dynamic_addr = listener_fd
            .socket_addr()
            .expect("could not get socket address");

        // (4) Spawn Connect
        let handle: JoinHandle<Result<()>> = ringolo::spawn(async move {
            let socket_op = Op::new(SocketOp::new(
                mode,
                addr_family,
                SockType::Stream,
                SockProtocol::Tcp,
            ));

            let sockfd = socket_op.await.expect("client socket creation failed");

            // TODO:
            // - cant call `getsockname` on Direct descriptor? How to find which port the kernel dynamically allocated?
            let connect_op = Op::new(ConnectOp::try_new(sockfd, &dynamic_addr)?);
            assert!(connect_op.await.is_ok());

            Ok(())
        });

        // (5) Accept
        let op = Op::new(AcceptOp::new(listener_fd, KernelFdMode::Legacy, true, None));
        let res = op.await;
        assert!(res.is_ok());

        let (fd, got_addr) = res.unwrap();
        assert!(fd > 0);
        assert!(got_addr.is_some());
        assert_eq!(got_addr.unwrap().ip(), bind_addr.ip());

        assert!(handle.await.is_ok());

        Ok(())
    }
}
