//! Single-shot `io_uring` opcodes.
//!
//! These types reflect the opcodes available in the `io_uring` submission queue.
//! See the [`io_uring` crate documentation](https://docs.rs/io-uring/latest/io_uring/opcode/index.html)
//! for all supported opcodes.

use crate::future::lib::fd::AsRawOrDirect;
use crate::future::lib::ops::sockopt::{AnySockOpt, SetSockOptIf};
use crate::future::lib::parse::{bytes_to_cstring, path_to_cstring};
use crate::future::lib::types::{
    AddressFamily, AtFlags, EpollEvent, EpollOp, FallocateFlags, Futex2Flags, FutexWaiter, Mode,
    OFlag, OpenHow, PosixFadviseAdvice, SockFlag, SockProtocol, SockType,
};
use crate::future::lib::{
    BorrowedUringFd, BufferFamily, KernelBufferMode, KernelFdMode, OpPayload, OpcodeError,
    OwnedUringFd, UringBuffer, parse,
};
use crate::sqe::{CqeRes, IoError};
use anyhow::anyhow;
use either::Either;
use io_uring::squeue::Entry;
use io_uring::types::{Fd, Fixed, FsyncFlags, TimeoutFlags, Timespec};
use pin_project::pin_project;
use std::ffi::CString;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;
use std::pin::Pin;
use std::task::Waker;
use std::time::Duration;

/// Accept a new connection on a socket.
///
/// Corresponds to [`io_uring_prep_accept`](https://man.archlinux.org/man/io_uring_prep_accept.3.en).
#[derive(Debug)]
#[pin_project]
pub struct Accept<'a> {
    sockfd: BorrowedUringFd<'a>,
    flags: SockFlag,
    fd_mode: KernelFdMode,
    with_addr: bool,
    // Safety: we have T from MaybeUninit<T> as POD so don't need manual drop.
    #[pin]
    addr: MaybeUninit<libc::sockaddr_storage>, // Can fit Ipv4 or Ipv6
    #[pin]
    addrlen: MaybeUninit<libc::socklen_t>,
}

impl<'a> Accept<'a> {
    /// Create a new `Accept` operation.
    pub fn new(
        sockfd: BorrowedUringFd<'a>,
        fd_mode: KernelFdMode,
        with_addr: bool,
        flags: Option<SockFlag>,
    ) -> Self {
        let mut addrlen = MaybeUninit::uninit();
        if with_addr {
            addrlen.write(std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t);
        }

        let flags = match fd_mode {
            KernelFdMode::Legacy => SockFlag::SOCK_CLOEXEC | flags.unwrap_or(SockFlag::empty()),
            // The ring itself is CLOEXEC, no need to set for direct descriptor. In fact it
            // triggers -EINVAL (22) invalid argument if you do.
            _ => flags.unwrap_or(SockFlag::empty()),
        };

        Self {
            sockfd,
            flags,
            with_addr,
            fd_mode,
            addr: MaybeUninit::uninit(),
            addrlen,
        }
    }
}

impl<'a> OpPayload for Accept<'a> {
    type Output = (OwnedUringFd, Option<SocketAddr>);

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();

        let (addr_ptr, addrlen_ptr) = if *this.with_addr {
            (
                this.addr.get_mut().as_mut_ptr(),
                this.addrlen.get_mut().as_mut_ptr(),
            )
        } else {
            (std::ptr::null_mut(), std::ptr::null_mut())
        };

        let entry = resolve_fd!(this.sockfd, |fd| {
            io_uring::opcode::Accept::new(fd, addr_ptr.cast(), addrlen_ptr)
                .file_index(this.fd_mode.try_into_slot()?)
                .flags(this.flags.bits())
        });

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let this = self.project();

        // SAFETY(1): Will return *immediately* if there was an error.
        let fd = result?.res.as_raw_fd();

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

        Ok((
            OwnedUringFd::from_result(fd, *this.fd_mode, waker),
            addr_info,
        ))
    }
}

/// Cancel a previously submitted request.
///
/// Corresponds to [`io_uring_prep_cancel`](https://man.archlinux.org/man/io_uring_prep_cancel.3.en).
#[derive(Debug, Clone)]
pub struct AsyncCancel {
    entry: Option<io_uring::squeue::Entry>,
}

impl AsyncCancel {
    /// Create a new `AsyncCancel` operation.
    pub fn new(builder: io_uring::types::CancelBuilder) -> Self {
        Self {
            entry: Some(io_uring::opcode::AsyncCancel2::new(builder).build()),
        }
    }
}

impl OpPayload for AsyncCancel {
    type Output = i32;

    fn create_entry(mut self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        Ok(self.entry.take().expect("only called once"))
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Bind a name to a socket.
///
/// Corresponds to [`io_uring_prep_bind`](https://man.archlinux.org/man/io_uring_prep_bind.3.en).
#[derive(Debug)]
#[pin_project]
pub struct Bind<'a> {
    sockfd: BorrowedUringFd<'a>,
    #[pin]
    addr: parse::SocketAddrCRepr,
    addr_len: libc::socklen_t,
}

impl<'a> Bind<'a> {
    /// Create a new `Bind` operation.
    pub fn new(sockfd: BorrowedUringFd<'a>, addr: &SocketAddr) -> Self {
        let (addr, addr_len) = parse::socket_addr_to_c(addr);

        Self {
            sockfd,
            addr,
            addr_len,
        }
    }
}

impl<'a> OpPayload for Bind<'a> {
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();

        let entry = resolve_fd!(this.sockfd, |fd| {
            io_uring::opcode::Bind::new(fd, this.addr.as_ptr(), *this.addr_len)
        });

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Close a file descriptor.
///
/// Corresponds to [`io_uring_prep_close`](https://man.archlinux.org/man/io_uring_prep_close.3.en).
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

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Initiate a connection on a socket.
///
/// Corresponds to [`io_uring_prep_connect`](https://man.archlinux.org/man/io_uring_prep_connect.3.en).
#[derive(Debug)]
#[pin_project]
pub struct Connect<'a> {
    sockfd: BorrowedUringFd<'a>,

    #[pin]
    addr: parse::SocketAddrCRepr,
    addr_len: libc::socklen_t,
}

impl<'a> Connect<'a> {
    /// Create a new `Connect` operation.
    pub fn new(sockfd: BorrowedUringFd<'a>, addr: &SocketAddr) -> Self {
        let (addr, addr_len) = parse::socket_addr_to_c(addr);

        Self {
            sockfd,
            addr,
            addr_len,
        }
    }
}

impl<'a> OpPayload for Connect<'a> {
    type Output = i32;

    fn create_entry(mut self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.as_mut().project();

        let entry = resolve_fd!(this.sockfd, |fd| {
            io_uring::opcode::Connect::new(fd, this.addr.as_ptr(), *this.addr_len)
        });

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Control an epoll file descriptor.
///
/// Corresponds to `io_uring_prep_epoll_ctl` (no docs available).
#[derive(Debug)]
#[pin_project]
pub struct EpollCtl<'a> {
    epfd: BorrowedUringFd<'a>,
    fd: RawFd,
    op: EpollOp,
    #[pin]
    event: EpollEvent,
}

impl<'a> EpollCtl<'a> {
    /// Create a new `EpollCtl` operation.
    pub fn try_new(
        epfd: BorrowedUringFd<'a>,
        fd: RawFd,
        op: EpollOp,
        event: Option<EpollEvent>,
    ) -> Result<Self, OpcodeError> {
        if event.is_none() && op != EpollOp::EpollCtlDel {
            Err(anyhow!("Event is required when op is not EpollCtlDel").into())
        } else {
            Ok(Self {
                epfd,
                fd,
                op,
                event: event.unwrap_or(EpollEvent::empty()),
            })
        }
    }
}

impl<'a> OpPayload for EpollCtl<'a> {
    type Output = i32;

    fn create_entry(mut self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.as_mut().project();

        let event_ptr = std::ptr::from_ref(&*this.event);

        let entry = resolve_fd!(this.epfd, |epfd| {
            io_uring::opcode::EpollCtl::new(
                epfd,
                io_uring::types::Fd(*this.fd),
                *this.op as i32,
                event_ptr.cast(),
            )
        });

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Wait for an I/O event on an epoll file descriptor.
///
/// Corresponds to [`io_uring_prep_epoll_wait`](https://man.archlinux.org/man/io_uring_prep_epoll_wait.3.en).
#[derive(Debug)]
#[pin_project]
pub struct EpollWait<'a> {
    epfd: BorrowedUringFd<'a>,
    #[pin]
    events: Vec<EpollEvent>,
    max_events: usize,
    timeout_ms: isize,
}

impl<'a> EpollWait<'a> {
    /// Create a new `EpollWait` operation.
    pub fn new(epfd: BorrowedUringFd<'a>, max_events: usize, timeout_ms: isize) -> Self {
        Self {
            epfd,
            // Make sure we initialize memory region to avoid UB. Can't use
            // `with_capacity` because len would be zero.
            events: vec![EpollEvent::empty(); max_events],
            max_events,
            timeout_ms,
        }
    }
}

impl<'a> OpPayload for EpollWait<'a> {
    type Output = (i32, Vec<EpollEvent>);

    fn create_entry(mut self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let mut this = self.as_mut().project();

        let entry = resolve_fd!(this.epfd, |epfd| {
            io_uring::opcode::EpollWait::new(
                epfd,
                this.events.as_mut_ptr().cast(),
                *this.max_events as u32,
            )
            // TODO: pretty sure this is a bug and should be i32, otherwise we can't
            // use `timeout_ms == -1` to wait indefinitely.
            .flags(*this.timeout_ms as u32)
        });

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        let mut this = self.project();
        Ok((res.res, std::mem::take(&mut this.events)))
    }
}

/// Get an extended attribute value.
///
/// Corresponds to [`io_uring_prep_fgetxattr`](https://man.archlinux.org/man/io_uring_prep_fgetxattr.3.en).
#[derive(Debug)]
#[pin_project]
pub struct FGetXattr<'a> {
    fd: BorrowedUringFd<'a>,
    #[pin]
    name: CString,
    #[pin]
    value: Vec<u8>,
    len: u32,
}

impl<'a> FGetXattr<'a> {
    /// Create a new `FGetXattr` operation.
    pub fn try_new(
        fd: BorrowedUringFd<'a>,
        name: impl Into<Vec<u8>>,
        len: u32,
    ) -> Result<Self, OpcodeError> {
        Ok(Self {
            fd,
            name: bytes_to_cstring(name)?,
            value: vec![0u8; len as usize],
            len,
        })
    }
}

impl<'a> OpPayload for FGetXattr<'a> {
    type Output = (i32, Vec<u8>);

    fn create_entry(mut self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let mut this = self.as_mut().project();

        let name_ptr = std::ptr::from_ref(&*this.name);

        let entry = resolve_fd!(this.fd, |fd| {
            io_uring::opcode::FGetXattr::new(
                fd,
                name_ptr.cast(),
                this.value.as_mut_ptr().cast(),
                *this.len,
            )
        });

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let mut this = self.project();
        let res = result?;
        Ok((res.res, std::mem::take(&mut this.value)))
    }
}

/// Set an extended attribute value.
///
/// Corresponds to [`io_uring_prep_fsetxattr`](https://man.archlinux.org/man/io_uring_prep_fsetxattr.3.en).
#[derive(Debug)]
#[pin_project]
pub struct FSetXattr<'a> {
    fd: BorrowedUringFd<'a>,
    #[pin]
    name: CString,
    #[pin]
    value: Vec<u8>,
    flags: i32,
}

impl<'a> FSetXattr<'a> {
    /// Create a new `FSetXattr` operation.
    pub fn try_new(
        fd: BorrowedUringFd<'a>,
        name: impl Into<Vec<u8>>,
        value: Vec<u8>,
        flags: Option<i32>,
    ) -> Result<Self, OpcodeError> {
        let name = CString::new(name.into()).map_err(|e| anyhow!("Invalid name: {:?}", e))?;

        Ok(Self {
            fd,
            name,
            value,
            flags: flags.unwrap_or(0),
        })
    }
}

impl<'a> OpPayload for FSetXattr<'a> {
    type Output = i32;

    fn create_entry(mut self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.as_mut().project();

        let name_ptr = std::ptr::from_ref(&*this.name);
        let value_ptr = std::ptr::from_ref(&*this.value);

        let entry = resolve_fd!(this.fd, |fd| {
            io_uring::opcode::FSetXattr::new(
                fd,
                name_ptr.cast(),
                value_ptr.cast(),
                this.value.len() as u32,
            )
            .flags(*this.flags)
        });

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Predeclare an access pattern for file data.
///
/// Corresponds to [`io_uring_prep_fadvise`](https://man.archlinux.org/man/io_uring_prep_fadvise.3.en).
#[derive(Debug)]
pub struct Fadvise<'a> {
    fd: BorrowedUringFd<'a>,
    offset: libc::off_t,
    len: libc::off_t,
    advice: PosixFadviseAdvice,
}

impl<'a> Fadvise<'a> {
    /// Create a new `Fadvise` operation.
    pub fn new(
        fd: BorrowedUringFd<'a>,
        offset: libc::off_t,
        len: libc::off_t,
        advice: PosixFadviseAdvice,
    ) -> Self {
        Self {
            fd,
            offset,
            len,
            advice,
        }
    }
}

impl<'a> OpPayload for Fadvise<'a> {
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let entry = resolve_fd!(self.fd, |fd| {
            io_uring::opcode::Fadvise::new(fd, self.len, self.advice as i32)
                .offset(self.offset as u64)
        });

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Manipulate file space.
///
/// Corresponds to [`io_uring_prep_fallocate`](https://man.archlinux.org/man/io_uring_prep_fallocate.3.en).
#[derive(Debug)]
pub struct Fallocate<'a> {
    fd: BorrowedUringFd<'a>,
    offset: libc::off_t,
    len: libc::off_t,
    mode: FallocateFlags,
}

impl<'a> Fallocate<'a> {
    /// Create a new `Fallocate` operation.
    pub fn new(
        fd: BorrowedUringFd<'a>,
        offset: libc::off_t,
        len: libc::off_t,
        mode: FallocateFlags,
    ) -> Self {
        Self {
            fd,
            offset,
            len,
            mode,
        }
    }
}

impl<'a> OpPayload for Fallocate<'a> {
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let entry = resolve_fd!(self.fd, |fd| {
            io_uring::opcode::Fallocate::new(fd, self.len as u64)
                .offset(self.offset as u64)
                .mode(self.mode.bits())
        });

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Install a direct/fixed file descriptor.
///
/// Corresponds to [`io_uring_prep_fixed_fd_install`](https://man.archlinux.org/man/io_uring_prep_fixed_fd_install.3.en).
#[derive(Debug)]
pub struct FixedFdInstall {
    fixed: Fixed,
    flags: u32,
}

impl FixedFdInstall {
    /// Create a new `FixedFdInstall` operation.
    pub fn try_new(fd: OwnedUringFd, flags: Option<u32>) -> Result<Self, OpcodeError> {
        Ok(Self {
            // Safety: we are transferring ownership of this fd.
            fixed: Fixed(unsafe { fd.leak_direct()? }),
            flags: flags.unwrap_or(0),
        })
    }
}

impl OpPayload for FixedFdInstall {
    type Output = OwnedUringFd;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let entry = io_uring::opcode::FixedFdInstall::new(self.fixed, self.flags);

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        // Return a new legacy OwnedUringFd, i.e.: a Raw descriptor. We still enforce
        // proper ownership semantics, with RAII to close the descriptor.
        Ok(OwnedUringFd::from_result(
            result?.res,
            KernelFdMode::Legacy,
            waker,
        ))
    }
}

/// Synchronize a file's in-core state with storage device.
///
/// Corresponds to [`io_uring_prep_fsync`](https://man.archlinux.org/man/io_uring_prep_fsync.3.en).
#[derive(Debug)]
pub struct Fsync<'a> {
    fd: BorrowedUringFd<'a>,
    flags: FsyncFlags,
}

impl<'a> Fsync<'a> {
    /// Create a new `Fsync` operation.
    pub fn new(fd: BorrowedUringFd<'a>, flags: Option<FsyncFlags>) -> Self {
        Self {
            fd,
            flags: flags.unwrap_or(FsyncFlags::empty()),
        }
    }
}

impl<'a> OpPayload for Fsync<'a> {
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let entry = resolve_fd!(self.fd, |fd| {
            io_uring::opcode::Fsync::new(fd).flags(self.flags)
        });

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Truncate a file to a specified length.
///
/// Corresponds to [`io_uring_prep_ftruncate`](https://man.archlinux.org/man/io_uring_prep_ftruncate.3.en).
#[derive(Debug)]
pub struct Ftruncate<'a> {
    fd: BorrowedUringFd<'a>,
    len: libc::off_t,
}

impl<'a> Ftruncate<'a> {
    /// Create a new `Ftruncate` operation.
    pub fn new(fd: BorrowedUringFd<'a>, len: libc::off_t) -> Self {
        Self { fd, len }
    }
}

impl<'a> OpPayload for Ftruncate<'a> {
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let entry = resolve_fd!(self.fd, |fd| {
            io_uring::opcode::Ftruncate::new(fd, self.len as u64)
        });

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Wait on a futex.
///
/// Corresponds to [`io_uring_prep_futex_wait`](https://man.archlinux.org/man/io_uring_prep_futex_wait.3.en).
#[derive(Debug)]
#[pin_project]
pub struct FutexWait {
    #[pin]
    futex: u32,
    val: u64,
    mask: u64,
    futex_flags: Futex2Flags,
}

impl FutexWait {
    /// Create a new `FutexWait` operation.
    pub fn new(futex: u32, val: u64, mask: u64, futex_flags: Futex2Flags) -> Self {
        Self {
            futex,
            val,
            mask,
            futex_flags,
        }
    }
}

impl OpPayload for FutexWait {
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();

        let futex_ptr = std::ptr::from_ref(&*this.futex);
        let entry = io_uring::opcode::FutexWait::new(
            futex_ptr,
            *this.val,
            *this.mask,
            this.futex_flags.bits(),
        );

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Wait on multiple futexes at the same time.
///
/// Corresponds to [`io_uring_prep_futex_waitv`](https://man.archlinux.org/man/io_uring_prep_futex_waitv.3.en)
#[derive(Debug)]
#[pin_project]
pub struct FutexWaitV {
    #[pin]
    futexv: Vec<FutexWaiter>,
}

impl FutexWaitV {
    /// Create a new `FutexWaitV` operation.
    pub fn new(futexv: Vec<FutexWaiter>) -> Self {
        Self { futexv }
    }
}

impl OpPayload for FutexWaitV {
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();

        let futexv_ptr = this.futexv.as_ptr();
        let entry = io_uring::opcode::FutexWaitV::new(futexv_ptr.cast(), this.futexv.len() as u32);

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Wake up waiters on a specific futex.
///
/// This operation wakes up at most `val` waiters that are waiting on the
/// futex address. It is equivalent to the `FUTEX_WAKE_BITSET` operation.
///
/// Corresponds to [`io_uring_prep_futex_wake`](https://man.archlinux.org/man/io_uring_prep_futex_wake.3.en)
#[derive(Debug)]
#[pin_project]
pub struct FutexWake {
    #[pin]
    futex: u32,
    val: u64,
    mask: u64,
    futex_flags: Futex2Flags,
}

impl FutexWake {
    /// Create a new `FutexWake` operation.
    ///
    /// # Arguments
    ///
    /// * `futex` - The value of the futex (note: this operation wakes the address of this field).
    /// * `val` - The maximum number of waiters to wake (e.g., `1` to wake one, `u32::MAX` to wake all).
    /// * `mask` - The bitmask to match against waiters.
    /// * `futex_flags` - Flags describing the futex size (e.g. [`Futex2Flags::SIZE_U32`])
    ///   and scope (e.g. [`Futex2Flags::PRIVATE`]).
    pub fn new(futex: u32, val: u64, mask: u64, futex_flags: Futex2Flags) -> Self {
        Self {
            futex,
            val,
            mask,
            futex_flags,
        }
    }
}

impl OpPayload for FutexWake {
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();

        let futex_ptr = std::ptr::from_ref(&*this.futex);
        let entry = io_uring::opcode::FutexWake::new(
            futex_ptr,
            *this.val,
            *this.mask,
            this.futex_flags.bits(),
        );

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Get an extended attribute value.
///
/// Corresponds to [`io_uring_prep_getxattr`](https://man.archlinux.org/man/io_uring_prep_getxattr.3.en).
#[derive(Debug)]
#[pin_project]
pub struct GetXattr {
    #[pin]
    name: CString,
    #[pin]
    value: Vec<u8>,
    #[pin]
    path: CString,
    len: u32,
}

impl GetXattr {
    /// Create a new `GetXattr` operation.
    pub fn try_new(
        name: impl Into<Vec<u8>>,
        path: impl AsRef<Path>,
        len: u32,
    ) -> Result<Self, OpcodeError> {
        Ok(Self {
            name: bytes_to_cstring(name)?,
            value: vec![0u8; len as usize],
            path: path_to_cstring(path)?,
            len,
        })
    }
}

impl OpPayload for GetXattr {
    type Output = (i32, Vec<u8>);

    fn create_entry(mut self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let mut this = self.as_mut().project();

        let name_ptr = std::ptr::from_ref(&*this.name);
        let path_ptr = std::ptr::from_ref(&*this.path);

        let entry = io_uring::opcode::GetXattr::new(
            name_ptr.cast(),
            this.value.as_mut_ptr().cast(),
            path_ptr.cast(),
            *this.len,
        );

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let mut this = self.project();
        let res = result?;
        Ok((res.res, std::mem::take(&mut this.value)))
    }
}

/// Listen for connections on a socket.
///
/// Corresponds to [`io_uring_prep_listen`](https://man.archlinux.org/man/io_uring_prep_listen.3.en).
#[derive(Debug)]
pub struct Listen<'a> {
    sockfd: BorrowedUringFd<'a>,
    backlog: i32,
}

impl<'a> Listen<'a> {
    /// Create a new `Listen` operation.
    pub fn new(sockfd: BorrowedUringFd<'a>, backlog: i32) -> Self {
        Self { sockfd, backlog }
    }
}

impl<'a> OpPayload for Listen<'a> {
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let entry = resolve_fd!(self.sockfd, |fd| {
            io_uring::opcode::Listen::new(fd, self.backlog)
        });

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Creates a new link (also known as a hard link) to an existing file.
/// Corresponds to [`io_uring_prep_linkat`](https://man7.org/linux/man-pages/man3/io_uring_prep_linkat.3.html)
#[derive(Debug)]
#[pin_project]
pub struct LinkAt {
    olddirfd: RawFd,
    #[pin]
    oldpath: CString,
    newdirfd: RawFd,
    #[pin]
    newpath: CString,
    flags: AtFlags,
}

impl LinkAt {
    pub fn try_new(
        olddirfd: Option<RawFd>,
        oldpath: impl AsRef<Path>,
        newdirfd: Option<RawFd>,
        newpath: impl AsRef<Path>,
        flags: AtFlags,
    ) -> Result<Self, OpcodeError> {
        if olddirfd.is_none() && !oldpath.as_ref().is_absolute() {
            return Err(anyhow!("olddirfd can only be ignored if oldpath is absolute.").into());
        }

        if newdirfd.is_none() && !newpath.as_ref().is_absolute() {
            return Err(anyhow!("newdirfd can only be ignored if newpath is absolute.").into());
        }

        Ok(Self {
            olddirfd: olddirfd.unwrap_or(libc::AT_FDCWD),
            oldpath: path_to_cstring(oldpath)?,
            newdirfd: newdirfd.unwrap_or(libc::AT_FDCWD),
            newpath: path_to_cstring(newpath)?,
            flags,
        })
    }
}

impl OpPayload for LinkAt {
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();

        let oldpath_ptr = std::ptr::from_ref(&*this.oldpath);
        let newpath_ptr = std::ptr::from_ref(&*this.newpath);

        let entry = io_uring::opcode::LinkAt::new(
            io_uring::types::Fd(*this.olddirfd),
            oldpath_ptr.cast(),
            io_uring::types::Fd(*this.newdirfd),
            newpath_ptr.cast(),
        )
        .flags(this.flags.bits());

        Ok(entry.build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Open and possibly create a file relative to a directory file descriptor.
///
/// Corresponds to [`io_uring_prep_openat`](https://man.archlinux.org/man/io_uring_prep_openat.3.en).
#[derive(Debug)]
#[pin_project]
pub struct OpenAt {
    fd_mode: KernelFdMode,
    dirfd: RawFd,
    #[pin]
    path: CString,
    flags: OFlag,
    mode: Mode,
}

impl OpenAt {
    /// Create a new `OpenAt` operation.
    pub fn try_new(
        fd_mode: KernelFdMode,
        dirfd: Option<RawFd>,
        path: impl AsRef<Path>,
        flags: OFlag,
        mode: Mode,
    ) -> Result<Self, OpcodeError> {
        if dirfd.is_none() && !path.as_ref().is_absolute() {
            return Err(anyhow!("dirfd can only be ignored if path is absolute.").into());
        }

        if flags.contains(OFlag::O_CREAT) && mode.is_empty() {
            return Err(anyhow!("Mode can't be empty if O_CREAT is set.").into());
        }

        Ok(Self {
            fd_mode,
            dirfd: dirfd.unwrap_or(libc::AT_FDCWD),
            path: path_to_cstring(path)?,
            flags,
            mode,
        })
    }
}

impl OpPayload for OpenAt {
    type Output = OwnedUringFd;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();

        Ok(
            io_uring::opcode::OpenAt::new(io_uring::types::Fd(*this.dirfd), this.path.as_ptr())
                .flags(this.flags.bits())
                .mode(this.mode.bits())
                .file_index(this.fd_mode.try_into_slot()?)
                .build(),
        )
    }

    fn into_output(
        self: Pin<&mut Self>,
        waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        Ok(OwnedUringFd::from_result(result?.res, self.fd_mode, waker))
    }
}

/// Open and possibly create a file with extended configuration.
///
/// Corresponds to [`io_uring_prep_openat2`](https://man.archlinux.org/man/io_uring_prep_openat2.3.en).
#[derive(Debug)]
#[pin_project]
pub struct OpenAt2 {
    fd_mode: KernelFdMode,
    dirfd: Option<RawFd>,
    #[pin]
    path: CString,
    #[pin]
    how: OpenHow,
}

impl OpenAt2 {
    /// Create a new `OpenAt2` operation.
    pub fn try_new(
        fd_mode: KernelFdMode,
        dirfd: Option<RawFd>,
        path: impl AsRef<Path>,
        how: OpenHow,
    ) -> Result<Self, OpcodeError> {
        if dirfd.is_none() && !path.as_ref().is_absolute() {
            return Err(anyhow!("DirFd can only be ignored for absolute path.").into());
        }

        Ok(Self {
            fd_mode,
            dirfd,
            path: path_to_cstring(path)?,
            how,
        })
    }
}

impl OpPayload for OpenAt2 {
    type Output = OwnedUringFd;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();

        let dirfd = io_uring::types::Fd(this.dirfd.unwrap_or(libc::AT_FDCWD));
        let how_ptr = std::ptr::from_ref(&*this.how);

        Ok(
            io_uring::opcode::OpenAt2::new(dirfd, this.path.as_ptr(), how_ptr.cast())
                .file_index(this.fd_mode.try_into_slot()?)
                .build(),
        )
    }

    fn into_output(
        self: Pin<&mut Self>,
        waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        Ok(OwnedUringFd::from_result(result?.res, self.fd_mode, waker))
    }
}

/// No-operation. Useful for testing and benchmarking.
///
/// Corresponds to [`io_uring_prep_nop`](https://man.archlinux.org/man/io_uring_prep_nop.3.en).
#[derive(Debug)]
pub struct Nop;

impl OpPayload for Nop {
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        Ok(io_uring::opcode::Nop::new().build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Prepares a recv request.
///
/// Corresponds to [`io_uring_prep_recv`](https://man.archlinux.org/man/io_uring_prep_recv.3.en).
#[derive(Debug)]
#[pin_project]
pub struct Recv<'a> {
    fd: BorrowedUringFd<'a>,
    #[pin]
    buf_mode: KernelBufferMode,
    buf_family: BufferFamily,
}

impl<'a> Recv<'a> {
    pub fn new(
        fd: BorrowedUringFd<'a>,
        buf_mode: KernelBufferMode,
        buf_family: BufferFamily,
    ) -> Self {
        Self {
            fd,
            buf_mode,
            buf_family,
        }
    }
}

impl<'a> OpPayload for Recv<'a> {
    type Output = UringBuffer; // TODO: also rename this

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();

        // TODO: refine BufferSubmission type
        let config = this.buf_mode.prepare(*this.buf_family)?;

        let entry = resolve_fd!(this.fd, |fd| {
            let mut builder = io_uring::opcode::Recv::new(fd, config.ptr, config.len);

            if config.is_mapped {
                builder = builder
                    .buf_group(config.bgid.val())
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT.bits() as i32);
            }

            builder.build()
        });

        Ok(entry)
    }

    fn into_output(
        self: Pin<&mut Self>,
        waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let cqe_res = result?;
        self.project()
            .buf_mode
            .complete(cqe_res.res as usize, cqe_res.flags, waker)
    }
}

/// Create an endpoint for communication.
///
/// Corresponds to [`io_uring_prep_socket`](https://man.archlinux.org/man/io_uring_prep_socket.3.en).
#[derive(Debug)]
pub struct Socket {
    fd_mode: KernelFdMode,
    addr_family: AddressFamily,
    sock_type: SockType,
    protocol: SockProtocol,
}

impl Socket {
    /// Create a new `Socket` operation.
    pub fn new(
        fd_mode: KernelFdMode,
        addr_family: AddressFamily,
        sock_type: SockType,
        protocol: SockProtocol,
    ) -> Self {
        Self {
            fd_mode,
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
        .file_index(self.fd_mode.try_into_slot()?)
        .build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        Ok(OwnedUringFd::from_result(result?.res, self.fd_mode, waker))
    }
}

/// Set the socket options.
///
/// Corresponds to [`io_uring_prep_setsockopt`] (no docs).
#[derive(Debug)]
#[pin_project]
pub struct SetSockOpt<'a> {
    sockfd: BorrowedUringFd<'a>,

    /// The opt is a self-contained struct that generates stable c ptrs when
    /// we unpack it on first poll. We use it as an entry generator and it's
    /// role is to inject the appropriate arguments in our SQE.
    #[pin]
    opt: AnySockOpt,
}

impl<'a> SetSockOpt<'a> {
    /// Create a new `SetSockOpt` operation.
    pub fn new<O: Into<AnySockOpt>>(sockfd: BorrowedUringFd<'a>, opt: O) -> Self {
        Self {
            sockfd,
            opt: opt.into(),
        }
    }
}

impl<'a> OpPayload for SetSockOpt<'a> {
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();
        this.opt.create_entry(*this.sockfd)
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

/// Register a timeout or timer.
///
/// Corresponds to [`io_uring_prep_timeout`](https://man.archlinux.org/man/io_uring_prep_timeout.3.en).
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
    type Output = i32;

    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        let this = self.project();

        let timespec_ptr = std::ptr::from_ref(&*this.timespec);

        Ok(io_uring::opcode::Timeout::new(timespec_ptr)
            .count(1)
            .flags(*this.flags)
            .build())
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        match result {
            Ok(res) => Ok(res.res),
            // Expired timeout yield -ETIME but this is a success case.
            Err(IoError::Io(e)) if e.raw_os_error() == Some(libc::ETIME) => Ok(0),
            Err(e) => Err(e),
        }
    }
}

/// Remove an existing timeout.
///
/// Corresponds to [`io_uring_prep_timeout_remove`](https://man.archlinux.org/man/io_uring_prep_timeout_remove.3.en).
#[derive(Debug, Clone)]
pub struct TimeoutRemove {
    entry: Option<io_uring::squeue::Entry>,
}

impl TimeoutRemove {
    /// Create a new `TimeoutRemove` operation.
    pub fn new(user_data: u64) -> Self {
        Self {
            entry: Some(io_uring::opcode::TimeoutRemove::new(user_data).build()),
        }
    }
}

impl OpPayload for TimeoutRemove {
    type Output = i32;

    fn create_entry(mut self: Pin<&mut Self>) -> Result<Entry, OpcodeError> {
        Ok(self.entry.take().expect("only called once"))
    }

    fn into_output(
        self: Pin<&mut Self>,
        _waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError> {
        let res = result?;
        Ok(res.res)
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use super::*;
    use crate::future::lib::BorrowedUringFd;
    use crate::future::lib::ops::sockopt::ReuseAddr;
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

            let connect_op = Op::new(Connect::new(sockfd.borrow(), &sock_addr));
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
