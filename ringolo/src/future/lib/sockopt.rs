#![allow(dead_code)]

//! Copied from `nix` crate and adapted for `io_uring` and async world.
//! Omitted many of the platform dependent options and focused on Linux.

use crate::future::lib::{AsRawOrDirect, OpcodeError};
use io_uring::squeue::Entry;

pub trait SetSockOptIf {
    fn create_entry(&self, fd: &impl AsRawOrDirect) -> Result<Entry, OpcodeError>;
}

/// Helper trait that describes what is expected from a `SetSockOptIf` setter.
#[doc(hidden)]
trait Set<T> {
    /// Initialize the setter with a given value.
    fn new(val: T) -> Self;

    /// Returns a pointer to the stored value. This pointer will be passed to the system's
    /// `setsockopt` call (`man 3p setsockopt`, argument `option_value`).
    fn ffi_ptr(&self) -> *const libc::c_void;

    /// Returns length of the stored value. This pointer will be passed to the system's
    /// `setsockopt` call (`man 3p setsockopt`, argument `option_len`).
    fn ffi_len(&self) -> libc::socklen_t;
}

/// Setter for a boolean value.
// Hide the docs, because it's an implementation detail of `sockopt_impl!`
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SetBool {
    val: libc::c_int,
}

impl Set<bool> for SetBool {
    fn new(val: bool) -> SetBool {
        SetBool {
            val: i32::from(val),
        }
    }

    fn ffi_ptr(&self) -> *const libc::c_void {
        std::ptr::from_ref(&self.val).cast()
    }

    fn ffi_len(&self) -> libc::socklen_t {
        std::mem::size_of_val(&self.val) as libc::socklen_t
    }
}

/// Setter for an `usize` value.
// Hide the docs, because it's an implementation detail of `sockopt_impl!`
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SetUsize {
    val: libc::c_int,
}

impl Set<usize> for SetUsize {
    fn new(val: usize) -> SetUsize {
        SetUsize {
            val: val as libc::c_int,
        }
    }

    fn ffi_ptr(&self) -> *const libc::c_void {
        std::ptr::from_ref(&self.val).cast()
    }

    fn ffi_len(&self) -> libc::socklen_t {
        std::mem::size_of_val(&self.val) as libc::socklen_t
    }
}

/// Setter for an arbitrary `struct`.
// Hide the docs, because it's an implementation detail of `sockopt_impl!`
#[doc(hidden)]
#[derive(Debug, Copy, Clone)]
pub struct SetStruct<T> {
    ptr: T,
}

impl<T> Set<T> for SetStruct<T>
where
    T: Copy + Clone,
{
    fn new(ptr: T) -> SetStruct<T> {
        SetStruct { ptr }
    }

    fn ffi_ptr(&self) -> *const libc::c_void {
        std::ptr::from_ref(&self.ptr).cast()
    }

    fn ffi_len(&self) -> libc::socklen_t {
        std::mem::size_of::<T>() as libc::socklen_t
    }
}

macro_rules! setsockopt_impl {
    ($(#[$attr:meta])* $name:ident, $level:expr, $flag:path, bool) => {
        setsockopt_impl!(
            $(#[$attr])*
            $name,
            $level,
            $flag,
            bool,
            $crate::future::lib::sockopt::SetBool
        );
    };

    ($(#[$attr:meta])* $name:ident, $level:expr, $flag:path, usize) => {
        setsockopt_impl!(
            $(#[$attr])*
            $name,
            $level,
            $flag,
            usize,
            $crate::future::lib::sockopt::SetUsize
        );
    };

    ($(#[$attr:meta])* $name:ident, $level:expr, $flag:path, $ty:ty) => {
        setsockopt_impl!(
            $(#[$attr])*
            $name,
            $level,
            $flag,
            $ty,
            $crate::future::lib::sockopt::SetStruct<$ty>
        );
    };

    ($(#[$attr:meta])* $name:ident, $level:expr, $flag:path, $ty:ty, $setter:ty) => {
        $(#[$attr])*
        #[derive(Debug)]
        pub struct $name {
            setter: $setter,
        }

        impl $name {
            pub fn new(val: $ty) -> Self {
                Self {
                    setter: <$setter>::new(val),
                }
            }
        }

        impl $crate::future::lib::sockopt::SetSockOptIf for $name {
            fn create_entry(
                &self,
                fd: &impl $crate::future::lib::fd::AsRawOrDirect,
            ) -> Result<io_uring::squeue::Entry, $crate::future::lib::OpcodeError> {
                let entry = resolve_fd!(fd, |fd| {
                    io_uring::opcode::SetSockOpt::new(
                        fd,
                        $level as u32,
                        $flag as u32,
                        self.setter.ffi_ptr(),
                        self.setter.ffi_len(),
                    )
                });

                Ok(entry.build())
            }
        }
    };
}

setsockopt_impl!(
    /// Enables local address reuse
    ReuseAddr,
    libc::SOL_SOCKET,
    libc::SO_REUSEADDR,
    bool
);

setsockopt_impl!(
    /// Permits multiple AF_INET or AF_INET6 sockets to be bound to an
    /// identical socket address.
    ReusePort,
    libc::SOL_SOCKET,
    libc::SO_REUSEPORT,
    bool
);

setsockopt_impl!(
    /// Used to disable Nagle's algorithm.
    ///
    /// Nagle's algorithm:
    ///
    /// Under most circumstances, TCP sends data when it is presented; when
    /// outstanding data has not yet been acknowledged, it gathers small amounts
    /// of output to be sent in a single packet once an acknowledgement is
    /// received.  For a small number of clients, such as window systems that
    /// send a stream of mouse events which receive no replies, this
    /// packetization may cause significant delays.  The boolean option, when
    /// enabled, defeats this algorithm.
    TcpNoDelay,
    libc::IPPROTO_TCP,
    libc::TCP_NODELAY,
    bool
);

setsockopt_impl!(
    /// When enabled, a close(2) or shutdown(2) will not return until all
    /// queued messages for the socket have been successfully sent or the
    /// linger timeout has been reached.
    Linger,
    libc::SOL_SOCKET,
    libc::SO_LINGER,
    libc::linger
);

setsockopt_impl!(
    /// Specify the receiving timeout until reporting an error.
    ReceiveTimeout,
    libc::SOL_SOCKET,
    libc::SO_RCVTIMEO,
    libc::timeval
);

setsockopt_impl!(
    /// Specify the sending timeout until reporting an error.
    SendTimeout,
    libc::SOL_SOCKET,
    libc::SO_SNDTIMEO,
    libc::timeval
);

setsockopt_impl!(
    /// Set the broadcast flag.
    Broadcast,
    libc::SOL_SOCKET,
    libc::SO_BROADCAST,
    bool
);

setsockopt_impl!(
    /// If this option is enabled, out-of-band data is directly placed into
    /// the receive data stream.
    OobInline,
    libc::SOL_SOCKET,
    libc::SO_OOBINLINE,
    bool
);

setsockopt_impl!(
    /// Set the don't route flag.
    DontRoute,
    libc::SOL_SOCKET,
    libc::SO_DONTROUTE,
    bool
);

setsockopt_impl!(
    /// Enable sending of keep-alive messages on connection-oriented sockets.
    KeepAlive,
    libc::SOL_SOCKET,
    libc::SO_KEEPALIVE,
    bool
);

setsockopt_impl!(
    /// Sets the maximum socket receive buffer in bytes.
    RcvBuf,
    libc::SOL_SOCKET,
    libc::SO_RCVBUF,
    usize
);

setsockopt_impl!(
    /// Sets the maximum socket send buffer in bytes.
    SndBuf,
    libc::SOL_SOCKET,
    libc::SO_SNDBUF,
    usize
);

setsockopt_impl!(
    /// Enable or disable the receiving of the `SO_TIMESTAMP` control message.
    ReceiveTimestamp,
    libc::SOL_SOCKET,
    libc::SO_TIMESTAMP,
    bool
);

setsockopt_impl!(
    /// Set the mark for each packet sent through this socket (similar to the
    /// netfilter MARK target but socket-based).
    Mark,
    libc::SOL_SOCKET,
    libc::SO_MARK,
    u32
);

setsockopt_impl!(
    /// Configures the behavior of time-based transmission of packets, for use
    /// with the `TxTime` control message.
    TxTime,
    libc::SOL_SOCKET,
    libc::SO_TXTIME,
    libc::sock_txtime
);

setsockopt_impl!(
    /// The socket is restricted to sending and receiving IPv6 packets only.
    Ipv6V6Only,
    libc::IPPROTO_IPV6,
    libc::IPV6_V6ONLY,
    bool
);

setsockopt_impl!(
    /// To be used with `ReusePort`,
    /// we can then attach a BPF (classic)
    /// to set how the packets are assigned
    /// to the socket (e.g. cpu distribution).
    AttachReusePortCbpf,
    libc::SOL_SOCKET,
    libc::SO_ATTACH_REUSEPORT_CBPF,
    libc::sock_fprog
);

#[cfg(test)]
mod tests {
    use std::os::fd::BorrowedFd;

    use super::*;
    use crate::future::lib::{Op, OwnedUringFd, SetSockOpt};
    use crate::test_utils::*;
    use crate::{self as ringolo, future::lib::KernelFdMode};
    use anyhow::{Context, Result};
    use nix::sys::socket as nix;
    use rstest::rstest;

    fn getsockopt<O: nix::GetSockOpt>(fd: &OwnedUringFd, opt: O) -> O::Val {
        fd.with_raw(|fd| {
            let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
            nix::getsockopt(&borrowed, opt)
        })
        .expect("failed to raw fd")
        .expect("failed to get sockopt")
    }

    #[rstest]
    #[case::reuse_addr(ReuseAddr::new(true), nix::sockopt::ReuseAddr)]
    #[case::reuse_port(ReusePort::new(true), nix::sockopt::ReusePort)]
    #[case::tcp_no_delay(TcpNoDelay::new(true), nix::sockopt::TcpNoDelay)]
    #[case::broadcast(Broadcast::new(true), nix::sockopt::Broadcast)]
    #[case::oob_inline(OobInline::new(true), nix::sockopt::OobInline)]
    #[case::dont_route(DontRoute::new(true), nix::sockopt::DontRoute)]
    #[case::keep_alive(KeepAlive::new(true), nix::sockopt::KeepAlive)]
    #[ringolo::test]
    async fn test_setsockopt_bool_opts<O, N>(
        #[case] ringolo_opt: O,
        #[case] nix_opt: N,
    ) -> Result<()>
    where
        O: SetSockOptIf,
        N: nix::GetSockOpt<Val = bool>,
    {
        let sockfd = tcp_socket4(KernelFdMode::Legacy)
            .await
            .context("failed to create sockfd")?;

        let before = getsockopt(&sockfd, nix_opt);
        assert_eq!(before, false);

        Op::new(SetSockOpt::new(sockfd.clone(), ringolo_opt))
            .await
            .context("failed to set opt")?;

        let after = getsockopt(&sockfd, nix_opt);
        assert_eq!(after, true);

        Ok(())
    }

    #[rstest]
    #[case::recv_buf(true, nix::sockopt::RcvBuf)]
    #[case::send_buf(false, nix::sockopt::SndBuf)]
    #[ringolo::test]
    async fn test_setsockopt_usize_opts<N>(#[case] recv_buf: bool, #[case] nix_opt: N) -> Result<()>
    where
        N: nix::GetSockOpt<Val = usize>,
    {
        let sockfd = tcp_socket4(KernelFdMode::Legacy)
            .await
            .context("failed to create sockfd")?;

        let before = getsockopt(&sockfd, nix_opt);
        if recv_buf {
            Op::new(SetSockOpt::new(sockfd.clone(), RcvBuf::new(before * 2))).await
        } else {
            Op::new(SetSockOpt::new(sockfd.clone(), SndBuf::new(before * 2))).await
        }
        .context("failed to setopt")?;

        // Kernel only uses the value as a *hint*
        let after = getsockopt(&sockfd, nix_opt);
        assert!(after > before);

        Ok(())
    }

    #[ringolo::test]
    async fn test_setsockopt_linger() -> Result<()> {
        let sockfd = tcp_socket4(KernelFdMode::Legacy)
            .await
            .context("failed to create sockfd")?;

        let before = getsockopt(&sockfd, nix::sockopt::Linger);
        assert_eq!(
            before,
            libc::linger {
                l_onoff: 0,
                l_linger: 0
            }
        );

        Op::new(SetSockOpt::new(
            sockfd.clone(),
            Linger::new(libc::linger {
                l_onoff: 1,
                l_linger: 10,
            }),
        ))
        .await
        .context("failed to set opt")?;

        let after = getsockopt(&sockfd, nix::sockopt::Linger);
        assert_eq!(
            after,
            libc::linger {
                l_onoff: 1,
                l_linger: 10,
            }
        );

        Ok(())
    }
}
