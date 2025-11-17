#![allow(unsafe_op_in_unsafe_fn)]
use std::fmt;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

///
/// C socket integration with `libc`, copied from `std::net`.
///
#[repr(C)]
pub(super) union SocketAddrCRepr {
    v4: libc::sockaddr_in,
    v6: libc::sockaddr_in6,
}

impl SocketAddrCRepr {
    pub(super) fn as_ptr(&self) -> *const libc::sockaddr {
        std::ptr::from_ref(self).cast()
    }

    pub(super) fn as_socket_addr(&self) -> Option<SocketAddr> {
        // SAFETY: Reading `sin_family` is safe because it's the first field
        // in both union variants and has the same type.
        match unsafe { self.v4.sin_family } as i32 {
            libc::AF_INET => {
                let addr = unsafe { self.v4 };
                Some(SocketAddr::V4(socket_addr_v4_from_c(addr)))
            }
            libc::AF_INET6 => {
                let addr = unsafe { self.v6 };
                Some(SocketAddr::V6(socket_addr_v6_from_c(addr)))
            }
            _ => None,
        }
    }
}

impl fmt::Debug for SocketAddrCRepr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(addr) = self.as_socket_addr() {
            fmt::Debug::fmt(&addr, f)
        } else {
            // Fallback for unknown address families
            let family = unsafe { self.v4.sin_family };
            f.debug_struct("SocketAddrCRepr")
                .field("unknown_family", &family)
                .finish()
        }
    }
}

pub(super) fn socket_addr_to_c(addr: &SocketAddr) -> (SocketAddrCRepr, libc::socklen_t) {
    match addr {
        SocketAddr::V4(a) => {
            let sockaddr = SocketAddrCRepr {
                v4: socket_addr_v4_to_c(a),
            };
            (sockaddr, size_of::<libc::sockaddr_in>() as libc::socklen_t)
        }
        SocketAddr::V6(a) => {
            let sockaddr = SocketAddrCRepr {
                v6: socket_addr_v6_to_c(a),
            };
            (sockaddr, size_of::<libc::sockaddr_in6>() as libc::socklen_t)
        }
    }
}

pub(super) unsafe fn socket_addr_from_c(
    storage: *const libc::sockaddr_storage,
    len: usize,
) -> io::Result<SocketAddr> {
    match (*storage).ss_family as libc::c_int {
        libc::AF_INET => {
            assert!(len >= size_of::<libc::sockaddr_in>());
            Ok(SocketAddr::V4(socket_addr_v4_from_c(*(storage.cast()))))
        }
        libc::AF_INET6 => {
            assert!(len >= size_of::<libc::sockaddr_in6>());
            Ok(SocketAddr::V6(socket_addr_v6_from_c(*(storage.cast()))))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid argument",
        )),
    }
}

fn socket_addr_v4_to_c(addr: &SocketAddrV4) -> libc::sockaddr_in {
    libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: addr.port().to_be(),
        sin_addr: ip_v4_addr_to_c(addr.ip()),
        ..unsafe { std::mem::zeroed() }
    }
}

fn socket_addr_v6_to_c(addr: &SocketAddrV6) -> libc::sockaddr_in6 {
    libc::sockaddr_in6 {
        sin6_family: libc::AF_INET6 as libc::sa_family_t,
        sin6_port: addr.port().to_be(),
        sin6_addr: ip_v6_addr_to_c(addr.ip()),
        sin6_flowinfo: addr.flowinfo(),
        sin6_scope_id: addr.scope_id(),
    }
}

fn socket_addr_v4_from_c(addr: libc::sockaddr_in) -> SocketAddrV4 {
    SocketAddrV4::new(
        ip_v4_addr_from_c(addr.sin_addr),
        u16::from_be(addr.sin_port),
    )
}

fn socket_addr_v6_from_c(addr: libc::sockaddr_in6) -> SocketAddrV6 {
    SocketAddrV6::new(
        ip_v6_addr_from_c(addr.sin6_addr),
        u16::from_be(addr.sin6_port),
        addr.sin6_flowinfo,
        addr.sin6_scope_id,
    )
}

fn ip_v4_addr_to_c(addr: &Ipv4Addr) -> libc::in_addr {
    // `s_addr` is stored as BE on all machines and the array is in BE order.
    // So the native endian conversion method is used so that it's never swapped.
    libc::in_addr {
        s_addr: u32::from_ne_bytes(addr.octets()),
    }
}

fn ip_v6_addr_to_c(addr: &Ipv6Addr) -> libc::in6_addr {
    libc::in6_addr {
        s6_addr: addr.octets(),
    }
}

fn ip_v4_addr_from_c(addr: libc::in_addr) -> Ipv4Addr {
    Ipv4Addr::from(addr.s_addr.to_ne_bytes())
}

fn ip_v6_addr_from_c(addr: libc::in6_addr) -> Ipv6Addr {
    Ipv6Addr::from(addr.s6_addr)
}
