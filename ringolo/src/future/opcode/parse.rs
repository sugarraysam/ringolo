use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

/// A helper function to safely convert raw `sockaddr_storage` and `socklen_t`
/// into a high-level Rust `SocketAddr`.
pub(super) fn sockaddr_storage_to_socket_addr(
    storage: &libc::sockaddr_storage,
    len: libc::socklen_t,
) -> io::Result<SocketAddr> {
    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            // Sanity check: ensure the length is at least the size of the IPv4 struct.
            if (len as usize) < std::mem::size_of::<libc::sockaddr_in>() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid length for sockaddr_in",
                ));
            }
            // SAFETY: We've checked the address family (AF_INET) and the length,
            // so it is safe to cast the pointer to the correct type.
            let addr = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
            let ip = Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes());

            // Port is in network byte order, so convert it to host byte order.
            let port = u16::from_be(addr.sin_port);
            Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        libc::AF_INET6 => {
            // Sanity check for IPv6.
            if (len as usize) < std::mem::size_of::<libc::sockaddr_in6>() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid length for sockaddr_in6",
                ));
            }
            // SAFETY: We've checked the address family (AF_INET6) and the length.
            let addr = unsafe { &*(storage as *const _ as *const libc::sockaddr_in6) };
            let ip = Ipv6Addr::from(addr.sin6_addr.s6_addr);

            // Port is in network byte order, so convert it to host byte order.
            let port = u16::from_be(addr.sin6_port);
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip,
                port,
                addr.sin6_flowinfo,
                addr.sin6_scope_id,
            )))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unsupported address family",
        )),
    }
}
