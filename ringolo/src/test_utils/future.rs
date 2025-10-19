use nix::sys::socket::{AddressFamily, SockProtocol, SockType};

use crate::future::lib::{KernelFdMode, Op, Socket};

pub(crate) fn tcp_socket(mode: KernelFdMode, addr_family: AddressFamily) -> Op<Socket> {
    Op::new(Socket::new(
        mode,
        addr_family,
        SockType::Stream,
        SockProtocol::Tcp,
    ))
}

pub(crate) fn tcp_socket4(mode: KernelFdMode) -> Op<Socket> {
    tcp_socket(mode, AddressFamily::Inet)
}

pub(crate) fn tcp_socket6(mode: KernelFdMode) -> Op<Socket> {
    tcp_socket(mode, AddressFamily::Inet6)
}
