use super::UringSocket;
use nix::sys::socket::{AddressFamily, SockProtocol, SockType};
use std::{
    io,
    net::{SocketAddr, ToSocketAddrs},
    ops::Deref,
};

use crate::{
    any_extract, any_extract_all, any_vec,
    future::lib::{Bind, KernelFdMode, Listen, ReuseAddr, ReusePort, SetSockOpt, list::OpList},
};

// TODO:
// - accept
// - accept_multi
// - TcpStream, a TcpSocket becomes a TcpStream? after connect?

#[derive(Debug)]
pub struct TcpListener {
    socket: TcpSocket,
}

impl TcpListener {
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        // TODO: this is blocking for string as we need to do DNS lookup. Tokio
        // uses `spawn_blocking`. We could implement resolution pure async on
        // `io_uring` with:
        // - reading local configs /w file IO
        // - sending UDP packet to nameserver
        // - waiting and retries
        let addrs = addr.to_socket_addrs()?;

        let mut last_err = None;

        for addr in addrs {
            match TcpListener::bind_addr(addr).await {
                Ok(listener) => return Ok(listener),
                Err(e) => last_err = Some(e),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not resolve to any address",
            )
        }))
    }

    async fn bind_addr(addr: SocketAddr) -> io::Result<Self> {
        let socket = TcpSocket::try_new(&addr).await?;
        let sockfd_ref = socket.borrow();

        // Chain all operations as a single task to prepare our socket.
        let results = OpList::new_chain(any_vec![
            SetSockOpt::new(sockfd_ref, ReuseAddr::new(true)),
            SetSockOpt::new(sockfd_ref, ReusePort::new(true)),
            Bind::new(sockfd_ref, &addr),
            Listen::new(sockfd_ref, 128),
        ])
        .await?;

        let (reuse_addr, reuse_port, bind, listen) =
            any_extract_all!(results, SetSockOpt, SetSockOpt, Bind, Listen);

        reuse_addr?;
        reuse_port?;
        bind?;
        listen?;

        Ok(Self { socket })
    }

    // pub async fn accept(&self) -> io::Result<TcpStream> {}

    // pub async fn accept_multi(&self) -> io::Result<> {}
}

#[derive(Debug)]
pub struct TcpSocket {
    socket: UringSocket,
}

impl TcpSocket {
    pub async fn try_new(addr: &SocketAddr) -> io::Result<Self> {
        let addr_family = match addr {
            SocketAddr::V4(_) => AddressFamily::Inet,
            SocketAddr::V6(_) => AddressFamily::Inet6,
        };

        let socket = UringSocket::try_new(
            KernelFdMode::DirectAuto,
            addr_family,
            SockType::Stream,
            SockProtocol::Tcp,
        )
        .await?;

        Ok(Self { socket })
    }
}

impl Deref for TcpSocket {
    type Target = UringSocket;

    fn deref(&self) -> &Self::Target {
        &self.socket
    }
}
