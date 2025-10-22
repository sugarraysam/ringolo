use nix::sys::socket::{AddressFamily, SockProtocol, SockType};
use std::io;

use crate::{
    any_extract, any_vec,
    future::lib::{
        AnySockOpt, BorrowedUringFd, KernelFdMode, Op, OwnedUringFd, ReuseAddr, ReusePort,
        SetSockOpt, SetSockOptIf, Socket, list::OpList,
    },
};

#[derive(Debug)]
pub struct UringSocket {
    sockfd: OwnedUringFd,
}

impl UringSocket {
    pub async fn try_new(
        mode: KernelFdMode,
        addr_family: AddressFamily,
        sock_type: SockType,
        protocol: SockProtocol,
    ) -> io::Result<Self> {
        let sockfd = Op::new(Socket::new(mode, addr_family, sock_type, protocol)).await?;
        Ok(Self { sockfd })
    }

    pub async fn set_opt<O: SetSockOptIf>(&self, opt: O) -> io::Result<()> {
        Op::new(SetSockOpt::new(self.sockfd.borrow(), opt)).await?;
        Ok(())
    }

    /// Sets one or more socket options as a single atomic batch.
    ///
    /// This is more efficient than calling `set_opt` multiple times,
    /// as it submits all operations to the kernel in a single batch.
    ///
    /// # Example
    ///
    /// Use the `any_vec!` macro to easily create the required `Vec<AnySockOpt>`.
    ///
    /// ```ignore
    /// use ringolo::any_vec;
    ///
    /// let my_socket = UringSocket::try_new(...).await?;
    /// my_socket.set_many_opt(any_vec![
    ///     ReuseAddr::new(true),
    ///     ReusePort::new(true)
    /// ]).await?;
    /// ```
    pub async fn set_many_opts(&self, opts: Vec<AnySockOpt>) -> io::Result<()> {
        if opts.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Need at least 2 sockopts to use this interface.",
            ));
        }

        let sockfd_ref = self.sockfd.borrow();

        let results = OpList::new_batch(
            opts.into_iter()
                .map(|o| SetSockOpt::new(sockfd_ref, o).into())
                .collect::<Vec<_>>(),
        )
        .await?;

        for (pos, res) in results.into_iter().enumerate() {
            let _ = any_extract!(res, SetSockOpt).map_err(|e| {
                let ioerr: io::Error = e.into();
                io::Error::new(
                    ioerr.kind(),
                    format!("failed to set opt at pos {pos}: {ioerr}"),
                )
            })?;
        }

        Ok(())
    }

    pub async fn set_reuseaddr_and_reuse_port(&self) -> io::Result<()> {
        self.set_many_opts(any_vec![ReuseAddr::new(true), ReusePort::new(true)])
            .await
    }

    pub fn borrow(&self) -> BorrowedUringFd<'_> {
        self.sockfd.borrow()
    }
}
