use crate::future::lib::{CloseOp, OpcodeError, OwnershipError};
use crate::runtime::CleanupTaskBuilder;
use either::Either;
use io_uring::types::{DestinationSlot, Fd, Fixed};
use std::mem::ManuallyDrop;
use std::os::fd::RawFd;
use std::sync::Arc;

/// Instructs how we want to retrieve file descriptors from the kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelFdMode {
    /// Return a regular file descriptor stored in global process fd table.
    Legacy,

    /// Link the resource to the fixed slot in the `io_uring` registered fds table.
    Direct(u32),

    /// Automatically link the resource to an available slot in the `io_uring`
    /// registered fds table. Uses the `IORING_FILE_INDEX_ALLOC` feature.
    DirectAuto,
}

impl KernelFdMode {
    pub fn try_into_slot(self) -> Result<Option<DestinationSlot>, OpcodeError> {
        match self {
            KernelFdMode::Legacy => Ok(None),
            KernelFdMode::Direct(slot) => DestinationSlot::try_from_slot_target(slot)
                .map_err(OpcodeError::InvalidFixedFd)
                .map(Some),

            KernelFdMode::DirectAuto => Ok(Some(DestinationSlot::auto_target())),
        }
    }
}

/// Reference-counted file descriptor that integrates with `io_uring` fixed descriptor.
/// It spawns an asynchronous cleanup task when the last reference goes out-of-scope.
#[derive(Debug, Clone)]
pub struct UringFd {
    inner: Arc<UringFdInner>,
}

#[derive(Debug)]
enum UringFdInner {
    Owned(UringFdKindInner),
    Borrowed(UringFdKindInner),
}

impl Drop for UringFdInner {
    fn drop(&mut self) {
        match &self {
            UringFdInner::Borrowed(_) => (),
            UringFdInner::Owned(kind) => {
                dbg!("Spawning async cleanup task for UringFd: {:?}", &self);
                let task = CleanupTaskBuilder::new(CloseOp::new(kind.as_either()));
                crate::runtime::spawn_cleanup(task);
            }
        }
    }
}

// We intentionally hide this UringFdKind to avoid making the RawFd/u32 accessible.
// We need to force users to use the from/into methods provided to guarantee ownership
// semantics safety.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum UringFdKindInner {
    // Regular unregistered linux file descriptor
    Raw(RawFd),

    // IoUring registered per-thread file descriptor
    Fixed(u32),
}

impl UringFdKindInner {
    // Seamless integration with `io_uring` crate.
    fn as_either(&self) -> Either<Fd, Fixed> {
        match &self {
            UringFdKindInner::Raw(raw) => Either::Left(Fd(*raw)),
            UringFdKindInner::Fixed(fixed) => Either::Right(Fixed(*fixed)),
        }
    }
}

/// Kind of fd owned by UringFd.
///
/// This is the public version stripped of resources.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum UringFdKind {
    // Regular unregistered linux file descriptor
    Raw,

    // IoUring registered per-thread file descriptor
    Fixed,
}

impl From<UringFdKindInner> for UringFdKind {
    fn from(kind: UringFdKindInner) -> Self {
        match kind {
            UringFdKindInner::Raw(_) => UringFdKind::Raw,
            UringFdKindInner::Fixed(_) => UringFdKind::Fixed,
        }
    }
}

impl UringFd {
    /// Creates a new **owned** `UringFd` from a raw file descriptor.
    /// The file descriptor will be closed when the last reference is dropped.
    pub fn new_raw_owned(fd: RawFd) -> Self {
        Self {
            inner: Arc::new(UringFdInner::Owned(UringFdKindInner::Raw(fd))),
        }
    }

    /// Creates a new **owned** `UringFd` from a fixed slot index.
    /// The file descriptor will be unregistered/closed when the last reference is dropped.
    pub fn new_fixed_owned(slot: u32) -> Self {
        Self {
            inner: Arc::new(UringFdInner::Owned(UringFdKindInner::Fixed(slot))),
        }
    }

    /// Creates a new **borrowed** `UringFd` from a raw file descriptor.
    /// The file descriptor will NOT be closed when the last reference is dropped.
    pub fn new_raw_borrowed(fd: RawFd) -> Self {
        Self {
            inner: Arc::new(UringFdInner::Borrowed(UringFdKindInner::Raw(fd))),
        }
    }

    /// Creates a new **borrowed** `UringFd` from a fixed slot index.
    /// The file descriptor will NOT be unregistered/closed when the last reference is dropped.
    pub fn new_fixed_borrowed(slot: u32) -> Self {
        Self {
            inner: Arc::new(UringFdInner::Borrowed(UringFdKindInner::Fixed(slot))),
        }
    }

    pub fn kind(&self) -> UringFdKind {
        match &*self.inner {
            UringFdInner::Owned(kind) => (*kind).into(),
            UringFdInner::Borrowed(kind) => (*kind).into(),
        }
    }

    /// Take the result from an `io_uring` opcode, and convert it back to an **owned** `UringFd`.
    /// Results from opcodes that create new file descriptors are always owned by us.
    pub fn from_result_into_owned(res: i32, mode: KernelFdMode) -> Self {
        match mode {
            KernelFdMode::Legacy => UringFd::new_raw_owned(res),
            // Kernel generates 0 on success in this mode, we must re-use the
            // slot given by the user.
            KernelFdMode::Direct(slot) => UringFd::new_fixed_owned(slot),
            KernelFdMode::DirectAuto => UringFd::new_fixed_owned(res as u32),
        }
    }

    pub fn is_raw(&self) -> bool {
        matches!(self.kind(), UringFdKind::Raw)
    }

    pub fn is_fixed(&self) -> bool {
        matches!(self.kind(), UringFdKind::Fixed)
    }

    pub fn is_owned(&self) -> bool {
        matches!(&*self.inner, UringFdInner::Owned(_))
    }

    pub fn is_borrowed(&self) -> bool {
        matches!(&*self.inner, UringFdInner::Borrowed(_))
    }

    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    /// Provides temporary access to the underlying raw file descriptor.
    ///
    /// This method allows you to run an operation on the raw file descriptor
    /// without taking ownership. It will return an error if the `UringFd` is not
    /// the `Raw` variant.
    ///
    /// The file descriptor is only valid for the duration of the closure.
    pub fn with_raw_fd<F, R>(&self, f: F) -> Result<R, OpcodeError>
    where
        F: FnOnce(RawFd) -> R,
    {
        match &*self.inner {
            UringFdInner::Owned(UringFdKindInner::Raw(fd))
            | UringFdInner::Borrowed(UringFdKindInner::Raw(fd)) => Ok(f(*fd)),
            _ => Err(OpcodeError::IncorrectFdVariant(self.kind())),
        }
    }

    /// Provides temporary access to the underlying fixed slot index.
    ///
    /// This method allows you to run an operation on the fixed slot index
    /// without taking ownership. It will return an error if the `UringFd` is not
    /// the `Fixed` variant.
    ///
    /// The slot index is only valid for the duration of the closure.
    pub fn with_fixed_fd<F, R>(&self, f: F) -> Result<R, OpcodeError>
    where
        F: FnOnce(u32) -> R,
    {
        match &*self.inner {
            UringFdInner::Owned(UringFdKindInner::Fixed(slot))
            | UringFdInner::Borrowed(UringFdKindInner::Fixed(slot)) => Ok(f(*slot)),
            _ => Err(OpcodeError::IncorrectFdVariant(self.kind())),
        }
    }

    /// Consumes the `UringFd` and returns the underlying raw file descriptor.
    ///
    /// This function will fail if:
    /// - The descriptor is shared (`strong_count > 1`).
    /// - The descriptor is a borrowed reference.
    /// - The descriptor is not the `Raw` variant.
    ///
    /// On success, the `UringFd` is consumed and its drop logic is disabled,
    /// transferring ownership of the file descriptor to the caller.
    pub unsafe fn into_raw(self) -> Result<RawFd, OpcodeError> {
        if self.is_fixed() {
            return Err(OpcodeError::IncorrectFdVariant(self.kind()));
        }

        if self.is_borrowed() {
            // Safety: ok to ignore fixed checks and ownership checks.
            return Ok(unsafe { self.get_raw_unchecked() });
        }

        let inner = match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner,
            Err(arc) => return Err(OwnershipError::SharedFd(Arc::strong_count(&arc)).into()),
        };

        // Ensure the destructor for `UringFdInner` is not called.
        let inner = ManuallyDrop::new(inner);

        match &*inner {
            UringFdInner::Owned(UringFdKindInner::Raw(fd)) => Ok(*fd),
            _ => unreachable!("can't be fixed or borrowed"),
        }
    }

    /// Consumes the `UringFd` and returns the underlying fixed slot index.
    ///
    /// This is the `Fixed` variant of `into_raw_fd`.
    pub unsafe fn into_fixed(self) -> Result<u32, OpcodeError> {
        if self.is_raw() {
            return Err(OpcodeError::IncorrectFdVariant(self.kind()));
        }

        if self.is_borrowed() {
            // Safety: ok to ignore raw checks and ownership checks.
            return Ok(unsafe { self.get_fixed_unchecked() });
        }

        let inner = match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner,
            Err(arc) => return Err(OwnershipError::SharedFd(Arc::strong_count(&arc)).into()),
        };

        let inner = ManuallyDrop::new(inner);

        match &*inner {
            UringFdInner::Owned(UringFdKindInner::Fixed(slot)) => Ok(*slot),
            _ => unreachable!("can't be raw or borrowed"),
        }
    }

    /// Resolves the file descriptor into an `Either<Fd, Fixed>` for use with
    /// `io_uring` opcodes. This is the preferred way to get the underlying
    /// descriptor for an operation.
    pub(super) unsafe fn as_either(&self) -> Either<Fd, Fixed> {
        // **Safety**:
        // This API leaks either a slot or a rawFd without accounting for a reference.
        // Only for internal use and it is `pub(super)` because we don't have a better
        // way to resolve to Fd/Fixed right now then the `resolve_fd` macro below.
        match &*self.inner {
            UringFdInner::Owned(kind) | UringFdInner::Borrowed(kind) => kind.as_either(),
        }
    }

    /// Unsafe API, ignores all checks and ownership rules.
    unsafe fn get_raw_unchecked(&self) -> RawFd {
        match &*self.inner {
            UringFdInner::Owned(UringFdKindInner::Raw(fd))
            | UringFdInner::Borrowed(UringFdKindInner::Raw(fd)) => *fd,
            _ => {
                panic!("Unexpected UringFd variant");
            }
        }
    }

    /// Unsafe API, ignores all checks and ownership rules.
    unsafe fn get_fixed_unchecked(&self) -> u32 {
        match &*self.inner {
            UringFdInner::Owned(UringFdKindInner::Fixed(slot))
            | UringFdInner::Borrowed(UringFdKindInner::Fixed(slot)) => *slot,
            _ => {
                panic!("Unexpected UringFd variant.")
            }
        }
    }
}

impl PartialEq for UringFd {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

// Safety: we use `as_either()` API and leak a file descriptor because we need
// to unpack Entries in this module to create `io_uring` operations.
macro_rules! resolve_fd {
    ($uring_fd:expr, |$fd_ident:ident| $body:expr) => {
        match unsafe { $uring_fd.as_either() } {
            either::Either::Left($fd_ident) => $body,
            either::Either::Right($fd_ident) => $body,
        }
    };
}

impl From<Fd> for UringFd {
    fn from(fd: Fd) -> Self {
        UringFd::new_raw_owned(fd.0)
    }
}

impl From<Fixed> for UringFd {
    fn from(fd: Fixed) -> Self {
        UringFd::new_fixed_owned(fd.0)
    }
}

impl TryFrom<&UringFd> for Fixed {
    type Error = OpcodeError;

    fn try_from(value: &UringFd) -> Result<Self, Self::Error> {
        match &*value.inner {
            UringFdInner::Owned(UringFdKindInner::Fixed(f))
            | UringFdInner::Borrowed(UringFdKindInner::Fixed(f)) => Ok(Fixed(*f)),
            _ => Err(OpcodeError::IncorrectFdVariant(value.kind())),
        }
    }
}

impl TryFrom<&UringFd> for Fd {
    type Error = OpcodeError;

    fn try_from(value: &UringFd) -> Result<Self, Self::Error> {
        match &*value.inner {
            UringFdInner::Owned(UringFdKindInner::Raw(f))
            | UringFdInner::Borrowed(UringFdKindInner::Raw(f)) => Ok(Fd(*f)),
            _ => Err(OpcodeError::IncorrectFdVariant(value.kind())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::TcpListener, os::fd::AsRawFd};

    use super::*;
    use crate as ringolo;
    use crate::{
        future::lib::{Op, single::SocketOp},
        runtime::{Builder, TaskOpts},
        test_utils::*,
        utils::scheduler::{Call, Method},
    };
    use anyhow::{Context, Result};
    use nix::sys::socket::{AddressFamily, SockProtocol, SockType};
    use rstest::rstest;

    fn socket_op(mode: KernelFdMode) -> Op<SocketOp> {
        Op::new(SocketOp::new(
            mode,
            AddressFamily::Inet,
            SockType::Stream,
            SockProtocol::Tcp,
        ))
    }

    #[ringolo::test]
    async fn test_owned_uringfd() -> Result<()> {
        {
            let sockfd = socket_op(KernelFdMode::DirectAuto)
                .await
                .context("failed to create sockfd")?;

            assert!(sockfd.is_owned() && sockfd.is_fixed());
            assert_eq!(sockfd.strong_count(), 1);

            // io_uring integration
            assert!(matches!(
                unsafe { sockfd.as_either() },
                either::Either::Right(Fixed(_))
            ));
            assert!(matches!(sockfd.kind(), UringFdKind::Fixed));

            // Clone a few times
            let sockfd1 = sockfd.clone();
            let sockfd2 = sockfd.clone();
            assert!(sockfd1.is_owned() && sockfd1.is_fixed());
            assert!(sockfd2.is_owned() && sockfd2.is_fixed());
            assert_eq!(sockfd.strong_count(), 3);

            // Drop 2 - no async cleanup
            drop(sockfd);
            drop(sockfd1);
            assert_eq!(sockfd2.strong_count(), 1);
        } // last ref dropped - triggers async close

        crate::with_scheduler!(|s| {
            let spawn_calls = s.tracker.get_calls(&Method::Spawn);
            assert_eq!(spawn_calls.len(), 1);
            assert!(matches!(spawn_calls[0], Call::Spawn { opts } if opts == TaskOpts::STICKY));
        });

        Ok(())
    }

    #[ringolo::test]
    async fn test_borrowed_uringfd() -> Result<()> {
        {
            let listener =
                TcpListener::bind("127.0.0.1:0").context("could not create tcplistener")?;
            let listener_fd = listener.as_raw_fd();

            let fd = UringFd::new_raw_borrowed(listener_fd);
            assert!(fd.is_borrowed() && fd.is_raw());
            assert_eq!(fd.strong_count(), 1);

            // io_uring integration
            assert!(matches!(
                unsafe { fd.as_either() },
                either::Either::Left(Fd(_))
            ));
            assert!(matches!(fd.kind(), UringFdKind::Raw));

            // Clone a few times
            let fd1 = fd.clone();
            let fd2 = fd.clone();
            assert!(fd1.is_borrowed() && fd1.is_raw());
            assert!(fd2.is_borrowed() && fd2.is_raw());
            assert_eq!(fd.strong_count(), 3);
        } // last ref dropped - does NOT trigger async close

        crate::with_scheduler!(|s| {
            let spawn_calls = s.tracker.get_calls(&Method::Spawn);
            assert_eq!(spawn_calls.len(), 0);
        });

        Ok(())
    }

    #[rstest]
    #[case::n_4(4)]
    #[case::n_8(8)]
    #[case::n_16(16)]
    #[test]
    fn test_fixed_descriptor_exhaustion(#[case] n: u32) -> Result<()> {
        let builder = Builder::new_local().direct_fds_per_ring(n);
        init_local_runtime_and_context(Some(builder))?;

        crate::block_on(async {
            use crate::sqe::IoError;

            let mut sockets = Vec::with_capacity(n as usize);

            for _ in 0..n {
                let sockfd = socket_op(KernelFdMode::DirectAuto)
                    .await
                    .expect("failed to create sockfd");
                sockets.push(sockfd);
            }

            // Creating one more exhausts our fixed slots and triggers -ENFILE
            assert!(
                matches!(socket_op(KernelFdMode::DirectAuto).await, Err(IoError::Io(e)) if e.raw_os_error() == Some(libc::ENFILE))
            );
        });

        // We expect `n` async cleanup operations
        crate::with_scheduler!(|s| {
            let spawn_calls = s.tracker.get_calls(&Method::Spawn);
            assert_eq!(spawn_calls.len(), n as usize);
        });

        Ok(())
    }

    /// Cover all ownership transfer scenarios.
    #[derive(Debug, Clone, Copy)]
    enum Ownership {
        OwnedUnique,
        OwnedShared,
        Borrowed,
    }

    // This test covers the ownership transfer logic of `into_raw` and `into_fixed`.
    #[rstest]
    // == Success Cases ==
    #[case::owned_unique_legacy(KernelFdMode::Legacy, Ownership::OwnedUnique, true)]
    #[case::owned_unique_fixed(KernelFdMode::DirectAuto, Ownership::OwnedUnique, true)]
    // == Failure Cases ==
    #[case::owned_shared_legacy(KernelFdMode::Legacy, Ownership::OwnedShared, false)]
    #[case::owned_shared_fixed(KernelFdMode::DirectAuto, Ownership::OwnedShared, false)]
    #[case::borrowed_legacy(KernelFdMode::Legacy, Ownership::Borrowed, true)]
    #[case::borrowed_fixed(KernelFdMode::DirectAuto, Ownership::Borrowed, true)]
    #[ringolo::test]
    async fn test_uringfd_ownership_transfer(
        #[case] mode: KernelFdMode,
        #[case] ownership: Ownership,
        #[case] should_succeed: bool,
    ) -> Result<()> {
        {
            let (sockfd, _clone, _listener) = match ownership {
                Ownership::OwnedUnique => (socket_op(mode).await?, None, None),
                Ownership::OwnedShared => {
                    let sock = socket_op(mode).await?;
                    let sock_clone = sock.clone();
                    (sock, Some(sock_clone), None)
                }
                Ownership::Borrowed => {
                    // We need a real FD to borrow from.
                    let listener = TcpListener::bind("127.0.0.1:0")?;
                    let raw_fd = listener.as_raw_fd();
                    let sock = match mode {
                        KernelFdMode::Legacy => UringFd::new_raw_borrowed(raw_fd),
                        // This is incorrect, but for borrowing case it does not really
                        // matter as we dont own this FD anyways.
                        _ => UringFd::new_fixed_borrowed(raw_fd as u32),
                    };
                    (sock, None, Some(listener))
                }
            };

            // Perform the ownership transfer and assert the expected outcome
            let result = match sockfd.is_raw() {
                true => unsafe { sockfd.into_raw() }.map(|_| ()),
                false => unsafe { sockfd.into_fixed() }.map(|_| ()),
            };

            if should_succeed {
                assert!(result.is_ok(), "Expected ownership transfer to succeed");
            } else {
                assert!(result.is_err(), "Expected ownership transfer to fail");
                // Verify the error type for shared/borrowed failures
                match ownership {
                    Ownership::OwnedShared => {
                        assert!(matches!(
                            result.unwrap_err(),
                            OpcodeError::Ownership(OwnershipError::SharedFd(_))
                        ));
                    }
                    Ownership::Borrowed => {
                        assert!(matches!(
                            result.unwrap_err(),
                            OpcodeError::Ownership(OwnershipError::BorrowedFd)
                        ));
                    }
                    _ => {}
                }
            }
        } // All uring fds are dropped if they are owned

        // Check if the drop logic was correctly suppressed or triggered.
        crate::with_scheduler!(|s| {
            let spawn_calls = s.tracker.get_calls(&Method::Spawn);
            if should_succeed {
                // If transfer succeeded, the UringFd was consumed without dropping,
                // so no cleanup task should be spawned.
                assert_eq!(
                    spawn_calls.len(),
                    0,
                    "No cleanup task should be spawned on successful ownership transfer"
                );
            } else if matches!(ownership, Ownership::OwnedUnique | Ownership::OwnedShared) {
                // If transfer failed on a shared FD, one FD was consumed by the failed `into_*` call,
                // and the other (the clone) is dropped, triggering one cleanup.
                assert_eq!(spawn_calls.len(), 1);
            } else {
                // For borrowed FDs or other failure modes, no cleanup is ever spawned.
                assert_eq!(spawn_calls.len(), 0);
            }
        });

        Ok(())
    }
}
