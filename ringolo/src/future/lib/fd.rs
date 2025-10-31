use crate::future::lib::{Close, OpcodeError};
use crate::runtime::{CleanupTaskBuilder, spawn_cleanup};
use either::Either;
use io_uring::types::{DestinationSlot, Fd, Fixed};
use std::marker::PhantomData;
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

// We intentionally hide this UringFdKind to avoid making the RawFd/u32 accessible.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum UringFdKindInner {
    // Regular unregistered linux file descriptor
    Raw(RawFd),

    // IoUring registered per-thread file descriptor
    Fixed(u32),
}

impl UringFdKindInner {
    // Seamless integration with `io_uring` crate.
    fn as_raw_or_direct(&self) -> Either<Fd, Fixed> {
        match &self {
            UringFdKindInner::Raw(raw) => Either::Left(Fd(*raw)),
            UringFdKindInner::Fixed(fixed) => Either::Right(Fixed(*fixed)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OwnedUringFd {
    state: Arc<SharedUringFdState>,
}

#[derive(Debug, Copy, Clone)]
pub struct BorrowedUringFd<'a> {
    kind: UringFdKindInner,
    _lifetime: PhantomData<&'a ()>,
}

/// The shared state for an owned descriptor, with drop logic for cleanup.
#[derive(Debug)]
struct SharedUringFdState {
    kind: UringFdKindInner,
}

impl Drop for SharedUringFdState {
    fn drop(&mut self) {
        let task = CleanupTaskBuilder::new(Close::new(self.kind.as_raw_or_direct()));
        spawn_cleanup(task);
    }
}

impl OwnedUringFd {
    /// Creates a new `OwnedUringFd` from a raw file descriptor.
    pub fn new_raw(fd: RawFd) -> Self {
        Self {
            state: Arc::new(SharedUringFdState {
                kind: UringFdKindInner::Raw(fd),
            }),
        }
    }

    /// Creates a new `OwnedUringFd` from a fixed slot index.
    pub fn new_fixed(slot: u32) -> Self {
        Self {
            state: Arc::new(SharedUringFdState {
                kind: UringFdKindInner::Fixed(slot),
            }),
        }
    }

    /// Take the result from an `io_uring` opcode, and convert it back to an **owned** `UringFd`.
    /// Results from opcodes that create new file descriptors are always owned by us.
    pub fn from_result(res: i32, mode: KernelFdMode) -> Self {
        match mode {
            KernelFdMode::Legacy => Self::new_raw(res),
            // Kernel generates 0 on success in this mode, we must re-use the
            // slot given by the user.
            KernelFdMode::Direct(slot) => Self::new_fixed(slot),
            KernelFdMode::DirectAuto => Self::new_fixed(res as u32),
        }
    }

    /// Returns a borrowed handle to the underlying file descriptor.
    pub fn borrow<'a>(&'a self) -> BorrowedUringFd<'a> {
        BorrowedUringFd {
            // Does not increment refcount as we are not cloning the Arc.
            kind: self.state.kind,
            _lifetime: PhantomData,
        }
    }

    pub fn kind(&self) -> UringFdKind {
        self.state.kind.into()
    }

    pub fn is_raw(&self) -> bool {
        matches!(self.state.kind, UringFdKindInner::Raw(_))
    }

    pub fn is_fixed(&self) -> bool {
        matches!(self.state.kind, UringFdKindInner::Fixed(_))
    }

    pub fn has_exclusive_ownership(&self) -> bool {
        Arc::strong_count(&self.state) == 1
    }

    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.state)
    }

    /// Provides temporary access to the underlying raw file descriptor.
    ///
    /// This method allows you to run an operation on the raw file descriptor
    /// without taking ownership. It will return an error if the `OwnedUringFd`
    /// is not the `Raw` variant.
    ///
    /// The file descriptor is only valid for the duration of the closure.
    pub fn with_raw<F, R>(&self, f: F) -> Result<R, OpcodeError>
    where
        F: FnOnce(RawFd) -> R,
    {
        match &self.state.kind {
            UringFdKindInner::Raw(fd) => Ok(f(*fd)),
            _ => Err(OpcodeError::IncorrectFdVariant(self.kind())),
        }
    }

    /// Provides temporary access to the underlying fixed slot index.
    ///
    /// This method allows you to run an operation on the fixed slot index
    /// without taking ownership. It will return an error if the `OwnedUringFd`
    /// is not the `Fixed` variant.
    ///
    /// The slot index is only valid for the duration of the closure.
    pub fn with_fixed<F, R>(&self, f: F) -> Result<R, OpcodeError>
    where
        F: FnOnce(u32) -> R,
    {
        match &self.state.kind {
            UringFdKindInner::Fixed(slot) => Ok(f(*slot)),
            _ => Err(OpcodeError::IncorrectFdVariant(self.kind())),
        }
    }

    /// Consumes the `OwnedUringFd` and leaks the underlying raw file descriptor.
    ///
    /// This function will fail if:
    /// - The descriptor is shared (`strong_count > 1`).
    /// - The descriptor is a borrowed reference.
    /// - The descriptor is not the `Raw` variant.
    ///
    /// On success, the `OwnedUringFd` is consumed and its drop logic is disabled,
    /// transferring ownership of the file descriptor to the caller.
    pub unsafe fn leak_raw(self) -> Result<RawFd, OpcodeError> {
        if self.is_fixed() {
            return Err(OpcodeError::IncorrectFdVariant(self.kind()));
        }

        let state = match Arc::try_unwrap(self.state) {
            Ok(state) => state,
            Err(arc) => return Err(OpcodeError::SharedFd(Arc::strong_count(&arc))),
        };

        // Ensure the destructor for `SharedUringFdState` is not called.
        let state = ManuallyDrop::new(state);

        match &state.kind {
            UringFdKindInner::Raw(fd) => Ok(*fd),
            _ => unreachable!("can't be fixed or borrowed"),
        }
    }

    /// Consumes the `OwnedUringFd` and leaks the underlying fixed slot index.
    ///
    /// This is the `Fixed` variant of `into_raw`.
    pub unsafe fn leak_fixed(self) -> Result<u32, OpcodeError> {
        if self.is_raw() {
            return Err(OpcodeError::IncorrectFdVariant(self.kind()));
        }

        let state = match Arc::try_unwrap(self.state) {
            Ok(state) => state,
            Err(arc) => return Err(OpcodeError::SharedFd(Arc::strong_count(&arc))),
        };

        // Ensure the destructor for `SharedUringFdState` is not called.
        let state = ManuallyDrop::new(state);

        match &state.kind {
            UringFdKindInner::Fixed(slot) => Ok(*slot),
            _ => unreachable!("can't be raw or borrowed"),
        }
    }

    /// Unsafe API, ignores all checks and ownership rules.
    ///
    /// # Panics
    ///
    /// Panics if the `OwnedUringFd` variant is Fixed.
    unsafe fn get_raw_unchecked(&self) -> RawFd {
        match &self.state.kind {
            UringFdKindInner::Raw(fd) => *fd,
            _ => {
                panic_unexpected_variant();
            }
        }
    }

    /// Unsafe API, ignores all checks and ownership rules.
    ///
    /// # Panics
    ///
    /// Panics if the `OwnedUringFd` variant is a RawFd.
    unsafe fn get_fixed_unchecked(&self) -> u32 {
        match &self.state.kind {
            UringFdKindInner::Fixed(slot) => *slot,
            _ => {
                panic_unexpected_variant();
            }
        }
    }
}

impl<'a> BorrowedUringFd<'a> {
    /// Creates a new `BorrowedUringFd` from a raw file descriptor.
    pub fn new_raw(fd: RawFd) -> Self {
        Self {
            kind: UringFdKindInner::Raw(fd),
            _lifetime: PhantomData,
        }
    }

    /// Creates a new `BorrowedUringFd` from a fixed slot index.
    pub fn new_fixed(slot: u32) -> Self {
        Self {
            kind: UringFdKindInner::Fixed(slot),
            _lifetime: PhantomData,
        }
    }

    pub fn kind(&self) -> UringFdKind {
        self.kind.into()
    }

    pub fn is_raw(&self) -> bool {
        matches!(self.kind, UringFdKindInner::Raw(_))
    }

    pub fn is_fixed(&self) -> bool {
        matches!(self.kind, UringFdKindInner::Fixed(_))
    }

    /// Provides temporary access to the underlying raw file descriptor.
    ///
    /// This method allows you to run an operation on the raw file descriptor
    /// without taking ownership. It will return an error if the `BorrowedUringFd`
    /// is not the `Raw` variant.
    ///
    /// The file descriptor is only valid for the duration of the closure.
    pub fn with_raw<F, R>(&self, f: F) -> Result<R, OpcodeError>
    where
        F: FnOnce(RawFd) -> R,
    {
        match &self.kind {
            UringFdKindInner::Raw(fd) => Ok(f(*fd)),
            _ => Err(OpcodeError::IncorrectFdVariant(self.kind())),
        }
    }

    /// Provides temporary access to the underlying fixed slot index.
    ///
    /// This method allows you to run an operation on the fixed slot index
    /// without taking ownership. It will return an error if the `BorrowedUringFd`
    /// is not the `Fixed` variant.
    ///
    /// The slot index is only valid for the duration of the closure.
    pub fn with_fixed<F, R>(&self, f: F) -> Result<R, OpcodeError>
    where
        F: FnOnce(u32) -> R,
    {
        match &self.kind {
            UringFdKindInner::Fixed(slot) => Ok(f(*slot)),
            _ => Err(OpcodeError::IncorrectFdVariant(self.kind())),
        }
    }

    /// Unsafe API, ignores all checks and ownership rules.
    ///
    /// # Panics
    ///
    /// Panics if the `OwnedUringFd` variant is Fixed.
    unsafe fn get_raw_unchecked(&self) -> RawFd {
        match &self.kind {
            UringFdKindInner::Raw(fd) => *fd,
            _ => {
                panic_unexpected_variant();
            }
        }
    }

    /// Unsafe API, ignores all checks and ownership rules.
    ///
    /// # Panics
    ///
    /// Panics if the `OwnedUringFd` variant is a RawFd.
    unsafe fn get_fixed_unchecked(&self) -> u32 {
        match &self.kind {
            UringFdKindInner::Fixed(slot) => *slot,
            _ => {
                panic_unexpected_variant();
            }
        }
    }
}

// Prevent CPU branch prediction from pulling in panic unwind code.
#[cold]
#[track_caller]
fn panic_unexpected_variant() -> ! {
    panic!("Unexpected UringFd variant");
}

impl PartialEq for OwnedUringFd {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl<'a> PartialEq for BorrowedUringFd<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

/// Trait for integration with the `io_uring` crate to easily create Entry from
/// Owned or Borrowed uring fd. This API is *unsafe* as you have the possibility
/// of leaking resources.
//
// TODO: this is really garbage, bothers me to no end but can't think of a better
// way to be able to integrate with `io_uring` Entry builder methods...
pub trait AsRawOrDirect {
    unsafe fn as_raw_or_direct(&self) -> Either<Fd, Fixed>;
}

impl AsRawOrDirect for OwnedUringFd {
    unsafe fn as_raw_or_direct(&self) -> Either<Fd, Fixed> {
        self.state.kind.as_raw_or_direct()
    }
}

impl<'a> AsRawOrDirect for BorrowedUringFd<'a> {
    unsafe fn as_raw_or_direct(&self) -> Either<Fd, Fixed> {
        self.kind.as_raw_or_direct()
    }
}

// Safety: we use `as_raw_or_direct()` API and leak a file descriptor because we need
// to unpack Entries in this module to create `io_uring` operations.
macro_rules! resolve_fd {
    ($uring_fd:expr, |$fd_ident:ident| $body:expr) => {
        unsafe {
            match $uring_fd.as_raw_or_direct() {
                either::Either::Left($fd_ident) => $body,
                either::Either::Right($fd_ident) => $body,
            }
        }
    };
}

impl From<Fd> for OwnedUringFd {
    fn from(fd: Fd) -> Self {
        OwnedUringFd::new_raw(fd.0)
    }
}

impl From<Fixed> for OwnedUringFd {
    fn from(fd: Fixed) -> Self {
        OwnedUringFd::new_fixed(fd.0)
    }
}

#[cfg(test)]
mod tests {
    use std::{net::TcpListener, os::fd::AsRawFd};

    use super::*;
    use crate as ringolo;
    use crate::sqe::IoError;
    use crate::{
        runtime::{Builder, TaskOpts},
        test_utils::*,
        utils::scheduler::{Call, Method},
    };
    use anyhow::{Context, Result};
    use rstest::rstest;

    #[ringolo::test]
    async fn test_owned_uringfd() -> Result<()> {
        {
            let sockfd = tcp_socket4(KernelFdMode::DirectAuto)
                .await
                .context("failed to create sockfd")?;

            assert!(sockfd.is_fixed());
            assert_eq!(sockfd.strong_count(), 1);

            // io_uring integration
            assert!(matches!(
                unsafe { sockfd.as_raw_or_direct() },
                either::Either::Right(Fixed(_))
            ));
            assert!(matches!(sockfd.kind(), UringFdKind::Fixed));

            // Clone a few times
            let sockfd1 = sockfd.clone();
            let sockfd2 = sockfd.clone();
            assert!(sockfd1.is_fixed() && sockfd2.is_fixed());
            assert_eq!(sockfd.strong_count(), 3);

            // Drop 2 - no async cleanup
            drop(sockfd);
            drop(sockfd1);
            assert_eq!(sockfd2.strong_count(), 1);
        } // last ref dropped - triggers async close

        crate::with_scheduler!(|s| {
            let spawn_calls = s.tracker.get_calls(&Method::Spawn);
            assert_eq!(spawn_calls.len(), 1);
            assert!(
                matches!(spawn_calls[0], Call::Spawn { opts, .. } if opts == TaskOpts::BACKGROUND_TASK | s.default_task_opts)
            );
        });

        Ok(())
    }

    #[ringolo::test]
    async fn test_borrowed_uringfd() -> Result<()> {
        {
            let listener =
                TcpListener::bind("127.0.0.1:0").context("could not create tcplistener")?;
            let listener_fd = listener.as_raw_fd();

            let fd = BorrowedUringFd::new_raw(listener_fd);
            assert!(fd.is_raw());

            // io_uring integration
            assert!(matches!(
                unsafe { fd.as_raw_or_direct() },
                either::Either::Left(Fd(_))
            ));
            assert!(matches!(fd.kind(), UringFdKind::Raw));

            // Clone a few times
            let fd1 = fd.clone();
            let fd2 = fd.clone();
            assert!(fd1.is_raw() && fd2.is_raw());
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
        let (_runtime, scheduler) = init_local_runtime_and_context(Some(builder))?;

        crate::block_on(async {
            let mut sockets = Vec::with_capacity(n as usize);

            for _ in 0..n {
                let sockfd = tcp_socket4(KernelFdMode::DirectAuto)
                    .await
                    .expect("failed to create sockfd");
                sockets.push(sockfd);
            }

            // Creating one more exhausts our fixed slots and triggers -ENFILE
            assert!(
                matches!(tcp_socket4(KernelFdMode::DirectAuto).await, Err(IoError::Io(e)) if e.raw_os_error() == Some(libc::ENFILE))
            );
        });

        // We expect `n` async cleanup operations
        let spawn_calls = scheduler.tracker.get_calls(&Method::Spawn);
        assert_eq!(spawn_calls.len(), n as usize);

        Ok(())
    }

    #[rstest]
    // == Success Cases ==
    #[case::owned_unique_legacy(KernelFdMode::Legacy, 0, Ok(()))]
    #[case::owned_unique_fixed(KernelFdMode::DirectAuto, 0, Ok(()))]
    // == Failure Cases ==
    #[case::owned_shared_legacy(KernelFdMode::Legacy, 1, Err(OpcodeError::SharedFd(2)))]
    #[case::owned_shared_fixed(KernelFdMode::DirectAuto, 4, Err(OpcodeError::SharedFd(5)))]
    #[ringolo::test]
    async fn test_uringfd_leak_ownership(
        #[case] mode: KernelFdMode,
        #[case] n_clones: usize,
        #[case] expected: Result<(), OpcodeError>,
    ) -> Result<()> {
        {
            let (sockfd, _clones) = {
                let sockfd = tcp_socket4(mode).await.context("failed to create socket")?;
                let clones = std::iter::repeat(sockfd.clone())
                    .take(n_clones)
                    .collect::<Vec<_>>();

                (sockfd, clones)
            };

            // Perform the ownership transfer and assert the expected outcome
            let result = match sockfd.kind() {
                UringFdKind::Raw => unsafe { sockfd.leak_raw() }.map(|_| ()),
                UringFdKind::Fixed => unsafe { sockfd.leak_fixed() }.map(|_| ()),
            };

            assert_eq!(result, expected);
        }

        // If we successfully leaked the UringFd, expected async cleanup to have spawned.
        if expected.is_ok() {
            crate::with_scheduler!(|s| {
                let spawn_calls = s.tracker.get_calls(&Method::Spawn);
                assert_eq!(
                    spawn_calls.len(),
                    0,
                    "No cleanup task should be spawned on successful ownership transfer"
                );
            });
        }

        Ok(())
    }
}
