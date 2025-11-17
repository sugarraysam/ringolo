use crate::context;
use crate::future::lib::OpcodeError;
use crate::task::Header;
use either::Either;
use io_uring::types::{DestinationSlot, Fd, Fixed};
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::os::fd::RawFd;
use std::ptr::NonNull;
use std::sync::Arc;
use std::task::Waker;

/// Instructs how we want to retrieve file descriptors from the kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KernelFdMode {
    /// Return a regular OS file descriptor from the process `fd` table.
    Legacy,

    /// Link the resource to a specific slot in the `io_uring` registered file table.
    /// **Advanced Use Only.**
    Direct(u32),

    /// Automatically link the resource to an available slot in the `io_uring`
    /// registered fds table. Uses the `IORING_FILE_INDEX_ALLOC` feature.
    ///
    /// **This is the default mode.**
    #[default]
    DirectAuto,
}

impl KernelFdMode {
    pub(super) fn try_into_slot(self) -> Result<Option<DestinationSlot>, OpcodeError> {
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
    /// Regular unregistered linux file descriptor
    Raw,

    /// IoUring registered per-thread file descriptor
    Direct,
}

impl From<UringFdKindInner> for UringFdKind {
    fn from(kind: UringFdKindInner) -> Self {
        match kind {
            UringFdKindInner::Raw(_) => UringFdKind::Raw,
            UringFdKindInner::Direct(_) => UringFdKind::Direct,
        }
    }
}

// We intentionally hide this UringFdKind to avoid making the RawFd/u32 accessible.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum UringFdKindInner {
    // Regular unregistered linux file descriptor
    Raw(RawFd),

    // IoUring registered per-thread file descriptor
    Direct(u32),
}

impl UringFdKindInner {
    // Seamless integration with `io_uring` crate.
    fn as_raw_or_direct(&self) -> Either<Fd, Fixed> {
        match &self {
            UringFdKindInner::Raw(raw) => Either::Left(Fd(*raw)),
            UringFdKindInner::Direct(direct) => Either::Right(Fixed(*direct)),
        }
    }
}

/// An owned handle to an `io_uring` file descriptor.
///
/// This struct provides **RAII** (Resource Acquisition Is Initialization) semantics
/// for both Raw (legacy) and Direct (registered) descriptors.
///
/// # Resource Tracking
///
/// When this struct is dropped, it:
/// 1.  Decrements the `owned_resources` counter on the associated Task (when
///     using direct descriptors).
/// 2.  Schedules an async close operation on the runtime's maintenance task.
#[derive(Debug, Clone)]
pub struct OwnedUringFd {
    state: Arc<SharedUringFdState>,
}

/// A temporary reference to an `OwnedUringFd`.
///
/// Used to pass descriptors into operations (like `Read` or `Write`) without
/// transferring ownership or incrementing reference counts.
#[derive(Debug, Copy, Clone)]
pub struct BorrowedUringFd<'a> {
    kind: UringFdKindInner,

    // Safety: We use a lifetime to bound BorrowedUringFd to the OwningUringFd.
    _lifetime: PhantomData<&'a ()>,
}

/// The shared state for an owned descriptor, with drop logic for cleanup.
#[derive(Debug)]
struct SharedUringFdState {
    kind: UringFdKindInner,

    // Pointer to the associated task where this OwnedUringFd was created. Only
    // used for direct descriptor, and the purpose is to keep track of locally
    // owned resources.
    task: Option<NonNull<Header>>,
}

// Safety: has to be Send/Sync because all futures on the runtime have this requirement.
// We prevent leaking local `iouring` direct descriptors through the `owned_resources`
// per-task counter,
unsafe impl Send for SharedUringFdState {}
unsafe impl Sync for SharedUringFdState {}

impl Drop for SharedUringFdState {
    fn drop(&mut self) {
        if let Some(task) = self.task {
            // Safety: per encapsulation, if we drop the OwnedUringFd, we can assume
            // the future where it was created is still alive, and by the same logic
            // the task that owns this future is also still alive.
            unsafe {
                Header::decrement_owned_resources(task);
            }
        }

        // We are handing this FD off to the maintenance task to be closed. To
        // ensure correct accounting, we decrement the original task's `owned_resources`
        // count *before* transferring ownership of the FD to the maintenance task.
        crate::async_close(self.kind.as_raw_or_direct());
    }
}

impl OwnedUringFd {
    /// Creates a new `OwnedUringFd` from a raw file descriptor.
    pub fn new_raw(fd: RawFd) -> Self {
        Self {
            state: Arc::new(SharedUringFdState {
                kind: UringFdKindInner::Raw(fd),
                task: None,
            }),
        }
    }

    /// Creates a new `OwnedUringFd` from a direct descriptor index.
    pub fn new_direct(waker: &Waker, slot: u32) -> Self {
        // A direct descriptor is only valid on the current thread. Increment the
        // task counter for locally owned resources to prevent task from moving
        // to another thread while we hold this descriptor.
        let task = context::with_core(|core| core.increment_task_owned_resources(waker));

        Self {
            state: Arc::new(SharedUringFdState {
                kind: UringFdKindInner::Direct(slot),
                task,
            }),
        }
    }

    /// Take the result from an `io_uring` opcode, and convert it back to an **owned** `UringFd`.
    /// Results from opcodes that create new file descriptors are always owned by us.
    pub fn from_result(res: i32, mode: KernelFdMode, waker: &Waker) -> Self {
        match mode {
            KernelFdMode::Legacy => Self::new_raw(res),
            // Kernel generates 0 on success in this mode, we must re-use the
            // slot given by the user.
            KernelFdMode::Direct(slot) => Self::new_direct(waker, slot),
            KernelFdMode::DirectAuto => Self::new_direct(waker, res as u32),
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

    /// Returns the kind of descriptor (Raw or Direct) currently held.
    pub fn kind(&self) -> UringFdKind {
        self.state.kind.into()
    }

    /// Returns `true` if it is a standard OS file descriptor.
    pub fn is_raw(&self) -> bool {
        matches!(self.state.kind, UringFdKindInner::Raw(_))
    }

    /// Returns `true` if it is an `io_uring` direct descriptor.
    pub fn is_direct(&self) -> bool {
        matches!(self.state.kind, UringFdKindInner::Direct(_))
    }

    /// Returns the number of strong references to the underlying descriptor state.
    #[cfg(test)]
    pub(super) fn strong_count(&self) -> usize {
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

    /// Provides temporary access to the underlying direct descriptor index.
    ///
    /// This method allows you to run an operation on direct descriptor index
    /// without taking ownership. It will return an error if the `OwnedUringFd`
    /// is not the `Direct` variant.
    ///
    /// The index is only valid for the duration of the closure.
    pub fn with_direct<F, R>(&self, f: F) -> Result<R, OpcodeError>
    where
        F: FnOnce(u32) -> R,
    {
        match &self.state.kind {
            UringFdKindInner::Direct(slot) => Ok(f(*slot)),
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
    ///
    /// # Safety
    ///
    /// Caller is responsible to close the raw descriptor.
    pub unsafe fn leak_raw(self) -> Result<RawFd, OpcodeError> {
        if self.is_direct() {
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
            _ => unreachable!("can't be direct or borrowed"),
        }
    }

    /// Consumes the `OwnedUringFd` and leaks the underlying direct descriptor
    /// index.
    ///
    /// This is the `Direct` variant of `into_raw`.
    ///
    /// # Safety
    ///
    /// Caller is responsible to close the direct descriptor.
    pub unsafe fn leak_direct(self) -> Result<u32, OpcodeError> {
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
            UringFdKindInner::Direct(slot) => Ok(*slot),
            _ => unreachable!("can't be raw or borrowed"),
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

    /// Creates a new `BorrowedUringFd` from a direct descriptor index.
    pub fn new_direct(slot: u32) -> Self {
        Self {
            kind: UringFdKindInner::Direct(slot),
            _lifetime: PhantomData,
        }
    }

    /// Returns the kind of descriptor (Raw or Direct) currently being borrowed.
    pub fn kind(&self) -> UringFdKind {
        self.kind.into()
    }

    /// Returns `true` if it is a standard OS file descriptor.
    pub fn is_raw(&self) -> bool {
        matches!(self.kind, UringFdKindInner::Raw(_))
    }

    /// Returns `true` if it is an `io_uring` direct descriptor.
    pub fn is_direct(&self) -> bool {
        matches!(self.kind, UringFdKindInner::Direct(_))
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

    /// Provides temporary access to the underlying direct descriptor.
    ///
    /// This method allows you to run an operation on the direct descriptor index
    /// without taking ownership. It will return an error if the `BorrowedUringFd`
    /// is not the `Direct` variant.
    ///
    /// The slot index is only valid for the duration of the closure.
    pub fn with_direct<F, R>(&self, f: F) -> Result<R, OpcodeError>
    where
        F: FnOnce(u32) -> R,
    {
        match &self.kind {
            UringFdKindInner::Direct(slot) => Ok(f(*slot)),
            _ => Err(OpcodeError::IncorrectFdVariant(self.kind())),
        }
    }
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
/// Owned or Borrowed uring fd.
///
/// This API is *unsafe* as you have the possibility of leaking resources.
/// *Internal use only* - mostly to power `resolve_fd!` macro.
pub(super) trait AsRawOrDirect {
    /// Returns the underlying descriptor as an `Either` type compatible with `io_uring` types.
    ///
    /// # Safety
    ///
    /// This exposes the raw resource. The caller must ensure the resource remains valid
    /// for the duration of its usage in the ring.
    unsafe fn as_raw_or_direct(&self) -> Either<Fd, Fixed>;
}

impl AsRawOrDirect for OwnedUringFd {
    unsafe fn as_raw_or_direct(&self) -> Either<Fd, Fixed> {
        self.state.kind.as_raw_or_direct()
    }
}

impl AsRawOrDirect for BorrowedUringFd<'_> {
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

#[cfg(test)]
mod tests {
    use std::{net::TcpListener, os::fd::AsRawFd};

    use super::*;
    use crate as ringolo;
    use crate::context;
    use crate::future::lib::Op;
    use crate::future::lib::ops::OpenAt;
    use crate::future::lib::types::{Mode, OFlag};
    use crate::runtime::Builder;
    use crate::sqe::IoError;
    use crate::test_utils::*;
    use anyhow::{Context, Result, anyhow};
    use rstest::rstest;
    use std::task::Poll;

    #[ringolo::test]
    async fn test_owned_uringfd() -> Result<()> {
        {
            let sockfd = tcp_socket4(KernelFdMode::DirectAuto)
                .await
                .context("failed to create sockfd")?;

            assert!(sockfd.is_direct());
            assert_eq!(sockfd.strong_count(), 1);

            // io_uring integration
            assert!(matches!(
                unsafe { sockfd.as_raw_or_direct() },
                either::Either::Right(Fixed(_))
            ));
            assert!(matches!(sockfd.kind(), UringFdKind::Direct));

            // Clone a few times
            let sockfd1 = sockfd.clone();
            let sockfd2 = sockfd.clone();
            assert!(sockfd1.is_direct() && sockfd2.is_direct());
            assert_eq!(sockfd.strong_count(), 3);

            // Drop 2 - no async cleanup
            drop(sockfd);
            drop(sockfd1);
            assert_eq!(sockfd2.strong_count(), 1);
        } // last ref dropped - triggers async close

        assert_inflight_cleanup(1);
        wait_for_cleanup().await;

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

        assert_inflight_cleanup(0);
        Ok(())
    }

    #[rstest]
    #[case::n_4(4)]
    #[case::n_8(8)]
    #[case::n_16(16)]
    #[test]
    fn test_direct_descriptor_exhaustion(#[case] n: u32) -> Result<()> {
        let builder = Builder::new_local().direct_fds_per_ring(n);
        let (_runtime, _scheduler) = init_local_runtime_and_context(Some(builder))?;

        crate::block_on(async {
            let mut sockets = Vec::with_capacity(n as usize);

            for _ in 0..n {
                let sockfd = tcp_socket4(KernelFdMode::DirectAuto)
                    .await
                    .expect("failed to create sockfd");
                sockets.push(sockfd);
            }

            // Creating one more exhausts our direct descriptor table and triggers -ENFILE
            assert!(
                matches!(tcp_socket4(KernelFdMode::DirectAuto).await, Err(IoError::Io(e)) if e.raw_os_error() == Some(libc::ENFILE))
            );
        });

        Ok(())
    }

    #[rstest]
    // == Success Cases ==
    #[case::owned_unique_legacy(KernelFdMode::Legacy, 0, Ok(()))]
    #[case::owned_unique_direct(KernelFdMode::DirectAuto, 0, Ok(()))]
    // == Failure Cases ==
    #[case::owned_shared_legacy(KernelFdMode::Legacy, 1, Err(OpcodeError::SharedFd(2)))]
    #[case::owned_shared_direct(KernelFdMode::DirectAuto, 4, Err(OpcodeError::SharedFd(5)))]
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
                UringFdKind::Direct => unsafe { sockfd.leak_direct() }.map(|_| ()),
            };

            assert_eq!(result, expected);
        }

        // No cleanup happens if we successfully leaked the UringFd
        if expected.is_ok() {
            assert_inflight_cleanup(0);
        } else {
            assert_inflight_cleanup(1);
        }

        Ok(())
    }

    #[rstest]
    #[case::two(2)]
    #[case::five(5)]
    #[case::ten(10)]
    fn test_owned_resources_tracking(#[case] num_fds: usize) -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;
        let (waker, _) = mock_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        let header = unsafe { NonNull::new_unchecked(waker.data() as *mut Header).as_ref() };

        let mut ops = Vec::with_capacity(num_fds);
        for _ in 0..num_fds {
            let mut op = Box::pin(Op::new(OpenAt::try_new(
                KernelFdMode::DirectAuto,
                None,
                gen_tempfile_name(),
                OFlag::O_RDONLY | OFlag::O_CREAT,
                Mode::S_IRUSR | Mode::S_IWUSR,
            )?));

            assert!(matches!(op.as_mut().poll(&mut cx), Poll::Pending));
            ops.push(op);
        }

        // Submit and complete
        context::with_slab_and_ring_mut(|slab, ring| -> Result<()> {
            ring.submit_and_wait(num_fds, None)?;
            ring.process_cqes(slab, None)?;
            Ok(())
        })?;

        let mut fds = Vec::with_capacity(num_fds);
        for mut op in ops {
            match op.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(fd)) => fds.push(fd),
                _ => return Err(anyhow!("Failed to open one of the tempfiles")),
            }
        }

        let assert_owned_dropped = |num_owned: u32, num_dropped: usize| {
            assert_eq!(header.is_stealable(), num_owned == 0,);
            assert_eq!(header.get_owned_resources(), num_owned);
            assert_eq!(
                context::with_core(|c| c.maintenance_task.cleanup_handler.len()),
                num_dropped
            );
        };

        assert_owned_dropped(num_fds as u32, 0);

        // Borrowing has no impact on resource tracking
        let borrowed = fds
            .iter()
            .take(num_fds / 2)
            .map(|fd| fd.borrow())
            .collect::<Vec<_>>();
        assert_owned_dropped(num_fds as u32, 0);
        drop(borrowed);
        assert_owned_dropped(num_fds as u32, 0);

        // Dropping frees resources
        drop(fds);
        assert_owned_dropped(0, num_fds);

        Ok(())
    }
}
