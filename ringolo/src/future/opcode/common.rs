use super::parse;
use crate::future::opcode::OpcodeError;
use io_uring::types::{DestinationSlot, Fd, Fixed};
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;

// TODO:
// - resolve Fixed slot from iouring automagically

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

impl UringFd {
    pub fn new_raw(fd: RawFd) -> Self {
        Self {
            inner: Arc::new(UringFdInner {
                kind: UringFdKind::Raw(fd),
            }),
        }
    }

    pub fn new_fixed(slot: u32) -> Self {
        Self {
            inner: Arc::new(UringFdInner {
                kind: UringFdKind::Fixed(slot),
            }),
        }
    }

    pub fn kind(&self) -> UringFdKind {
        self.inner.kind
    }

    /// Take the result from `io_uring` opcode, and convert it back to `UringFd`
    pub fn from_result(res: i32, mode: KernelFdMode) -> Self {
        match mode {
            KernelFdMode::Legacy => UringFd::new_raw(res),
            // Kernel generates 0 on success in this mode, we must re-use the
            // slot given by the user.
            KernelFdMode::Direct(slot) => UringFd::new_fixed(slot),
            KernelFdMode::DirectAuto => UringFd::new_fixed(res as u32),
        }
    }

    pub fn is_raw(&self) -> bool {
        matches!(self.inner.kind, UringFdKind::Raw(_))
    }

    pub fn is_fixed(&self) -> bool {
        matches!(self.inner.kind, UringFdKind::Fixed(_))
    }
}

impl PartialEq for UringFd {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

#[macro_export]
macro_rules! resolve_fd {
    ($uring_fd:expr, |$fd_ident:ident| $body:expr) => {
        match $uring_fd.kind() {
            $crate::future::opcode::common::UringFdKind::Raw(raw) => {
                let $fd_ident = io_uring::types::Fd(raw);
                $body
            }
            $crate::future::opcode::common::UringFdKind::Fixed(fixed) => {
                let $fd_ident = io_uring::types::Fixed(fixed);
                $body
            }
        }
    };
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum UringFdKind {
    // Regular unregistered linux file descriptor
    Raw(RawFd),

    // IoUring registered per-thread file descriptor
    Fixed(u32),
}

#[derive(Debug)]
struct UringFdInner {
    pub(super) kind: UringFdKind,
}

// TODO:
// - async cleanup task framework refactor
// - Rc should be ok, cleanup_task sticky to this thread, so no need for Arc
// impl Drop for UringFdInner {
//     fn drop(&mut self) {
//         ringolo::spawn(async move {
//             if let Err(e) = Op::new(CloseOp::new(self.kind)).await {
//                 eprintln!("Failed to close UringFd {:?}: {}", e);
//             }
//         });
//     }
// }

impl From<i32> for UringFd {
    fn from(fd: i32) -> Self {
        UringFd::new_raw(fd)
    }
}

impl TryInto<i32> for UringFd {
    type Error = OpcodeError;

    fn try_into(self) -> Result<i32, Self::Error> {
        match self.inner.kind {
            UringFdKind::Raw(fd) => Ok(fd),
            UringFdKind::Fixed(_) => Err(OpcodeError::IncorrectFdVariant(self)),
        }
    }
}

impl From<u32> for UringFd {
    fn from(fd: u32) -> Self {
        UringFd::new_fixed(fd)
    }
}

impl TryInto<u32> for UringFd {
    type Error = OpcodeError;

    fn try_into(self) -> Result<u32, Self::Error> {
        match self.inner.kind {
            UringFdKind::Fixed(slot) => Ok(slot),
            UringFdKind::Raw(_) => Err(OpcodeError::IncorrectFdVariant(self)),
        }
    }
}

impl From<Fd> for UringFd {
    fn from(fd: Fd) -> Self {
        UringFd::new_raw(fd.0)
    }
}

impl From<Fixed> for UringFd {
    fn from(fd: Fixed) -> Self {
        UringFd::new_fixed(fd.0)
    }
}

impl TryFrom<UringFd> for Fixed {
    type Error = OpcodeError;

    fn try_from(value: UringFd) -> Result<Self, Self::Error> {
        match value.inner.kind {
            UringFdKind::Fixed(f) => Ok(Fixed(f)),
            _ => Err(OpcodeError::IncorrectFdVariant(value)),
        }
    }
}

impl TryFrom<UringFd> for Fd {
    type Error = OpcodeError;

    fn try_from(value: UringFd) -> Result<Self, Self::Error> {
        match value.inner.kind {
            UringFdKind::Raw(f) => Ok(Fd(f)),
            _ => Err(OpcodeError::IncorrectFdVariant(value)),
        }
    }
}

impl<T: Into<i32> + Copy> PartialEq<T> for UringFd {
    fn eq(&self, other: &T) -> bool {
        match self.inner.kind {
            UringFdKind::Raw(fd) => fd == (*other).into(),
            UringFdKind::Fixed(fixed) => (fixed as i32) == (*other).into(),
        }
    }
}

impl<T: Into<i32> + Copy> PartialOrd<T> for UringFd {
    fn partial_cmp(&self, other: &T) -> Option<std::cmp::Ordering> {
        match self.inner.kind {
            UringFdKind::Raw(fd) => fd.partial_cmp(&(*other).into()),
            UringFdKind::Fixed(fixed) => (fixed as i32).partial_cmp(&(*other).into()),
        }
    }
}

///
/// Socket integration
///
impl UringFd {
    // TODO: how to auto-resolve a Uring::Fixed ?
    pub fn socket_addr(&self) -> Result<SocketAddr, OpcodeError> {
        let raw = self.clone().try_into()?;
        parse::sockname(|buf, len| unsafe { libc::getsockname(raw, buf, len) })
            .map_err(OpcodeError::from)
    }
}
