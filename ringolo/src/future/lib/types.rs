use nix::fcntl::OFlag;
use nix::fcntl::ResolveFlag;
use nix::sys::stat::Mode;

/// Wrapper for the OpenHow struct.
//
// The reason we can't use the `nix::fcntl::OpenHow` is field access is private,
// so we can't do the transformation to `io_uring::types::OpenHow`.
#[derive(Debug)]
pub struct OpenHow {
    flags: OFlag,
    mode: Mode,
    resolve: ResolveFlag,
}

impl OpenHow {
    pub fn new() -> Self {
        Self {
            flags: OFlag::empty(),
            mode: Mode::empty(),
            resolve: ResolveFlag::empty(),
        }
    }

    pub fn with_flags(mut self, flags: OFlag) -> Self {
        self.flags = flags;
        self
    }

    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_resolve(mut self, resolve: ResolveFlag) -> Self {
        self.resolve = resolve;
        self
    }
}

impl From<OpenHow> for io_uring::types::OpenHow {
    fn from(val: OpenHow) -> Self {
        io_uring::types::OpenHow::new()
            .flags(val.flags.bits() as libc::c_ulonglong)
            .mode(val.mode.bits() as libc::c_ulonglong)
            .resolve(val.resolve.bits())
    }
}
