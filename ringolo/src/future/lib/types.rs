use bitflags::bitflags;

/// Specifies the address family (domain) used for socket communication.
///
/// This corresponds to the `domain` argument in `socket(2)`.
///
/// # References
///
/// [address_families(7)](https://man7.org/linux/man-pages/man7/address_families.7.html)
#[repr(i32)]
#[non_exhaustive]
#[derive(Copy, Clone, PartialEq, Eq, Debug, Hash)]
pub enum AddressFamily {
    /// Local communication (see [`unix(7)`](https://man7.org/linux/man-pages/man7/unix.7.html))
    Unix = libc::AF_UNIX,
    /// IPv4 Internet protocols (see [`ip(7)`](https://man7.org/linux/man-pages/man7/ip.7.html))
    Inet = libc::AF_INET,
    /// IPv6 Internet protocols (see [`ipv6(7)`](https://man7.org/linux/man-pages/man7/ipv6.7.html))
    Inet6 = libc::AF_INET6,
    /// Kernel interface for interacting with the routing table
    Route = libc::PF_ROUTE,
    /// IPX - Novell protocols
    Ipx = libc::AF_IPX,
    /// AppleTalk
    AppleTalk = libc::AF_APPLETALK,
    /// IBM SNA
    Sna = libc::AF_SNA,
    /// InfiniBand native addressing
    #[cfg(all(target_os = "linux", not(target_env = "uclibc")))]
    Ib = libc::AF_IB,
    /// Multiprotocol Label Switching
    #[cfg(all(target_os = "linux", not(target_env = "uclibc")))]
    Mpls = libc::AF_MPLS,
    /// Bluetooth low-level socket protocol
    Bluetooth = libc::AF_BLUETOOTH,
    /// New "modular ISDN" driver interface protocol
    Isdn = libc::AF_ISDN,
    /// Near field communication
    Nfc = libc::AF_NFC,
}
bitflags! {
    /// `AT_*` constants for use with [`LinkAt`], and other `*at` ops.
    ///
    /// [`LinkAt`]: crate::future::lib::ops::LinkAt
    #[repr(transparent)]
    #[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
    pub struct AtFlags: libc::c_int {
        /// Do not dereference symbolic links; operate on the link itself.
        const SYMLINK_NOFOLLOW = libc::AT_SYMLINK_NOFOLLOW;

        /// Perform access checks using the effective user and group IDs.
        const EACCESS = libc::AT_EACCESS;

        /// Remove a directory instead of a file (equivalent to `rmdir`).
        const REMOVEDIR = libc::AT_REMOVEDIR;

        /// Follow symbolic links if the path is a link.
        const SYMLINK_FOLLOW = libc::AT_SYMLINK_FOLLOW;

        /// Suppress automounting if the path refers to an automount point.
        const NO_AUTOMOUNT = libc::AT_NO_AUTOMOUNT;

        /// Operate on the file descriptor directly if the path is an empty string.
        const EMPTY_PATH = libc::AT_EMPTY_PATH;

        /// Synchronize with the filesystem based on the default `stat()` behavior.
        const STATX_SYNC_AS_STAT = libc::AT_STATX_SYNC_AS_STAT;

        /// Force synchronization with the backing storage or server to ensure fresh data.
        const STATX_FORCE_SYNC = libc::AT_STATX_FORCE_SYNC;

        /// Do not synchronize; accept cached attributes if they are available.
        const STATX_DONT_SYNC = libc::AT_STATX_DONT_SYNC;

        /// <https://docs.rs/bitflags/*/bitflags/#externally-defined-flags>
        const _ = !0;
    }
}

/// Represents an event returned by `epoll_wait`.
///
/// This structure encapsulates the events that occurred and the user data
/// associated with the file descriptor.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct EpollEvent {
    event: libc::epoll_event,
}

impl EpollEvent {
    /// Creates a new `EpollEvent` with the specified flags and user data.
    pub fn new(events: EpollFlags, data: u64) -> Self {
        EpollEvent {
            event: libc::epoll_event {
                events: events.bits() as u32,
                u64: data,
            },
        }
    }

    /// Creates an empty `EpollEvent` initialized to zero.
    pub fn empty() -> Self {
        unsafe { std::mem::zeroed::<EpollEvent>() }
    }

    /// Returns the `EpollFlags` associated with this event.
    pub fn events(&self) -> EpollFlags {
        EpollFlags::from_bits(self.event.events as libc::c_int).unwrap()
    }

    /// Returns the user data associated with this event.
    pub fn data(&self) -> u64 {
        self.event.u64
    }
}

bitflags!(
    /// Available flags for epoll family of syscalls. See [`epoll_ctl man page`]
    /// for the complete description of all these flags. For completion, we list
    /// legacy flags inherited from [`poll`] as well.
    ///
    /// [`epoll_ctl man page`]: https://man7.org/linux/man-pages/man2/epoll_ctl.2.html
    /// [`poll`]: https://man7.org/linux/man-pages/man2/poll.2.html
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct EpollFlags: libc::c_int {
        /// The associated file is available for read(2) operations.
        const EPOLLIN = libc::EPOLLIN;

        /// There is an exceptional condition on the file descriptor.
        /// See the discussion of POLLPRI in poll(2).
        const EPOLLPRI = libc::EPOLLPRI;

        /// The associated file is available for write(2) operations.
        const EPOLLOUT = libc::EPOLLOUT;

        /// Error condition happened on the associated file descriptor.
        const EPOLLERR = libc::EPOLLERR;

        /// Hang up happened on the associated file descriptor.
        const EPOLLHUP = libc::EPOLLHUP;

        /// Stream socket peer closed connection, or shut down writing
        /// half of connection.
        const EPOLLRDHUP = libc::EPOLLRDHUP;

        /// Sets an exclusive wakeup mode for the epoll file descriptor
        /// that is being attached to the target file descriptor, fd.
        const EPOLLEXCLUSIVE = libc::EPOLLEXCLUSIVE;

        /// If `EPOLLONESHOT` and `EPOLLET` are clear and the process has
        /// the `CAP_BLOCK_SUSPEND` capability, ensure that the system
        /// does not enter "suspend" or "hibernate" while this event is
        /// pending or being processed.
        const EPOLLWAKEUP = libc::EPOLLWAKEUP;

        /// Requests one-shot notification for the associated file
        /// descriptor.
        const EPOLLONESHOT = libc::EPOLLONESHOT;

        /// Requests edge-triggered notification for the associated
        /// file descriptor.
        const EPOLLET = libc::EPOLLET;

        /// [Legacy] Equivalent to EPOLLIN.
        const EPOLLRDNORM = libc::EPOLLRDNORM;

        /// [Legacy] Priority band data can be read (generally unused on Linux).
        const EPOLLRDBAND = libc::EPOLLRDBAND;

        /// [Legacy] Equivalent to EPOLLOUT.
        const EPOLLWRNORM = libc::EPOLLWRNORM;

        /// [Legacy] Priority data may be written.
        const EPOLLWRBAND = libc::EPOLLWRBAND;

        /// [Legacy] Unused.
        const EPOLLMSG = libc::EPOLLMSG;

        /// <https://docs.rs/bitflags/*/bitflags/#externally-defined-flags>
        const _ = !0;
    }
);

/// Valid operations for the [`epoll_ctl`](https://man7.org/linux/man-pages/man2/epoll_ctl.2.html) system call.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(i32)]
#[non_exhaustive]
pub enum EpollOp {
    /// Register the target file descriptor on the epoll instance.
    EpollCtlAdd = libc::EPOLL_CTL_ADD,
    /// Deregister the target file descriptor from the epoll instance.
    EpollCtlDel = libc::EPOLL_CTL_DEL,
    /// Change the event event associated with the target file descriptor.
    EpollCtlMod = libc::EPOLL_CTL_MOD,
}

bitflags!(
    /// Mode argument flags for fallocate determining operation performed on a given range.
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct FallocateFlags: libc::c_int {
        /// File size is not changed.
        ///
        /// offset + len can be greater than file size.
        const FALLOC_FL_KEEP_SIZE = libc::FALLOC_FL_KEEP_SIZE;

        /// Deallocates space by creating a hole.
        ///
        /// Must be ORed with FALLOC_FL_KEEP_SIZE. Byte range starts at offset and continues for len bytes.
        const FALLOC_FL_PUNCH_HOLE = libc::FALLOC_FL_PUNCH_HOLE;

        /// Removes byte range from a file without leaving a hole.
        ///
        /// Byte range to collapse starts at offset and continues for len bytes.
        const FALLOC_FL_COLLAPSE_RANGE = libc::FALLOC_FL_COLLAPSE_RANGE;

        /// Zeroes space in specified byte range.
        ///
        /// Byte range starts at offset and continues for len bytes.
        const FALLOC_FL_ZERO_RANGE = libc::FALLOC_FL_ZERO_RANGE;

        /// Increases file space by inserting a hole within the file size.
        ///
        /// Does not overwrite existing data. Hole starts at offset and continues for len bytes.
        const FALLOC_FL_INSERT_RANGE = libc::FALLOC_FL_INSERT_RANGE;

        /// Shared file data extants are made private to the file.
        ///
        /// Guarantees that a subsequent write will not fail due to lack of space.
        const FALLOC_FL_UNSHARE_RANGE = libc::FALLOC_FL_UNSHARE_RANGE;

        /// <https://docs.rs/bitflags/*/bitflags/#externally-defined-flags>
        const _ = !0;
    }
);

/// Wrapper around `futex_waitv` as used in [`futex_waitv` system
/// call](https://www.kernel.org/doc/html/latest/userspace-api/futex2.html).
#[derive(Default, Debug, Clone, Copy)]
#[repr(transparent)]
pub struct FutexWaiter(io_uring::types::FutexWaitV);

impl FutexWaiter {
    /// Creates a new, empty `FutexWaiter`.
    pub fn new() -> Self {
        Self(io_uring::types::FutexWaitV::new())
    }

    /// Sets the expected value of the futex.
    ///
    /// The kernel will atomically verify that the value at `uaddr` matches this `val`.
    /// If they do not match, the wait operation will fail immediately with `EAGAIN`.
    pub fn val(mut self, val: u64) -> Self {
        self.0 = self.0.val(val);
        self
    }

    /// Sets the user address (pointer) of the futex variable to monitor.
    ///
    /// This should be the virtual address of the integer in memory.
    pub fn uaddr(mut self, uaddr: u64) -> Self {
        self.0 = self.0.uaddr(uaddr);
        self
    }

    /// Configures the flags for this specific futex waiter.
    ///
    /// These flags determine the size of the futex variable (8, 16, 32, or 64 bits)
    /// and whether it is process-private or shared.
    pub fn flags(mut self, flags: Futex2Flags) -> Self {
        self.0 = self.0.flags(flags.bits());
        self
    }
}

bitflags! {
    /// `FUTEX2_*` flags for use with [`FutexWait`]
    ///
    /// Not to be confused with [`WaitvFlags`], which is passed as an argument
    /// to the `waitv` function.
    ///
    /// [`FutexWait`]: crate::future::lib::ops::FutexWait
    #[repr(transparent)]
    #[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
    pub struct Futex2Flags: u32 {
        /// `FUTEX_U8`
        const SIZE_U8 = linux_raw_sys::general::FUTEX2_SIZE_U8;
        /// `FUTEX_U16`
        const SIZE_U16 = linux_raw_sys::general::FUTEX2_SIZE_U16;
        /// `FUTEX_U32`
        const SIZE_U32 = linux_raw_sys::general::FUTEX2_SIZE_U32;
        /// `FUTEX_U64`
        const SIZE_U64 = linux_raw_sys::general::FUTEX2_SIZE_U64;
        /// `FUTEX_SIZE_MASK`
        const SIZE_MASK = linux_raw_sys::general::FUTEX2_SIZE_MASK;

        /// `FUTEX2_NUMA`
        const NUMA = linux_raw_sys::general::FUTEX2_NUMA;

        /// `FUTEX2_PRIVATE`
        const PRIVATE = linux_raw_sys::general::FUTEX2_PRIVATE;

        /// <https://docs.rs/bitflags/*/bitflags/#externally-defined-flags>
        const _ = !0;
    }
}

impl Default for Futex2Flags {
    fn default() -> Self {
        Self::empty()
    }
}

bitflags! {
    /// "File mode / permissions" flags.
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Mode: libc::mode_t {
        /// Read, write and execute for owner.
        const S_IRWXU = libc::S_IRWXU;

        /// Read for owner.
        const S_IRUSR = libc::S_IRUSR;

        /// Write for owner.
        const S_IWUSR = libc::S_IWUSR;

        /// Execute for owner.
        const S_IXUSR = libc::S_IXUSR;

        /// Read write and execute for group.
        const S_IRWXG = libc::S_IRWXG;

        /// Read for group.
        const S_IRGRP = libc::S_IRGRP;

        /// Write for group.
        const S_IWGRP = libc::S_IWGRP;

        /// Execute for group.
        const S_IXGRP = libc::S_IXGRP;

        /// Read, write and execute for other.
        const S_IRWXO = libc::S_IRWXO;

        /// Read for other.
        const S_IROTH = libc::S_IROTH;

        /// Write for other.
        const S_IWOTH = libc::S_IWOTH;

        /// Execute for other.
        const S_IXOTH = libc::S_IXOTH;

        /// Set user id on execution.
        const S_ISUID = libc::S_ISUID;

        /// Set group id on execution.
        const S_ISGID = libc::S_ISGID;

        /// Sticky bit.
        ///
        /// When applied to a directory, it restricts file deletion to the file owner,
        /// the directory owner, or the superuser.
        const S_ISVTX = libc::S_ISVTX;

        /// <https://docs.rs/bitflags/*/bitflags/#externally-defined-flags>
        const _ = !0;
    }
}

bitflags!(
    /// Configuration options for opened files.
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct OFlag: libc::c_int {
        /// Mask for the access mode of the file.
        const O_ACCMODE = libc::O_ACCMODE;

        /// Open the file in append-only mode.
        const O_APPEND = libc::O_APPEND;

        /// Generate a signal when input or output becomes possible.
        const O_ASYNC = libc::O_ASYNC;

        /// Closes the file descriptor once an `execve` call is made.
        ///
        /// Also sets the file offset to the beginning of the file.
        const O_CLOEXEC = libc::O_CLOEXEC;

        /// Create the file if it does not exist.
        const O_CREAT = libc::O_CREAT;

        /// Try to minimize cache effects of the I/O for this file.
        const O_DIRECT = libc::O_DIRECT;

        /// If the specified path isn't a directory, fail.
        const O_DIRECTORY = libc::O_DIRECTORY;

        /// Implicitly follow each `write()` with an `fdatasync()`.
        const O_DSYNC = libc::O_DSYNC;

        /// Error out if a file was not created.
        const O_EXCL = libc::O_EXCL;

        /// Same as `O_SYNC`.
        const O_FSYNC = libc::O_FSYNC;

        /// Allow files whose sizes can't be represented in an `off_t` to be opened.
        const O_LARGEFILE = libc::O_LARGEFILE;

        /// Do not update the file last access time during `read(2)`s.
        const O_NOATIME = libc::O_NOATIME;

        /// Don't attach the device as the process' controlling terminal.
        const O_NOCTTY = libc::O_NOCTTY;

        /// Same as `O_NONBLOCK`.
        const O_NDELAY = libc::O_NDELAY;

        /// `open()` will fail if the given path is a symbolic link.
        const O_NOFOLLOW = libc::O_NOFOLLOW;

        /// When possible, open the file in nonblocking mode.
        const O_NONBLOCK = libc::O_NONBLOCK;

        /// Obtain a file descriptor for low-level access.
        ///
        /// The file itself is not opened and other file operations will fail.
        const O_PATH = libc::O_PATH;

        /// Only allow reading.
        ///
        /// This should not be combined with `O_WRONLY` or `O_RDWR`.
        const O_RDONLY = libc::O_RDONLY;

        /// Allow both reading and writing.
        ///
        /// This should not be combined with `O_WRONLY` or `O_RDONLY`.
        const O_RDWR = libc::O_RDWR;

        /// Similar to `O_DSYNC` but applies to `read`s instead.
        const O_RSYNC = libc::O_RSYNC;

        /// Implicitly follow each `write()` with an `fsync()`.
        const O_SYNC = libc::O_SYNC;

        /// Create an unnamed temporary file.
        const O_TMPFILE = libc::O_TMPFILE;

        /// Truncate an existing regular file to 0 length if it allows writing.
        const O_TRUNC = libc::O_TRUNC;

        /// Only allow writing.
        ///
        /// This should not be combined with `O_RDONLY` or `O_RDWR`.
        const O_WRONLY = libc::O_WRONLY;

        /// <https://docs.rs/bitflags/*/bitflags/#externally-defined-flags>
        const _ = !0;
    }
);

/// Wrapper for the OpenHow struct.
//
// The reason we can't use the `nix::fcntl::OpenHow` is field access is private,
// so we can't do the transformation to `io_uring::types::OpenHow`.
#[derive(Debug)]
#[repr(transparent)]
pub struct OpenHow(io_uring::types::OpenHow);

impl Default for OpenHow {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenHow {
    /// Creates a new `OpenHow` with empty flags, mode, and resolve.
    pub fn new() -> Self {
        Self(io_uring::types::OpenHow::new())
    }

    /// Sets the `OFlag` for the `OpenHow` structure.
    pub fn with_flags(mut self, flags: OFlag) -> Self {
        self.0 = self.0.flags(flags.bits() as libc::c_ulonglong);
        self
    }

    /// Sets the `Mode` for the `OpenHow` structure.
    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.0 = self.0.mode(mode.bits() as libc::c_ulonglong);
        self
    }

    /// Sets the `ResolveFlag` for the `OpenHow` structure.
    pub fn with_resolve(mut self, resolve: ResolveFlag) -> Self {
        self.0 = self.0.resolve(resolve.bits());
        self
    }
}

/// The specific advice provided to [`posix_fadvise`](https://man7.org/linux/man-pages/man2/posix_fadvise.2.html).
#[repr(i32)]
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PosixFadviseAdvice {
    /// Revert to the default data access behavior.
    Normal,

    /// The file data will be accessed sequentially.
    Sequential,

    /// A hint that file data will be accessed randomly, and prefetching is likely not
    /// advantageous.
    Random,

    /// The specified data will only be accessed once and then not reused.
    NoReuse,

    /// The specified data will be accessed in the near future.
    WillNeed,

    /// The specified data will not be accessed in the near future.
    DontNeed,
}

bitflags! {
    /// Path resolution flags.
    ///
    /// See [path resolution(7)](https://man7.org/linux/man-pages/man7/path_resolution.7.html)
    /// for details of the resolution process.
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ResolveFlag: libc::c_ulonglong {
        /// Do not permit the path resolution to succeed if any component of
        /// the resolution is not a descendant of the directory indicated by
        /// dirfd.  This causes absolute symbolic links (and absolute values of
        /// pathname) to be rejected.
        const RESOLVE_BENEATH = libc::RESOLVE_BENEATH;

        /// Treat the directory referred to by dirfd as the root directory
        /// while resolving pathname.
        const RESOLVE_IN_ROOT = libc::RESOLVE_IN_ROOT;

        /// Disallow all magic-link resolution during path resolution. Magic
        /// links are symbolic link-like objects that are most notably found
        /// in proc(5);  examples include `/proc/[pid]/exe` and `/proc/[pid]/fd/*`.
        ///
        /// See symlink(7) for more details.
        const RESOLVE_NO_MAGICLINKS = libc::RESOLVE_NO_MAGICLINKS;

        /// Disallow resolution of symbolic links during path resolution. This
        /// option implies RESOLVE_NO_MAGICLINKS.
        const RESOLVE_NO_SYMLINKS = libc::RESOLVE_NO_SYMLINKS;

        /// Disallow traversal of mount points during path resolution (including
        /// all bind mounts).
        const RESOLVE_NO_XDEV = libc::RESOLVE_NO_XDEV;

        /// <https://docs.rs/bitflags/*/bitflags/#externally-defined-flags>
        const _ = !0;
    }
}

bitflags! {
    /// Additional socket options
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct SockFlag: libc::c_int {
        /// Set non-blocking mode on the new socket
        const SOCK_NONBLOCK = libc::SOCK_NONBLOCK;

        /// Set close-on-exec on the new descriptor
        const SOCK_CLOEXEC = libc::SOCK_CLOEXEC;

        /// <https://docs.rs/bitflags/*/bitflags/#externally-defined-flags>
        const _ = !0;
    }
}

/// Specifies the specific protocol to be used with the socket.
///
/// This corresponds to the `protocol` argument in [`socket(2)`](https://man7.org/linux/man-pages/man2/socket.2.html).
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SockProtocol {
    /// TCP protocol ([ip(7)](https://man7.org/linux/man-pages/man7/ip.7.html))
    Tcp = libc::IPPROTO_TCP,
    /// UDP protocol ([ip(7)](https://man7.org/linux/man-pages/man7/ip.7.html))
    Udp = libc::IPPROTO_UDP,
    /// Raw sockets ([raw(7)](https://man7.org/linux/man-pages/man7/raw.7.html))
    Raw = libc::IPPROTO_RAW,
    /// Packet filter on loopback traffic
    EthLoop = (libc::ETH_P_LOOP as u16).to_be() as i32,
    /// ICMP protocol ([icmp(7)](https://man7.org/linux/man-pages/man7/icmp.7.html))
    Icmp = libc::IPPROTO_ICMP,
    /// ICMPv6 protocol (ICMP over IPv6)
    IcmpV6 = libc::IPPROTO_ICMPV6,
}

/// Specifies the communication semantics (socket type).
///
/// This corresponds to the `type` argument in [`socket(2)`](https://man7.org/linux/man-pages/man2/socket.2.html).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(i32)]
#[non_exhaustive]
pub enum SockType {
    /// Provides sequenced, reliable, two-way, connection-
    /// based byte streams.  An out-of-band data transmission
    /// mechanism may be supported.
    Stream = libc::SOCK_STREAM,
    /// Supports datagrams (connectionless, unreliable
    /// messages of a fixed maximum length).
    Datagram = libc::SOCK_DGRAM,
    /// Provides a sequenced, reliable, two-way connection-
    /// based data transmission path for datagrams of fixed
    /// maximum length; a consumer is required to read an
    /// entire packet with each input system call.
    SeqPacket = libc::SOCK_SEQPACKET,
    /// Provides raw network protocol access.
    Raw = libc::SOCK_RAW,
    /// Provides a reliable datagram layer that does not
    /// guarantee ordering.
    Rdm = libc::SOCK_RDM,
}
