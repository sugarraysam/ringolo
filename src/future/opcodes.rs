#![allow(clippy::all)]

// Required imports for the macro and the generated code.
use crate::sqe::{Sqe, SqeSingle};
use anyhow::Result;
use io_uring::squeue::Entry;
use io_uring::types::{
    DestinationSlot, Fd, Fixed, FsyncFlags, OpenHow, TimeoutFlags, Timespec, epoll_event, statx,
};
use std::future::Future;
use std::io;
use std::os::fd::RawFd;
use std::pin::{Pin, pin};
use std::task::{Context, Poll};

/// This macro generates a `Future` implementation for an `io_uring` opcode.
///
/// It handles the boilerplate for creating the `Future` struct, the `new`
/// constructor, and the `poll` method. It specifically supports optional
/// arguments by generating `if let Some` blocks to call the corresponding
/// builder methods on the opcode.
///
/// # Usage
/// `generate_io_uring_future!(
///     StructName,                      // The name of the struct to create.
///     io_uring::opcode::OpcodeName,    // The path to the io_uring opcode.
///     [                                // Required arguments for the `new` function.
///         arg_name: arg_type,
///         ...
///     ],
///     [                                // Optional arguments (Option<T>) for the `new` function.
///         opt_arg_name: opt_arg_type,  // The macro will call `op.opt_arg_name(value)`.
///         ...
///     ]
/// );`
#[macro_export]
macro_rules! generate_io_uring_future {
    (
        $name:ident,
        $opcode:path,
        ( $( $req_arg:ident: $req_ty:ty ),* ),
        ( $( $opt_arg:ident: $opt_ty:ty ),* )
    ) => { paste::paste! {

        pub struct $name {
            sqe: Sqe<SqeSingle>,
        }

        pub struct [<$name Builder>] {
            op: $opcode,
        }

        impl [<$name Builder>] {
            pub fn new(
                $( $req_arg: $req_ty, )*
            ) -> [<$name Builder>] {
                [<$name Builder>] {
                    op: $opcode::new($( $req_arg ),*),
                }
            }

            // Generate a builder method for each optional argument.
            $(
                pub fn $opt_arg(mut self, $opt_arg: $opt_ty) -> Self {
                    self.op = self.op.$opt_arg($opt_arg);
                    self
                }
            )*

            // The `build` method consumes the builder and returns the final struct.
            pub fn build(self) -> Result<$name> {
                Ok($name {
                    sqe: Sqe::new(SqeSingle::try_new(self.op.build())?),
                })
            }
        }

        // The `Future` implementation is consistent across all generated structs.
        impl Future for $name {
            type Output = Result<(Entry, io::Result<i32>)>;

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let mut pinned = pin!(&mut self.sqe);
                pinned.as_mut().poll(cx)
            }
        }
    }
}
}

generate_io_uring_future!(
    Accept,
    io_uring::opcode::Accept,
    ( fd: Fd, addr: *mut libc::sockaddr, addrlen: *mut libc::socklen_t ),
    ( file_index: Option<DestinationSlot>, flags: i32 )
);

generate_io_uring_future!(
    AcceptMulti,
    io_uring::opcode::AcceptMulti,
    ( fd: Fd ),
    ( allocate_file_index: bool, flags: i32 )
);

generate_io_uring_future!(
    Bind,
    io_uring::opcode::Bind,
    ( fd: Fd, addr: *const libc::sockaddr, addrlen: libc::socklen_t ),
    ()
);

generate_io_uring_future!(
    Close,
    io_uring::opcode::Close,
    ( fd: Fd ),
    ()
);

generate_io_uring_future!(
    Connect,
    io_uring::opcode::Connect,
    ( fd: Fd, addr: *const libc::sockaddr, addrlen: libc::socklen_t ),
    ()
);

generate_io_uring_future!(
    EpollCtl,
    io_uring::opcode::EpollCtl,
    ( epfd: Fd, fd: Fd, op: i32, event: *const epoll_event ),
    ()
);

generate_io_uring_future!(
    EpollWait,
    io_uring::opcode::EpollWait,
    ( fd: Fd, events: *mut epoll_event, maxevents: u32),
    ( flags: u32 )
);

generate_io_uring_future!(
    FGetXattr,
    io_uring::opcode::FGetXattr,
    ( fd: Fd, name: *const libc::c_char, value: *mut libc::c_void, len: u32 ),
    ()
);

generate_io_uring_future!(
    FSetXattr,
    io_uring::opcode::FSetXattr,
    ( fd: Fd, name: *const libc::c_char, value: *mut libc::c_void, len: u32 ),
    ( flags: i32 )
);

generate_io_uring_future!(
    Fadvise,
    io_uring::opcode::Fadvise,
    ( fd: Fd, len: libc::off_t, advice: i32 ),
    ( offset: u64 )
);

generate_io_uring_future!(
    Fallocate,
    io_uring::opcode::Fallocate,
    ( fd: Fd, len: u64 ),
    ( offset: u64 , mode: i32 )
);

generate_io_uring_future!(
    FilesUpdate,
    io_uring::opcode::FilesUpdate,
    ( fds: *const RawFd, len: u32 ),
    ( offset: i32 )
);

generate_io_uring_future!(
    FixedFdInstall,
    io_uring::opcode::FixedFdInstall,
    ( fd: Fixed, file_flags: u32 ),
    ()
);

generate_io_uring_future!(
    Fsync,
    io_uring::opcode::Fsync,
    ( fd: Fd ),
    ( flags: FsyncFlags )
);

generate_io_uring_future!(
    Ftruncate,
    io_uring::opcode::Ftruncate,
    ( fd: Fd, len: u64 ),
    ()
);

generate_io_uring_future!(
    FutexWait,
    io_uring::opcode::FutexWait,
    ( futex: *const u32, val: u64, mask: u64, futex_flags: u32 ),
    ( flags: u32 )
);

generate_io_uring_future!(
    FutexWaitV,
    io_uring::opcode::FutexWaitV,
    ( futexv: *const io_uring::types::FutexWaitV, nr_futex: u32 ),
    ( flags: u32 )
);

generate_io_uring_future!(
    FutexWake,
    io_uring::opcode::FutexWake,
    ( futex: *const u32, val: u64, mask: u64, futex_flags: u32 ),
    ( flags: u32 )
);

generate_io_uring_future!(
    GetXattr,
    io_uring::opcode::GetXattr,
    ( name: *const libc::c_char, value: *mut libc::c_void, path: *const libc::c_char, len: u32 ),
    ()
);

generate_io_uring_future!(
    LinkAt,
    io_uring::opcode::LinkAt,
    ( olddirfd: Fd, oldpath: *const libc::c_char, newdirfd: Fd, newpath: *const libc::c_char ),
    ( flags: i32 )
);

generate_io_uring_future!(
    LinkTimeout,
    io_uring::opcode::LinkTimeout,
    ( timespec: *const Timespec ),
    ( flags: TimeoutFlags )
);

generate_io_uring_future!(
    Listen,
    io_uring::opcode::Listen,
    ( fd: Fd, backlog: i32 ),
    ()
);

generate_io_uring_future!(
    Madvise,
    io_uring::opcode::Madvise,
    ( addr: *const libc::c_void, len: libc::off_t, advice: i32 ),
    ()
);

generate_io_uring_future!(
    MkDirAt,
    io_uring::opcode::MkDirAt,
    ( dirfd: Fd, pathname: *const libc::c_char ),
    ( mode: libc::mode_t )
);

generate_io_uring_future!(
    MsgRingData,
    io_uring::opcode::MsgRingData,
    ( ring_fd: Fd, result: i32, user_data: u64, user_flags: Option<u32> ),
    ( opcode_flags: u32 )
);

generate_io_uring_future!(
    MsgRingSendFd,
    io_uring::opcode::MsgRingSendFd,
    ( ring_fd: Fd, fixed_slot_src: Fixed, dest_slot_index: DestinationSlot, user_data: u64 ),
    ( opcode_flags: u32 )
);

generate_io_uring_future!(Nop, io_uring::opcode::Nop, (), ());

generate_io_uring_future!(
    OpenAt,
    io_uring::opcode::OpenAt,
    ( dirfd: Fd, pathname: *const libc::c_char ),
    ( file_index: Option<DestinationSlot>, flags: i32, mode: libc::mode_t )
);

generate_io_uring_future!(
    OpenAt2,
    io_uring::opcode::OpenAt2,
    ( dirfd: Fd, pathname: *const libc::c_char, how: *const OpenHow ),
    ( file_index: Option<DestinationSlot> )
);

generate_io_uring_future!(
    Pipe,
    io_uring::opcode::Pipe,
    ( fds: *mut RawFd ),
    ( flags: u32, file_index: Option<DestinationSlot> )
);

generate_io_uring_future!(
    PollAdd,
    io_uring::opcode::PollAdd,
    ( fd: Fd, flags: u32 ),
    ( multi: bool )
);

generate_io_uring_future!(
    PollRemove,
    io_uring::opcode::PollRemove,
    ( user_data: u64 ),
    ()
);

generate_io_uring_future!(
    ProvideBuffers,
    io_uring::opcode::ProvideBuffers,
    ( addr: *mut u8, len: i32, nbufs: u16, bgid: u16, bid:u16 ),
    ()
);

generate_io_uring_future!(
    Read,
    io_uring::opcode::Read,
    ( fd: Fd, buf: *mut u8, len: u32 ),
    ( offset: u64, ioprio: u16, rw_flags: i32, buf_group: u16 )
);

generate_io_uring_future!(
    ReadFixed,
    io_uring::opcode::ReadFixed,
    ( fd: Fd, buf: *mut u8, len: u32, buf_index: u16 ),
    ( ioprio: u16, offset: u64, rw_flags: i32 )
);

generate_io_uring_future!(
    Recv,
    io_uring::opcode::Recv,
    ( fd: Fd, buf: *mut u8, len: u32 ),
    ( flags: i32, buf_group: u16 )
);

generate_io_uring_future!(
    RecvBundle,
    io_uring::opcode::RecvBundle,
    ( fd: Fd, buf_group: u16 ),
    ( flags: i32 )
);

generate_io_uring_future!(
    RecvMsg,
    io_uring::opcode::RecvMsg,
    ( fd: Fd, msg: *mut libc::msghdr ),
    ( ioprio: u16, flags: u32, buf_group: u16 )
);

generate_io_uring_future!(
    RecvMsgMulti,
    io_uring::opcode::RecvMsgMulti,
    ( fd: Fd, msg: *mut libc::msghdr, buf_group: u16 ),
    ( ioprio: u16, flags: u32 )
);

generate_io_uring_future!(
    RecvMulti,
    io_uring::opcode::RecvMulti,
    ( fd: Fd, buf_group: u16 ),
    ( flags: i32 )
);

generate_io_uring_future!(
    RecvMultiBundle,
    io_uring::opcode::RecvMultiBundle,
    ( fd: Fd, buf_group: u16 ),
    ( flags: i32 )
);

generate_io_uring_future!(
    RecvZc,
    io_uring::opcode::RecvZc,
    ( fd: Fd, len: u32 ),
    ( ifq: u32, ioprio: u16 )
);

generate_io_uring_future!(
    RemoveBuffers,
    io_uring::opcode::RemoveBuffers,
    ( nbufs: u16, bgid: u16 ),
    ()
);

generate_io_uring_future!(
    RenameAt,
    io_uring::opcode::RenameAt,
    ( olddirfd: Fd, oldpath: *const libc::c_char, newdirfd: Fd, newpath: *const libc::c_char ),
    ( flags: u32 )
);

generate_io_uring_future!(
    Send,
    io_uring::opcode::Send,
    ( fd: Fd, buf: *const u8, len: u32 ),
    ( flags: i32, dest_addr: *const libc::sockaddr, dest_addr_len: libc::socklen_t )
);

generate_io_uring_future!(
    SendBundle,
    io_uring::opcode::SendBundle,
    ( fd: Fd, buf_group: u16 ),
    ( flags: i32, len: u32 )
);

generate_io_uring_future!(
    SendMsg,
    io_uring::opcode::SendMsg,
    ( fd: Fd, msg: *const libc::msghdr ),
    ( ioprio: u16, flags: u32 )
);

generate_io_uring_future!(
    SendMsgZc,
    io_uring::opcode::SendMsgZc,
    ( fd: Fd, msg: *const libc::msghdr ),
    ( ioprio: u16, flags: u32 )
);

generate_io_uring_future!(
    SendZc,
    io_uring::opcode::SendZc,
    ( fd: Fd, buf: *const u8, len: u32 ),
    ( buf_index: Option<u16>, dest_addr: *const libc::sockaddr, dest_addr_len: libc::socklen_t, flags: i32, zc_flags: u16 )
);

generate_io_uring_future!(
    SetSockOpt,
    io_uring::opcode::SetSockOpt,
    ( fd: Fd, level: u32, optname: u32, optval: *const libc::c_void, optlen: u32 ),
    ( flags: u32 )
);

generate_io_uring_future!(
    SetXattr,
    io_uring::opcode::SetXattr,
    ( name: *const libc::c_char, value: *const libc::c_void, path: *const libc::c_char, len: u32 ),
    ( flags: i32 )
);

generate_io_uring_future!(
    Shutdown,
    io_uring::opcode::Shutdown,
    ( fd: Fd, how: i32 ),
    ()
);

generate_io_uring_future!(
    Socket,
    io_uring::opcode::Socket,
    ( domain: i32, socket_type: i32, protocol: i32 ),
    ( file_index: Option<DestinationSlot>, flags: i32 )
);

generate_io_uring_future!(
    Splice,
    io_uring::opcode::Splice,
    ( fd_in: Fd, off_in: i64, fd_out: Fd, off_out: i64, len: u32 ),
    ( flags: u32 )
);

generate_io_uring_future!(
    Statx,
    io_uring::opcode::Statx,
    ( dirfd: Fd, pathname: *const libc::c_char, statxbuf: *mut statx ),
    ( flags: i32, mask: u32 )
);

generate_io_uring_future!(
    SymlinkAt,
    io_uring::opcode::SymlinkAt,
    ( newdirfd: Fd, target: *const libc::c_char, linkpath: *const libc::c_char ),
    ()
);

generate_io_uring_future!(
    SyncFileRange,
    io_uring::opcode::SyncFileRange,
    ( fd: Fd, len: u32 ),
    ( offset: u64, flags: u32 )
);

generate_io_uring_future!(
    Tee,
    io_uring::opcode::Tee,
    ( fd_in: Fd, fd_out: Fd, len: u32 ),
    ( flags: u32 )
);

generate_io_uring_future!(
    Timeout,
    io_uring::opcode::Timeout,
    ( timespec: *const Timespec ),
    ( count: u32, flags: TimeoutFlags )
);

generate_io_uring_future!(
    TimeoutRemove,
    io_uring::opcode::TimeoutRemove,
    ( user_data: u64 ),
    ()
);

generate_io_uring_future!(
    TimeoutUpdate,
    io_uring::opcode::TimeoutUpdate,
    ( user_data: u64, timespec: *const Timespec ),
    ( flags: TimeoutFlags )
);

generate_io_uring_future!(
    UnlinkAt,
    io_uring::opcode::UnlinkAt,
    ( dirfd: Fd, pathname: *const libc::c_char ),
    ( flags: i32 )
);

generate_io_uring_future!(
    UringCmd16,
    io_uring::opcode::UringCmd16,
    ( fd: Fd, cmd_op: u32 ),
    ( buf_index: Option<u16>, cmd: [u8; 16] )
);

// TODO: would need a type-erased RawSqe to support Entry128 because we store
// the RawSqe in the Slab collection.
//generate_io_uring_future!(
//    UringCmd80,
//    io_uring::opcode::UringCmd80,
//    ( fd: Fd, cmd_op: u32 ),
//    ( buf_index: Option<u16>, cmd: [u8; 80] )
//);

generate_io_uring_future!(
    WaitId,
    io_uring::opcode::WaitId,
    ( idtype: libc::idtype_t, id: libc::id_t, options: libc::c_int ),
    ( infop: *const libc::siginfo_t, flags: libc::c_uint )
);

generate_io_uring_future!(
    Write,
    io_uring::opcode::Write,
    ( fd: Fd, buf: *const u8, len: u32 ),
    ( offset: u64, ioprio: u16, rw_flags: i32 )
);

generate_io_uring_future!(
    WriteFixed,
    io_uring::opcode::WriteFixed,
    ( fd: Fd, buf: *const u8, len: u32, buf_index: u16 ),
    ( ioprio: u16, offset: u64, rw_flags: i32 )
);

generate_io_uring_future!(
    Writev,
    io_uring::opcode::Writev,
    ( fd: Fd, iovec: *const libc::iovec, len: u32 ),
    ( ioprio: u16, offset: u64, rw_flags: i32 )
);

generate_io_uring_future!(
    WritevFixed,
    io_uring::opcode::WritevFixed,
    ( fd: Fd, iovec: *const libc::iovec, len: u32, buf_index: u16 ),
    ( ioprio: u16, offset: u64, rw_flags: i32 )
);
