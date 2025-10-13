#![allow(clippy::all)]

// Required imports for the macro and the generated code.
use crate::sqe::SqeSingle;
use io_uring::types::{CancelBuilder, DestinationSlot, Fd, Fixed, FsyncFlags};
use std::future::Future;

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
macro_rules! generate_io_uring_future {
    (
        $name:ident,
        $opcode:path,
        ( $( $req_arg:ident: $req_ty:ty ),* ),
        ( $( $opt_arg:ident: $opt_ty:ty ),* )
    ) => { paste::paste! {

        pub struct $name {
            sqe: $crate::sqe::Sqe<$crate::sqe::SqeSingle>,
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
            pub fn build(self) -> $name {
                $name {
                    sqe: $crate::sqe::Sqe::new(SqeSingle::new(self.op.build())),
                }
            }
        }

        // The `Future` implementation is consistent across all generated structs.
        impl Future for $name {
            type Output = Result<i32, $crate::sqe::IoError>;

            fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                let mut pinned = std::pin::pin!(&mut self.sqe);
                pinned.as_mut().poll(cx)
            }
        }
    }
}
}

generate_io_uring_future!(
    AsyncCancel,
    io_uring::opcode::AsyncCancel,
    ( user_data: u64 ),
    ( )
);

generate_io_uring_future!(
    AsyncCancel2,
    io_uring::opcode::AsyncCancel2,
    ( builder: CancelBuilder ),
    ( )
);

generate_io_uring_future!(
    Close,
    io_uring::opcode::Close,
    ( fd: Fd ),
    ()
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
    Listen,
    io_uring::opcode::Listen,
    ( fd: Fd, backlog: i32 ),
    ()
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
    RecvBundle,
    io_uring::opcode::RecvBundle,
    ( fd: Fd, buf_group: u16 ),
    ( flags: i32 )
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
    SendBundle,
    io_uring::opcode::SendBundle,
    ( fd: Fd, buf_group: u16 ),
    ( flags: i32, len: u32 )
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
    TimeoutRemove,
    io_uring::opcode::TimeoutRemove,
    ( user_data: u64 ),
    ()
);

generate_io_uring_future!(
    UringCmd16,
    io_uring::opcode::UringCmd16,
    ( fd: Fd, cmd_op: u32 ),
    ( buf_index: Option<u16>, cmd: [u8; 16] )
);
