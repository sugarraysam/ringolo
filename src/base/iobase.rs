use io_uring::squeue::Entry;
use io_uring::{opcode, types};
use std::os::unix::io::RawFd;

// Create my own types, independent of `io_uring::opcode` because want to
// abstract away and maybe be able to move away. Will also pass less data on
// channel.
// Write the enum itself on channel, then do pattern matching on other side and
// explode to `io_uring::opcode` and then SQE with `<opcode>.build()`

// Model supported IO operations based on liburing as source-of-truth: https://github.com/axboe/liburing/blob/master/src/include/liburing.h
// Find `io_uring_prep_**`
pub enum IoBase {
    // IoUring supports Nop for benchmark/testing
    Nop,

    // fd, buf, nbytes, offset
    Read(RawFd, *mut u8, u32, u64),

    // fd, buf, nbytes, offset
    Write(RawFd, *mut u8, u32, u64),
}

pub struct IoWrapper {
    io: IoBase,
}

impl IoWrapper {
    fn new() -> Self {
        Self { io: IoBase::Nop }
    }

    fn prep_sqe(&self) -> Entry {
        match self.io {
            IoBase::Nop => opcode::Nop::new().build(),

            IoBase::Read(fd, buf, nbytes, offset) => opcode::Read::new(types::Fd(fd), buf, nbytes)
                .offset(offset)
                .build(),

            IoBase::Write(fd, buf, nbytes, offset) => {
                opcode::Write::new(types::Fd(fd), buf, nbytes)
                    .offset(offset)
                    .build()
            }
        }
    }
}
