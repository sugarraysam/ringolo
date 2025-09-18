use std::os::unix::io::RawFd;

// Create my own types, independent of `io_uring::opcode` because want to
// abstract away and maybe be able to move away. Will also pass less data on
// channel.
// Write the enum itself on channel, then do pattern matching on other side and
// explode to `io_uring::opcode` and then SQE with `<opcode>.build()`

// Model supported IO operations based on liburing as source-of-truth: https://github.com/axboe/liburing/blob/master/src/include/liburing.h
// Find `io_uring_prep_**`
#[derive(Debug, Clone)]
pub enum IoBase {
    Nop,
    Read {
        fd: RawFd,
        buf: *mut u8,
        nbytes: u32,
        offset: u64,
    },
    Write {
        fd: RawFd,
        buf: *mut u8,
        nbytes: u32,
        offset: u64,
    },
}
