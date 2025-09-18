use io_uring::squeue::Entry;
use io_uring::{opcode, types};
use std::os::unix::io::RawFd;

// Model supported IO operations based on liburing as source-of-truth:
// https://github.com/axboe/liburing/blob/master/src/include/liburing.h
#[derive(Debug, Clone, Default)]
pub enum IoBase {
    #[default]
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

impl IoBase {
    pub fn into_entry(self) -> Entry {
        match self {
            IoBase::Nop => opcode::Nop::new().build(),

            IoBase::Read {
                fd,
                buf,
                nbytes,
                offset,
            } => opcode::Read::new(types::Fd(fd), buf, nbytes)
                .offset(offset)
                .build(),

            IoBase::Write {
                fd,
                buf,
                nbytes,
                offset,
            } => opcode::Write::new(types::Fd(fd), buf, nbytes)
                .offset(offset)
                .build(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqeWrapper {
    entry: Entry,
}

impl SqeWrapper {
    pub fn reset(&mut self, io: IoBase) {
        self.entry = io.into_entry();
    }
}

impl Default for SqeWrapper {
    fn default() -> Self {
        let entry = IoBase::default().into_entry();

        Self { entry }
    }
}
