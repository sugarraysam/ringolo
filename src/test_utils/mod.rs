use anyhow::Result;
use io_uring::opcode::{Nop, OpenAt};
use io_uring::squeue::Entry;
use io_uring::types::Fd;
use std::ffi::CString;

use crate::sqe::{SqeBatchBuilder, SqeChainBuilder, SqeList, SqeListKind};

pub mod mocks;
pub use mocks::mock_waker;

pub fn build_list_with_entries(kind: SqeListKind, entries: Vec<Entry>) -> Result<SqeList> {
    let sqe_list = match kind {
        SqeListKind::Batch => build_batch_with_entries(entries),
        SqeListKind::Chain => build_chain_with_entries(entries),
    }?;
    Ok(sqe_list)
}

pub fn build_batch(size: usize) -> Result<SqeList> {
    let mut builder = SqeBatchBuilder::new();
    for _ in 0..size {
        builder.add_entry(nop(), None);
    }
    builder.try_build()
}

pub fn build_batch_with_entries(entries: Vec<Entry>) -> Result<SqeList> {
    let mut builder = SqeBatchBuilder::new();
    for entry in entries {
        builder.add_entry(entry, None);
    }
    builder.try_build()
}

pub fn build_chain(size: usize) -> Result<SqeList> {
    let mut builder = SqeChainBuilder::new();
    for _ in 0..size {
        builder.add_entry(nop(), None);
    }
    builder.try_build()
}

pub fn build_chain_with_entries(entries: Vec<Entry>) -> Result<SqeList> {
    let mut builder = SqeChainBuilder::new();
    for entry in entries {
        builder.add_entry(entry, None);
    }
    builder.try_build()
}

pub fn openat(fd: i32, path: &str) -> Entry {
    // Must ensure string is null terminated
    let c_path = CString::new(path).expect("can't convert to CString");
    OpenAt::new(Fd(fd), c_path.as_ptr()).build()
}

pub fn nop() -> Entry {
    Nop::new().build()
}
