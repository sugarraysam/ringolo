use crate::sqe::{SqeBatchBuilder, SqeChainBuilder, SqeList, SqeListBuilder, SqeListKind};
use crate::task::TaskNode;
use anyhow::{Context, Result};
use io_uring::opcode::{Fsync, Nop, OpenAt, Read, Write};
use io_uring::squeue::Entry;
use io_uring::types::Fd;
use std::ffi::CString;
use std::fs::File;
use std::os::fd::AsRawFd;
use std::sync::Arc;

// Exports
pub(crate) mod context;
pub(crate) use context::init_local_runtime_and_context;

pub(crate) mod future;
pub(crate) use future::*;

pub(crate) mod mocks;
pub(crate) use mocks::{DummyScheduler, mock_waker};

pub(crate) fn compute_task_node_tree_size(root: Arc<TaskNode>) -> Result<usize> {
    let mut total = 0;
    let mut to_visit = vec![root];

    while !to_visit.is_empty() {
        let curr = to_visit.pop().context("nothing to visit")?;
        total += 1;
        to_visit.extend(curr.clone_children());
    }

    Ok(total)
}

// Make sure to keep `data` and `File` alive until SQ has been submitted.
// We write-fsync-read and skip open+close because the tempfile library is
// already doing this work, so we want to avoid conflicts.
#[must_use]
pub(crate) fn build_chain_write_fsync_read_a_tempfile() -> Result<(SqeList, Vec<u8>, Vec<u8>, File)>
{
    let tmp = tempfile::tempfile()?;
    let fd = Fd(tmp.as_raw_fd());
    let data_out = b"I love me some ringolos!\n".to_vec();
    let mut data_in = Vec::with_capacity(data_out.len());

    let chain = SqeChainBuilder::new()
        .add_entry(
            Write::new(fd, data_out.as_ptr(), data_out.len() as u32).build(),
            None,
        )
        .add_entry(Fsync::new(fd).build(), None)
        .add_entry(
            Read::new(fd, data_in.as_mut_ptr(), data_out.len() as u32).build(),
            None,
        )
        .build();

    Ok((chain, data_out, data_in, tmp))
}

pub(crate) fn build_list_with_entries(kind: SqeListKind, entries: Vec<Entry>) -> SqeList {
    match kind {
        SqeListKind::Batch => build_batch_with_entries(entries),
        SqeListKind::Chain => build_chain_with_entries(entries),
    }
}

pub(crate) fn build_batch(size: usize) -> SqeList {
    (0..size)
        .fold(SqeBatchBuilder::new(), |batch, _| {
            batch.add_entry(nop(), None)
        })
        .build()
}

pub(crate) fn build_batch_with_entries(entries: Vec<Entry>) -> SqeList {
    entries
        .into_iter()
        .fold(SqeBatchBuilder::new(), |batch, entry| {
            batch.add_entry(entry, None)
        })
        .build()
}

pub(crate) fn build_chain(size: usize) -> SqeList {
    (0..size)
        .fold(SqeChainBuilder::new(), |chain, _| {
            chain.add_entry(nop(), None)
        })
        .build()
}

pub(crate) fn build_chain_with_entries(entries: Vec<Entry>) -> SqeList {
    entries
        .into_iter()
        .fold(SqeChainBuilder::new(), |chain, entry| {
            chain.add_entry(entry, None)
        })
        .build()
}

pub(crate) fn openat(fd: i32, path: &str) -> Entry {
    // Must ensure string is null terminated
    let c_path = CString::new(path).expect("can't convert to CString");
    OpenAt::new(Fd(fd), c_path.as_ptr()).build()
}

pub(crate) fn nop() -> Entry {
    Nop::new().build()
}
