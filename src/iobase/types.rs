use anyhow::{Result, anyhow};
use crossbeam_queue::ArrayQueue;
use io_uring::squeue::Entry;
use io_uring::{opcode, types};
use std::os::unix::io::RawFd;
use std::sync::Arc;
use tokio::sync::watch;

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
pub struct IoWrapper {
    entry: Option<Entry>,
    tx: watch::Sender<i32>,

    // Store the address on the heap for this IoWrapper. Will be able to
    // reconstruct and deallocate at the end of the program.
    ptr_address: u64,
}

const DEFAULT_PTR_ADDRESS: u64 = 0123456789;

impl IoWrapper {
    pub fn new() -> *mut Self {
        let entry = None;
        let tx = watch::Sender::new(0);

        let io_wrapper = Box::into_raw(Box::new(Self {
            entry,
            ptr_address: DEFAULT_PTR_ADDRESS,
            tx,
        }));

        // Store location on heap where this IoWrapper is allocated. Will be valid
        // for the duration of the program.
        unsafe {
            (*io_wrapper).ptr_address = io_wrapper as u64;
        }

        io_wrapper
    }

    pub fn drop_guard(&self) -> IoWrapperGuard {
        IoWrapperGuard {
            ptr: self.ptr_address as *mut IoWrapper,
        }
    }

    pub fn reset(&mut self, io: IoBase) {
        self.entry = Some(io.into_entry().user_data(self.ptr_address));
    }

    pub fn take_entry(&mut self) -> Option<Entry> {
        self.entry.take()
    }

    pub fn subscribe(&self) -> watch::Receiver<i32> {
        self.tx.subscribe()
    }

    pub fn send_result(&self, result: i32) -> Result<()> {
        Ok(self.tx.send(result)?)
    }
}

// Guard around IoWrapper to ensure we drop the raw ptr on scope exit.
#[derive(Debug)]
struct IoWrapperGuard {
    ptr: *mut IoWrapper,
}

impl Drop for IoWrapperGuard {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                Box::from_raw(self.ptr);
            }
        }
    }
}

// MPMC pool of IoWrapper used to transport IO operations in the SQ ring and
// out of the CQ ring. Clients will consume (i.e.: pop) IoWrapper and Workers
// will produce (i.e.: push) the IoWrapper after they've gone through the SQ+CQ
// ring.
#[derive(Debug)]
pub struct IoWrapperPool {
    pool: ArrayQueue<*mut IoWrapper>,
    capacity: usize,

    // We keep a hold of the IoWrapper guards so that we can drop every IoWrapper
    // when the pool goes out of scope and avoid leaking memory.
    guards: Vec<IoWrapperGuard>,
}

impl IoWrapperPool {
    pub fn new(capacity: usize) -> Arc<Self> {
        let pool = ArrayQueue::new(capacity);
        let mut guards = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            let io_wrapper = IoWrapper::new();
            unsafe {
                guards.push((*io_wrapper).drop_guard());
            }
            pool.push(io_wrapper);
        }

        Arc::new(Self {
            pool,
            capacity,
            guards,
        })
    }

    pub fn push(&self, item: *mut IoWrapper) -> Result<()> {
        self.pool
            .push(item)
            .map_err(|e| anyhow!("Failed to push IoWrapper: {:?}", e))
    }

    pub fn pop(&self) -> Result<*mut IoWrapper> {
        self.pool.pop().ok_or(anyhow!("Empty IoWrapper pool."))
    }

    pub fn len(&self) -> usize {
        self.pool.len()
    }

    pub const fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use crate::iobase::types::{DEFAULT_PTR_ADDRESS, IoWrapper, IoWrapperPool};
    use anyhow::Result;

    #[test]
    fn test_io_wrapper_lifecycle() {
        let io_wrapper = IoWrapper::new();
        assert!(!io_wrapper.is_null());

        unsafe {
            let _guard = (*io_wrapper).drop_guard();

            // should have been overriden
            assert_ne!((*io_wrapper).ptr_address, DEFAULT_PTR_ADDRESS);
            assert!((*io_wrapper).entry.is_none());
        }
    }

    #[tokio::test]
    async fn test_io_wrapper_sender() -> Result<()> {
        let io_wrapper = IoWrapper::new();
        assert!(!io_wrapper.is_null());

        let expected: i32 = 42;

        unsafe {
            let _guard = (*io_wrapper).drop_guard();

            let mut rx = (*io_wrapper).subscribe();
            (*io_wrapper).send_result(expected)?;

            assert!(rx.changed().await.is_ok());
            assert_eq!(*rx.borrow_and_update(), expected);
        }

        Ok(())
    }

    #[test]
    fn test_io_wrapper_pool_no_memory_leaks() -> Result<()> {
        let n = 10;
        let pool = IoWrapperPool::new(n);

        // Even if we pop all io_wrappers, we should not have memory leaks
        let mut leaked: Vec<*mut IoWrapper> = Vec::with_capacity(n);
        for _ in 0..n {
            leaked.push(pool.pop()?);
        }

        assert_eq!(pool.len(), 0);
        drop(pool);

        // Validate all leaked io_wrappers have been deallocated
        leaked.into_iter().for_each(|io_wrapper| {
            assert!(io_wrapper.is_null());
        });

        Ok(())
    }
}
