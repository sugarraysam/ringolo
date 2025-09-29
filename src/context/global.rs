use crate::context::ThreadId;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{OnceLock, RwLock};

pub struct GlobalContext {
    thread_id_counter: AtomicU8,

    thread_id_to_ring_fd: RwLock<HashMap<ThreadId, RawFd>>,
}

impl GlobalContext {
    // Private as this is a singleton
    fn new() -> Self {
        Self {
            // Start at 1 so we can detect overflow, this means we are accepting
            // that we will only be able to register 255 threads
            thread_id_counter: AtomicU8::new(1),

            thread_id_to_ring_fd: RwLock::new(HashMap::new()),
        }
    }

    // Singleton public access point, protected with OnceLock.
    pub fn instance() -> &'static GlobalContext {
        static INSTANCE: OnceLock<GlobalContext> = OnceLock::new();

        INSTANCE.get_or_init(|| GlobalContext::new())
    }

    fn next_thread_id(&self) -> Result<ThreadId> {
        let thread_id = self.thread_id_counter.fetch_add(1, Ordering::Relaxed);
        if thread_id == 0 {
            Err(anyhow!(
                "Registered maximum amount of threads: {:?}",
                u8::MAX - 1
            ))
        } else {
            Ok(thread_id)
        }
    }

    pub fn register_ring_fd(&self, ring_fd: RawFd) -> Result<ThreadId> {
        let thread_id = self.next_thread_id()?;

        let mut map = self.thread_id_to_ring_fd.write().unwrap();

        if let Entry::Vacant(e) = map.entry(thread_id) {
            // Safely wraps around on overflow - use to detect if we registered
            // too many threads.

            e.insert(ring_fd);
            Ok(thread_id)
        } else {
            // Should never happen as we use fetch_add.
            Err(anyhow!("ThreadId is already registered: {:?}", thread_id))
        }
    }

    pub fn get_ring_fd(&self, thread_id: ThreadId) -> Result<RawFd> {
        self.thread_id_to_ring_fd
            .read()
            .unwrap()
            .get(&thread_id)
            .ok_or(anyhow!("Invalid thread_id"))
            .copied()
    }
}
