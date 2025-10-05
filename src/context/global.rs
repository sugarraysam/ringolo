use crate::context::ThreadId;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::os::fd::RawFd;
use std::sync::{Mutex, OnceLock, RwLock};

pub(crate) struct GlobalContext {
    thread_id_manager: ThreadIdManager,

    thread_id_to_ring_fd: RwLock<HashMap<ThreadId, RawFd>>,
}

impl GlobalContext {
    // Private as this is a singleton
    fn new() -> Self {
        Self {
            thread_id_manager: ThreadIdManager::new(),

            thread_id_to_ring_fd: RwLock::new(HashMap::new()),
        }
    }

    // Singleton public access point, protected with OnceLock.
    pub(crate) fn instance() -> &'static GlobalContext {
        static INSTANCE: OnceLock<GlobalContext> = OnceLock::new();

        INSTANCE.get_or_init(GlobalContext::new)
    }

    pub(crate) fn release_thread_id_and_ring_fd(&self, thread_id: ThreadId) -> Result<()> {
        // Release ring_fd first to avoid race condition on a new thread being
        // spun up and re-using the same thread id to register it's ring_fd.
        let ring_fd_removed = {
            let mut map = self
                .thread_id_to_ring_fd
                .write()
                .expect("Poisoned ring_fd map on write");

            map.remove(&thread_id).is_some()
        };

        // Make sure we release thread_id in all paths
        self.thread_id_manager
            .release_id(thread_id)
            .map_err(move |e| {
                anyhow!(
                    "Failed to release thread_id: {:?}. Releasing ring_fd status: {:?}",
                    e,
                    ring_fd_removed
                )
            })
    }

    pub(crate) fn register_ring_fd(&self, ring_fd: RawFd) -> Result<ThreadId> {
        let thread_id = self.thread_id_manager.acquire_id()?;

        let mut map = self
            .thread_id_to_ring_fd
            .write()
            .expect("Poisoned ring_fd map on write");

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

    pub(crate) fn get_ring_fd(&self, thread_id: ThreadId) -> Result<RawFd> {
        self.thread_id_to_ring_fd
            .read()
            .expect("Poisoned ring_fd map on read")
            .get(&thread_id)
            .ok_or(anyhow!("Invalid thread_id"))
            .copied()
    }
}

// A bitset to manage 256 IDs (0 to 255). We use 4 x u64 to cover 4 * 64 = 256 bits.
// - Bit 'N' being SET (1) means ID 'N' is AVAILABLE.
// - Bit 'N' being UNSET (0) means ID 'N' is IN USE.
//
// This implementation allows returning ThreadId's which is important to implement
// autoscaling of workers. We leverage a bitset as it is very fast and memory
// efficient.
struct ThreadIdManager {
    id_map: Mutex<[u64; 4]>,
}

impl ThreadIdManager {
    fn new() -> Self {
        ThreadIdManager {
            // 256 bits/threads available
            id_map: Mutex::new([u64::MAX; 4]),
        }
    }

    /// Acquires the lowest available ThreadId.
    fn acquire_id(&self) -> Result<ThreadId> {
        let mut map = self.id_map.lock().expect("Lock poisoned");

        for (i, word) in map.iter_mut().enumerate() {
            if *word != 0 {
                let bit_idx = word.trailing_zeros() as u8;

                let block_offset = (i * 64) as u8;
                let thread_id = block_offset + bit_idx;

                // Clear the bit to indicate "in use"
                *word &= !(1u64 << bit_idx);

                return Ok(thread_id);
            }
        }

        Err(anyhow!("ThreadIds have been exhausted."))
    }

    /// Releases a ThreadId, making it available for reuse.
    /// Panics if the ID is out of bounds or was already marked as free.
    fn release_id(&self, id: ThreadId) -> Result<()> {
        let mut map = self.id_map.lock().expect("Lock poisoned");

        let block_offset = (id / 64) as usize;
        let bit_idx = (id % 64) as u32;

        let mask = 1u64 << bit_idx;

        if (map[block_offset] & mask) != 0 {
            Err(anyhow!(
                "Attempted to release ThreadId {:?} which was already free.",
                id
            ))
        } else {
            map[block_offset] |= mask;
            Ok(())
        }
    }

    fn is_available(&self, id: ThreadId) -> bool {
        let block_offset = (id / 64) as usize;
        let bit_idx = (id % 64) as u32;
        let mask = 1u64 << bit_idx;

        let map = self.id_map.lock().expect("Lock poisoned");
        map[block_offset] & mask != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_thread_id_manager() -> Result<()> {
        let n = 50;

        let manager = ThreadIdManager::new();
        let mut ids = HashSet::with_capacity(n);

        for _ in 0..n {
            let id = manager.acquire_id()?;
            assert!(ids.insert(id));
            assert!(!manager.is_available(id));
        }

        // Ensure N unique ids were generated
        assert_eq!(ids.len(), n);

        for id in ids {
            manager.release_id(id)?;
            assert!(manager.is_available(id));
        }

        Ok(())
    }
}
