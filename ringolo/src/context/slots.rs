use crate::context::Core;
use anyhow::{Context, Result, anyhow};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use std::collections::{HashMap, VecDeque};
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::thread::ThreadId;

#[derive(Debug, Clone)]
pub(super) struct WorkerData {
    pub(super) pending_ios: Arc<AtomicUsize>,
    pub(super) ring_fd: RawFd,
}

impl WorkerData {
    fn from_core(core: &Core) -> Self {
        Self {
            pending_ios: Arc::new(AtomicUsize::new(0)),
            ring_fd: core.ring_fd,
        }
    }
}

/// Struct to manage and register workers in the shared context. We map the space
/// of ThreadId which is monotonically increasing, to available slots, which is
/// confined from `(0..num_workers)`. All accesses on the `slots` vector can be
/// done with a read lock and will be uncontended most of the time.
///
/// The only time we can experience contention is when autoscaling workers during
/// program execution. This should be quite rare so let's not bother optimizing that
/// case.
#[derive(Debug)]
pub(super) struct WorkerSlots {
    slots: RwLock<Vec<Option<WorkerData>>>,

    /// Maps a `ThreadId` to a slot
    mapping: RwLock<SlotsMapping>,
}

impl WorkerSlots {
    pub(super) fn new(num_workers: usize) -> Self {
        Self {
            slots: RwLock::new(vec![None; num_workers]),
            mapping: RwLock::new(SlotsMapping::new(num_workers)),
        }
    }

    pub(super) fn with_data<F, R>(&self, thread_id: &ThreadId, f: F) -> Result<R>
    where
        F: FnOnce(&WorkerData) -> R,
    {
        let slot = self.get(thread_id)?;
        Ok(f(&slot))
    }

    fn get(&self, thread_id: &ThreadId) -> Result<MappedRwLockReadGuard<'_, WorkerData>> {
        let slot_idx = self.mapping.read().get_slot(thread_id)?;

        RwLockReadGuard::try_map(self.slots.read(), |slots| slots[slot_idx].as_ref())
            .map_err(|_| anyhow!("ThreadId {:?} not found", thread_id))
    }

    pub(super) fn register(&self, core: &Core) -> Result<WorkerData> {
        let slot_idx = self.mapping.write().reserve_slot(core.thread_id)?;

        let worker_data = WorkerData::from_core(core);
        let worker_data_clone = worker_data.clone();

        self.slots.write()[slot_idx] = Some(worker_data);
        Ok(worker_data_clone)
    }

    pub(super) fn unregister(&self, core: &Core) -> Result<()> {
        let slot_idx = self.mapping.write().free_slot(&core.thread_id)?;
        self.slots.write()[slot_idx] = None;

        Ok(())
    }
}

#[derive(Debug)]
struct SlotsMapping {
    /// Maps a `ThreadId` to its allocated `usize` index in `slots`.
    mapping: HashMap<ThreadId, usize>,

    /// A pool of available slot indices (`0..N-1`).
    free_slots: VecDeque<usize>,
}

impl SlotsMapping {
    fn new(num_workers: usize) -> Self {
        Self {
            mapping: HashMap::with_capacity(num_workers),
            free_slots: (0..num_workers).collect(),
        }
    }

    fn get_slot(&self, thread_id: &ThreadId) -> Result<usize> {
        self.mapping
            .get(thread_id)
            .copied()
            .with_context(|| format!("ThreadId {:?} not found", &thread_id))
    }

    fn reserve_slot(&mut self, thread_id: ThreadId) -> Result<usize> {
        let slot = self
            .free_slots
            .pop_back()
            .context("No free slots available")?;

        match self.mapping.insert(thread_id, slot) {
            Some(_) => Err(anyhow!("ThreadId {:?} already registered", thread_id)),
            None => Ok(slot),
        }
    }

    fn free_slot(&mut self, thread_id: &ThreadId) -> Result<usize> {
        let slot = self
            .mapping
            .remove(thread_id)
            .with_context(|| format!("ThreadId {:?} not found", thread_id))?;

        self.free_slots.push_back(slot);

        Ok(slot)
    }
}
