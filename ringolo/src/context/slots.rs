use crate::context::Core;
use crate::task::ThreadId;
use anyhow::{Context, Result, anyhow};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use std::collections::{HashMap, VecDeque};
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

#[derive(Debug, Clone)]
pub(crate) struct WorkerData {
    pub(crate) should_unpark: Arc<AtomicBool>,

    pub(crate) ring_fd: RawFd,
}

impl WorkerData {
    fn from_core(core: &Core) -> Self {
        Self {
            should_unpark: Arc::new(AtomicBool::new(false)),
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
pub(crate) struct WorkerSlots {
    slots: RwLock<Vec<Option<WorkerData>>>,

    /// Maps a `ThreadId` to a slot
    mapping: RwLock<SlotsMapping>,
}

impl WorkerSlots {
    pub(crate) fn new(num_workers: usize) -> Self {
        Self {
            slots: RwLock::new(vec![None; num_workers]),
            mapping: RwLock::new(SlotsMapping::new(num_workers)),
        }
    }

    #[track_caller]
    pub(crate) fn with_data<F, R>(&self, thread_id: &ThreadId, f: F) -> R
    where
        F: FnOnce(&WorkerData) -> R,
    {
        let slot = self.get(thread_id).expect("slot uninitialized");
        f(&slot)
    }

    fn get(&self, thread_id: &ThreadId) -> Result<MappedRwLockReadGuard<'_, WorkerData>> {
        let slot_idx = self.mapping.read().get_slot(thread_id)?;

        RwLockReadGuard::try_map(self.slots.read(), |slots| slots[slot_idx].as_ref())
            .map_err(|_| anyhow!("ThreadId {:?} not found", thread_id))
    }

    pub(crate) fn register(&self, core: &Core) -> Result<()> {
        let slot_idx = self.mapping.write().reserve_slot(core.thread_id)?;

        let worker_data = WorkerData::from_core(core);
        self.slots.write()[slot_idx] = Some(worker_data);

        Ok(())
    }

    pub(crate) fn unregister(&self, thread_id: &ThreadId) -> Result<()> {
        let slot_idx = self.mapping.write().free_slot(thread_id)?;
        self.slots.write()[slot_idx] = None;

        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.slots.read().len()
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
