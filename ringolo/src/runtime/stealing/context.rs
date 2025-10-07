use crate::context::{Core, RawSqeSlab, SingleIssuerRing, ThreadId};
// use crate::protocol::message::{MAX_MSG_COUNTER_VALUE, MSG_COUNTER_BITS, MsgId};
use crate::runtime::RuntimeConfig;
use crate::runtime::stealing;
use anyhow::{Result, anyhow};
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::RwLock;

pub(crate) struct Context {
    pub(crate) scheduler: stealing::Handle,

    pub(crate) worker: &'static stealing::Worker,

    pub(crate) core: RefCell<Core>,

    pub(crate) shared: Arc<Shared>,

    /// TODO: implement RingMessage protocol so runtime threads can communicate
    /// with each other. Most interesting use case is ringolo-console CLI to
    /// send various commands from a TUI/GUI.
    #[allow(unused)]
    pub(crate) ring_msg_counter: u32,
}

impl Context {
    pub(crate) fn try_new(
        cfg: &RuntimeConfig,
        scheduler: stealing::Handle,
        worker: &'static stealing::Worker,
        shared: Arc<Shared>,
    ) -> Result<Self> {
        let core = Core::try_new(cfg)?;

        shared.register_worker(&core)?;

        Ok(Self {
            scheduler,
            worker,
            core: RefCell::new(core),
            shared,
            ring_msg_counter: 0,
        })
    }

    pub(crate) fn get_worker(&self) -> &stealing::Worker {
        self.worker
    }

    pub(crate) fn with_core<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Core) -> R,
    {
        f(&self.core.borrow())
    }

    pub(crate) fn with_core_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Core) -> R,
    {
        f(&mut self.core.borrow_mut())
    }

    pub(crate) fn with_slab<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&RawSqeSlab) -> R,
    {
        let core = self.core.borrow();
        let slab = core.slab.borrow();
        f(&slab)
    }

    pub(crate) fn with_slab_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut RawSqeSlab) -> R,
    {
        let core = self.core.borrow();
        let mut slab = core.slab.borrow_mut();
        f(&mut slab)
    }

    pub(crate) fn with_ring<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&SingleIssuerRing) -> R,
    {
        let core = self.core.borrow();
        let slab = core.ring.borrow();
        f(&slab)
    }

    pub(crate) fn with_ring_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut SingleIssuerRing) -> R,
    {
        let core = self.core.borrow();
        let mut slab = core.ring.borrow_mut();
        f(&mut slab)
    }

    // TODO: impl with ringolo-console
    // Each thread is responsible for generating MsgId to be used in the
    // RingMessage protocol. The reasoning is we want to avoid collisions when
    // storing MsgIds in the Mailbox. We achieve this goal by including the
    // unique `thread_id` in the upper bits of the MsgId.
    // #[allow(dead_code)]
    // pub(crate) fn next_ring_msg_id(&mut self) -> MsgId {
    //     let prev_msg_counter = self.ring_msg_counter as i32;

    //     // We have 10 bits for ring_msg_counter, so we need to wrap around and
    //     // re-use previous ids when we reach the maximum.
    //     self.ring_msg_counter += 1;
    //     if self.ring_msg_counter >= MAX_MSG_COUNTER_VALUE {
    //         self.ring_msg_counter = 0;
    //     }

    //     let thread_id = (self.core.borrow().thread_id.as_u64() as i32) << MSG_COUNTER_BITS;

    //     MsgId::from(thread_id | prev_msg_counter)
    // }
}

impl Drop for Context {
    fn drop(&mut self) {
        if let Err(e) = self.shared.unregister_worker(&self.core.borrow()) {
            eprintln!(
                "Failed to unregister worker {:?}: {:?}",
                self.core.borrow().thread_id,
                e
            );
        }
    }
}

/// Context shared amongst all stealing workers and stored in thread_local storage.
/// Needs to be sync and thread safe.
#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) thread_id_to_ring_fd: RwLock<HashMap<ThreadId, RawFd>>,
}

impl Shared {
    pub(super) fn new() -> Self {
        Self {
            thread_id_to_ring_fd: RwLock::new(HashMap::new()),
        }
    }

    pub(super) fn register_worker(&self, core: &Core) -> Result<()> {
        let mut map = self
            .thread_id_to_ring_fd
            .write()
            .expect("Poisoned ring_fd map on write");

        match map.entry(core.thread_id) {
            Entry::Occupied(entry) => {
                Err(anyhow!("Thread ID {:?} is already registered", entry.key()))
            }
            Entry::Vacant(entry) => {
                entry.insert(core.ring_fd);
                Ok(())
            }
        }
    }

    pub(super) fn unregister_worker(&self, core: &Core) -> Result<()> {
        let mut map = self
            .thread_id_to_ring_fd
            .write()
            .expect("Poisoned ring_fd map on write");

        map.remove(&core.thread_id)
            .map(|_| {})
            .ok_or_else(|| anyhow!("Thread ID {:?} not found", core.thread_id))
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

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::assert_impl_all;

    assert_impl_all!(Shared: Send, Sync);
}
