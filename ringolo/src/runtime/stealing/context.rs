use crate::context::{Core, RawSqeSlab, Shared, SingleIssuerRing};
use crate::runtime::RuntimeConfig;
use crate::runtime::stealing;
use anyhow::{Result, anyhow};
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;

pub(crate) struct Context {
    pub(crate) scheduler: stealing::Handle,

    pub(crate) core: RefCell<Core>,

    pub(crate) shared: Arc<Shared>,
}

impl Context {
    pub(crate) fn try_new(
        cfg: &RuntimeConfig,
        scheduler: stealing::Handle,
        shared: Arc<Shared>,
    ) -> Result<Self> {
        let core = Core::try_new(cfg, &shared)?;

        Ok(Self {
            scheduler,
            core: RefCell::new(core),
            shared,
        })
    }

    #[inline(always)]
    pub(crate) fn with_core<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Core) -> R,
    {
        f(&self.core.borrow())
    }

    #[inline(always)]
    pub(crate) fn with_shared<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Arc<Shared>) -> R,
    {
        f(&self.shared)
    }

    #[inline(always)]
    pub(crate) fn with_slab<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&RawSqeSlab) -> R,
    {
        let core = self.core.borrow();
        let slab = core.slab.borrow();
        f(&slab)
    }

    #[inline(always)]
    pub(crate) fn with_slab_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut RawSqeSlab) -> R,
    {
        let core = self.core.borrow();
        let mut slab = core.slab.borrow_mut();
        f(&mut slab)
    }

    #[inline(always)]
    pub(crate) fn with_ring<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&SingleIssuerRing) -> R,
    {
        let core = self.core.borrow();
        let slab = core.ring.borrow();
        f(&slab)
    }

    #[inline(always)]
    pub(crate) fn with_ring_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut SingleIssuerRing) -> R,
    {
        let core = self.core.borrow();
        let mut slab = core.ring.borrow_mut();
        f(&mut slab)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::assert_impl_all;

    assert_impl_all!(Shared: Send, Sync);
}
