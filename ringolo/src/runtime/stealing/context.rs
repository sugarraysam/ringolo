use crate::context::{Core, RawSqeSlab, Shared, SingleIssuerRing};
use crate::runtime::RuntimeConfig;
use anyhow::Result;
use std::cell::RefCell;
use std::sync::Arc;

pub(crate) struct Context {
    pub(crate) core: RefCell<Core>,

    pub(crate) shared: Arc<Shared>,
}

impl Context {
    pub(crate) fn try_new(cfg: &RuntimeConfig, shared: Arc<Shared>) -> Result<Self> {
        Ok(Self {
            core: RefCell::new(Core::try_new(cfg, &shared)?),
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

    #[inline(always)]
    pub(crate) fn with_slab_and_ring_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut RawSqeSlab, &mut SingleIssuerRing) -> R,
    {
        let core = self.core.borrow();
        let mut slab = core.slab.borrow_mut();
        let mut ring = core.ring.borrow_mut();
        f(&mut slab, &mut ring)
    }
}
