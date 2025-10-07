use crate::context::Core;
use crate::context::RawSqeSlab;
use crate::context::SingleIssuerRing;
use crate::runtime::RuntimeConfig;
use crate::runtime::local;
use anyhow::Result;
use std::cell::RefCell;
use std::rc::Rc;

pub(crate) struct Context {
    pub scheduler: local::Handle,

    pub worker: Rc<local::Worker>,

    pub core: RefCell<Core>,
}

impl Context {
    pub(crate) fn try_new(
        cfg: &RuntimeConfig,
        scheduler: local::Handle,
        worker: Rc<local::Worker>,
    ) -> Result<Self> {
        let core = Core::try_new(cfg)?;

        Ok(Self {
            scheduler,
            worker,
            core: RefCell::new(core),
        })
    }

    pub(crate) fn get_worker(&self) -> &local::Worker {
        &self.worker
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
}
