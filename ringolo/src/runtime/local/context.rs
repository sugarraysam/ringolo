// Expose rich API for developers even if unused.
#![allow(unused)]

use crate::context::Core;
use crate::context::RawSqeSlab;
use crate::context::SingleIssuerRing;
use crate::runtime::OwnedTasks;
use crate::runtime::RuntimeConfig;
use crate::runtime::local;
use crate::utils::ScopeGuard;
use anyhow::Result;
use std::cell::RefCell;

pub(crate) struct Context {
    pub(crate) core: RefCell<Core>,
}

impl Context {
    pub(crate) fn try_new(cfg: &RuntimeConfig) -> Result<Self> {
        let core = Core::try_new(cfg)?;

        Ok(Self {
            core: RefCell::new(core),
        })
    }

    pub(crate) fn set_polling_root(&self) -> ScopeGuard<'_, impl FnOnce()> {
        self.with_core(|c| c.polling_root_future.replace(true));
        ScopeGuard::new(|| {
            self.with_core(|c| c.polling_root_future.replace(false));
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
    pub(crate) fn with_core_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Core) -> R,
    {
        f(&mut self.core.borrow_mut())
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
