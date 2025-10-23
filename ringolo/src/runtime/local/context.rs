// Expose rich API for developers even if unused.
#![allow(dead_code)]

use crate::context::RawSqeSlab;
use crate::context::SingleIssuerRing;
use crate::context::{Core, Shared};
use crate::runtime::RuntimeConfig;
use crate::task::id::ROOT_FUTURE_ID;
use crate::utils::ScopeGuard;
use anyhow::Result;
use std::cell::RefCell;
use std::sync::Arc;

pub(crate) struct Context {
    pub(crate) core: RefCell<Core>,

    pub(crate) shared: Arc<Shared>,
}

impl Context {
    pub(crate) fn try_new(cfg: &RuntimeConfig) -> Result<Self> {
        let shared = Arc::new(Shared::new(cfg));
        let core = Core::try_new(cfg, &shared)?;

        Ok(Self {
            core: RefCell::new(core),
            shared,
        })
    }

    pub(crate) fn set_polling_root(&self) -> ScopeGuard<'_, impl FnOnce()> {
        let prev_task_id = self.with_core(|c| {
            c.polling_root_future.replace(true);
            c.current_task_id.replace(Some(ROOT_FUTURE_ID))
        });

        // Should be None because by definition the root future has no parent.
        debug_assert!(prev_task_id.is_none());

        ScopeGuard::new(move || {
            self.with_core(|c| {
                c.polling_root_future.replace(false);
                c.current_task_id.replace(prev_task_id);
            })
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
        let ring = core.ring.borrow();
        f(&ring)
    }

    #[inline(always)]
    pub(crate) fn with_ring_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut SingleIssuerRing) -> R,
    {
        let core = self.core.borrow();
        let mut ring = core.ring.borrow_mut();
        f(&mut ring)
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
