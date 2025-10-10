// Expose rich API for developers even if unused.
#![allow(unused)]

use crate::context::Core;
use crate::context::RawSqeSlab;
use crate::context::SingleIssuerRing;
use crate::runtime::RuntimeConfig;
use crate::runtime::local;
use crate::util::ScopeGuard;
use anyhow::Result;
use std::cell::RefCell;

// Trick to enable observability of scheduler calls for tests. This is more powerful
// than mocks as the *real* methods are actually invoked on the *real* scheduler
// implementation.
#[cfg(test)]
mod details {
    use super::local;
    use crate::test_utils::spy::SpyScheduler;

    pub(super) type Scheduler = SpyScheduler<local::Handle>;
    pub(super) fn wrap_scheduler(scheduler: local::Handle) -> Scheduler {
        SpyScheduler::new(scheduler)
    }
}

#[cfg(not(test))]
mod details {
    use super::local;

    pub(super) type Scheduler = local::Handle;
    pub(super) fn wrap_scheduler(scheduler: local::Handle) -> Scheduler {
        scheduler
    }
}

pub(crate) struct Context {
    pub(crate) scheduler: details::Scheduler,

    pub(crate) core: RefCell<Core>,
}

impl Context {
    pub(crate) fn try_new(cfg: &RuntimeConfig, scheduler: local::Handle) -> Result<Self> {
        let core = Core::try_new(cfg)?;

        Ok(Self {
            scheduler: details::wrap_scheduler(scheduler),
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
