use anyhow::Result;
use slab::RawSqeSlab;
use std::cell::{OnceCell, RefCell};
use std::io;
use std::thread_local;

use crate::context::ring::SingleIssuerRing;

// Exports
pub mod ring;
pub mod slab;

pub struct Context {
    // Indicate if the context is properly initialized
    pub init: OnceCell<bool>,

    // Slab to manage RawSqe allocations for this thread.
    pub slab: OnceCell<RawSqeSlab>,

    // IoUring to submit and complete IO operations.
    pub ring: OnceCell<SingleIssuerRing>,
}

impl Context {
    fn new() -> Self {
        Self {
            init: OnceCell::new(),
            slab: OnceCell::new(),
            ring: OnceCell::new(),
        }
    }

    fn try_initialize(&mut self, sq_ring_size: usize) -> Result<()> {
        let ring = SingleIssuerRing::try_new(sq_ring_size as u32)?;

        _ = self.init.get_or_init(|| true);
        _ = self.slab.get_or_init(|| RawSqeSlab::new(sq_ring_size));
        _ = self.ring.get_or_init(|| ring);

        Ok(())
    }

    fn check_init(&self) {
        debug_assert!(self.init.get().is_some(), "uninitialized");
    }

    // Queues SQEs in the SQ ring, fetching them from the slab using the provided
    // indices. To submit the SQEs to the kernel, we need to call `q.sync()` and
    // make an `io_uring_enter` syscall unless kernel side SQ polling is enabled.
    pub fn submit_sqes<'a>(
        &mut self,
        indices: impl IntoIterator<Item = &'a usize>,
    ) -> io::Result<i32> {
        // Need to manually access the fields to avoid immutable vs. mutable
        // borrow issues or double borrow problem.
        let slab = self.slab.get().unwrap();
        let mut q = self.ring.get_mut().unwrap().submission();

        for idx in indices {
            let entry = slab
                .get(*idx)
                .and_then(|sqe| sqe.get_entry())
                .map_err(|_e| io::Error::new(io::ErrorKind::Other, "Can't find RawSqe in slab."))?;

            unsafe { q.push(entry) }
                .map_err(|_e| io::Error::new(io::ErrorKind::ResourceBusy, "SQ ring full."))?;
        }

        Ok(0)
    }

    pub fn get_slab(&self) -> &RawSqeSlab {
        // Safety: we assert ctx is init.
        self.slab.get().unwrap()
    }

    pub fn get_slab_mut(&mut self) -> &mut RawSqeSlab {
        // Safety: we assert ctx is init.
        self.slab.get_mut().unwrap()
    }

    pub fn get_ring(&self) -> &SingleIssuerRing {
        // Safety: we assert ctx is init.
        self.ring.get().unwrap()
    }

    pub fn get_ring_mut(&mut self) -> &mut SingleIssuerRing {
        // Safety: we assert ctx is init.
        self.ring.get_mut().unwrap()
    }
}

thread_local! {
    static CONTEXT: RefCell<Context> = RefCell::new(Context::new());
}

pub fn init_context(sq_ring_size: usize) {
    CONTEXT.with(|ctx| {
        ctx.borrow_mut()
            .try_initialize(sq_ring_size)
            // There is no coming back if we fail to initialize the context,
            // let's just crash.
            .expect("Failed to initialize thread-local context.");
    });
}

// Warning: all of the `with_*` functions are not re-entrant. Do not nest calls.
#[inline]
pub fn with_context<F, R>(f: F) -> R
where
    F: FnOnce(&Context) -> R,
{
    CONTEXT.with_borrow(|ctx| {
        ctx.check_init();
        f(&ctx)
    })
}

#[inline]
pub fn with_context_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut Context) -> R,
{
    CONTEXT.with_borrow_mut(|mut ctx| {
        ctx.check_init();
        f(&mut ctx)
    })
}

#[inline]
pub fn with_slab<F, R>(f: F) -> R
where
    F: FnOnce(&RawSqeSlab) -> R,
{
    with_context(|ctx| {
        // Safety: context is checked for initialization.
        let slab = ctx.get_slab();
        f(&slab)
    })
}

#[inline]
pub fn with_slab_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut RawSqeSlab) -> R,
{
    with_context_mut(|ctx| {
        // Safety: context is checked for initialization.
        let mut slab = ctx.get_slab_mut();
        f(&mut slab)
    })
}

#[inline]
pub fn with_ring<F, R>(f: F) -> R
where
    F: FnOnce(&SingleIssuerRing) -> R,
{
    with_context(|ctx| {
        // Safety: context is checked for initialization.
        let ring = ctx.get_ring();
        f(&ring)
    })
}

#[inline]
pub fn with_ring_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut SingleIssuerRing) -> R,
{
    with_context_mut(|ctx| {
        // Safety: context is checked for initialization.
        let mut ring = ctx.get_ring_mut();
        f(&mut ring)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::catch_unwind;
    use std::thread;

    #[test]
    fn test_context_is_thread_local() {
        const THREAD_A_RING_SIZE: usize = 64;
        const THREAD_B_RING_SIZE: usize = 64;

        init_context(THREAD_A_RING_SIZE);
        with_context_mut(|ctx| {
            assert_eq!(ctx.get_slab().capacity(), THREAD_A_RING_SIZE);
            assert_eq!(
                ctx.get_ring_mut().submission().capacity(),
                THREAD_A_RING_SIZE
            );
        });

        let handle = thread::spawn(move || {
            init_context(THREAD_B_RING_SIZE);
            with_context_mut(|ctx| {
                assert_eq!(ctx.get_slab().capacity(), THREAD_B_RING_SIZE);
                assert_eq!(
                    ctx.get_ring_mut().submission().capacity(),
                    THREAD_B_RING_SIZE
                );
            });
        });

        assert!(handle.join().is_ok());

        with_context_mut(|ctx| {
            assert_eq!(ctx.get_slab().capacity(), THREAD_A_RING_SIZE);
            assert_eq!(
                ctx.get_ring_mut().submission().capacity(),
                THREAD_A_RING_SIZE
            );
        });
    }

    #[test]
    fn test_context_is_not_reentrant() {
        init_context(64);

        // Scenario 1: Nesting a mutable borrow inside an immutable borrow.
        assert!(
            catch_unwind(|| {
                with_context(|_outer_ctx| {
                    with_context_mut(|_inner_ctx| {});
                })
            })
            .is_err(),
            "Nesting mutable access inside immutable access must panic due to RefCell rules."
        );

        // Scenario 2: Nesting a mutable borrow inside a mutable borrow.
        assert!(
            catch_unwind(|| {
                with_context_mut(|_outer_ctx| {
                    with_context_mut(|_inner_ctx| {});
                })
            })
            .is_err(),
            "Nesting mutable access inside mutable access must panic due to RefCell rules."
        );

        // Scenario 3: Nesting immutable borrow inside a mutable borrow.
        assert!(
            catch_unwind(|| {
                with_context_mut(|_outer_ctx| {
                    with_context(|_inner_ctx| {});
                })
            })
            .is_err(),
            "Nesting immutable access inside mutable access must panic due to RefCell rules."
        );
    }
}
