use slab::RawSqeSlab;
use std::cell::{OnceCell, RefCell};
use std::thread_local;

// exports
pub mod ring;
pub mod slab;

pub struct Context {
    // Indicate if the context is properly initialized
    pub init: OnceCell<bool>,

    // Slab to manage RawSqe allocations for this thread.
    pub slab: OnceCell<RawSqeSlab>,
}

impl Context {
    fn new() -> Self {
        Self {
            init: OnceCell::new(),
            slab: OnceCell::new(),
        }
    }

    fn initialize(&mut self, sq_ring_size: usize) {
        _ = self.init.get_or_init(|| true);
        _ = self.slab.get_or_init(|| RawSqeSlab::new(sq_ring_size));
    }

    fn check_init(&self) {
        debug_assert!(self.init.get().is_some(), "uninitialized");
    }

    pub fn get_slab(&self) -> &RawSqeSlab {
        // Safety: we assert ctx is init.
        self.slab.get().unwrap()
    }

    pub fn get_slab_mut(&mut self) -> &mut RawSqeSlab {
        // Safety: we assert ctx is init.
        self.slab.get_mut().unwrap()
    }
}

// TODO:
// - add thread local iouring
// Placeholder initialization; need to call `init_context` before using.
thread_local! {
    static CONTEXT: RefCell<Context> = RefCell::new(Context::new());
}

pub fn init_context(sq_ring_size: usize) {
    CONTEXT.with(|ctx| {
        ctx.borrow_mut().initialize(sq_ring_size);
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
        with_slab(|slab| {
            assert_eq!(slab.capacity(), THREAD_A_RING_SIZE);
        });

        let handle = thread::spawn(move || {
            init_context(THREAD_B_RING_SIZE);
            with_slab(|slab| {
                assert_eq!(slab.capacity(), THREAD_B_RING_SIZE);
            });
        });

        assert!(handle.join().is_ok());

        with_slab(|slab| {
            assert_eq!(slab.capacity(), THREAD_A_RING_SIZE);
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
