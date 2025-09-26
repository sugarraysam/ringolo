use slab::RawSqeSlab;
use std::cell::{OnceCell, RefCell};
use std::thread_local;

// exports
pub mod slab;
pub mod ring;

pub struct Context {
    // Indicate if the context is properly initialized
    is_initialized: OnceCell<bool>,

    // Slab to manage RawSqe allocations for this thread.
    slab: OnceCell<RawSqeSlab>,
}

impl Context {
    fn new() -> Self {
        Self {
            is_initialized: OnceCell::new(),
            slab: OnceCell::new(),
        }
    }

    pub fn initialize(&mut self, sq_ring_size: usize) {
        _ = self.is_initialized.get_or_init(|| true);
        _ = self.slab.get_or_init(|| RawSqeSlab::new(sq_ring_size));
    }
}

// TODO:
// - add thread local iouring
// Placeholder initialization; need to call `init_context` before using.
thread_local! {
    pub static CONTEXT: RefCell<Context> = RefCell::new(Context::new());
}

pub fn init_context(sq_ring_size: usize) {
    CONTEXT.with(|ctx| {
        ctx.borrow_mut().initialize(sq_ring_size);
    });
}

// Always use `with_context_checked`. Nested lambdas should be optimized away by compiler.
#[inline]
fn with_context_checked<F, R>(f: F) -> R
where
    F: FnOnce(&Context) -> R,
{
    CONTEXT.with_borrow(|ctx| {
        debug_assert!(
            ctx.is_initialized.get().unwrap(),
            "Thread-local Context not initialized. Call `init_context()` first"
        );

        f(&ctx)
    })
}

#[inline]
fn with_context_mut_checked<F, R>(f: F) -> R
where
    F: FnOnce(&mut Context) -> R,
{
    CONTEXT.with_borrow_mut(|mut ctx| {
        debug_assert!(
            ctx.is_initialized.get().unwrap(),
            "Thread-local Context not initialized. Call `init_context()` first"
        );

        f(&mut ctx)
    })
}

#[inline]
pub fn with_context<F, R>(f: F) -> R
where
    F: FnOnce(&Context) -> R,
{
    with_context_checked(f)
}

#[inline]
pub fn with_context_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut Context) -> R,
{
    with_context_mut_checked(f)
}

#[inline]
pub fn with_slab<F, R>(f: F) -> R
where
    F: FnOnce(&RawSqeSlab) -> R,
{
    with_context_checked(|ctx| {
        // Safety: context is checked for initialization.
        let slab = ctx.slab.get().unwrap();
        f(&slab)
    })
}

#[inline]
pub fn with_slab_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut RawSqeSlab) -> R,
{
    with_context_mut_checked(|ctx| {
        // Safety: context is checked for initialization.
        let mut slab = ctx.slab.get_mut().unwrap();
        f(&mut slab)
    })
}
