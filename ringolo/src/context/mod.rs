// Keep unused context methods to provide rich API for future developers.
#![allow(dead_code)]

use crate::runtime::{local, stealing, RuntimeConfig, Scheduler};
use crate::task::Id;
use anyhow::Result;
use std::cell::{OnceCell, RefCell};
use std::thread_local;

// Exports
mod core;
pub(crate) use core::{Core, ThreadId};

pub(crate) mod ring;
pub(crate) use ring::SingleIssuerRing;

pub(crate) mod slab;
pub(crate) use slab::RawSqeSlab;

pub(crate) struct RootContext {
    context: Context,
    scheduler: Scheduler,
}

impl RootContext {
    fn new_local(ctx: local::Context, scheduler: local::Handle) -> Self {
        Self {
            context: Context::Local(ctx),
            scheduler: Scheduler::Local(scheduler),
        }
    }

    fn new_stealing(ctx: stealing::Context, scheduler: stealing::Handle) -> Self {
        Self {
            context: Context::Stealing(ctx),
            scheduler: Scheduler::Stealing(scheduler),
        }
    }
}

// We need type erasure in Thread-local storage so we hide the runtime context
// impl behind this enum and unfortunately have to match everywhere. This is
// still better than doing dynamic dispatch.
pub(crate) enum Context {
    Local(local::Context),
    Stealing(stealing::Context),
}

thread_local! {
    static CONTEXT: OnceCell<RefCell<RootContext>> = const { OnceCell::new() };
}

pub(crate) fn init_local_context(cfg: &RuntimeConfig, scheduler: local::Handle) -> Result<()> {
    CONTEXT.with(|ctx| {
        ctx.get_or_init(|| {
            let ctx =
                local::Context::try_new(cfg).expect("Failed to initialize thread-local context");
            RefCell::new(RootContext::new_local(ctx, scheduler))
        });
    });

    Ok(())
}

#[track_caller]
pub(crate) fn expect_local_scheduler<F, R>(f: F) -> R
where
    F: FnOnce(&local::Context, &local::Handle) -> R,
{
    CONTEXT.with(|ctx| {
        let root = ctx.get().expect("Context not initialized").borrow();

        match (&root.context, &root.scheduler) {
            (Context::Local(ctx), Scheduler::Local(scheduler)) => f(ctx, scheduler),
            _ => {
                panic!("Expected local scheduler to be initialized.");
            }
        }
    })
}

#[track_caller]
pub(crate) fn expect_stealing_scheduler<F, R>(f: F) -> R
where
    F: FnOnce(&stealing::Context, &stealing::Handle) -> R,
{
    CONTEXT.with(|ctx| {
        let root = ctx.get().expect("Context not initialized").borrow();

        match (&root.context, &root.scheduler) {
            (Context::Stealing(ctx), Scheduler::Stealing(scheduler)) => f(ctx, scheduler),
            _ => {
                panic!("Expected stealing scheduler to be initialized.");
            }
        }
    })
}

#[inline(always)]
pub(crate) fn with_core<F, R>(f: F) -> R
where
    F: FnOnce(&Core) -> R,
{
    with_context(|outer| match outer {
        Context::Local(c) => c.with_core(f),
        Context::Stealing(c) => c.with_core(f),
    })
}

#[inline(always)]
pub(crate) fn with_core_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut Core) -> R,
{
    with_context(|outer| match outer {
        Context::Local(c) => c.with_core_mut(f),
        Context::Stealing(c) => c.with_core_mut(f),
    })
}

#[inline(always)]
pub(crate) fn with_slab<F, R>(f: F) -> R
where
    F: FnOnce(&RawSqeSlab) -> R,
{
    with_core(|core| f(&core.slab.borrow()))
}

#[inline(always)]
pub(crate) fn with_slab_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut RawSqeSlab) -> R,
{
    with_core_mut(|core| f(&mut core.slab.borrow_mut()))
}

#[inline(always)]
pub(crate) fn with_ring<F, R>(f: F) -> R
where
    F: FnOnce(&SingleIssuerRing) -> R,
{
    with_core(|core| f(&core.ring.borrow()))
}

#[inline(always)]
pub(crate) fn with_ring_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut SingleIssuerRing) -> R,
{
    with_core_mut(|core| f(&mut core.ring.borrow_mut()))
}

#[inline(always)]
pub(crate) fn with_slab_and_ring<F, R>(f: F) -> R
where
    F: FnOnce(&RawSqeSlab, &SingleIssuerRing) -> R,
{
    with_slab(|slab| with_ring(|ring| f(slab, ring)))
}

#[inline(always)]
pub(crate) fn with_slab_and_ring_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut RawSqeSlab, &mut SingleIssuerRing) -> R,
{
    with_core_mut(|core| {
        let mut slab = core.slab.borrow_mut();
        let mut ring = core.ring.borrow_mut();
        f(&mut slab, &mut ring)
    })
}

pub(crate) fn current_thread_id() -> ThreadId {
    with_context(|outer| match outer {
        Context::Local(c) => c.core.borrow().thread_id,
        Context::Stealing(c) => c.core.borrow().thread_id,
    })
}

pub(crate) fn current_task_id() -> Option<Id> {
    with_context(|outer| match outer {
        Context::Local(c) => c.core.borrow().current_task_id.get(),
        Context::Stealing(c) => c.core.borrow().current_task_id.get(),
    })
}

pub(crate) fn set_current_task_id(id: Option<Id>) -> Option<Id> {
    with_context(|outer| match outer {
        Context::Local(c) => c.core.borrow().current_task_id.replace(id),
        Context::Stealing(c) => c.core.borrow().current_task_id.replace(id),
    })
}

// Private helpers.
#[inline(always)]
pub(crate) fn with_context<F, R>(f: F) -> R
where
    F: FnOnce(&Context) -> R,
{
    CONTEXT.with(|ctx| {
        let root = ctx.get().expect("Context not initialized").borrow();
        f(&root.context)
    })
}

#[inline(always)]
pub(crate) fn with_scheduler<F, R>(f: F) -> R
where
    F: FnOnce(&Scheduler) -> R,
{
    CONTEXT.with(|ctx| {
        let root = ctx.get().expect("Context not initialized").borrow();
        f(&root.scheduler)
    })
}

/// The macro accepts a pattern that looks just like a closure.
/// |$scheduler:ident| is the argument name (e.g., |s| or |scheduler|)
/// $body:block is the code block that follows.
//
// Could not make this work without a macro. Tried a few things but:
// - can't coerce a scheduler reference to &dyn Schedule because of Sized bound
// - can't impl enum static dispatch (impl Schedule on Context) because
//   Task<Self> arguments
// - `for<S: Schedule> FnOnce(&S)` HRBT not yet supported: https://github.com/rust-lang/rust/issues/108185
#[macro_export]
macro_rules! with_scheduler {
    (|$scheduler:ident| $body:block) => {
        $crate::context::with_scheduler(|root| match root {
            $crate::runtime::Scheduler::Local(s) => {
                let $scheduler = s;
                $body
            }
            $crate::runtime::Scheduler::Stealing(s) => {
                let $scheduler = s;
                $body
            }
        })
    };
}

#[cfg(test)]
mod tests {
    use crate::runtime::Builder;
    use crate::test_utils::init_local_runtime_and_context;

    use super::*;
    use anyhow::Result;
    use std::collections::HashSet;
    use std::os::fd::RawFd;
    use std::panic::catch_unwind;
    use std::thread;
    // use std::sync::atomic::{AtomicBool, Ordering};
    // use std::sync::{Arc, Mutex};
    // use std::thread::{self, JoinHandle};
    // use std::time::Duration;

    #[test]
    fn test_context_is_thread_local() -> Result<()> {
        const THREAD_A_RING_SIZE: usize = 64;
        const THREAD_B_RING_SIZE: usize = 64;

        let builder_a = Builder::new_local().sq_ring_size(THREAD_A_RING_SIZE);
        let builder_b = Builder::new_local().sq_ring_size(THREAD_B_RING_SIZE);

        init_local_runtime_and_context(Some(builder_a))?;

        with_slab_and_ring_mut(|slab, ring| {
            assert_eq!(slab.capacity(), THREAD_A_RING_SIZE * 2);
            assert_eq!(ring.sq().capacity(), THREAD_A_RING_SIZE);
        });

        let handle = thread::spawn(move || -> Result<()> {
            init_local_runtime_and_context(Some(builder_b))?;
            with_slab_and_ring_mut(|slab, ring| {
                assert_eq!(slab.capacity(), THREAD_B_RING_SIZE * 2);
                assert_eq!(ring.sq().capacity(), THREAD_B_RING_SIZE);
            });
            Ok(())
        });

        assert!(handle.join().is_ok());

        with_slab_and_ring_mut(|slab, ring| {
            assert_eq!(slab.capacity(), THREAD_B_RING_SIZE * 2);
            assert_eq!(ring.sq().capacity(), THREAD_B_RING_SIZE);
        });

        Ok(())
    }

    #[test]
    fn test_context_has_interior_mutability() -> Result<()> {
        init_local_runtime_and_context(None)?;

        assert!(
            catch_unwind(|| {
                with_context(|_outer_ctx| with_slab_and_ring_mut(|_slab, _ring| {}))
            })
            .is_ok(),
            "Can borrow two root fields as mut because interior mutability."
        );

        assert!(
            catch_unwind(|| {
                with_context(|_outer_ctx| {
                    with_context(|_inner_ctx| {});
                })
            })
            .is_ok(),
            "Can have N immutable borrows of root context."
        );

        Ok(())
    }

    struct ThreadData {
        pub thread_ids: HashSet<ThreadId>,
        pub ring_fds: HashSet<RawFd>,
    }

    impl ThreadData {
        pub fn new() -> Self {
            Self {
                thread_ids: HashSet::new(),
                ring_fds: HashSet::new(),
            }
        }
    }

    // TODO: fix with stealing scheduler (need thread pool)
    // #[test]
    // fn test_context_ring_fd_registration() -> Result<()> {
    //     let expected = 10;
    //     let done = Arc::new(AtomicBool::new(false));

    //     let data = Arc::new(Mutex::new(ThreadData::new()));
    //     let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();

    //     // register N threads and guarantee uniqueness for both:
    //     // - thread_ids
    //     // - ring_fds
    //     for _ in 0..expected {
    //         let data_clone = Arc::clone(&data);
    //         let done_clone = Arc::clone(&done);

    //         let handle = thread::spawn(move || -> Result<()> {
    //             init_context(None, 32)?;
    //             let (thread_id, ring_fd) = with_context(|ctx| -> (ThreadId, RawFd) {
    //                 let ring_fd = GlobalContext::instance().get_ring_fd(ctx.thread_id);
    //                 assert!(ring_fd.is_ok());
    //                 assert_eq!(ring_fd.unwrap(), ctx.ring_fd);

    //                 (ctx.thread_id, ctx.ring_fd)
    //             });

    //             let mut data = data_clone.lock().unwrap();
    //             assert!(data.thread_ids.insert(thread_id));
    //             assert!(data.ring_fds.insert(ring_fd));

    //             while !done_clone.load(Ordering::Relaxed) {
    //                 thread::sleep(Duration::from_millis(10));
    //             }

    //             Ok(())
    //         });

    //         handles.push(handle);
    //     }

    //     done.store(true, Ordering::Relaxed);

    //     for handle in handles {
    //         assert!(handle.join().is_ok());
    //     }

    //     let data = data.lock().unwrap();
    //     assert_eq!(data.thread_ids.len(), expected);
    //     assert_eq!(data.ring_fds.len(), expected);

    //     Ok(())
    // }
}
