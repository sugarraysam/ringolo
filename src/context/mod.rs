use slab::RawSqeSlab;
use std::cell::{OnceCell, RefCell};
use std::thread_local;

// Exports
pub mod global;
pub use global::GlobalContext;

pub mod local;
use local::LocalContext;

pub mod ring;
pub mod slab;

// Why are we limiting ourselves to 255 threads in our program? The reason is
// this is a constrait of the RingMessage protocol. We have very limited space
// to encode information in our messages, and we need to fit our header in 32
// bits.
pub type ThreadId = u8;

thread_local! {
    static CONTEXT: OnceCell<RefCell<LocalContext>> = OnceCell::new();
}

// Lazily initialize LocalContext /w custom args.
pub fn init_context(sq_ring_size: usize) {
    CONTEXT.with(|ctx| {
        ctx.get_or_init(|| {
            RefCell::new(
                LocalContext::try_new(sq_ring_size)
                    .expect("Failed to initialize thread-local context"),
            )
        });
    });
}

// Warning: all of the `with_*` functions are not re-entrant. Do not nest calls.
#[inline]
pub fn with_context<F, R>(f: F) -> R
where
    F: FnOnce(&LocalContext) -> R,
{
    CONTEXT.with(|ctx| {
        let ctx = ctx.get().expect("LocalContext not initialized").borrow();
        f(&ctx)
    })
}

#[inline]
pub fn with_context_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut LocalContext) -> R,
{
    CONTEXT.with(|ctx| {
        let mut ctx = ctx
            .get()
            .expect("LocalContext not initialized")
            .borrow_mut();
        f(&mut ctx)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::collections::HashSet;
    use std::os::fd::RawFd;
    use std::panic::catch_unwind;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    #[test]
    fn test_context_is_thread_local() {
        const THREAD_A_RING_SIZE: usize = 64;
        const THREAD_B_RING_SIZE: usize = 64;

        init_context(THREAD_A_RING_SIZE);
        with_context_mut(|ctx| {
            assert_eq!(ctx.slab.capacity(), THREAD_A_RING_SIZE);
            assert_eq!(ctx.ring.submission().capacity(), THREAD_A_RING_SIZE);
        });

        let handle = thread::spawn(move || {
            init_context(THREAD_B_RING_SIZE);
            with_context_mut(|ctx| {
                assert_eq!(ctx.slab.capacity(), THREAD_B_RING_SIZE);
                assert_eq!(ctx.ring.submission().capacity(), THREAD_B_RING_SIZE);
            });
        });

        assert!(handle.join().is_ok());

        with_context_mut(|ctx| {
            assert_eq!(ctx.slab.capacity(), THREAD_A_RING_SIZE);
            assert_eq!(ctx.ring.submission().capacity(), THREAD_A_RING_SIZE);
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

    #[test]
    fn test_context_ring_fd_registration() -> Result<()> {
        let expected = 10;
        let done = Arc::new(AtomicBool::new(false));

        let data = Arc::new(Mutex::new(ThreadData::new()));
        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        // register N threads and guarantee uniqueness for both:
        // - thread_ids
        // - ring_fds
        for _ in 0..expected {
            let data_clone = Arc::clone(&data);
            let done_clone = Arc::clone(&done);

            let handle = thread::spawn(move || {
                init_context(32);
                let (thread_id, ring_fd) = with_context(|ctx| -> (ThreadId, RawFd) {
                    let ring_fd = GlobalContext::instance().get_ring_fd(ctx.thread_id);
                    assert!(ring_fd.is_ok());
                    assert_eq!(ring_fd.unwrap(), ctx.ring_fd);

                    (ctx.thread_id, ctx.ring_fd)
                });

                let mut data = data_clone.lock().unwrap();
                assert!(data.thread_ids.insert(thread_id));
                assert!(data.ring_fds.insert(ring_fd));

                while !done_clone.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(10));
                }
            });

            handles.push(handle);
        }

        done.store(true, Ordering::Relaxed);

        for handle in handles {
            assert!(handle.join().is_ok());
        }

        let data = data.lock().unwrap();
        assert_eq!(data.thread_ids.len(), expected);
        assert_eq!(data.ring_fds.len(), expected);

        // TODO: thread unregisters itself upon exit
        Ok(())
    }
}
