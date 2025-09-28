use anyhow::Result;
use either::Either;
use io_uring::types::{SubmitArgs, Timespec};
use slab::RawSqeSlab;
use std::cell::{OnceCell, RefCell};
use std::io;
use std::os::unix::io::RawFd;
use std::thread_local;
use std::time::Duration;

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

    pub fn ring_fd(&self) -> RawFd {
        self.get_ring().as_raw_fd()
    }

    // Queues SQEs in the SQ ring, fetching them from the slab using the provided
    // indices. To submit the SQEs to the kernel, we need to call `q.sync()` and
    // make an `io_uring_enter` syscall unless kernel side SQ polling is enabled.
    pub fn push_sqes<'a>(
        &mut self,
        indices: impl IntoIterator<Item = &'a usize>,
    ) -> io::Result<i32> {
        // Need to manually access the fields to avoid immutable vs. mutable
        // borrow issues or double borrow problem.
        let slab = self.slab.get().unwrap();
        let mut sq = self.ring.get_mut().unwrap().submission();

        for idx in indices {
            let entry = slab
                .get(*idx)
                .and_then(|sqe| sqe.get_entry())
                .map_err(|_e| io::Error::new(io::ErrorKind::Other, "Can't find RawSqe in slab."))?;

            unsafe { sq.push(entry) }
                .map_err(|_e| io::Error::new(io::ErrorKind::ResourceBusy, "SQ ring full."))?;
        }

        Ok(0)
    }

    pub fn submit_and_wait_timeout(
        &mut self,
        num_to_wait: usize,
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        let ring = self.get_ring_mut().get_mut();

        // Sync user space and kernel shared queue
        ring.submission().sync();

        if let Some(duration) = timeout {
            let ts = Timespec::from(duration);
            let args = SubmitArgs::new().timespec(&ts);

            return ring.submitter().submit_with_args(num_to_wait, &args);
        }

        ring.submitter().submit_and_wait(num_to_wait)
    }

    pub fn has_pending_cqes(&self) -> bool {
        self.get_ring().has_pending_cqes()
    }

    pub fn process_cqes(&mut self, num_to_complete: Option<usize>) -> Result<usize> {
        let mut num_completed = 0;

        {
            let slab = self.slab.get_mut().unwrap();
            let cq = self.ring.get_mut().unwrap().completion();

            // If `num_to_complete` is not provided, we complete as many CQEs as
            // possible.
            let q_iter = if let Some(n) = num_to_complete {
                Either::Right(cq.into_iter().take(n))
            } else {
                Either::Left(cq.into_iter())
            };

            for cqe in q_iter {
                let raw_sqe = match slab.get_mut(cqe.user_data() as usize) {
                    Err(e) => {
                        eprintln!("CQE user data not found in RawSqeSlab: {:?}", e);
                        continue;
                    }
                    Ok(sqe) => sqe,
                };

                let head_to_wake = raw_sqe.on_completion(cqe.result())?;
                num_completed += 1;

                // We have to handle waking up the head outside the scope of RawSqe
                // to get around Rust's double borrow rule on the slab.
                if let Some(head) = head_to_wake {
                    slab.get_mut(head)?.wake()?;
                }
            }
        }

        // Sync with kernel queue.
        let mut cq = self.ring.get_mut().unwrap().completion();
        cq.sync();

        Ok(num_completed)
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
