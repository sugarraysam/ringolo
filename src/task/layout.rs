#![allow(unsafe_op_in_unsafe_fn)]

use crate::task::header::Header;
use crate::task::id::TaskIdGuard;
use crate::task::state::State;
use crate::task::trailer::Trailer;
use crate::task::vtable::vtable;
use crate::task::{Id, Schedule};
use std::boxed::Box;
use std::cell::UnsafeCell;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

/// The task cell. Contains the components of the task.
///
/// It is critical for `Header` to be the first field as the task structure will
/// be referenced by both *mut Cell and *mut Header.
///
/// Any changes to the layout of this struct _must_ also be reflected in the
/// `const` fns in raw.rs.
///
// # This struct should be cache padded to avoid false sharing. The cache padding rules are copied
// from crossbeam-utils/src/cache_padded.rs
//
// Starting from Intel's Sandy Bridge, spatial prefetcher is now pulling pairs of 64-byte cache
// lines at a time, so we have to align to 128 bytes rather than 64.
//
// Sources:
// - https://www.intel.com/content/dam/www/public/us/en/documents/manuals/64-ia-32-architectures-optimization-manual.pdf
// - https://github.com/facebook/folly/blob/1b5288e6eea6df074758f877c849b6e73bbb9fbb/folly/lang/Align.h#L107
#[repr(C)]
pub(super) struct TaskLayout<T: Future, S> {
    /// Hot task state data
    pub(super) header: Header,

    /// Either the future or output, depending on the execution stage.
    pub(super) core: Core<T, S>,

    /// Cold data
    pub(super) trailer: Trailer,
}

impl<T: Future, S: Schedule> TaskLayout<T, S> {
    /// Allocates a new task cell, containing the header, trailer, and core
    /// structures.
    pub(super) fn new(future: T, scheduler: S, state: State, id: Id) -> Box<TaskLayout<T, S>> {
        let vtable = vtable::<T, S>();

        let result = Box::new(TaskLayout {
            header: Header::new(state, vtable),
            core: Core {
                scheduler,
                id,
                stage: CoreStage {
                    stage: UnsafeCell::new(Stage::Running(future)),
                },
            },
            trailer: Trailer::new(),
        });

        #[cfg(debug_assertions)]
        {
            // Using a separate function for this code avoids instantiating it separately for every `T`.
            unsafe fn check<S>(header: &Header, trailer: &Trailer, scheduler: &S) {
                let scheduler_addr = scheduler as *const S as usize;
                let scheduler_ptr = unsafe { Header::get_scheduler::<S>(NonNull::from(header)) };
                assert_eq!(scheduler_addr, scheduler_ptr.as_ptr() as usize);

                let trailer_addr = trailer as *const Trailer as usize;
                let trailer_ptr = unsafe { Header::get_trailer(NonNull::from(header)) };
                assert_eq!(trailer_addr, trailer_ptr.as_ptr() as usize);
            }

            unsafe {
                check(&result.header, &result.trailer, &result.core.scheduler);
            }
        }

        result
    }
}

/// The core of the task.
///
/// Holds the future or output, depending on the stage of execution.
///
/// Any changes to the layout of this struct _must_ also be reflected in the
/// `const` fns in raw.rs.
#[repr(C)]
pub(super) struct Core<T: Future, S> {
    /// Scheduler used to drive this future.
    pub(super) scheduler: S,

    /// The task's ID, used for populating `JoinError`s.
    pub(super) id: Id,

    /// Either the future or the output.
    pub(super) stage: CoreStage<T>,
}

pub(super) struct CoreStage<T: Future> {
    stage: UnsafeCell<Stage<T>>,
}

/// Either the future or the output.
#[repr(C)] // https://github.com/rust-lang/miri/issues/3780
pub(super) enum Stage<T: Future> {
    Running(T),
    Finished(super::Result<T::Output>),
    Consumed,
}

impl<T: Future> CoreStage<T> {
    pub(super) fn with_mut<R>(&self, f: impl FnOnce(*mut Stage<T>) -> R) -> R {
        f(self.stage.get())
    }
}

impl<T: Future, S: 'static> Core<T, S> {
    /// Polls the future.
    ///
    /// # Safety
    ///
    /// The caller must ensure it is safe to mutate the `state` field. This
    /// requires ensuring mutual exclusion between any concurrent thread that
    /// might modify the future or output field.
    ///
    /// The mutual exclusion is implemented by `Harness` and the `Lifecycle`
    /// component of the task state.
    ///
    /// `self` must also be pinned. This is handled by storing the task on the
    /// heap.
    pub(super) fn poll(&self, mut cx: Context<'_>) -> Poll<T::Output> {
        let res = {
            self.stage.with_mut(|ptr| {
                // Safety: The caller ensures mutual exclusion to the field.
                let future = match unsafe { &mut *ptr } {
                    Stage::Running(future) => future,
                    _ => unreachable!("unexpected stage"),
                };

                // Safety: The caller ensures the future is pinned.
                let future = unsafe { Pin::new_unchecked(future) };

                let _guard = TaskIdGuard::enter(self.id);
                future.poll(&mut cx)
            })
        };

        if res.is_ready() {
            self.drop_future_or_output();
        }

        res
    }

    /// Drops the future.
    ///
    /// # Safety
    ///
    /// The caller must ensure it is safe to mutate the `stage` field.
    pub(super) fn drop_future_or_output(&self) {
        // Safety: the caller ensures mutual exclusion to the field.
        unsafe {
            self.set_stage(Stage::Consumed);
        }
    }

    /// Stores the task output.
    ///
    /// # Safety
    ///
    /// The caller must ensure it is safe to mutate the `stage` field.
    pub(super) fn store_output(&self, output: super::Result<T::Output>) {
        // Safety: the caller ensures mutual exclusion to the field.
        unsafe {
            self.set_stage(Stage::Finished(output));
        }
    }

    /// Takes the task output.
    ///
    /// # Safety
    ///
    /// The caller must ensure it is safe to mutate the `stage` field.
    pub(super) fn take_output(&self) -> super::Result<T::Output> {
        self.stage.with_mut(|ptr| {
            // Safety:: the caller ensures mutual exclusion to the field.
            match mem::replace(unsafe { &mut *ptr }, Stage::Consumed) {
                Stage::Finished(output) => output,
                _ => panic!("JoinHandle polled after completion"),
            }
        })
    }

    unsafe fn set_stage(&self, stage: Stage<T>) {
        let _guard = TaskIdGuard::enter(self.id);
        self.stage.with_mut(|ptr| *ptr = stage);
    }
}
