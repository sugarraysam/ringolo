use crate::runtime::{PanicReason, Schedule, SchedulerPanic};
use crate::task::error::panic_payload_as_str;
use crate::task::layout::{Core, TaskLayout};
use crate::task::raw::can_read_output;
use crate::task::state::{State, TransitionToIdle, TransitionToRunning};
use crate::task::trailer::Trailer;
use crate::task::waker::waker_ref;
use crate::task::{Header, Id, JoinError, Notified, Task};
use crate::with_scheduler;

use std::future::Future;

use std::any::Any;
use std::mem;
use std::mem::ManuallyDrop;
use std::panic;
use std::ptr::NonNull;
use std::task::{Context, Poll, Waker};

/// Typed raw task handle.
pub(super) struct Harness<T: Future, S: 'static> {
    layout: NonNull<TaskLayout<T, S>>,
}

impl<T, S> Harness<T, S>
where
    T: Future,
    S: 'static,
{
    pub(super) unsafe fn from_raw(ptr: NonNull<Header>) -> Harness<T, S> {
        Harness {
            layout: ptr.cast::<TaskLayout<T, S>>(),
        }
    }

    fn header_ptr(&self) -> NonNull<Header> {
        self.layout.cast()
    }

    fn header(&self) -> &Header {
        unsafe { &*self.header_ptr().as_ptr() }
    }

    fn state(&self) -> &State {
        &self.header().state
    }

    fn trailer(&self) -> &Trailer {
        unsafe { &self.layout.as_ref().trailer }
    }

    fn core(&self) -> &Core<T, S> {
        unsafe { &self.layout.as_ref().core }
    }

    pub(super) fn drop_reference(self) {
        if self.state().ref_dec() {
            self.dealloc();
        }
    }

    pub(super) fn dealloc(self) {
        // Safety: The caller of this method just transitioned our ref-count to
        // zero, so it is our responsibility to release the allocation.
        //
        // We don't hold any references into the allocation at this point, but
        // it is possible for another thread to still hold a `&State` into the
        // allocation if that other thread has decremented its last ref-count,
        // but has not yet returned from the relevant method on `State`.
        //
        // However, the `State` type consists of just an `AtomicUsize`, and an
        // `AtomicUsize` wraps the entirety of its contents in an `UnsafeTaskLayout`.
        // As explained in the documentation for `UnsafeTaskLayout`, such references
        // are allowed to be dangling after their last use, even if the
        // reference has not yet gone out of scope.
        unsafe {
            drop(Box::from_raw(self.layout.as_ptr()));
        }
    }

    /// Creates a new task that holds its own ref-count.
    ///
    /// # Safety
    ///
    /// Any use of `self` after this call must ensure that a ref-count to the
    /// task holds the task alive until after the use of `self`. Passing the
    /// returned Task to any method on `self` is unsound if dropping the Task
    /// could drop `self` before the call on `self` returned.
    fn get_new_task(&self) -> Task<S> {
        // safety: The header is at the beginning of the layout, so this cast is
        // safe.
        unsafe { Task::from_raw(self.layout.cast()) }
    }
}

// Methods that use the Scheduler and require a stricter bound on S.
impl<T: Future, S: Schedule> Harness<T, S> {
    /// Polls the inner future. A ref-count is consumed.
    ///
    /// All necessary state checks and transitions are performed.
    /// Panics raised while polling the future are handled.
    pub(super) fn poll(self) {
        // We pass our ref-count to `poll_inner`.
        match self.poll_inner() {
            PollFuture::Notified => {
                // The `poll_inner` call has given us two ref-counts back.
                // We give one of them to a new task and schedule it.
                self.core()
                    .scheduler
                    .schedule(Notified::new(self.get_new_task()), None);

                // The remaining ref-count is now dropped. We kept the extra
                // ref-count until now to ensure that even if the `schedule`
                // call drops the provided task, the task isn't deallocated
                // before `schedule` returns.
                self.drop_reference();
            }
            PollFuture::Complete => {
                self.complete();
            }
            PollFuture::Dealloc => {
                self.dealloc();
            }
            PollFuture::Done => (),
            PollFuture::Panicked(payload) => {
                // TODO: we might need to decrement pending IO if we implement
                // panic handlers on the scheduler that ignore panics somehow.
                // For now we always panic so we dont care.
                self.complete();
                with_scheduler!(|s| {
                    s.unhandled_panic(payload);
                });
            }
        }
    }

    /// Polls the task and cancel it if necessary. This takes ownership of a
    /// ref-count.
    ///
    /// If the return value is Notified, the caller is given ownership of two
    /// ref-counts.
    ///
    /// If the return value is Complete, the caller is given ownership of a
    /// single ref-count, which should be passed on to `complete`.
    ///
    /// If the return value is `Dealloc`, then this call consumed the last
    /// ref-count and the caller should call `dealloc`.
    ///
    /// Otherwise the ref-count is consumed and the caller should not access
    /// `self` again.
    fn poll_inner(&self) -> PollFuture {
        match self.state().transition_to_running() {
            TransitionToRunning::Success => {
                // Separated to reduce LLVM codegen
                fn transition_result_to_poll_future(result: TransitionToIdle) -> PollFuture {
                    match result {
                        TransitionToIdle::Ok => PollFuture::Done,
                        TransitionToIdle::OkNotified => PollFuture::Notified,
                        TransitionToIdle::OkDealloc => PollFuture::Dealloc,
                        TransitionToIdle::Cancelled => PollFuture::Complete,
                    }
                }
                let header_ptr = self.header_ptr();
                let waker_ref = waker_ref::<S>(&header_ptr);
                let cx = Context::from_waker(&waker_ref);

                // Propagate panic to `poll` method as it can call `self.complete()` and
                // we can make the panic output available to the `JoinHandle`.
                if let Poll::Ready(panic_payload) = poll_future(self.core(), cx) {
                    if let Some(payload) = panic_payload {
                        return PollFuture::Panicked(payload);
                    } else {
                        return PollFuture::Complete;
                    }
                }

                let transition_res = self.state().transition_to_idle();
                if let TransitionToIdle::Cancelled = transition_res {
                    // The transition to idle failed because the task was
                    // cancelled during the poll.
                    self.cancel_task();
                }
                transition_result_to_poll_future(transition_res)
            }
            TransitionToRunning::Cancelled => {
                self.cancel_task();
                PollFuture::Complete
            }
            TransitionToRunning::Failed => PollFuture::Done,
            TransitionToRunning::Dealloc => PollFuture::Dealloc,
        }
    }

    /// Forcibly shuts down the task.
    ///
    /// Attempt to transition to `Running` in order to forcibly shutdown the
    /// task. If the task is currently running or in a state of completion, then
    /// there is nothing further to do. When the task completes running, it will
    /// notice the `CANCELLED` bit and finalize the task.
    pub(super) fn shutdown(self) {
        if !self.state().transition_to_shutdown() {
            // The task is concurrently running. No further work needed.
            self.drop_reference();
            return;
        }

        // By transitioning the lifecycle to `Running`, we have permission to
        // drop the future.
        self.cancel_task();
        self.complete();
    }

    // ===== join handle =====

    /// Read the task output into `dst`.
    pub(super) fn try_read_output(self, dst: &mut Poll<super::Result<T::Output>>, waker: &Waker) {
        if can_read_output(self.header(), self.trailer(), waker) {
            *dst = Poll::Ready(self.core().take_output());
        }
    }

    pub(super) fn drop_join_handle_slow(self) {
        // Try to unset `JOIN_INTEREST` and `JOIN_WAKER`. This must be done as a first step in
        // case the task concurrently completed.
        let transition = self.state().transition_to_join_handle_dropped();

        if transition.drop_output {
            // It is our responsibility to drop the output. This is critical as
            // the task output may not be `Send` and as such must remain with
            // the scheduler or `JoinHandle`. i.e. if the output remains in the
            // task structure until the task is deallocated, it may be dropped
            // by a Waker on any arbitrary thread.
            //
            // Panics are delivered to the user via the `JoinHandle`. Given that
            // they are dropping the `JoinHandle`, we assume they are not
            // interested in the panic and swallow it.
            _ = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                self.core().drop_future_or_output();
            }));
        }

        if transition.drop_waker {
            // If the JOIN_WAKER flag is unset at this point, the task is either
            // already terminal or not complete so the `JoinHandle` is responsible
            // for dropping the waker.
            // Safety:
            // If the JOIN_WAKER bit is not set the join handle has exclusive
            // access to the waker as per rule 2 in task/mod.rs.
            // This can only be the case at this point in two scenarios:
            // 1. The task completed and the runtime unset `JOIN_WAKER` flag
            //    after accessing the waker during task completion. So the
            //    `JoinHandle` is the only one to access the  join waker here.
            // 2. The task is not completed so the `JoinHandle` was able to unset
            //    `JOIN_WAKER` bit itself to get mutable access to the waker.
            //    The runtime will not access the waker when this flag is unset.
            unsafe { self.trailer().set_waker(None) };
        }

        // Drop the `JoinHandle` reference, possibly deallocating the task
        self.drop_reference();
    }

    // ====== internal ======

    /// Cancels the task and store the appropriate error in the stage field.
    fn cancel_task(&self) {
        let core = self.core();

        // Drop the future from a panic guard.
        let res = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            core.drop_future_or_output();
        }));

        core.store_output(Err(panic_result_to_join_error(core.task_node.id, res)));
    }

    /// Completes the task. This method assumes that the state is RUNNING.
    fn complete(self) {
        // The future has completed and its output has been written to the task
        // stage. We transition from running to complete.
        let snapshot = self.state().transition_to_complete();

        // We catch panics here in case dropping the future or waking the
        // JoinHandle panics.
        _ = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            if !snapshot.is_join_interested() {
                // The `JoinHandle` is not interested in the output of
                // this task. It is our responsibility to drop the
                // output. The join waker was already dropped by the
                // `JoinHandle` before.
                self.core().drop_future_or_output();
            } else if snapshot.is_join_waker_set() {
                // Notify the waker. Reading the waker field is safe per rule 4
                // in task/mod.rs, since the JOIN_WAKER bit is set and the call
                // to transition_to_complete() above set the COMPLETE bit.
                self.trailer().wake_join();

                // Inform the `JoinHandle` that we are done waking the waker by
                // unsetting the `JOIN_WAKER` bit. If the `JoinHandle` has
                // already been dropped and `JOIN_INTEREST` is unset, then we must
                // drop the waker ourselves.
                if !self
                    .state()
                    .unset_waker_after_complete()
                    .is_join_interested()
                {
                    // SAFETY: We have COMPLETE=1 and JOIN_INTEREST=0, so
                    // we have exclusive access to the waker.
                    unsafe { self.trailer().set_waker(None) };
                }
            }
        }));

        // The task has completed execution and will no longer be scheduled.
        let num_release = self.release();

        if self.state().transition_to_terminal(num_release) {
            self.dealloc();
        }
    }

    /// Releases the task from the scheduler. Returns the number of ref-counts
    /// that should be decremented.
    fn release(&self) -> usize {
        // We don't actually increment the ref-count here, but the new task is
        // never destroyed, so that's ok.
        let me = ManuallyDrop::new(self.get_new_task());

        let core = self.core();
        core.task_node.release();

        if let Some(task) = core.scheduler.release(&me) {
            mem::forget(task);
            2
        } else {
            1
        }
    }
}

enum PollFuture {
    Complete,
    Notified,
    Done,
    Dealloc,
    Panicked(SchedulerPanic),
}

fn panic_result_to_join_error(
    task_id: Id,
    res: Result<(), Box<dyn Any + Send + 'static>>,
) -> JoinError {
    match res {
        Ok(()) => JoinError::cancelled(task_id),
        Err(panic) => JoinError::panic(task_id, panic),
    }
}

/// Polls the future. If the future completes, the output is written to the
/// stage field.
fn poll_future<T: Future, S: Schedule>(
    core: &Core<T, S>,
    cx: Context<'_>,
) -> Poll<Option<SchedulerPanic>> {
    // Poll the future.
    let output = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        struct Guard<'a, T: Future, S: Schedule> {
            core: &'a Core<T, S>,
        }
        impl<'a, T: Future, S: Schedule> Drop for Guard<'a, T, S> {
            fn drop(&mut self) {
                // If the future panics on poll, we drop it inside the panic
                // guard.
                self.core.drop_future_or_output();
            }
        }
        let guard = Guard { core };
        let res = guard.core.poll(cx);
        mem::forget(guard);
        res
    }));

    // Prepare output for being placed in the core stage.
    let (scheduler_panic, output) = match output {
        Ok(Poll::Pending) => return Poll::Pending,
        Ok(Poll::Ready(output)) => (None, Ok(output)),
        Err(panic) => parse_panic::<T::Output>(core.task_node.id, panic),
    };

    // Store output for JoinHandle to reap before we panic.
    let res = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        core.store_output(output);
    }));

    let panic_payload = scheduler_panic.or_else(|| {
        res.is_err().then(|| {
            SchedulerPanic::new(
                PanicReason::StoringTaskOutput,
                "failed to store task output after poll",
            )
        })
    });

    Poll::Ready(panic_payload)
}

#[cold]
fn parse_panic<T>(
    task_id: Id,
    panic: Box<dyn Any + Send + 'static>,
) -> (Option<SchedulerPanic>, Result<T, JoinError>) {
    let scheduler_panic = {
        if let Some(payload) = panic.downcast_ref::<SchedulerPanic>() {
            payload.clone()
        } else {
            let msg = panic_payload_as_str(&panic);
            SchedulerPanic::new(PanicReason::Unknown, msg.unwrap_or("unknown").to_string())
        }
    };

    (Some(scheduler_panic), Err(JoinError::panic(task_id, panic)))
}
