#![allow(unused)]

use crate::task::{Header, Id, JoinError, RawTask, Result};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Waker};

/// An owned permission to join on a task (await its termination).
///
/// This can be thought of as the equivalent of [`std::thread::JoinHandle`]
/// for a Ringolo task rather than a thread. Note that the background task
/// associated with this `JoinHandle` started running immediately when you
/// called spawn, even if you have not yet awaited the `JoinHandle`.
///
/// A `JoinHandle` *detaches* the associated task when it is dropped, which
/// means that there is no longer any handle to the task, and no way to `join`
/// on it.
///
/// This `struct` is created by the [`task::spawn`] function.
///
/// # Cancel safety
///
/// The `&mut JoinHandle<T>` type is cancel safe.
///
/// If a `JoinHandle` is dropped, then the task continues running in the
/// background and its return value is lost.
///
/// # Examples
///
/// Creation from [`task::spawn`]:
///
/// ```no_run
/// use ringolo::task::JoinHandle;
///
/// # async fn doc() {
/// let join_handle: JoinHandle<_> = ringolo::spawn(async {
///     // some work here
/// });
/// # }
/// ```
///
/// The generic parameter `T` in `JoinHandle<T>` is the return type of the spawned task.
/// If the return value is an `i32`, the join handle has type `JoinHandle<i32>`:
///
/// ```no_run
/// use ringolo::task::JoinHandle;
///
/// # async fn doc() {
/// let join_handle: JoinHandle<i32> = ringolo::spawn(async {
///     5 + 3
/// });
/// # }
///
/// ```
///
/// If the task does not have a return value, the join handle has type `JoinHandle<()>`:
///
/// ```no_run
/// use ringolo::task::JoinHandle;
///
/// # async fn doc() {
/// let join_handle: JoinHandle<()> = ringolo::spawn(async {
///     println!("I return nothing.");
/// });
/// # }
/// ```
///
/// Note that `handle.await` doesn't give you the return type directly. It is wrapped in a
/// `Result` because panics in the spawned task are caught by Ringolo. The `?` operator has
/// to be double chained to extract the returned value:
///
/// ```
/// use ringolo::task::JoinHandle;
/// use std::io;
///
/// # #[ringolo::main(flavor = "local")]
/// # async fn main() -> io::Result<()> {
/// let join_handle: JoinHandle<Result<i32, io::Error>> = ringolo::spawn(async {
///     Ok(5 + 3)
/// });
///
/// let result = join_handle.await??;
/// assert_eq!(result, 8);
/// Ok(())
/// # }
/// ```
///
/// If the task panics, the error is a [`JoinError`] that contains the panic:
///
/// ```
/// # {
/// use ringolo::task::JoinHandle;
/// use std::io;
/// use std::panic;
///
/// #[ringolo::main]
/// async fn main() -> io::Result<()> {
///     let join_handle: JoinHandle<Result<i32, io::Error>> = ringolo::spawn(async {
///         panic!("boom");
///     });
///
///     let err = join_handle.await.unwrap_err();
///     assert!(err.is_panic());
///     Ok(())
/// }
/// # }
/// ```
/// Child being detached and outliving its parent:
///
/// ```no_run
/// # #[cfg(feature = "experimental")]
/// # {
/// use ringolo::time::Sleep;
/// use std::time::Duration;
///
/// # #[ringolo::main(flavor = "local")]
/// # async fn main() {
/// let original_task = ringolo::spawn(async {
///     let _detached_task = ringolo::spawn(async {
///         // Here we sleep to make sure that the first task returns before.
///         let res = Sleep::try_new(Duration::from_millis(10)).expect("failed to sleep").await;
///         assert!(res.is_ok());
///
///         // This will be called, even though the JoinHandle is dropped.
///         println!("♫ Still alive ♫");
///     });
/// });
///
/// original_task.await.expect("The task being joined has panicked");
/// println!("Original task is joined.");
///
/// // We make sure that the new task has time to run, before the main
/// // task returns.
///
/// let res = Sleep::try_new(Duration::from_millis(1000)).expect("failed to sleep").await;
/// assert!(res.is_ok());
/// # }
/// # } // End of cfg block
/// ```
///
/// [`task::spawn`]: crate::task::spawn()
/// [`std::thread::JoinHandle`]: std::thread::JoinHandle
/// [`JoinError`]: crate::task::JoinError
pub struct JoinHandle<T> {
    raw: RawTask,
    _p: PhantomData<T>,
}

unsafe impl<T: Send> Send for JoinHandle<T> {}
unsafe impl<T: Send> Sync for JoinHandle<T> {}

impl<T> UnwindSafe for JoinHandle<T> {}
impl<T> RefUnwindSafe for JoinHandle<T> {}

impl<T> JoinHandle<T> {
    pub(super) fn new(raw: RawTask) -> JoinHandle<T> {
        JoinHandle {
            raw,
            _p: PhantomData,
        }
    }

    /// Abort the task associated with the handle.
    ///
    /// Awaiting a cancelled task might complete as usual if the task was
    /// already completed at the time it was cancelled, but most likely it
    /// will fail with a [cancelled] `JoinError`.
    ///
    /// Be aware that tasks spawned using [`spawn_blocking`] cannot be aborted
    /// because they are not async. If you call `abort` on a `spawn_blocking`
    /// task, then this *will not have any effect*, and the task will continue
    /// running normally. The exception is if the task has not started running
    /// yet; in that case, calling `abort` may prevent the task from starting.
    ///
    /// ```
    /// # #[cfg(feature = "experimental")]
    /// # {
    /// use ringolo::task::JoinHandle;
    /// use ringolo::time::Sleep;
    /// use std::time::Duration;
    /// use anyhow::Result;
    ///
    /// # #[ringolo::main(flavor = "local")]
    /// # async fn main() -> Result<()> {
    /// let mut handles: Vec<JoinHandle<Result<bool>>> = Vec::new();
    ///
    /// handles.push(ringolo::spawn(async {
    ///     let res = Sleep::try_new(Duration::from_secs(10))?.await;
    ///     assert!(res.is_ok());
    ///     Ok(true)
    /// }));
    ///
    /// handles.push(ringolo::spawn(async {
    ///     let res = Sleep::try_new(Duration::from_secs(10))?.await;
    ///     assert!(res.is_ok());
    ///     Ok(false)
    /// }));
    ///
    /// for handle in &handles {
    ///     handle.abort();
    /// }
    ///
    /// for handle in handles {
    ///     assert!(handle.await.unwrap_err().is_cancelled());
    /// }
    ///
    /// Ok(())
    /// # }
    /// # } // End of cfg block
    /// ```
    ///
    /// [cancelled]: method@super::error::JoinError::is_cancelled
    pub fn abort(&self) {
        self.raw.remote_abort();
    }

    /// Checks if the task associated with this `JoinHandle` has finished.
    ///
    /// Please note that this method can return `false` even if [`abort`] has been
    /// called on the task. This is because the cancellation process may take
    /// some time, and this method does not return `true` until it has
    /// completed.
    ///
    /// ```no_run
    /// # #[cfg(feature = "experimental")]
    /// # {
    /// use ringolo::task::JoinHandle;
    /// use ringolo::time::Sleep;
    /// use std::time::Duration;
    /// use anyhow::Result;
    ///
    /// # #[ringolo::main(flavor = "local")]
    /// # async fn main() -> Result<()> {
    /// let handle1 = ringolo::spawn(async {
    ///     // do some stuff here
    /// });
    /// let handle2: JoinHandle<Result<()>> = ringolo::spawn(async {
    ///     // do some other stuff here
    ///     let res = Sleep::try_new(Duration::from_secs(10))?.await;
    ///     assert!(res.is_ok());
    ///     Ok(())
    /// });
    /// // Wait for the task to finish
    /// handle2.abort();
    /// let res = Sleep::try_new(Duration::from_secs(1))?.await;
    /// assert!(res.is_ok());
    /// assert!(handle1.is_finished());
    /// assert!(handle2.is_finished());
    ///
    /// Ok(())
    /// # }
    /// # } // End of cfg block
    /// ```
    /// [`abort`]: method@JoinHandle::abort
    pub fn is_finished(&self) -> bool {
        let state = self.raw.header().state.load();
        state.is_complete()
    }

    /// Set the waker that is notified when the task completes.
    pub(crate) fn set_join_waker(&mut self, waker: &Waker) {
        if self.raw.try_set_join_waker(waker) {
            // In this case the task has already completed. We wake the waker immediately.
            waker.wake_by_ref();
        }
    }

    /// Returns a new `AbortHandle` that can be used to remotely abort this task.
    ///
    /// Awaiting a task cancelled by the `AbortHandle` might complete as usual if the task was
    /// already completed at the time it was cancelled, but most likely it
    /// will fail with a [cancelled] `JoinError`.
    ///
    /// ```
    /// # #[cfg(feature = "experimental")]
    /// # {
    /// use ringolo::task::{AbortHandle, JoinHandle};
    /// use std::time::Duration;
    /// use ringolo::time::Sleep;
    /// use anyhow::Result;
    ///
    /// # #[ringolo::main(flavor = "local")]
    /// # async fn main() -> Result<()> {
    /// let mut handles: Vec<JoinHandle<Result<bool>>> = Vec::new();
    ///
    /// handles.push(ringolo::spawn(async {
    ///    let res = Sleep::try_new(Duration::from_secs(10))?.await;
    ///    assert!(res.is_ok());
    ///    Ok(true)
    /// }));
    ///
    /// handles.push(ringolo::spawn(async {
    ///    let res = Sleep::try_new(Duration::from_secs(10))?.await;
    ///    assert!(res.is_ok());
    ///    Ok(false)
    /// }));
    ///
    /// let abort_handles: Vec<AbortHandle> = handles.iter().map(|h| h.abort_handle()).collect();
    ///
    /// for handle in abort_handles {
    ///     handle.abort();
    /// }
    ///
    /// for handle in handles {
    ///     assert!(handle.await.unwrap_err().is_cancelled());
    /// }
    ///
    /// Ok(())
    /// # }
    /// # } // End of cfg block
    /// ```
    /// [cancelled]: method@super::error::JoinError::is_cancelled
    #[must_use = "abort handles do nothing unless `.abort` is called"]
    pub fn abort_handle(&self) -> super::AbortHandle {
        self.raw.ref_inc();
        super::AbortHandle::new(self.raw)
    }

    /// Returns a [task ID] that uniquely identifies this task relative to other
    /// currently spawned tasks.
    ///
    /// [task ID]: crate::task::Id
    pub fn id(&self) -> Id {
        // Safety: The header pointer is valid.
        unsafe { Header::get_task_node(self.raw.header_ptr()).id }
    }

    /// Tries to resolve the task immediately and return its output.
    ///
    /// This method polls the task **once**. If the task is complete, the result is returned.
    /// If the task is still pending, the `JoinHandle` is consumed, the task is detached
    /// (continues running in the background), and a [cancelled] `JoinError` is returned.
    ///
    /// This is useful for retrieving the result in synchronous contexts (e.g., `Drop`
    /// implementations) where awaiting is not possible.
    ///
    /// [cancelled]: method@super::error::JoinError::is_cancelled
    pub fn get_result(self) -> Result<T> {
        let id = self.id();
        let mut this = pin!(self);

        match this
            .as_mut()
            .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
        {
            Poll::Ready(res) => res,
            Poll::Pending => Err(JoinError::cancelled(id)),
        }
    }
}

impl<T> Unpin for JoinHandle<T> {}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut ret = Poll::Pending;

        // TODO: Task Budget
        // Keep track of task budget
        // let coop = ready!(crate::task::coop::poll_proceed(cx));

        // Try to read the task output. If the task is not yet complete, the
        // waker is stored and is notified once the task does complete.
        //
        // The function must go via the vtable, which requires erasing generic
        // types. To do this, the function "return" is placed on the stack
        // **before** calling the function and is passed into the function using
        // `*mut ()`.
        //
        // Safety:
        //
        // The type of `T` must match the task's output type.
        unsafe {
            self.raw
                .try_read_output(&mut ret as *mut _ as *mut (), cx.waker());
        }

        // TODO: Task Budget
        // if ret.is_ready() {
        //     coop.made_progress();
        // }

        ret
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        if self.raw.state().drop_join_handle_fast().is_ok() {
            return;
        }

        self.raw.drop_join_handle_slow();
    }
}

impl<T> fmt::Debug for JoinHandle<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Safety: The header pointer is valid.
        fmt.debug_struct("JoinHandle")
            .field("id", &self.id())
            .finish()
    }
}
