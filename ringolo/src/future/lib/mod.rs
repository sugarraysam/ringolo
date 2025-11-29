//! # Low-Level I/O Building Blocks
//!
//! This module contains the foundational primitives for constructing asynchronous
//! `io_uring` operations. It acts as a bridge between the raw, unsafe world of
//! **SQE Backends** and the safe, idiomatic world of high-level Runtime APIs
//! (like `fs` or `net`).
//!
//! ## For Runtime Developers
//!
//! **Target Audience:** This module is designed for **internal use** by runtime
//! maintainers. If you are building a new high-level API (e.g., a new socket type),
//! you will use the types defined here.
//!
//! ## Design Principles
//!
//! * **Ownership & RAII:** Payloads strictly take ownership of resources (buffers,
//!   paths, file descriptors). Upon completion, the `Output` returns ownership back
//!   to the caller. This prevents use-after-free errors common in async C bindings.
//!
//! * **Self-Referential Stability:** `io_uring` requires pointers to remain valid
//!   while the kernel holds them. We leverage [`Pin`](std::pin::Pin) and self-referential
//!   structs to guarantee argument stability without heap allocation overhead.
//!
//! * **Idiomatic Abstractions:** We abstract away `libc` structs (`timespec`,
//!   `sockaddr`) in favor of Rust standard types (`Duration`, `SocketAddr`).
//!

use crate::sqe::{CqeRes, IoError, Sqe, SqeSingle, SqeStream};
use futures::Stream;
use io_uring::squeue::Entry;
use pin_project::{pin_project, pinned_drop};
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll, Waker, ready};

pub(crate) mod errors;
pub(crate) use errors::OpcodeError;

mod buffer;
#[doc(inline)]
pub use buffer::{BufferFamily, KernelBufferMode, UringBuffer};

#[macro_use]
mod fd;
#[doc(inline)]
pub use fd::{BorrowedUringFd, KernelFdMode, OwnedUringFd, UringFdKind};

/// Convenience macros to work with `OpList`.
#[macro_use]
#[doc(hidden)]
pub mod list_macros;
#[doc(inline)]
pub use crate::{any_extract, any_extract_all, any_vec};

#[doc(hidden)]
pub mod list;
#[doc(inline)]
pub use list::{AnyOp, AnyOpOutput, OpList};

pub mod ops;

pub(super) mod parse;

/// Helper types to interact with `io_uring` and kernel interfaces.
pub mod types;

/// Traits and structs for one-shot `io_uring` operation.
///
/// Implement this trait to define how a high-level struct (like `OpenAt`)
/// translates into a raw `io_uring` Submission Queue Entry (SQE).
pub trait OpPayload {
    /// The result type produced when the operation completes (e.g., `i32`, `OwnedUringFd`).
    type Output;

    /// Constructs the raw `io_uring` entry.
    ///
    /// This is called when the `Op` is first polled. The `self` is pinned, ensuring
    /// that any pointers derived from `self` (like buffer pointers) remain valid
    /// until the future is dropped.
    fn create_entry(self: Pin<&mut Self>) -> Result<Entry, OpcodeError>;

    /// Transforms the raw kernel result into the high-level `Output`.
    ///
    /// The `waker` is passed to allow reconstructing the `Task` context if needed
    /// (e.g., for resource tracking).
    ///
    // Safety: This method should only be called once, when the operation is complete.
    // It is allowed to "consume the pin" so we can return owned data without copies.
    fn into_output(
        self: Pin<&mut Self>,
        waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Output, IoError>;
}

/// A generic Future that drives an `OpPayload`.
///
/// This wrapper manages the lifecycle of the operation:
/// 1.  **Initialization:** Calls `create_entry` and submits to the ring.
/// 2.  **Pinning:** Holds the data in place so the kernel can access it safely.
/// 3.  **Completion:** Awaits the CQE and transforms the result via `into_output`.
#[pin_project(PinnedDrop)]
#[derive(Debug)]
pub struct Op<T: OpPayload> {
    #[pin]
    data: T,

    #[pin]
    backend: MaybeUninit<Sqe<SqeSingle>>,

    initialized: bool,
    dropped: bool,
}

impl<T: OpPayload + Clone> Clone for Op<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            backend: MaybeUninit::uninit(),
            initialized: false,
            dropped: false,
        }
    }
}

impl<T: OpPayload> Op<T> {
    #[allow(unused)]
    pub(crate) fn new(data: T) -> Self {
        Self {
            data,
            backend: MaybeUninit::uninit(),
            initialized: false,
            dropped: false,
        }
    }
}

impl<T: OpPayload> Future for Op<T> {
    type Output = Result<T::Output, IoError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        if !*this.initialized {
            let entry = this.data.as_mut().create_entry()?;
            let backend = Sqe::new(SqeSingle::new(entry));
            this.backend.write(backend);
            *this.initialized = true;
        }

        // Safety: The logic above guarantees that `this.backend` has been initialized.
        let backend = Pin::new(unsafe { this.backend.assume_init_mut() });

        let res = ready!(backend.poll(cx));
        let output = this.data.as_mut().into_output(cx.waker(), res);
        Poll::Ready(output)
    }
}

#[pinned_drop]
impl<T: OpPayload> PinnedDrop for Op<T> {
    fn drop(mut self: Pin<&mut Self>) {
        let mut this = self.project();
        let dropped = std::mem::replace(this.dropped, true);

        // We must manually drop the Sqe<SqeSingle> if it was initialized. Make
        // sure we invoke the backend destructor only once with the dropped flag.
        if *this.initialized && !dropped {
            // SAFETY: We know the backend was initialized and has not been dropped.
            unsafe { this.backend.assume_init_drop() };
        }
    }
}

/// Specifies the mechanism used to cancel an in-flight operation.
#[derive(Debug)]
pub enum CancelHow {
    /// Cancels the operation using [`IORING_OP_ASYNC_CANCEL`](https://man7.org/linux/man-pages/man3/io_uring_prep_cancel.3.html)
    ///
    /// This is the standard cancellation method for almost all I/O operations
    /// (files, sockets, `poll_add`, etc.). It attempts to locate the request
    /// in the ring by its `user_data` and cancel it.
    AsyncCancel,

    /// Cancels the operation using [`IORING_OP_TIMEOUT_REMOVE`](https://man.archlinux.org/man/io_uring_prep_timeout_remove.3.en).
    ///
    /// This is required specifically for operations submitted via `IORING_OP_TIMEOUT`.
    /// Attempting to cancel a native `io_uring` timer with `ASYNC_CANCEL` is incorrect
    /// and will not work as intended.
    TimeoutRemove,
}

/// Traits and structs for multishot `io_uring` operations (i.e.: SqeStream).
///
/// Unlike [`OpPayload`], which maps 1 SQE to 1 CQE, a `MultishotPayload`
/// maps **1 SQE to N CQEs**. This is used for operations like [`io_uring_prep_recv_multishot`]
/// or repeated timer expirations.
///
/// Implementors of this trait define how to construct the initial request and how to
/// parse the stream of completion results coming back from the kernel.
///
/// [`io_uring_prep_recv_multishot`]: https://man7.org/linux/man-pages/man3/io_uring_prep_recv_multishot.3.html
pub trait MultishotPayload {
    /// The type of each item yielded by the stream.
    type Item;

    /// Determines the cancellation strategy for this operation.
    fn cancel_how(&self) -> CancelHow;

    /// Constructs the parameters for the initial `io_uring` submission.
    ///
    /// This returns the raw `Entry` to be submitted and the expected completion count.
    /// This is called exactly once when the `Multishot` stream is first polled.
    fn create_params(self: Pin<&mut Self>) -> Result<MultishotParams, OpcodeError>;

    /// Transforms a raw kernel result (CQE `res` field) into the high-level `Item`.
    ///
    /// # Arguments
    ///
    /// * `waker` - The waker from the current task context. This is provided so that
    ///   the payload can facilitate complex state tracking if necessary.
    /// * `result` - The `res` field from the completion queue entry.
    fn into_next(
        self: Pin<&mut Self>,
        waker: &Waker,
        result: Result<CqeRes, IoError>,
    ) -> Result<Self::Item, IoError>;
}

/// Configuration parameters for initializing a multishot operation.
#[derive(Debug)]
pub struct MultishotParams {
    /// The raw Submission Queue Entry (SQE) to be submitted to the ring.
    pub entry: Entry,

    /// The number of expected completions for this operation.
    ///
    /// * `count == 0`: **Infinite Multishot**. The operation remains active in the kernel
    ///   until explicitly cancelled or an error occurs (e.g., `multishot accept`).
    /// * `count > 0`: **Fixed Multishot**. The operation is expected to produce exactly
    ///   `n` completions before terminating automatically.
    pub count: u32,
}

impl MultishotParams {
    pub(crate) fn new(entry: Entry, count: u32) -> Self {
        Self { entry, count }
    }
}

/// A generic `Stream` that drives a [`MultishotPayload`].
///
/// This wrapper manages the lifecycle of a persistent `io_uring` operation:
/// 1. **Initialization**: Submits the SQE on the first poll.
/// 2. **Streaming**: Yields items as CQEs arrive.
/// 3. **Cancellation**: Automatically cancels the operation in the kernel when dropped.
///
/// # Pinning
/// This struct is pinned to ensure that any buffers or resources owned by the `data`
/// payload remain at a stable memory address for the duration of the kernel operation.
#[derive(Debug)]
#[pin_project(PinnedDrop)]
pub struct Multishot<T: MultishotPayload> {
    #[pin]
    data: T,

    #[pin]
    backend: MaybeUninit<SqeStream>,

    initialized: bool,
    cancelled: bool,
}

impl<T: MultishotPayload> Multishot<T> {
    #[allow(unused)]
    pub(crate) fn new(data: T) -> Self {
        Self {
            data,
            backend: MaybeUninit::uninit(),
            initialized: false,
            cancelled: false,
        }
    }

    pub(crate) fn cancel(self: Pin<&mut Self>) -> Option<usize> {
        let mut this = self.project();

        let cancelled = std::mem::replace(this.cancelled, true);
        if cancelled || !*this.initialized {
            // Nothing to cancel.
            return None;
        }

        // Safety: The logic above guarantees that `this.backend` has been initialized.
        let mut backend = Pin::new(unsafe { this.backend.assume_init_mut() });

        let user_data = match backend.cancel() {
            Some(idx) => idx,
            None => {
                // Nothing to cancel.
                return None;
            }
        };

        match this.data.cancel_how() {
            CancelHow::AsyncCancel => {
                let builder = io_uring::types::CancelBuilder::user_data(user_data as u64);
                crate::async_cancel(builder, user_data);
            }
            CancelHow::TimeoutRemove => {
                crate::async_timeout_remove(user_data);
            }
        }

        Some(user_data)
    }
}

impl<T: MultishotPayload> Stream for Multishot<T> {
    type Item = Result<T::Item, IoError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if !*this.initialized {
            let params = this.data.as_mut().create_params()?;
            let backend = SqeStream::new(params.entry, params.count);

            this.backend.write(backend);
            *this.initialized = true;
        }

        // Safety: The logic above guarantees that `this.backend` has been initialized.
        let backend = Pin::new(unsafe { this.backend.assume_init_mut() });

        match ready!(backend.poll_next(cx)) {
            Some(res) => {
                let next = this.data.as_mut().into_next(cx.waker(), res);
                Poll::Ready(Some(next))
            }
            None => Poll::Ready(None),
        }
    }
}

#[pinned_drop]
impl<T: MultishotPayload> PinnedDrop for Multishot<T> {
    fn drop(self: Pin<&mut Self>) {
        // CancelTask is responsible for dropping the backend. It is async in
        // nature and we can't block and wait for the result here. The task handles
        // errors and retries internally. It is also safe to drop the JoinHandle,
        // the task will continue in the background and it's result will be lost.
        let should_cancel = self.cancel();
        eprintln!(
            "Multishot responsible to cancel RawSqe: {:?}",
            should_cancel
        );
    }
}

#[cfg(test)]
mod tests {
    use super::ops::AsyncCancel;
    use super::*;
    use crate::test_utils::*;
    use anyhow::Result;
    use core::panic;
    use std::pin::pin;

    #[test]
    fn test_op_double_drop_no_undefined_behavior() -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        let op = Op::new(AsyncCancel::new(
            io_uring::types::CancelBuilder::user_data(42).all(),
        ));

        let (mock_waker, _) = mock_waker();
        let mut cx = Context::from_waker(&mock_waker);

        // Poll once to initialize the backend
        let mut op_fut = pin!(op);
        assert!(matches!(op_fut.as_mut().poll(&mut cx), Poll::Pending));

        let res = std::panic::catch_unwind(panic::AssertUnwindSafe(|| {
            unsafe {
                // Drop is idempotent
                let ptr = op_fut.get_unchecked_mut();
                std::ptr::drop_in_place(ptr);
                std::ptr::drop_in_place(ptr);
            }
        }));
        assert!(res.is_ok());

        Ok(())
    }
}
