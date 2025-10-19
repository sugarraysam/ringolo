//! A collection of safe, idiomatic `OpPayload` and `OpStream` implementations
//! for `io_uring`.
//!
//! ## Design Principles
//!
//! - **Ownership:** Payloads take ownership of resources (like buffers or paths).
//!   Upon completion, the `Output` of the operation returns ownership of the
//!   resource to the caller. For example, a `Read` operation returns `(buffer, bytes_read)`.
//!
//! - **Safety:** The public API for each operation is safe. Constructors take safe
//!   Rust types (`&Path`, `Vec<u8>`, `Duration`, etc.) and handle conversions
//!   to the raw C types required by the kernel internally. We leverage self-referential
//!   structs and pinning to guarantee argument stability until submit and/or complete.
//!   This is enforced by the rust compiler through Pin.
//!
//! - **Self-Reference:** Operations that require stable pointers to their own data
//!   (e.g., `Timeout`) store that data directly. The `OpPayload` trait's design
//!   enables the safe creation of these pointers when the operation is first polled.
//!
//! - **Idiomatic Rust:** We want to abstract away working with `libc::*` structs
//!   and raw pointers. We expose `std::lib` types in the arguments and return
//!   `std::lib` types in the results.
//!
//! - **Don't do too much:** Let's not make opinionated decisions about how raw
//!   results should be parsed. This is meant to be a low-level building block
//!   library.
//!
use crate::runtime::CleanupTaskBuilder;
use crate::sqe::{IoError, Sqe, SqeSingle, SqeStream};
use crate::task::JoinHandle;
use futures::Stream;
use io_uring::squeue::Entry;
use pin_project::{pin_project, pinned_drop};
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

pub(crate) mod builder;

pub(crate) mod errors;
pub(crate) use errors::{OpcodeError, OwnershipError};

#[macro_use]
pub mod fd;
pub use fd::{KernelFdMode, UringFd};

pub mod multishot;
pub use multishot::TimeoutMultishot;

pub(super) mod parse;

pub mod single;
pub use single::*;

pub mod sockopt;
pub use sockopt::*;

// TODO:
// - single :: impl Nop
// - start with stream + single, ponder on List because few issues, builder + try_build
//   is a bit weird since we insert in Slab. Also, we would want to create a combination of N
//   self-referential structs, not re-invent the wheel. This is where the crazy recursive macro
//   could be useful, and then SqeList is only a constructor that accepts a Vec<Entry>, and try_build
//   still used. But need to change logic, how we set head idx and all.
// - remove old `opcodes.rs` /w macro

// <--- CHAINED OPCODES --->
// - Idea: chained stream? N single op followed by 1 stream?
//   - Socket + SetSocketOpt + Bind + AcceptMulti
// - Chains to create:
//   - Socket + SetSocketOpt + Bind -> RawFd
//   - Socket + SetSocketOpt + Connect -> RawFd

/// Traits and structs for one-shot `io_uring` operation (i.e.: SqeSingle).
pub(crate) trait OpPayload {
    type Output;

    fn create_params(self: Pin<&mut Self>) -> Result<OpParams, OpcodeError>;

    // Safety: This method should only be called once, when the operation is complete.
    // It is allowed to "consume the pin" so we can return owned data without copies.
    fn into_output(
        self: Pin<&mut Self>,
        result: Result<i32, IoError>,
    ) -> Result<Self::Output, IoError>;
}

#[derive(Debug)]
pub(crate) struct OpParams(Entry);

impl From<Entry> for OpParams {
    fn from(entry: Entry) -> Self {
        Self(entry)
    }
}

#[pin_project(PinnedDrop)]
#[derive(Debug)]
pub(crate) struct Op<T: OpPayload> {
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
            let params = this.data.as_mut().create_params()?;
            let backend = Sqe::new(SqeSingle::new(params.0));
            this.backend.write(backend);
            *this.initialized = true;
        }

        // Safety: The logic above guarantees that `this.backend` has been initialized.
        let backend = Pin::new(unsafe { this.backend.assume_init_mut() });

        let res = ready!(backend.poll(cx));
        let output = this.data.as_mut().into_output(res);
        Poll::Ready(output)
    }
}

#[pinned_drop]
impl<T: OpPayload> PinnedDrop for Op<T> {
    fn drop(mut self: Pin<&mut Self>) {
        let mut this = self.project();
        let dropped = std::mem::replace(this.dropped, true);

        // We must manually drop the Sqe if it was initialized. Make sure we invoke
        // the backend destructor only once with the dropped flag.
        if *this.initialized && !dropped {
            // SAFETY: We know the backend was initialized and has not been dropped.
            unsafe { this.backend.assume_init_drop() };
        }
    }
}

/// Traits and structs for multishot `io_uring` operations (i.e.: SqeStream).
pub(crate) trait MultishotPayload {
    type Item;

    fn create_params(self: Pin<&mut Self>) -> Result<MultishotParams, OpcodeError>;

    fn into_next(self: Pin<&mut Self>, result: Result<i32, IoError>)
    -> Result<Self::Item, IoError>;
}

#[derive(Debug)]
pub(crate) struct MultishotParams {
    pub(crate) entry: Entry,

    // count == 0 means infinite multishot
    // count == n means complete after n completions
    pub(crate) count: u32,
}

impl MultishotParams {
    pub(crate) fn new(entry: Entry, count: u32) -> Self {
        Self { entry, count }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct Multishot<T: MultishotPayload> {
    #[pin]
    data: T,

    #[pin]
    backend: MaybeUninit<SqeStream>,

    initialized: bool,
    cancelled: bool,
}

impl<T: MultishotPayload> Multishot<T> {
    pub(crate) fn new(data: T) -> Self {
        Self {
            data,
            backend: MaybeUninit::uninit(),
            initialized: false,
            cancelled: false,
        }
    }

    pub(crate) fn cancel(self: Pin<&mut Self>) -> Option<JoinHandle<()>> {
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

        // Using `.all()` will NOT return -ENOENT if we can't find the associated SQE with user_data.
        // Better to target a single SQE even if it is multishot.
        let builder = io_uring::types::CancelBuilder::user_data(user_data as u64);
        let task = CleanupTaskBuilder::new(AsyncCancel::new(builder)).with_slab_entry(user_data);

        Some(crate::runtime::spawn_cleanup(task))
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
                let next = this.data.as_mut().into_next(res);
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
        let _ = self.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use anyhow::Result;
    use core::panic;
    use std::pin::pin;

    #[test]
    fn test_op_double_drop_no_undefined_behavior() -> Result<()> {
        init_local_runtime_and_context(None)?;

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
