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
use crate::sqe::{IoError, Sqe, SqeSingle, SqeStream};
use futures::Stream;
use io_uring::squeue::Entry;
use pin_project::pin_project;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

pub(crate) mod builder;

pub(crate) mod common;
pub(crate) use common::Fd;

pub mod multishot;
pub use multishot::TimeoutMultishot;

pub(super) mod parse;
pub mod single;
pub use single::TimeoutOp;

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
pub trait OpPayload {
    type Output;

    fn create_params(self: Pin<&mut Self>) -> OpParams;

    // Safety: This method should only be called once, when the operation is complete.
    // It is allowed to "consume the pin" so we can return owned data without copies.
    fn into_output(self: Pin<&mut Self>, result: Result<i32, IoError>) -> Self::Output;
}

#[derive(Debug)]
pub struct OpParams(Entry);

impl From<Entry> for OpParams {
    fn from(entry: Entry) -> Self {
        Self(entry)
    }
}

#[pin_project]
pub struct Op<T: OpPayload> {
    #[pin]
    data: T,

    #[pin]
    backend: MaybeUninit<Sqe<SqeSingle>>,

    initialized: bool,
}

impl<T: OpPayload> Op<T> {
    pub fn new(data: T) -> Self {
        Self {
            data,
            backend: MaybeUninit::uninit(),
            initialized: false,
        }
    }
}

impl<T: OpPayload> Future for Op<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        if !*this.initialized {
            let params = this.data.as_mut().create_params();
            let backend = Sqe::new(SqeSingle::new(params.0));
            this.backend.write(backend);
            *this.initialized = true;
        }

        // Safety: The logic above guarantees that `this.backend` has been initialized.
        let backend = unsafe { Pin::new_unchecked(this.backend.assume_init_mut()) };

        let res = ready!(backend.poll(cx));
        let output = this.data.as_mut().into_output(res);
        Poll::Ready(output)
    }
}

/// Traits and structs for multishot `io_uring` operations (i.e.: SqeStream).
pub trait MultishotPayload {
    type Item;

    fn create_params(self: Pin<&mut Self>) -> MultishotParams;

    fn into_next(self: Pin<&mut Self>, result: Result<i32, IoError>) -> Self::Item;
}

#[derive(Debug)]
pub struct MultishotParams {
    pub entry: Entry,

    // count == 0 means infinite multishot
    // count == n means complete after n completions
    pub count: u32,
}

impl MultishotParams {
    pub fn new(entry: Entry, count: u32) -> Self {
        Self { entry, count }
    }
}

#[pin_project]
pub struct Multishot<T: MultishotPayload> {
    #[pin]
    data: T,

    #[pin]
    backend: MaybeUninit<SqeStream>,

    initialized: bool,
}

impl<T: MultishotPayload> Multishot<T> {
    pub fn new(data: T) -> Self {
        Self {
            data,
            backend: MaybeUninit::uninit(),
            initialized: false,
        }
    }

    pub fn cancel(self: Pin<&mut Self>) -> Option<usize> {
        // Nothing to cancel.
        if !self.initialized {
            return None;
        }

        // Safety: The logic above guarantees that `this.backend` has been initialized.
        let mut this = self.project();
        let mut backend = unsafe { Pin::new_unchecked(this.backend.assume_init_mut()) };
        backend.cancel()
    }
}

impl<T: MultishotPayload> Stream for Multishot<T> {
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if !*this.initialized {
            let params = this.data.as_mut().create_params();
            let backend = SqeStream::new(params.entry, params.count);

            this.backend.write(backend);
            *this.initialized = true;
        }

        // Safety: The logic above guarantees that `this.backend` has been initialized.
        let backend = unsafe { Pin::new_unchecked(this.backend.assume_init_mut()) };

        match ready!(backend.poll_next(cx)) {
            Some(res) => {
                let next = this.data.as_mut().into_next(res);
                Poll::Ready(Some(next))
            }
            None => Poll::Ready(None),
        }
    }
}
