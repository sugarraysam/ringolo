#![allow(
    clippy::cognitive_complexity,
    clippy::large_enum_variant,
    clippy::module_inception,
    clippy::needless_doctest_main
)]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![deny(unused_must_use)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]

//! Ringolo is a completion-driven asynchronous runtime built on top of `io_uring`.
//! Its task system is derived from Tokio's battle-tested [task module], which
//! implements a state machine for managing a future's lifecycle.
//!
//! [task module]: https://docs.rs/tokio/latest/tokio/#working-with-tasks
//!
//! ## Quick Start
//!
//! ```no_run
//! #[ringolo::main]
//! async fn main() {
//!     println!("Hello from the runtime!");
//!
//!     let join_handle = ringolo::spawn(async {
//!         // ... do some async work ...
//!         "Hello from a spawned task!"
//!     });
//!
//!     let result = join_handle.await.unwrap();
//!     println!("{}", result);
//! }
//! ```
//!
//! ## Key features
//!
//! ### I/O Model: Readiness vs. Completion
//!
//! A key difference from Tokio is the underlying I/O model. Tokio is
//! **readiness-based**, typically using `epoll`. This `epoll` context is global
//! and accessible by all threads. Consequently, if a task is stolen by another
//! thread, readiness events remain valid.
//!
//! Ringolo is **completion-based**, using `io_uring`. This model is fundamentally
//! **thread-local**, as each worker thread manages its own ring. Many resources,
//! such as registered direct descriptors or provided buffers, are bound to that
//! specific ring and are invalid on any other thread.
//!
//! ### Work-Stealing and Thread-Local Resources
//!
//! This thread-local design presents a core challenge for work-stealing.
//! When an I/O operation is submitted on a thread's ring, its corresponding
//! completion event *must* be processed by the *same thread*.
//!
//! If a task were migrated to another thread *after* submission but *before*
//! completion, the resulting I/O event would be delivered to the original
//! thread's ring, but the task would no longer be there to process it. This
//! would lead to lost I/O and undefined behavior.
//!
//! Ringolo's work-stealing scheduler is designed around this constraint. It
//! performs resource and pending I/O accounting to determine when a task is
//! "stealable". View the detailed implementation within the [task header module].
//!
//! [task header module]: crate::task::Header::is_stealable
//!
//! ### Structured Concurrency
//!
//! Another key difference from Tokio is Ringolo's adoption of [Structured Concurrency].
//! While this can be an overloaded term, in Ringolo it provides a simple guarantee:
//! **tasks are not allowed to outlive their parent.**
//!
//! To enforce this, the runtime maintains a [global task tree] to track the task
//! hierarchy. When a parent task exits, all of its child tasks are automatically
//! cancelled.
//!
//! This behavior is controlled by the orphan policy. The default policy is
//! [`OrphanPolicy::Enforced`], which is the recommended setting for most
//! programs. This behavior can be relaxed in two ways:
//!
//! 1.  **Per-Task:** To create a single "detached" task, you can explicitly use
//!     [`TaskOpts::BACKGROUND_TASK`]. This option bypasses the current task's hierarchy
//!     by attaching the new task directly to the `ROOT_NODE` of the task tree.
//!
//! 2.  **Globally:** You can configure the entire runtime with [`OrphanPolicy::Permissive`].
//!     This setting effectively disables structured concurrency guarantees for
//!     all tasks, but it is not the intended model for typical use.
//!
//! [Structured Concurrency]: https://en.wikipedia.org/wiki/Structured_concurrency
//! [global task tree]: crate::task::node::TaskNode
//! [`OrphanPolicy::Enforced`]: crate::runtime::OrphanPolicy
//! [`TaskOpts::BACKGROUND_TASK`]: crate::runtime::spawn::TaskOpts
//! [`OrphanPolicy::Permissive`]: crate::runtime::OrphanPolicy
//!
//! #### Motivation: Safer Cancellation APIs
//!
//! The primary motivation for this design is to provide powerful and safe
//! cancellation APIs.
//!
//! Experience with other asynchronous frameworks, including Tokio and [folly/coro],
//! shows that relying on [cancellation tokens] has significant drawbacks.
//! Manually passing tokens is error-prone, introduces code bloat, and becomes
//! exceptionally difficult to manage correctly in a large codebase.
//!
//! Ringolo's design provides a user-friendly way to perform cancellation
//! *without* requiring tokens to be passed throughout the call stack.
//! The global task tree enables this robust, built-in cancellation model.
//!
//! For a detailed guide, please see the [cancellation APIs].
//!
//! [folly/coro]: https://github.com/facebook/folly/tree/main/folly/experimental/coro
//! [cancellation tokens]: https://github.com/facebook/folly/blob/main/folly/CancellationToken.h#L50-L68
//! [cancellation APIs]: crate::runtime::cancel
//!
//! ### Async cleanup
//!
//! The thread-local design of `io_uring` also dictates the model for resource
//! cleanup. Certain "leaky" operations, like [multishot timers], and
//! thread-local resources, like [direct descriptors], must be explicitly
//! unregistered or closed.
//!
//! This cleanup **must** happen on the same thread's ring that created them.
//! To solve this without blocking on `drop`, Ringolo uses a [maintenance task]
//! on each worker thread. When a future is dropped, it enqueues an async
//! cleanup operation with its local maintenance task. This task then batches
//! and submits these operations, ensuring all resources are freed on the correct
//! thread.
//!
//! The runtime's behavior on a failed cleanup operation is controlled by
//! the [OnCleanupError] policy.
//!
//! [multishot timers]: crate::future::lib::TimeoutMultishot
//! [direct descriptors]: crate::future::lib::UringFdKind::Fixed
//! [maintenance task]: crate::context::maintenance::task::MaintenanceTask
//! [OnCleanupError]: crate::context::maintenance::OnCleanupError

#[doc(inline)]
pub use ringolo_macros::main;

#[doc(inline)]
pub use ringolo_macros::test;

/// Thread-local and shared context for the runtime workers.
mod context;
#[doc(inline)]
pub use context::maintenance::cleanup::{async_cancel, async_close, async_timeout_remove};

/// User-facing set of libraries exposing native futures for the runtime.
pub mod future;
#[doc(inline)]
#[cfg(any(test, feature = "experimental"))]
pub use future::experimental::time;

/// Builder API to configure and create a runtime.
pub mod runtime;
#[doc(inline)]
pub use runtime::{TaskMetadata, TaskOpts, block_on, spawn, spawn_builder};

#[doc(inline)]
pub use runtime::{
    recursive_cancel_all, recursive_cancel_all_leaves, recursive_cancel_all_metadata,
    recursive_cancel_all_orphans, recursive_cancel_any_metadata,
};

/// Internal SQE backends for submitting and completing I/O with `io_uring`.
mod sqe;

/// Internal implementation of the task state machine that manages a future's lifecycle.
pub mod task;

#[doc(hidden)]
mod utils;

#[cfg(test)]
mod test_utils;
