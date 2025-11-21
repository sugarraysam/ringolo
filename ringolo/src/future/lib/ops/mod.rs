//! # `io_uring` Opcodes and Operations.
//!
//! This module serves as the catalog of supported `io_uring` operations, providing high-level,
//! safe wrappers around raw SQEs (Submission Queue Entries).
//!
//! # Operation Categories
//!
//! Operations are strictly divided into two categories based on their lifecycle and completion behavior:
//!
//! * **Single-shot Operations**: These map 1:1 with standard Rust [`Future`](std::future::Future)s.
//!   A single submission results in exactly one completion event from the kernel.
//!   Examples include [`OpenAt`], [`Close`], and [`Socket`].
//!
//! * **Multishot Operations**: These map to [`Stream`](futures::stream::Stream)s.
//!   A single submission remains active in the kernel, generating multiple completion events
//!   over time until explicitly cancelled. This is particularly efficient for high-throughput
//!   tasks like accepting connections. Examples include [`AcceptMultishot`].
//!
//! # Design Philosophy: Type Isolation
//!
//! A core design principle of this library is the strict isolation of third-party FFI types.
//!
//! **We explicitly do not expose types from `libc`, `rustix`, or `nix` in our public interface.**
//!
//! Instead, we define our own minimal kernel stubs and types (see [`future::lib::types`](crate::future::lib::types)).
//! This decision was made to:
//!
//! 1.  **Prevent Dependency Leakage**: Users should not need to import `libc` just to configure a socket.
//! 2.  **Ensure API Stability**: Breaking changes in low-level FFI crates do not propagate to our
//!     public API surface.
//! 3.  **Simplify Usage**: We provide idiomatic Rust enums and structs (e.g., [`SockFlag`](crate::future::lib::types::SockFlag))
//!     rather than forcing users to bitwise-OR raw integers.

// Inline both `multishot` and `single` ops to make them available at `ringolo::ops::*`,
// but keep `sockopt` in a separate module to avoid bloating the namespace with
// dozens of option structs.
#[doc(hidden)]
pub mod multishot;

#[doc(inline)]
pub use multishot::*;

#[doc(hidden)]
pub mod single;

#[doc(inline)]
pub use single::*;

pub mod sockopt;
