//! # Asynchronous Time Utilities
//!
//! This module provides time-based primitives (sleeping, intervals, yielding) designed
//! specifically for the `ringolo` runtime.
//!
//! ## Integration with `io_uring`
//!
//! Unlike [`std::thread::sleep`], which blocks the entire OS thread, these utilities
//! are fully asynchronous and non-blocking. They leverage the [`IORING_OP_TIMEOUT`]
//! opcode provided by the Linux kernel. The runtime only wakes up to process the
//! completion event when the timer expires.
//!
//! <div class="warning">
//!
//! **Warning: Scheduler Latency & Precision**
//!
//! These utilities are **not suitable for high-precision timers** (e.g., real-time audio).
//! The sleep duration represents the time until the kernel posts a completion event.
//! It **excludes** any runtime or OS scheduler latency required to actually wake
//! the task, which is inevitable in asynchronous systems.
//!
//! </div>
//!
//! ## Primitives
//!
//! * **[`Sleep`]:** Pause the current task for a specific duration.
//! * **[`Tick`]:** Create a stream of events at a fixed interval. This uses
//!   **multishot** timeouts for maximum efficiency (one syscall, multiple wakeups).
//! * **[`YieldNow`]:** Cooperatively yield execution back to the scheduler to
//!   prevent starvation of other tasks.
//!
//! [`IORING_OP_TIMEOUT`]: https://man.archlinux.org/man/io_uring_prep_timeout.3.en

mod sleep;
pub use sleep::Sleep;

mod tick;
pub use tick::Tick;

mod yield_now;
pub use yield_now::YieldNow;
