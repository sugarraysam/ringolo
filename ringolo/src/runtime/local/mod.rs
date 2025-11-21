//! Implementation of a simple scheduler that only works on the current thread.
//! Mostly for testing and ensuring the rest of the crate works as expected.

// Public API
pub mod scheduler;
pub use scheduler::Handle;

// Re-exports
pub(crate) mod context;
pub(crate) use context::Context;

pub(crate) use scheduler::Scheduler;

#[cfg(test)]
mod tests;

pub(crate) mod worker;
