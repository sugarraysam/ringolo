//! Implementation of a work stealing scheduler.

// Public API
pub mod scheduler;
pub use scheduler::Handle;

// Re-exports
pub mod context;
pub(crate) use context::Context;

mod pool;

mod root_worker;

pub(crate) use scheduler::Scheduler;

mod worker;

#[cfg(test)]
mod tests;
