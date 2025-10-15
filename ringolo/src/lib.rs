#[doc(inline)]
pub use ringolo_macros::main;

#[doc(inline)]
pub use ringolo_macros::test;

mod context;

mod future;

pub mod runtime;
pub use runtime::{block_on, spawn};

mod sqe;

mod task;

mod utils;

#[cfg(test)]
mod test_utils;
