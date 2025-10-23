#[doc(inline)]
pub use ringolo_macros::main;

#[doc(inline)]
pub use ringolo_macros::test;

mod context;

mod future;
pub use future::experimental::time;

pub mod runtime;
pub use runtime::{block_on, cancel_all_children, cancel_all_leaf_children, spawn};

mod sqe;

pub mod task;

mod utils;

#[cfg(test)]
mod test_utils;
