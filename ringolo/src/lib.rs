#[doc(inline)]
pub use ringolo_macros::main;

#[doc(inline)]
pub use ringolo_macros::test;

mod context;

mod future;
pub use future::experimental::time;

pub mod runtime;
pub use runtime::{block_on, spawn, spawn_builder};

pub use runtime::{
    recursive_cancel_all, recursive_cancel_all_metadata, recursive_cancel_all_orphans,
    recursive_cancel_any_metadata, recursive_cancel_leaf,
};

mod sqe;

pub mod task;

mod utils;

#[cfg(test)]
mod test_utils;
