#[doc(inline)]
pub use ringolo_macros::main;

#[doc(inline)]
pub use ringolo_macros::test;

pub mod context;

pub mod future;

pub mod runtime;
pub use runtime::{block_on, spawn, spawn_cancel};

pub mod sqe;

#[allow(dead_code, unused)]
pub mod task;

pub mod utils;

#[cfg(test)]
pub mod test_utils;

// TODO: impl with ringolo-console
// #[allow(dead_code, unused)]
// pub mod protocol;
