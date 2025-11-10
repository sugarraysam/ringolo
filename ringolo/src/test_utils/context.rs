use crate::runtime::{Builder, Runtime, local, stealing};
use anyhow::Result;

#[must_use]
pub(crate) fn init_local_runtime_and_context(
    builder: Option<Builder>,
) -> Result<(Runtime, local::Handle)> {
    let builder = builder.unwrap_or(Builder::new_local());
    let runtime = builder.try_build()?;
    let handle = runtime.expect_local_scheduler();

    Ok((runtime, handle))
}

#[must_use]
pub(crate) fn init_stealing_runtime_and_context(
    worker_threads: usize,
    builder: Option<Builder>,
) -> Result<(Runtime, stealing::Handle)> {
    let builder = builder
        .unwrap_or(Builder::new_stealing())
        .worker_threads(worker_threads);

    let runtime = builder.try_build()?;
    let handle = runtime.expect_stealing_scheduler();

    Ok((runtime, handle))
}
