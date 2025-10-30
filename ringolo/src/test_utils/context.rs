use crate::context;
use crate::runtime::{local, Builder, Runtime};
use anyhow::Result;

#[must_use]
pub(crate) fn init_local_runtime_and_context(
    builder: Option<Builder>,
) -> Result<(Runtime, local::Handle)> {
    let builder = builder.unwrap_or(Builder::new_local());
    let runtime = builder.try_build()?;
    let handle = runtime.expect_local_scheduler();

    context::init_local_context(&handle.cfg, handle.clone())?;

    Ok((runtime, handle))
}
