use crate::context::init_local_context;
use crate::runtime::{Builder, local};
use anyhow::Result;

pub(crate) fn init_local_runtime_and_context(builder: Option<Builder>) -> Result<local::Handle> {
    let builder = builder.unwrap_or(Builder::new_local());
    let handle = builder.try_build()?.expect_local_scheduler();

    init_local_context(&handle.cfg, handle.clone())?;

    Ok(handle)
}
