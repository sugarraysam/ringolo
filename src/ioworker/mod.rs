use anyhow::Result;
use async_trait::async_trait;

mod base;
pub use base::WorkerBase;

// This trait abstracts the core logic of an `IoWorker`. Each implementation's
// primary goal is to optimally manage the submission and completion of I/O operations
// using `io_uring`. While the `WorkerBase` provides the foundational capabilities,
// the `IoWorker` interface is where the specific operational logic is implemented.
#[async_trait]
pub trait IoWorker {
    async fn run(&mut self) -> Result<()>;
}
