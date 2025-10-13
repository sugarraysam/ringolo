use crate::future::opcode::builder::AsyncCancel2Builder;
use crate::{context::with_slab_mut, sqe::IoError};

pub type CancelBuilder = io_uring::types::CancelBuilder;

/// A task dedicated to performing an asynchronous cancellation.
///
/// This struct encapsulates all the logic for building the cancellation future,
/// handling its result, and cleaning up associated resources.
#[derive(Debug)]
pub struct CancellationTask {
    builder: CancelBuilder,
    user_data: usize,
}

impl CancellationTask {
    pub fn new(builder: CancelBuilder, user_data: usize) -> Self {
        Self { builder, user_data }
    }

    /// Executes the cancellation logic.
    ///
    /// This async function performs the following steps:
    /// 1. Builds and awaits the cancellation future.
    /// 2. Handles potential errors.
    /// 3. Cleans up the corresponding entry from the SQE slab.
    pub(crate) async fn run(self) {
        let cancel_fut = AsyncCancel2Builder::new(self.builder).build();

        // TODO: only remove after last retry
        // The cancellation task is now responsible for removing the RawSqe from the slab.
        with_slab_mut(|slab| {
            if slab.try_remove(self.user_data).is_none() {
                eprintln!(
                    "Warning: SQE {} not found in slab during cancellation cleanup.",
                    self.user_data
                );
            }
        });

        if let Err(err) = cancel_fut.await {
            match err {
                IoError::Io(e) if e.raw_os_error() == Some(libc::EALREADY) => {
                    // This is not a true error. The operation likely completed
                    // before we had a chance to cancel it. We can safely ignore this.
                }
                _ => {
                    // TODO: OnCancelError::{Ignore, Panic, Retry} integration
                    // - wrap CancelBuilder + user_data + handler in custom CancelTaskBuilder
                    // - CancellationTask -> CancelTask
                    // - CancelTaskBuilder needs to be clonable so we can retry
                    panic!(
                        "FATAL?: Failed to cancel operation (user_data: {}): {:?}",
                        self.user_data, err
                    );
                }
            }
        }
    }
}
