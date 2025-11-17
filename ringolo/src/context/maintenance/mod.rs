pub(crate) mod cleanup;

mod queue;

pub(crate) mod task;

const DEFAULT_NUM_RETRIES: usize = 3;

/// Defines the runtime's policy for handling I/O cleanup failures.
///
/// Asynchronous cleanup operations (like closing a file or cancelling a timer)
/// can fail. This enum dictates how the runtime should react.
///
/// This policy is configured via [`Builder::on_cleanup_error`].
///
/// [`Builder::on_cleanup_error`]: crate::runtime::Builder::on_cleanup_error
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnCleanupError {
    /// Ignore the error and drop the cleanup operation.
    ///
    /// **Use with caution**, as this can lead to leaked kernel resources.
    Ignore,

    /// Panic the scheduler, stopping the thread and likely the runtime.
    Panic,

    /// Retry the operation a specified number of times.
    ///
    /// If the operation still fails after all retries, the runtime will panic.
    Retry(usize),
}

impl Default for OnCleanupError {
    /// The default policy is to retry `DEFAULT_NUM_RETRIES` times.
    fn default() -> Self {
        OnCleanupError::Retry(DEFAULT_NUM_RETRIES)
    }
}
