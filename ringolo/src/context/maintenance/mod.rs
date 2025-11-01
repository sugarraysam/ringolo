pub mod cleanup;

mod queue;

pub mod task;

const DEFAULT_NUM_RETRIES: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnCleanupError {
    Ignore,
    Panic,
    Retry(usize),
}

impl Default for OnCleanupError {
    fn default() -> Self {
        OnCleanupError::Retry(DEFAULT_NUM_RETRIES)
    }
}
