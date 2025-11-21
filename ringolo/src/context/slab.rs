use crate::runtime::SPILL_TO_HEAP_THRESHOLD;
use crate::sqe::errors::IoError;
use crate::sqe::raw::RawSqe;
use anyhow::anyhow;
use slab::{Slab, VacantEntry};
use smallvec::SmallVec;
use std::io::{self, Error, ErrorKind};
use std::ops::{Deref, DerefMut};

/// A dedicated slab allocator for `io_uring` Submission Queue Entries (SQEs).
///
/// This structure manages the lifecycle of raw pointers used in I/O requests.
/// It utilizes a "Reserve-Commit" pattern to ensure that entries are only
/// permanently occupied if the I/O submission logic succeeds.
pub(crate) struct RawSqeSlab {
    slab: slab::Slab<RawSqe>,
}

impl RawSqeSlab {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            slab: Slab::with_capacity(capacity),
        }
    }

    /// Reserve a single slab entry for insertion. Insertion is a 2-step process
    /// where we first reserve the entry, but need to insert the entry to commit.
    pub(crate) fn reserve_entry(&'_ mut self) -> Result<SlabReservedEntry<'_>, IoError> {
        if self.slab.len() == self.slab.capacity() {
            return Err(IoError::SlabFull);
        }

        let v = self.slab.vacant_entry();
        Ok(SlabReservedEntry::new(v))
    }

    /// Reserve a batch of N slab entries for insertion. Insertion is a 2-step process
    /// where we first reserve the batch, but need to insert the entries to commit.
    pub(crate) fn reserve_batch(
        &'_ mut self,
        size: usize,
    ) -> Result<SlabReservedBatch<'_>, IoError> {
        if self.slab.len() + size > self.slab.capacity() {
            return Err(IoError::SlabFull);
        }

        // We do a tricky thing as we *can't* borrow mut N vacant entries at once. Instead
        // we insert dummy RawSqe in the slab and collect indices. We will later replace them
        // with the user entries OR cleanup upon exit if we did not commit the operation.
        let indices = (0..size)
            .map(|_| self.slab.insert(RawSqe::default()))
            .collect::<SmallVec<_>>();

        Ok(SlabReservedBatch::new(indices, self))
    }

    pub(crate) fn get(&self, key: usize) -> io::Result<&RawSqe> {
        self.slab.get(key).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("Key {:?} not found in slab.", key),
            )
        })
    }

    pub(crate) fn get_mut(&mut self, key: usize) -> io::Result<&mut RawSqe> {
        self.slab.get_mut(key).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("Key {:?} not found in slab.", key),
            )
        })
    }
}

#[doc(hidden)]
impl Deref for RawSqeSlab {
    type Target = Slab<RawSqe>;

    fn deref(&self) -> &Self::Target {
        &self.slab
    }
}

#[doc(hidden)]
impl DerefMut for RawSqeSlab {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slab
    }
}

/// A wrapper around slab::VacantEntry that increments a counter upon insertion.
pub(crate) struct SlabReservedEntry<'a> {
    entry: VacantEntry<'a, RawSqe>,
}

impl<'a> SlabReservedEntry<'a> {
    /// Creates a new SlabReservedEntry.
    fn new(entry: slab::VacantEntry<'a, RawSqe>) -> Self {
        Self { entry }
    }

    /// Gets the key that will be used for the next insertion.
    pub(crate) fn key(&self) -> usize {
        self.entry.key()
    }

    /// Inserts a value into the slab. This consumes the entry, just like the
    /// original `insert` method.
    pub(crate) fn commit(self, value: RawSqe) -> &'a mut RawSqe {
        self.entry.insert(value)
    }
}

/// Reserve a batch of N slab entries for insertion. Insertion is a 2-step process
/// where we first reserve the batch, but need to insert the entries to commit.
pub(crate) struct SlabReservedBatch<'a> {
    indices: SmallVec<[usize; SPILL_TO_HEAP_THRESHOLD]>,
    slab: &'a mut RawSqeSlab,
    committed: bool,
}

impl<'a> SlabReservedBatch<'a> {
    fn new(indices: SmallVec<[usize; SPILL_TO_HEAP_THRESHOLD]>, slab: &'a mut RawSqeSlab) -> Self {
        Self {
            indices,
            slab,
            committed: false,
        }
    }

    pub(crate) fn keys(&self) -> SmallVec<[usize; SPILL_TO_HEAP_THRESHOLD]> {
        self.indices.clone()
    }

    pub(crate) fn commit(
        mut self,
        values: SmallVec<[RawSqe; SPILL_TO_HEAP_THRESHOLD]>,
    ) -> Result<(), IoError> {
        if values.len() != self.indices.len() {
            return Err(anyhow!(
                "You need to insert *exactly* {} values, got {}.",
                self.indices.len(),
                values.len()
            )
            .into());
        }

        for (idx, entry) in self.indices.iter().zip(values.into_iter()) {
            let placeholder = self
                .slab
                .get_mut(*idx)
                .map_err(|_| IoError::SlabInvalidState)?;

            *placeholder = entry;
        }

        self.committed = true;
        Ok(())
    }
}

impl Drop for SlabReservedBatch<'_> {
    fn drop(&mut self) {
        // If we did not commit the batch, release the reserved entries.
        if !self.committed {
            // SAFETY: we know the indices are valid so should not panic.
            self.indices.iter().for_each(|idx| {
                self.slab.remove(*idx);
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context;
    use crate::sqe::raw::CompletionHandler;
    use crate::test_utils::*;
    use anyhow::Result;

    #[test]
    fn test_new_and_capacity() {
        let capacity = 5;
        let slab = RawSqeSlab::new(capacity);
        assert_eq!(slab.slab.capacity(), capacity);
        assert_eq!(slab.slab.len(), 0);
    }

    #[test]
    fn test_insert_and_len() -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        let n_sqes = 3;
        let mut slab = RawSqeSlab::new(n_sqes);

        for i in 1..=n_sqes {
            let reserved = slab.reserve_entry()?;
            let idx = reserved.key();
            reserved.commit(RawSqe::new(CompletionHandler::new_single()));

            assert!(slab.get(idx).is_ok());
            assert_eq!(slab.len(), i);
        }

        Ok(())
    }

    #[test]
    fn test_slab_full() -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        let n_sqes = 3;
        let mut slab = RawSqeSlab::new(n_sqes - 1);

        for i in 1..=n_sqes {
            let res = slab.reserve_entry().map(|reserved| {
                reserved.commit(RawSqe::new(CompletionHandler::new_single()));
            });

            if i == n_sqes {
                assert!(res.is_err());
            } else {
                assert!(res.is_ok());
            }
        }

        Ok(())
    }

    #[test]
    fn test_reserve_batch() -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        context::with_slab_mut(|slab| -> Result<()> {
            let n_sqes = 16;

            {
                let batch = slab.reserve_batch(n_sqes)?;

                // Slab entries are reserved but *not committed*
                assert_eq!(batch.slab.len(), n_sqes);
                assert_eq!(batch.keys().len(), n_sqes);
            }

            // Batch was not committed, so slab entries are released.
            assert_eq!(slab.len(), 0);

            {
                let batch = slab.reserve_batch(n_sqes)?;

                // Slab entries are reserved but *not committed*
                assert_eq!(batch.slab.len(), n_sqes);
                assert_eq!(batch.keys().len(), n_sqes);

                // Commit the batch
                let entries = (0..n_sqes)
                    .map(|_| RawSqe::new(CompletionHandler::new_single()))
                    .collect::<SmallVec<[_; SPILL_TO_HEAP_THRESHOLD]>>();

                batch.commit(entries)?;
            } // Batch was committed, nothing happens.

            assert_eq!(slab.len(), n_sqes);

            Ok(())
        })
    }
}
