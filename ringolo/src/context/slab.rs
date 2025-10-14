use crate::runtime::SPILL_TO_HEAP_THRESHOLD;
use crate::sqe::errors::IoError;
use crate::sqe::raw::RawSqe;
use anyhow::{Context, anyhow};
use slab::{Slab, VacantEntry};
use smallvec::SmallVec;
use std::io::{self, Error, ErrorKind};
use std::ops::{Deref, DerefMut};

pub(crate) struct RawSqeSlab {
    slab: slab::Slab<RawSqe>,

    // Keep track of number of pending IO operations. We can't rely on slab size
    // as RawSqe's are kept alive *after completion* so we can consume result in
    // after waking up the future.
    pub(crate) pending_ios: usize,
}

impl RawSqeSlab {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            slab: Slab::with_capacity(capacity),

            // Pending ios is used by local worker as a signal of "is there more work to do". If
            // there are no pending ios, then we are NOT expecting any more CQE, and we can decide
            // to park the thread or poll the root future.
            pending_ios: 0,
        }
    }

    /// Reserve a single slab entry for insertion. Insertion is a 2-step process
    /// where we first reserve the entry, but need to insert the entry to commit.
    pub(crate) fn reserve_entry(&'_ mut self) -> Result<SlabReservedEntry<'_>, IoError> {
        if self.slab.len() == self.slab.capacity() {
            return Err(IoError::SlabFull);
        }

        let v = self.slab.vacant_entry();
        Ok(SlabReservedEntry::new(v, &mut self.pending_ios))
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

impl Deref for RawSqeSlab {
    type Target = Slab<RawSqe>;

    fn deref(&self) -> &Self::Target {
        &self.slab
    }
}

impl DerefMut for RawSqeSlab {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slab
    }
}

/// A wrapper around slab::VacantEntry that increments a counter upon insertion.
pub(crate) struct SlabReservedEntry<'a> {
    entry: VacantEntry<'a, RawSqe>,
    counter: &'a mut usize,
}

impl<'a> SlabReservedEntry<'a> {
    /// Creates a new SlabReservedEntry.
    fn new(entry: slab::VacantEntry<'a, RawSqe>, counter: &'a mut usize) -> Self {
        Self { entry, counter }
    }

    /// Gets the key that will be used for the next insertion.
    pub(crate) fn key(&self) -> usize {
        self.entry.key()
    }

    /// Inserts a value into the slab, incrementing the `pending_io` counter.
    /// This consumes the entry, just like the original `insert` method.
    pub(crate) fn commit(self, value: RawSqe) -> &'a mut RawSqe {
        *self.counter += 1;
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
        self.slab.pending_ios += self.indices.len();

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
    use crate::context::with_slab_mut;
    use crate::sqe::CompletionEffect;
    use crate::sqe::raw::CompletionHandler;
    use crate::test_utils::*;
    use anyhow::Result;
    use libc::CN_IDX_BB;

    #[test]
    fn test_new_and_capacity() {
        let capacity = 5;
        let slab = RawSqeSlab::new(capacity);
        assert_eq!(slab.slab.capacity(), capacity);
        assert_eq!(slab.slab.len(), 0);
    }

    #[test]
    fn test_insert_and_len() -> Result<()> {
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
    fn test_slab_full() {
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
    }

    #[test]
    fn test_pending_io_tracking() -> Result<()> {
        init_local_runtime_and_context(None)?;

        with_slab_mut(|slab| -> Result<()> {
            let n_sqes = 16;
            let (waker, waker_data) = mock_waker();

            let indices = (0..n_sqes)
                .map(|_| {
                    let reserved = slab.reserve_entry()?;
                    let idx = reserved.key();

                    let mut raw = RawSqe::new(CompletionHandler::new_single());
                    raw.set_waker(&waker);

                    reserved.commit(raw);
                    Ok(idx)
                })
                .collect::<Result<Vec<_>>>()?;

            assert_eq!(slab.pending_ios, n_sqes);

            // Waking sqes decrement counter
            for idx in indices {
                let effects = slab.get_mut(idx)?.on_completion(0, None)?;
                assert_eq!(*effects, [CompletionEffect::DecrementPendingIo]);
            }

            // Nothing has actually decremented the slab, we need a handler on the effects.
            assert_eq!(slab.pending_ios, n_sqes);
            assert_eq!(waker_data.get_count(), n_sqes);

            Ok(())
        })
    }

    #[test]
    fn test_reserve_batch() -> Result<()> {
        init_local_runtime_and_context(None)?;

        with_slab_mut(|slab| -> Result<()> {
            let n_sqes = 16;

            {
                let batch = slab.reserve_batch(n_sqes)?;

                // Slab entries are reserved but *not committed*
                assert_eq!(batch.slab.pending_ios, 0);
                assert_eq!(batch.slab.len(), n_sqes);
                assert_eq!(batch.keys().len(), n_sqes);
            }

            // Batch was not committed, so slab entries are released.
            assert_eq!(slab.pending_ios, 0);
            assert_eq!(slab.len(), 0);

            {
                let batch = slab.reserve_batch(n_sqes)?;

                // Slab entries are reserved but *not committed*
                assert_eq!(batch.slab.pending_ios, 0);
                assert_eq!(batch.slab.len(), n_sqes);
                assert_eq!(batch.keys().len(), n_sqes);

                // Commit the batch
                let entries = (0..n_sqes)
                    .map(|_| RawSqe::new(CompletionHandler::new_single()))
                    .collect::<SmallVec<[_; SPILL_TO_HEAP_THRESHOLD]>>();

                batch.commit(entries)?;
            } // Batch was committed, nothing happens.

            assert_eq!(slab.pending_ios, n_sqes);
            assert_eq!(slab.len(), n_sqes);

            Ok(())
        })
    }
}
