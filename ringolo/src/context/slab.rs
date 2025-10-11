use crate::sqe::errors::IoError;
use crate::sqe::raw::RawSqe;
// use anyhow::{Context, Result, anyhow};
use slab::{Slab, VacantEntry};
use std::io::{self, Error, ErrorKind};

pub struct RawSqeSlab {
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
            pending_ios: 0,
        }
    }

    /// Always rely on our SlabVacantEntry wrapper for insertions as it will
    /// take care of incrementing the number of `pending_ios`.
    pub(crate) fn vacant_entry(&'_ mut self) -> Result<SlabVacantEntry<'_>, IoError> {
        if self.slab.len() == self.slab.capacity() {
            return Err(IoError::SlabFull);
        }

        let v = self.slab.vacant_entry();
        Ok(SlabVacantEntry::new(v, &mut self.pending_ios))
    }

    pub(crate) fn insert(&mut self, mut raw_sqe: RawSqe) -> Result<(usize, &mut RawSqe), IoError> {
        let entry = self.vacant_entry()?;
        raw_sqe.set_user_data(entry.key() as u64)?;

        Ok((entry.key(), entry.insert(raw_sqe)))
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

    // Removes and drop the entry if it exists. Returns true if an entry was dropped.
    pub(crate) fn try_remove(&mut self, key: usize) -> Option<RawSqe> {
        self.slab.try_remove(key).map(|mut sqe| {
            sqe.set_available();
            sqe
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.slab.len()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.slab.capacity()
    }
}

/// A wrapper around slab::VacantEntry that increments a counter upon insertion.
pub(crate) struct SlabVacantEntry<'a> {
    entry: VacantEntry<'a, RawSqe>,
    counter: &'a mut usize,
}

impl<'a> SlabVacantEntry<'a> {
    /// Creates a new SlabVacantEntry.
    fn new(entry: slab::VacantEntry<'a, RawSqe>, counter: &'a mut usize) -> Self {
        Self { entry, counter }
    }

    /// Gets the key that will be used for the next insertion.
    pub(crate) fn key(&self) -> usize {
        self.entry.key()
    }

    /// Inserts a value into the slab, incrementing the `pending_io` counter.
    /// This consumes the entry, just like the original `insert` method.
    pub(crate) fn insert(self, value: RawSqe) -> &'a mut RawSqe {
        *self.counter += 1;
        self.entry.insert(value)
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

    #[test]
    fn test_new_and_capacity() {
        let capacity = 5;
        let slab = RawSqeSlab::new(capacity);
        assert_eq!(slab.slab.capacity(), capacity);
        assert_eq!(slab.slab.len(), 0);
    }

    #[test]
    fn test_insert_get_and_user_data() -> Result<()> {
        let n_sqes = 3;
        let mut slab = RawSqeSlab::new(n_sqes);

        for i in 1..=n_sqes {
            let sqe = RawSqe::new(nop(), CompletionHandler::new_single());

            let (key, inserted) = slab.insert(sqe)?;

            assert_eq!(inserted.get_entry()?.get_user_data(), key as u64);
            assert_eq!(slab.get(key)?.get_entry()?.get_user_data(), key as u64);

            assert_eq!(slab.len(), i);
        }

        Ok(())
    }

    #[test]
    fn test_slab_full() {
        let n_sqes = 3;
        let mut slab = RawSqeSlab::new(n_sqes - 1);

        for i in 1..=n_sqes {
            let res = slab.insert(RawSqe::new(nop(), CompletionHandler::new_single()));
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
                    let mut raw_sqe = RawSqe::new(nop(), CompletionHandler::new_single());
                    raw_sqe.set_waker(&waker);
                    slab.insert(raw_sqe)
                        .map(|(idx, _)| idx)
                        .map_err(|e| e.into())
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
}
