use crate::sqe::raw::RawSqe;
use anyhow::{Context, Result, anyhow};
use slab::{Slab, VacantEntry};

pub struct RawSqeSlab {
    slab: slab::Slab<RawSqe>,
}

impl RawSqeSlab {
    pub fn new(capacity: usize) -> Self {
        Self {
            slab: Slab::with_capacity(capacity),
        }
    }

    pub fn vacant_entry(&mut self) -> Result<VacantEntry<'_, RawSqe>> {
        if self.slab.len() == self.slab.capacity() {
            return Err(anyhow!("RawSqeSlab is full"));
        }

        Ok(self.slab.vacant_entry())
    }

    pub fn insert(&mut self, mut raw_sqe: RawSqe) -> Result<(usize, &mut RawSqe)> {
        if self.slab.len() == self.slab.capacity() {
            return Err(anyhow!("RawSqeSlab is full"));
        }

        let entry = self.slab.vacant_entry();
        raw_sqe.set_user_data(entry.key() as u64)?;

        Ok((entry.key(), entry.insert(raw_sqe)))
    }

    pub fn get(&self, key: usize) -> Result<&RawSqe> {
        self.slab
            .get(key)
            .with_context(|| format!("Key {} not found in RawSqeSlab", key))
    }

    pub fn get_mut(&mut self, key: usize) -> Result<&mut RawSqe> {
        self.slab
            .get_mut(key)
            .with_context(|| format!("Key {} not found in RawSqeSlab", key))
    }

    // Removes and drop the entry if it exists. Returns true if an entry was dropped.
    pub fn try_remove(&mut self, key: usize) -> Option<RawSqe> {
        self.slab.try_remove(key).map(|mut sqe| {
            sqe.set_available();
            sqe
        })
    }

    pub fn len(&self) -> usize {
        self.slab.len()
    }

    pub fn capacity(&self) -> usize {
        self.slab.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqe::raw::CompletionHandler;
    use crate::test_utils::*;

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
}
