use crate::iouring::ring::SingleIssuerRing;
use crate::iouring::types::SqeWrapper;
use std::collections::VecDeque;

use anyhow::Result;

pub struct Worker {
    ring: SingleIssuerRing,

    // Leverage object pool of SqeWrapper to avoid mem allocations at runtime
    sqe_pool: VecDeque<Box<SqeWrapper>>,
}

impl Worker {
    pub fn try_new(sq_ring_size: u32) -> Result<Worker> {
        let ring = SingleIssuerRing::try_new(sq_ring_size)?;

        let mut sqe_pool = VecDeque::with_capacity(sq_ring_size as usize);
        for _ in 0..sq_ring_size {
            sqe_pool.push_back(Box::new(SqeWrapper::default()));
        }

        Ok(Self { ring, sqe_pool })
    }

    pub fn num_inflight(&self) -> usize {
        self.sqe_pool.capacity() - self.sqe_pool.len()
    }

    pub fn num_available(&self) -> usize {
        self.sqe_pool.len()
    }
}
