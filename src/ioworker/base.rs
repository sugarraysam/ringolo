use crate::iobase::ring::SingleIssuerRing;
use crate::iobase::types::{IoWrapper, IoWrapperPool};
use either::Either;
use io_uring::squeue::Entry;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use anyhow::{Result, anyhow};

// After how many busy loops we yield the thread.
const DEFAULT_YIELD_EVERY_N: usize = 10;

// This class represents all of the operations that can be performed by a worker.
// Think of the methods as building blocks to power higher-level IO submission
// and completion algorithms. These methods *are not thread-safe* and should be
// called from the context of a single thread.
pub struct WorkerBase {
    // IoUring instance setup for this single-threaded worker.
    ring: SingleIssuerRing,

    // Channel to receive IO operations from clients.
    receiver: mpsc::Receiver<*mut IoWrapper>,

    // When we wait for the next io on the receiver channel, we have to deque
    // the io from the channel if one is received. We use this field to store
    // this extra io to provide a better UX to users of the worker class.
    carry_io: Option<*mut IoWrapper>,

    // Object pool of IoWrapper to avoid mem allocations at runtime.
    //
    // To manage memory for the IoWrapper manually, we use a raw pointer
    // to its memory address. We'll store this address in the user_data field
    // of the SQE. When the operation completes, the same user_data value will
    // be available in the CQE, allowing us to reconstruct the IoWrapper object
    // and regain ownership of its memory.
    io_pool: Arc<IoWrapperPool>,

    // Stats
    capacity: usize,
    inflight: usize,
}

impl WorkerBase {
    pub fn try_new(
        sq_ring_size: usize,
        receiver: mpsc::Receiver<*mut IoWrapper>,
    ) -> Result<WorkerBase> {
        if receiver.max_capacity() != sq_ring_size {
            return Err(anyhow!(
                "Receiver max capacity does not match the SQ ring size: expected {}, got {}",
                sq_ring_size,
                receiver.max_capacity()
            ));
        }

        Ok(Self {
            ring: SingleIssuerRing::try_new(sq_ring_size as u32)?,
            receiver,
            carry_io: None,
            io_pool: IoWrapperPool::new(sq_ring_size),
            capacity: sq_ring_size,
            inflight: 0,
        })
    }

    // Submit `num_to_submit` pending ios from the mpsc channel, waiting for
    // `num_to_wait` cqes to complete.
    pub async fn submit(
        &mut self,
        num_to_submit: Option<usize>,
        num_to_wait: Option<usize>,
        timeout: Option<Duration>,
    ) -> Result<usize> {
        let num_pending = self.num_pending();
        let n = num_to_submit.unwrap_or(num_pending);

        let ios = self.receive_ios(n).await?;
        let sqes = self.prepare_sqes(ios)?;

        let num_submitted = self.ring.submit_and_wait_timeout(
            sqes.as_slice(),
            num_to_wait.unwrap_or(0),
            timeout,
        )?;

        self.inflight += num_submitted;
        Ok(num_submitted)
    }

    // Complete exactly N cqes, waiting for them with a call to `io_uring_enter`.
    pub fn wait_and_complete(
        &mut self,
        num_to_complete: Option<usize>,
        timeout: Option<Duration>,
    ) -> Result<usize> {
        let n = num_to_complete.unwrap_or(self.inflight);
        if n > self.inflight {
            return Err(anyhow!(
                "Not enough pending CQEs. Requested: {}, Inflight: {}",
                n,
                self.inflight
            ));
        }

        self.ring.wait_cqes_timeout(n as u32, timeout)?;

        let num_completed = self.process_cqes(Some(n))?;

        if num_completed != n {
            Err(anyhow!(
                "Failed to complete the expected number of elements. Expected: {}, Completed: {}",
                n,
                num_completed
            ))
        } else {
            Ok(num_completed)
        }
    }

    // Complete as many CQEs as possible without waiting or using an `io_uring_enter`
    // syscall. You should call this method after having ensured there is at
    // least 1 CQ available.
    pub fn complete_eager(&mut self) -> Result<usize> {
        if self.inflight == 0 {
            Ok(0)
        } else {
            self.process_cqes(None)
        }
    }

    // We busy loop on the CQ ring to complete `num_to_complete` submissions. This
    // method will use more CPU as it will keep iterating until we've reached the
    // desired number of completions.
    pub fn complete_busy_loop(
        &mut self,
        num_to_complete: Option<usize>,
        yield_every_n: Option<usize>,
    ) -> Result<usize> {
        let n = num_to_complete.unwrap_or(self.inflight);
        if n > self.inflight {
            return Err(anyhow!(
                "Not enough pending CQEs. Requested: {}, Inflight: {}",
                n,
                self.inflight
            ));
        }

        let mut num_completed: usize = 0;
        let mut spin_count = 0;
        let yield_every_n = yield_every_n.unwrap_or(DEFAULT_YIELD_EVERY_N);

        loop {
            num_completed += self.process_cqes(None)?;
            if num_completed >= n {
                break;
            }

            // We mimic spinlock heuristics to mix busy-looping and yielding to
            // avoid wasting too much CPU.
            spin_count += 1;
            if spin_count % yield_every_n == 0 {
                std::thread::yield_now();
            }
        }

        Ok(num_completed)
    }

    // This function can returns false if the timeout expires without receiving
    // an io on the channel. If a value is received, it is stored in an internal
    // field `carry_io` which will be flushed next time we submit to sq ring.
    pub async fn wait_for_next_io(&mut self, timeout: Option<Duration>) -> Result<bool> {
        if self.carry_io.is_some() {
            return Err(anyhow!("Previous carry_io was never submitted."));
        }

        tokio::select! {
            maybe_io = self.receiver.recv() => {
                self.carry_io = maybe_io;
                Ok(true)
            },
            _ = async {
                match timeout {
                    Some(duration) => tokio::time::sleep(duration).await,
                    // If no timeout is provided, wait for a future that never completes.
                    None => std::future::pending::<()>().await,
                }
            } => {
                Ok(false)
            }

        }
    }

    pub fn wait_for_next_cqe(&mut self, timeout: Option<Duration>) -> Result<usize> {
        if self.ring.has_pending_completions() {
            return Ok(0);
        }

        Ok(self.ring.wait_cqes_timeout(1, timeout)?)
    }

    pub fn num_pending(&self) -> usize {
        self.receiver.len()
    }

    pub fn num_inflight(&self) -> usize {
        self.inflight
    }

    pub const fn capacity(&self) -> usize {
        self.capacity
    }
}

// Private methods
impl WorkerBase {
    async fn receive_ios(&mut self, n: usize) -> Result<Vec<*mut IoWrapper>> {
        let c = self.carry_io.is_some() as usize;
        let mut ios: Vec<*mut IoWrapper> = Vec::with_capacity(n + c);

        if let Some(io) = self.carry_io.take() {
            ios.push(io);
        }

        let num_received = self.receiver.recv_many(&mut ios, n).await;

        if num_received != n - c {
            return Err(anyhow!(
                "Received {} IOs on channel but was expecting {}.",
                num_received,
                n
            ));
        }

        Ok(ios)
    }

    fn prepare_sqes(&mut self, ios: Vec<*mut IoWrapper>) -> Result<Vec<Entry>> {
        ios.into_iter()
            .map(|io| unsafe {
                (*io)
                    .take_entry()
                    .ok_or_else(|| anyhow!("Unexpected empty entry on IoWrapper."))
            })
            .collect::<Result<Vec<_>>>()
    }

    // The private helper function to handle the common CQE processing logic.
    // It iterates through the completion queue, processes each entry, and returns
    // the number of completed items.
    fn process_cqes(&mut self, num_to_take: Option<usize>) -> Result<usize> {
        let mut num_completed = 0;

        {
            let cq = self.ring.as_mut().completion();
            let mut q_iter = if let Some(n) = num_to_take {
                Either::Right(cq.into_iter().take(n))
            } else {
                Either::Left(cq.into_iter())
            };

            q_iter.try_for_each(|cqe| -> Result<()> {
                // SAFETY: reconstruct from raw ptr address and wake up receiver.
                let io_wrapper = cqe.user_data() as *mut IoWrapper;
                if !io_wrapper.is_null() {
                    unsafe {
                        (*io_wrapper).send_result(cqe.result());
                    }
                    self.io_pool.push(io_wrapper)?;
                    num_completed += 1;
                }
                Ok(())
            })?;
        }

        self.inflight -= num_completed;
        self.ring.as_mut().completion().sync();

        Ok(num_completed)
    }
}
