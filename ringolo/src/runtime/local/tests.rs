use super::*;
use crate as ringolo;
use crate::future::lib::{Nop, Op};
use crate::runtime::waker::Wake;
use crate::runtime::{Builder, Schedule, YieldReason};
use crate::sqe::{IoError, Sqe, SqeCollection};
use crate::test_utils::*;
use crate::utils::scheduler::*;
use crate::with_scheduler;
use anyhow::Result;
use static_assertions::assert_impl_all;

assert_impl_all!(Scheduler: Send, Sync, Wake);
assert_impl_all!(Handle: Send, Sync, Schedule);

#[test]
fn test_local_scheduler_single_nop() -> Result<()> {
    let runtime = Builder::new_local().try_build()?;
    runtime.block_on(async {
        assert!(matches!(Op::new(Nop).await, Ok(_)));
        Ok(())
    })
}

#[test]
fn test_local_scheduler_batch_of_nops() -> Result<()> {
    let runtime = Builder::new_local().try_build()?;
    runtime.block_on(async {
        let batch = Sqe::new(build_batch(10));
        let res = batch.await;

        assert!(res.is_ok());
        for res in res.unwrap() {
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), 0);
        }

        Ok(())
    })
}

#[test]
fn test_local_scheduler_chain_write_fsync_read_tempfile() -> Result<()> {
    let runtime = Builder::new_local().try_build()?;
    runtime.block_on(async {
        let (chain, data_out, mut data_in, _tmp) = build_chain_write_fsync_read_a_tempfile()?;
        let chain = Sqe::new(chain);
        let res = chain.await;

        assert!(res.is_ok());

        let expected: Vec<i32> = vec![data_out.len() as i32, 0, data_out.len() as i32];
        expected
            .into_iter()
            .zip(res.unwrap())
            .for_each(|(expected, got)| {
                assert!(got.is_ok());
                assert_eq!(expected, got.unwrap())
            });

        // vec is unaware that data was written as it was through a *mut T
        unsafe {
            data_in.set_len(data_out.len());
        }

        assert_eq!(data_in, data_out);

        Ok(())
    })
}

#[test]
fn test_local_scheduler_sq_batch_too_large() -> Result<()> {
    let sq_ring_size = 8;
    let runtime = Builder::new_local()
        .sq_ring_size(sq_ring_size)
        .try_build()?;

    let res = runtime.block_on(async move {
        let batch = Sqe::new(build_batch(sq_ring_size + 1));
        batch.await
    });

    assert!(matches!(res, Err(IoError::SqBatchTooLarge)));
    Ok(())
}

#[test]
fn test_local_scheduler_sq_ring_full_recovers() -> Result<()> {
    let sq_ring_size = 8;
    let runtime = Builder::new_local()
        .sq_ring_size(sq_ring_size)
        .try_build()?;

    let results = runtime.block_on(async move {
        let batch = SqeCollection::new(vec![build_batch(5), build_batch(4)]);
        batch.await
    });

    for res in results {
        assert!(res.is_ok());
        for res in res.unwrap() {
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), 0);
        }
    }

    // The first batch of 5 succeeds without error. The second overflows the SQ ring,
    // triggering a `yield_now` + submit from Worker. After we retry it succeeds.
    with_scheduler!(|s| {
        let yield_calls = s.tracker.get_calls(&Method::YieldNow);
        assert_eq!(yield_calls.len(), 1);
        assert!(matches!(
            yield_calls[0],
            Call::YieldNow {
                reason: YieldReason::SqRingFull,
                mode: None,
            }
        ));
    });

    Ok(())
}

#[test]
fn test_local_scheduler_spawn_before_block_on() -> Result<()> {
    let sq_ring_size = 8;
    let runtime = Builder::new_local()
        .sq_ring_size(sq_ring_size)
        .try_build()?;

    // Default current task ID is ROOT_FUTURE instead of None?
    // Set default current task ID when starting worker
    // - stealing creating thread == ROOT_FUTURE
    // - stealing not creating == None
    // - local == ROOT_FUTURE
    let handle = runtime.spawn(async move {
        let batch = SqeCollection::new(vec![build_batch(2), build_batch(2)]);
        batch.await
    });

    let res = runtime.block_on(async move { Op::new(Nop).await });

    assert!(matches!(res, Ok(_)));
    assert!(handle.is_finished());

    for res in handle.get_result()? {
        assert!(res.is_ok());
        for res in res.unwrap() {
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), 0);
        }
    }

    Ok(())
}

#[test]
fn test_local_scheduler_spawn_within_block_on() -> Result<()> {
    let runtime = Builder::new_local().try_build()?;

    let res = runtime.block_on(async {
        let res = ringolo::spawn(async {
            let batch = Sqe::new(build_batch(10));
            let res = batch.await;

            assert!(res.is_ok());
            for res in res.unwrap() {
                assert!(res.is_ok());
                assert_eq!(res.unwrap(), 0);
            }
        })
        .await;

        assert!(res.is_ok());

        with_scheduler!(|s| {
            let spawn_calls = s.tracker.get_calls(&Method::Spawn);
            assert_eq!(spawn_calls.len(), 1);
        });

        42
    });

    assert_eq!(res, 42);
    Ok(())
}
