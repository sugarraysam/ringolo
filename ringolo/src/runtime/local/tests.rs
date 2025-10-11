use super::*;
use crate as ringolo;
use crate::context::{expect_local_scheduler, with_ring_mut, with_slab_and_ring_mut};
use crate::future::opcodes::NopBuilder;
use crate::runtime::waker::Wake;
use crate::runtime::{Builder, Schedule, YieldReason};
use crate::sqe::{IoError, Sqe, SqeSingle, Submittable};
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
        let fut = NopBuilder::new().build();
        let res = fut.await;

        assert!(res.is_ok());
        let (_, res) = res.unwrap();

        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
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
        for (_, res) in res.unwrap() {
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
            .for_each(|(expected, (_entry, got))| {
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

// TODO:
// - SqeBatchTooLarge -> panic
// - SqeCollection :: 2 batches, second one overflows, triggers yield + retry == success, no duplicates in SLAB
// - work within `block_on` + `spawn` otherwise does not make sense, not realistic.
// - push_sqes atomic, meaning all succeed or all fail
// - batch larger than SQ ring => retried once then panics
#[test]
fn test_local_scheduler_sq_ring_full_triggers_yield_now() -> Result<()> {
    let sq_ring_size = 8;
    let runtime = Builder::new_local().sq_ring_size(sq_ring_size);
    init_local_runtime_and_context(Some(runtime))?;

    let (waker, waker_data) = mock_waker();

    // Fill the ring
    {
        let mut sqes = build_batch(sq_ring_size);
        assert!(sqes.submit(&waker).is_ok());
        sqes.set_waker(&waker)?;

        with_ring_mut(|ring| assert_eq!(ring.sq().len(), sq_ring_size));

        // Adding one more SQE triggers PushError
        let mut sqe = SqeSingle::new(nop());
        assert!(matches!(sqe.submit(&waker), Err(IoError::SqRingFull)));
    }

    with_scheduler!(|s| {
        s.block_on(async {
            let fut = Sqe::new(SqeSingle::new(nop()));
            let res = fut.await;

            assert!(res.is_ok());
            let (_, res) = res.unwrap();

            assert!(res.is_ok());
            assert_eq!(res.unwrap(), 0);
        })
    });

    with_slab_and_ring_mut(|slab, ring| {
        assert_eq!(ring.sq().len(), 0);
        // assert_eq!(slab.pending_ios, 0);
        assert_eq!(slab.len(), 0);
    });

    assert_eq!(waker_data.get_count(), 1);

    expect_local_scheduler(|_ctx, scheduler| {
        let yield_calls = scheduler.tracker.get_calls(&Method::YieldNow);
        assert_eq!(yield_calls.len(), 1);
        assert!(matches!(
            yield_calls[0],
            Call::YieldNow {
                reason: YieldReason::SqRingFull
            }
        ));
    });

    Ok(())
}

#[test]
fn test_local_scheduler_spawn() -> Result<()> {
    let runtime = Builder::new_local().try_build()?;

    // Spawning before `block_on` is allowed, just enqueues the task.
    let handle = runtime.spawn(async {
        // Sync future is allowed.
        assert!(true);
        42
    });

    let res = runtime.block_on(async {
        let res = ringolo::spawn(async {
            let batch = Sqe::new(build_batch(10));
            let res = batch.await;

            assert!(res.is_ok());
            for (_, res) in res.unwrap() {
                assert!(res.is_ok());
                assert_eq!(res.unwrap(), 0);
            }
        })
        .await;

        assert!(res.is_ok());

        with_scheduler!(|s| {
            let spawn_calls = s.tracker.get_calls(&Method::Spawn);
            assert_eq!(spawn_calls.len(), 2);
        });

        42
    });

    assert_eq!(res, 42);
    assert!(handle.is_finished());
    Ok(())
}
