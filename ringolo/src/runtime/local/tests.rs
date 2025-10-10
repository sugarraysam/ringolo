use super::*;
use crate::context::{expect_local_context, with_ring_mut};
use crate::future::opcodes::NopBuilder;
use crate::runtime::waker::Wake;
use crate::runtime::{Builder, Schedule, YieldReason};
use crate::sqe::{IoError, Sqe, SqeSingle, Submittable};
use crate::test_utils::spy::{Call, Method};
use crate::test_utils::*;
use anyhow::Result;
use static_assertions::assert_impl_all;

assert_impl_all!(Scheduler: Send, Sync, Wake);
assert_impl_all!(Handle: Send, Sync, Schedule);

#[test]
fn test_local_scheduler_single_nop() -> Result<()> {
    let builder = Builder::new_local().try_build()?;
    builder.block_on(async {
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
    let builder = Builder::new_local().try_build()?;
    builder.block_on(async {
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
    let builder = Builder::new_local().try_build()?;
    builder.block_on(async {
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

#[test]
fn test_local_scheduler_sq_ring_full_triggers_yield_now() -> Result<()> {
    let sq_ring_size = 8;
    let builder = Builder::new_local().sq_ring_size(sq_ring_size);
    init_local_runtime_and_context(Some(builder))?;

    let (waker, waker_data) = mock_waker();

    // Fill the ring
    let mut sqes = build_batch(sq_ring_size);
    assert!(sqes.submit(&waker).is_ok());
    sqes.set_waker(&waker)?;

    with_ring_mut(|ring| assert_eq!(ring.sq().len(), sq_ring_size));

    // Adding one more SQE triggers PushError
    let mut sqe = SqeSingle::new(nop());
    assert!(matches!(sqe.submit(&waker), Err(IoError::SqRingFull)));

    expect_local_context(|ctx| {
        ctx.scheduler.block_on(async {
            let fut = Sqe::new(SqeSingle::new(nop()));
            let res = fut.await;

            assert!(res.is_ok());
            let (_, res) = res.unwrap();

            assert!(res.is_ok());
            assert_eq!(res.unwrap(), 0);
        })
    });

    assert_eq!(waker_data.get_count(), 1);

    expect_local_context(|ctx| {
        let yield_calls = ctx.scheduler.state.get_calls(&Method::YieldNow);
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
