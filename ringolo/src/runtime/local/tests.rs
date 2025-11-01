use super::*;
use crate as ringolo;
use crate::context;
use crate::context::maintenance::task::MAINTENANCE_TASK_OPTS;
use crate::future::lib::{Nop, Op};
use crate::runtime::waker::Wake;
use crate::runtime::{Builder, Schedule, TaskRegistry, YieldReason, get_root};
use crate::sqe::{IoError, Sqe, SqeCollection};
use crate::test_utils::*;
use crate::time::{Sleep, YieldNow};
use crate::utils::scheduler::*;
use crate::utils::thread::get_current_thread_name;
use crate::with_scheduler;
use anyhow::Result;
use static_assertions::assert_impl_all;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

assert_impl_all!(Scheduler: Send, Sync, Wake);
assert_impl_all!(Handle: Send, Sync, Schedule);

#[test]
fn test_single_nop() -> Result<()> {
    let runtime = Builder::new_local().try_build()?;
    runtime.block_on(async {
        assert!(matches!(Op::new(Nop).await, Ok(_)));
        Ok(())
    })
}

#[test]
fn test_batch_of_nops() -> Result<()> {
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
fn test_sqe_chain_backend_file_io() -> Result<()> {
    let runtime = Builder::new_local().try_build()?;
    runtime.block_on(async {
        let (chain, data_out, mut data_in, _tmp) = build_chain_write_fsync_read_tempfile();
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
fn test_sq_batch_too_large() -> Result<()> {
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
fn test_sq_ring_full_recovers() -> Result<()> {
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
fn test_spawn_before_block_on() -> Result<()> {
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
fn test_spawn_within_block_on() -> Result<()> {
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

#[test]
fn test_runtime_shutdown() -> Result<()> {
    let start = Instant::now();
    let runtime = Builder::new_local().try_build()?;

    let scheduler = runtime.expect_local_scheduler();
    let shared = runtime.block_on(async move {
        context::with_shared(|shared| {
            assert!(!shared.shutdown.load(Ordering::Relaxed));
            Arc::clone(&shared)
        })
    });

    // Spawn tasks on runtime that are very long
    let n = 3;
    let handles = (0..n)
        .into_iter()
        .map(|_| {
            runtime.spawn(async {
                Sleep::try_new(Duration::from_secs(10))?
                    .await
                    .map_err(anyhow::Error::from)
            })
        })
        .collect::<Vec<_>>();

    // We shutdown, but have saved both shared + scheduler to verify shutdown.
    runtime.shutdown();

    assert!(shared.shutdown.load(Ordering::Relaxed));
    assert!(scheduler.tasks.is_closed());
    assert_eq!(scheduler.tasks.len(), n + 1);

    // Make sure we did not have to wait for the tasks to complete
    assert!(start.elapsed() < Duration::from_secs(1));
    for handle in handles {
        assert!(handle.is_finished());
        assert!(handle.get_result().is_err_and(|e| e.is_cancelled()));
    }

    Ok(())
}

#[ringolo::test]
async fn test_default_thread_name() -> Result<()> {
    let thread_name = get_current_thread_name()?;

    // Parses to `ringolo-{id}`, id monotonically increasing.
    let parts = thread_name.split("-").collect::<Vec<_>>();
    assert_eq!(parts[0], "ringolo");
    assert!(matches!(parts[1].parse::<usize>(), Ok(id) if id == 0));

    Ok(())
}

#[ringolo::test]
async fn test_maintenance_task() -> Result<()> {
    let root_node = get_root();
    assert_eq!(
        root_node.num_children(),
        1,
        "Maintenance task always running"
    );

    assert!(
        root_node
            .clone_children()
            .iter()
            .any(|child| child.is_maintenance_task()
                && child.opts.contains(*MAINTENANCE_TASK_OPTS))
    );

    // On local scheduler we need to yield once for the maintenance task to be
    // polled once and set `is_running` to true.
    YieldNow::new(None).await?;
    context::with_core(|core| assert!(core.maintenance_task.is_running()));

    Ok(())
}
