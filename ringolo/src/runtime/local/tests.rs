use super::*;
use crate as ringolo;
use crate::context;
use crate::future::lib::ops::{Nop, OpenAt};
use crate::future::lib::types::{Mode, OFlag};
use crate::future::lib::{KernelFdMode, Op};
use crate::runtime::waker::Wake;
use crate::runtime::{
    AddMode, Builder, PanicReason, Schedule, SchedulerPanic, TaskRegistry, YieldReason, get_root,
};
use crate::spawn::{MAINTENANCE_TASK_OPTS, TaskOpts};
use crate::sqe::{Sqe, SqeCollection};
use crate::task::JoinHandle;
use crate::test_utils::*;
use crate::time::{Sleep, YieldNow};
use crate::utils::scheduler::*;
use crate::utils::thread::get_current_thread_name;
use anyhow::Result;
use rstest::rstest;
use static_assertions::assert_impl_all;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

assert_impl_all!(Scheduler: Send, Sync, Wake);
assert_impl_all!(Handle: Send, Sync, Schedule);

#[ringolo::test]
async fn test_single_nop() -> Result<()> {
    assert!(matches!(Op::new(Nop).await, Ok(_)));
    Ok(())
}

#[ringolo::test]
async fn test_batch_of_nops() -> Result<()> {
    let batch = Sqe::new(build_batch(10));
    let res = batch.await;

    assert!(res.is_ok());
    for res in res.unwrap() {
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
    }

    Ok(())
}

#[ringolo::test]
async fn test_sqe_chain_backend_file_io() -> Result<()> {
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
}

#[test]
fn test_sq_batch_too_large() -> Result<()> {
    let sq_ring_size = 8;
    let runtime = Builder::new_local()
        .sq_ring_size(sq_ring_size)
        .try_build()?;

    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = runtime.block_on(async move {
            let batch = Sqe::new(build_batch(sq_ring_size + 1));
            batch.await
        });
    }));

    assert!(res.is_err());
    assert!(matches!(
        res.unwrap_err()
            .downcast_ref::<SchedulerPanic>()
            .map(|p| p.reason),
        Some(PanicReason::SqBatchTooLarge)
    ));

    Ok(())
}

#[test]
fn test_sq_ring_full_recovers() -> Result<()> {
    let sq_ring_size = 8;
    let builder = Builder::new_local().sq_ring_size(sq_ring_size);
    let (runtime, scheduler) = init_local_runtime_and_context(Some(builder))?;

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
    let yield_calls = scheduler.tracker.get_calls(&Method::YieldNow);
    assert_eq!(yield_calls.len(), 1);
    assert!(matches!(
        yield_calls[0],
        Call::YieldNow {
            reason: YieldReason::SqRingFull,
            mode: None,
        }
    ));

    Ok(())
}

#[test]
fn test_spawn_before_block_on() -> Result<()> {
    let sq_ring_size = 8;
    let runtime = Builder::new_local()
        .sq_ring_size(sq_ring_size)
        .try_build()?;

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
    let (runtime, scheduler) = init_local_runtime_and_context(None)?;

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

        42
    });

    let spawn_calls = scheduler.tracker.get_calls(&Method::Spawn);
    assert_eq!(spawn_calls.len(), 1);

    assert_eq!(res, 42);
    Ok(())
}

#[rstest]
#[case::three(3)]
#[case::seven(7)]
#[case::eleven(11)]
fn test_runtime_shutdown(#[case] n: usize) -> Result<()> {
    let start = Instant::now();
    let (runtime, scheduler) = init_local_runtime_and_context(None)?;

    let shared = runtime.block_on(async move {
        context::with_shared(|shared| {
            assert!(!shared.shutdown.load(Ordering::Relaxed));
            Arc::clone(&shared)
        })
    });

    // Spawn tasks on runtime that are very long
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

    let maintenance_task = 1;
    assert_eq!(scheduler.tasks.len(), n + maintenance_task);

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

#[rstest]
#[case::three(3)]
#[case::seven(7)]
#[case::thirteen(13)]
#[test]
fn test_tasks_are_not_stealable(#[case] num_yields: usize) -> Result<()> {
    let (runtime, scheduler) = init_local_runtime_and_context(None)?;

    runtime.block_on(async {
        let handle: JoinHandle<Result<()>> = ringolo::spawn(async move {
            // Owning an fd makes the task not stealable.
            let _fd = Op::new(OpenAt::try_new(
                KernelFdMode::DirectAuto,
                None,
                gen_tempfile_name(),
                OFlag::O_RDONLY | OFlag::O_CREAT,
                Mode::S_IRUSR | Mode::S_IWUSR,
            )?)
            .await?;

            for _ in 0..num_yields {
                YieldNow::new(None).await?;
            }

            Ok(())
        });

        assert!(handle.await.is_ok());
    });

    // Yield internally calls schedule
    assert_eq!(
        scheduler.tracker.get_calls(&Method::YieldNow).len(),
        num_yields
    );

    let schedule_calls = scheduler.tracker.get_calls(&Method::Schedule);
    assert_eq!(
        schedule_calls.len(),
        num_yields + 2 /* initial_spawn + process_cqes */
    );

    // only initial spawn is stealable
    assert_eq!(
        schedule_calls
            .iter()
            .filter(|c| matches!(c, Call::Schedule { is_stealable, .. } if *is_stealable))
            .count(),
        1,
    );

    runtime.shutdown();

    Ok(())
}

#[rstest]
#[case::lifo(Some(TaskOpts::HINT_SPAWN_LIFO), Some(AddMode::Lifo))]
#[case::fifo(Some(TaskOpts::HINT_SPAWN_FIFO), Some(AddMode::Fifo))]
#[case::default(None, None)]
#[test]
fn test_hint_spawn_add_mode(
    #[case] hint_spawn_opt: Option<TaskOpts>,
    #[case] expected_add_mode: Option<AddMode>,
) -> Result<()> {
    let (runtime, scheduler) = init_local_runtime_and_context(None)?;

    runtime.block_on(async {
        let handle: JoinHandle<Result<()>> = ringolo::spawn_builder()
            .with_opts(hint_spawn_opt.unwrap_or_default())
            .spawn(async move { Ok(()) });

        assert!(handle.await.is_ok());
    });

    let schedule_calls = scheduler.tracker.get_calls(&Method::Schedule);
    assert_eq!(schedule_calls.len(), 1);
    assert!(
        schedule_calls
            .iter()
            .all(|c| matches!(c, Call::Schedule { mode, ..} if *mode == expected_add_mode))
    );

    runtime.shutdown();

    Ok(())
}
