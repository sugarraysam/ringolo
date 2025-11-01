use super::*;
use crate as ringolo;
use crate::context::maintenance::task::MAINTENANCE_TASK_OPTS;
use crate::context::{self, expect_stealing_scheduler};
use crate::future::lib::{Nop, Op};
use crate::runtime::waker::Wake;
use crate::runtime::{Builder, Schedule, TaskRegistry, YieldReason, get_root};
use crate::sqe::{IoError, Sqe, SqeCollection};
use crate::task::JoinHandle;
use crate::test_utils::*;
use crate::time::{Sleep, YieldNow};
use crate::utils::scheduler::*;
use crate::utils::thread::get_current_thread_name;
use crate::with_scheduler;
use anyhow::Result;
use rstest::rstest;
use static_assertions::assert_impl_all;
use std::f64::consts::E;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

assert_impl_all!(Scheduler: Send, Sync, Wake);
assert_impl_all!(Handle: Send, Sync, Schedule);

#[ringolo::test(flavor = "stealing", worker_threads = 2)]
async fn test_scheduler_init_and_thread_pool() -> Result<()> {
    // Test root worker thread name
    let thread_name = get_current_thread_name()?;
    let parts = thread_name.split("-").collect::<Vec<_>>();
    assert_eq!(parts[0], "ringolo");
    assert!(matches!(parts[1].parse::<usize>(), Ok(id) if id == 0));

    expect_stealing_scheduler(|ctx, s| {
        assert_eq!(s.cfg.worker_threads, 2);

        // Check thread pool
        let pool = s.pool.get().unwrap();
        assert_eq!(pool.workers.len(), 2);

        for (_, handle) in pool.handles.lock().iter() {
            let parts = handle
                .thread()
                .name()
                .unwrap()
                .split("-")
                .collect::<Vec<_>>();

            assert_eq!(parts[0], "ringolo");
            assert!(matches!(parts[1].parse::<usize>(), Ok(id) if id > 0));
        }

        // Check context
        assert_eq!(ctx.shared.worker_slots.len(), 3);
        assert!(!ctx.shared.shutdown.load(Ordering::Relaxed));
    });

    Ok(())
}

#[test]
fn test_worker_parking_logic() -> Result<()> {
    let builder = Builder::new_stealing().worker_threads(2);
    let (runtime, _scheduler) = init_stealing_runtime_and_context(Some(builder))?;

    let start = Instant::now();
    runtime.block_on(async {
        // Spin until all workers are parked
        expect_stealing_scheduler(|ctx, s| {
            let mut all_parked = false;

            while !all_parked {
                all_parked = ctx.shared.parked_threads.read().len() == s.cfg.worker_threads;
            }
        });

        // Spawn tasks until all workers are unparked
        expect_stealing_scheduler(|ctx, _scheduler| {
            let mut handles = Vec::new();

            while !ctx.shared.parked_threads.read().is_empty() {
                handles.push(ringolo::spawn(async {
                    Sleep::try_new(Duration::from_secs(10))?
                        .await
                        .map_err(anyhow::Error::from)
                }))
            }
        });
    });

    runtime.shutdown();
    assert!(start.elapsed() < Duration::from_secs(1));
    Ok(())
}

#[test]
fn test_single_nop() -> Result<()> {
    let runtime = Builder::new_stealing().worker_threads(1).try_build()?;
    runtime.block_on(async {
        assert!(matches!(Op::new(Nop).await, Ok(_)));
        Ok(())
    })
}

#[test]
fn test_batch_of_nops() -> Result<()> {
    let runtime = Builder::new_stealing().worker_threads(1).try_build()?;
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

#[ringolo::test(flavor = "stealing", worker_threads = 2)]
async fn test_maintenance_task() -> Result<()> {
    let root_node = get_root();
    assert_eq!(
        root_node.num_children(),
        3,
        "Maintenance task runs on all workers including root_worker"
    );

    assert!(
        root_node
            .clone_children()
            .iter()
            .all(|child| child.is_maintenance_task() && child.opts.contains(*MAINTENANCE_TASK_OPTS))
    );

    // Ensure maintenance task is running on Root and a non-root worker
    context::with_core(|core| assert!(core.maintenance_task.is_running()));
    let handle = ringolo::spawn(async {
        context::with_core(|core| assert!(core.maintenance_task.is_running()));
    });

    assert!(handle.await.is_ok());
    Ok(())
}

#[rstest]
#[case::n_5(5)]
#[case::n_10(10)]
#[case::n_20(20)]
#[ringolo::test(flavor = "stealing", worker_threads = 3)]
async fn test_yield_now(#[case] n: usize) -> Result<()> {
    for _ in 0..n {
        assert!(matches!(YieldNow::new(None).await, Ok(_)));

        let handle: JoinHandle<Result<()>> = ringolo::spawn(async {
            assert!(
                !context::with_core(|c| c.is_polling_root()),
                "Root worker should never be polling root."
            );
            assert!(matches!(YieldNow::new(None).await, Ok(_)));
            Ok(())
        });

        assert!(matches!(handle.await, Ok(_)));
    }
    Ok(())
}

#[rstest]
#[case::n_2(2)]
#[case::n_5(5)]
#[case::n_7(7)]
#[ringolo::test(flavor = "stealing", worker_threads = 3)]
async fn test_sqe_chain_backend_file_io(#[case] n: usize) -> Result<()> {
    let mut handles: Vec<JoinHandle<Result<()>>> = Vec::with_capacity(n);

    for _ in 0..n {
        handles.push(ringolo::spawn(async {
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
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
    Ok(())
}

// #[test]
// fn test_sq_batch_too_large() -> Result<()> {
//     let sq_ring_size = 8;
//     let runtime = Builder::new_local()
//         .sq_ring_size(sq_ring_size)
//         .try_build()?;

//     let res = runtime.block_on(async move {
//         let batch = Sqe::new(build_batch(sq_ring_size + 1));
//         batch.await
//     });

//     assert!(matches!(res, Err(IoError::SqBatchTooLarge)));
//     Ok(())
// }

// #[test]
// fn test_sq_ring_full_recovers() -> Result<()> {
//     let sq_ring_size = 8;
//     let runtime = Builder::new_local()
//         .sq_ring_size(sq_ring_size)
//         .try_build()?;

//     let results = runtime.block_on(async move {
//         let batch = SqeCollection::new(vec![build_batch(5), build_batch(4)]);
//         batch.await
//     });

//     for res in results {
//         assert!(res.is_ok());
//         for res in res.unwrap() {
//             assert!(res.is_ok());
//             assert_eq!(res.unwrap(), 0);
//         }
//     }

//     // The first batch of 5 succeeds without error. The second overflows the SQ ring,
//     // triggering a `yield_now` + submit from Worker. After we retry it succeeds.
//     with_scheduler!(|s| {
//         let yield_calls = s.tracker.get_calls(&Method::YieldNow);
//         assert_eq!(yield_calls.len(), 1);
//         assert!(matches!(
//             yield_calls[0],
//             Call::YieldNow {
//                 reason: YieldReason::SqRingFull,
//                 mode: None,
//             }
//         ));
//     });

//     Ok(())
// }

// #[test]
// fn test_spawn_before_block_on() -> Result<()> {
//     let sq_ring_size = 8;
//     let runtime = Builder::new_local()
//         .sq_ring_size(sq_ring_size)
//         .try_build()?;

//     // Default current task ID is ROOT_FUTURE instead of None?
//     // Set default current task ID when starting worker
//     // - stealing creating thread == ROOT_FUTURE
//     // - stealing not creating == None
//     // - local == ROOT_FUTURE
//     let handle = runtime.spawn(async move {
//         let batch = SqeCollection::new(vec![build_batch(2), build_batch(2)]);
//         batch.await
//     });

//     let res = runtime.block_on(async move { Op::new(Nop).await });

//     assert!(matches!(res, Ok(_)));
//     assert!(handle.is_finished());

//     for res in handle.get_result()? {
//         assert!(res.is_ok());
//         for res in res.unwrap() {
//             assert!(res.is_ok());
//             assert_eq!(res.unwrap(), 0);
//         }
//     }

//     Ok(())
// }

// #[test]
// fn test_spawn_within_block_on() -> Result<()> {
//     let runtime = Builder::new_local().try_build()?;

//     let res = runtime.block_on(async {
//         let res = ringolo::spawn(async {
//             let batch = Sqe::new(build_batch(10));
//             let res = batch.await;

//             assert!(res.is_ok());
//             for res in res.unwrap() {
//                 assert!(res.is_ok());
//                 assert_eq!(res.unwrap(), 0);
//             }
//         })
//         .await;

//         assert!(res.is_ok());

//         with_scheduler!(|s| {
//             let spawn_calls = s.tracker.get_calls(&Method::Spawn);
//             assert_eq!(spawn_calls.len(), 1);
//         });

//         42
//     });

//     assert_eq!(res, 42);
//     Ok(())
// }

// #[test]
// fn test_runtime_shutdown() -> Result<()> {
//     let start = Instant::now();
//     let runtime = Builder::new_local().try_build()?;

//     let scheduler = runtime.expect_stealing_scheduler();
//     let shared = runtime.block_on(async move {
//         context::with_shared(|shared| {
//             assert!(!shared.shutdown.load(Ordering::Relaxed));
//             Arc::clone(&shared)
//         })
//     });

//     // Spawn tasks on runtime that are very long
//     let n = 3;
//     let handles = (0..n)
//         .into_iter()
//         .map(|_| {
//             runtime.spawn(async {
//                 Sleep::try_new(Duration::from_secs(10))?
//                     .await
//                     .map_err(anyhow::Error::from)
//             })
//         })
//         .collect::<Vec<_>>();

//     // We shutdown, but have saved both shared + scheduler to verify shutdown.
//     runtime.shutdown();

//     assert!(shared.shutdown.load(Ordering::Relaxed));
//     assert!(scheduler.tasks.is_closed());
//     assert_eq!(scheduler.tasks.len(), n);

//     // Make sure we did not have to wait for the tasks to complete
//     assert!(start.elapsed() < Duration::from_secs(1));
//     for handle in handles {
//         assert!(handle.is_finished());
//         assert!(handle.get_result().is_err_and(|e| e.is_cancelled()));
//     }

//     Ok(())
// }
