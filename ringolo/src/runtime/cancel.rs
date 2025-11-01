use crate::context;
use crate::runtime::{TaskMetadata, get_orphan_root};
use crate::task::CancellationStats;

#[derive(thiserror::Error, Debug)]
pub enum CancelError {
    #[error("No task in context. This function should be called within a future.")]
    EmptyContext,
}

pub fn recursive_cancel_all() -> Result<CancellationStats, CancelError> {
    let curr = context::current_task().ok_or(CancelError::EmptyContext)?;
    Ok(curr.recursive_cancel_all())
}

pub fn recursive_cancel_leaf() -> Result<CancellationStats, CancelError> {
    let curr = context::current_task().ok_or(CancelError::EmptyContext)?;
    Ok(curr.recursive_cancel_leaf())
}

pub fn recursive_cancel_any_metadata(
    metadata: &TaskMetadata,
) -> Result<CancellationStats, CancelError> {
    let curr = context::current_task().ok_or(CancelError::EmptyContext)?;
    Ok(curr.recursive_cancel_any_metadata(metadata))
}

pub fn recursive_cancel_all_metadata(
    metadata: &TaskMetadata,
) -> Result<CancellationStats, CancelError> {
    let curr = context::current_task().ok_or(CancelError::EmptyContext)?;
    Ok(curr.recursive_cancel_all_metadata(metadata))
}

pub fn recursive_cancel_all_orphans() -> Result<CancellationStats, CancelError> {
    let orphan_root = get_orphan_root();
    Ok(orphan_root.recursive_cancel_all())
}

#[cfg(test)]
mod tests {
    use crate::future::experimental::time::{Sleep, YieldNow};
    use crate::runtime::runtime::Kind;
    use crate::runtime::{AddMode, Builder, OrphanPolicy, Runtime, get_orphan_root};
    use crate::spawn::{TaskMetadata, TaskOpts};
    use crate::task::JoinHandle;
    use crate::test_utils::{init_local_runtime_and_context, init_stealing_runtime_and_context};
    use crate::{self as ringolo, context};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    const WORKER_THREADS: usize = 2;

    fn init_runtime(kind: Kind) -> Result<Runtime> {
        Ok(match kind {
            Kind::Local => {
                let builder = Builder::new_local();
                init_local_runtime_and_context(Some(builder))?.0
            }
            Kind::Stealing => {
                let builder = Builder::new_stealing().worker_threads(WORKER_THREADS);
                init_stealing_runtime_and_context(Some(builder))?.0
            }
        })
    }

    #[rstest]
    #[case::local(Kind::Local)]
    #[case::stealing(Kind::Stealing)]
    fn test_cancelling_node_creates_orphans(#[case] kind: Kind) -> Result<()> {
        // Need to explicilty disable OrphanPolicy as the default is Enforced where
        // we are not allowed to create orphans.
        let runtime = match kind {
            Kind::Local => {
                let builder = Builder::new_local().orphan_policy(OrphanPolicy::Permissive);
                init_local_runtime_and_context(Some(builder))?.0
            }
            Kind::Stealing => {
                let builder = Builder::new_stealing().orphan_policy(OrphanPolicy::Permissive);
                init_stealing_runtime_and_context(Some(builder))?.0
            }
        };

        let res: anyhow::Result<()> = runtime.block_on(async {
            let n = 3;
            let num_polled = Arc::new(AtomicUsize::new(0));
            let num_polled_clone = num_polled.clone();

            let cancelled: JoinHandle<Result<()>> = ringolo::spawn_builder()
                .with_metadata(TaskMetadata::from(["dead"]))
                .spawn(async move {
                    let orphans = (0..n)
                        .map(|_| {
                            let num_polled = Arc::clone(&num_polled_clone);
                            ringolo::spawn(async move {
                                num_polled.fetch_add(1, Ordering::Relaxed);
                                Sleep::try_new(Duration::from_secs(10))?
                                    .await
                                    .map_err(anyhow::Error::from)
                            })
                        })
                        .collect::<Vec<_>>();

                    let parent = context::current_task().context("no task in context")?;
                    assert_eq!(parent.num_children(), n);

                    for orphan in orphans {
                        orphan.await??;
                    }

                    Ok(())
                });

            // Make sure we spawn all nodes
            while num_polled.load(Ordering::Relaxed) < n {
                assert!(YieldNow::new(Some(AddMode::Fifo)).await.is_ok());
            }

            // Cancel parent
            let stats = ringolo::recursive_cancel_all_metadata(&TaskMetadata::from(["dead"]))?;
            assert_eq!(stats.cancelled, 1);
            assert_eq!(stats.visited, n + 1);

            // Check orphanage - only happens after we drop the join_handle
            let orphan_root = get_orphan_root();
            assert_eq!(orphan_root.num_children(), 0);
            drop(cancelled);
            assert_eq!(orphan_root.num_children(), n);

            // Now cancel remaining orphans
            let stats = ringolo::recursive_cancel_all_orphans()?;
            assert_eq!(stats.cancelled, n);
            assert_eq!(stats.visited, n);

            // Orphans remove themselves from orphanage
            let orphan_root = get_orphan_root();
            assert_eq!(orphan_root.num_children(), 0);

            Ok(())
        });

        assert!(res.is_ok());
        Ok(())
    }

    #[rstest]
    #[case::local_3(Kind::Local, 3)]
    #[case::local_5(Kind::Local, 5)]
    #[case::stealing_3(Kind::Stealing, 3)]
    #[case::stealing_5(Kind::Stealing, 5)]
    #[test]
    fn test_recursive_cancel_all_not_started(#[case] kind: Kind, #[case] n: usize) -> Result<()> {
        let runtime = init_runtime(kind)?;

        let res: Result<()> = runtime.block_on(async move {
            let handles = (0..n)
                .map(|_| {
                    ringolo::spawn(async {
                        Sleep::try_new(Duration::from_secs(10))?
                            .await
                            .map_err(anyhow::Error::from)
                    })
                })
                .collect::<Vec<_>>();

            // (1) Cancel all spawned tasks
            let stats = ringolo::recursive_cancel_all().context("failed to cancel all")?;
            assert_eq!(stats.visited, n);
            assert_eq!(stats.cancelled, n);

            let parent = context::current_task().context("no task in context")?;
            assert!(!parent.has_children());

            // (2) Cancelling a second time is safe - cancelling is idempotent, also it did not
            //     invalidate the current task.
            let stats = ringolo::recursive_cancel_all().context("failed to cancel all")?;
            assert_eq!(stats.visited, 0);
            assert_eq!(stats.cancelled, 0);

            for handle in handles {
                assert!(matches!(handle.await, Err(join_err) if join_err.is_cancelled()));
            }

            Ok(())
        });

        assert!(res.is_ok());
        Ok(())
    }

    #[rstest]
    #[case::local_3(Kind::Local, 3)]
    #[case::local_5(Kind::Local, 5)]
    #[case::stealing_3(Kind::Stealing, 3)]
    #[case::stealing_5(Kind::Stealing, 5)]
    #[test]
    fn test_recursive_cancel_all_no_wait(#[case] kind: Kind, #[case] n: usize) -> Result<()> {
        let start = Instant::now();
        let runtime = init_runtime(kind)?;

        let res: Result<()> = runtime.block_on(async move {
            let num_polled = Arc::new(AtomicUsize::new(0));

            let handles = (0..n)
                .map(|_| {
                    let num_polled = Arc::clone(&num_polled);
                    ringolo::spawn(async move {
                        num_polled.fetch_add(1, Ordering::Relaxed);
                        Sleep::try_new(Duration::from_secs(10))?
                            .await
                            .map_err(anyhow::Error::from)
                    })
                })
                .collect::<Vec<_>>();

            // Yield to scheduler, putting this task at the back of the queue and
            // forcing the scheduler to poll all of the spawned tasks.
            while num_polled.load(Ordering::Relaxed) < n {
                assert!(YieldNow::new(Some(AddMode::Fifo)).await.is_ok());
            }

            let stats = ringolo::recursive_cancel_all().context("failed to cancel all")?;
            assert_eq!(stats.visited, n);
            assert_eq!(stats.cancelled, n);

            for handle in handles {
                let res = handle.await;
                assert!(res.is_err());
                assert!(res.unwrap_err().is_cancelled());
            }

            Ok(())
        });

        // Make sure we did not wait for sleep to finish.
        assert!(res.is_ok());
        assert!(start.elapsed() < Duration::from_secs(1));
        Ok(())
    }

    #[rstest]
    #[case::local_3(Kind::Local, 3)]
    #[case::local_5(Kind::Local, 5)]
    #[case::stealing_3(Kind::Stealing, 3)]
    #[case::stealing_5(Kind::Stealing, 5)]
    #[test]
    fn test_recursive_cancel_all_on_exit(#[case] kind: Kind, #[case] n: usize) -> Result<()> {
        let start = Instant::now();
        let runtime = init_runtime(kind)?;

        let res: Result<()> = runtime.block_on(async move {
            let num_polled = Arc::new(AtomicUsize::new(0));
            let num_polled_cloned = Arc::clone(&num_polled);

            let parent = ringolo::spawn_builder()
                .with_opts(TaskOpts::CANCEL_ALL_CHILDREN_ON_EXIT)
                .spawn(async move {
                    let _handles = (0..n)
                        .map(|_| {
                            let num_polled = Arc::clone(&num_polled_cloned);
                            ringolo::spawn(async move {
                                num_polled.fetch_add(1, Ordering::Relaxed);
                                Sleep::try_new(Duration::from_secs(10))?
                                    .await
                                    .map_err(anyhow::Error::from)
                            })
                        })
                        .collect::<Vec<_>>();

                    let node = context::current_task().context("no task in context")?;
                    assert_eq!(node.num_children(), n);

                    num_polled_cloned.fetch_add(1, Ordering::Relaxed);
                    Sleep::try_new(Duration::from_secs(10))?
                        .await
                        .map_err(anyhow::Error::from)
                });

            let root_node = context::current_task().context("no task in context")?;
            assert_eq!(
                root_node.num_children(),
                if matches!(kind, Kind::Local) {
                    2 // maintenance root_worker + parent
                } else {
                    WORKER_THREADS + 2 // maintenance root_worker + 2 * maintenance worker + parent
                },
                "Unexpected number of children, should be 1 + maintenance task(s)."
            );

            // Yield to scheduler, putting this task at the back of the queue and
            // forcing the scheduler to poll all of the spawned tasks.
            while num_polled.load(Ordering::Relaxed) < n + 1 {
                assert!(YieldNow::new(Some(AddMode::Fifo)).await.is_ok());
            }

            parent.abort();
            assert!(matches!(parent.await, Err(join_err) if join_err.is_cancelled()));

            let expected_num_children = if matches!(kind, Kind::Local) {
                1 // maintenance root_worker
            } else {
                WORKER_THREADS + 1 // maintenance root_worker + 2 * maintenance worker
            };

            // Wait for children to be cancelled after parent cancelled. On abort
            // parent is re-scheduled, so we have to await this before children are dropped.
            while root_node.num_children() != expected_num_children {
                assert!(YieldNow::new(Some(AddMode::Fifo)).await.is_ok());
            }

            Ok(())
        });

        assert!(res.is_ok());
        assert!(start.elapsed() < Duration::from_secs(1));
        Ok(())
    }

    #[rstest]
    #[case::max_depth_5_linear_local(Kind::Local, 5, 1)]
    #[case::max_depth_3_binary_local(Kind::Local, 3, 2)]
    #[case::max_depth_2_three_local(Kind::Local, 2, 3)]
    #[case::max_depth_5_linear_stealing(Kind::Stealing, 5, 1)]
    #[case::max_depth_3_binary_stealing(Kind::Stealing, 3, 2)]
    #[case::max_depth_2_three_stealing(Kind::Stealing, 2, 3)]
    #[test]
    fn test_recursive_cancel_all_leaf(
        #[case] kind: Kind,
        #[case] max_depth: usize,
        #[case] branching: usize,
    ) -> Result<()> {
        // Helper to recursively spawn child tasks
        fn recursive_spawn(
            leaf_nodes: Arc<AtomicUsize>,
            current_depth: usize,
            max_depth: usize,
            branching: usize,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
            Box::pin(async move {
                if current_depth > max_depth {
                    leaf_nodes.fetch_add(1, Ordering::Relaxed);

                    // Leaf node will sleep for a very long time.
                    return Sleep::try_new(Duration::from_secs(10))?
                        .await
                        .map_err(anyhow::Error::from);
                }

                let mut handles = Vec::new();
                for _ in 0..branching {
                    handles.push(ringolo::spawn(recursive_spawn(
                        Arc::clone(&leaf_nodes),
                        current_depth + 1,
                        max_depth,
                        branching,
                    )));
                }

                for handle in handles {
                    let _ = handle.await;
                }

                Ok(())
            })
        }

        let start = Instant::now();
        let runtime = init_runtime(kind)?;

        let res: Result<()> = runtime.block_on(async move {
            let leaf_nodes = Arc::new(AtomicUsize::new(0));
            let expected_leaf_nodes = branching.pow((max_depth + 1) as u32);

            let handle = ringolo::spawn(recursive_spawn(
                Arc::clone(&leaf_nodes),
                0,
                max_depth,
                branching,
            ));

            // Yield to scheduler until we've spawned all of the leaf nodes.
            while leaf_nodes.load(Ordering::Relaxed) < expected_leaf_nodes {
                assert!(YieldNow::new(Some(AddMode::Fifo)).await.is_ok());
            }

            let stats = ringolo::recursive_cancel_leaf().context("failed to cancel leaf")?;
            assert_eq!(stats.cancelled, expected_leaf_nodes);

            handle.await??;

            Ok(())
        });

        assert!(res.is_ok());
        assert!(start.elapsed() < Duration::from_secs(1));
        Ok(())
    }

    #[rstest]
    #[case::max_depth_5_linear_local(Kind::Local, 5, 1)]
    #[case::max_depth_3_binary_local(Kind::Local, 3, 2)]
    #[case::max_depth_2_three_local(Kind::Local, 2, 3)]
    #[case::max_depth_5_linear_stealing(Kind::Stealing, 5, 1)]
    #[case::max_depth_3_binary_stealing(Kind::Stealing, 3, 2)]
    #[case::max_depth_2_three_stealing(Kind::Stealing, 2, 3)]
    #[test]
    fn test_recursive_cancel_metadata(
        #[case] kind: Kind,
        #[case] max_depth: usize,
        #[case] branching: usize,
    ) -> Result<()> {
        enum Color {
            Red,
            Black,
        }

        impl Color {
            fn flip(&self) -> Color {
                match self {
                    Color::Red => Color::Black,
                    Color::Black => Color::Red,
                }
            }

            fn metadata(&self) -> TaskMetadata {
                match self {
                    Color::Red => TaskMetadata::from(["red"]),
                    Color::Black => TaskMetadata::from(["black"]),
                }
            }
        }

        // Helper to recursively spawn child tasks
        fn recursive_spawn(
            leaf_nodes: Arc<AtomicUsize>,
            color: Color,
            current_depth: usize,
            max_depth: usize,
            branching: usize,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
            Box::pin(async move {
                if current_depth > max_depth {
                    leaf_nodes.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }

                let mut handles = Vec::new();

                for _ in 0..branching {
                    let next_color = color.flip();
                    handles.push(
                        ringolo::spawn_builder()
                            .with_metadata(next_color.metadata())
                            .spawn(recursive_spawn(
                                Arc::clone(&leaf_nodes),
                                next_color,
                                current_depth + 1,
                                max_depth,
                                branching,
                            )),
                    );
                }

                // All reds sleep for a very long time.
                if matches!(color, Color::Red) {
                    let _ = Sleep::try_new(Duration::from_secs(10))?.await?;
                }

                for handle in handles {
                    let _ = handle.await;
                }

                Ok(())
            })
        }

        let start = Instant::now();
        let start_color = Color::Black;

        let runtime = init_runtime(kind)?;

        let res: Result<()> = runtime.block_on(async move {
            let leaf_nodes = Arc::new(AtomicUsize::new(0));
            let expected_leaf_nodes = branching.pow((max_depth + 1) as u32);

            let handle = ringolo::spawn_builder()
                .with_metadata(start_color.metadata())
                .spawn(recursive_spawn(
                    Arc::clone(&leaf_nodes),
                    start_color,
                    0,
                    max_depth,
                    branching,
                ));

            // Yield to scheduler until we've spawned all of the leaf nodes.
            while leaf_nodes.load(Ordering::Relaxed) < expected_leaf_nodes {
                assert!(YieldNow::new(Some(AddMode::Fifo)).await.is_ok());
            }

            let _ = ringolo::recursive_cancel_any_metadata(&Color::Red.metadata())
                .context("failed to cancel red nodes")?;
            handle.await??;

            Ok(())
        });

        assert!(res.is_ok());
        assert!(start.elapsed() < Duration::from_secs(1));
        Ok(())
    }
}
