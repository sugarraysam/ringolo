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
    use crate::runtime::{AddMode, get_orphan_root};
    use crate::spawn::{TaskMetadata, TaskOpts};
    use crate::task::JoinHandle;
    use crate::{self as ringolo, context};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    #[ringolo::test]
    async fn test_cancelling_node_creates_orphans() -> Result<()> {
        let n = 3;

        let cancelled: JoinHandle<Result<()>> = ringolo::spawn_builder()
            .with_metadata(TaskMetadata::from(["dead"]))
            .spawn(async move {
                let orphans = (0..n)
                    .map(|_| {
                        ringolo::spawn(async {
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
        YieldNow::new(Some(AddMode::Fifo)).await?;

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
    }

    #[ringolo::test]
    async fn test_recursive_cancel_all_not_started() -> Result<()> {
        let n = 3;

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
    }

    #[ringolo::test]
    async fn test_recursive_cancel_all_no_wait() -> Result<()> {
        let start = Instant::now();
        let n = 3;
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
            YieldNow::new(Some(AddMode::Fifo)).await?;
        }

        let stats = ringolo::recursive_cancel_all().context("failed to cancel all")?;
        assert_eq!(stats.visited, n);
        assert_eq!(stats.cancelled, n);

        // Make sure we did not wait for sleep to finish.
        assert!(start.elapsed() < Duration::from_secs(1));

        for handle in handles {
            assert!(matches!(handle.await, Err(join_err) if join_err.is_cancelled()));
        }

        Ok(())
    }

    #[ringolo::test]
    async fn test_recursive_cancel_all_on_exit() -> Result<()> {
        let start = Instant::now();
        let n = 3;
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
                assert_eq!(node.num_children(), 3);

                num_polled_cloned.fetch_add(1, Ordering::Relaxed);
                Sleep::try_new(Duration::from_secs(10))?
                    .await
                    .map_err(anyhow::Error::from)
            });

        let root_node = context::current_task().context("no task in context")?;
        assert_eq!(root_node.num_children(), 1);

        // Yield to scheduler, putting this task at the back of the queue and
        // forcing the scheduler to poll all of the spawned tasks.
        while num_polled.load(Ordering::Relaxed) < n + 1 {
            YieldNow::new(Some(AddMode::Fifo)).await?;
        }

        parent.abort();
        assert!(matches!(parent.await, Err(join_err) if join_err.is_cancelled()));

        // All of parent's child got cancelled on exit and we did not have to wait
        // for sleep to finish.
        assert_eq!(root_node.num_children(), 0);
        assert!(start.elapsed() < Duration::from_secs(1));

        Ok(())
    }

    #[rstest]
    #[case::max_depth_5_linear(5, 1)]
    #[case::max_depth_3_binary(3, 2)]
    #[case::max_depth_2_three(2, 3)]
    #[ringolo::test]
    async fn test_recursive_cancel_all_leaf(
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
            YieldNow::new(Some(AddMode::Fifo)).await?;
        }

        let stats = ringolo::recursive_cancel_leaf().context("failed to cancel leaf")?;
        assert_eq!(stats.cancelled, expected_leaf_nodes);

        handle.await??;

        // Ensure we did not wait for sleep to terminate
        assert!(start.elapsed() < Duration::from_secs(1));

        Ok(())
    }

    #[rstest]
    #[case::max_depth_5_linear(5, 1)]
    #[case::max_depth_3_binary(3, 2)]
    #[case::max_depth_2_three(2, 3)]
    #[ringolo::test]
    async fn test_recursive_cancel_metadata(
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
            YieldNow::new(Some(AddMode::Fifo)).await?;
        }

        let _ = ringolo::recursive_cancel_any_metadata(&Color::Red.metadata())
            .context("failed to cancel red nodes")?;
        handle.await??;

        // Ensure we did not have to wait 10 secs and all red nodes got cancelled.
        assert!(start.elapsed() < Duration::from_secs(1));

        Ok(())
    }
}
