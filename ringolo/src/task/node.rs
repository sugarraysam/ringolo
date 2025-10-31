use dashmap::DashMap;
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::fmt;
use std::ops::Deref;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use crate::context;
use crate::runtime::{
    OrphanPolicy, SPILL_TO_HEAP_THRESHOLD, TaskMetadata, TaskOpts, TaskRegistry, get_orphan_root,
    get_root,
};
use crate::task::Id;

// We implement Structured Concurrency and track the parent<->children relationship
// of tasks. This is useful to allow implementing cancellation APIs that don't require
// passing tokens around and put the responsibility on the user to periodically
// check for cancellation signals.
pub(crate) struct TaskNode {
    pub(crate) id: Id,

    pub(crate) opts: TaskOpts,

    pub(crate) metadata: Option<TaskMetadata>,

    // A thread-safe map of children.
    // - We use Arc<TaskNode> to share ownership with the parent.
    // - We use a DashMap for fast (O(1)) child removal.
    // - We Lazily initialize the children map to reduce memory usage because
    //   a majority of tasks will be "leaf nodes".
    children: LazyLock<DashMap<Id, Arc<TaskNode>>>,

    // This is an optimization because we want to check if we have children in O(1)
    // and avoid initializing the DashMap if it was unused. Also, the DashMap
    // `is_empty()` implementation calls `len() == 0` which iterates through all
    // shards so we want to avoid that.
    child_count: AtomicUsize,

    // A weak reference to the parent. This MUST be Weak to prevent reference cycles.
    // The reason we need a mutex is we need to swap the parent when
    parent: Mutex<Option<Weak<TaskNode>>>,

    registry: Arc<dyn TaskRegistry>,

    released: AtomicBool,
}

impl TaskNode {
    /// We create two flavors of root:
    /// (1) ROOT_NODE: for the regular task tree.
    /// (2) ORPHAN_ROOT: to adopt all orphans created through cancellation.
    ///
    /// This allows writing clean APIs with separation of concerns. Also if no
    /// orphans are created the cost is just about zero.
    pub(crate) fn new_root(root_id: Id, registry: Arc<dyn TaskRegistry>) -> Self {
        Self {
            id: root_id,
            opts: TaskOpts::default(),
            metadata: None,

            // (1) Tune-down the number of shards on the dashmap because default is
            //     `shards == num_cores * 4`.
            // (2) Let's have more shards for the root nodes vs. regular node as
            //     we expect it should have more children.
            children: LazyLock::new(|| DashMap::with_shard_amount(8)),
            child_count: AtomicUsize::new(0),
            parent: Mutex::new(None),
            registry,
            released: AtomicBool::new(false),
        }
    }

    pub(crate) fn new(
        id: Id,
        opts: Option<TaskOpts>,
        metadata: Option<TaskMetadata>,
        registry: Arc<dyn TaskRegistry>,
    ) -> Arc<Self> {
        let opts = opts.unwrap_or_default();

        // Background tasks are owned by the root and can outlive the parent.
        let parent = if opts.contains(TaskOpts::BACKGROUND_TASK) {
            get_root()
        } else {
            // There are two edge cases where tasks are spawned *outside* of polling,
            // meaning the context `current_task` will be None. For both of these,
            // we default to setting the ROOT_NODE as the parent.
            // (1) task spawned before calling `block_on`
            // (2) cleanup tasks spawned in Drop impl
            context::current_task().unwrap_or(get_root())
        };

        let child = Arc::new(TaskNode {
            id,
            opts,
            metadata,
            // Tune-down the number of shards on the dashmap because default is
            // `shards == num_cores * 4`.
            children: LazyLock::new(|| DashMap::with_shard_amount(4)),
            child_count: AtomicUsize::new(0),
            parent: Mutex::new(Some(Arc::downgrade(&parent))),
            registry,
            released: AtomicBool::new(false),
        });

        // # Safety
        //
        // Cancellation happens at a specific point in time. It does not
        // invalidate the parent. It should be safe to spawn new children, and
        // have the parent call cancel again on this new batch of children. What
        // is not allowed is to cancel a child twice.
        parent.add_child(child.id, Arc::clone(&child));
        child
    }

    fn parent(&self) -> Option<Arc<TaskNode>> {
        self.parent.lock().as_ref().and_then(|p| p.upgrade())
    }

    fn swap_parent(&self, new_parent: Weak<TaskNode>) -> Option<Weak<TaskNode>> {
        self.parent.lock().replace(new_parent)
    }

    fn add_child(&self, k: Id, v: Arc<TaskNode>) {
        self.child_count.fetch_add(1, Ordering::Relaxed);
        let old = self.children.insert(k, v);
        debug_assert!(old.is_none());
    }

    fn remove_child(&self, k: &Id) -> Option<Id> {
        self.children
            .remove(k)
            .inspect(|_| {
                self.child_count.fetch_sub(1, Ordering::Relaxed);
            })
            .map(|(id, _)| id)
    }

    // Concurrent inserts are possible as we lock each shard one-by-one. As
    // mentioned above, this should be fine as we are cancelling at *a specific
    // point in time*, and the parent is still valid after.
    fn take_all_children(&self) -> SmallVec<[Arc<TaskNode>; SPILL_TO_HEAP_THRESHOLD]> {
        // Move the Arc's instead of cloning by leveraging dashmap raw-api.
        let children = self.children.shards().iter().fold(
            SmallVec::with_capacity(self.child_count.load(Ordering::Relaxed)),
            |mut acc, shard| {
                acc.extend(shard.write().drain().map(|(_, v)| v.into_inner()));
                acc
            },
        );

        self.child_count
            .fetch_sub(children.len(), Ordering::Relaxed);

        children
    }

    // Same comment as above applies for concurrent inserts.
    fn take_children_predicate<F>(
        &self,
        predicate: F,
    ) -> SmallVec<[Arc<TaskNode>; SPILL_TO_HEAP_THRESHOLD]>
    where
        F: Fn(&TaskNode) -> bool,
    {
        let mut children = SmallVec::new();

        self.children.retain(|_, child| {
            if predicate(child) {
                children.push(Arc::clone(child));
                false
            } else {
                true
            }
        });

        self.child_count
            .fetch_sub(children.len(), Ordering::Relaxed);

        children
    }

    pub(crate) fn clone_children(&self) -> SmallVec<[Arc<TaskNode>; SPILL_TO_HEAP_THRESHOLD]> {
        self.children
            .iter()
            .map(|e| Arc::clone(e.value()))
            .collect()
    }

    // Checks if we have children in O(1) without accidentally initializing the
    // children DashMap.
    pub(crate) fn has_children(&self) -> bool {
        self.child_count.load(Ordering::Relaxed) > 0
    }

    pub(crate) fn num_children(&self) -> usize {
        self.child_count.load(Ordering::Relaxed)
    }

    pub(crate) fn num_children_recursive(&self) -> usize {
        let mut total = 0;
        let mut to_visit = self.clone_children();

        while !to_visit.is_empty() {
            // Safety: we just checked to_visit is not empty.
            let curr = to_visit.pop().unwrap();
            total += 1;
            to_visit.extend(curr.clone_children());
        }

        total
    }

    fn has_any_metadata(&self, metadata: &TaskMetadata) -> bool {
        self.metadata
            .as_ref()
            .is_some_and(|m| !m.is_disjoint(metadata))
    }

    fn has_all_metadata(&self, metadata: &TaskMetadata) -> bool {
        self.metadata
            .as_ref()
            .is_some_and(|m| m.is_superset(metadata))
    }

    /// Shuts down the task.
    fn shutdown(&self) {
        // We want to go through the `Task<S>::shutdown()` path to ensure we manage
        // ref counts properly.
        self.registry.shutdown(&self.id);
    }

    /// Called when we release the underlying `Task<S>`, either through normal task
    /// lifecycle or cancellation.
    pub(super) fn release(&self) {
        // If this TaskNode was released through one of the cancel APIs, no need
        // to remove it from the parent, Prevents thundering herd issue when
        // parent cancels many children.
        if !self.id.is_root() && !self.released.swap(true, Ordering::AcqRel) {
            let parent = self.parent.lock();

            if let Some(parent) = parent.as_ref().and_then(|p| p.upgrade()) {
                let found = parent.remove_child(&self.id);
                debug_assert!(found.is_some(), "Child not found in parent.");
            }
        }
    }

    // Orphan the child, by swapping it's parent and adding it to the orphan
    // task tree. The reason we keep track of orphans in a separate tree is to
    // have the possibility of cancelling them, and also clearly separate concerns
    // depending on if the user cares about Structured Concurrency or not.
    fn orphan(self: Arc<Self>) {
        let orphan_root = get_orphan_root();
        self.swap_parent(Arc::downgrade(&orphan_root));
        orphan_root.add_child(self.id, Arc::clone(&self));
    }
}

// Skip both parent and children to avoid printing the whole tree and
// also avoid initializing children.
impl fmt::Debug for TaskNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskNode")
            .field("id", &self.id)
            .field("opts", &self.opts)
            .field("metadata", &self.metadata)
            .field("child_count", &self.child_count)
            .field("released", &self.released)
            .finish()
    }
}

impl Drop for TaskNode {
    fn drop(&mut self) {
        if self.has_children() && !self.registry.is_closed() {
            let policy_enforced = matches!(self.registry.orphan_policy(), OrphanPolicy::Enforced);
            let cancel_on_exit = self.opts.contains(TaskOpts::CANCEL_ALL_CHILDREN_ON_EXIT);

            if policy_enforced || cancel_on_exit {
                let stats = self.recursive_cancel_all();
                let msg = if policy_enforced {
                    "OrphanPolicy is enforced"
                } else {
                    "parent explicitly set TaskOpts::CANCEL_ALL_CHILDREN_ON_EXIT"
                };

                // TODO: tracing
                eprintln!(
                    "WARNING: Cancelled {} child task(s) to prevent orphans because {}.",
                    stats.cancelled, msg
                );
            } else {
                // Otherwise, we orphan the children.
                self.take_all_children().into_iter().for_each(|child| {
                    child.orphan();
                });
            }
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CancellationStats {
    /// Total number of task nodes visited during the traversal.
    pub visited: usize,
    /// Number of tasks that matched the predicate and were shut down.
    pub cancelled: usize,
}

impl CancellationStats {
    pub fn new(visited: usize) -> Self {
        Self {
            visited,
            cancelled: 0,
        }
    }
}

impl std::ops::AddAssign for CancellationStats {
    fn add_assign(&mut self, rhs: Self) {
        self.visited += rhs.visited;
        self.cancelled += rhs.cancelled;
    }
}

impl TaskNode {
    /// Recursively cancels all node.
    pub(crate) fn recursive_cancel_all(&self) -> CancellationStats {
        recursive_cancel_all(self)
    }

    /// Recursively cancels all node that are "leaf" nodes (have no children).
    pub(crate) fn recursive_cancel_leaf(&self) -> CancellationStats {
        recursive_cancel_predicate(self, |node| !node.has_children())
    }

    /// Recursively cancels all node that matches "any" metadata.
    pub(crate) fn recursive_cancel_any_metadata(
        &self,
        metadata: &TaskMetadata,
    ) -> CancellationStats {
        recursive_cancel_predicate(self, |node| node.has_any_metadata(metadata))
    }

    /// Recursively cancels all node that matches "all" metadata.
    pub(crate) fn recursive_cancel_all_metadata(
        &self,
        metadata: &TaskMetadata,
    ) -> CancellationStats {
        recursive_cancel_predicate(self, |node| node.has_all_metadata(metadata))
    }
}

/// Recursively traverses the tree downwards, cancelling all nodes and collecting stats.
fn recursive_cancel_all(node: &TaskNode) -> CancellationStats {
    if !node.has_children() {
        return CancellationStats::default();
    }

    node.take_all_children()
        .into_iter()
        .fold(CancellationStats::default(), |mut stats, child| {
            stats.visited += 1;

            debug_assert!(
                !child.released.load(Ordering::Acquire),
                "Child should not be cancelled twice."
            );

            if !child.released.swap(true, Ordering::AcqRel) {
                child.shutdown();
                stats.cancelled += 1;
            }

            stats += recursive_cancel_all(&child);
            stats
        })
}

enum Cancel {
    Yes(Arc<TaskNode>),
    No(Arc<TaskNode>),
}

impl Cancel {
    fn should_cancel(&self) -> bool {
        matches!(self, Cancel::Yes(_))
    }
}

impl Deref for Cancel {
    type Target = TaskNode;
    fn deref(&self) -> &Self::Target {
        match self {
            Cancel::Yes(node) => node,
            Cancel::No(node) => node,
        }
    }
}

/// Recursively traverses the tree downwards, cancelling tasks and collecting stats.
fn recursive_cancel_predicate<F>(node: &TaskNode, predicate: F) -> CancellationStats
where
    F: Copy + Fn(&TaskNode) -> bool,
{
    let n = node.num_children();
    if n == 0 {
        return CancellationStats::default();
    }

    let mut children = Vec::with_capacity(n);

    node.children.retain(|_id, child| {
        if predicate(child) {
            children.push(Cancel::Yes(Arc::clone(child)));
            false
        } else {
            children.push(Cancel::No(Arc::clone(child)));
            true
        }
    });

    children
        .into_iter()
        .fold(CancellationStats::default(), |mut stats, child| {
            stats.visited += 1;

            if child.should_cancel() {
                debug_assert!(
                    !child.released.load(Ordering::Acquire),
                    "Child should not be cancelled twice."
                );

                if !child.released.swap(true, Ordering::AcqRel) {
                    child.shutdown();
                    stats.cancelled += 1;
                }
            }

            stats += recursive_cancel_predicate(&child, predicate);
            stats
        })
}

/// Set and clear the task node in the context when the future is executed or
/// dropped, or when the output produced by the future is dropped.
pub(crate) struct TaskNodeGuard {
    parent_task: Option<Arc<TaskNode>>,
}

impl TaskNodeGuard {
    pub(crate) fn enter_root_node() -> Self {
        let parent_task = context::set_current_task(Some(get_root()));
        debug_assert!(
            parent_task.is_none(),
            "ROOT_NODE by definition has no parent."
        );

        TaskNodeGuard { parent_task }
    }

    pub(super) fn enter(task: Arc<TaskNode>) -> Self {
        TaskNodeGuard {
            parent_task: context::set_current_task(Some(task)),
        }
    }
}

impl Drop for TaskNodeGuard {
    fn drop(&mut self) {
        context::set_current_task(self.parent_task.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as ringolo;
    use crate::runtime::{TaskMetadata, TaskOpts};
    use crate::task::JoinHandle;
    use crate::task::id::ROOT_ID;
    use anyhow::{Context, Result};
    use rstest::rstest;
    use std::pin::Pin;

    #[rstest]
    // All tasks on local scheduler are sticky
    #[case::one(TaskOpts::STICKY, TaskMetadata::default())]
    #[case::two(TaskOpts::STICKY | TaskOpts::BACKGROUND_TASK, TaskMetadata::from(["a","b","c"]))]
    #[case::three(TaskOpts::STICKY | TaskOpts::CANCEL_ALL_CHILDREN_ON_EXIT | TaskOpts::BACKGROUND_TASK, TaskMetadata::from(["d","e"]))]
    #[ringolo::test]
    async fn test_spawn_task_with_opts_and_metadata(
        #[case] opts: TaskOpts,
        #[case] metadata: TaskMetadata,
    ) -> Result<()> {
        let handle: JoinHandle<Result<()>> = ringolo::spawn_builder()
            .with_opts(opts)
            .with_metadata(metadata.clone())
            .spawn(async move {
                let node = context::current_task().context("no task node in context")?;

                crate::with_scheduler!(|s| {
                    assert_eq!(node.opts, opts | s.default_task_opts);
                    assert_eq!(node.metadata, Some(metadata));
                });

                Ok(())
            });

        assert!(handle.await.is_ok());

        Ok(())
    }

    #[ringolo::test]
    async fn test_background_task_parent_is_root() -> Result<()> {
        let parent: JoinHandle<Result<()>> = ringolo::spawn(async {
            let child: JoinHandle<Result<()>> = ringolo::spawn_builder()
                .with_opts(TaskOpts::BACKGROUND_TASK)
                .spawn(async move {
                    let node = context::current_task().context("no task node in context")?;
                    let parent = node.parent().context("expected parent")?;
                    assert!(parent.id.is_root());
                    Ok(())
                });

            assert!(child.await.is_ok());
            Ok(())
        });

        assert!(parent.await.is_ok());

        Ok(())
    }

    #[rstest]
    #[case::three(3)]
    #[case::three(5)]
    #[ringolo::test]
    async fn test_children_map_lifecycle(#[case] n: usize) -> Result<()> {
        let node = context::current_task().context("no task node in context")?;

        assert_eq!(node.num_children(), 0);
        assert!(!node.has_children());

        let handles: Vec<JoinHandle<Result<()>>> = (0..n)
            .into_iter()
            .map(|_| ringolo::spawn(async { Ok(()) }))
            .collect();

        assert!(node.has_children());
        assert_eq!(node.num_children(), n);

        for handle in handles {
            assert!(handle.await.is_ok());
        }

        assert!(!node.has_children());

        Ok(())
    }

    #[rstest]
    #[case::max_depth_5_linear(5, 1)]
    #[case::max_depth_3_binary(3, 2)]
    #[case::max_depth_2_three(2, 3)]
    #[ringolo::test]
    async fn test_parent_child_relationships(
        #[case] max_depth: usize,
        #[case] branching: usize,
    ) -> Result<()> {
        // Helper to recursively spawn child tasks
        fn recursive_spawn(
            max_tree_size: Arc<AtomicUsize>,
            curr_tree_size: Arc<AtomicUsize>,
            expected_parent_id: Id,
            current_depth: usize,
            max_depth: usize,
            branching: usize,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
            max_tree_size.fetch_add(1, Ordering::Relaxed);
            curr_tree_size.fetch_add(1, Ordering::Relaxed);

            Box::pin(async move {
                if current_depth > max_depth {
                    let got_curr_tree_size = get_root().num_children_recursive() + 1;
                    assert_eq!(got_curr_tree_size, curr_tree_size.load(Ordering::Relaxed));

                    curr_tree_size.fetch_sub(1, Ordering::Relaxed);
                    return Ok(());
                }

                let node = ringolo::context::current_task().context("no task node in context")?;
                let parent = node.parent().context("expected parent")?;
                assert_eq!(parent.id, expected_parent_id);

                if current_depth == 0 {
                    assert!(parent.id.is_root());
                }

                let mut handles = Vec::new();
                for _ in 0..branching {
                    handles.push(ringolo::spawn(recursive_spawn(
                        Arc::clone(&max_tree_size),
                        Arc::clone(&curr_tree_size),
                        node.id,
                        current_depth + 1,
                        max_depth,
                        branching,
                    )));
                }

                for handle in handles {
                    handle.await??;
                }

                curr_tree_size.fetch_sub(1, Ordering::Relaxed);
                Ok(())
            })
        }

        let max_tree_size = Arc::new(AtomicUsize::new(1));
        let curr_tree_size = Arc::new(AtomicUsize::new(1));

        let handle = ringolo::spawn(recursive_spawn(
            Arc::clone(&max_tree_size),
            Arc::clone(&curr_tree_size),
            ROOT_ID,
            0,
            max_depth,
            branching,
        ));

        handle.await??;

        // Make sure we spawned the expected number of nodes.
        let mut expected_max_tree_size = 1;
        for i in 0..=(max_depth + 1) {
            expected_max_tree_size += branching.pow(i as u32);
        }
        assert_eq!(
            expected_max_tree_size,
            max_tree_size.load(Ordering::Relaxed)
        );

        Ok(())
    }
}
