use crate::runtime::Schedule;
use crate::task::id::ROOT_FUTURE_ID;
use crate::task::{Id, Task};
use dashmap::DashMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

// We store two types of tasks in our OwnedTasks. The Root is treated differently
// as it *does not* have a proper task state. This means we can't call `shutdown`
// on it, and we also have to give it a special task ID.
#[derive(Debug)]
enum TaskInner<S: 'static> {
    Root(Id),
    Child(Task<S>),
}

impl<S: 'static> TaskInner<S> {
    fn new(task: Task<S>) -> Self {
        Self::Child(task)
    }

    fn new_root(root_id: Id) -> Self {
        Self::Root(root_id)
    }

    fn id(&self) -> Id {
        match self {
            TaskInner::Root(id) => *id,
            TaskInner::Child(task) => task.id(),
        }
    }

    fn shutdown(self)
    where
        S: Schedule,
    {
        match self {
            TaskInner::Root(_) => {}
            TaskInner::Child(task) => task.shutdown(),
        }
    }
}

// We leverate the Supervision Tree idea and track the parent<->children relationship
// of tasks. This is useful to allow implementing cancellation APIs that don't require
// passing tokens around and put the responsibility on the user to periodically
// check for cancellation signals.
#[derive(Debug)]
struct TaskNode<S: 'static> {
    // Make task "takable" as we need an owned reference to call shutdown
    task: Mutex<Option<TaskInner<S>>>,

    // A weak reference to the parent. This MUST be Weak to prevent reference cycles.
    parent: Option<Weak<TaskNode<S>>>,

    // A thread-safe map of children.
    // - We use Arc<TaskNode> to share ownership with the parent.
    // - We use a DashMap for fast (O(1)) child removal.
    // - We Lazily initialize the children map to reduce memory usage because
    //   a majority of tasks will be "leaf nodes".
    // - We also tune-down the number of shards on the dashmap for memory reason
    //   again. Last I look the default was `shards = num_cores * 4`.
    children: LazyLock<DashMap<Id, ()>>,
}

impl<S: 'static> TaskNode<S> {
    fn new_root(root_id: Id) -> Self {
        Self {
            task: Mutex::new(Some(TaskInner::Root(root_id))),
            parent: None,
            // Let's have more shards for the root future as we expect it should
            // have more children.
            children: LazyLock::new(|| DashMap::with_shard_amount(8)),
        }
    }

    fn new(task: Task<S>, parent: Option<Weak<TaskNode<S>>>) -> Self {
        Self {
            task: Mutex::new(Some(TaskInner::Child(task))),
            parent,
            children: LazyLock::new(|| DashMap::with_shard_amount(4)),
        }
    }

    fn shutdown(&self)
    where
        S: Schedule,
    {
        let Some(task) = self.task.lock().unwrap().take() else {
            return;
        };

        task.shutdown();
    }

    fn children_ids(&self) -> Vec<Id> {
        self.children.iter().map(|c| *c.key()).collect()
    }
}

// Implement Drop to automatically remove this node from its parent's child list.
impl<S> Drop for TaskNode<S> {
    fn drop(&mut self) {
        let Some(task_id) = self.task.lock().unwrap().as_ref().map(|t| t.id()) else {
            return;
        };

        if let Some(parent_weak) = &self.parent
            && let Some(parent) = parent_weak.upgrade()
        {
            let _ = parent.children.remove(&task_id);
        }
    }
}

// A collection of all tasks owned by the runtime. We copied the 3 reference model
// from tokio where each task is split into:
//
// 1. JoinHandle  :: owned by awaiter of task, claim to result
// 2. Notified<S> :: owned by the scheduler, goes in/out of scope as task suspends/resumes
// 3. Task<S>     :: owned by the runtime <--- this is what we store in OwnedTasks
//
// It is essential that we keep a reference alive to our tasks to implement things
// like shutdown but also because the Task module inner workings rely on this reference
// to make state transitions decision.
//
// We make the container thread-safe and it is a globally shared data structure, so
// we must minimize contention and ensure optimal performance.
#[derive(Debug, Clone)]
pub(crate) struct OwnedTasks<S: 'static> {
    tasks: Arc<DashMap<Id, Arc<TaskNode<S>>>>,

    shutdown: Arc<AtomicBool>,
}

impl<S: 'static> OwnedTasks<S> {
    pub(crate) fn new(capacity: usize) -> Self {
        let tasks = DashMap::with_capacity(capacity);

        let root_node = Arc::new(TaskNode::<S>::new_root(ROOT_FUTURE_ID));
        tasks.insert(ROOT_FUTURE_ID, root_node);

        Self {
            tasks: Arc::new(tasks),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Inserts a new task, returning `true` if we detected a collision on task ID.
    pub(crate) fn insert(&self, task: Task<S>, parent_id: Option<Id>) -> bool
    where
        S: Schedule,
    {
        // There are two edge cases where tasks are spawned *outside* of polling,
        // meaning the context `current_task_id` will be None. For both of these,
        // we default to setting the ROOT_FUTURE as the parent.
        // (1) task spawned before calling `block_on`
        // (2) cleanup tasks spawned in Drop impl
        let parent_id = parent_id.unwrap_or(ROOT_FUTURE_ID);

        if self.shutdown.load(Ordering::Acquire) {
            task.shutdown();
            return false;
        }

        let Some(parent_arc) = self.tasks.get(&parent_id).map(|p| Arc::clone(&p)) else {
            task.shutdown();
            return false;
        };

        let task_id = task.id();
        let task_node = Arc::new(TaskNode::new(task, Some(Arc::downgrade(&parent_arc))));

        // Register as a new child and in global map
        parent_arc.children.insert(task_id, ());
        self.tasks.insert(task_id, task_node).is_some()
    }

    /// Removes a task by its ID, returning it.
    pub(crate) fn remove(&self, id: &Id) -> Option<Task<S>> {
        self.tasks
            .remove(id)
            .and_then(|(_, node)| node.task.lock().unwrap().take())
            .and_then(|task| match task {
                TaskInner::Root(_) => None,
                TaskInner::Child(task) => Some(task),
            })
    }

    /// Returns the number of tasks currently owned by the executor.
    pub(crate) fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Returns the number of tasks currently owned by the executor.
    pub(crate) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Initiates a graceful shutdown by signaling all tasks.
    pub(crate) fn shutdown_all(&self)
    where
        S: Schedule,
    {
        // Shutdown only once.
        if !self.shutdown.swap(true, Ordering::Release) {
            // Can't get owned iterator so we first collect keys and then use
            // the remove API. We are shutting down anyways so performance is
            // not such a concern.
            let keys = self.tasks.iter().map(|e| *e.key()).collect::<Vec<_>>();

            keys.iter()
                .filter_map(|k| self.tasks.remove(k))
                .for_each(|(_, task)| {
                    task.shutdown();
                });
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

impl std::ops::AddAssign for CancellationStats {
    fn add_assign(&mut self, rhs: Self) {
        self.visited += rhs.visited;
        self.cancelled += rhs.cancelled;
    }
}

type CancelPredicate<S> = fn(&Arc<TaskNode<S>>) -> bool;

// Cancellation APIs
impl<S: Schedule> OwnedTasks<S> {
    /// Cancels all direct and indirect children of a given task.
    pub(crate) fn cancel_all_children(&self, parent_id: Id) -> CancellationStats
    where
        S: Schedule,
    {
        if let Some(node) = self.tasks.get(&parent_id) {
            self.recursive_shutdown::<CancelPredicate<S>>(node.clone(), None)
        } else {
            CancellationStats::default()
        }
    }

    /// Cancels only the children that are "leaf" nodes (have no children).
    pub(crate) fn cancel_all_leaf_children(&self, parent_id: Id) -> CancellationStats
    where
        S: Schedule,
    {
        if let Some(node) = self.tasks.get(&parent_id) {
            self.recursive_shutdown::<CancelPredicate<S>>(
                node.clone(),
                Some(|child| child.children.is_empty()),
            )
        } else {
            CancellationStats::default()
        }
    }

    /// Recursively traverses the tree downwards, cancelling tasks and collecting stats.
    fn recursive_shutdown<F>(
        &self,
        node: Arc<TaskNode<S>>,
        predicate: Option<F>,
    ) -> CancellationStats
    where
        S: Schedule,
        F: Copy + Fn(&Arc<TaskNode<S>>) -> bool,
    {
        if node.children.is_empty() {
            dbg!("nothing to cancel");
            return CancellationStats::default();
        }

        let children_to_cancel = node.children_ids();
        dbg!("cancelling children: {:?}", &children_to_cancel);

        children_to_cancel
            .into_iter()
            .filter_map(|child_id| self.tasks.get(&child_id).map(|c| Arc::clone(&c)))
            .fold(CancellationStats::default(), |mut stats, child| {
                stats.visited += 1;

                if predicate.is_none_or(|p| p(&child)) {
                    child.shutdown();
                    stats.cancelled += 1;
                }

                // Child was released by scheduler and removed from OwnedTasks, but
                // since we cloned the Arc, we can still access it's children without
                // issues and continue the recursion.
                stats += self.recursive_shutdown(child, predicate);
                stats
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as ringolo;
    use crate::future::experimental::time::{Sleep, YieldNow};
    use crate::runtime::AddMode;
    use crate::task::JoinError;
    use anyhow::Result;
    use std::time::Duration;

    // TODO:
    // - if sleep started and runtime is waiting for next completion, will never
    //   wake up, and we will wait for duration of sleep
    // - test other scenarios, rstest, etc.

    #[ringolo::test]
    async fn test_recursive_cancel_all_children_not_started() -> Result<()> {
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

        // Cancel all spawned tasks
        let stats = ringolo::cancel_all_children();
        assert_eq!(stats.visited, n);
        assert_eq!(stats.cancelled, n);

        for handle in handles {
            assert!(matches!(handle.await, Err(join_err) if join_err.is_cancelled()));
        }

        Ok(())
    }

    // The difference with the previous test, is that here we make sure we poll
    // all of the spawned tasks once so that they are in progress. We want to
    // ensure cancellation can wake up a parked worker thread.
    #[ringolo::test]
    async fn test_recursive_cancel_all_children_in_progress() -> Result<()> {
        let n = 3;

        let handles = (0..n)
            .map(|_| {
                ringolo::spawn(async {
                    Sleep::try_new(Duration::from_secs(1))?
                        .await
                        .map_err(anyhow::Error::from)
                })
            })
            .collect::<Vec<_>>();

        // Yield to scheduler, putting this task at the back of the queue and
        // forcing the scheduler to poll all of the spawned tasks above.
        dbg!("yielding to scheduler...");
        assert!(YieldNow::new(Some(AddMode::Fifo)).await.is_ok());
        dbg!("done yielding...");

        let stats = ringolo::cancel_all_children();
        assert_eq!(stats.visited, n);
        assert_eq!(stats.cancelled, n);

        for handle in handles {
            dbg!("waiting for handles...");
            assert!(matches!(handle.await, Err(join_err) if join_err.is_cancelled()));
        }

        dbg!("all cancelled...");
        Ok(())
    }

    #[ringolo::test]
    async fn test_recursive_cancel_all_leaf_children() -> Result<()> {
        Ok(())
    }

    // TODO: test multi-threaded cancellation
}
