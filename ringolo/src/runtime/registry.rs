use crate::runtime::Schedule;
use crate::task::id::{ORPHAN_ROOT_ID, ROOT_ID};
use crate::task::{Id, Task, TaskNode};
use dashmap::{DashMap, DashSet};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

pub(crate) fn get_root() -> Arc<TaskNode> {
    crate::with_scheduler!(|s| { s.tasks.get_root() })
}

pub(crate) fn get_orphan_root() -> Arc<TaskNode> {
    crate::with_scheduler!(|s| { s.tasks.get_orphan_root() })
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
#[derive(Debug)]
pub(crate) struct OwnedTasks<S: 'static> {
    /// Structured Concurrency: Root of the task task tree.
    root: OnceLock<Arc<TaskNode>>,

    /// Structured Concurrency: Root of the orphan task tree created through cancellation.
    orphan_root: OnceLock<Arc<TaskNode>>,

    tasks: DashMap<Id, Task<S>>,

    // Keep track of size separately because DashMap impl for len iterates over
    // all shards.
    size: AtomicUsize,

    orphans: DashSet<Id>,

    shutdown: Arc<AtomicBool>,
}

impl<S: Schedule> OwnedTasks<S> {
    // TODO: shutdown pass by argument
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        let registry = Arc::new(Self {
            root: OnceLock::new(),
            orphan_root: OnceLock::new(),
            tasks: DashMap::with_capacity(capacity),
            size: AtomicUsize::new(0),
            orphans: DashSet::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
        });

        registry
            .root
            .get_or_init(|| Arc::new(TaskNode::new_root(ROOT_ID, registry.clone())));

        registry
            .orphan_root
            .get_or_init(|| Arc::new(TaskNode::new_root(ORPHAN_ROOT_ID, registry.clone())));

        registry
    }

    pub(crate) fn get_root(&self) -> Arc<TaskNode> {
        // Safety: we initialize `root_node` in constructor.
        Arc::clone(self.root.get().unwrap())
    }

    pub(crate) fn get_orphan_root(&self) -> Arc<TaskNode> {
        // Safety: we initialize `root_node` in constructor.
        Arc::clone(self.orphan_root.get().unwrap())
    }

    /// Inserts a new task, returning its ID.
    pub(crate) fn insert(&self, task: Task<S>) -> Option<Task<S>> {
        if self.shutdown.load(Ordering::Acquire) {
            task.shutdown();
            None
        } else {
            self.size.fetch_add(1, Ordering::Relaxed);
            self.tasks.insert(task.id(), task)
        }
    }

    /// Removes a task by its ID, returning it.
    pub(crate) fn remove(&self, id: &Id) -> Option<Task<S>> {
        self.tasks.remove(id).map(|(_id, task)| {
            self.size.fetch_sub(1, Ordering::Relaxed);
            task
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.size.load(Ordering::Relaxed) == 0
    }

    /// Initiates a graceful shutdown by signaling all tasks.
    pub(crate) fn shutdown_all(&self) {
        if !self.shutdown.swap(true, Ordering::AcqRel) {
            // Leverage dashmap `raw-api` for more efficient locking and draining.
            let all_tasks = self.tasks.shards().iter().fold(
                HashMap::with_capacity(self.len()),
                |mut acc, shard| {
                    acc.extend(shard.write().drain());
                    acc
                },
            );

            // See note in `task::node::TaskNode::release` to understand why we
            // need to filter `orphan_ids` as there is a non-zero chance some of them
            // are stale.
            let orphan_ids = self
                .orphans
                .shards()
                .iter()
                .fold(Vec::new(), |mut acc, shard| {
                    acc.extend(
                        shard
                            .write()
                            .drain()
                            .map(|(id, _)| id)
                            .filter(|id| all_tasks.contains_key(id)),
                    );
                    acc
                });

            // TODO: tracing
            eprintln!(
                "Shutting down {} tasks and {} orphans",
                all_tasks.len(),
                orphan_ids.len(),
            );

            for (_id, task) in all_tasks {
                task.into_inner().shutdown();
            }
        }
    }
}

/// This trait is used by TaskNode to shutdown specific tasks.
pub trait TaskRegistry: Send + Sync + std::fmt::Debug {
    fn shutdown(&self, id: &Id);
}

impl<S: Schedule> TaskRegistry for OwnedTasks<S> {
    fn shutdown(&self, id: &Id) {
        if let Some((_, task)) = self.tasks.remove(id) {
            task.shutdown()
        }
    }
}
