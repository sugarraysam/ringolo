use crate::runtime::{OrphanPolicy, RuntimeConfig, Schedule};
use crate::task::id::{ORPHAN_ROOT_ID, ROOT_ID};
use crate::task::{Id, Task, TaskNode};
use dashmap::DashMap;
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
    orphan_policy: OrphanPolicy,

    tasks: DashMap<Id, Task<S>>,

    // Keep track of size separately because DashMap impl for len iterates over
    // all shards.
    size: AtomicUsize,

    // Close the OwnedTasks when we are shutting down. This is to prevent adding
    // new tasks and guarantee shutdown is only called once.
    closed: Arc<AtomicBool>,

    /// Structured Concurrency: Root of the task task tree.
    root: OnceLock<Arc<TaskNode>>,

    /// Structured Concurrency: Root of the orphan task tree created through cancellation.
    orphan_root: OnceLock<Arc<TaskNode>>,
}

impl<S: Schedule> OwnedTasks<S> {
    pub(crate) fn new(cfg: &RuntimeConfig) -> Arc<Self> {
        let registry = Arc::new(Self {
            orphan_policy: cfg.orphan_policy,
            tasks: DashMap::with_capacity(cfg.sq_ring_size),
            size: AtomicUsize::new(0),
            closed: Arc::new(AtomicBool::new(false)),
            root: OnceLock::new(),
            orphan_root: OnceLock::new(),
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
        if self.closed.load(Ordering::Acquire) {
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
        if !self.closed.swap(true, Ordering::AcqRel) {
            // Leverage dashmap `raw-api` for more efficient locking and draining.
            let tasks = self.tasks.shards().iter().fold(
                Vec::with_capacity(self.len()),
                |mut acc, shard| {
                    acc.extend(shard.write().drain());
                    acc
                },
            );

            // TODO: tracing
            eprintln!(
                "Shutting down {} tasks and {} orphans...",
                tasks.len(),
                self.get_orphan_root().num_children_recursive()
            );

            for (_id, task) in tasks {
                task.into_inner().shutdown();
            }
        }
    }
}

/// Expose functionality of the TaskRegistry common to any scheduler.
pub trait TaskRegistry: Send + Sync + std::fmt::Debug {
    /// Allow shutting down a specific task.
    fn shutdown(&self, id: &Id);

    /// Returns true if the registry was closed as part of runtime shutdown.
    fn is_closed(&self) -> bool;

    /// Get the OrphanPolicy to decide if we need to cancel or adopt orphans.
    fn orphan_policy(&self) -> OrphanPolicy;
}

impl<S: Schedule> TaskRegistry for OwnedTasks<S> {
    fn shutdown(&self, id: &Id) {
        if let Some((_, task)) = self.tasks.remove(id) {
            task.shutdown()
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn orphan_policy(&self) -> OrphanPolicy {
        self.orphan_policy
    }
}
