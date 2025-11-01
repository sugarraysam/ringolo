use crate::runtime::{OrphanPolicy, RuntimeConfig, Schedule};
use crate::task::id::{ORPHAN_ROOT_ID, ROOT_ID};
use crate::task::{Id, Task, TaskNode};
use dashmap::DashMap;
use parking_lot::{Condvar, Mutex};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::ThreadId;

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

    /// Used to allow workers to drop the tasks they own and ensure underlying future
    /// Drop impl can use the corresponding thread-local context.
    shutdown_partitions: Arc<(Mutex<Option<HashMap<ThreadId, Vec<Task<S>>>>>, Condvar)>,
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
            shutdown_partitions: Arc::new((Mutex::new(None), Condvar::new())),
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

    /// We need the Drop impl for SQE backends to run on the owning thread. This
    /// function partitions the task registry by thread_id, which can be retrieved by
    /// the owning workers with the `wait_for_shutdown_partition` method.
    pub(crate) fn close_and_partition(&self) {
        self.close();

        let (lock, cvar) = &*self.shutdown_partitions;

        let mut partitions = lock.lock();
        *partitions = Some(self.partition_by_thread_id());

        cvar.notify_all();
    }

    /// Waits for registry partition to be created by shutdown process.
    pub(crate) fn wait_for_shutdown_partition(&self, thread_id: &ThreadId) -> Option<Vec<Task<S>>> {
        let (lock, cvar) = &*self.shutdown_partitions;
        let mut partitions = lock.lock();

        // Safety: ok to use if instead of while because parking_lot's Condvar will never
        // spuriously wake up.
        if partitions.is_none() {
            cvar.wait(&mut partitions);
        }

        // Safety: we know the Option is not None anymore.
        partitions.as_mut().unwrap().remove(thread_id)
    }

    /// Shutdown partitions that were unclaimed by other workers.
    pub(crate) fn shutdown_all_partitions(&self) {
        self.close();

        let mut partitions = self.shutdown_partitions.0.lock();
        debug_assert!(
            partitions.is_some(),
            "OwnedTasks was not partitioned on shutdown."
        );

        let n_orphans = self.get_orphan_root().num_children_recursive();
        let n = partitions
            .take()
            .unwrap()
            .into_iter()
            .flat_map(|(_, tasks)| tasks.into_iter())
            .map(|t| t.shutdown())
            .count();

        // TODO: tracing
        eprintln!("Shutting down {} tasks and {} orphans...", n, n_orphans,);
    }
}

// Private methods
impl<S: Schedule> OwnedTasks<S> {
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    fn partition_by_thread_id(&self) -> HashMap<ThreadId, Vec<Task<S>>> {
        debug_assert!(
            self.closed.load(Ordering::Relaxed),
            "Should not partition before closing."
        );

        let mut partitions: HashMap<ThreadId, Vec<Task<S>>> = HashMap::with_capacity(self.len());

        self.tasks.shards().iter().for_each(|shard| {
            shard
                .write()
                .drain()
                .map(|(_id, task)| task.into_inner())
                .for_each(|task| {
                    partitions
                        .entry(task.get_owner_id())
                        .or_default()
                        .push(task);
                })
        });

        partitions
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum InsertResult {
    Ok,
    Duplicate(Id),
    Shutdown,
}

impl<S: Schedule> OwnedTasks<S> {
    pub(crate) fn insert(&self, task: Task<S>) -> InsertResult {
        if self.closed.load(Ordering::Acquire) {
            task.shutdown();
            InsertResult::Shutdown
        } else {
            match self.tasks.insert(task.id(), task) {
                None => {
                    self.size.fetch_add(1, Ordering::Relaxed);
                    InsertResult::Ok
                }
                Some(duplicate) => InsertResult::Duplicate(duplicate.id()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use anyhow::Result;
    use rstest::rstest;
    use std::sync::Barrier;
    use std::thread::{self, JoinHandle};

    #[rstest]
    #[case::ten(10)]
    #[case::hundred(100)]
    #[case::thousand(1000)]
    fn test_registry_insert(#[case] n: usize) -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        let registry = OwnedTasks::<DummyScheduler>::new(&RuntimeConfig::default());
        assert!(registry.is_empty());

        for _ in 0..n {
            registry.insert(mock_task(None, None));
        }

        assert_eq!(registry.len(), n);
        assert!(!registry.is_empty());
        assert!(!registry.is_closed());

        Ok(())
    }

    #[rstest]
    #[case::ten(2, 10)]
    #[case::hundred(5, 20)]
    #[case::thousand(10, 100)]
    fn test_registry_insert_and_partition(
        #[case] n_partitions: usize,
        #[case] tasks_per_partition: usize,
    ) -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        let insert_barrier = Arc::new(Barrier::new(n_partitions));

        let registry = OwnedTasks::<DummyScheduler>::new(&RuntimeConfig::default());
        assert!(registry.is_empty());

        for _ in 0..tasks_per_partition {
            registry.insert(mock_task(None, None));
        }

        let check_partition = Arc::new(move |registry: Arc<OwnedTasks<DummyScheduler>>| {
            let thread_id = thread::current().id();
            let partition = registry.wait_for_shutdown_partition(&thread_id);
            assert!(partition.is_some());

            let tasks = partition.unwrap();
            assert_eq!(tasks.len(), tasks_per_partition);
            assert!(tasks.iter().all(|t| t.get_owner_id() == thread_id));
        });

        let handles: Vec<JoinHandle<Result<()>>> = (0..(n_partitions - 1))
            .into_iter()
            .map(|_| {
                let registry = registry.clone();
                let insert_barrier = insert_barrier.clone();
                let check_partition = check_partition.clone();

                std::thread::spawn(move || -> Result<()> {
                    let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

                    for _ in 0..tasks_per_partition {
                        registry.insert(mock_task(None, None));
                    }
                    insert_barrier.wait();

                    // Spin until registry is closed
                    while !registry.is_closed() {
                        std::hint::spin_loop();
                    }

                    check_partition(registry);

                    Ok(())
                })
            })
            .collect();

        // Wait for inserts to be done
        insert_barrier.wait();
        assert_eq!(registry.len(), n_partitions * tasks_per_partition);
        assert!(!registry.is_empty());
        assert!(!registry.is_closed());

        // Close and partition
        registry.close_and_partition();
        assert!(registry.is_closed());

        check_partition(registry);

        for handle in handles {
            assert!(handle.join().is_ok());
        }

        Ok(())
    }

    #[test]
    fn test_insert_scenarios() -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        let registry = OwnedTasks::<DummyScheduler>::new(&RuntimeConfig::default());

        let task = mock_task(None, None);
        let task_id = task.id();
        let task_clone: Task<DummyScheduler> = Task::new(task.as_raw());

        assert!(matches!(registry.insert(task), InsertResult::Ok));
        assert!(
            matches!(registry.insert(task_clone), InsertResult::Duplicate(id) if id == task_id)
        );

        registry.close();
        assert!(matches!(
            registry.insert(mock_task(None, None)),
            InsertResult::Shutdown
        ));

        Ok(())
    }
}
