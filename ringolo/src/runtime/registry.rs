use crate::runtime::Schedule;
use crate::task::{Id, Task};
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
    tasks: Arc<DashMap<Id, Task<S>>>,

    shutdown: Arc<AtomicBool>,
}

impl<S: 'static> OwnedTasks<S> {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            tasks: Arc::new(DashMap::with_capacity(capacity)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Inserts a new task, returning its ID.
    pub(crate) fn insert(&self, task: Task<S>) -> Option<Task<S>>
    where
        S: Schedule,
    {
        if self.shutdown.load(Ordering::Acquire) {
            task.shutdown();
            None
        } else {
            self.tasks.insert(task.id(), task)
        }
    }

    /// Removes a task by its ID, returning it.
    pub(crate) fn remove(&self, id: &Id) -> Option<Task<S>> {
        self.tasks.remove(id).map(|(_, task)| task)
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

            keys.iter().for_each(|k| {
                if let Some((_, task)) = self.tasks.remove(k) {
                    task.shutdown();
                }
            });
        }
    }
}
