use crate::context::init_stealing_context;
use crate::runtime::stealing::{self, root_worker::RootWorker, worker::Worker};
use crate::runtime::EventLoop;
use anyhow::{anyhow, Result};
use crossbeam_deque::Worker as CbWorker;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread::{self, ThreadId};

/// Abstraction for the scheduler workers and thread pool.
#[derive(Debug)]
pub(super) struct ThreadPool {
    // The `root_worker` is the *current thread*. It is the thread used to create
    // the runtime. It is also where the user will call `block_on`. It's role is
    // to:
    // - spawn other workers
    // - drive `root_future` to completion
    // - init runtime shutdown sequence
    pub(super) root_worker: Arc<RootWorker>,

    // The "regular" workers, participate in work stealing, and drive completion
    // of every other tasks. They will park themselves if there is nothing to be
    // done and they don't have any pending IO on their local `io_uring`.
    pub(super) workers: HashMap<ThreadId, Arc<Worker>>,
    pub(super) handles: Mutex<HashMap<ThreadId, thread::JoinHandle<()>>>,
}

impl ThreadPool {
    pub(super) fn new(scheduler: &stealing::Handle) -> Self {
        let num_workers = scheduler.cfg.worker_threads;

        // Create Crossbeam LIFO queues and their stealers
        let mut local_queues = Vec::with_capacity(num_workers);
        let mut stealers = Vec::with_capacity(num_workers);

        for _ in 0..num_workers {
            let w = CbWorker::new_lifo();
            stealers.push(w.stealer());
            local_queues.push(w);
        }

        // Init root_worker *before* other threads to ensure it gets `ringolo-0`
        // thread name.
        let root_worker = Arc::new(RootWorker::new(scheduler));

        let workers = local_queues
            .into_iter()
            .enumerate()
            .map(|(i, local_queue)| {
                // Give each worker a list of all *other* workers' stealers
                let other_stealers = stealers
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(_, s)| s.clone())
                    .collect::<Vec<_>>();

                Arc::new(Worker::new(
                    &scheduler.cfg,
                    scheduler.injector.clone(),
                    local_queue,
                    other_stealers,
                ))
            })
            .collect::<Vec<_>>();

        // 3. Spawn all threads and collect workers and join handles
        let barrier = Arc::new(Barrier::new(num_workers + 1));
        let (workers, handles): (HashMap<_, _>, HashMap<_, _>) = workers
            .into_iter()
            .map(|worker| {
                let (id, handle) =
                    spawn_worker_thread(scheduler.clone(), worker.clone(), barrier.clone());
                ((id, worker), (id, handle))
            })
            .unzip();

        // Spawning threads is async, wait for all threads to be started...
        barrier.wait();

        Self {
            root_worker,
            workers,
            handles: Mutex::new(handles),
        }
    }

    pub(super) fn join_all(&self) -> Result<()> {
        let mut handles = self.handles.lock();

        let errors = handles
            .drain()
            .filter_map(|(thread_id, handle)| handle.join().err().map(|_| thread_id))
            .collect::<Vec<_>>();

        if errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow!("{} thread(s) panicked", errors.len(),))
        }
    }
}

fn spawn_worker_thread(
    scheduler: stealing::Handle,
    worker: Arc<Worker>,
    barrier: Arc<Barrier>,
) -> (ThreadId, thread::JoinHandle<()>) {
    let mut builder = thread::Builder::new();

    if let Some(stack_size) = scheduler.cfg.thread_stack_size {
        builder = builder.stack_size(stack_size);
    }

    let handle = builder
        .name(scheduler.cfg.thread_name.0())
        .spawn(move || {
            init_stealing_context(scheduler);
            barrier.wait();

            let res = worker.event_loop::<std::future::Ready<()>>(None);
            if let Err(e) = &res {
                eprintln!("Worker thread {:?} panicked: {:?}", worker.thread_id(), e);
                debug_assert!(false, "Worker thread panicked");
            }
        })
        .expect("failed to spawn worker thread");

    (handle.thread().id(), handle)
}

// We abstract both workers behind enum dispatch because the EventLoop trait is not
// dyn compatible as it is not object safe, so we can't do dyn coercion.
pub(super) enum WorkerRef<'a> {
    Root(&'a Arc<RootWorker>),
    NonRoot(&'a Arc<Worker>),
}

impl ThreadPool {
    #[track_caller]
    pub(super) fn with_worker<F, R>(&self, thread_id: &ThreadId, f: F) -> R
    where
        F: FnOnce(WorkerRef) -> R,
    {
        let worker = if thread_id == &self.root_worker.thread_id {
            WorkerRef::Root(&self.root_worker)
        } else {
            WorkerRef::NonRoot(self.workers.get(thread_id).expect("worker not found"))
        };

        // Safety:
        // - every thread should have an associated worker.
        // - thread-safe because hashmap is read-only after init.
        f(worker)
    }
}
