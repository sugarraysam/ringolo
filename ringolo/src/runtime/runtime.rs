use crate as ringolo;
use crate::context::init_local_context;
use crate::runtime::Scheduler;
use crate::runtime::cancel::OnCancelError;
use crate::runtime::local;
use crate::runtime::stealing;
use crate::task::JoinHandle;
use anyhow::{Result, anyhow};
use std::cell::Cell;
use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;
use std::thread;

// TODO missing from tokio:
// - UnhandledPanic
// - metrics

// Used wherever we rely on SmallVec to store entries on stack first.
// Prevent most heap allocations. Threads have 2 MB stack size by default and so
// far the largest thing we store here is 64 byte Entry structs.
pub(crate) const SPILL_TO_HEAP_THRESHOLD: usize = 16;

/// Unimplemented!
const MAX_BLOCKING_THREADS: usize = 512;

/// Default size for io_uring SQ ring.
const SQ_RING_SIZE: usize = 64;

/// Final cq ring size is SQ_RING_SIZE * multipler
const CQ_RING_SIZE_MULTIPLIER: usize = 2;

///
/// Event Loop policies
//
/// Fairly arbitrary, copied from tokio `event_interval`.
const PROCESS_CQES_INTERVAL: u32 = 61;

/// Submit 4 times as frequently as we complete to keep kernel busy.
const SUBMIT_INTERVAL: u32 = PROCESS_CQES_INTERVAL / 4;

/// Submit if sq ring 33% full
const MAX_UNSUBMITTED_SQES: usize = SQ_RING_SIZE / 3;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Kind {
    Local,
    Stealing,
}

#[derive(Clone)]
pub(crate) struct ThreadNameFn(Arc<dyn Fn() -> String + Send + Sync + 'static>);

// TODO: add thread_id
fn default_thread_name_fn() -> ThreadNameFn {
    ThreadNameFn(Arc::new(move || "ringolo-worker-{}".into()))
}

impl fmt::Debug for ThreadNameFn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // As before, we print a placeholder since the closure itself isn't printable.
        f.debug_tuple("ThreadNameFn").field(&"<function>").finish()
    }
}

// Safety: can safely generate thread names from many threads
unsafe impl Send for ThreadNameFn {}
unsafe impl Sync for ThreadNameFn {}

#[derive(Debug)]
pub struct Builder {
    /// Runtime type
    kind: Kind,

    /// The number of worker threads, used by Runtime.
    ///
    /// Only used when not using the current-thread executor.
    ///
    /// Defaults to 1 worker per CPU core.
    worker_threads: Option<usize>,

    max_blocking_threads: usize,

    /// Name fn used for threads spawned by the runtime.
    pub(super) thread_name: ThreadNameFn,

    /// Stack size used for threads spawned by the runtime.
    pub(super) thread_stack_size: Option<usize>,

    /// How many ticks before pulling a task from the global/remote queue.
    ///
    /// When `None`, the value is unspecified and behavior details are left to
    /// the scheduler. Each scheduler flavor could choose to either pick its own
    /// default value or use some other strategy to decide when to poll from the
    /// global queue. For example, the multi-threaded scheduler uses a
    /// self-tuning strategy based on mean task poll times.
    pub(super) global_queue_interval: Option<u32>,

    /// Submit SQEs every N ticks
    pub(super) submit_interval: u32,

    /// Process CQEs every N ticks
    pub(super) process_cqes_interval: u32,

    /// Maximum amount of unsubmitted sqes before we force submit
    pub(super) max_unsubmitted_sqes: usize,

    /// Size of io_uring SQ ring
    pub(super) sq_ring_size: usize,

    /// Final size of cq ring will be `sq_ring_size * cq_ring_size_multiplier`.
    /// Default is 2 as per `io_uring`'s own default value. Why? We have SQE primitives
    /// like SqeStream which will post N CQEs for each SQE so we need to have a buffer
    /// as most of the time CQ >> SQ.
    ///
    /// The size of the RawSqeSlab will also match the size of the cq ring. The
    /// reason is that if the SQ ring is full, we can recover from the error by
    /// simply retrying to `poll()` the task in the next `event_loop` pass. But
    /// if the RawSqeSlab is the same size as the SQ ring, we will overflow in the slab
    /// and won't be able to gracefully handle SQ ring going above capacity.
    ///
    /// In all cases, if the SQ ring goes above capacity, the program should send a
    /// loud signal to the user and runtim configuration needs to be changed ASAP.
    pub(super) cq_ring_size_multiplier: usize,

    /// When cancellation an `io_uring` operation fails, what to do?
    pub(super) on_cancel_error: OnCancelError,
}

impl Builder {
    fn new(kind: Kind) -> Self {
        Self {
            kind,
            worker_threads: None,
            max_blocking_threads: MAX_BLOCKING_THREADS,
            thread_name: default_thread_name_fn(),
            thread_stack_size: None,
            global_queue_interval: None,
            submit_interval: SUBMIT_INTERVAL,
            process_cqes_interval: PROCESS_CQES_INTERVAL,
            max_unsubmitted_sqes: MAX_UNSUBMITTED_SQES,
            sq_ring_size: SQ_RING_SIZE,
            cq_ring_size_multiplier: CQ_RING_SIZE_MULTIPLIER,
            on_cancel_error: OnCancelError::default(),
        }
    }

    /// Returns a new builder with the local thread scheduler selected.
    ///
    /// Configuration methods can be chained on the return value.
    pub fn new_local() -> Builder {
        Builder::new(Kind::Local)
    }

    pub fn new_stealing() -> Builder {
        Builder::new(Kind::Stealing)
    }

    pub fn worker_threads(mut self, val: usize) -> Self {
        assert!(val > 0, "Worker threads cannot be set to 0");
        self.worker_threads = Some(val);
        self
    }

    // TODO
    fn max_blocking_threads(self, _val: usize) -> Self {
        unimplemented!("TODO");
        // assert!(val > 0, "Max blocking threads cannot be set to 0");
        // self.max_blocking_threads = val;
        // self
    }

    /// Sets name of threads spawned by the `Runtime`'s thread pool.
    ///
    /// The default name is "ringolo-worker-{thread_id}".
    pub fn thread_name(mut self, val: impl Into<String>) -> Self {
        let val = val.into();
        self.thread_name = ThreadNameFn(Arc::new(move || val.clone()));
        self
    }

    /// Sets a function used to generate the name of threads spawned by the `Runtime`'s thread pool.
    ///
    /// The default name fn yields worker names with monotonically increasing N
    /// "ringolo-worker-{N}".
    pub fn thread_name_fn<F>(mut self, f: F) -> Self
    where
        F: Fn() -> String + Send + Sync + 'static,
    {
        self.thread_name = ThreadNameFn(Arc::new(f));
        self
    }

    /// Sets the stack size (in bytes) for worker threads.
    ///
    /// The actual stack size may be greater than this value if the platform
    /// specifies minimal stack size.
    ///
    /// The default stack size for spawned threads is 2 MiB, though this
    /// particular stack size is subject to change in the future.
    pub fn thread_stack_size(mut self, val: usize) -> Self {
        self.thread_stack_size = Some(val);
        self
    }

    /// Sets the number of scheduler ticks after which the scheduler will poll the global
    /// task queue.
    ///
    /// A scheduler "tick" roughly corresponds to one `poll` invocation on a task.
    ///
    /// By default the global queue interval is 31 for the current-thread scheduler.
    ///
    /// Schedulers have a local queue of already-claimed tasks, and a global queue of incoming
    /// tasks. Setting the interval to a smaller value increases the fairness of the scheduler,
    /// at the cost of more synchronization overhead. That can be beneficial for prioritizing
    /// getting started on new work, especially if tasks frequently yield rather than complete
    /// or await on further I/O. Setting the interval to `1` will prioritize the global queue and
    /// tasks from the local queue will be executed only if the global queue is empty.
    /// Conversely, a higher value prioritizes existing work, and is a good choice when most
    /// tasks quickly complete polling.
    ///
    /// # Panics
    ///
    /// This function will panic if 0 is passed as an argument.
    #[track_caller]
    pub fn global_queue_interval(mut self, val: u32) -> Self {
        assert!(val > 0, "global_queue_interval must be greater than 0");
        self.global_queue_interval = Some(val);
        self
    }

    pub fn submit_interval(mut self, val: u32) -> Self {
        self.submit_interval = val;
        self
    }

    pub fn process_cqes_interval(mut self, val: u32) -> Self {
        self.process_cqes_interval = val;
        self
    }

    pub fn max_unsubmitted_sqes(mut self, val: usize) -> Self {
        self.max_unsubmitted_sqes = val;
        self
    }

    pub fn sq_ring_size(mut self, val: usize) -> Self {
        self.sq_ring_size = val;
        self
    }

    pub fn cq_ring_size_multiplier(mut self, val: usize) -> Self {
        self.cq_ring_size_multiplier = val;
        self
    }

    pub fn on_cancel_error(mut self, on_cancel: OnCancelError) -> Self {
        self.on_cancel_error = on_cancel;
        self
    }

    /// Creates the configured `Runtime`.
    ///
    /// The returned `Runtime` instance is ready to spawn tasks.
    pub fn try_build(self) -> Result<Runtime> {
        match &self.kind {
            Kind::Local => self.try_build_local_runtime(),
            Kind::Stealing => self.try_build_stealing_runtime(),
        }
    }
}

// Use a thread_local variable to track if a runtime is already active on this thread.
thread_local! {
    static IS_RUNTIME_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

// --- Private methods ---
// The contract of the builder is that after we have called `try_build()`, the
// runtime is properly initialized. This means that calls to `block_on` or `spawn`
// will succeed. It means different things depending on the flavor of the runtime,
// for example spawning on a local runtime does not *start* any task in the background.
// We have to call `block_on` to start executing tasks.
impl Builder {
    #[track_caller]
    fn try_build_local_runtime(self) -> Result<Runtime> {
        IS_RUNTIME_ACTIVE.with(|is_active| -> Result<()> {
            if is_active.get() {
                Err(anyhow!(
                    "Cannot create a new LocalRuntime: a runtime is already active on this thread."
                ))
            } else {
                is_active.set(true);
                Ok(())
            }
        })?;

        let cfg = self.try_into()?;
        let scheduler = local::Scheduler::new(&cfg).into_handle();

        if let Err(e) = init_local_context(&cfg, scheduler.clone()) {
            panic!("Failed to initialize local context: {:?}", e);
        }

        Ok(Runtime::new(Scheduler::Local(scheduler)))
    }

    fn try_build_stealing_runtime(self) -> Result<Runtime> {
        let cfg = self.try_into()?;
        let scheduler = stealing::Scheduler::new(cfg);
        Ok(Runtime::new(Scheduler::Stealing(scheduler.into_handle())))
    }
}

#[derive(Debug)]
pub struct Runtime {
    scheduler: Scheduler,
}

impl Runtime {
    pub(super) fn new(scheduler: Scheduler) -> Runtime {
        Runtime { scheduler }
    }

    pub fn block_on<F: Future>(&self, root_fut: F) -> F::Output {
        ringolo::block_on(root_fut)
    }

    /// Spawn a future onto the runtime.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        ringolo::spawn(future)
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        match self.scheduler {
            Scheduler::Local(_) => {
                IS_RUNTIME_ACTIVE.with(|is_active| is_active.set(false));
            }
            Scheduler::Stealing(_) => { /* nothing to do */ }
        }
    }
}

// Test-only helpers
#[cfg(test)]
impl Runtime {
    pub(crate) fn expect_local_scheduler(&self) -> local::Handle {
        match &self.scheduler {
            Scheduler::Local(handle) => handle.clone(),
            _ => panic!("Runtime not using local sheduler"),
        }
    }

    pub(crate) fn expect_stealing_scheduler(&self) -> stealing::Handle {
        match &self.scheduler {
            Scheduler::Stealing(handle) => handle.clone(),
            _ => panic!("Runtime not using stealing sheduler"),
        }
    }
}

// Export runtime builder as a RuntimeConfig object to be consumed by each
// scheduler and their associated context.
#[derive(Debug, Clone)]
pub(crate) struct RuntimeConfig {
    pub(crate) worker_threads: usize,
    pub(crate) max_blocking_threads: usize,
    pub(crate) thread_name: ThreadNameFn,
    pub(crate) thread_stack_size: Option<usize>,
    pub(crate) global_queue_interval: Option<u32>,
    pub(crate) submit_interval: u32,
    pub(crate) process_cqes_interval: u32,
    pub(crate) max_unsubmitted_sqes: usize,
    pub(crate) sq_ring_size: usize,
    pub(crate) cq_ring_size_multiplier: usize,
    pub(crate) on_cancel_error: OnCancelError,
}

impl TryFrom<Builder> for RuntimeConfig {
    type Error = anyhow::Error;

    fn try_from(builder: Builder) -> Result<Self, Self::Error> {
        let worker_threads = builder
            .worker_threads
            .unwrap_or(thread::available_parallelism()?.get());

        Ok(RuntimeConfig {
            worker_threads,
            max_blocking_threads: builder.max_blocking_threads,
            thread_name: builder.thread_name,
            thread_stack_size: builder.thread_stack_size,
            global_queue_interval: builder.global_queue_interval,
            submit_interval: builder.submit_interval,
            process_cqes_interval: builder.process_cqes_interval,
            max_unsubmitted_sqes: builder.max_unsubmitted_sqes,
            sq_ring_size: builder.sq_ring_size,
            cq_ring_size_multiplier: builder.cq_ring_size_multiplier,
            on_cancel_error: builder.on_cancel_error,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::assert_impl_all;

    // We need this to easily inject RuntimeConfig in Thread-local context, by
    // first cloning and sending it into each spawned worker.
    assert_impl_all!(RuntimeConfig: Send, Sync, Clone);
}
