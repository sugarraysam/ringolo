use crate::context::ThreadId;
use crate::runtime::Scheduler;
use crate::runtime::local;
use crate::runtime::stealing;
use anyhow::{Error, Result};
use std::convert::TryFrom;
use std::default;
use std::f64::MAX;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::thread::Thread;

pub struct Runtime {
    scheduler: Scheduler,
}

impl Runtime {
    pub(super) fn new(scheduler: Scheduler) -> Runtime {
        Runtime { scheduler }
    }

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

    // TODO:
    // - block_on
    // - shutdown
    // - methods just forward to scheduler
}

// TODO missing from tokio:
// - UnhandledPanic
// - metrics

/// How many ticks before yielding to check I/O completions
const EVENT_INTERVAL: u32 = 61;

/// Unimplemented!
const MAX_BLOCKING_THREADS: usize = 512;

/// Default size for io_uring SQ ring.
const SQ_RING_SIZE: usize = 1024;

/// Final slab size is SQ_RING_SIZE * multipler
const SLAB_SIZE_MULTIPLIER: usize = 2;

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

    /// How many ticks before yielding to check I/O completions
    pub(super) event_interval: u32,

    /// Size of io_uring SQ ring
    pub(super) sq_ring_size: usize,

    /// Final size of slab will be `sq_ring_size * slab_size_multiplier`.
    /// Default is 2 to reflect the size of the CQ ring that is also twice the SQ ring.
    ///
    /// Why? We have SQE primitives like SqeStream which will post N CQEs for each SQE.
    /// Since each SQE is associated with a RawSqe in the slab, we need to have a buffer
    /// as most of the time CQ >> SQ.
    ///
    /// The other reason is that if the SQ ring is full, we can recover from the error
    /// by simply retrying to `poll()` the task in the next `event_loop` pass. But
    /// if the RawSqeSlab is the same size as the SQ ring, we will overflow in the slab
    /// and won't be able to gracefully handle SQ ring going above capacity.
    ///
    /// In all cases, if the SQ ring goes above capacity, the program should send a
    /// loud signal to the user and runtim configuration needs to be changed ASAP.
    pub(super) slab_size_multiplier: usize,
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
            event_interval: EVENT_INTERVAL,
            sq_ring_size: SQ_RING_SIZE,
            slab_size_multiplier: SLAB_SIZE_MULTIPLIER,
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
    fn max_blocking_threads(mut self, val: usize) -> Self {
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
    pub fn global_queue_interval(mut self, val: u32) -> Self {
        assert!(val > 0, "global_queue_interval must be greater than 0");
        self.global_queue_interval = Some(val);
        self
    }

    /// Sets the number of scheduler ticks after which the scheduler will poll for
    /// I/O completions.
    ///
    /// A scheduler "tick" roughly corresponds to one `poll` invocation on a task.
    ///
    /// By default, the event interval is `61` for all scheduler types.
    ///
    /// Setting the event interval determines the effective "priority" of delivering
    /// these external events (which may wake up additional tasks), compared to
    /// executing tasks that are currently ready to run. A smaller value is useful
    /// when tasks frequently spend a long time in polling, or frequently yield,
    /// which can result in overly long delays picking up I/O events. Conversely,
    /// picking up new events requires extra synchronization and syscall overhead,
    /// so if tasks generally complete their polling quickly, a higher event interval
    /// will minimize that overhead while still keeping the scheduler responsive to
    /// events.
    pub fn event_interval(mut self, val: u32) -> Self {
        self.event_interval = val;
        self
    }

    pub fn sq_ring_size(mut self, val: usize) -> Self {
        self.sq_ring_size = val;
        self
    }

    pub fn slab_size_multiplier(mut self, val: usize) -> Self {
        self.slab_size_multiplier = val;
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

// Private methods
impl Builder {
    fn try_build_local_runtime(self) -> Result<Runtime> {
        let cfg = self.try_into()?;
        let scheduler = local::Scheduler::try_new(cfg)?;
        Ok(Runtime::new(Scheduler::Local(scheduler.into_handle())))
    }

    fn try_build_stealing_runtime(self) -> Result<Runtime> {
        let cfg = self.try_into()?;
        let scheduler = stealing::Scheduler::new(cfg);
        Ok(Runtime::new(Scheduler::Stealing(scheduler.into_handle())))
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
    pub(crate) event_interval: u32,
    pub(crate) sq_ring_size: usize,
    pub(crate) slab_size_multiplier: usize,
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
            event_interval: builder.event_interval,
            sq_ring_size: builder.sq_ring_size,
            slab_size_multiplier: builder.slab_size_multiplier,
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
