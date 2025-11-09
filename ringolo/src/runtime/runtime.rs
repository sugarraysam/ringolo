use crate as ringolo;
use crate::context;
use crate::context::maintenance::OnCleanupError;
use crate::runtime::local;
use crate::runtime::stealing;
use crate::runtime::Scheduler;
use crate::spawn::TaskOpts;
use crate::task::JoinHandle;
use crate::utils::thread::set_current_thread_name;
use anyhow::{anyhow, Result};
use std::cell::Cell;
use std::convert::TryFrom;
use std::fmt;
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

// Used wherever we rely on SmallVec to store entries on stack first.
// Prevent most heap allocations. Threads have 2 MB stack size by default and so
// far the largest thing we store here is 64 byte Entry structs.
pub(crate) const SPILL_TO_HEAP_THRESHOLD: usize = 16;

/// Default size for io_uring SQ ring.
const SQ_RING_SIZE: usize = 1024;

/// Final cq ring size is SQ_RING_SIZE * multipler
const CQ_RING_SIZE_MULTIPLIER: usize = 2;

/// Number of direct file descriptors per ring
const DIRECT_FDS_PER_RING: u32 = 1024;

///
/// Event Loop policies
//
/// Global queue interval default value.
const GLOBAL_QUEUE_INTERVAL: u32 = 31;

/// Fairly arbitrary, copied from tokio `event_interval`.
#[cfg(not(test))]
const PROCESS_CQES_INTERVAL: u32 = 61;

#[cfg(test)]
const PROCESS_CQES_INTERVAL: u32 = 8; // make tests tick faster

/// Submit 4 times as frequently as we complete to keep kernel busy.
const SUBMIT_INTERVAL: u32 = PROCESS_CQES_INTERVAL / 4;

/// Submit if sq ring 33% full
const MAX_UNSUBMITTED_SQES: usize = SQ_RING_SIZE / 3;

/// Maximum number of stealing attempts.
const MAX_STEAL_RETRIES: usize = 3;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Kind {
    Local,
    Stealing,
}

#[derive(Clone)]
pub(crate) struct ThreadNameFn(pub(crate) Arc<dyn Fn() -> String + Send + Sync + 'static>);

fn default_thread_name_fn() -> ThreadNameFn {
    let worker_count = Arc::new(AtomicUsize::new(0));

    ThreadNameFn(Arc::new(move || {
        let id = worker_count.fetch_add(1, Ordering::Relaxed);
        format!("ringolo-{}", id)
    }))
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

/// Defines how we enforce structured concurrency. This is the idea that every
/// task should have a clear owner and should not be allowed to outlive it's parent.
/// In structured concurrency, you can't "leak" tasks and all of a task children
/// are cancelled on exit. You have to explicitly use `TaskOpts::BACKGROUND_TASK`
/// if you want a child to outlive it's parent, as this option attaches the child
/// to the ROOT_NODE of the task tree instead of the parent.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanPolicy {
    /// All children of a task are cancelled on exit. The program is *not allowed*
    /// to create orphans. This leads to better performance because we prevent many
    /// synchronous parent swapping operations and avoid child adoption by the
    /// ORPHAN_ROOT_NODE.
    #[default]
    Enforced,

    /// Children are allowed to outlive their parents. If a parent terminates before
    /// it's children, all of the children are adopted by the ORPHAN_ROOT_NODE and
    /// become orphans. This flexibility comes at a cost, with synchronous parent
    /// swapping operations and child adoption.
    Permissive,
}

#[derive(Debug)]
pub struct Builder {
    /// Runtime type
    kind: Kind,

    /// The number of worker threads, used by the stealing scheduler. Defaults
    /// to 1 per core and a `root_worker` to drive the future provided in the
    /// `block_on` function.
    worker_threads: Option<usize>,

    /// Name fn used for threads spawned by the runtime.
    thread_name: ThreadNameFn,

    /// Stack size used for threads spawned by the runtime.
    thread_stack_size: Option<usize>,

    /// How many ticks before pulling a task from the global injector queue.
    global_queue_interval: u32,

    /// Process CQEs every N ticks
    process_cqes_interval: u32,

    /// Submit SQEs every N ticks
    submit_interval: u32,

    /// Maximum amount of unsubmitted sqes before we force submit
    max_unsubmitted_sqes: usize,

    /// How many times a worker will loop over the global injector queue and
    /// other stealable queues to try and find work, before moving on to other
    /// tasks from event_loop.
    max_steal_retries: usize,

    /// Size of io_uring SQ ring
    sq_ring_size: usize,

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
    cq_ring_size_multiplier: usize,

    /// Number of direct file descriptors per ring. We use `register_files_sparse`
    /// so make sure you use `KernelFdMode::DirectAuto` for best results.
    direct_fds_per_ring: u32,

    /// When an async cleanup operation fails, what to do?
    on_cleanup_error: OnCleanupError,

    /// Defines how to enforce structured concurrency.
    orphan_policy: OrphanPolicy,
}

impl Builder {
    fn new(kind: Kind) -> Self {
        Self {
            kind,
            worker_threads: None,
            thread_name: default_thread_name_fn(),
            thread_stack_size: None,
            global_queue_interval: GLOBAL_QUEUE_INTERVAL,
            process_cqes_interval: PROCESS_CQES_INTERVAL,
            submit_interval: SUBMIT_INTERVAL,
            max_unsubmitted_sqes: MAX_UNSUBMITTED_SQES,
            max_steal_retries: MAX_STEAL_RETRIES,
            sq_ring_size: SQ_RING_SIZE,
            cq_ring_size_multiplier: CQ_RING_SIZE_MULTIPLIER,
            on_cleanup_error: OnCleanupError::default(),
            direct_fds_per_ring: DIRECT_FDS_PER_RING,
            orphan_policy: OrphanPolicy::default(),
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

    /// The number of worker threads, used by the Runtime. Only relevant when
    /// using a multi-threaded scheduler (i.e.: stealing scheduler).
    ///
    /// Defaults to 1 worker per CPU core.
    ///
    /// Please note the runtime also has a `root_worker` which *does not count*
    /// towards the total number of worker threads. This `root_worker` is
    /// responsible to drive the `root_future` provided to the `block_on` call.
    #[track_caller]
    pub fn worker_threads(mut self, val: usize) -> Self {
        assert!(val > 0, "worker_threads must be greater than 0");
        self.worker_threads = Some(val);
        self
    }

    /// Sets name of threads spawned by the `Runtime`'s thread pool.
    ///
    /// The default name is "ringolo-{id}", where id is monotonically
    /// increasing.
    ///
    /// Thread names are truncated beyond 15 bytes according to pthread
    /// limitations.
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
    #[track_caller]
    pub fn thread_stack_size(mut self, val: usize) -> Self {
        assert!(
            val.is_power_of_two(),
            "thread_stack_size must be a power of two"
        );
        self.thread_stack_size = Some(val);
        self
    }

    /// Sets the number of scheduler ticks after which the scheduler will poll the global
    /// task queue.
    ///
    /// A scheduler "tick" roughly corresponds to one `poll` invocation on a task.
    ///
    /// Schedulers have a local queue of already-claimed tasks, and a global queue of incoming
    /// tasks. Setting the interval to a smaller value increases the fairness of the scheduler,
    /// at the cost of more synchronization overhead. That can be beneficial for prioritizing
    /// getting started on new work, especially if tasks frequently yield rather than complete
    /// or await on further I/O. Setting the interval to `1` will prioritize the global queue and
    /// tasks from the local queue will be executed only if the global queue is empty.
    /// Conversely, a higher value prioritizes existing work, and is a good choice when most
    /// tasks quickly complete polling.
    #[track_caller]
    pub fn global_queue_interval(mut self, val: u32) -> Self {
        assert!(val > 0, "global_queue_interval must be greater than 0");
        self.global_queue_interval = val;
        self
    }

    #[track_caller]
    pub fn process_cqes_interval(mut self, val: u32) -> Self {
        assert!(val > 0, "process_cqes_interval must be greater than 0");
        self.process_cqes_interval = val;
        self
    }

    #[track_caller]
    pub fn submit_interval(mut self, val: u32) -> Self {
        assert!(val > 0, "submit_interval must be greater than 0");
        self.submit_interval = val;
        self
    }

    #[track_caller]
    pub fn max_unsubmitted_sqes(mut self, val: usize) -> Self {
        assert!(val > 0, "max_unsubmitted_sqes must be greater than 0");
        self.max_unsubmitted_sqes = val;
        self
    }

    #[track_caller]
    pub fn max_steal_retries(mut self, val: usize) -> Self {
        assert!(val > 0, "max_steal_retries must be greater than 0");
        self.max_steal_retries = val;
        self
    }

    #[track_caller]
    pub fn sq_ring_size(mut self, val: usize) -> Self {
        assert!(val.is_power_of_two(), "sq_ring_size must be a power of two");
        self.sq_ring_size = val;
        self
    }

    #[track_caller]
    pub fn cq_ring_size_multiplier(mut self, val: usize) -> Self {
        assert!(val > 0, "cq_ring_size_multiplier must be greater than 0");
        self.cq_ring_size_multiplier = val;
        self
    }

    #[track_caller]
    pub fn direct_fds_per_ring(mut self, val: u32) -> Self {
        assert!(val > 0, "direct_fds_per_ring must be greater than 0");
        self.direct_fds_per_ring = val;
        self
    }

    pub fn on_cleanup_error(mut self, on_cleanup: OnCleanupError) -> Self {
        self.on_cleanup_error = on_cleanup;
        self
    }

    pub fn orphan_policy(mut self, orphan_policy: OrphanPolicy) -> Self {
        self.orphan_policy = orphan_policy;
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

        // There is no way to modify the current thread name using `std::thread`,
        // so we use `libc::` and platform specific low-level interface.
        set_current_thread_name(&cfg.thread_name);
        context::init_local_context(scheduler.clone());

        Ok(Runtime::new(Scheduler::Local(scheduler)))
    }

    fn try_build_stealing_runtime(self) -> Result<Runtime> {
        let cfg = self.try_into()?;
        let scheduler = stealing::Scheduler::new(&cfg).into_handle();

        scheduler.spawn_workers();

        Ok(Runtime::new(Scheduler::Stealing(scheduler)))
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

    /// Shutdown the runtime.
    ///
    /// Tasks spawned through [`ringolo::spawn`] keep running until they yield.
    /// Then they are dropped. They are not *guaranteed* to run to completion,
    /// but *might* do so if they do not yield until completion.
    ///
    /// The thread initiating the shutdown blocks until all spawned work has been
    /// stopped. This can take an indefinite amount of time. The `Drop`
    /// implementation waits forever for this.
    pub fn shutdown(self) {
        self.shutdown_inner();
    }

    fn shutdown_inner(&self) {
        match &self.scheduler {
            Scheduler::Local(handle) => {
                IS_RUNTIME_ACTIVE.with(|is_active| is_active.set(false));
                handle.shutdown();
            }
            Scheduler::Stealing(handle) => {
                if let Err(e) = handle.shutdown() {
                    eprintln!("error during runtime shutdown: {:?}", e);
                }
            }
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.shutdown_inner();
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
    pub(crate) kind: Kind,
    pub(crate) worker_threads: usize,
    pub(crate) thread_name: ThreadNameFn,
    pub(crate) thread_stack_size: Option<usize>,
    pub(crate) global_queue_interval: u32,
    pub(crate) process_cqes_interval: u32,
    pub(crate) submit_interval: u32,
    pub(crate) max_unsubmitted_sqes: usize,
    pub(crate) max_steal_retries: usize,
    pub(crate) sq_ring_size: usize,
    pub(crate) cq_ring_size_multiplier: usize,
    pub(crate) direct_fds_per_ring: u32,
    pub(crate) on_cleanup_error: OnCleanupError,
    pub(crate) orphan_policy: OrphanPolicy,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            kind: Kind::Local,
            worker_threads: 1,
            thread_name: default_thread_name_fn(),
            thread_stack_size: None,
            global_queue_interval: GLOBAL_QUEUE_INTERVAL,
            process_cqes_interval: PROCESS_CQES_INTERVAL,
            submit_interval: SUBMIT_INTERVAL,
            max_unsubmitted_sqes: MAX_UNSUBMITTED_SQES,
            max_steal_retries: MAX_STEAL_RETRIES,
            sq_ring_size: SQ_RING_SIZE,
            cq_ring_size_multiplier: CQ_RING_SIZE_MULTIPLIER,
            direct_fds_per_ring: DIRECT_FDS_PER_RING,
            on_cleanup_error: OnCleanupError::default(),
            orphan_policy: OrphanPolicy::default(),
        }
    }
}

impl RuntimeConfig {
    fn validate(&self) -> Result<()> {
        let num_workers = match self.kind {
            Kind::Local => 1,
            Kind::Stealing => self.worker_threads,
        };

        check_fd_ulimit(num_workers * self.direct_fds_per_ring as usize)?;

        Ok(())
    }

    pub(crate) fn default_task_opts(&self) -> TaskOpts {
        let mut opts = TaskOpts::default();

        // All tasks are sticky on local scheduler since we have a single thread.
        if matches!(self.kind, Kind::Local) {
            opts |= TaskOpts::STICKY;
        }

        if matches!(self.orphan_policy, OrphanPolicy::Enforced) {
            opts |= TaskOpts::CANCEL_ALL_CHILDREN_ON_EXIT;
        };

        opts
    }
}

impl TryFrom<Builder> for RuntimeConfig {
    type Error = anyhow::Error;

    fn try_from(builder: Builder) -> Result<Self, Self::Error> {
        let worker_threads = builder
            .worker_threads
            .unwrap_or(thread::available_parallelism()?.get());

        let cfg = RuntimeConfig {
            kind: builder.kind,
            worker_threads,
            thread_name: builder.thread_name,
            thread_stack_size: builder.thread_stack_size,
            global_queue_interval: builder.global_queue_interval,
            process_cqes_interval: builder.process_cqes_interval,
            submit_interval: builder.submit_interval,
            max_unsubmitted_sqes: builder.max_unsubmitted_sqes,
            max_steal_retries: builder.max_steal_retries,
            sq_ring_size: builder.sq_ring_size,
            cq_ring_size_multiplier: builder.cq_ring_size_multiplier,
            direct_fds_per_ring: builder.direct_fds_per_ring,
            on_cleanup_error: builder.on_cleanup_error,
            orphan_policy: builder.orphan_policy,
        };

        cfg.validate()?;

        Ok(cfg)
    }
}

/// Checks if the desired number of file descriptors is within the system's soft limit.
fn check_fd_ulimit(desired_fds: usize) -> io::Result<()> {
    let mut rlimit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, rlimit.as_mut_ptr()) };

    if ret != 0 {
        return Err(io::Error::last_os_error());
    }

    let rlimit = unsafe { rlimit.assume_init() };
    let current_limit = rlimit.rlim_cur as usize;

    if desired_fds > current_limit {
        let error_message = format!(
            "Required file descriptors ({}) exceed the current ulimit ({}) for open files. \
             Please increase the limit. For example, run '$ ulimit -n 65536' in your shell \
             before starting the application.",
            desired_fds, current_limit
        );
        Err(io::Error::other(error_message))
    } else {
        Ok(())
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
