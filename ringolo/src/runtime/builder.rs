use crate::runtime::Runtime;
use std::default;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// TODO missing from tokio:
// - UnhandledPanic
// - thread stack size
// - metrics

/// How many ticks before yielding to check I/O completions
const EVENT_INTERVAL: u32 = 61;

const MAX_BLOCKING_THREADS: usize = 512;

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    CurrentThread,
    MultiThread,
}

pub(crate) type ThreadNameFn = Arc<dyn Fn() -> String + Send + Sync + 'static>;

fn default_thread_name_fn() -> ThreadNameFn {
    let counter = Arc::new(AtomicUsize::new(0));

    Arc::new(move || {
        let prev = counter.fetch_add(1, Ordering::Relaxed);
        format!("ringolo-worker-{}", prev)
    })
}

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
}

impl Builder {
    pub(crate) fn new(kind: Kind) -> Self {
        Self {
            kind,
            worker_threads: None,
            max_blocking_threads: MAX_BLOCKING_THREADS,
            thread_name: default_thread_name_fn(),
            thread_stack_size: None,
            global_queue_interval: None,
            event_interval: EVENT_INTERVAL,
        }
    }

    /// Returns a new builder with the current thread scheduler selected.
    ///
    /// Configuration methods can be chained on the return value.
    pub fn new_current_thread() -> Builder {
        Builder::new(Kind::CurrentThread)
    }

    pub fn new_multi_thread() -> Builder {
        Builder::new(Kind::MultiThread)
    }

    pub fn worker_threads(&mut self, val: usize) -> &mut Self {
        assert!(val > 0, "Worker threads cannot be set to 0");
        self.worker_threads = Some(val);
        self
    }

    // TODO
    fn max_blocking_threads(&mut self, val: usize) -> &mut Self {
        unimplemented!("TODO");
        // assert!(val > 0, "Max blocking threads cannot be set to 0");
        // self.max_blocking_threads = val;
        // self
    }

    /// Sets name of threads spawned by the `Runtime`'s thread pool.
    ///
    /// The default name is "ringolo-worker-{N}".
    pub fn thread_name(&mut self, val: impl Into<String>) -> &mut Self {
        let val = val.into();
        self.thread_name = Arc::new(move || val.clone());
        self
    }

    /// Sets a function used to generate the name of threads spawned by the `Runtime`'s thread pool.
    ///
    /// The default name fn yields worker names with monotonically increasing N
    /// "ringolo-worker-{N}".
    pub fn thread_name_fn<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() -> String + Send + Sync + 'static,
    {
        self.thread_name = Arc::new(f);
        self
    }

    /// Sets the stack size (in bytes) for worker threads.
    ///
    /// The actual stack size may be greater than this value if the platform
    /// specifies minimal stack size.
    ///
    /// The default stack size for spawned threads is 2 MiB, though this
    /// particular stack size is subject to change in the future.
    pub fn thread_stack_size(&mut self, val: usize) -> &mut Self {
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
    pub fn global_queue_interval(&mut self, val: u32) -> &mut Self {
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
    pub fn event_interval(&mut self, val: u32) -> &mut Self {
        self.event_interval = val;
        self
    }

    /// Creates the configured `Runtime`.
    ///
    /// The returned `Runtime` instance is ready to spawn tasks.
    pub fn build(&mut self) -> io::Result<Runtime> {
        match &self.kind {
            Kind::CurrentThread => self.build_current_thread_runtime(),
            Kind::MultiThread => self.build_threaded_runtime(),
        }
    }
}
