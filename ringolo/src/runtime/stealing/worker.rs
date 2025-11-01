use crate::context;
use crate::runtime::stealing;
use crate::runtime::stealing::scheduler::StealableTask;
use crate::runtime::ticker::{Ticker, TickerData, TickerEvents};
use crate::runtime::{AddMode, EventLoop, RuntimeConfig};
use anyhow::Result;
use crossbeam_deque::{Injector, Stealer, Worker as CbWorker};
use std::cell::{OnceCell, RefCell};
use std::collections::VecDeque;
use std::iter;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread::{self, ThreadId};
use std::time::Duration;

#[derive(Debug)]
pub(crate) struct Worker {
    /// Optimization to avoid TLS read to get thread_id when calling
    /// `task.set_owner_id()`;
    thread_id: OnceCell<thread::ThreadId>,

    /// Determines how we run the event loop.
    cfg: RefCell<EventLoopConfig>,

    /// Event loop ticker.
    ticker: RefCell<Ticker>,

    /// Whether we should try to find our next task in the global queue.
    pop_global_queue: RefCell<bool>,

    /// Global injector queue where new tasks are pushed.
    global: Arc<Injector<StealableTask>>,

    /// Local pollable queue for tasks that are not stealable by other threads.
    /// This means that these tasks have locally pending IO on this thread's
    /// iouring.
    /// We use a VecDeque to be able to push/pop from both ends, this way we can
    /// respect FIFO/LIFO ordering interchangeably.
    pollable: RefCell<VecDeque<StealableTask>>,

    /// Local stealable queue for tasks that don't have any locally pending IO.
    /// By definition they are also pollable.
    stealable: CbWorker<StealableTask>,

    /// Handle to all of the other worker's stealable queues. If there are N
    /// workers we will have N-1 queues to steal from.
    stealers: Vec<Stealer<StealableTask>>,
}

impl Worker {
    pub(super) fn new(
        cfg: &RuntimeConfig,
        global: Arc<Injector<StealableTask>>,
        stealable: CbWorker<StealableTask>,
        mut stealers: Vec<Stealer<StealableTask>>,
    ) -> Self {
        // Shuffle the stealers so that each worker's search order when trying to
        // steal work is different and hopefully unique to reduce contention.
        fastrand::shuffle(&mut stealers);

        Self {
            thread_id: OnceCell::new(),
            cfg: RefCell::new(cfg.into()),
            ticker: RefCell::new(Ticker::new()),
            pop_global_queue: RefCell::new(false),
            global,
            pollable: RefCell::new(VecDeque::new()),
            stealable,
            stealers,
        }
    }

    fn tick<T: TickerData>(&self, ctx: &T::Context, data: &mut T) -> TickerEvents {
        self.ticker.borrow_mut().tick(ctx, data)
    }

    pub(super) fn thread_id(&self) -> &ThreadId {
        self.thread_id.get_or_init(|| thread::current().id())
    }
}

// Safety: we store worker in `Arc<Worker>` on scheduler, but will guarantee we
// access worker fields in a safe way.
unsafe impl Send for Worker {}
unsafe impl Sync for Worker {}

impl EventLoop for Worker {
    type Task = StealableTask;

    fn add_task(&self, task: Self::Task, mode: AddMode) {
        task.set_owner_id(*self.thread_id());
        let mut pollable = self.pollable.borrow_mut();

        // # Fast path
        //
        // If we have no tasks on this worker, we add it to pollable and poll
        // it right away. This prevents having a task "bounce around" workers, where each
        // worker add the task to "stealable", while they have nothing else to do, and
        // every worker is trying to steal that single task away.
        if pollable.is_empty() {
            pollable.push_back(task);
            return;
        }

        match mode {
            // A task is stealable if it has no pending IO on the local `io_uring`.
            // This is why we have a separate `stealable` queue from which other
            // workers can steal tasks from.
            AddMode::Lifo => {
                if !task.is_stealable() {
                    pollable.push_back(task);
                } else {
                    self.stealable.push(task);
                }
            }
            // Our only option to enforce FIFO is to use the pollable queue, because
            // crossbeam worker is setup with LIFO. This is notably used to implement
            // `yield_now` which is infrequent but very useful.
            AddMode::Fifo => pollable.push_front(task),
        }
    }

    fn find_task(&self) -> Option<Self::Task> {
        // Enforce scheduler fairness by force-checking the global queue with
        // frequency `global_queue_interval`.
        if self.pop_global_queue.replace(false)
            && let Some(task) = self.global.steal_batch_and_pop(&self.stealable).success()
        {
            return Some(task);
        }

        // 1. Always start popping from pollable as these tasks were just scheduled so
        //    we expect the CPU cache to be hot because of LIFO.
        //
        // Another reason why this is a good choice is that it gives more time for
        // other workers to steal tasks from our stealable queue.
        self.pollable.borrow_mut().pop_back().or_else(|| {
            // 2. Look in our stealable queue
            self.stealable.pop().or_else(|| {
                // 3. No local work, repeatedly try the global injector and other
                //    workers stealable queues.
                iter::repeat_with(|| {
                    // Work from the injector is by definition stealable as it has not
                    // scheduled any IO on any thread. This is why we put the batch in
                    // our stealable queue.
                    self.global.steal_batch_and_pop(&self.stealable).or_else(||
                            // The behavior of collect here is to return the first Success(T) so
                            // *we are not* iterating through all stealers everytime.
                            self.stealers.iter().map(|s| s.steal()).collect())
                })
                // Repeat a maximum of `MAX_STEAL_RETRIES` otherwise return None.
                .take(self.cfg.borrow().max_steal_retries)
                .find(|s| !s.is_retry())
                .and_then(|s| s.success())
            })
        })
    }

    fn event_loop<F: Future>(&self, root_future: Option<F>) -> Result<Option<F::Output>> {
        debug_assert!(
            root_future.is_none(),
            "Non root worker does not have root_future"
        );
        context::expect_stealing_scheduler(|ctx, scheduler| {
            scheduler.wait_for_thread_pool();
            ctx.with_core(|c| c.spawn_maintenance_task());

            scheduler.wait_for_all_maintenance_tasks();
            self.event_loop_inner::<F>(ctx, scheduler)
        })
    }
}

// This is a short-term fix - we force-wake the worker after waiting 100ms for the
// next completion. This is to avoid shutdown latency. The real long-term solution
// is to trigger a wakeup through the RingMessage protocol.
const WAIT_NEXT_COMPLETION_TIMEOUT: Duration = Duration::from_millis(100);

impl Worker {
    fn event_loop_inner<F: Future>(
        &self,
        ctx: &stealing::Context,
        _scheduler: &stealing::Handle, // Not used by non-root workers
    ) -> Result<Option<F::Output>> {
        'event_loop: loop {
            if let Some(task) = self.find_task() {
                task.run();
            } else if ctx.with_core(|core| core.get_pending_ios() > 0) {
                // Block thread waiting for next completion.
                ctx.with_slab_and_ring_mut(|slab, ring| -> Result<()> {
                    ring.submit_and_wait(1, Some(WAIT_NEXT_COMPLETION_TIMEOUT))?;
                    ring.process_cqes(slab, None)?;
                    Ok(())
                })?;
            } else {
                // Park the thread, and wait for new tasks to be scheduled on the global injector and
                // an unpark signal from the scheduler.
                ctx.shared.park_current_thread(&self.global);
            }

            let events = self.tick(ctx, &mut *self.cfg.borrow_mut());
            if let ControlFlow::Break(_) = self.process_ticker_events(ctx, events)? {
                break 'event_loop;
            }
        }

        self.shutdown();
        Ok(None)
    }

    #[inline(always)]
    fn process_ticker_events(
        &self,
        ctx: &stealing::Context,
        events: TickerEvents,
    ) -> Result<ControlFlow<()>> {
        if events.contains(TickerEvents::SHUTDOWN) {
            return Ok(ControlFlow::Break(()));

            // Enforce scheduler fairness for newly scheduled tasks.
        } else if events.contains(TickerEvents::POP_GLOBAL_QUEUE) {
            self.pop_global_queue.replace(true);

            // Because we use `DEFER_TASKRUN`, we need to make an `io_uring_enter` syscall /w GETEVENTS
            // to process pending kernel `task_work` and post CQEs. It would be silly to not submit
            // pending SQEs in same syscall.
        } else if events.contains(TickerEvents::PROCESS_CQES) {
            ctx.with_slab_and_ring_mut(|slab, ring| {
                ring.submit_and_wait(1, None)?;
                ring.process_cqes(slab, None)
            })?;

            // Only submit, no need to wait for anything.
        } else if events.contains(TickerEvents::SUBMIT_SQES) {
            ctx.with_ring_mut(|ring| ring.submit_no_wait())?;
        }

        Ok(ControlFlow::Continue(()))
    }

    fn shutdown(&self) {
        crate::with_scheduler!(|s| {
            if let Some(tasks) = s.tasks.wait_for_shutdown_partition(self.thread_id()) {
                for task in tasks {
                    task.shutdown();
                }
            }
        })
    }
}

#[derive(Debug, Clone)]
struct EventLoopConfig {
    // Policies
    global_queue_interval: u32,

    process_cqes_interval: u32,

    submit_interval: u32,

    max_unsubmitted_sqes: usize,

    max_steal_retries: usize,

    // Data
    unsubmitted_sqes: usize,

    has_ready_cqes: bool,
}

impl EventLoopConfig {
    #[inline(always)]
    fn update(&mut self, ctx: &stealing::Context) {
        ctx.with_ring_mut(|ring| {
            self.unsubmitted_sqes = ring.num_unsubmitted_sqes();
            self.has_ready_cqes = ring.has_ready_cqes();
        });
    }

    #[inline(always)]
    fn should_pop_global_queue(&self, tick: u32) -> bool {
        tick.is_multiple_of(self.global_queue_interval)
    }

    #[inline(always)]
    fn should_process_cqes(&self, tick: u32) -> bool {
        self.has_ready_cqes && tick.is_multiple_of(self.process_cqes_interval)
    }

    #[inline(always)]
    fn should_submit(&self, tick: u32) -> bool {
        self.unsubmitted_sqes > 0
            && (tick.is_multiple_of(self.submit_interval)
                || self.unsubmitted_sqes >= self.max_unsubmitted_sqes)
    }
}

impl From<&RuntimeConfig> for EventLoopConfig {
    fn from(cfg: &RuntimeConfig) -> EventLoopConfig {
        EventLoopConfig {
            global_queue_interval: cfg.global_queue_interval,
            process_cqes_interval: cfg.process_cqes_interval,
            submit_interval: cfg.submit_interval,
            max_unsubmitted_sqes: cfg.max_unsubmitted_sqes,
            max_steal_retries: cfg.max_steal_retries,
            unsubmitted_sqes: 0,
            has_ready_cqes: false,
        }
    }
}

impl TickerData for EventLoopConfig {
    type Context = stealing::Context;

    #[inline(always)]
    fn update_and_check(&mut self, ctx: &Self::Context, tick: u32) -> TickerEvents {
        // (1) Fast-path to enforce shutdown
        if ctx.shared.shutdown.load(Ordering::Acquire) {
            return TickerEvents::SHUTDOWN;
        }

        // (2) Update data
        self.update(ctx);

        // (3) Check policies
        let mut events = TickerEvents::empty();

        if self.should_pop_global_queue(tick) {
            events.insert(TickerEvents::POP_GLOBAL_QUEUE);
        }

        if self.should_process_cqes(tick) {
            events.insert(TickerEvents::PROCESS_CQES);
        }

        if self.should_submit(tick) {
            events.insert(TickerEvents::SUBMIT_SQES);
        }

        events
    }
}
