use crate::context::{self, init_stealing_context};
use crate::runtime::stealing;
use crate::runtime::stealing::scheduler::StealableTask;
use crate::runtime::ticker::{Ticker, TickerData, TickerEvents};
use crate::runtime::waker::waker_ref;
use crate::runtime::{AddMode, EventLoop, RuntimeConfig};
use crate::task::TaskNodeGuard;
use crate::task::ThreadId;
use crate::utils::thread::set_current_thread_name;
use anyhow::Result;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::pin::pin;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

#[derive(Debug)]
pub(crate) struct RootWorker {
    /// ThreadId used to set the owner id on tasks.
    pub(crate) thread_id: ThreadId,

    /// Determines how we run the event loop.
    cfg: RefCell<EventLoopConfig>,

    /// Event loop ticker.
    ticker: RefCell<Ticker>,

    /// Only used to schedule the maintenance task for now. The RootWorker does
    /// not participate in work stealing.
    pollable: RefCell<VecDeque<StealableTask>>,
}

impl RootWorker {
    pub(super) fn new(scheduler: &stealing::Handle) -> Self {
        let thread_id = ThreadId::next();

        // There is no way to modify the current thread name using `std::thread`,
        // so we use `libc::` and platform specific low-level interface.
        set_current_thread_name(&scheduler.cfg.thread_name);
        init_stealing_context(thread_id, scheduler.clone());

        Self {
            thread_id,
            cfg: RefCell::new((&scheduler.cfg).into()),
            ticker: RefCell::new(Ticker::new()),
            pollable: RefCell::new(VecDeque::with_capacity(1 /* only maintenance task */)),
        }
    }

    fn tick<T: TickerData>(&self, ctx: &T::Context, data: &mut T) -> TickerEvents {
        self.ticker.borrow_mut().tick(ctx, data)
    }
}

impl EventLoop for RootWorker {
    type Task = StealableTask;

    fn add_task(&self, task: Self::Task, mode: AddMode) {
        debug_assert!(
            task.is_maintenance_task() || matches!(mode, AddMode::Cancel),
            "Can only schedule maintenance task or unclaimed tasks
                        that needs cancellation on root_worker"
        );
        task.set_owner_id(self.thread_id);
        self.pollable.borrow_mut().push_back(task);
    }

    fn find_task(&self) -> Option<Self::Task> {
        self.pollable.borrow_mut().pop_back()
    }

    fn event_loop<F: Future>(&self, root_future: Option<F>) -> Result<Option<F::Output>> {
        debug_assert!(root_future.is_some(), "root worker needs a root_future");

        context::expect_stealing_scheduler(|ctx, scheduler| {
            scheduler.wait_for_thread_pool();

            ctx.with_core(|c| c.spawn_maintenance_task());

            scheduler.wait_for_all_maintenance_tasks();

            self.event_loop_inner(ctx, scheduler, root_future.unwrap())
        })
    }
}

const WAIT_NEXT_COMPLETION_TIMEOUT: Duration = Duration::from_millis(100);

impl RootWorker {
    fn event_loop_inner<F: Future>(
        &self,
        ctx: &stealing::Context,
        scheduler: &stealing::Handle,
        root_fut: F,
    ) -> Result<Option<F::Output>> {
        let mut data = self.cfg.borrow_mut();
        let mut root_fut = pin!(root_fut);

        let waker = waker_ref(scheduler);
        let mut cx = std::task::Context::from_waker(&waker);

        loop {
            if let Some(task) = self.find_task() {
                task.run();
            } else if ctx.with_core(|core| core.get_pending_ios() > 0) {
                // Block thread waiting for next completion.
                ctx.with_slab_and_ring_mut(|slab, ring| -> Result<()> {
                    ring.submit_and_wait(1, Some(WAIT_NEXT_COMPLETION_TIMEOUT))?;
                    ring.process_cqes(slab, None)?;
                    Ok(())
                })?;
            }

            if scheduler.reset_root_woken() {
                let _guard = TaskNodeGuard::enter_root();

                if let Poll::Ready(v) = root_fut.as_mut().poll(&mut cx) {
                    return Ok(Some(v));
                }
            }

            let events = self.tick(ctx, &mut *data);
            self.process_ticker_events(ctx, events)?;
        }
    }

    #[inline(always)]
    fn process_ticker_events(&self, ctx: &stealing::Context, events: TickerEvents) -> Result<()> {
        if events.contains(TickerEvents::PROCESS_CQES) {
            ctx.with_slab_and_ring_mut(|slab, ring| {
                ring.submit_and_wait(1, None)?;
                ring.process_cqes(slab, None)
            })?;

            // Only submit, no need to wait for anything.
        } else if events.contains(TickerEvents::SUBMIT_SQES) {
            ctx.with_ring_mut(|ring| ring.submit_no_wait())?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct EventLoopConfig {
    // Policies
    process_cqes_interval: u32,

    submit_interval: u32,

    max_unsubmitted_sqes: usize,

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
            process_cqes_interval: cfg.process_cqes_interval,
            submit_interval: cfg.submit_interval,
            max_unsubmitted_sqes: cfg.max_unsubmitted_sqes,
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
        if ctx.shared.shutdown.load(Ordering::Relaxed) {
            return TickerEvents::SHUTDOWN;
        }

        // (2) Update data
        self.update(ctx);

        // (3) Check policies
        let mut events = TickerEvents::empty();

        if self.should_process_cqes(tick) {
            events.insert(TickerEvents::PROCESS_CQES);
        }

        if self.should_submit(tick) {
            events.insert(TickerEvents::SUBMIT_SQES);
        }

        events
    }
}
