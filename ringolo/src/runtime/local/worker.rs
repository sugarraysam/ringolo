use crate::context;
use crate::runtime::local;
use crate::runtime::local::scheduler::LocalTask;
use crate::runtime::runtime::RuntimeConfig;
use crate::runtime::waker::waker_ref;
use crate::runtime::{AddMode, EventLoop};
use crate::runtime::{Ticker, TickerData, TickerEvents};
use crate::task::TaskNodeGuard;
use crate::task::ThreadId;
use anyhow::{Result, anyhow};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

#[derive(Debug)]
pub(crate) struct Worker {
    /// ThreadId to set owner_id on task
    pub(crate) thread_id: ThreadId,

    /// Determines how we run the event loop.
    cfg: RefCell<EventLoopConfig>,

    /// Event loop ticker.
    ticker: RefCell<Ticker>,

    /// Handle to single local queue.
    pollable: RefCell<VecDeque<LocalTask>>,
}

impl Worker {
    pub(super) fn new(cfg: &RuntimeConfig) -> Self {
        Self {
            thread_id: ThreadId::next(),
            cfg: RefCell::new(cfg.into()),
            ticker: RefCell::new(Ticker::new()),
            pollable: RefCell::new(VecDeque::new()),
        }
    }

    fn tick<T: TickerData>(&self, ctx: &T::Context, data: &mut T) -> TickerEvents {
        self.ticker.borrow_mut().tick(ctx, data)
    }
}

impl EventLoop for Worker {
    type Task = LocalTask;

    fn add_task(&self, task: Self::Task, mode: AddMode) {
        task.set_owner_id(self.thread_id);

        let mut q = self.pollable.borrow_mut();

        // We always pop from the back, so the start of the q is the back, and
        // the end corresponds to the front.
        match mode {
            AddMode::Fifo => q.push_front(task),
            AddMode::Lifo => q.push_back(task),
        }
    }

    fn find_task(&self) -> Option<Self::Task> {
        self.pollable.borrow_mut().pop_back()
    }

    fn event_loop<F: Future>(&self, root_fut: Option<F>) -> Result<Option<F::Output>> {
        if let Some(root_fut) = root_fut {
            context::expect_local_scheduler(|ctx, scheduler| {
                ctx.with_core(|c| c.spawn_maintenance_task());
                self.event_loop_inner(ctx, scheduler, root_fut)
            })
        } else {
            Err(anyhow!("Unexpected empty root_future"))
        }
    }
}

const WAIT_NEXT_COMPLETION_TIMEOUT: Duration = Duration::from_millis(100);

impl Worker {
    fn event_loop_inner<F: Future>(
        &self,
        ctx: &local::Context,
        scheduler: &local::Handle,
        root_fut: F,
    ) -> Result<Option<F::Output>> {
        let mut data = self.cfg.borrow_mut();
        let mut root_fut = pin!(root_fut);

        let waker = waker_ref(scheduler);
        let mut cx = std::task::Context::from_waker(&waker);

        let mut root_result: Option<F::Output> = None;

        loop {
            if let Some(task) = self.find_task() {
                task.run();
            } else if ctx.with_core(|core| core.get_pending_ios() > 0) {
                // Block thread waiting for next completion.
                context::with_slab_and_ring_mut(|slab, ring| -> Result<()> {
                    ring.submit_and_wait(1, Some(WAIT_NEXT_COMPLETION_TIMEOUT))?;
                    ring.process_cqes(slab, None)?;
                    Ok(())
                })?;
            } else {
                match root_result.is_some() {
                    true => return Ok(root_result),
                    false => scheduler.set_root_woken(),
                }
            }

            if scheduler.reset_root_woken() && root_result.is_none() {
                let _guard = TaskNodeGuard::enter_root();

                if let Poll::Ready(v) = root_fut.as_mut().poll(&mut cx) {
                    root_result = Some(v);
                }
            }

            let events = self.tick(ctx, &mut *data);
            self.process_ticker_events(ctx, events)?;
        }
    }

    #[inline(always)]
    fn process_ticker_events(&self, ctx: &local::Context, events: TickerEvents) -> Result<()> {
        // Because we use `DEFER_TASKRUN`, we need to make an `io_uring_enter` syscall /w GETEVENTS
        // to process pending kernel `task_work` and post CQEs. It would be silly to not submit
        // pending SQEs in same syscall.
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
    submit_interval: u32,

    process_cqes_interval: u32,

    max_unsubmitted_sqes: usize,

    // Data
    unsubmitted_sqes: usize,

    has_ready_cqes: bool,
}

impl EventLoopConfig {
    #[inline(always)]
    fn update(&mut self, ctx: &local::Context) {
        ctx.with_ring_mut(|ring| {
            self.unsubmitted_sqes = ring.num_unsubmitted_sqes();
            self.has_ready_cqes = ring.has_ready_cqes();
        });
    }

    #[inline(always)]
    fn should_submit(&self, tick: u32) -> bool {
        self.unsubmitted_sqes > 0
            && (tick.is_multiple_of(self.submit_interval)
                || self.unsubmitted_sqes >= self.max_unsubmitted_sqes)
    }

    #[inline(always)]
    fn should_process_cqes(&self, tick: u32) -> bool {
        self.has_ready_cqes && tick.is_multiple_of(self.process_cqes_interval)
    }
}

impl From<&RuntimeConfig> for EventLoopConfig {
    fn from(from: &RuntimeConfig) -> EventLoopConfig {
        EventLoopConfig {
            submit_interval: from.submit_interval,
            process_cqes_interval: from.process_cqes_interval,
            max_unsubmitted_sqes: from.max_unsubmitted_sqes,
            unsubmitted_sqes: 0,
            has_ready_cqes: false,
        }
    }
}

impl TickerData for EventLoopConfig {
    type Context = local::Context;

    #[inline(always)]
    fn update_and_check(&mut self, ctx: &Self::Context, tick: u32) -> TickerEvents {
        // (1) Update data
        self.update(ctx);

        // (2) Check policies
        let mut events = TickerEvents::empty();

        if self.should_submit(tick) {
            events.insert(TickerEvents::SUBMIT_SQES);
        }

        if self.should_process_cqes(tick) {
            events.insert(TickerEvents::PROCESS_CQES);
        }

        events
    }
}
