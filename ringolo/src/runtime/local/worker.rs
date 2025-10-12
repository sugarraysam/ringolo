use crate::context::{expect_local_scheduler, with_core_mut, with_slab};
use crate::runtime::local;
use crate::runtime::local::scheduler::LocalTask;
use crate::runtime::runtime::RuntimeConfig;
use crate::runtime::waker::waker_ref;
use crate::runtime::{AddMode, EventLoop};
use crate::runtime::{Ticker, TickerData, TickerEvents};
use anyhow::{Result, anyhow};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::pin::pin;
use std::task::Poll;

#[derive(Debug)]
pub(crate) struct Worker {
    /// Handle to single local queue.
    pollable: RefCell<VecDeque<LocalTask>>,

    /// Determines how we run the event loop.
    cfg: RefCell<EventLoopConfig>,

    /// Event loop ticker.
    ticker: RefCell<Ticker>,
}

impl Worker {
    pub(super) fn new(cfg: &RuntimeConfig) -> Self {
        Self {
            pollable: RefCell::new(VecDeque::new()),
            cfg: RefCell::new(cfg.into()),
            ticker: RefCell::new(Ticker::new()),
        }
    }

    fn tick<T: TickerData>(&self, ctx: &T::Context, data: &mut T) -> TickerEvents {
        self.ticker.borrow_mut().tick(ctx, data)
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
        ctx.with_core(|core| {
            self.unsubmitted_sqes = core.num_unsubmitted_sqes();
            self.has_ready_cqes = core.has_ready_cqes();
        });
    }

    #[inline(always)]
    fn should_submit(&self, tick: u32) -> bool {
        self.unsubmitted_sqes > 0
            && (tick % self.submit_interval == 0
                || self.unsubmitted_sqes >= self.max_unsubmitted_sqes)
    }

    #[inline(always)]
    fn should_process_cqes(&self, tick: u32) -> bool {
        self.has_ready_cqes && tick % self.process_cqes_interval == 0
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

        // TODO:
        // - shutdown
        // - unhandled panic

        if self.should_submit(tick) {
            events.insert(TickerEvents::SUBMIT_SQES);
        }

        if self.should_process_cqes(tick) {
            events.insert(TickerEvents::PROCESS_CQES);
        }

        events
    }
}

impl EventLoop for Worker {
    type Task = LocalTask;

    fn add_task(&self, task: Self::Task, mode: AddMode) {
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

    fn event_loop<F: Future>(&self, root_fut: Option<F>) -> Result<F::Output> {
        if let Some(root_fut) = root_fut {
            expect_local_scheduler(|ctx, scheduler| self.event_loop_inner(ctx, scheduler, root_fut))
        } else {
            Err(anyhow!("Unexpected empty root_future"))
        }
    }
}

impl Worker {
    fn event_loop_inner<F: Future>(
        &self,
        ctx: &local::Context,
        scheduler: &local::Handle,
        root_fut: F,
    ) -> Result<F::Output> {
        let mut data = self.cfg.borrow_mut();
        let mut root_fut = pin!(root_fut);

        let waker = waker_ref(scheduler);
        let mut cx = std::task::Context::from_waker(&waker);

        let mut root_result: Option<F::Output> = None;

        // Here are a couple of tricky things about the event loop:
        // - the root future might finish before OR after all tasks are done
        // - if the root future panics, we propagate to user immediately
        // - if a spawned task panics, it is handled asynchronously
        // - weird interactions with SQ ring or Slab can result in corrupted state
        //   and force scheduler to panic
        loop {
            if scheduler.reset_root_woken() && root_result.is_none() {
                dbg!("polling root future");
                let _g = ctx.set_polling_root();

                if let Poll::Ready(v) = root_fut.as_mut().poll(&mut cx) {
                    root_result = Some(v);
                }
            }

            let events = self.tick(ctx, &mut *data);
            self.process_ticker_events(ctx, events)?;

            if let Some(task) = self.find_task() {
                task.run();
            } else {
                // No more work to do. This means it is time to poll the root future.
                if with_slab(|s| s.pending_ios == 0) {
                    // By definition if there are no pending IOs, then we should have no `ready_cqes`
                    // and no `unsubmitted_sqes` because we have a 1:1 mapping between RawSqe <-> SQE.
                    debug_assert!(!data.has_ready_cqes);
                    debug_assert!(data.unsubmitted_sqes == 0);

                    // TODO: do we have to check scheduler.tasks.is_empty() be false?
                    match root_result.is_some() {
                        true => return Ok(root_result.unwrap()),
                        false => scheduler.set_root_woken(),
                    }
                } else {
                    dbg!("parking thread waiting for completions");
                    // "Park" the thread waiting for next completion.
                    with_core_mut(|core| -> Result<()> {
                        core.submit_and_wait(1, None)?;
                        core.process_cqes(None)?;
                        Ok(())
                    })?;
                }
            }
        }
    }

    #[inline(always)]
    fn process_ticker_events(&self, ctx: &local::Context, events: TickerEvents) -> Result<()> {
        if events.contains(TickerEvents::SHUTDOWN) {
            unimplemented!("TODO");

        // Because we use `DEFER_TASKRUN`, we need to make an `io_uring_enter` syscall /w GETEVENTS
        // to process pending kernel `task_work` and post CQEs. It would be silly to not submit
        // pending SQEs in same syscall.
        } else if events.contains(TickerEvents::PROCESS_CQES) {
            dbg!("processing cqes");
            ctx.with_core_mut(|core| {
                core.submit_and_wait(1, None)?;
                core.process_cqes(None)
            })?;

        // Only submit, no need to wait for anything.
        } else if events.contains(TickerEvents::SUBMIT_SQES) {
            dbg!("submitting sqes");
            ctx.with_core_mut(|core| core.submit_no_wait())?;
        }

        Ok(())
    }
}
