use bitflags::bitflags;

#[derive(Debug)]
pub(crate) struct Ticker {
    tick: u32,
}

impl Ticker {
    pub(crate) fn new() -> Self {
        Self { tick: 0 }
    }

    pub(crate) fn tick<T: TickerData>(&mut self, ctx: &T::Context, data: &mut T) -> TickerEvents {
        self.tick = self.tick.wrapping_add(1);
        data.update_and_check(ctx, self.tick)
    }
}

/// Trait for each flavor of scheduler to implement. The scheduler is responsible
/// for consuming the tick and generating TickerEvent. This way we decouple logic
/// from data.
pub(crate) trait TickerData {
    type Context;

    // We perform a maximum of one action per tick, so it is important to order
    // actions by importance. We might want to consider multiple actions per tick
    // model.
    fn update_and_check(&mut self, ctx: &Self::Context, tick: u32) -> TickerEvents;
}

bitflags! {
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(crate) struct TickerEvents: u16 {
        /// A request to submit any pending submission queue entries to the kernel.
        const SUBMIT_SQES = 1;

        /// A request to process any available completion queue entries from the kernel.
        const PROCESS_CQES = 1 << 1;

        /// A request to shut down the runtime.
        const SHUTDOWN = 1 << 2;

        /// An unhandled panic happened while polling tasks.
        const UNHANDLED_PANIC = 1 << 3;
    }
}
