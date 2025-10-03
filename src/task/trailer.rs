#![allow(unsafe_op_in_unsafe_fn)]

use std::cell::UnsafeCell;
use std::task::Waker;

/// Cold data is stored after the future. Data is considered cold if it is only
/// used during creation or shutdown of the task.
pub(super) struct Trailer {
    /// Consumer task waiting on completion of this task.
    pub(super) waker: UnsafeCell<Option<Waker>>,
}

impl Trailer {
    pub(super) fn new() -> Self {
        Trailer {
            waker: UnsafeCell::new(None),
        }
    }

    pub(super) unsafe fn set_waker(&self, waker: Option<Waker>) {
        let ptr = self.waker.get();
        *ptr = waker;
    }

    pub(super) unsafe fn will_wake(&self, waker: &Waker) -> bool {
        let ptr = self.waker.get();
        (*ptr).as_ref().unwrap().will_wake(waker)
    }

    pub(super) fn wake_join(&self) {
        let ptr = self.waker.get();
        match unsafe { &*ptr } {
            Some(waker) => waker.wake_by_ref(),
            None => panic!("waker missing"),
        };
    }
}
