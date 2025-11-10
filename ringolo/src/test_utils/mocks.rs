#![allow(unsafe_op_in_unsafe_fn)]

use crate::context;
use crate::runtime::{
    AddMode, OrphanPolicy, Schedule, SchedulerPanic, TaskOpts, TaskRegistry, YieldReason,
};
use crate::spawn::TaskMetadata;
use crate::task::layout::vtable;
use crate::task::{Header, Id, Notified, State, Task};
use std::future::Ready;
use std::mem::ManuallyDrop;
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{RawWaker, RawWakerVTable, Waker};

#[derive(Debug, Default, Copy, Clone)]
pub(crate) struct DummyScheduler;

impl Schedule for DummyScheduler {
    fn schedule(&self, _task: Notified<Self>, _mode: Option<AddMode>) {}

    fn yield_now(&self, _waker: &Waker, _reason: YieldReason, _mode: Option<AddMode>) {}

    fn release(&self, _task: &Task<Self>) -> Option<Task<Self>> {
        None
    }

    fn unhandled_panic(&self, _payload: SchedulerPanic) {}

    fn task_registry(&self) -> Arc<dyn TaskRegistry> {
        Arc::new(DummyTaskRegistry)
    }
}

pub(crate) fn mock_task(
    opts: Option<TaskOpts>,
    metadata: Option<TaskMetadata>,
) -> Task<DummyScheduler> {
    assert!(
        std::panic::catch_unwind(|| context::current_task()).is_ok(),
        "Task creation requires initialized context."
    );

    let (task, _, _) =
        crate::task::new_task(std::future::ready(1), opts, metadata, DummyScheduler {});
    task
}

#[derive(Debug)]
pub(crate) struct DummyTaskRegistry;

impl TaskRegistry for DummyTaskRegistry {
    fn shutdown(&self, _id: &Id) {}

    fn is_closed(&self) -> bool {
        false
    }

    fn orphan_policy(&self) -> OrphanPolicy {
        OrphanPolicy::default()
    }
}

#[repr(C)]
pub(crate) struct WakerData {
    // Header has to be the first field in the WakerData. This is because we
    // access the raw pointer from the `waker.data()` mechanics when submitting
    // IO. We can properly mock a RawTask by ensuring we can cast the `*const ()`
    // to NonNull<Header>.
    pub header: Header,

    pub wake_count: AtomicUsize,
}

impl WakerData {
    pub(crate) fn new() -> Self {
        let vtable = vtable::<Ready<()>, DummyScheduler>();

        Self {
            header: Header::new(State::new(), vtable, None),
            wake_count: AtomicUsize::new(0),
        }
    }

    pub(crate) fn get_count(&self) -> usize {
        self.wake_count.load(Ordering::Relaxed)
    }

    pub(crate) fn get_pending_ios(&self) -> u32 {
        unsafe {
            let ptr = ptr::addr_of!(self.header) as *mut Header;
            NonNull::new_unchecked(ptr).as_ref().get_pending_ios()
        }
    }
}

unsafe fn mock_wake(data: *const ()) {
    // Need to consume 1 Arc reference
    let data = Arc::<WakerData>::from_raw(data.cast());
    data.wake_count.fetch_add(1, Ordering::Relaxed);
}

unsafe fn mock_wake_by_ref(data: *const ()) {
    // Not consuming any Arc ref
    let data = ManuallyDrop::new(Arc::<WakerData>::from_raw(data.cast()));
    data.wake_count.fetch_add(1, Ordering::Relaxed);
}

// Drop the Waker Arc reference.
unsafe fn mock_drop(data: *const ()) {
    if !data.is_null() {
        drop(Arc::<WakerData>::from_raw(data.cast()));
    }
}

unsafe fn mock_clone(data: *const ()) -> RawWaker {
    Arc::<WakerData>::increment_strong_count(data.cast());
    RawWaker::new(data, &MOCK_VTABLE)
}

// The custom VTable (vtable) for our mock Waker.
static MOCK_VTABLE: std::task::RawWakerVTable =
    RawWakerVTable::new(mock_clone, mock_wake, mock_wake_by_ref, mock_drop);

// Mocking where the Waker will increment the atomic everytime it is woken up.
// Calling Arc::into_raw *does not decrement the reference count*, so we need to
// ensure we call `Arc::from_raw` on all these leaked ptrs.
pub(crate) fn mock_waker() -> (Waker, Arc<WakerData>) {
    let data = Arc::new(WakerData::new());
    let raw_data = Arc::into_raw(Arc::clone(&data));

    let raw_waker = RawWaker::new(raw_data as *const (), &MOCK_VTABLE);

    unsafe { (Waker::from_raw(raw_waker), data) }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::init_local_runtime_and_context;

    use super::*;
    use anyhow::Result;
    use std::ptr::NonNull;

    #[test]
    fn test_mock_raw_waker() -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        let (waker1, waker_data) = mock_waker();
        waker1.wake_by_ref();
        assert_eq!(waker_data.get_count(), 1);

        let waker2 = waker1.clone();
        waker2.wake();
        assert_eq!(waker_data.get_count(), 2);

        drop(waker1);
        assert_eq!(waker_data.get_count(), 2);
        Ok(())
    }

    #[test]
    fn test_mock_raw_waker_data_ptr_is_header() -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        let (waker, _) = mock_waker();

        unsafe {
            let h = NonNull::new_unchecked(waker.data() as *mut Header).as_ref();
            assert_eq!(h.get_pending_ios(), 0);
            assert!(h.is_stealable());
        }

        Ok(())
    }
}
