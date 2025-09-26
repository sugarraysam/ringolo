use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use std::task::{RawWaker, RawWakerVTable, Waker};

// Counter to keep track of how many times we woke the Waker.
type WakerData = Arc<AtomicU32>;

unsafe fn mock_wake(data: *const ()) {
    // Need to consume 1 Arc reference
    let data = unsafe { Arc::from_raw(data as *const AtomicU32) };
    data.fetch_add(1, Ordering::Relaxed);
}

unsafe fn mock_wake_by_ref(data: *const ()) {
    // Not consuming any Arc ref
    let data = unsafe { &*(data as *const AtomicU32) };
    data.fetch_add(1, Ordering::Relaxed);
}

// Drop the Waker Arc reference.
unsafe fn mock_drop(data: *const ()) {
    if !data.is_null() {
        let _data = unsafe { Arc::from_raw(data as *const AtomicU32) };
    }
}

unsafe fn mock_clone(data: *const ()) -> RawWaker {
    let data = unsafe { Arc::from_raw(data as *const AtomicU32) };
    let raw_data = Arc::into_raw(Arc::clone(&data));

    // Need to leak data again avoid decrementing Arc ref count
    _ = Arc::into_raw(data);

    RawWaker::new(raw_data as *const (), &MOCK_VTABLE)
}

// The custom VTable (vtable) for our mock Waker.
static MOCK_VTABLE: std::task::RawWakerVTable =
    RawWakerVTable::new(mock_clone, mock_wake, mock_wake_by_ref, mock_drop);

// Mocking where the Waker will increment the atomic everytime it is woken up.
// Calling Arc::into_raw *does not decrement the reference count*, so we need to
// ensure we call `Arc::from_raw` on all these leaked ptrs.
pub fn mock_waker() -> (Waker, WakerData) {
    let data = Arc::new(AtomicU32::new(0));
    let raw_data = Arc::into_raw(Arc::clone(&data));

    let raw_waker = RawWaker::new(raw_data as *const (), &MOCK_VTABLE);

    unsafe { (Waker::from_raw(raw_waker), data) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_raw_waker() {
        let (waker1, waker_data) = mock_waker();
        waker1.wake_by_ref();
        assert_eq!(waker_data.load(Ordering::Relaxed), 1);

        let waker2 = waker1.clone();
        waker2.wake();
        assert_eq!(waker_data.load(Ordering::Relaxed), 2);

        drop(waker1);
        assert_eq!(waker_data.load(Ordering::Relaxed), 2);
    }
}
