use futures::task::noop_waker;
use std::cell::RefCell;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

#[derive(Debug, Clone)]
pub(crate) struct AsyncLocalQueue<T> {
    data: RefCell<Vec<T>>,

    waker: RefCell<Waker>,
}

unsafe impl<T> Sync for AsyncLocalQueue<T> {}
unsafe impl<T> Send for AsyncLocalQueue<T> {}

const DEFAULT_CAPACITY: usize = 32;

impl<T> AsyncLocalQueue<T> {
    pub(crate) fn new() -> Self {
        Self {
            data: RefCell::new(Vec::with_capacity(DEFAULT_CAPACITY)),
            waker: RefCell::new(noop_waker()),
        }
    }

    fn set_waker(&self, waker: &Waker) {
        if !waker.will_wake(&self.waker.borrow()) {
            *self.waker.borrow_mut() = waker.clone();
        }
    }

    pub(crate) fn push(&self, item: T) {
        self.data.borrow_mut().push(item);
        self.waker.borrow().wake_by_ref();
    }

    pub(crate) fn take(&self) -> Vec<T> {
        // Replace with pre-allocated data buffer to save a few mem allocations.
        let size = std::cmp::max(DEFAULT_CAPACITY, self.len().next_power_of_two());
        self.data.replace(Vec::with_capacity(size))
    }

    pub(crate) fn len(&self) -> usize {
        self.data.borrow().len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.borrow().is_empty()
    }

    pub(crate) fn drain(&self) -> Drain<'_, T> {
        Drain { queue: self }
    }
}

#[derive(Debug)]
pub(crate) struct Drain<'a, T> {
    queue: &'a AsyncLocalQueue<T>,
}

impl<'a, T> Future for Drain<'a, T> {
    type Output = Vec<T>;

    /// Polls the queue.
    ///
    /// - If the queue has data, it returns `Poll::Ready(Vec<T>)` draining the
    ///   elements in the queue.
    ///
    /// - If the queue is empty, it returns `Poll::Pending` and stores the
    ///   `Waker` from the context. This `Waker` will be woken up when we next
    ///   push data in the queue.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.queue.is_empty() {
            self.queue.set_waker(cx.waker());
            Poll::Pending
        } else {
            Poll::Ready(self.queue.take())
        }
    }
}

impl<T> Default for AsyncLocalQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as ringolo;
    use crate::test_utils::*;
    use std::future::Future;
    use std::pin::pin;

    #[test]
    fn test_queue_lifecycle() {
        let (waker, waker_data) = mock_waker();
        let mut cx = Context::from_waker(&waker);
        let queue = AsyncLocalQueue::<i32>::new();

        assert!(matches!(pin!(queue.drain()).poll(&mut cx), Poll::Pending));
        assert_eq!(waker_data.get_count(), 0);

        // Adding items wakes up the waker
        queue.push(10);
        queue.push(20);

        let res = pin!(queue.drain()).poll(&mut cx);
        assert_eq!(res, Poll::Ready(vec![10, 20]));
        assert_eq!(waker_data.get_count(), 2);

        // Pop items
        assert!(matches!(pin!(queue.drain()).poll(&mut cx), Poll::Pending));
    }

    #[test]
    fn test_queue_is_fifo() {
        let queue = AsyncLocalQueue::<String>::new();

        queue.push("hello".to_string());
        queue.push("world".to_string());
        assert_eq!(queue.len(), 2);

        assert_eq!(queue.take(), vec!["hello".to_string(), "world".to_string()]);
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());
    }

    #[ringolo::test]
    async fn test_queue_async_interface() {
        let queue = AsyncLocalQueue::<i32>::new();
        queue.push(10);
        queue.push(20);

        assert_eq!(queue.drain().await, vec![10, 20]);
    }
}
