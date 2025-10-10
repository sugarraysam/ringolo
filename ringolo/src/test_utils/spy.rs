use crate::runtime::{Schedule, YieldReason};
use crate::task::{Notified, Task};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};
use std::task::Waker;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Method {
    Schedule,
    YieldNow,
    Release,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Call {
    Schedule { is_new: bool },
    YieldNow { reason: YieldReason },
    Release,
}

#[derive(Debug, Clone)]
pub(crate) struct SpyState {
    calls: Arc<RwLock<HashMap<Method, Vec<Call>>>>,
}

impl SpyState {
    pub(crate) fn new() -> Self {
        Self {
            calls: Arc::new(RwLock::new(HashMap::from([
                (Method::Schedule, Vec::new()),
                (Method::YieldNow, Vec::new()),
                (Method::Release, Vec::new()),
            ]))),
        }
    }

    pub(crate) fn record(&self, method: Method, call: Call) {
        self.calls
            .write()
            .unwrap()
            .get_mut(&method)
            .expect("method not found")
            .push(call)
    }

    pub(crate) fn get_calls(&self, method: &Method) -> Vec<Call> {
        self.calls.read().unwrap().get(&method).cloned().unwrap()
    }

    pub(crate) fn num_calls(&self, method: &Method) -> usize {
        self.calls
            .read()
            .unwrap()
            .get(&method)
            .map_or(0, |calls| calls.len())
    }
}

pub(crate) struct SpyScheduler<T> {
    pub(crate) inner: T,
    pub(crate) state: SpyState,
}

impl<T> SpyScheduler<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self {
            inner,
            state: SpyState::new(),
        }
    }
}

impl<T> Deref for SpyScheduler<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for SpyScheduler<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T: Schedule> Schedule for SpyScheduler<T> {
    fn schedule(&self, is_new: bool, task: Notified<Self>) {
        self.state
            .record(Method::Schedule, Call::Schedule { is_new });

        // Safety: Since we wrap the scheduler, safe to transmute task ptr.
        let t = unsafe { std::mem::transmute(task) };
        self.inner.schedule(is_new, t);
    }

    fn yield_now(&self, waker: &Waker, reason: YieldReason) {
        self.state.record(
            Method::YieldNow,
            Call::YieldNow {
                reason: reason.clone(),
            },
        );
        self.inner.yield_now(waker, reason);
    }

    fn release(&self, task: &Task<Self>) -> Option<Task<Self>> {
        self.state.record(Method::Release, Call::Release);

        let t = unsafe { std::mem::transmute(task) };
        let res = self.inner.release(t);
        unsafe { std::mem::transmute(res) }
    }
}
