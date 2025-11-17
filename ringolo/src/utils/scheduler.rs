#![allow(unused)]

use crate::runtime::{AddMode, PanicReason, TaskOpts, YieldReason};
use crate::spawn::TaskMetadata;
use crate::task::Id;
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Method {
    Schedule,
    YieldNow,
    Release,
    Spawn,
    UnhandledPanic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Call {
    Schedule {
        id: Id,
        is_stealable: bool,
        opts: TaskOpts,
        mode: Option<AddMode>,
    },
    YieldNow {
        reason: YieldReason,
        mode: Option<AddMode>,
    },
    Release {
        id: Id,
    },
    Spawn {
        opts: TaskOpts,
        metadata: Option<TaskMetadata>,
    },
    UnhandledPanic {
        reason: PanicReason,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct Tracker {
    calls: Arc<DashMap<Method, Vec<Call>>>,
}

impl Tracker {
    pub(crate) fn new() -> Self {
        let map = DashMap::new();
        map.insert(Method::Schedule, Vec::new());
        map.insert(Method::YieldNow, Vec::new());
        map.insert(Method::Release, Vec::new());
        map.insert(Method::Spawn, Vec::new());
        map.insert(Method::UnhandledPanic, Vec::new());

        Self {
            calls: Arc::new(map),
        }
    }

    pub(crate) fn record(&self, method: Method, call: Call) {
        // Do not record maintenance task otherwise it is super annoying to keep
        // track of in tests expectations.
        if let Call::Spawn { opts, .. } | Call::Schedule { opts, .. } = &call
            && opts.is_maintenance_task()
        {
            return;
        }

        self.calls
            .get_mut(&method)
            .expect("method not found")
            .push(call)
    }

    pub(crate) fn get_calls(&self, method: &Method) -> Vec<Call> {
        self.calls
            .get(method)
            .expect("method not found")
            .value()
            .clone()
    }

    pub(crate) fn num_calls(&self, method: &Method) -> usize {
        self.calls.get(method).map_or(0, |calls| calls.len())
    }
}
