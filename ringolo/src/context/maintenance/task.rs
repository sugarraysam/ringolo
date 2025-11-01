use crate::context::maintenance::cleanup::{CleanupHandler, CleanupOp};
use crate::runtime::RuntimeConfig;
use crate::spawn::{TaskOpts, TaskOptsInternal};
use crate::utils::scope_guard::ScopeGuard;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};

pub(crate) static MAINTENANCE_TASK_OPTS: LazyLock<TaskOpts> = LazyLock::new(|| {
    TaskOpts::STICKY | TaskOpts::BACKGROUND_TASK | TaskOptsInternal::MAINTENANCE_TASK.into()
});

#[derive(Debug)]
pub(crate) struct MaintenanceTask {
    pub(crate) cleanup_handler: CleanupHandler,

    pub(crate) shutdown: Arc<AtomicBool>,

    pub(crate) is_running: Arc<AtomicBool>,
}

// # Safety
// Maintenance task is sticky to the task where it is spawned, and runs for the
// duration of the program.
unsafe impl Sync for MaintenanceTask {}
unsafe impl Send for MaintenanceTask {}

impl MaintenanceTask {
    pub(crate) fn new(cfg: &RuntimeConfig, shutdown: Arc<AtomicBool>) -> Self {
        Self {
            cleanup_handler: CleanupHandler::new(cfg.on_cleanup_error),
            shutdown,
            is_running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn add_cleanup_op(&self, op: CleanupOp) {
        self.cleanup_handler.push(op);
    }

    pub(crate) fn spawn(self: Arc<Self>) {
        // Drop handle so that when we cancel the maintenance task, we also drop the
        // result. The result of the maintenance task *is never consumed* from outside
        // the maintenance task.
        let _ = crate::spawn_builder()
            .with_opts(*MAINTENANCE_TASK_OPTS)
            .spawn(run(self));
    }

    pub(crate) fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Acquire)
    }
}

async fn run(me: Arc<MaintenanceTask>) {
    me.is_running.store(true, Ordering::Release);

    let _guard = {
        let is_running = me.is_running.clone();
        ScopeGuard::new(move || {
            is_running.store(false, Ordering::Release);
        })
    };

    while !me.shutdown.load(Ordering::Acquire) {
        let res = me.cleanup_handler.cleanup().await;
        debug_assert!(res.is_ok(), "Cleanup failed: {:?}", res);
    }

    // TODO: do stuff after shutdown if we want with access to async code...?
}
