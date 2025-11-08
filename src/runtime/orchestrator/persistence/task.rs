use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Condvar, Mutex};

use crate::error::MhinStoreError;
use crate::types::{BlockId, BlockUndo, Operation};

/// Context captured when applying a block so that metrics can be recorded later.
#[derive(Debug, Clone, Copy)]
pub struct ApplyMetricsContext {
    pub started_at: Instant,
    pub ops_count: usize,
    pub set_count: usize,
    pub zero_delete_count: usize,
}

impl ApplyMetricsContext {
    pub fn from_ops(started_at: Instant, ops: &[Operation]) -> Self {
        let mut set_count = 0usize;
        let mut zero_delete_count = 0usize;

        for op in ops {
            if op.value == 0 {
                zero_delete_count += 1;
            } else {
                set_count += 1;
            }
        }

        Self {
            started_at,
            ops_count: ops.len(),
            set_count,
            zero_delete_count,
        }
    }
}

#[derive(Debug)]
pub enum TaskStatus {
    Pending,
    Persisting,
    Cancelled,
    Completed(Result<(), Arc<MhinStoreError>>),
}

/// Work item queued for the persistence runtime.
pub struct PersistenceTask {
    pub block_height: BlockId,
    pub operations: Vec<Operation>,
    pub undo: BlockUndo,
    pub metrics: ApplyMetricsContext,
    cancelled: AtomicBool,
    status: Mutex<TaskStatus>,
    status_cv: Condvar,
}

impl PersistenceTask {
    pub fn new(
        block_height: BlockId,
        operations: Vec<Operation>,
        undo: BlockUndo,
        metrics: ApplyMetricsContext,
    ) -> Arc<Self> {
        Arc::new(Self {
            block_height,
            operations,
            undo,
            metrics,
            cancelled: AtomicBool::new(false),
            status: Mutex::new(TaskStatus::Pending),
            status_cv: Condvar::new(),
        })
    }

    pub fn mark_cancelled(&self) {
        self.cancelled.store(true, Ordering::Release);
        let mut status = self.status.lock();
        *status = TaskStatus::Cancelled;
        self.status_cv.notify_all();
    }

    pub fn wait_completion(&self) -> Result<(), Arc<MhinStoreError>> {
        let mut status = self.status.lock();
        loop {
            match &*status {
                TaskStatus::Completed(result) => {
                    return result.clone();
                }
                TaskStatus::Cancelled => {
                    return Ok(());
                }
                TaskStatus::Pending | TaskStatus::Persisting => {
                    self.status_cv.wait(&mut status);
                }
            }
        }
    }

    pub fn set_status(&self, new_status: TaskStatus) {
        let mut status = self.status.lock();
        *status = new_status;
        self.status_cv.notify_all();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}
