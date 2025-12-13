use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::error::{StoreError, StoreResult};
use crate::types::BlockId;

use super::task::PersistenceTask;

/// Bounded multi-producer queue used by the persistence runtime.
pub struct PersistenceQueue {
    inner: Mutex<VecDeque<Arc<PersistenceTask>>>,
    not_empty: Condvar,
    not_full: Condvar,
    stopped: AtomicBool,
    max_size: usize,
}

impl PersistenceQueue {
    pub fn new(max_size: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            stopped: AtomicBool::new(false),
            max_size: max_size.max(1),
        }
    }

    pub fn push(&self, task: Arc<PersistenceTask>) -> StoreResult<()> {
        let mut queue = self.inner.lock();
        while queue.len() >= self.max_size && !self.stopped.load(Ordering::Acquire) {
            self.not_full.wait(&mut queue);
        }
        if self.stopped.load(Ordering::Acquire) {
            task.mark_cancelled();
            return Err(StoreError::DurabilityFailure {
                block: task.block_height,
                reason: "persistence runtime has stopped".to_string(),
            });
        }
        queue.push_back(task);
        self.not_empty.notify_one();
        Ok(())
    }

    pub fn pop(&self) -> Option<Arc<PersistenceTask>> {
        let mut queue = self.inner.lock();
        loop {
            if let Some(task) = queue.pop_front() {
                self.not_full.notify_one();
                return Some(task);
            }

            if self.stopped.load(Ordering::Acquire) {
                return None;
            }

            self.not_empty.wait(&mut queue);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    pub fn cancel_after(&self, block_height: BlockId) -> Vec<Arc<PersistenceTask>> {
        let mut queue = self.inner.lock();
        let mut cancelled = Vec::new();

        while let Some(last) = queue.back() {
            if last.block_height > block_height {
                let task = queue.pop_back().expect("checked with back");
                cancelled.push(task);
            } else {
                break;
            }
        }

        for task in &cancelled {
            task.mark_cancelled();
        }

        self.not_full.notify_all();
        cancelled
    }

    pub fn drain(&self) -> Vec<Arc<PersistenceTask>> {
        let mut queue = self.inner.lock();
        let drained: Vec<_> = queue.drain(..).collect();
        self.not_full.notify_all();
        drained
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.not_empty.notify_all();
        self.not_full.notify_all();
    }
}
