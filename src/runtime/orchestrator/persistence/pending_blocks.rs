use parking_lot::Mutex;
use std::collections::VecDeque;

use crate::types::{BlockId, BlockUndo};

/// Thread-safe pending block undo stack used by asynchronous persistence.
#[derive(Debug, Default)]
pub struct PendingBlocks {
    inner: Mutex<VecDeque<BlockUndo>>,
}

impl PendingBlocks {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
        }
    }

    pub fn push(&self, undo: BlockUndo) {
        self.inner.lock().push_back(undo);
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    pub fn pop_latest(&self, block_height: BlockId) -> Option<BlockUndo> {
        let mut guard = self.inner.lock();
        if let Some(last) = guard.back() {
            if last.block_height == block_height {
                return guard.pop_back();
            }
        }
        None
    }

    pub fn pop_until(&self, target: BlockId) -> Vec<BlockUndo> {
        let mut guard = self.inner.lock();
        let mut removed = Vec::new();

        while let Some(last) = guard.back() {
            if last.block_height > target {
                let undo = guard.pop_back().expect("checked via back()");
                removed.push(undo);
            } else {
                break;
            }
        }

        removed
    }

    pub fn pop_front(&self, block_height: BlockId) -> Option<BlockUndo> {
        let mut guard = self.inner.lock();
        if let Some(front) = guard.front() {
            if front.block_height == block_height {
                return guard.pop_front();
            }
        }
        None
    }

    pub fn drain(&self) -> Vec<BlockUndo> {
        let mut guard = self.inner.lock();
        guard.drain(..).collect()
    }
}
