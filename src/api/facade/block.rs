use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use crate::error::{MhinStoreError, StoreResult};
use crate::types::{BlockId, Operation, StoreKey as Key, Value};

use super::core::MhinStoreFacade;
use super::StoreFacade;

struct PendingBlock {
    block_height: BlockId,
    operations: Vec<Operation>,
    intermediate_state: HashMap<Key, Value>,
}

impl PendingBlock {
    fn new(block_height: BlockId) -> Self {
        Self {
            block_height,
            operations: Vec::new(),
            intermediate_state: HashMap::new(),
        }
    }

    fn record_operation(&mut self, operation: Operation) {
        let staged_value = if operation.value.is_delete() {
            Value::empty()
        } else {
            operation.value.clone()
        };
        self.intermediate_state.insert(operation.key, staged_value);
        self.operations.push(operation);
    }

    fn resolved_value(&self, key: &Key) -> Option<Value> {
        self.intermediate_state.get(key).cloned()
    }
}

/// Block-oriented facade that buffers operations before committing them.
///
/// This facade allows callers to stage operations for a block via
/// [`start_block`](Self::start_block), [`set`](Self::set) and
/// [`end_block`](Self::end_block) while exposing intermediate reads that
/// reflect uncommitted changes.
pub struct MhinStoreBlockFacade {
    inner: MhinStoreFacade,
    pending: Mutex<Option<PendingBlock>>,
}

impl MhinStoreBlockFacade {
    /// Creates a new block facade from a configuration.
    pub fn new(config: super::config::StoreConfig) -> StoreResult<Self> {
        MhinStoreFacade::new(config).map(Self::from_facade)
    }

    /// Creates a block facade from an existing [`MhinStoreFacade`].
    pub fn from_facade(inner: MhinStoreFacade) -> Self {
        Self {
            inner,
            pending: Mutex::new(None),
        }
    }

    fn lock_pending(&self) -> StoreResult<MutexGuard<'_, Option<PendingBlock>>> {
        match self.pending.lock() {
            Ok(guard) => Ok(guard),
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                *guard = None;
                drop(guard);
                self.pending.clear_poison();
                Err(MhinStoreError::LockPoisoned { lock: "pending" })
            }
        }
    }

    /// Starts staging operations for the provided `block_height`.
    pub fn start_block(&self, block_height: BlockId) -> StoreResult<()> {
        self.inner.durable_block()?;

        let mut pending = self.lock_pending()?;
        if let Some(current) = pending.as_ref() {
            return Err(MhinStoreError::BlockInProgress {
                current: current.block_height,
            });
        }

        *pending = Some(PendingBlock::new(block_height));
        Ok(())
    }

    /// Adds a new [`Operation`] to the current block.
    pub fn set(&self, operation: Operation) -> StoreResult<()> {
        self.inner.durable_block()?;

        let mut pending = self.lock_pending()?;
        let staged = pending.as_mut().ok_or(MhinStoreError::NoBlockInProgress)?;
        staged.record_operation(operation);
        Ok(())
    }

    /// Removes a key inside the staged block and returns its previous value.
    pub fn pop(&self, key: Key) -> StoreResult<Value> {
        self.inner.durable_block()?;

        let mut pending = self.lock_pending()?;
        let block_height = pending
            .as_ref()
            .ok_or(MhinStoreError::NoBlockInProgress)?
            .block_height;

        if let Some(value) = pending
            .as_ref()
            .and_then(|block| block.resolved_value(&key))
        {
            let staged = pending
                .as_mut()
                .expect("staged block present after resolved_value check");
            staged.record_operation(Operation {
                key,
                value: Value::empty(),
            });
            return Ok(value);
        }

        let previous = self.inner.get(key)?;
        let staged = pending.as_mut().ok_or(MhinStoreError::NoBlockInProgress)?;
        if staged.block_height != block_height {
            return Err(MhinStoreError::BlockInProgress {
                current: staged.block_height,
            });
        }
        staged.record_operation(Operation {
            key,
            value: Value::empty(),
        });
        Ok(previous)
    }

    /// Commits the staged block through the underlying facade.
    pub fn end_block(&self) -> StoreResult<()> {
        let mut guard = self.lock_pending()?;
        let (block_height, operations) = {
            let pending_block = guard.as_ref().ok_or(MhinStoreError::NoBlockInProgress)?;
            (pending_block.block_height, pending_block.operations.clone())
        };

        match self.inner.set(block_height, operations) {
            Ok(()) => {
                *guard = None;
                Ok(())
            }
            Err(err) => {
                tracing::error!(
                    block_height,
                    error = ?err,
                    "Failed to finalize staged block; marking store as fatal"
                );
                if let Some(metrics) = self.inner.metrics() {
                    metrics.record_failure();
                }
                *guard = None;

                let reason = format!("block facade failed to finalize block: {err}");
                self.inner
                    .orchestrator()
                    .record_fatal_error(block_height, reason.clone());

                Err(MhinStoreError::DurabilityFailure {
                    block: block_height,
                    reason,
                })
            }
        }
    }

    /// Retrieves a value, including staged changes when a block is in progress.
    pub fn get(&self, key: Key) -> StoreResult<Value> {
        self.inner.durable_block()?;

        let staged = {
            let guard = self.lock_pending()?;
            guard.as_ref().and_then(|block| block.resolved_value(&key))
        };

        if let Some(staged) = staged {
            return Ok(staged);
        }

        self.inner.get(key)
    }

    /// Rolls back committed state through the underlying facade.
    ///
    /// Fails if a block is currently being staged.
    pub fn rollback(&self, target: BlockId) -> StoreResult<()> {
        self.inner.durable_block()?;

        let pending_block = {
            let guard = self.lock_pending()?;
            guard.as_ref().map(|block| block.block_height)
        };

        if let Some(pending) = pending_block {
            return Err(MhinStoreError::BlockInProgress { current: pending });
        }

        self.inner.rollback(target)
    }

    /// Flushes pending state and closes the underlying store.
    ///
    /// Returns [`MhinStoreError::BlockInProgress`] if a block is currently being staged.
    pub fn close(&self) -> StoreResult<()> {
        self.inner.durable_block()?;

        let pending_block = {
            let guard = self.lock_pending()?;
            guard.as_ref().map(|block| block.block_height)
        };

        if let Some(pending) = pending_block {
            return Err(MhinStoreError::BlockInProgress { current: pending });
        }

        self.inner.close()
    }

    /// Provides access to the underlying facade.
    pub fn inner(&self) -> &MhinStoreFacade {
        &self.inner
    }

    /// Enables relaxed durability by delegating to the underlying facade.
    pub fn enable_relaxed_mode(&self, sync_every_n_blocks: usize) -> StoreResult<()> {
        self.inner.enable_relaxed_mode(sync_every_n_blocks)
    }

    /// Returns whether the underlying facade currently runs in relaxed mode.
    pub fn relaxed_mode_enabled(&self) -> bool {
        self.inner.relaxed_mode_enabled()
    }

    /// Disables relaxed durability and flushes pending writes.
    pub fn disable_relaxed_mode(&self) -> StoreResult<()> {
        self.inner.disable_relaxed_mode()
    }

    /// Checks the health of the underlying store without performing an operation.
    pub fn ensure_healthy(&self) -> StoreResult<()> {
        self.inner.ensure_healthy()
    }

    /// Returns the current committed block height.
    ///
    /// When a block is being staged, this returns the height of the last
    /// committed block (the staged block is not yet committed).
    pub fn current_block(&self) -> StoreResult<BlockId> {
        self.inner.current_block()
    }
}

impl StoreFacade for MhinStoreBlockFacade {
    fn set(&self, block_height: BlockId, operations: Vec<Operation>) -> StoreResult<()> {
        let pending_block = {
            let guard = self.lock_pending()?;
            guard.as_ref().map(|block| block.block_height)
        };

        if let Some(pending) = pending_block {
            return Err(MhinStoreError::BlockInProgress { current: pending });
        }

        self.inner.set(block_height, operations)
    }

    fn rollback(&self, target: BlockId) -> StoreResult<()> {
        MhinStoreBlockFacade::rollback(self, target)
    }

    fn get(&self, key: Key) -> StoreResult<Value> {
        MhinStoreBlockFacade::get(self, key)
    }

    fn multi_get(&self, keys: &[Key]) -> StoreResult<Vec<Value>> {
        self.inner.durable_block()?;

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let staged_hits: Option<Vec<Option<Value>>> = {
            let guard = self.lock_pending()?;
            guard.as_ref().map(|block| {
                keys.iter()
                    .map(|key| block.resolved_value(key))
                    .collect::<Vec<_>>()
            })
        };

        let mut results = vec![Value::empty(); keys.len()];
        let mut missing = Vec::new();

        if let Some(staged) = staged_hits {
            for (idx, staged_value) in staged.into_iter().enumerate() {
                if let Some(value) = staged_value {
                    results[idx] = value;
                } else {
                    missing.push(idx);
                }
            }
        } else {
            missing.extend(0..keys.len());
        }

        if missing.is_empty() {
            return Ok(results);
        }

        let fetch_keys: Vec<Key> = missing.iter().map(|&idx| keys[idx]).collect();
        let fetched = self.inner.multi_get(&fetch_keys)?;
        for (idx, value) in missing.into_iter().zip(fetched.into_iter()) {
            results[idx] = value;
        }

        Ok(results)
    }

    fn enable_relaxed_mode(&self, sync_every_n_blocks: usize) -> StoreResult<()> {
        self.inner.enable_relaxed_mode(sync_every_n_blocks)
    }

    fn relaxed_mode_enabled(&self) -> bool {
        self.inner.relaxed_mode_enabled()
    }

    fn disable_relaxed_mode(&self) -> StoreResult<()> {
        self.inner.disable_relaxed_mode()
    }

    fn close(&self) -> StoreResult<()> {
        MhinStoreBlockFacade::close(self)
    }

    fn current_block(&self) -> StoreResult<BlockId> {
        self.inner.current_block()
    }

    fn applied_block(&self) -> StoreResult<BlockId> {
        self.inner.applied_block()
    }

    fn durable_block(&self) -> StoreResult<BlockId> {
        self.inner.durable_block()
    }

    fn ensure_healthy(&self) -> StoreResult<()> {
        self.inner.ensure_healthy()
    }

    fn pop(&self, block_height: BlockId, key: Key) -> StoreResult<Value> {
        let pending_block = {
            let guard = self.lock_pending()?;
            guard.as_ref().map(|block| block.block_height)
        };

        if let Some(pending) = pending_block {
            return Err(MhinStoreError::BlockInProgress { current: pending });
        }

        self.inner.pop(block_height, key)
    }
}
