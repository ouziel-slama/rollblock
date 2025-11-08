use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::error::{MhinStoreError, StoreResult};
use crate::metadata::MetadataStore;
use crate::metrics::StoreMetrics;
use crate::state_engine::StateEngine;
use crate::types::{BlockId, Key, Operation, Value};

pub struct ReadOnlyBlockOrchestrator<E, M>
where
    E: StateEngine + 'static,
    M: MetadataStore + 'static,
{
    state_engine: Arc<E>,
    metadata: Arc<M>,
    metrics: StoreMetrics,
    applied_block: AtomicU64,
}

impl<E, M> ReadOnlyBlockOrchestrator<E, M>
where
    E: StateEngine + 'static,
    M: MetadataStore + 'static,
{
    pub fn new(
        state_engine: Arc<E>,
        metadata: Arc<M>,
        restored_block: BlockId,
    ) -> StoreResult<Self> {
        let metadata_block = metadata.current_block()?;
        let initial_block = metadata_block.max(restored_block);

        if metadata_block < restored_block {
            tracing::info!(
                metadata_block,
                restored_block,
                "Metadata lagging behind snapshot while opening in read-only mode; \
                 reporting restored block height until metadata catches up"
            );
        }

        let metrics = StoreMetrics::new();
        metrics.update_durable_block(initial_block);
        metrics.update_applied_block(initial_block);
        metrics.update_key_count(state_engine.total_keys());

        Ok(Self {
            state_engine,
            metadata,
            metrics,
            applied_block: AtomicU64::new(initial_block),
        })
    }

    fn reject(&self, operation: &'static str) -> StoreResult<()> {
        Err(MhinStoreError::ReadOnlyOperation { operation })
    }
}

impl<E, M> super::BlockOrchestrator for ReadOnlyBlockOrchestrator<E, M>
where
    E: StateEngine + 'static,
    M: MetadataStore + 'static,
{
    fn apply_operations(&self, _block_height: BlockId, _ops: Vec<Operation>) -> StoreResult<()> {
        self.reject("apply_operations")
    }

    fn revert_to(&self, _block: BlockId) -> StoreResult<()> {
        self.reject("revert_to")
    }

    fn fetch(&self, key: Key) -> StoreResult<Value> {
        let start = Instant::now();
        let result = self.state_engine.lookup(&key);
        let duration = start.elapsed();
        self.metrics.record_lookup(duration);
        Ok(result.unwrap_or(0))
    }

    fn metrics(&self) -> Option<&StoreMetrics> {
        Some(&self.metrics)
    }

    fn current_block(&self) -> StoreResult<BlockId> {
        let recorded = self.metadata.current_block()?;
        let cached = self.applied_block.load(Ordering::Acquire);

        if recorded >= cached {
            self.applied_block.store(recorded, Ordering::Release);
            self.metrics.update_applied_block(recorded);
            self.metrics.update_durable_block(recorded);
            Ok(recorded)
        } else {
            self.metrics.update_applied_block(cached);
            self.metrics.update_durable_block(cached);
            Ok(cached)
        }
    }

    fn applied_block_height(&self) -> BlockId {
        self.applied_block.load(Ordering::Acquire)
    }

    fn durable_block_height(&self) -> StoreResult<BlockId> {
        self.current_block()
    }

    fn shutdown(&self) -> StoreResult<()> {
        Ok(())
    }

    fn ensure_healthy(&self) -> StoreResult<()> {
        Ok(())
    }
}
