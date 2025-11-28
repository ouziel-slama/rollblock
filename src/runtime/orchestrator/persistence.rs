//! Persistence subsystem for the block orchestrator.
//!
//! This module owns the background runtime, task queue, and bookkeeping that
//! keep asynchronous durability consistent with the in-memory state.

mod pending_blocks;
mod queue;
pub mod runtime;
mod task;

pub use pending_blocks::{block_undo_from_arc, PendingBlocks};
pub use task::{ApplyMetricsContext, PersistenceTask};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::block_journal::BlockJournal;
use crate::error::{MhinStoreError, StoreResult};
use crate::metadata::MetadataStore;
use crate::metrics::StoreMetrics;
use crate::snapshot::Snapshotter;
use crate::state_engine::StateEngine;
use crate::types::BlockId;

use self::queue::PersistenceQueue;
use self::runtime::PersistenceRuntime;

use super::durability::PersistenceSettings;

/// Aggregates persistence-related state shared between the orchestrator and the runtime.
pub struct PersistenceContext<E, J, S, M>
where
    E: StateEngine + 'static,
    J: BlockJournal + 'static,
    S: Snapshotter + 'static,
    M: MetadataStore + 'static,
{
    journal: Arc<J>,
    metrics: StoreMetrics,
    pending_blocks: Arc<PendingBlocks>,
    durable_block: Arc<AtomicU64>,
    applied_block: Arc<AtomicU64>,
    rollback_barrier: Arc<AtomicU64>,
    runtime: Option<Arc<PersistenceRuntime<E, J, S, M>>>,
}

impl<E, J, S, M> PersistenceContext<E, J, S, M>
where
    E: StateEngine + 'static,
    J: BlockJournal + 'static,
    S: Snapshotter + 'static,
    M: MetadataStore + 'static,
{
    pub fn new(
        state_engine: Arc<E>,
        journal: Arc<J>,
        snapshotter: Arc<S>,
        metadata: Arc<M>,
        update_mutex: Arc<Mutex<()>>,
        settings: &PersistenceSettings,
    ) -> StoreResult<Self> {
        let mut current_block = metadata.current_block()?;
        let durable_block_height = metadata
            .last_journal_offset_at_or_before(BlockId::MAX)?
            .map(|meta| meta.block_height)
            .unwrap_or(current_block);

        if current_block < durable_block_height {
            current_block = durable_block_height;
            metadata.set_current_block(current_block)?;
        }

        let metrics = StoreMetrics::new();
        metrics.update_key_count(state_engine.total_keys());
        metrics.update_durable_block(durable_block_height);
        metrics.update_applied_block(current_block);

        let pending_blocks = Arc::new(PendingBlocks::new());
        let durable_block = Arc::new(AtomicU64::new(durable_block_height));
        let applied_block = Arc::new(AtomicU64::new(current_block));
        let rollback_barrier = Arc::new(AtomicU64::new(current_block));

        let runtime = if settings.durability_mode.is_async() {
            let queue = Arc::new(PersistenceQueue::new(
                settings.durability_mode.max_pending_blocks(),
            ));
            Some(PersistenceRuntime::spawn(
                queue,
                Arc::clone(&pending_blocks),
                Arc::clone(&state_engine),
                Arc::clone(&journal),
                Arc::clone(&snapshotter),
                Arc::clone(&metadata),
                metrics.clone(),
                Arc::clone(&durable_block),
                Arc::clone(&applied_block),
                Arc::clone(&rollback_barrier),
                Arc::clone(&update_mutex),
                settings.durability_mode.sync_every_n_blocks(),
                settings.snapshot_interval,
                settings.max_snapshot_interval,
            ))
        } else {
            None
        };

        Ok(Self {
            journal,
            metrics,
            pending_blocks,
            durable_block,
            applied_block,
            rollback_barrier,
            runtime,
        })
    }

    pub fn metrics(&self) -> &StoreMetrics {
        &self.metrics
    }

    pub fn pending_blocks(&self) -> &PendingBlocks {
        self.pending_blocks.as_ref()
    }

    pub fn durable_block_height(&self) -> BlockId {
        self.durable_block.load(Ordering::Acquire)
    }

    pub fn durable_block_tracker(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.durable_block)
    }

    pub fn applied_block_height(&self) -> BlockId {
        self.applied_block.load(Ordering::Acquire)
    }

    pub fn set_applied_block(&self, block_height: BlockId) {
        self.applied_block.store(block_height, Ordering::Release);
        self.metrics.update_applied_block(block_height);
    }

    pub fn set_durable_block(&self, block_height: BlockId) {
        self.durable_block.store(block_height, Ordering::Release);
        self.metrics.update_durable_block(block_height);
    }

    pub fn rollback_barrier(&self) -> &Arc<AtomicU64> {
        &self.rollback_barrier
    }

    pub fn set_rollback_barrier(&self, block_height: BlockId) {
        self.rollback_barrier.store(block_height, Ordering::Release);
    }

    pub fn update_key_count(&self, total_keys: usize) {
        self.metrics.update_key_count(total_keys);
    }

    pub fn is_async(&self) -> bool {
        self.runtime.is_some()
    }

    pub fn enqueue(&self, task: Arc<PersistenceTask>) -> StoreResult<()> {
        match &self.runtime {
            Some(runtime) => runtime.enqueue(task),
            None => Err(MhinStoreError::DurabilityFailure {
                block: task.block_height,
                reason: "persistence runtime is not initialized".to_string(),
            }),
        }
    }

    pub fn set_metadata_sync_interval(&self, sync_every_n_blocks: usize) -> StoreResult<()> {
        match &self.runtime {
            Some(runtime) => runtime.set_metadata_sync_interval(sync_every_n_blocks),
            None => Ok(()),
        }
    }

    pub fn cancel_after(&self, block_height: BlockId) -> Vec<Arc<PersistenceTask>> {
        match &self.runtime {
            Some(runtime) => runtime.cancel_after(block_height),
            None => Vec::new(),
        }
    }

    pub fn discard_pending_metadata_after(&self, block_height: BlockId) {
        if let Some(runtime) = &self.runtime {
            runtime.discard_pending_metadata_after(block_height);
        }
    }

    pub fn flush_pending_metadata_through(&self, block_height: BlockId) -> StoreResult<()> {
        match &self.runtime {
            Some(runtime) => runtime.flush_pending_metadata_through(block_height),
            None => Ok(()),
        }
    }

    pub fn flush(&self) -> StoreResult<()> {
        match &self.runtime {
            Some(runtime) => runtime.flush(),
            None => {
                self.journal.force_sync()?;
                let applied = self.applied_block.load(Ordering::Acquire);
                if applied > self.durable_block.load(Ordering::Acquire) {
                    self.set_durable_block(applied);
                }
                Ok(())
            }
        }
    }

    pub fn enforce_snapshot_freshness(&self) -> StoreResult<()> {
        match &self.runtime {
            Some(runtime) => runtime.force_snapshot_if_overdue(),
            None => Ok(()),
        }
    }

    pub fn shutdown(&self) -> StoreResult<()> {
        match &self.runtime {
            Some(runtime) => runtime.shutdown(),
            None => Ok(()),
        }
    }

    pub fn fatal_error(&self) -> Option<MhinStoreError> {
        self.runtime
            .as_ref()
            .and_then(|runtime| runtime.fatal_error())
    }

    pub fn ensure_healthy(&self) -> StoreResult<()> {
        if let Some(runtime) = &self.runtime {
            runtime.ensure_healthy()
        } else {
            Ok(())
        }
    }
}
