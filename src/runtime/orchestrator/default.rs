use std::sync::Arc;
use std::time::Instant;

use crate::block_journal::BlockJournal;
use crate::error::{MhinStoreError, StoreResult};
use crate::metadata::MetadataStore;
use crate::snapshot::Snapshotter;
use crate::state_engine::StateEngine;
use crate::types::{BlockId, BlockUndo, Operation, Value};
use parking_lot::{Mutex, MutexGuard};

use super::durability::PersistenceSettings;
use super::persistence::{ApplyMetricsContext, PersistenceContext, PersistenceTask};

type Shared<T> = Arc<T>;

pub struct DefaultBlockOrchestrator<E, J, S, M>
where
    E: StateEngine + 'static,
    J: BlockJournal + 'static,
    S: Snapshotter + 'static,
    M: MetadataStore + 'static,
{
    state_engine: Shared<E>,
    journal: Shared<J>,
    snapshotter: Shared<S>,
    metadata: Shared<M>,
    update_mutex: Shared<Mutex<()>>,
    persistence: PersistenceContext<E, J, S, M>,
    fatal_error: Mutex<Option<(BlockId, String)>>,
}

impl<E, J, S, M> DefaultBlockOrchestrator<E, J, S, M>
where
    E: StateEngine + 'static,
    J: BlockJournal + 'static,
    S: Snapshotter + 'static,
    M: MetadataStore + 'static,
{
    pub fn new(
        state_engine: Shared<E>,
        journal: Shared<J>,
        snapshotter: Shared<S>,
        metadata: Shared<M>,
        persistence_settings: PersistenceSettings,
    ) -> StoreResult<Self> {
        let update_mutex = Shared::new(Mutex::new(()));

        let persistence = PersistenceContext::new(
            Arc::clone(&state_engine),
            Arc::clone(&journal),
            Arc::clone(&snapshotter),
            Arc::clone(&metadata),
            Arc::clone(&update_mutex),
            &persistence_settings,
        )?;

        Ok(Self {
            state_engine,
            journal,
            snapshotter,
            metadata,
            update_mutex,
            persistence,
            fatal_error: Mutex::new(None),
        })
    }

    fn check_health(&self) -> StoreResult<()> {
        if let Some((block, reason)) = self.fatal_error.lock().clone() {
            return Err(MhinStoreError::DurabilityFailure { block, reason });
        }
        self.persistence.ensure_healthy()
    }

    fn validate_block_height(&self, block_height: BlockId) -> StoreResult<()> {
        let current_durable = self.metadata.current_block()?;
        let current_applied = self.persistence.applied_block_height();
        let current = current_applied.max(current_durable);

        if block_height <= current {
            self.persistence.metrics().record_failure();
            return Err(MhinStoreError::BlockIdNotIncreasing {
                block_height,
                current,
            });
        }

        Ok(())
    }

    fn apply_empty_block(
        &self,
        block_height: BlockId,
        metrics_context: ApplyMetricsContext,
        guard: MutexGuard<'_, ()>,
    ) -> StoreResult<()> {
        let mut mutation_guard = Some(guard);
        if self.persistence.is_async() {
            self.persistence.set_rollback_barrier(block_height);

            let block_undo = BlockUndo {
                block_height,
                shard_undos: Vec::new(),
            };
            self.persistence.pending_blocks().push(block_undo.clone());

            let task = PersistenceTask::new(block_height, Vec::new(), block_undo, metrics_context);

            mutation_guard.take();
            let enqueue_result = self.persistence.enqueue(task);
            let _update_lock = mutation_guard.insert(self.update_mutex.lock());

            if let Err(err) = enqueue_result {
                self.persistence.pending_blocks().pop_latest(block_height);
                self.persistence.metrics().record_failure();
                return Err(err);
            }

            self.persistence.set_applied_block(block_height);
            tracing::info!(
                block_height,
                "Empty block queued for asynchronous persistence"
            );
        } else {
            let block_undo = BlockUndo {
                block_height,
                shard_undos: Vec::new(),
            };
            tracing::debug!(
                block_height,
                "Empty block, appending synchronous journal entry"
            );
            match self.journal.append(block_height, &block_undo, &[]) {
                Ok(meta) => {
                    if let Err(err) = self.metadata.record_block_commit(block_height, &meta) {
                        self.persistence.metrics().record_failure();
                        self.set_fatal_error(block_height, err.to_string());
                        self.journal
                            .truncate_after(block_height.saturating_sub(1))
                            .ok();
                        self.metadata
                            .remove_journal_offsets_after(block_height.saturating_sub(1))
                            .ok();
                        return Err(err);
                    }
                    self.persistence.set_durable_block(block_height);
                }
                Err(err) => {
                    self.persistence.metrics().record_failure();
                    self.set_fatal_error(block_height, err.to_string());
                    return Err(err);
                }
            }

            self.persistence.set_applied_block(block_height);
            let duration = metrics_context.started_at.elapsed();
            self.persistence.metrics().record_apply(
                block_height,
                metrics_context.ops_count,
                metrics_context.set_count,
                metrics_context.zero_delete_count,
                duration,
            );
            tracing::info!(
                block_height,
                duration_ms = duration.as_millis(),
                "Empty block applied"
            );
        }

        self.persistence
            .update_key_count(self.state_engine.total_keys());
        Ok(())
    }

    fn apply_non_empty_block(
        &self,
        block_height: BlockId,
        ops: Vec<Operation>,
        metrics_context: ApplyMetricsContext,
        guard: MutexGuard<'_, ()>,
    ) -> StoreResult<()> {
        let mut mutation_guard = Some(guard);
        tracing::debug!("Preparing journal");
        let delta = self.state_engine.prepare_journal(block_height, &ops)?;
        tracing::debug!("Journal prepared");

        let (stats, block_undo) = self.state_engine.commit(block_height, delta)?;
        tracing::debug!(
            operations = stats.operation_count,
            modified_keys = stats.modified_keys,
            "State committed"
        );

        let persist_result = if self.persistence.is_async() {
            self.persistence.set_rollback_barrier(block_height);
            let queued_undo = block_undo.clone();
            self.persistence.pending_blocks().push(queued_undo.clone());
            let task = PersistenceTask::new(block_height, ops, queued_undo, metrics_context);

            mutation_guard.take();
            let enqueue_result = self.persistence.enqueue(task);
            let _update_lock = mutation_guard.insert(self.update_mutex.lock());

            enqueue_result
        } else {
            self.persist_synchronously(block_height, &block_undo, &ops)
        };

        if let Err(err) = persist_result {
            return self.handle_persistence_error(
                block_height,
                block_undo,
                err,
                stats.operation_count,
            );
        }

        self.persistence.set_applied_block(block_height);

        if !self.persistence.is_async() {
            let duration = metrics_context.started_at.elapsed();
            self.persistence.metrics().record_apply(
                block_height,
                metrics_context.ops_count,
                metrics_context.set_count,
                metrics_context.zero_delete_count,
                duration,
            );
        }

        tracing::info!(
            block_height,
            operations = stats.operation_count,
            modified_keys = stats.modified_keys,
            "Block applied successfully"
        );

        self.persistence
            .update_key_count(self.state_engine.total_keys());
        Ok(())
    }

    fn persist_synchronously(
        &self,
        block_height: BlockId,
        block_undo: &BlockUndo,
        ops: &[Operation],
    ) -> StoreResult<()> {
        match self.journal.append(block_height, block_undo, ops) {
            Ok(meta) => {
                if let Err(err) = self.metadata.record_block_commit(block_height, &meta) {
                    Err(err)
                } else {
                    self.persistence.set_durable_block(block_height);
                    Ok(())
                }
            }
            Err(err) => Err(err),
        }
    }

    fn handle_persistence_error(
        &self,
        block_height: BlockId,
        block_undo: BlockUndo,
        err: MhinStoreError,
        operation_count: usize,
    ) -> StoreResult<()> {
        self.persistence.metrics().record_failure();
        let is_async = self.persistence.is_async();
        if is_async {
            self.persistence.pending_blocks().pop_latest(block_height);
        } else {
            self.set_fatal_error(block_height, err.to_string());
        }

        let revert_result = self.state_engine.revert(block_height, block_undo);
        if let Err(revert_err) = revert_result {
            tracing::error!(
                block_height,
                ?revert_err,
                "Failed to revert state after persistence failure"
            );
            if !is_async {
                self.cleanup_after_sync_failure();
            }
            return Err(revert_err);
        }

        if !is_async {
            self.cleanup_after_sync_failure();
        }

        tracing::error!(
            block_height,
            operations = operation_count,
            "Failed to persist block, reverted state"
        );
        self.persistence
            .update_key_count(self.state_engine.total_keys());
        Err(err)
    }

    fn revert_to_internal(&self, block: BlockId) -> StoreResult<()> {
        let mut current_applied = self.persistence.applied_block_height();
        if block > current_applied {
            return Err(MhinStoreError::RollbackTargetAhead {
                target: block,
                current: current_applied,
            });
        }

        if self.persistence.is_async() {
            let durable = self.persistence.durable_block_height();
            if durable < current_applied {
                self.persistence.flush()?;
                current_applied = self.persistence.applied_block_height();
            }
        }

        if self.persistence.is_async() {
            self.persistence.set_rollback_barrier(block);
        }

        if block == current_applied {
            return Ok(());
        }

        if self.persistence.is_async() {
            let cancelled = self.persistence.cancel_after(block);
            if !cancelled.is_empty() {
                tracing::debug!(
                    removed = cancelled.len(),
                    "Cancelling pending persistence tasks above rollback target"
                );
            }
        }

        let pending_reverts = self.persistence.pending_blocks().pop_until(block);
        if !pending_reverts.is_empty() {
            for undo in pending_reverts {
                tracing::debug!(
                    block_height = undo.block_height,
                    "Reverting pending block above rollback target"
                );
                self.state_engine.revert(undo.block_height, undo)?;
            }
        }

        let current_durable = self.metadata.current_block()?;
        if block > current_durable {
            self.persistence
                .update_key_count(self.state_engine.total_keys());
            self.persistence.set_applied_block(block);
            return Ok(());
        }

        let actual_target = self
            .metadata
            .last_journal_offset_at_or_before(block)?
            .map(|meta| meta.block_height)
            .unwrap_or(0);

        let range_start = actual_target.saturating_add(1);
        let revert_offsets = if range_start <= current_durable {
            self.metadata
                .get_journal_offsets(range_start..=current_durable)?
        } else {
            Vec::new()
        };

        if !revert_offsets.is_empty() {
            let mut iter = self.journal.iter_backwards(current_durable, range_start)?;

            while let Some(result) = iter.next_entry() {
                let entry = result?;
                self.state_engine
                    .revert(entry.block_height, entry.undo.clone())?;
            }
        }

        self.journal.truncate_after(actual_target)?;
        self.metadata.remove_journal_offsets_after(actual_target)?;

        self.metadata.set_current_block(actual_target)?;
        self.snapshotter.prune_snapshots_after(actual_target)?;
        self.persistence.set_durable_block(actual_target);
        self.persistence.set_applied_block(actual_target);

        self.persistence
            .update_key_count(self.state_engine.total_keys());
        Ok(())
    }

    fn set_fatal_error(&self, block: BlockId, reason: String) {
        let mut fatal = self.fatal_error.lock();
        if fatal.is_none() {
            *fatal = Some((block, reason));
        }
    }

    fn cleanup_after_sync_failure(&self) {
        let durable = self.persistence.durable_block_height();
        if let Err(truncate_err) = self.journal.truncate_after(durable) {
            tracing::error!(
                latest_durable_block = durable,
                ?truncate_err,
                "Failed to truncate journal after synchronous durability failure"
            );
        }
        if let Err(meta_err) = self.metadata.remove_journal_offsets_after(durable) {
            tracing::error!(
                latest_durable_block = durable,
                ?meta_err,
                "Failed to remove metadata offsets after synchronous durability failure"
            );
        }
        if let Err(meta_err) = self.metadata.set_current_block(durable) {
            tracing::error!(
                latest_durable_block = durable,
                ?meta_err,
                "Failed to reset metadata current block after synchronous durability failure"
            );
        }
        self.persistence.set_applied_block(durable);
    }
}

impl<E, J, S, M> super::BlockOrchestrator for DefaultBlockOrchestrator<E, J, S, M>
where
    E: StateEngine + 'static,
    J: BlockJournal + 'static,
    S: Snapshotter + 'static,
    M: MetadataStore + 'static,
{
    #[tracing::instrument(skip(self, ops), fields(block_height, ops_count = ops.len()))]
    fn apply_operations(&self, block_height: BlockId, ops: Vec<Operation>) -> StoreResult<()> {
        let start = Instant::now();
        let guard = self.update_mutex.lock();

        self.check_health()?;

        tracing::debug!("Acquiring mutation mutex");
        self.validate_block_height(block_height)?;

        let metrics_context = ApplyMetricsContext::from_ops(start, &ops);
        if ops.is_empty() {
            return self.apply_empty_block(block_height, metrics_context, guard);
        }

        self.apply_non_empty_block(block_height, ops, metrics_context, guard)
    }

    #[tracing::instrument(skip(self), fields(target_block = block))]
    fn revert_to(&self, block: BlockId) -> StoreResult<()> {
        let start = Instant::now();
        let _guard = self.update_mutex.lock();

        self.check_health()?;

        tracing::debug!("Starting rollback");
        match self.revert_to_internal(block) {
            Ok(()) => {
                let duration = start.elapsed();
                self.persistence.metrics().record_rollback(block, duration);
                tracing::info!(
                    target_block = block,
                    duration_ms = duration.as_millis(),
                    "Rollback completed successfully"
                );
                Ok(())
            }
            Err(err) => {
                tracing::error!(?err, target_block = block, "Rollback failed");
                self.persistence.metrics().record_failure();
                Err(err)
            }
        }
    }

    #[tracing::instrument(skip(self), fields(key = ?key))]
    fn fetch(&self, key: crate::types::Key) -> StoreResult<Value> {
        self.check_health()?;

        let start = Instant::now();
        let result = self.state_engine.lookup(&key);
        let duration = start.elapsed();
        self.persistence.metrics().record_lookup(duration);

        tracing::trace!(
            key = ?key,
            found = result.is_some(),
            duration_us = duration.as_micros(),
            "Lookup performed"
        );

        Ok(result.unwrap_or(0))
    }

    fn metrics(&self) -> Option<&crate::metrics::StoreMetrics> {
        Some(self.persistence.metrics())
    }

    fn current_block(&self) -> StoreResult<BlockId> {
        self.check_health()?;
        self.metadata.current_block()
    }

    fn applied_block_height(&self) -> BlockId {
        self.persistence.applied_block_height()
    }

    fn durable_block_height(&self) -> StoreResult<BlockId> {
        self.check_health()?;
        Ok(self.persistence.durable_block_height())
    }

    fn shutdown(&self) -> StoreResult<()> {
        let _guard = self.update_mutex.lock();
        self.check_health()?;
        self.persistence.flush()?;

        let durable_block = self.persistence.durable_block_height();
        self.persistence.set_applied_block(durable_block);
        tracing::info!(
            block_height = durable_block,
            "Creating snapshot for graceful shutdown"
        );

        let shards = self.state_engine.snapshot_shards();
        let snapshot_path = self.snapshotter.create_snapshot(durable_block, &shards)?;

        tracing::info!(
            block_height = durable_block,
            path = ?snapshot_path,
            "Snapshot created successfully"
        );

        self.persistence.shutdown();

        Ok(())
    }

    fn ensure_healthy(&self) -> StoreResult<()> {
        self.check_health()
    }
}
