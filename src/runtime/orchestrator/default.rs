use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::block_journal::{BlockJournal, SyncPolicy};
use crate::error::{MhinStoreError, StoreResult};
use crate::metadata::MetadataStore;
use crate::snapshot::Snapshotter;
use crate::state_engine::StateEngine;
use crate::storage::journal::{JournalPruneReport, JournalPruner};
use crate::types::{BlockId, BlockUndo, JournalMeta, Key, Operation, Value};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockWriteGuard};

use super::durability::PersistenceSettings;
use super::persistence::{
    block_undo_from_arc, ApplyMetricsContext, PersistenceContext, PersistenceTask,
};

type Shared<T> = Arc<T>;

#[derive(Clone)]
struct PruneNowHandle<J, M> {
    pruner: JournalPruner<J, M>,
    gate: Shared<Mutex<()>>,
}

impl<J, M> PruneNowHandle<J, M>
where
    J: BlockJournal + 'static,
    M: MetadataStore + 'static,
{
    fn new(pruner: JournalPruner<J, M>, gate: Shared<Mutex<()>>) -> Self {
        Self { pruner, gate }
    }

    fn prune_now(&self) -> StoreResult<Option<JournalPruneReport>> {
        let _guard = self.gate.lock();
        self.pruner.prune_now()
    }
}

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
    reader_gate: Shared<RwLock<()>>,
    persistence: PersistenceContext<E, J, S, M>,
    pruner: Option<JournalPruner<J, M>>,
    prune_now_handle: Option<PruneNowHandle<J, M>>,
    prune_gate: Shared<Mutex<()>>,
    fatal_error: Mutex<Option<(BlockId, String)>>,
    sync_metadata_interval: AtomicUsize,
    pending_metadata: Mutex<Vec<(BlockId, JournalMeta)>>,
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
        let reader_gate = Shared::new(RwLock::new(()));
        let prune_gate = Shared::new(Mutex::new(()));

        Self::apply_initial_sync_policy(&journal, &persistence_settings);

        let persistence = PersistenceContext::new(
            Arc::clone(&state_engine),
            Arc::clone(&journal),
            Arc::clone(&snapshotter),
            Arc::clone(&metadata),
            Arc::clone(&update_mutex),
            &persistence_settings,
        )?;

        let pruner = if persistence_settings.min_rollback_window == BlockId::MAX {
            None
        } else {
            let durable_tracker = persistence.durable_block_tracker();
            let journal_for_pruner = Arc::clone(&journal);
            let metadata_for_pruner = Arc::clone(&metadata);
            let pruner = JournalPruner::spawn(
                journal_for_pruner,
                metadata_for_pruner,
                persistence_settings.min_rollback_window,
                persistence_settings.prune_interval,
                move || durable_tracker.load(Ordering::Acquire),
                None,
            );
            pruner.resume_pending_work()?;
            Some(pruner)
        };
        let prune_now_handle = pruner
            .as_ref()
            .map(|pruner| PruneNowHandle::new(pruner.clone(), Arc::clone(&prune_gate)));

        let initial_metadata_interval = if persistence.is_async() {
            0
        } else {
            persistence_settings.durability_mode.sync_every_n_blocks()
        };

        Ok(Self {
            state_engine,
            journal,
            snapshotter,
            metadata,
            update_mutex,
            reader_gate,
            persistence,
            pruner,
            prune_now_handle,
            prune_gate,
            fatal_error: Mutex::new(None),
            sync_metadata_interval: AtomicUsize::new(initial_metadata_interval),
            pending_metadata: Mutex::new(Vec::new()),
        })
    }

    fn apply_initial_sync_policy(journal: &Arc<J>, settings: &PersistenceSettings) {
        let sync_every_n = settings.durability_mode.sync_every_n_blocks();
        if sync_every_n == 0 {
            journal.set_sync_policy(SyncPolicy::EveryBlock);
        } else {
            journal.set_sync_policy(SyncPolicy::every_n_blocks(sync_every_n));
        }
    }

    pub fn prune_now(&self) -> StoreResult<Option<JournalPruneReport>> {
        if let Some(handle) = &self.prune_now_handle {
            handle.prune_now()
        } else {
            Ok(None)
        }
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

        let allow_genesis_zero = block_height == 0
            && current == 0
            && self.metadata.last_journal_offset_at_or_before(0)?.is_none();

        if block_height <= current && !allow_genesis_zero {
            self.persistence.metrics().record_failure();
            return Err(MhinStoreError::BlockIdNotIncreasing {
                block_height,
                current,
            });
        }

        Ok(())
    }

    fn record_sync_metadata(
        &self,
        block_height: BlockId,
        meta: &JournalMeta,
        append_synced: bool,
    ) -> StoreResult<()> {
        if self.sync_metadata_interval.load(Ordering::Acquire) == 0 {
            return self.metadata.record_block_commit(block_height, meta);
        }

        {
            let mut pending = self.pending_metadata.lock();
            pending.push((block_height, meta.clone()));
        }

        if append_synced {
            self.flush_sync_pending_metadata()
        } else {
            Ok(())
        }
    }

    fn flush_sync_pending_metadata(&self) -> StoreResult<()> {
        self.flush_sync_pending_metadata_through(BlockId::MAX)
    }

    fn flush_sync_pending_metadata_through(&self, block: BlockId) -> StoreResult<()> {
        let entries = {
            let mut guard = self.pending_metadata.lock();
            if guard.is_empty() {
                return Ok(());
            }
            let split = guard.partition_point(|(height, _)| *height <= block);
            if split == 0 {
                return Ok(());
            }
            guard.drain(..split).collect::<Vec<_>>()
        };
        self.metadata.record_block_commits(&entries)
    }

    fn discard_sync_pending_metadata_after(&self, block: BlockId) {
        let mut guard = self.pending_metadata.lock();
        guard.retain(|(height, _)| *height <= block);
    }

    fn revert_journal_range(&self, range_end: BlockId, range_start: BlockId) -> StoreResult<()> {
        if range_start > range_end {
            return Ok(());
        }

        let mut iter = self.journal.iter_backwards(range_end, range_start)?;
        while let Some(result) = iter.next_entry() {
            let entry = result?;
            self.state_engine
                .revert(entry.block_height, entry.undo.clone())?;
        }

        Ok(())
    }

    fn apply_block(
        &self,
        block_height: BlockId,
        ops: Vec<Operation>,
        capture_key: Option<Key>,
    ) -> StoreResult<Option<Value>> {
        self.persistence.enforce_snapshot_freshness()?;
        let start = Instant::now();
        let guard = self.update_mutex.lock();

        self.check_health()?;

        tracing::debug!("Acquiring mutation mutex");
        self.validate_block_height(block_height)?;
        let reader_guard = self.reader_gate.write();

        let metrics_context = ApplyMetricsContext::from_ops(start, &ops);
        if ops.is_empty() {
            self.apply_empty_block(block_height, metrics_context, guard, reader_guard)?;
            return Ok(None);
        }

        self.apply_non_empty_block(
            block_height,
            ops,
            metrics_context,
            guard,
            reader_guard,
            capture_key,
        )
    }

    fn apply_empty_block(
        &self,
        block_height: BlockId,
        metrics_context: ApplyMetricsContext,
        guard: MutexGuard<'_, ()>,
        _reader_guard: RwLockWriteGuard<'_, ()>,
    ) -> StoreResult<()> {
        let mut mutation_guard = Some(guard);
        if self.persistence.is_async() {
            self.persistence.set_rollback_barrier(block_height);

            let block_undo = Arc::new(BlockUndo {
                block_height,
                shard_undos: Vec::new(),
            });
            self.persistence
                .pending_blocks()
                .push(Arc::clone(&block_undo));

            let task = PersistenceTask::new(
                block_height,
                Vec::new(),
                Arc::clone(&block_undo),
                metrics_context,
            );

            mutation_guard.take();
            let enqueue_result = self.persistence.enqueue(task);
            let _update_lock = mutation_guard.insert(self.update_mutex.lock());

            if let Err(err) = enqueue_result {
                self.persistence.pending_blocks().pop_latest(block_height);
                self.persistence.metrics().record_failure();
                drop(block_undo);
                return Err(err);
            }

            drop(block_undo);
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
                Ok(outcome) => {
                    if let Err(err) =
                        self.record_sync_metadata(block_height, &outcome.meta, outcome.synced)
                    {
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
                    if outcome.synced {
                        self.persistence.set_durable_block(block_height);
                    }
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
        _reader_guard: RwLockWriteGuard<'_, ()>,
        capture_key: Option<Key>,
    ) -> StoreResult<Option<Value>> {
        let mut mutation_guard = Some(guard);
        tracing::debug!("Preparing journal");
        let delta = self.state_engine.prepare_journal(block_height, &ops)?;
        tracing::debug!("Journal prepared");

        let (stats, block_undo) = self.state_engine.commit(block_height, delta)?;
        let captured_value =
            capture_key.and_then(|target| Self::capture_previous_value(&block_undo, target));
        let block_undo = Arc::new(block_undo);
        tracing::debug!(
            operations = stats.operation_count,
            modified_keys = stats.modified_keys,
            "State committed"
        );

        let persist_result = if self.persistence.is_async() {
            self.persistence.set_rollback_barrier(block_height);
            self.persistence
                .pending_blocks()
                .push(Arc::clone(&block_undo));
            let task =
                PersistenceTask::new(block_height, ops, Arc::clone(&block_undo), metrics_context);

            mutation_guard.take();
            let enqueue_result = self.persistence.enqueue(task);
            let _update_lock = mutation_guard.insert(self.update_mutex.lock());

            enqueue_result
        } else {
            self.persist_synchronously(block_height, block_undo.as_ref(), &ops)
        };

        if let Err(err) = persist_result {
            return self
                .handle_persistence_error(
                    block_height,
                    Arc::clone(&block_undo),
                    err,
                    stats.operation_count,
                )
                .map(|_| None);
        }
        drop(block_undo);

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
        Ok(captured_value)
    }

    fn capture_previous_value(block_undo: &BlockUndo, target: Key) -> Option<Value> {
        block_undo
            .shard_undos
            .iter()
            .flat_map(|shard| shard.entries.iter())
            .find(|entry| entry.key == target)
            .map(|entry| entry.previous.clone().unwrap_or_else(Value::empty))
    }

    fn persist_synchronously(
        &self,
        block_height: BlockId,
        block_undo: &BlockUndo,
        ops: &[Operation],
    ) -> StoreResult<()> {
        match self.journal.append(block_height, block_undo, ops) {
            Ok(outcome) => {
                if let Err(err) =
                    self.record_sync_metadata(block_height, &outcome.meta, outcome.synced)
                {
                    Err(err)
                } else {
                    if outcome.synced {
                        self.persistence.set_durable_block(block_height);
                    }
                    Ok(())
                }
            }
            Err(err) => Err(err),
        }
    }

    fn handle_persistence_error(
        &self,
        block_height: BlockId,
        block_undo: Arc<BlockUndo>,
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

        let undo = block_undo_from_arc(block_undo);
        let revert_result = self.state_engine.revert(block_height, undo);
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
        let current_applied = self.persistence.applied_block_height();
        if block > current_applied {
            return Err(MhinStoreError::RollbackTargetAhead {
                target: block,
                current: current_applied,
            });
        }

        let is_async = self.persistence.is_async();
        if is_async {
            self.persistence.set_rollback_barrier(block);
        }

        let mut current_durable = self.persistence.durable_block_height();

        if block == current_applied {
            if !is_async || block <= current_durable {
                self.metadata.set_current_block(block)?;
            }
            return Ok(());
        }

        if is_async {
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
            for undo_arc in pending_reverts {
                let revert_block = undo_arc.block_height;
                tracing::debug!(
                    block_height = revert_block,
                    "Reverting pending block above rollback target"
                );
                let undo = block_undo_from_arc(undo_arc);
                self.state_engine.revert(revert_block, undo)?;
            }
        }

        let highest_committed_before_flush = self
            .metadata
            .last_journal_offset_at_or_before(BlockId::MAX)?
            .map(|meta| meta.block_height)
            .unwrap_or(0);

        let revert_floor = block.saturating_add(1);
        let non_durable_start = highest_committed_before_flush
            .saturating_add(1)
            .max(revert_floor);
        if non_durable_start <= current_applied {
            self.revert_journal_range(current_applied, non_durable_start)?;
        }

        let committed_ceiling = highest_committed_before_flush.min(current_applied);
        if revert_floor <= committed_ceiling {
            self.revert_journal_range(committed_ceiling, revert_floor)?;
        }

        self.journal.truncate_after(block)?;
        self.metadata.remove_journal_offsets_after(block)?;
        self.discard_sync_pending_metadata_after(block);
        self.persistence.discard_pending_metadata_after(block);
        self.journal.force_sync()?;
        self.flush_sync_pending_metadata_through(block)?;
        self.persistence.flush_pending_metadata_through(block)?;

        self.metadata.set_current_block(block)?;
        self.snapshotter.prune_snapshots_after(block)?;
        current_durable = self.persistence.durable_block_height();
        let new_durable = current_durable.min(block);
        self.persistence.set_durable_block(new_durable);
        self.persistence.set_applied_block(block);

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
        self.discard_sync_pending_metadata_after(durable);
        self.persistence.set_applied_block(durable);
    }

    fn lookup_batch(&self, keys: &[Key]) -> StoreResult<Vec<Value>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        self.check_health()?;

        let start = Instant::now();
        let _reader_guard = self.reader_gate.read();
        let lookup_results = self.state_engine.lookup_many(keys);
        let duration = start.elapsed();

        self.persistence
            .metrics()
            .record_lookup_batch(keys.len(), duration);

        tracing::trace!(
            key_count = keys.len(),
            duration_us = duration.as_micros(),
            "Lookup batch executed"
        );

        Ok(lookup_results
            .into_iter()
            .map(|value| value.map(Value::from).unwrap_or_else(Value::empty))
            .collect())
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
        self.apply_block(block_height, ops, None).map(|_| ())
    }

    #[tracing::instrument(skip(self), fields(target_block = block))]
    fn revert_to(&self, block: BlockId) -> StoreResult<()> {
        let start = Instant::now();
        let _guard = self.update_mutex.lock();

        self.check_health()?;
        let _reader_guard = self.reader_gate.write();

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
        let mut values = self.lookup_batch(std::slice::from_ref(&key))?;
        Ok(values.pop().unwrap_or_else(Value::empty))
    }

    #[tracing::instrument(skip(self, keys), fields(key_count = keys.len()))]
    fn fetch_many(&self, keys: &[Key]) -> StoreResult<Vec<Value>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        self.lookup_batch(keys)
    }

    fn pop(&self, block_height: BlockId, key: Key) -> StoreResult<Value> {
        let op = Operation {
            key,
            value: Value::empty(),
        };
        let captured = self.apply_block(block_height, vec![op], Some(key))?;
        Ok(captured.unwrap_or_else(Value::empty))
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
        let _prune_guard = self.prune_gate.lock();
        if let Some(pruner) = &self.pruner {
            pruner.shutdown();
        }
        let _reader_guard = self.reader_gate.write();
        self.persistence.flush()?;
        if !self.persistence.is_async() {
            self.flush_sync_pending_metadata()?;
        }

        let durable_block = self.persistence.durable_block_height();
        self.persistence.set_applied_block(durable_block);
        tracing::info!(
            block_height = durable_block,
            "Creating snapshot for graceful shutdown"
        );

        let shards = self.state_engine.snapshot_shards();
        let snapshot_path = self.snapshotter.create_snapshot(durable_block, &shards)?;
        self.metadata.store_snapshot_watermark(durable_block)?;

        tracing::info!(
            block_height = durable_block,
            path = ?snapshot_path,
            "Snapshot created successfully"
        );

        self.persistence.shutdown()?;

        Ok(())
    }

    fn ensure_healthy(&self) -> StoreResult<()> {
        self.check_health()
    }

    fn record_fatal_error(&self, block: BlockId, reason: String) {
        self.set_fatal_error(block, reason);
    }

    fn set_sync_policy(&self, policy: crate::block_journal::SyncPolicy) {
        self.journal.set_sync_policy(policy);
    }

    fn set_metadata_sync_interval(&self, sync_every_n_blocks: usize) -> StoreResult<()> {
        if !self.persistence.is_async() {
            self.sync_metadata_interval
                .store(sync_every_n_blocks, Ordering::Release);
            if sync_every_n_blocks == 0 {
                self.journal.force_sync()?;
                self.flush_sync_pending_metadata()?;
            }
        }
        self.persistence
            .set_metadata_sync_interval(sync_every_n_blocks)
    }

    fn flush(&self) -> StoreResult<()> {
        self.check_health()?;
        let _update_guard = self.update_mutex.lock();
        self.persistence.flush()?;
        if !self.persistence.is_async() {
            self.flush_sync_pending_metadata()?;
        }
        Ok(())
    }
}

impl<E, J, S, M> Drop for DefaultBlockOrchestrator<E, J, S, M>
where
    E: StateEngine + 'static,
    J: BlockJournal + 'static,
    S: Snapshotter + 'static,
    M: MetadataStore + 'static,
{
    fn drop(&mut self) {
        if let Some(pruner) = &self.pruner {
            let _guard = self.prune_gate.lock();
            pruner.shutdown();
        }
    }
}
