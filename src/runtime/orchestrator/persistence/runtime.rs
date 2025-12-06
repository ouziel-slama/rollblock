use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::block_journal::BlockJournal;
use crate::error::{MhinStoreError, StoreResult};
use crate::metadata::MetadataStore;
use crate::metrics::StoreMetrics;
use crate::snapshot::Snapshotter;
use crate::state_engine::StateEngine;
use crate::state_shard::{RawTableShard, StateShard};
use crate::types::{BlockId, JournalMeta};

use super::block_undo_from_arc;
use super::pending_blocks::PendingBlocks;
use super::queue::PersistenceQueue;
use super::task::{PersistenceTask, TaskStatus};

enum PersistOutcome {
    Committed { synced: bool },
    Skipped,
}

enum SnapshotCommand {
    Trigger,
    Shutdown,
}

enum SnapshotLockMode {
    Blocking,
    NonBlocking,
}

/// Background worker that persists blocks and manages durability bookkeeping.
pub struct PersistenceRuntime<E, J, S, M>
where
    E: StateEngine + 'static,
    J: BlockJournal + 'static,
    S: Snapshotter + 'static,
    M: MetadataStore + 'static,
{
    queue: Arc<PersistenceQueue>,
    pending_blocks: Arc<PendingBlocks>,
    state_engine: Arc<E>,
    journal: Arc<J>,
    snapshotter: Arc<S>,
    metadata: Arc<M>,
    metrics: StoreMetrics,
    fatal_error: Mutex<Option<(BlockId, String)>>,
    durable_block: Arc<AtomicU64>,
    applied_block: Arc<AtomicU64>,
    rollback_barrier: Arc<AtomicU64>,
    metadata_sync_interval: AtomicUsize,
    pending_metadata: Mutex<Vec<(BlockId, JournalMeta)>>,
    update_mutex: Arc<Mutex<()>>,
    snapshot_interval: Duration,
    max_snapshot_interval: Duration,
    worker: Mutex<Option<JoinHandle<()>>>,
    snapshot_worker: Mutex<Option<JoinHandle<()>>>,
    snapshot_scheduler: Mutex<Option<JoinHandle<()>>>,
    snapshot_tx: Mutex<Option<mpsc::Sender<SnapshotCommand>>>,
    snapshot_inflight: AtomicBool,
    last_snapshot: Mutex<Instant>,
    force_snapshot_mutex: Mutex<()>,
    stop: AtomicBool,
    flush_mutex: Mutex<()>,
    flush_cv: Condvar,
}

impl<E, J, S, M> PersistenceRuntime<E, J, S, M>
where
    E: StateEngine + 'static,
    J: BlockJournal + 'static,
    S: Snapshotter + 'static,
    M: MetadataStore + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        queue: Arc<PersistenceQueue>,
        pending_blocks: Arc<PendingBlocks>,
        state_engine: Arc<E>,
        journal: Arc<J>,
        snapshotter: Arc<S>,
        metadata: Arc<M>,
        metrics: StoreMetrics,
        durable_block: Arc<AtomicU64>,
        applied_block: Arc<AtomicU64>,
        rollback_barrier: Arc<AtomicU64>,
        update_mutex: Arc<Mutex<()>>,
        metadata_sync_interval: usize,
        snapshot_interval: Duration,
        max_snapshot_interval: Duration,
    ) -> Arc<Self> {
        let (snapshot_tx, snapshot_rx) = mpsc::channel();

        let runtime = Arc::new(Self {
            queue: Arc::clone(&queue),
            pending_blocks,
            state_engine,
            journal,
            snapshotter,
            metadata,
            metrics: metrics.clone(),
            fatal_error: Mutex::new(None),
            durable_block,
            applied_block,
            rollback_barrier,
            metadata_sync_interval: AtomicUsize::new(metadata_sync_interval),
            pending_metadata: Mutex::new(Vec::new()),
            update_mutex,
            snapshot_interval,
            max_snapshot_interval,
            worker: Mutex::new(None),
            snapshot_worker: Mutex::new(None),
            snapshot_scheduler: Mutex::new(None),
            snapshot_tx: Mutex::new(Some(snapshot_tx)),
            snapshot_inflight: AtomicBool::new(false),
            last_snapshot: Mutex::new(Instant::now()),
            force_snapshot_mutex: Mutex::new(()),
            stop: AtomicBool::new(false),
            flush_mutex: Mutex::new(()),
            flush_cv: Condvar::new(),
        });

        let worker_runtime = Arc::clone(&runtime);
        let handle = std::thread::Builder::new()
            .name("rollblock-persistence".to_string())
            .spawn(move || worker_runtime.run_worker())
            .expect("failed to spawn persistence worker");

        *runtime.worker.lock() = Some(handle);

        let snapshot_runtime = Arc::clone(&runtime);
        let snapshot_handle = std::thread::Builder::new()
            .name("rollblock-snapshot".to_string())
            .spawn(move || snapshot_runtime.run_snapshot_worker(snapshot_rx))
            .expect("failed to spawn snapshot worker");

        *runtime.snapshot_worker.lock() = Some(snapshot_handle);

        if !snapshot_interval.is_zero() {
            let scheduler_runtime = Arc::clone(&runtime);
            let scheduler_handle = std::thread::Builder::new()
                .name("rollblock-snapshot-scheduler".to_string())
                .spawn(move || scheduler_runtime.run_snapshot_scheduler())
                .expect("failed to spawn snapshot scheduler");
            *runtime.snapshot_scheduler.lock() = Some(scheduler_handle);
        }

        runtime
    }

    pub fn enqueue(&self, task: Arc<PersistenceTask>) -> StoreResult<()> {
        self.ensure_healthy()?;
        if self.stop.load(Ordering::Acquire) {
            if let Some(err) = self.fatal_error() {
                return Err(err);
            } else {
                return Err(MhinStoreError::DurabilityFailure {
                    block: task.block_height,
                    reason: "persistence runtime is shutting down".to_string(),
                });
            }
        }
        self.queue.push(task)
    }

    pub fn cancel_after(&self, block_height: BlockId) -> Vec<Arc<PersistenceTask>> {
        let cancelled = self.queue.cancel_after(block_height);
        if !cancelled.is_empty() {
            for task in &cancelled {
                task.release_undo();
            }
            self.flush_cv.notify_all();
        }
        cancelled
    }

    fn run_worker(self: &Arc<Self>) {
        while !self.stop.load(Ordering::Acquire) {
            let Some(task) = self.queue.pop() else {
                break;
            };

            if task.is_cancelled() {
                task.set_status(TaskStatus::Cancelled);
                self.flush_cv.notify_all();
                continue;
            }

            let rollback_barrier = self.rollback_barrier.load(Ordering::Acquire);
            if task.block_height > rollback_barrier {
                tracing::debug!(
                    block_height = task.block_height,
                    rollback_barrier,
                    "Skipping persistence for block above rollback target"
                );
                task.set_status(TaskStatus::Cancelled);
                self.flush_cv.notify_all();
                continue;
            }

            task.set_status(TaskStatus::Persisting);

            let result = self.persist_block(&task);

            match result {
                Ok(PersistOutcome::Committed { synced }) => {
                    task.release_undo();
                    let _ = self.pending_blocks.pop_front(task.block_height);
                    if synced {
                        self.durable_block
                            .store(task.block_height, Ordering::Release);
                        self.metrics.update_durable_block(task.block_height);
                    }
                    let metrics_ctx = task.metrics;
                    let duration = metrics_ctx.started_at.elapsed();
                    self.metrics.record_apply(
                        task.block_height,
                        metrics_ctx.ops_count,
                        metrics_ctx.set_count,
                        metrics_ctx.zero_delete_count,
                        duration,
                    );
                    task.set_status(TaskStatus::Completed(Ok(())));
                    self.flush_cv.notify_all();
                }
                Ok(PersistOutcome::Skipped) => {
                    task.release_undo();
                    if let Some(undo_arc) = self.pending_blocks.pop_front(task.block_height) {
                        let undo = block_undo_from_arc(undo_arc);
                        if let Err(err) = self.state_engine.revert(task.block_height, undo) {
                            tracing::error!(
                                block_height = task.block_height,
                                ?err,
                                "Failed to revert skipped block after rollback barrier moved"
                            );
                        } else {
                            tracing::debug!(
                                block_height = task.block_height,
                                "Reverted skipped block after rollback barrier moved"
                            );
                        }
                    }
                    task.set_status(TaskStatus::Cancelled);
                    self.flush_cv.notify_all();
                    continue;
                }
                Err(err) => {
                    task.release_undo();
                    self.handle_persist_failure(task, err);
                    break;
                }
            }

            if self.stop.load(Ordering::Acquire) {
                break;
            }

            self.maybe_request_snapshot();
        }

        self.flush_cv.notify_all();
    }

    fn run_snapshot_worker(self: Arc<Self>, rx: mpsc::Receiver<SnapshotCommand>) {
        while let Ok(command) = rx.recv() {
            match command {
                SnapshotCommand::Trigger => {
                    match self.try_create_snapshot() {
                        Ok(true) => {
                            *self.last_snapshot.lock() = Instant::now();
                        }
                        Ok(false) => {}
                        Err(err) => {
                            tracing::warn!(?err, "Failed to create scheduled snapshot");
                            self.metrics.record_failure();
                        }
                    }
                    self.snapshot_inflight.store(false, Ordering::Release);
                }
                SnapshotCommand::Shutdown => break,
            }
        }

        self.snapshot_inflight.store(false, Ordering::Release);
    }

    fn maybe_request_snapshot(&self) {
        if self.snapshot_interval.is_zero() {
            return;
        }

        if self.stop.load(Ordering::Acquire) {
            return;
        }

        if self.snapshot_inflight.load(Ordering::Acquire) {
            return;
        }

        if !self.queue.is_empty() || !self.pending_blocks.is_empty() {
            return;
        }

        let should_trigger = {
            let last = *self.last_snapshot.lock();
            last.elapsed() >= self.snapshot_interval
        };

        if !should_trigger {
            return;
        }

        if self
            .snapshot_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let sender = {
            let guard = self.snapshot_tx.lock();
            guard.as_ref().cloned()
        };
        if let Some(tx) = sender {
            if tx.send(SnapshotCommand::Trigger).is_err() {
                self.snapshot_inflight.store(false, Ordering::Release);
            }
        } else {
            self.snapshot_inflight.store(false, Ordering::Release);
        }
    }

    fn signal_snapshot_shutdown(&self) {
        let sender = {
            let mut guard = self.snapshot_tx.lock();
            guard.take()
        };

        if let Some(tx) = sender {
            let _ = tx.send(SnapshotCommand::Shutdown);
        }
    }

    pub fn set_metadata_sync_interval(&self, sync_every_n_blocks: usize) -> StoreResult<()> {
        self.metadata_sync_interval
            .store(sync_every_n_blocks, Ordering::Release);
        if sync_every_n_blocks == 0 {
            self.journal.force_sync()?;
            self.flush_pending_metadata()?;
            let applied = self.applied_block.load(Ordering::Acquire);
            self.update_durable_after_sync(applied);
        }
        Ok(())
    }

    fn metadata_batching_enabled(&self) -> bool {
        self.metadata_sync_interval.load(Ordering::Acquire) > 0
    }

    fn append_pending_metadata(&self, block: BlockId, meta: JournalMeta) {
        let mut guard = self.pending_metadata.lock();
        guard.push((block, meta));
    }

    fn flush_pending_metadata(&self) -> StoreResult<()> {
        self.flush_pending_metadata_through(BlockId::MAX)
    }

    pub fn flush_pending_metadata_through(&self, block: BlockId) -> StoreResult<()> {
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

    pub fn discard_pending_metadata_after(&self, block: BlockId) {
        let mut guard = self.pending_metadata.lock();
        guard.retain(|(height, _)| *height <= block);
    }

    fn clear_pending_metadata(&self) {
        self.pending_metadata.lock().clear();
    }

    fn record_metadata(
        &self,
        block: BlockId,
        meta: &JournalMeta,
        append_synced: bool,
    ) -> StoreResult<()> {
        if self.metadata_batching_enabled() {
            self.append_pending_metadata(block, meta.clone());
            if append_synced {
                self.flush_pending_metadata()?;
            }
            Ok(())
        } else {
            self.metadata.record_block_commit(block, meta)
        }
    }

    fn persist_block(&self, task: &PersistenceTask) -> StoreResult<PersistOutcome> {
        let undo = task.clone_undo();
        let append_outcome =
            self.journal
                .append(task.block_height, undo.as_ref(), &task.operations)?;

        let rollback_barrier = self.rollback_barrier.load(Ordering::Acquire);
        if task.block_height > rollback_barrier {
            tracing::debug!(
                block_height = task.block_height,
                rollback_barrier,
                "Discarding journal entry written after rollback barrier moved"
            );
            self.journal.truncate_after(rollback_barrier)?;
            return Ok(PersistOutcome::Skipped);
        }

        self.record_metadata(
            task.block_height,
            &append_outcome.meta,
            append_outcome.synced,
        )?;

        let rollback_barrier_after = self.rollback_barrier.load(Ordering::Acquire);
        if task.block_height > rollback_barrier_after {
            tracing::debug!(
                block_height = task.block_height,
                previous_barrier = rollback_barrier,
                rollback_barrier = rollback_barrier_after,
                "Rollback barrier moved during persistence; discarding committed block"
            );
            self.discard_pending_metadata_after(rollback_barrier_after);
            self.metadata
                .remove_journal_offsets_after(rollback_barrier_after)?;
            self.metadata.set_current_block(rollback_barrier_after)?;
            self.journal.truncate_after(rollback_barrier_after)?;
            return Ok(PersistOutcome::Skipped);
        }

        Ok(PersistOutcome::Committed {
            synced: append_outcome.synced,
        })
    }

    fn update_durable_after_sync(&self, target: BlockId) {
        let applied = self.applied_block.load(Ordering::Acquire);
        let capped_target = target.min(applied);
        let current = self.durable_block.load(Ordering::Acquire);
        if capped_target > current {
            self.durable_block.store(capped_target, Ordering::Release);
            self.metrics.update_durable_block(capped_target);
        }
    }

    fn handle_persist_failure(&self, task: Arc<PersistenceTask>, err: MhinStoreError) {
        let block = task.block_height;
        let reason = err.to_string();

        tracing::error!(
            block_height = block,
            %reason,
            "Durability failure while persisting block"
        );

        self.metrics.record_failure();

        {
            let mut fatal = self.fatal_error.lock();
            if fatal.is_none() {
                *fatal = Some((block, reason.clone()));
            }
        }

        // Remove the failed block from pending bookkeeping before reverting.
        let failed_undo = self.pending_blocks.pop_front(block);
        if let Some(undo_arc) = failed_undo {
            let undo = block_undo_from_arc(undo_arc);
            if let Err(revert_err) = self.state_engine.revert(block, undo) {
                tracing::error!(
                    block_height = block,
                    ?revert_err,
                    "Failed to revert state after durability failure"
                );
            }
        }

        task.set_status(TaskStatus::Completed(Err(Arc::new(err))));

        // Stop further processing and wake any waiting threads.
        self.queue.stop();
        self.stop.store(true, Ordering::Release);
        self.flush_cv.notify_all();
        self.signal_snapshot_shutdown();

        // Drain any queued tasks so we can drop their undo references.
        let drained_tasks = self.queue.drain();
        for pending in &drained_tasks {
            pending.release_undo();
        }

        // Revert any still-pending blocks beyond the failed one.
        let remaining_undos = self.pending_blocks.drain();
        self.clear_pending_metadata();
        for undo_arc in remaining_undos.into_iter().rev() {
            let revert_block = undo_arc.block_height;
            let undo = block_undo_from_arc(undo_arc);
            if let Err(revert_err) = self.state_engine.revert(revert_block, undo) {
                tracing::error!(
                    block_height = revert_block,
                    ?revert_err,
                    "Failed to revert pending block after durability failure"
                );
            } else {
                tracing::debug!(
                    block_height = revert_block,
                    "Reverted pending block after durability failure"
                );
            }
        }

        // Make sure no stray journal entries remain beyond the last durable block.
        if let Err(truncate_err) = self
            .journal
            .truncate_after(self.durable_block.load(Ordering::Acquire))
        {
            tracing::error!(
                ?truncate_err,
                "Failed to truncate journal after durability failure"
            );
        }

        // Update applied block to last durable height.
        let durable = self.durable_block.load(Ordering::Acquire);
        self.applied_block.store(durable, Ordering::Release);
        self.metrics.update_applied_block(durable);
        self.metrics
            .update_key_count(self.state_engine.total_keys());

        // Notify drained tasks of the failure.
        for pending in drained_tasks {
            pending.set_status(TaskStatus::Completed(Err(Arc::new(
                MhinStoreError::DurabilityFailure {
                    block,
                    reason: reason.clone(),
                },
            ))));
        }
    }

    fn try_create_snapshot(&self) -> StoreResult<bool> {
        self.create_snapshot_with_mode(SnapshotLockMode::NonBlocking)
    }

    fn create_snapshot_blocking(&self) -> StoreResult<bool> {
        self.create_snapshot_with_mode(SnapshotLockMode::Blocking)
    }

    fn create_snapshot_with_mode(&self, mode: SnapshotLockMode) -> StoreResult<bool> {
        let guard = match mode {
            SnapshotLockMode::Blocking => Some(self.update_mutex.lock()),
            SnapshotLockMode::NonBlocking => self.update_mutex.try_lock(),
        };

        let Some(update_guard) = guard else {
            return Ok(false);
        };

        if !self.pending_blocks.is_empty() {
            drop(update_guard);
            return Ok(false);
        }

        let durable = self.durable_block.load(Ordering::Acquire);
        let applied = self.applied_block.load(Ordering::Acquire);

        if durable != applied {
            drop(update_guard);
            return Ok(false);
        }

        let snapshot_shards = self.clone_state_for_snapshot();
        drop(update_guard);

        self.write_snapshot(durable, snapshot_shards)
    }

    fn clone_state_for_snapshot(&self) -> Vec<Arc<dyn StateShard>> {
        let source_shards = self.state_engine.snapshot_shards();
        source_shards
            .into_iter()
            .enumerate()
            .map(|(index, shard)| {
                let data = shard.export_data();
                let clone = RawTableShard::new(index, data.len());
                clone.import_data(data);
                let shard_arc: Arc<dyn StateShard> = Arc::new(clone);
                shard_arc
            })
            .collect()
    }

    fn write_snapshot(
        &self,
        block: BlockId,
        shards: Vec<Arc<dyn StateShard>>,
    ) -> StoreResult<bool> {
        let path = self.snapshotter.create_snapshot(block, &shards)?;
        self.metadata.store_snapshot_watermark(block)?;
        tracing::info!(block = block, path = ?path, "Snapshot created");
        Ok(true)
    }

    pub fn flush(&self) -> StoreResult<()> {
        let mut guard = self.flush_mutex.lock();
        loop {
            if let Some(err) = self.fatal_error() {
                return Err(err);
            }

            let durable = self.durable_block.load(Ordering::Acquire);
            let applied = self.applied_block.load(Ordering::Acquire);
            if durable >= applied {
                // Force sync to ensure all writes are durable before returning
                self.journal.force_sync()?;
                self.flush_pending_metadata()?;
                self.update_durable_after_sync(applied);
                return Ok(());
            }

            // Rollbacks may legitimately leave `applied_block` ahead of `durable_block`
            // while there are no in-flight persistence tasks. In that case, there is
            // nothing left to wait for, so allow flush to return once both the queue
            // and pending undo stack are drained.
            if self.queue.is_empty() && self.pending_blocks.is_empty() {
                // Force sync to ensure all writes are durable before returning
                self.journal.force_sync()?;
                self.flush_pending_metadata()?;
                self.update_durable_after_sync(applied);
                return Ok(());
            }

            let _ = self
                .flush_cv
                .wait_for(&mut guard, Duration::from_millis(10));
        }
    }

    pub fn force_snapshot_if_overdue(&self) -> StoreResult<()> {
        if self.max_snapshot_interval.is_zero() || !self.is_snapshot_overdue() {
            return Ok(());
        }

        let _guard = self.force_snapshot_mutex.lock();

        if !self.is_snapshot_overdue() {
            return Ok(());
        }

        if self.snapshot_inflight.load(Ordering::Acquire) {
            // A snapshot is already running; let it finish without blocking callers.
            return Ok(());
        }

        tracing::info!(
            elapsed_secs = self.last_snapshot.lock().elapsed().as_secs(),
            max_secs = self.max_snapshot_interval.as_secs(),
            "Forcing snapshot because maximum snapshot interval was exceeded"
        );

        self.flush()?;

        if !self.is_snapshot_overdue() || self.snapshot_inflight.load(Ordering::Acquire) {
            return Ok(());
        }

        let snapshot_created = self.run_snapshot_blocking()?;

        if snapshot_created {
            *self.last_snapshot.lock() = Instant::now();
        } else {
            tracing::warn!("Forced snapshot request skipped because prerequisites were not met");
        }

        Ok(())
    }

    fn is_snapshot_overdue(&self) -> bool {
        if self.max_snapshot_interval.is_zero() {
            return false;
        }
        self.last_snapshot.lock().elapsed() >= self.max_snapshot_interval
    }

    fn run_snapshot_blocking(&self) -> StoreResult<bool> {
        loop {
            match self.snapshot_inflight.compare_exchange(
                false,
                true,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => std::thread::sleep(Duration::from_millis(10)),
            }
        }

        let result = self.create_snapshot_blocking();
        self.snapshot_inflight.store(false, Ordering::Release);
        result
    }

    fn run_snapshot_scheduler(self: Arc<Self>) {
        let poll_interval = self.snapshot_scheduler_poll_interval();
        while !self.stop.load(Ordering::Acquire) {
            self.maybe_request_snapshot();
            std::thread::sleep(poll_interval);
        }
    }

    fn snapshot_scheduler_poll_interval(&self) -> Duration {
        if self.snapshot_interval.is_zero() {
            return Duration::from_millis(100);
        }
        let minimum = Duration::from_millis(10);
        let maximum = Duration::from_secs(1);
        self.snapshot_interval.min(maximum).max(minimum)
    }

    pub fn shutdown(&self) -> StoreResult<()> {
        self.queue.stop();

        let flush_result = self.flush();

        self.stop.store(true, Ordering::Release);
        self.flush_cv.notify_all();

        self.signal_snapshot_shutdown();
        if let Some(handle) = self.worker.lock().take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.snapshot_worker.lock().take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.snapshot_scheduler.lock().take() {
            let _ = handle.join();
        }

        flush_result
    }

    pub fn fatal_error(&self) -> Option<MhinStoreError> {
        self.fatal_error
            .lock()
            .clone()
            .map(|(block, reason)| MhinStoreError::DurabilityFailure { block, reason })
    }

    pub fn ensure_healthy(&self) -> StoreResult<()> {
        if let Some(err) = self.fatal_error() {
            Err(err)
        } else {
            Ok(())
        }
    }
}
