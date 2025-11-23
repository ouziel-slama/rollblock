use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
use crate::types::BlockId;

use super::pending_blocks::PendingBlocks;
use super::queue::PersistenceQueue;
use super::task::{PersistenceTask, TaskStatus};

enum PersistOutcome {
    Committed,
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
    update_mutex: Arc<Mutex<()>>,
    snapshot_interval: Duration,
    max_snapshot_interval: Duration,
    worker: Mutex<Option<JoinHandle<()>>>,
    snapshot_worker: Mutex<Option<JoinHandle<()>>>,
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
            update_mutex,
            snapshot_interval,
            max_snapshot_interval,
            worker: Mutex::new(None),
            snapshot_worker: Mutex::new(None),
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
                Ok(PersistOutcome::Committed) => {
                    let _ = self.pending_blocks.pop_front(task.block_height);
                    self.durable_block
                        .store(task.block_height, Ordering::Release);
                    self.metrics.update_durable_block(task.block_height);
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
                    if let Some(undo) = self.pending_blocks.pop_front(task.block_height) {
                        if let Err(err) = self.state_engine.revert(task.block_height, undo.clone())
                        {
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

    fn persist_block(&self, task: &PersistenceTask) -> StoreResult<PersistOutcome> {
        let journal_meta = self
            .journal
            .append(task.block_height, &task.undo, &task.operations)?;

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

        self.metadata
            .record_block_commit(task.block_height, &journal_meta)?;

        let rollback_barrier_after = self.rollback_barrier.load(Ordering::Acquire);
        if task.block_height > rollback_barrier_after {
            tracing::debug!(
                block_height = task.block_height,
                previous_barrier = rollback_barrier,
                rollback_barrier = rollback_barrier_after,
                "Rollback barrier moved during persistence; discarding committed block"
            );
            self.metadata
                .remove_journal_offsets_after(rollback_barrier_after)?;
            self.metadata.set_current_block(rollback_barrier_after)?;
            self.journal.truncate_after(rollback_barrier_after)?;
            return Ok(PersistOutcome::Skipped);
        }

        Ok(PersistOutcome::Committed)
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
        let _ = self.pending_blocks.pop_front(block);

        if let Err(revert_err) = self.state_engine.revert(block, task.undo.clone()) {
            tracing::error!(
                block_height = block,
                ?revert_err,
                "Failed to revert state after durability failure"
            );
        }

        task.set_status(TaskStatus::Completed(Err(Arc::new(err))));

        // Revert any still-pending blocks beyond the failed one.
        let remaining_undos = self.pending_blocks.drain();
        for undo in remaining_undos.into_iter().rev() {
            let revert_block = undo.block_height;
            if let Err(revert_err) = self.state_engine.revert(revert_block, undo.clone()) {
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

        // Stop further processing and wake any waiting threads.
        self.queue.stop();
        self.stop.store(true, Ordering::Release);
        self.flush_cv.notify_all();
        self.signal_snapshot_shutdown();

        // Drain any queued tasks and notify them of the failure.
        let drained_tasks = self.queue.drain();
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

        let Some(_guard) = guard else {
            return Ok(false);
        };

        if !self.pending_blocks.is_empty() {
            return Ok(false);
        }

        let durable = self.durable_block.load(Ordering::Acquire);
        let applied = self.applied_block.load(Ordering::Acquire);

        if durable != applied {
            return Ok(false);
        }

        let shards = self.state_engine.snapshot_shards();
        let path = self.snapshotter.create_snapshot(durable, &shards)?;
        tracing::info!(block = durable, path = ?path, "Snapshot created");
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
                return Ok(());
            }

            // Rollbacks may legitimately leave `applied_block` ahead of `durable_block`
            // while there are no in-flight persistence tasks. In that case, there is
            // nothing left to wait for, so allow flush to return once both the queue
            // and pending undo stack are drained.
            if self.queue.is_empty() && self.pending_blocks.is_empty() {
                return Ok(());
            }

            let _ = self
                .flush_cv
                .wait_for(&mut guard, Duration::from_millis(10));
        }
    }

    pub fn force_snapshot_if_overdue(&self) -> StoreResult<()> {
        if self.max_snapshot_interval.is_zero() {
            return Ok(());
        }

        if !self.is_snapshot_overdue() {
            return Ok(());
        }

        let _guard = self.force_snapshot_mutex.lock();

        if !self.is_snapshot_overdue() {
            return Ok(());
        }

        self.wait_for_active_snapshot();

        if !self.is_snapshot_overdue() {
            return Ok(());
        }

        tracing::warn!(
            elapsed_secs = self.last_snapshot.lock().elapsed().as_secs(),
            max_secs = self.max_snapshot_interval.as_secs(),
            "Forcing snapshot because maximum snapshot interval was exceeded"
        );

        self.flush()?;

        // It's possible that flush triggered a regular snapshot in the background.
        self.wait_for_active_snapshot();

        if !self.is_snapshot_overdue() {
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

    fn wait_for_active_snapshot(&self) {
        while self.snapshot_inflight.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(10));
        }
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

    pub fn shutdown(&self) {
        self.queue.stop();
        self.stop.store(true, Ordering::Release);
        self.flush_cv.notify_all();
        self.signal_snapshot_shutdown();
        if let Some(handle) = self.worker.lock().take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.snapshot_worker.lock().take() {
            let _ = handle.join();
        }
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
