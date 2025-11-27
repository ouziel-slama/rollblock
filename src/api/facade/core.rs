use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::block_journal::{FileBlockJournal, JournalOptions, SyncPolicy};
use crate::error::{MhinStoreError, StoreResult};
use crate::metadata::LmdbMetadataStore;
use crate::net::{RemoteServerHandle, RemoteStoreServer, ServerError, ServerMetricsSnapshot};
use crate::orchestrator::{BlockOrchestrator, DefaultBlockOrchestrator, DurabilityMode};
use crate::snapshot::MmapSnapshotter;
use crate::state_engine::ShardedStateEngine;
use crate::state_shard::{RawTableShard, StateShard};
use crate::store_lock::StoreLockGuard;
use crate::types::{BlockId, Key, Operation, Value};
use parking_lot::RwLock;
use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::config::{RemoteServerSettings, StoreConfig};
use super::recovery::{
    reconcile_metadata_with_journal, replay_committed_blocks, resolve_shard_layout,
    restore_existing_state,
};
use super::StoreFacade;

/// Main implementation of the store facade.
///
/// This is the primary entry point for all store operations. It manages
/// the lifecycle of internal components and provides a thread-safe interface.
pub struct MhinStoreFacade {
    orchestrator: Arc<dyn BlockOrchestrator>,
    metadata: Option<Arc<LmdbMetadataStore>>,
    durability_mode: Arc<RwLock<DurabilityMode>>,
    lock: Option<Arc<StoreLockGuard>>,
    shutdown_state: Arc<AtomicBool>,
    handle_count: Arc<AtomicUsize>,
    remote_server: Option<RemoteServerController>,
}

impl Clone for MhinStoreFacade {
    fn clone(&self) -> Self {
        self.handle_count.fetch_add(1, Ordering::AcqRel);
        Self {
            orchestrator: Arc::clone(&self.orchestrator),
            metadata: self.metadata.clone(),
            durability_mode: Arc::clone(&self.durability_mode),
            lock: self.lock.clone(),
            shutdown_state: Arc::clone(&self.shutdown_state),
            handle_count: Arc::clone(&self.handle_count),
            remote_server: self.remote_server.clone(),
        }
    }
}

impl MhinStoreFacade {
    /// Creates a new store instance from a configuration.
    ///
    /// This initializes all internal components including:
    /// - Metadata store (LMDB)
    /// - Block journal (file-based with compression)
    /// - Snapshotter (memory-mapped snapshots)
    /// - State engine with shards
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Directory creation fails
    /// - LMDB initialization fails
    /// - Thread pool creation fails (when thread_count > 1)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use rollblock::{MhinStoreFacade, StoreConfig};
    ///
    /// let config = StoreConfig::new("./data", 4, 1000, 1, false);
    /// let store = MhinStoreFacade::new(config)?;
    /// ```
    pub fn new(config: StoreConfig) -> StoreResult<Self> {
        let lock = StoreLockGuard::acquire(&config.data_dir)?;
        let lock = Arc::new(lock);

        Self::build(config, lock)
    }

    fn build(config: StoreConfig, lock: Arc<StoreLockGuard>) -> StoreResult<Self> {
        std::fs::create_dir_all(&config.data_dir)?;

        let metadata = Arc::new(LmdbMetadataStore::new_with_map_size(
            config.metadata_dir(),
            config.lmdb_map_size,
        )?);
        match metadata.load_durability_mode()? {
            Some(stored) if stored != config.durability_mode => {
                // Config always wins over stored value - user explicitly requested a different mode
                tracing::info!(
                    stored_mode = ?stored,
                    configured_mode = ?config.durability_mode,
                    "Configured durability mode overrides stored value; persisting new setting"
                );
                metadata.store_durability_mode(&config.durability_mode)?;
            }
            Some(_) => {}
            None => {
                metadata.store_durability_mode(&config.durability_mode)?;
            }
        }
        match metadata.load_journal_chunk_size()? {
            Some(stored) if stored != config.journal_chunk_size_bytes => {
                tracing::info!(
                    stored_chunk_size = stored,
                    configured_chunk_size = config.journal_chunk_size_bytes,
                    "Configured journal chunk size overrides stored value; persisting new setting"
                );
                metadata.store_journal_chunk_size(config.journal_chunk_size_bytes)?;
            }
            Some(_) => {}
            None => {
                metadata.store_journal_chunk_size(config.journal_chunk_size_bytes)?;
            }
        }
        let sync_policy = match &config.durability_mode {
            crate::orchestrator::DurabilityMode::AsyncRelaxed {
                sync_every_n_blocks,
                ..
            }
            | crate::orchestrator::DurabilityMode::SynchronousRelaxed {
                sync_every_n_blocks,
            } => SyncPolicy::every_n_blocks(*sync_every_n_blocks),
            _ => SyncPolicy::default(),
        };
        let journal_options = JournalOptions {
            compress: config.compress_journal,
            compression_level: config.journal_compression_level,
            sync_policy,
            max_chunk_size_bytes: config.journal_chunk_size_bytes,
        };
        let journal = Arc::new(FileBlockJournal::with_options(
            config.journal_dir(),
            journal_options,
        )?);
        let snapshotter = Arc::new(MmapSnapshotter::new(config.snapshots_dir())?);

        reconcile_metadata_with_journal(journal.as_ref(), metadata.as_ref())?;

        let shard_layout = resolve_shard_layout(metadata.as_ref(), &config, true)?;

        let shards: Vec<Arc<dyn StateShard>> = (0..shard_layout.shards_count)
            .map(|i| {
                Arc::new(RawTableShard::new(i, shard_layout.initial_capacity))
                    as Arc<dyn StateShard>
            })
            .collect();

        let restored_block =
            restore_existing_state(snapshotter.as_ref(), metadata.as_ref(), &shards)?;

        let thread_pool = if config.thread_count > 1 {
            Some(Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(config.thread_count)
                    .build()?,
            ))
        } else {
            None
        };

        let engine = match &thread_pool {
            Some(pool) => Arc::new(ShardedStateEngine::with_thread_pool(
                shards.clone(),
                metadata.clone(),
                Some(Arc::clone(pool)),
            )),
            None => Arc::new(ShardedStateEngine::new(shards.clone(), metadata.clone())),
        };

        replay_committed_blocks(
            journal.as_ref(),
            metadata.as_ref(),
            engine.as_ref(),
            restored_block,
        )?;

        let persistence_settings = crate::orchestrator::PersistenceSettings {
            durability_mode: config.durability_mode.clone(),
            snapshot_interval: config.snapshot_interval,
            max_snapshot_interval: config.max_snapshot_interval,
        };

        let orchestrator: Arc<dyn BlockOrchestrator> = Arc::new(DefaultBlockOrchestrator::new(
            Arc::clone(&engine),
            journal,
            snapshotter,
            Arc::clone(&metadata),
            persistence_settings,
        )?);

        let durability_mode = Arc::new(RwLock::new(config.durability_mode.clone()));

        let mut store = Self {
            orchestrator,
            metadata: Some(metadata),
            durability_mode,
            lock: Some(lock),
            shutdown_state: Arc::new(AtomicBool::new(false)),
            handle_count: Arc::new(AtomicUsize::new(1)),
            remote_server: None,
        };

        if config.enable_server {
            if let Some(settings) = config.remote_server.clone() {
                store.remote_server = Some(RemoteServerController::spawn(&store, settings)?);
            }
        }

        Ok(store)
    }

    /// Creates a new store from an existing orchestrator.
    ///
    /// This is useful for testing or custom orchestrator implementations.
    ///
    /// # Arguments
    ///
    /// * `orchestrator` - A custom BlockOrchestrator implementation
    pub fn from_orchestrator(orchestrator: Arc<dyn BlockOrchestrator>) -> Self {
        Self {
            orchestrator,
            metadata: None,
            durability_mode: Arc::new(RwLock::new(DurabilityMode::default())),
            lock: None,
            shutdown_state: Arc::new(AtomicBool::new(false)),
            handle_count: Arc::new(AtomicUsize::new(1)),
            remote_server: None,
        }
    }

    /// Returns a reference to the underlying orchestrator.
    ///
    /// This provides access to the orchestrator for advanced use cases.
    pub fn orchestrator(&self) -> &Arc<dyn BlockOrchestrator> {
        &self.orchestrator
    }

    /// Returns the store metrics if available.
    ///
    /// Provides runtime statistics about operations, performance, and state.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// if let Some(metrics) = store.metrics() {
    ///     let snapshot = metrics.snapshot();
    ///     println!("Operations applied: {}", snapshot.operations_applied);
    ///     println!("Current block: {}", snapshot.current_block_height);
    /// }
    /// ```
    pub fn metrics(&self) -> Option<&crate::metrics::StoreMetrics> {
        self.orchestrator.metrics()
    }

    /// Returns the health status of the store.
    ///
    /// The health status includes information about errors, activity, and overall state.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use rollblock::metrics::HealthState;
    ///
    /// if let Some(metrics) = store.metrics() {
    ///     let health = metrics.health();
    ///     match health.state {
    ///         HealthState::Healthy => println!("Store is healthy"),
    ///         HealthState::Degraded => println!("Store has issues"),
    ///         _ => println!("Check store status"),
    ///     }
    /// }
    /// ```
    pub fn health(&self) -> Option<crate::metrics::HealthStatus> {
        self.orchestrator.metrics().map(|m| m.health())
    }

    /// Returns remote server metrics if the embedded server is enabled.
    pub fn remote_server_metrics(&self) -> Option<ServerMetricsSnapshot> {
        self.remote_server
            .as_ref()
            .map(|controller| controller.snapshot())
    }

    /// Returns the current committed block height.
    pub fn current_block(&self) -> StoreResult<BlockId> {
        self.orchestrator.current_block()
    }

    /// Returns the highest block applied in memory.
    pub fn applied_block(&self) -> StoreResult<BlockId> {
        self.orchestrator.ensure_healthy()?;
        Ok(self.orchestrator.applied_block_height())
    }

    /// Returns the highest block durably persisted.
    pub fn durable_block(&self) -> StoreResult<BlockId> {
        self.orchestrator.durable_block_height()
    }

    /// Returns true if relaxed durability semantics are currently active.
    pub fn relaxed_mode_enabled(&self) -> bool {
        self.durability_mode.read().is_relaxed()
    }

    /// Disables relaxed durability so every block is synced immediately again.
    ///
    /// Use this when approaching the chain tip where data loss would be more critical.
    /// This keeps the original async/sync persistence mode, but forces the journal to
    /// fsync after each append (no batching).
    ///
    /// This method also flushes any pending writes that were buffered under relaxed mode,
    /// ensuring all previously written blocks are durable before returning.
    ///
    /// # Example
    /// ```ignore
    /// // During initial sync, use relaxed mode for speed
    /// // When approaching tip:
    /// if chain_tip - current_block < 10 {
    ///     store.disable_relaxed_mode()?;
    /// }
    /// ```
    pub fn disable_relaxed_mode(&self) -> StoreResult<()> {
        self.orchestrator.set_sync_policy(SyncPolicy::EveryBlock);
        // Flush pending journal/index writes before claiming metadata is durable
        self.orchestrator.flush()?;
        self.orchestrator.set_metadata_sync_interval(0)?;

        let updated_mode = {
            let mut guard = self.durability_mode.write();
            match &*guard {
                DurabilityMode::SynchronousRelaxed { .. } => {
                    *guard = DurabilityMode::Synchronous;
                    Some(DurabilityMode::Synchronous)
                }
                DurabilityMode::AsyncRelaxed {
                    max_pending_blocks, ..
                } => {
                    let updated = DurabilityMode::Async {
                        max_pending_blocks: (*max_pending_blocks).max(1),
                    };
                    *guard = updated.clone();
                    Some(updated)
                }
                _ => None,
            }
        };

        if let Some(mode) = updated_mode.as_ref() {
            self.persist_durability_mode(mode)?;
        }

        tracing::info!(
            relaxed_mode_previously_enabled = updated_mode.is_some(),
            "Relaxed durability disabled; syncing every block"
        );
        Ok(())
    }

    /// Enables relaxed durability where syncs happen every N blocks.
    ///
    /// This significantly improves throughput during initial sync or catch-up,
    /// but increases the window of potential data loss in case of crash.
    ///
    /// # Arguments
    /// * `sync_every_n_blocks` - Number of blocks between fsync calls (minimum 1)
    ///
    /// # Example
    /// ```ignore
    /// // For initial blockchain sync:
    /// store.enable_relaxed_mode(100)?; // Sync every 100 blocks
    /// ```
    pub fn enable_relaxed_mode(&self, sync_every_n_blocks: usize) -> StoreResult<()> {
        let n = sync_every_n_blocks.max(1);
        // Enable metadata batching before loosening the journal policy so we never
        // advertise durability for blocks that have not reached disk yet.
        self.orchestrator.set_metadata_sync_interval(n)?;
        self.orchestrator
            .set_sync_policy(SyncPolicy::every_n_blocks(n));

        let new_mode = {
            let mut guard = self.durability_mode.write();
            let updated = match &*guard {
                DurabilityMode::Synchronous | DurabilityMode::SynchronousRelaxed { .. } => {
                    DurabilityMode::SynchronousRelaxed {
                        sync_every_n_blocks: n,
                    }
                }
                DurabilityMode::Async { max_pending_blocks }
                | DurabilityMode::AsyncRelaxed {
                    max_pending_blocks, ..
                } => DurabilityMode::AsyncRelaxed {
                    max_pending_blocks: (*max_pending_blocks).max(1),
                    sync_every_n_blocks: n,
                },
            };
            *guard = updated.clone();
            updated
        };

        self.persist_durability_mode(&new_mode)?;

        tracing::info!(
            sync_every_n_blocks = n,
            "Switched to relaxed durability mode"
        );
        Ok(())
    }

    fn persist_durability_mode(&self, mode: &DurabilityMode) -> StoreResult<()> {
        if let Some(metadata) = &self.metadata {
            if let Err(err) = metadata.store_durability_mode(mode) {
                let block = self.orchestrator.applied_block_height();
                let reason = format!("failed to persist durability mode {:?}: {}", mode, err);
                self.orchestrator.record_fatal_error(block, reason);
                return Err(err);
            }
        }
        Ok(())
    }

    /// Flushes all in-memory state and closes the store gracefully.
    pub fn close(&self) -> StoreResult<()> {
        if self
            .shutdown_state
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        if let Some(controller) = &self.remote_server {
            if let Err(err) = controller.shutdown() {
                self.shutdown_state.store(false, Ordering::Release);
                return Err(err);
            }
        }

        match self.orchestrator.shutdown() {
            Ok(()) => Ok(()),
            Err(err) => {
                self.shutdown_state.store(false, Ordering::Release);
                Err(err)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_testing(
        orchestrator: Arc<dyn BlockOrchestrator>,
        shutdown_state: Arc<AtomicBool>,
        handle_count: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            orchestrator,
            metadata: None,
            durability_mode: Arc::new(RwLock::new(DurabilityMode::default())),
            lock: None,
            shutdown_state,
            handle_count,
            remote_server: None,
        }
    }
}

impl StoreFacade for MhinStoreFacade {
    fn set(&self, block_height: BlockId, operations: Vec<Operation>) -> StoreResult<()> {
        for op in operations.iter() {
            op.value.ensure_within_limit()?;
        }
        self.orchestrator.apply_operations(block_height, operations)
    }

    fn rollback(&self, target: BlockId) -> StoreResult<()> {
        self.orchestrator.revert_to(target)
    }

    fn get(&self, key: Key) -> StoreResult<Value> {
        self.orchestrator.fetch(key)
    }

    fn multi_get(&self, keys: &[Key]) -> StoreResult<Vec<Value>> {
        self.orchestrator.fetch_many(keys)
    }

    fn enable_relaxed_mode(&self, sync_every_n_blocks: usize) -> StoreResult<()> {
        MhinStoreFacade::enable_relaxed_mode(self, sync_every_n_blocks)
    }

    fn relaxed_mode_enabled(&self) -> bool {
        MhinStoreFacade::relaxed_mode_enabled(self)
    }

    fn disable_relaxed_mode(&self) -> StoreResult<()> {
        MhinStoreFacade::disable_relaxed_mode(self)
    }

    fn close(&self) -> StoreResult<()> {
        MhinStoreFacade::close(self)
    }

    fn current_block(&self) -> StoreResult<BlockId> {
        self.orchestrator.current_block()
    }

    fn applied_block(&self) -> StoreResult<BlockId> {
        MhinStoreFacade::applied_block(self)
    }

    fn durable_block(&self) -> StoreResult<BlockId> {
        self.orchestrator.durable_block_height()
    }

    fn ensure_healthy(&self) -> StoreResult<()> {
        self.orchestrator.ensure_healthy()
    }
}

impl Drop for MhinStoreFacade {
    fn drop(&mut self) {
        if self.handle_count.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }

        if self
            .shutdown_state
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        if let Some(controller) = &self.remote_server {
            if let Err(err) = controller.shutdown() {
                tracing::warn!(error = ?err, "Failed to shutdown remote server during drop");
            }
        }

        if let Err(err) = self.orchestrator.shutdown() {
            tracing::warn!(
                error = ?err,
                "Failed to shutdown store during drop; persistence thread may remain active"
            );
            // We intentionally leave the shutdown flag set to prevent repeated attempts.
        }
    }
}

#[derive(Clone)]
struct RemoteServerController {
    shared: Arc<RemoteServerShared>,
}

struct RemoteServerShared {
    runtime: Mutex<Option<Runtime>>,
    task: Mutex<Option<JoinHandle<Result<(), ServerError>>>>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    metrics: RemoteServerHandle,
}

impl RemoteServerController {
    fn spawn(store: &MhinStoreFacade, settings: RemoteServerSettings) -> StoreResult<Self> {
        let worker_threads = settings.worker_threads.max(1);
        let runtime = TokioRuntimeBuilder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .build()?;

        let server = RemoteStoreServer::new(store.clone(), settings.to_server_config())?;
        let listener = runtime.block_on(server.bind_listener())?;
        let metrics = server.handle();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let task = runtime.spawn(async move {
            let shutdown = async move {
                let _ = shutdown_rx.await;
            };
            server
                .run_until_shutdown_with_listener(listener, shutdown)
                .await
        });

        Ok(Self {
            shared: Arc::new(RemoteServerShared {
                runtime: Mutex::new(Some(runtime)),
                task: Mutex::new(Some(task)),
                shutdown_tx: Mutex::new(Some(shutdown_tx)),
                metrics,
            }),
        })
    }

    fn shutdown(&self) -> StoreResult<()> {
        self.shared.shutdown()
    }

    fn snapshot(&self) -> ServerMetricsSnapshot {
        self.shared.metrics.snapshot()
    }
}

impl RemoteServerShared {
    fn shutdown(&self) -> StoreResult<()> {
        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }

        let task = self.task.lock().unwrap().take();
        let runtime = self.runtime.lock().unwrap().take();

        if let (Some(runtime), Some(task)) = (runtime, task) {
            match runtime.block_on(task) {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(MhinStoreError::from(err)),
                Err(join_err) => Err(MhinStoreError::RemoteServerTaskFailure {
                    reason: join_err.to_string(),
                }),
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for RemoteServerShared {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
