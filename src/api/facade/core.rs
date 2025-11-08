use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::block_journal::{FileBlockJournal, JournalOptions};
use crate::error::{MhinStoreError, StoreResult};
use crate::metadata::LmdbMetadataStore;
use crate::orchestrator::{BlockOrchestrator, DefaultBlockOrchestrator, ReadOnlyBlockOrchestrator};
use crate::snapshot::MmapSnapshotter;
use crate::state_engine::ShardedStateEngine;
use crate::state_shard::{RawTableShard, StateShard};
use crate::store_lock::StoreLockGuard;
use crate::types::{BlockId, Key, Operation, Value};

use super::config::{StoreConfig, StoreMode};
use super::recovery::{
    reconcile_metadata_with_journal, replay_committed_blocks, resolve_shard_layout,
    restore_existing_state, restore_existing_state_read_only,
};
use super::StoreFacade;

/// Main implementation of the store facade.
///
/// This is the primary entry point for all store operations. It manages
/// the lifecycle of internal components and provides a thread-safe interface.
pub struct MhinStoreFacade {
    orchestrator: Arc<dyn BlockOrchestrator>,
    lock: Option<Arc<StoreLockGuard>>,
    mode: StoreMode,
    shutdown_state: Arc<AtomicBool>,
    handle_count: Arc<AtomicUsize>,
}

impl Clone for MhinStoreFacade {
    fn clone(&self) -> Self {
        self.handle_count.fetch_add(1, Ordering::AcqRel);
        Self {
            orchestrator: Arc::clone(&self.orchestrator),
            lock: self.lock.clone(),
            mode: self.mode,
            shutdown_state: Arc::clone(&self.shutdown_state),
            handle_count: Arc::clone(&self.handle_count),
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
        let mode = config.mode;
        let lock = StoreLockGuard::acquire(&config.data_dir, mode)?;
        let lock = Arc::new(lock);

        match mode {
            StoreMode::ReadWrite => Self::build_read_write(config, lock),
            StoreMode::ReadOnly => Self::build_read_only(config, lock),
        }
    }

    fn build_read_write(config: StoreConfig, lock: Arc<StoreLockGuard>) -> StoreResult<Self> {
        std::fs::create_dir_all(&config.data_dir)?;

        let metadata = Arc::new(LmdbMetadataStore::new_with_map_size(
            config.metadata_dir(),
            config.lmdb_map_size,
        )?);
        let journal_options = JournalOptions {
            compress: config.compress_journal,
            compression_level: config.journal_compression_level,
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

        if metadata
            .load_durability_mode()?
            .map(|stored| stored != config.durability_mode)
            .unwrap_or(true)
        {
            metadata.store_durability_mode(&config.durability_mode)?;
        }

        let persistence_settings = crate::orchestrator::PersistenceSettings {
            durability_mode: config.durability_mode.clone(),
            snapshot_interval: config.snapshot_interval,
        };

        let orchestrator: Arc<dyn BlockOrchestrator> = Arc::new(DefaultBlockOrchestrator::new(
            Arc::clone(&engine),
            journal,
            snapshotter,
            metadata,
            persistence_settings,
        )?);

        Ok(Self {
            orchestrator,
            lock: Some(lock),
            mode: StoreMode::ReadWrite,
            shutdown_state: Arc::new(AtomicBool::new(false)),
            handle_count: Arc::new(AtomicUsize::new(1)),
        })
    }

    fn build_read_only(config: StoreConfig, lock: Arc<StoreLockGuard>) -> StoreResult<Self> {
        if !config.data_dir.exists() {
            return Err(MhinStoreError::MissingMetadata("data directory"));
        }

        let metadata = Arc::new(LmdbMetadataStore::open_read_only_with_map_size(
            config.metadata_dir(),
            config.lmdb_map_size,
        )?);
        let journal = Arc::new(FileBlockJournal::open_read_only(config.journal_dir())?);
        let snapshotter = Arc::new(MmapSnapshotter::open_read_only(config.snapshots_dir())?);

        let shard_layout = resolve_shard_layout(metadata.as_ref(), &config, false)?;

        let shards: Vec<Arc<dyn StateShard>> = (0..shard_layout.shards_count)
            .map(|i| {
                Arc::new(RawTableShard::new(i, shard_layout.initial_capacity))
                    as Arc<dyn StateShard>
            })
            .collect();

        let restored_block =
            restore_existing_state_read_only(snapshotter.as_ref(), metadata.as_ref(), &shards)?;

        let engine = Arc::new(ShardedStateEngine::new(shards, metadata.clone()));

        replay_committed_blocks(
            journal.as_ref(),
            metadata.as_ref(),
            engine.as_ref(),
            restored_block,
        )?;

        let orchestrator: Arc<dyn BlockOrchestrator> = Arc::new(ReadOnlyBlockOrchestrator::new(
            engine,
            metadata,
            restored_block,
        )?);

        Ok(Self {
            orchestrator,
            lock: Some(lock),
            mode: StoreMode::ReadOnly,
            shutdown_state: Arc::new(AtomicBool::new(false)),
            handle_count: Arc::new(AtomicUsize::new(1)),
        })
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
            lock: None,
            mode: StoreMode::ReadWrite,
            shutdown_state: Arc::new(AtomicBool::new(false)),
            handle_count: Arc::new(AtomicUsize::new(1)),
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

    /// Returns the current committed block height.
    pub fn current_block(&self) -> StoreResult<BlockId> {
        self.orchestrator.current_block()
    }

    /// Returns the highest block applied in memory.
    pub fn applied_block(&self) -> BlockId {
        self.orchestrator.applied_block_height()
    }

    /// Returns the highest block durably persisted.
    pub fn durable_block(&self) -> StoreResult<BlockId> {
        self.orchestrator.durable_block_height()
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
            lock: None,
            mode: StoreMode::ReadWrite,
            shutdown_state,
            handle_count,
        }
    }
}

impl StoreFacade for MhinStoreFacade {
    fn set(&self, block_height: BlockId, operations: Vec<Operation>) -> StoreResult<()> {
        self.orchestrator.apply_operations(block_height, operations)
    }

    fn rollback(&self, target: BlockId) -> StoreResult<()> {
        self.orchestrator.revert_to(target)
    }

    fn get(&self, key: Key) -> StoreResult<Value> {
        self.orchestrator.fetch(key)
    }

    fn close(&self) -> StoreResult<()> {
        MhinStoreFacade::close(self)
    }

    fn current_block(&self) -> StoreResult<BlockId> {
        self.orchestrator.current_block()
    }

    fn applied_block(&self) -> StoreResult<BlockId> {
        self.orchestrator.durable_block_height()?;
        Ok(self.orchestrator.applied_block_height())
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

        if let Err(err) = self.orchestrator.shutdown() {
            tracing::warn!(
                error = ?err,
                "Failed to shutdown store during drop; persistence thread may remain active"
            );
            // We intentionally leave the shutdown flag set to prevent repeated attempts.
        }
    }
}
