use std::path::{Path, PathBuf};
use std::time::Duration;

use heed::Error as HeedError;
use heed::MdbError;

use crate::error::{MhinStoreError, StoreResult};
use crate::metadata::LmdbMetadataStore;
use crate::orchestrator::DurabilityMode;

/// Operating mode for the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StoreMode {
    #[default]
    ReadWrite,
    ReadOnly,
}

impl StoreMode {
    pub fn is_read_only(&self) -> bool {
        matches!(self, StoreMode::ReadOnly)
    }

    pub fn is_read_write(&self) -> bool {
        matches!(self, StoreMode::ReadWrite)
    }
}

/// Configuration for initializing the store.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Base directory for all store data
    pub data_dir: PathBuf,
    /// Number of shards for the state engine (required when creating a new store)
    pub shards_count: Option<usize>,
    /// Initial capacity of each shard (required when creating a new store)
    pub initial_capacity: Option<usize>,
    /// Number of threads for parallelism (if > 1, uses parallel mode)
    pub thread_count: usize,
    /// Durability mode for persistence (synchronous vs async)
    pub durability_mode: DurabilityMode,
    /// Interval between automatic snapshots when async durability is enabled
    pub snapshot_interval: Duration,
    /// Whether to compress journal payloads on disk
    pub compress_journal: bool,
    /// Compression level for zstd (when compression enabled)
    pub journal_compression_level: i32,
    /// Operating mode for the store (read/write or read-only)
    pub mode: StoreMode,
    /// LMDB map size in bytes (default: 2GB, sufficient for Bitcoin and testnets)
    pub lmdb_map_size: usize,
}

impl StoreConfig {
    fn base(
        data_dir: impl AsRef<Path>,
        shards_count: Option<usize>,
        initial_capacity: Option<usize>,
        thread_count: usize,
        compress_journal: bool,
    ) -> Self {
        let durability_mode = DurabilityMode::Async {
            max_pending_blocks: 1024,
        };
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            shards_count,
            initial_capacity,
            thread_count,
            durability_mode,
            snapshot_interval: Duration::from_secs(3600),
            compress_journal,
            journal_compression_level: 0,
            mode: StoreMode::default(),
            lmdb_map_size: 2 << 30, // 2GB - sufficient for Bitcoin mainnet and all testnets
        }
    }

    /// Creates a new store configuration for initializing a fresh data directory.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Base directory for all store data (metadata, journal, snapshots)
    /// * `shards_count` - Number of shards for parallel processing (recommended: 4-16)
    /// * `initial_capacity` - Initial capacity per shard (should account for expected entries)
    /// * `thread_count` - Number of threads for parallelism (1 = sequential, >1 = parallel)
    /// * `use_compression` - Whether to compress journal payloads (default: false)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use rollblock::StoreConfig;
    ///
    /// // Basic configuration
    /// let config = StoreConfig::new("./data", 4, 1000, 1, false);
    ///
    /// // High-performance configuration
    /// let config = StoreConfig::new("./data", 16, 1_000_000, 8, false);
    /// ```
    pub fn new(
        data_dir: impl AsRef<Path>,
        shards_count: usize,
        initial_capacity: usize,
        thread_count: usize,
        use_compression: bool,
    ) -> Self {
        Self::base(
            data_dir,
            Some(shards_count),
            Some(initial_capacity),
            thread_count,
            use_compression,
        )
    }

    /// Creates a store configuration for opening an existing data directory.
    ///
    /// Shard layout information will be loaded from metadata. You may call
    /// [`StoreConfig::with_shard_layout`] if you need to override the detected values.
    pub fn existing(data_dir: impl AsRef<Path>) -> Self {
        let mut config = Self::base(data_dir, None, None, 1, false);
        let metadata_dir = config.metadata_dir();

        if metadata_dir.exists() {
            if let Ok((metadata, _used_map_size)) =
                Self::open_metadata_with_compatible_map_size(&metadata_dir, config.lmdb_map_size)
            {
                if let Ok(Some(mode)) = metadata.load_durability_mode() {
                    config.durability_mode = mode;
                }

                let fallback_map_size = metadata.effective_map_size();
                config.lmdb_map_size = match metadata.load_lmdb_map_size() {
                    Ok(Some(recorded)) => recorded,
                    _ => fallback_map_size,
                };
            }
        }

        config
    }

    /// Provides explicit shard layout information. Useful when opening older stores
    /// that were created before shard layout metadata was persisted.
    pub fn with_shard_layout(mut self, shards_count: usize, initial_capacity: usize) -> Self {
        self.shards_count = Some(shards_count);
        self.initial_capacity = Some(initial_capacity);
        self
    }

    pub fn with_durability_mode(mut self, mode: DurabilityMode) -> Self {
        self.durability_mode = mode;
        self
    }

    pub fn with_snapshot_interval(mut self, interval: Duration) -> Self {
        self.snapshot_interval = interval;
        self
    }

    pub fn with_journal_compression(mut self, enabled: bool) -> Self {
        self.compress_journal = enabled;
        self
    }

    pub fn with_journal_compression_level(mut self, level: i32) -> Self {
        self.journal_compression_level = level;
        self
    }

    pub fn with_async_max_pending(mut self, max_pending_blocks: usize) -> Self {
        self.durability_mode = DurabilityMode::Async {
            max_pending_blocks: max_pending_blocks.max(1),
        };
        self
    }

    pub fn with_mode(mut self, mode: StoreMode) -> Self {
        self.mode = mode;
        self
    }

    /// Sets the LMDB map size in bytes.
    ///
    /// The map size determines the maximum size of the metadata database.
    /// Default is 2GB which is sufficient for Bitcoin and all its testnets.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // For high-frequency blockchains (e.g., Polygon)
    /// let config = StoreConfig::new("./data", 4, 1000, 1, false)
    ///     .with_lmdb_map_size(10 << 30); // 10GB
    /// ```
    pub fn with_lmdb_map_size(mut self, size: usize) -> Self {
        self.lmdb_map_size = size;
        self
    }

    /// Returns the path to the metadata directory.
    pub fn metadata_dir(&self) -> PathBuf {
        self.data_dir.join("metadata")
    }

    /// Returns the path to the journal directory.
    pub fn journal_dir(&self) -> PathBuf {
        self.data_dir.join("journal")
    }

    /// Returns the path to the snapshots directory.
    pub fn snapshots_dir(&self) -> PathBuf {
        self.data_dir.join("snapshots")
    }

    fn open_metadata_with_compatible_map_size(
        metadata_dir: &Path,
        initial_map_size: usize,
    ) -> StoreResult<(LmdbMetadataStore, usize)> {
        let mut map_size = initial_map_size.max(1);

        loop {
            match LmdbMetadataStore::open_read_only_with_map_size(metadata_dir, map_size) {
                Ok(metadata) => return Ok((metadata, map_size)),
                Err(err) => {
                    if Self::should_retry_with_larger_map(&err) {
                        if let Some(next) = map_size.checked_mul(2) {
                            map_size = next;
                            continue;
                        }
                    }
                    return Err(err);
                }
            }
        }
    }

    fn should_retry_with_larger_map(error: &MhinStoreError) -> bool {
        matches!(
            error,
            MhinStoreError::Heed(HeedError::Mdb(MdbError::MapFull | MdbError::MapResized))
        )
    }
}
