use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use heed::Error as HeedError;
use heed::MdbError;

use crate::error::{MhinStoreError, StoreResult};
use crate::metadata::LmdbMetadataStore;
use crate::net::{BasicAuthConfig, RemoteServerConfig, RemoteServerSecurity};
use crate::orchestrator::DurabilityMode;
use crate::storage::journal::JournalSizingSnapshot;
use crate::types::BlockId;

pub(crate) const DEFAULT_REMOTE_USERNAME: &str = "proto";
pub(crate) const DEFAULT_REMOTE_PASSWORD: &str = "proto";

/// Settings that control the embedded remote server started by `MhinStoreFacade`.
/// Defaults bind to `127.0.0.1:9443`, use plaintext transport, and ship placeholder
/// `proto`/`proto` credentials so local development can override them quickly. The
/// server refuses to start until [`RemoteServerSettings::with_basic_auth`] (or
/// [`RemoteServerSettings::with_auth_config`]) replaces those placeholders.
#[derive(Debug, Clone)]
pub struct RemoteServerSettings {
    pub bind_address: SocketAddr,
    pub tls: Option<RemoteServerTlsConfig>,
    pub auth: BasicAuthConfig,
    pub max_connections: usize,
    pub request_timeout: Duration,
    pub client_idle_timeout: Duration,
    /// Tokio runtime worker threads dedicated to the embedded server.
    pub worker_threads: usize,
}

/// TLS configuration for the remote server.
#[derive(Debug, Clone)]
pub struct RemoteServerTlsConfig {
    pub certificate_path: PathBuf,
    pub private_key_path: PathBuf,
}

/// Configuration for initializing the store (including the embedded remote server).
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
    /// Maximum time allowed between snapshots (forces a snapshot when exceeded)
    pub max_snapshot_interval: Duration,
    /// Whether to compress journal payloads on disk
    pub compress_journal: bool,
    /// Compression level for zstd (when compression enabled)
    pub journal_compression_level: i32,
    /// Maximum size of a single journal chunk before rotation.
    pub journal_chunk_size_bytes: u64,
    /// LMDB map size in bytes (default: 2GB, sufficient for Bitcoin and testnets)
    pub lmdb_map_size: usize,
    /// Whether to launch the embedded remote server managed by the facade.
    pub enable_server: bool,
    /// Optional remote server configuration (paired with `enable_server`).
    pub remote_server: Option<RemoteServerSettings>,
    /// Minimum rollback window retained on disk. `BlockId::MAX` disables pruning.
    pub min_rollback_window: BlockId,
    /// Interval between background pruning passes.
    pub prune_interval: Duration,
    /// Bootstrap estimate of blocks per chunk before history accumulates.
    pub bootstrap_block_profile: u64,
}

impl StoreConfig {
    const DEFAULT_MIN_ROLLBACK_WINDOW: BlockId = 100;
    const DEFAULT_PRUNE_INTERVAL_SECS: u64 = 10;
    const PRUNE_MEDIAN_SAMPLE: usize = 1000;
    const PRUNE_MIN_SAMPLE_SIZE: usize = 128;
    const MIN_HISTORICAL_CHUNKS: BlockId = 1;

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
        let journal_chunk_size_bytes = 128 << 20;
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            shards_count,
            initial_capacity,
            thread_count,
            durability_mode,
            snapshot_interval: Duration::from_secs(3600),
            max_snapshot_interval: Duration::from_secs(3600),
            compress_journal,
            journal_compression_level: 0,
            journal_chunk_size_bytes,
            lmdb_map_size: 2 << 30, // 2GB - sufficient for Bitcoin mainnet and all testnets
            enable_server: false,
            remote_server: Some(RemoteServerSettings::default()),
            min_rollback_window: Self::DEFAULT_MIN_ROLLBACK_WINDOW,
            prune_interval: Duration::from_secs(Self::DEFAULT_PRUNE_INTERVAL_SECS),
            bootstrap_block_profile: Self::default_bootstrap_block_profile(
                journal_chunk_size_bytes,
            ),
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
    /// # Durability
    ///
    /// New configurations default to `DurabilityMode::Async { max_pending_blocks: 1024 }`,
    /// which acknowledges blocks as soon as they enter the persistence queue. Call
    /// [`StoreConfig::with_durability_mode`] with [`DurabilityMode::Synchronous`] (or
    /// invoke [`crate::facade::MhinStoreFacade::disable_relaxed_mode`] at runtime) if you require
    /// an fsync after every block.
    ///
    /// # Errors
    ///
    /// Returns [`MhinStoreError::InvalidConfiguration`] if:
    /// - `shards_count` is 0
    /// - `initial_capacity` is 0
    /// - `thread_count` is 0
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use rollblock::StoreConfig;
    ///
    /// // Basic configuration
    /// let config = StoreConfig::new("./data", 4, 1000, 1, false)?;
    ///
    /// // High-performance configuration
    /// let config = StoreConfig::new("./data", 16, 1_000_000, 8, false)?;
    /// ```
    pub fn new(
        data_dir: impl AsRef<Path>,
        shards_count: usize,
        initial_capacity: usize,
        thread_count: usize,
        use_compression: bool,
    ) -> StoreResult<Self> {
        if shards_count == 0 {
            return Err(MhinStoreError::InvalidConfiguration {
                field: "shards_count",
                min: 1,
                value: 0,
            });
        }
        if initial_capacity == 0 {
            return Err(MhinStoreError::InvalidConfiguration {
                field: "initial_capacity",
                min: 1,
                value: 0,
            });
        }
        if thread_count == 0 {
            return Err(MhinStoreError::InvalidConfiguration {
                field: "thread_count",
                min: 1,
                value: 0,
            });
        }
        Ok(Self::base(
            data_dir,
            Some(shards_count),
            Some(initial_capacity),
            thread_count,
            use_compression,
        ))
    }

    /// Creates a store configuration for opening an existing data directory.
    ///
    /// Shard layout information will be loaded from metadata. You may call
    /// [`StoreConfig::with_shard_layout`] if you need to override the detected values.
    pub fn existing(data_dir: impl AsRef<Path>) -> Self {
        let config = Self::base(data_dir, None, None, 1, false);
        Self::existing_from_base(config)
    }

    /// Creates a store configuration for an existing data directory with a custom LMDB map size.
    ///
    /// This avoids temporarily expanding the LMDB file to the default 2 GiB before the recorded
    /// map size is loaded, which is particularly helpful in tests that rely on small temp dirs.
    pub fn existing_with_lmdb_map_size(data_dir: impl AsRef<Path>, map_size: usize) -> Self {
        let mut config = Self::base(data_dir, None, None, 1, false);
        config.lmdb_map_size = map_size.max(1);
        Self::existing_from_base(config)
    }

    /// Provides explicit shard layout information. Useful when opening older stores
    /// that were created before shard layout metadata was persisted.
    ///
    /// # Errors
    ///
    /// Returns [`MhinStoreError::InvalidConfiguration`] if `shards_count` or
    /// `initial_capacity` is 0.
    pub fn with_shard_layout(
        mut self,
        shards_count: usize,
        initial_capacity: usize,
    ) -> StoreResult<Self> {
        if shards_count == 0 {
            return Err(MhinStoreError::InvalidConfiguration {
                field: "shards_count",
                min: 1,
                value: 0,
            });
        }
        if initial_capacity == 0 {
            return Err(MhinStoreError::InvalidConfiguration {
                field: "initial_capacity",
                min: 1,
                value: 0,
            });
        }
        self.shards_count = Some(shards_count);
        self.initial_capacity = Some(initial_capacity);
        Ok(self)
    }

    pub fn with_durability_mode(mut self, mode: DurabilityMode) -> Self {
        self.durability_mode = mode;
        self
    }

    pub fn with_snapshot_interval(mut self, interval: Duration) -> Self {
        self.snapshot_interval = interval;
        self.max_snapshot_interval = interval;
        self
    }

    /// Sets the maximum lag allowed between snapshots. Setting this to zero disables
    /// the forced snapshot safeguard.
    pub fn with_max_snapshot_interval(mut self, interval: Duration) -> Self {
        self.max_snapshot_interval = interval;
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

    /// Sets the maximum on-disk size for a single journal chunk before rotation.
    ///
    /// Values below the size of a single entry (header + minimal payload) are clamped
    /// when the journal is initialized, so tests may supply tiny values to stress rotation.
    pub fn with_journal_chunk_size(mut self, bytes: u64) -> Self {
        let prev_default =
            Self::default_bootstrap_block_profile(self.journal_chunk_size_bytes).max(1);
        let prev_profile = self.bootstrap_block_profile;

        self.journal_chunk_size_bytes = bytes.max(1);

        if prev_profile == prev_default {
            self.bootstrap_block_profile =
                Self::default_bootstrap_block_profile(self.journal_chunk_size_bytes);
        }

        self
    }

    pub fn with_min_rollback_window(mut self, window: BlockId) -> StoreResult<Self> {
        if window == 0 {
            return Err(MhinStoreError::InvalidConfiguration {
                field: "min_rollback_window",
                min: 1,
                value: 0,
            });
        }
        self.min_rollback_window = window;
        Ok(self)
    }

    pub fn with_prune_interval(mut self, interval: Duration) -> StoreResult<Self> {
        if interval.is_zero() {
            return Err(MhinStoreError::InvalidConfiguration {
                field: "prune_interval",
                min: 1,
                value: 0,
            });
        }
        self.prune_interval = interval;
        Ok(self)
    }

    pub fn with_bootstrap_block_profile(mut self, blocks_per_chunk: u64) -> StoreResult<Self> {
        if blocks_per_chunk == 0 {
            return Err(MhinStoreError::InvalidConfiguration {
                field: "bootstrap_block_profile",
                min: 1,
                value: 0,
            });
        }
        self.bootstrap_block_profile = blocks_per_chunk;
        Ok(self)
    }

    fn default_bootstrap_block_profile(chunk_size_bytes: u64) -> u64 {
        const MIN_PROFILE: u64 = 1;
        let four_kib = 4 * 1024;
        let estimated = chunk_size_bytes / four_kib;
        estimated.max(MIN_PROFILE)
    }

    pub(crate) fn prune_sample_window() -> usize {
        Self::PRUNE_MEDIAN_SAMPLE
    }

    /// Returns advisory pruning diagnostics so operators can compare their
    /// configured window against recent journal statistics. The result never
    /// blocks startup—`with_min_rollback_window` already enforces the sole
    /// hard constraint (`window > 0`).
    pub(crate) fn pruning_diagnostics(
        &self,
        snapshot: &JournalSizingSnapshot,
    ) -> StoreResult<PruneDiagnostics> {
        if self.min_rollback_window == BlockId::MAX {
            return Ok(PruneDiagnostics::disabled(self.prune_interval));
        }

        let sample_size = snapshot.sample_size();
        let min_historical_chunks = Self::MIN_HISTORICAL_CHUNKS as usize;
        let (observed_block_bytes, used_bootstrap) =
            if sample_size >= Self::PRUNE_MIN_SAMPLE_SIZE && !snapshot.entry_sizes.is_empty() {
                let mut sorted = snapshot.entry_sizes.clone();
                sorted.sort_unstable();
                let median_idx = sorted.len() / 2;
                (sorted[median_idx], false)
            } else {
                (self.bootstrap_observed_block_bytes(snapshot), true)
            };

        let clamped_observed = observed_block_bytes
            .max(snapshot.min_entry_size_bytes)
            .min(snapshot.max_chunk_size_bytes)
            .max(1);
        let blocks_per_chunk = div_ceil(snapshot.max_chunk_size_bytes, clamped_observed);
        let safety_chunk_span = blocks_per_chunk.saturating_mul(Self::MIN_HISTORICAL_CHUNKS);
        let required_window = blocks_per_chunk.saturating_add(safety_chunk_span);

        let waiting_for_history = snapshot.sealed_chunk_count < min_historical_chunks;
        let window_satisfied = self.min_rollback_window >= required_window;

        Ok(PruneDiagnostics {
            min_window: self.min_rollback_window,
            prune_interval: self.prune_interval,
            blocks_per_chunk,
            safety_chunk_span,
            observed_block_bytes: clamped_observed,
            sample_size,
            used_bootstrap,
            required_window,
            pruning_disabled: false,
            waiting_for_history,
            window_satisfied,
        })
    }

    fn bootstrap_observed_block_bytes(&self, snapshot: &JournalSizingSnapshot) -> u64 {
        let profile = self.bootstrap_block_profile.max(1);
        let estimated = snapshot.max_chunk_size_bytes / profile;
        estimated.max(1)
    }

    pub fn with_async_max_pending(mut self, max_pending_blocks: usize) -> Self {
        self.durability_mode = DurabilityMode::Async {
            max_pending_blocks: max_pending_blocks.max(1),
        };
        self
    }

    /// Configures the store to use relaxed async durability.
    ///
    /// This mode syncs to disk every `sync_every_n_blocks` blocks instead of every block,
    /// significantly improving throughput at the cost of increased data loss window.
    ///
    /// # Arguments
    /// * `max_pending_blocks` - Maximum blocks that can be queued for persistence
    /// * `sync_every_n_blocks` - Number of blocks between fsync calls
    ///
    /// # Example
    /// ```ignore
    /// let config = StoreConfig::new("./data", 4, 1000, 1, false)?
    ///     .with_async_relaxed(1024, 100); // Sync every 100 blocks
    /// ```
    pub fn with_async_relaxed(
        mut self,
        max_pending_blocks: usize,
        sync_every_n_blocks: usize,
    ) -> Self {
        self.durability_mode = DurabilityMode::AsyncRelaxed {
            max_pending_blocks: max_pending_blocks.max(1),
            sync_every_n_blocks: sync_every_n_blocks.max(1),
        };
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
    /// let config = StoreConfig::new("./data", 4, 1000, 1, false)?
    ///     .with_lmdb_map_size(10 << 30); // 10GB
    /// ```
    pub fn with_lmdb_map_size(mut self, size: usize) -> Self {
        self.lmdb_map_size = size;
        self
    }

    /// Enables or customizes the embedded remote server that
    /// `MhinStoreFacade::new` manages automatically. Calling this method also
    /// flips [`StoreConfig::enable_server`] to `true`.
    pub fn with_remote_server(mut self, settings: RemoteServerSettings) -> Self {
        self.remote_server = Some(settings);
        self.enable_server = true;
        self
    }

    /// Enables the embedded remote server using the current settings.
    ///
    /// Returns an error if the remote server settings were previously removed
    /// with [`StoreConfig::without_remote_server`].
    pub fn enable_remote_server(mut self) -> StoreResult<Self> {
        if self.remote_server.is_none() {
            return Err(MhinStoreError::RemoteServerConfigMissing);
        }
        self.enable_server = true;
        Ok(self)
    }

    /// Disables the embedded remote server but keeps the settings in place.
    pub fn disable_remote_server(mut self) -> Self {
        self.enable_server = false;
        self
    }

    /// Disables the embedded remote server entirely (useful for tests or
    /// standalone benchmarking binaries) and removes all stored settings.
    pub fn without_remote_server(mut self) -> Self {
        self.remote_server = None;
        self.enable_server = false;
        self
    }

    /// Returns the remote server settings, if configured.
    pub fn remote_server(&self) -> Option<&RemoteServerSettings> {
        self.remote_server.as_ref()
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
            match LmdbMetadataStore::new_with_map_size(metadata_dir, map_size) {
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
    fn existing_from_base(mut config: Self) -> Self {
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

                if let Ok(Some(chunk_size)) = metadata.load_journal_chunk_size() {
                    config.journal_chunk_size_bytes = chunk_size;
                }
                config.bootstrap_block_profile =
                    Self::default_bootstrap_block_profile(config.journal_chunk_size_bytes);
                if let Ok(Some(window)) = metadata.load_min_rollback_window() {
                    config.min_rollback_window = window;
                }
                if let Ok(Some(interval)) = metadata.load_prune_interval() {
                    config.prune_interval = interval;
                }
                if let Ok(Some(profile)) = metadata.load_bootstrap_block_profile() {
                    config.bootstrap_block_profile = profile.max(1);
                }
            }
        }

        config
    }
}

impl RemoteServerSettings {
    pub fn with_bind_address(mut self, addr: SocketAddr) -> Self {
        self.bind_address = addr;
        self
    }

    pub fn with_bind_port(mut self, port: u16) -> Self {
        match &mut self.bind_address {
            SocketAddr::V4(addr) => addr.set_port(port),
            SocketAddr::V6(addr) => addr.set_port(port),
        }
        self
    }

    pub fn with_tls(
        mut self,
        certificate_path: impl Into<PathBuf>,
        private_key_path: impl Into<PathBuf>,
    ) -> Self {
        self.tls = Some(RemoteServerTlsConfig {
            certificate_path: certificate_path.into(),
            private_key_path: private_key_path.into(),
        });
        self
    }

    pub fn without_tls(mut self) -> Self {
        self.tls = None;
        self
    }

    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth = BasicAuthConfig::new(username, password);
        self
    }

    /// Returns true if the credentials match the built-in defaults.
    pub fn uses_default_auth(&self) -> bool {
        self.auth.username == DEFAULT_REMOTE_USERNAME
            && self.auth.password == DEFAULT_REMOTE_PASSWORD
    }

    pub fn with_auth_config(mut self, auth: BasicAuthConfig) -> Self {
        self.auth = auth;
        self
    }

    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max.max(1);
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_client_idle_timeout(mut self, timeout: Duration) -> Self {
        self.client_idle_timeout = timeout;
        self
    }

    /// Overrides the number of Tokio worker threads used by the embedded remote server.
    ///
    /// The value is clamped to at least `1`. Defaults to `std::thread::available_parallelism()`.
    pub fn with_worker_threads(mut self, threads: usize) -> Self {
        self.worker_threads = threads.max(1);
        self
    }

    pub(crate) fn to_server_config(&self) -> RemoteServerConfig {
        let security = match &self.tls {
            Some(tls) => RemoteServerSecurity::Tls {
                certificate_path: tls.certificate_path.clone(),
                private_key_path: tls.private_key_path.clone(),
            },
            None => RemoteServerSecurity::Plain,
        };

        RemoteServerConfig {
            bind_address: self.bind_address,
            security,
            auth: self.auth.clone(),
            max_connections: self.max_connections,
            request_timeout: self.request_timeout,
            client_idle_timeout: self.client_idle_timeout,
        }
    }
}

/// Advisory telemetry about the pruning window. This is purely informational and
/// never drives hard validation.
#[derive(Debug, Clone)]
pub(crate) struct PruneDiagnostics {
    pub min_window: BlockId,
    pub prune_interval: Duration,
    pub blocks_per_chunk: BlockId,
    pub safety_chunk_span: BlockId,
    pub observed_block_bytes: u64,
    pub sample_size: usize,
    pub used_bootstrap: bool,
    pub required_window: BlockId,
    pub pruning_disabled: bool,
    pub waiting_for_history: bool,
    pub window_satisfied: bool,
}

impl PruneDiagnostics {
    pub fn disabled(prune_interval: Duration) -> Self {
        Self {
            min_window: BlockId::MAX,
            prune_interval,
            blocks_per_chunk: 0,
            safety_chunk_span: 0,
            observed_block_bytes: 0,
            sample_size: 0,
            used_bootstrap: false,
            required_window: 0,
            pruning_disabled: true,
            waiting_for_history: false,
            window_satisfied: true,
        }
    }
}

fn div_ceil(num: u64, denom: u64) -> BlockId {
    if denom == 0 {
        return BlockId::MAX;
    }
    let quotient = num / denom;
    if num.is_multiple_of(denom) {
        quotient
    } else {
        quotient.saturating_add(1)
    }
}

impl Default for RemoteServerSettings {
    fn default() -> Self {
        Self {
            bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9443),
            tls: None,
            auth: BasicAuthConfig::new(DEFAULT_REMOTE_USERNAME, DEFAULT_REMOTE_PASSWORD),
            max_connections: 512,
            request_timeout: Duration::from_secs(2),
            client_idle_timeout: Duration::from_secs(10),
            worker_threads: default_worker_threads(),
        }
    }
}

fn default_worker_threads() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::journal::JournalSizingSnapshot;
    use tempfile::{tempdir, tempdir_in};

    #[test]
    fn enable_remote_server_uses_existing_settings() {
        let dir = tempdir().unwrap();
        let config = StoreConfig::new(dir.path(), 1, 1, 1, false)
            .expect("valid config")
            .enable_remote_server()
            .expect("defaults exist");

        assert!(config.enable_server);
        assert!(config.remote_server.is_some());
    }

    #[test]
    fn enable_remote_server_fails_without_settings() {
        let dir = tempdir().unwrap();
        let error = StoreConfig::new(dir.path(), 1, 1, 1, false)
            .expect("valid config")
            .without_remote_server()
            .enable_remote_server()
            .expect_err("settings removed");

        assert!(matches!(error, MhinStoreError::RemoteServerConfigMissing));
    }

    #[test]
    fn new_rejects_zero_shards_count() {
        let dir = tempdir().unwrap();
        let error = StoreConfig::new(dir.path(), 0, 1, 1, false).expect_err("zero shards");

        assert!(matches!(
            error,
            MhinStoreError::InvalidConfiguration {
                field: "shards_count",
                min: 1,
                value: 0,
            }
        ));
    }

    #[test]
    fn new_rejects_zero_initial_capacity() {
        let dir = tempdir().unwrap();
        let error = StoreConfig::new(dir.path(), 1, 0, 1, false).expect_err("zero capacity");

        assert!(matches!(
            error,
            MhinStoreError::InvalidConfiguration {
                field: "initial_capacity",
                min: 1,
                value: 0,
            }
        ));
    }

    #[test]
    fn new_rejects_zero_thread_count() {
        let dir = tempdir().unwrap();
        let error = StoreConfig::new(dir.path(), 1, 1, 0, false).expect_err("zero threads");

        assert!(matches!(
            error,
            MhinStoreError::InvalidConfiguration {
                field: "thread_count",
                min: 1,
                value: 0,
            }
        ));
    }

    #[test]
    fn with_shard_layout_rejects_zero_values() {
        let dir = tempdir().unwrap();
        let config = StoreConfig::existing(dir.path());

        let error = config
            .clone()
            .with_shard_layout(0, 100)
            .expect_err("zero shards");
        assert!(matches!(
            error,
            MhinStoreError::InvalidConfiguration {
                field: "shards_count",
                ..
            }
        ));

        let error = config.with_shard_layout(4, 0).expect_err("zero capacity");
        assert!(matches!(
            error,
            MhinStoreError::InvalidConfiguration {
                field: "initial_capacity",
                ..
            }
        ));
    }

    #[test]
    fn worker_thread_override_is_clamped() {
        let settings = RemoteServerSettings::default().with_worker_threads(0);
        assert_eq!(settings.worker_threads, 1);
    }

    #[test]
    fn with_min_rollback_window_rejects_zero() {
        let dir = tempdir().unwrap();
        let base = StoreConfig::new(dir.path(), 1, 1, 1, false).expect("valid config");
        let err = base
            .with_min_rollback_window(0)
            .expect_err("zero window rejected");
        assert!(matches!(
            err,
            MhinStoreError::InvalidConfiguration {
                field: "min_rollback_window",
                ..
            }
        ));
    }

    #[test]
    fn with_prune_interval_rejects_zero() {
        let dir = tempdir().unwrap();
        let base = StoreConfig::new(dir.path(), 1, 1, 1, false).expect("valid config");
        let err = base
            .with_prune_interval(Duration::from_secs(0))
            .expect_err("zero interval rejected");
        assert!(matches!(
            err,
            MhinStoreError::InvalidConfiguration {
                field: "prune_interval",
                ..
            }
        ));
    }

    #[test]
    fn with_bootstrap_block_profile_rejects_zero() {
        let dir = tempdir().unwrap();
        let base = StoreConfig::new(dir.path(), 1, 1, 1, false).expect("valid config");
        let err = base
            .with_bootstrap_block_profile(0)
            .expect_err("zero profile rejected");
        assert!(matches!(
            err,
            MhinStoreError::InvalidConfiguration {
                field: "bootstrap_block_profile",
                ..
            }
        ));
    }

    #[test]
    fn existing_config_uses_stored_chunk_size() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let data_dir = tmp.path();
        let metadata_path = data_dir.join("metadata");
        std::fs::create_dir_all(&metadata_path).unwrap();
        let chunk_size = 32_u64 << 20;

        let metadata = LmdbMetadataStore::new_with_map_size(&metadata_path, 1 << 20).unwrap();
        metadata
            .store_journal_chunk_size(chunk_size)
            .expect("chunk size persisted");
        metadata
            .store_min_rollback_window(256)
            .expect("window persisted");
        metadata
            .store_prune_interval(Duration::from_secs(3))
            .expect("interval persisted");
        metadata
            .store_bootstrap_block_profile(64)
            .expect("profile persisted");
        drop(metadata);

        let config = StoreConfig::existing(data_dir);
        assert_eq!(config.journal_chunk_size_bytes, chunk_size);
        assert_eq!(config.min_rollback_window, 256);
        assert_eq!(config.prune_interval, Duration::from_secs(3));
        assert_eq!(config.bootstrap_block_profile, 64);
    }

    #[test]
    fn pruning_diagnostics_accepts_sufficient_window() {
        let dir = tempdir().unwrap();
        let config = StoreConfig::new(dir.path(), 1, 1, 1, false)
            .expect("valid config")
            .with_journal_chunk_size(64 << 10);
        let snapshot = JournalSizingSnapshot {
            entry_sizes: vec![4096; 256],
            sealed_chunk_count: 2,
            chunk_count: 3,
            min_entry_size_bytes: 1024,
            max_chunk_size_bytes: config.journal_chunk_size_bytes,
        };
        let validation = config
            .pruning_diagnostics(&snapshot)
            .expect("validation succeeds");
        assert!(!validation.pruning_disabled);
        assert!(!validation.used_bootstrap);
        assert!(validation.window_satisfied);
    }

    #[test]
    fn pruning_diagnostics_reports_small_window() {
        let dir = tempdir().unwrap();
        let mut config = StoreConfig::new(dir.path(), 1, 1, 1, false).expect("valid config");
        config.min_rollback_window = 2;
        let snapshot = JournalSizingSnapshot {
            entry_sizes: vec![128; 256],
            sealed_chunk_count: 1,
            chunk_count: 2,
            min_entry_size_bytes: 64,
            max_chunk_size_bytes: 256,
        };
        let validation = config
            .pruning_diagnostics(&snapshot)
            .expect("validation should no longer fail");
        assert!(
            !validation.window_satisfied,
            "window should be flagged as insufficient"
        );
    }

    #[test]
    fn pruning_diagnostics_uses_bootstrap_without_history() {
        let dir = tempdir().unwrap();
        let mut config = StoreConfig::new(dir.path(), 1, 1, 1, false).expect("valid config");
        // Use an oversized window so bootstrap validation succeeds even before history accrues.
        config.min_rollback_window = config.journal_chunk_size_bytes;
        let snapshot = JournalSizingSnapshot {
            entry_sizes: Vec::new(),
            sealed_chunk_count: 0,
            chunk_count: 1,
            min_entry_size_bytes: 128,
            max_chunk_size_bytes: config.journal_chunk_size_bytes,
        };
        let validation = config
            .pruning_diagnostics(&snapshot)
            .expect("validation succeeds");
        assert!(validation.used_bootstrap);
        assert!(validation.waiting_for_history);
    }

    #[test]
    fn pruning_diagnostics_waiting_for_history_reports_small_window() {
        let dir = tempdir().unwrap();
        let mut config = StoreConfig::new(dir.path(), 1, 1, 1, false).expect("valid config");
        config.min_rollback_window = 1;
        let snapshot = JournalSizingSnapshot {
            entry_sizes: Vec::new(),
            sealed_chunk_count: 0,
            chunk_count: 1,
            min_entry_size_bytes: 128,
            max_chunk_size_bytes: config.journal_chunk_size_bytes,
        };
        let validation = config
            .pruning_diagnostics(&snapshot)
            .expect("validation succeeds even when waiting for history");
        assert!(validation.waiting_for_history);
        assert!(
            !validation.window_satisfied,
            "should report that configured window does not meet derived requirement"
        );
    }

    #[test]
    fn pruning_diagnostics_detects_small_window_once_history_exists() {
        let dir = tempdir().unwrap();
        let mut config = StoreConfig::new(dir.path(), 1, 1, 1, false).expect("valid config");
        config.min_rollback_window = 1;
        let snapshot = JournalSizingSnapshot {
            entry_sizes: vec![256; 128],
            sealed_chunk_count: 1,
            chunk_count: 2,
            min_entry_size_bytes: 128,
            max_chunk_size_bytes: config.journal_chunk_size_bytes,
        };
        let validation = config
            .pruning_diagnostics(&snapshot)
            .expect("validation succeeds even with small window");
        assert!(
            !validation.window_satisfied,
            "derived requirement should be reported even after history accumulates"
        );
        assert!(!validation.waiting_for_history);
    }

    #[test]
    fn journal_chunk_size_preserves_custom_bootstrap_profile() {
        let dir = tempdir().unwrap();
        let base = StoreConfig::new(dir.path(), 1, 1, 1, false).expect("valid config");

        // Without overrides the profile should track chunk size changes.
        let default_profile = base.bootstrap_block_profile;
        let reduced = base
            .clone()
            .with_journal_chunk_size(base.journal_chunk_size_bytes / 2);
        assert_ne!(
            reduced.bootstrap_block_profile, default_profile,
            "default profile should adapt with chunk size"
        );

        // Once overridden, chunk size updates must preserve the explicit value.
        let custom = base
            .with_bootstrap_block_profile(42)
            .expect("custom profile accepted")
            .with_journal_chunk_size(8 << 20);
        assert_eq!(
            custom.bootstrap_block_profile, 42,
            "custom profile should remain intact after chunk resize"
        );
    }
}
