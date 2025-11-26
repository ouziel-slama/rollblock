use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use heed::Error as HeedError;
use heed::MdbError;

use crate::error::{MhinStoreError, StoreResult};
use crate::metadata::LmdbMetadataStore;
use crate::net::{BasicAuthConfig, RemoteServerConfig, RemoteServerSecurity};
use crate::orchestrator::DurabilityMode;

/// Settings that control the embedded remote server started by `MhinStoreFacade`.
/// Defaults bind to `127.0.0.1:9443`, use plaintext transport, and authenticate
/// with `proto`/`proto` credentials so local development works out of the box.
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
    /// LMDB map size in bytes (default: 2GB, sufficient for Bitcoin and testnets)
    pub lmdb_map_size: usize,
    /// Whether to launch the embedded remote server managed by the facade.
    pub enable_server: bool,
    /// Optional remote server configuration (paired with `enable_server`).
    pub remote_server: Option<RemoteServerSettings>,
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
            max_snapshot_interval: Duration::from_secs(3600),
            compress_journal,
            journal_compression_level: 0,
            lmdb_map_size: 2 << 30, // 2GB - sufficient for Bitcoin mainnet and all testnets
            enable_server: false,
            remote_server: Some(RemoteServerSettings::default()),
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
    /// let config = StoreConfig::new("./data", 4, 1000, 1, false)
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
    /// let config = StoreConfig::new("./data", 4, 1000, 1, false)
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

impl Default for RemoteServerSettings {
    fn default() -> Self {
        Self {
            bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9443),
            tls: None,
            auth: BasicAuthConfig::new("proto", "proto"),
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
    use tempfile::tempdir;

    #[test]
    fn enable_remote_server_uses_existing_settings() {
        let dir = tempdir().unwrap();
        let config = StoreConfig::new(dir.path(), 1, 1, 1, false)
            .enable_remote_server()
            .expect("defaults exist");

        assert!(config.enable_server);
        assert!(config.remote_server.is_some());
    }

    #[test]
    fn enable_remote_server_fails_without_settings() {
        let dir = tempdir().unwrap();
        let error = StoreConfig::new(dir.path(), 1, 1, 1, false)
            .without_remote_server()
            .enable_remote_server()
            .expect_err("settings removed");

        assert!(matches!(error, MhinStoreError::RemoteServerConfigMissing));
    }

    #[test]
    fn worker_thread_override_is_clamped() {
        let settings = RemoteServerSettings::default().with_worker_threads(0);
        assert_eq!(settings.worker_threads, 1);
    }
}
