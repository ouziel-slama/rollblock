//! # Rollblock
//!
//! A high-performance, reliable state storage system with rollback support,
//! optimized for blockchain applications.
//!
//! ## Features
//!
//! - **Instant Rollback**: Revert to any previous block
//! - **High Performance**: Parallel sharding with optimized hashmaps
//! - **Compressed Snapshots**: Periodic state backups with zstd compression
//! - **Sparse Blocks**: Support for gaps in block numbering
//! - **Secure**: Data integrity with Blake3 checksums
//!
//! ## Quick Start
//!
//! ```ignore
//! use rollblock::*;
//! use rollblock::types::Operation;
//!
//! // Create configuration
//! let config = StoreConfig::new(
//!     "./data",     // data directory
//!     4,            // number of shards
//!     1000,         // initial capacity per shard
//!     1,            // thread count (1 = sequential mode)
//!     false,        // use compression (default: false)
//! )?;
//!
//! // Initialize store
//! let store = MhinStoreFacade::new(config)?;
//!
//! // Apply operations
//! let ops = vec![
//!     Operation {
//!         key: [1, 2, 3, 4, 5, 6, 7, 8],
//!         value: b"answer".to_vec().into(),
//!     },
//! ];
//! store.set(1, ops)?;
//!
//! // Read value
//! let key = [1, 2, 3, 4, 5, 6, 7, 8];
//! let value = store.get(key)?;
//! if value.is_delete() {
//!     println!("Key not found");
//! } else {
//!     println!("Value bytes: {:?}", value.as_slice());
//! }
//!
//! // Rollback
//! store.rollback(0)?;
//! store.close()?;
//! # Ok::<(), rollblock::error::MhinStoreError>(())
//! ```
//! Networking is opt-in: call `.enable_remote_server()` (or
//! `.with_remote_server(...)`) to start the embedded server. It binds to
//! `127.0.0.1:9443` by default but refuses to start until you override the
//! placeholder Basic Auth credentials via `RemoteServerSettings::with_basic_auth(...)`.

pub mod api;
pub mod client;
pub mod net;
pub mod runtime;
pub mod state;
pub mod storage;

pub use crate::api::{error, facade, types};
pub use crate::runtime::{metrics, orchestrator};
pub use crate::state::{engine as state_engine, shard as state_shard};
pub use crate::storage::fs::store_lock;
pub use crate::storage::journal as block_journal;
pub use crate::storage::metadata;
pub use crate::storage::snapshot;

pub use api::error::{MhinStoreError, StoreResult};
pub use api::facade::{
    MhinStoreBlockFacade, MhinStoreFacade, RemoteServerSettings, StoreConfig, StoreFacade,
};
pub use api::types::*;
pub use client::{ClientError, RemoteStoreClient};
pub use net::server::{
    RemoteServerConfig, RemoteServerHandle, RemoteServerSecurity, RemoteStoreServer, ServerError,
    ServerMetricsSnapshot,
};
pub use net::BasicAuthConfig;
pub use runtime::metrics::{HealthState, HealthStatus, MetricsSnapshot, StoreMetrics};
pub use runtime::orchestrator::{
    BlockOrchestrator, DefaultBlockOrchestrator, DurabilityMode, PersistenceSettings,
};
pub use state::engine::{ShardedStateEngine, StateEngine};
pub use state::shard::{RawTableShard, StateShard};
pub use storage::fs::store_lock::StoreLockGuard;
pub use storage::journal::{
    BlockJournal, FileBlockJournal, JournalBlock, JournalIter, JournalOptions,
    JournalPruneObserver, JournalPruneReport, JournalPruner, SyncPolicy,
};
pub use storage::metadata::{LmdbMetadataStore, MetadataStore, ShardLayout};
pub use storage::snapshot::{MmapSnapshotter, Snapshotter};
