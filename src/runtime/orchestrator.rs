//! Block orchestration and durability infrastructure.
//!
//! The orchestrator coordinates mutation application, persistence, and rollback.
//! Concurrency is guarded by a global `update_mutex` while the persistence runtime
//! owns its own queue and synchronization primitives. The invariants that callers
//! rely on are:
//! - once a block height is reported durable it never regresses;
//! - rollback barriers are respected by both the mutation path and the background
//!   persistence worker;
//! - metrics always reflect the latest applied/durable heights exposed to callers.

pub mod durability;
pub mod persistence;

mod default;

pub use default::DefaultBlockOrchestrator;
pub use durability::{DurabilityMode, PersistenceSettings};
pub use persistence::PersistenceContext;

use crate::block_journal::SyncPolicy;
use crate::error::StoreResult;
use crate::metrics::StoreMetrics;
use crate::types::{BlockId, Key, Operation, Value};

/// API surfaced by orchestrator implementations.
pub trait BlockOrchestrator: Send + Sync {
    fn apply_operations(&self, block_height: BlockId, ops: Vec<Operation>) -> StoreResult<()>;
    fn revert_to(&self, block: BlockId) -> StoreResult<()>;
    fn fetch(&self, key: Key) -> StoreResult<Value>;
    fn fetch_many(&self, keys: &[Key]) -> StoreResult<Vec<Value>>;
    fn metrics(&self) -> Option<&StoreMetrics>;
    fn current_block(&self) -> StoreResult<BlockId>;
    fn applied_block_height(&self) -> BlockId;
    fn durable_block_height(&self) -> StoreResult<BlockId>;
    fn shutdown(&self) -> StoreResult<()>;
    fn ensure_healthy(&self) -> StoreResult<()>;
    /// Records a fatal durability error so subsequent calls fail fast.
    ///
    /// Implementations that do not track fatal state can keep the default
    /// no-op behavior.
    fn record_fatal_error(&self, _block: BlockId, _reason: String) {}

    /// Changes the journal sync policy at runtime.
    ///
    /// This allows switching between relaxed mode (during initial sync)
    /// and strict mode (when approaching chain tip).
    fn set_sync_policy(&self, _policy: SyncPolicy) {
        // Default no-op
    }

    /// Updates the metadata sync interval to match the journal policy.
    ///
    /// Implementations that batch metadata commits in relaxed mode should use this
    /// hook to decide whether to buffer LMDB writes or flush immediately.
    fn set_metadata_sync_interval(&self, _sync_every_n_blocks: usize) -> StoreResult<()> {
        Ok(())
    }

    /// Flushes all pending writes to disk and ensures they are durable.
    ///
    /// This blocks until all in-flight persistence tasks complete and
    /// an fsync has been performed on the journal.
    fn flush(&self) -> StoreResult<()> {
        Ok(()) // Default no-op for implementations without persistence
    }
}
