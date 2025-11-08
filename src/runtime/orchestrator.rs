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
mod read_only;

pub use default::DefaultBlockOrchestrator;
pub use durability::{DurabilityMode, PersistenceSettings};
pub use persistence::PersistenceContext;
pub use read_only::ReadOnlyBlockOrchestrator;

use crate::error::StoreResult;
use crate::metrics::StoreMetrics;
use crate::types::{BlockId, Key, Operation, Value};

/// API surfaced by orchestrator implementations.
pub trait BlockOrchestrator: Send + Sync {
    fn apply_operations(&self, block_height: BlockId, ops: Vec<Operation>) -> StoreResult<()>;
    fn revert_to(&self, block: BlockId) -> StoreResult<()>;
    fn fetch(&self, key: Key) -> StoreResult<Value>;
    fn metrics(&self) -> Option<&StoreMetrics>;
    fn current_block(&self) -> StoreResult<BlockId>;
    fn applied_block_height(&self) -> BlockId;
    fn durable_block_height(&self) -> StoreResult<BlockId>;
    fn shutdown(&self) -> StoreResult<()>;
    fn ensure_healthy(&self) -> StoreResult<()>;
}
