//! High-level store facade APIs and supporting utilities.

mod block;
mod config;
mod core;
mod recovery;

#[cfg(test)]
mod tests;

use crate::error::StoreResult;
use crate::types::{BlockId, Key, Operation, Value};

pub use block::MhinStoreBlockFacade;
pub use config::{RemoteServerSettings, StoreConfig};
pub use core::MhinStoreFacade;

/// Main interface for interacting with the state store.
///
/// This trait provides high-level operations for managing blockchain state
/// with rollback support.
pub trait StoreFacade: Send + Sync {
    /// Applies a batch of set/delete operations at the specified block height.
    ///
    /// # Arguments
    ///
    /// * `block_height` - Must be strictly greater than the current block height
    /// * `operations` - Vector of set operations (`value.is_delete()` removes the key)
    ///
    /// # Errors
    ///
    /// Returns `BlockIdNotIncreasing` if `block_height <= current_block` (when auto-rollback is disabled)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use rollblock::types::Operation;
    /// let op = Operation {
    ///     key: [1, 2, 3, 4, 5, 6, 7, 8],
    ///     value: 42.into(),
    /// };
    /// store.set(1, vec![op])?;
    /// ```
    fn set(&self, block_height: BlockId, operations: Vec<Operation>) -> StoreResult<()>;

    /// Rolls back the store state to the specified block height.
    ///
    /// # Arguments
    ///
    /// * `target` - The block height to rollback to (must be <= current block)
    ///
    /// # Errors
    ///
    /// Returns `RollbackTargetAhead` if target > current block
    ///
    /// # Examples
    ///
    /// ```ignore
    /// store.rollback(5)?; // Rollback to block 5
    /// ```
    fn rollback(&self, target: BlockId) -> StoreResult<()>;

    /// Retrieves the value associated with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The 8-byte key to look up
    ///
    /// # Returns
    ///
    /// * `Ok(value)` where `!value.is_delete()` if the key exists
    /// * `Ok(Value::empty())` if the key does not exist
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let key = [1, 2, 3, 4, 5, 6, 7, 8];
    /// let value = store.get(key)?;
    /// if value.is_delete() {
    ///     println!("Key not found");
    /// } else {
    ///     println!("Value bytes: {:?}", value.as_slice());
    /// }
    /// ```
    fn get(&self, key: Key) -> StoreResult<Value>;

    /// Retrieves multiple values in a single call.
    ///
    /// Keys are returned in the same order they were provided. Missing keys
    /// are represented by `Value::empty()` in the returned vector.
    fn multi_get(&self, keys: &[Key]) -> StoreResult<Vec<Value>>;

    /// Flushes in-memory state and closes the store gracefully.
    fn close(&self) -> StoreResult<()>;

    /// Returns the current committed block height.
    fn current_block(&self) -> StoreResult<BlockId>;

    /// Returns the highest block that has been applied in memory.
    fn applied_block(&self) -> StoreResult<BlockId>;

    /// Returns the highest block that has been durably persisted.
    fn durable_block(&self) -> StoreResult<BlockId>;

    /// Verifies that the store has not recorded a fatal durability error.
    ///
    /// Useful for long idle periods between operations when using asynchronous durability.
    fn ensure_healthy(&self) -> StoreResult<()>;
}
