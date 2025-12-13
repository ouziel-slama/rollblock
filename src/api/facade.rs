//! High-level store facade APIs and supporting utilities.

mod block;
mod config;
mod core;
mod recovery;

#[cfg(test)]
mod tests;

use crate::error::{StoreError, StoreResult};
use crate::types::{BlockId, Operation, StoreKey as Key, Value};

pub use block::BlockStoreFacade;
pub use config::{RemoteServerSettings, StoreConfig};
pub use core::SimpleStoreFacade;

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
    ///   (the genesis block `0` is allowed when initializing a brand-new store)
    /// * `operations` - Vector of set operations (`value.is_delete()` removes the key)
    ///
    /// # Errors
    ///
    /// Returns `BlockIdNotIncreasing` if `block_height <= current_block`
    /// (unless applying the genesis block to an empty store)
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
    /// * `key` - The fixed-width key to look up (compile-time width)
    ///
    /// # Returns
    ///
    /// * `Ok(value)` where `!value.is_delete()` if the key exists
    /// * `Ok(Value::empty())` if the key does not exist
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let key = Key::from([1, 2, 3, 4, 5, 6, 7, 8]);
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

    /// Removes a key in a single write and returns its previous value.
    ///
    /// This behaves like a combined `get` + `set(Value::empty())`, but runs
    /// under one orchestration lock so callers avoid the extra lookup and
    /// serialization.
    ///
    /// # Returns
    ///
    /// * `Ok(value)` containing the removed bytes if the key existed.
    /// * `Ok(Value::empty())` if the key was missing.
    fn pop(&self, _block_height: BlockId, _key: Key) -> StoreResult<Value> {
        pop_not_supported()
    }

    /// Enables relaxed durability by syncing every N blocks instead of every block.
    ///
    /// This improves throughput during long catch-up phases at the cost of a larger
    /// potential data-loss window if the process crashes.
    fn enable_relaxed_mode(&self, _sync_every_n_blocks: usize) -> StoreResult<()> {
        relaxed_mode_not_supported()
    }

    /// Returns `true` when relaxed durability is currently enabled.
    fn relaxed_mode_enabled(&self) -> bool {
        false
    }

    /// Disables relaxed durability and forces pending writes to become durable immediately.
    fn disable_relaxed_mode(&self) -> StoreResult<()> {
        relaxed_mode_not_supported()
    }

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

fn relaxed_mode_not_supported() -> StoreResult<()> {
    Err(StoreError::UnsupportedOperation {
        reason: "relaxed durability mode is not supported by this StoreFacade".to_string(),
    })
}

fn pop_not_supported() -> StoreResult<Value> {
    Err(StoreError::UnsupportedOperation {
        reason: "pop is not supported by this StoreFacade".to_string(),
    })
}
