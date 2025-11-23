use std::cmp::Ordering;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};

use crate::api::error::MhinStoreError;

/// Fixed-size 8-byte key used throughout the store.
///
/// Keys are typically derived from hashing larger identifiers.
pub type Key = [u8; 8];

/// Maximum number of bytes allowed per value payload.
pub const MAX_VALUE_BYTES: usize = 65_535;

/// Owned value payload stored for each key.
///
/// Empty values represent deletes across the stack.
#[derive(Clone, Serialize, PartialEq, Eq, Default)]
pub struct Value(Vec<u8>);

impl Value {
    #[inline]
    fn ensure_len_within_limit(len: usize) -> Result<(), MhinStoreError> {
        if len > MAX_VALUE_BYTES {
            Err(MhinStoreError::ValueTooLarge {
                actual: len,
                max: MAX_VALUE_BYTES,
            })
        } else {
            Ok(())
        }
    }

    /// Creates a value from owned bytes while validating the payload length.
    pub fn try_from_vec(bytes: Vec<u8>) -> Result<Self, MhinStoreError> {
        Self::ensure_len_within_limit(bytes.len())?;
        Ok(Self(bytes))
    }

    /// Clones the provided slice into a new value while validating the length.
    pub fn try_from_slice(bytes: &[u8]) -> Result<Self, MhinStoreError> {
        Self::ensure_len_within_limit(bytes.len())?;
        Ok(Self(bytes.to_vec()))
    }

    /// Ensures the current payload stays within the configured limit.
    pub fn ensure_within_limit(&self) -> Result<(), MhinStoreError> {
        Self::ensure_len_within_limit(self.len())
    }

    /// Creates a value from owned bytes.
    ///
    /// # Panics
    ///
    /// Panics if `bytes.len()` exceeds [`MAX_VALUE_BYTES`]. Use
    /// [`Value::try_from_vec`] to handle validation errors explicitly.
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self::try_from_vec(bytes).expect("value exceeds MAX_VALUE_BYTES; use try_from_vec")
    }

    /// Clones the provided slice into a new value.
    ///
    /// # Panics
    ///
    /// Panics if `bytes.len()` exceeds [`MAX_VALUE_BYTES`]. Use
    /// [`Value::try_from_slice`] to handle validation errors explicitly.
    pub fn from_slice(bytes: &[u8]) -> Self {
        Self::try_from_slice(bytes).expect("value exceeds MAX_VALUE_BYTES; use try_from_slice")
    }

    /// Creates an empty (delete) marker.
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    /// Returns true when the payload has zero bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns true when this value represents a delete.
    #[inline]
    pub fn is_delete(&self) -> bool {
        self.is_empty()
    }

    /// Borrows the raw bytes.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the value and returns the owned bytes.
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }

    /// Number of bytes contained in this value.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true when the payload is non-empty.
    pub fn is_set(&self) -> bool {
        !self.is_delete()
    }

    /// Interprets the value as a little-endian `u64` if it fits.
    pub fn to_u64(&self) -> Option<u64> {
        if self.len() > 8 {
            return None;
        }
        let mut buf = [0u8; 8];
        buf[..self.len()].copy_from_slice(self.as_slice());
        Some(u64::from_le_bytes(buf))
    }

    /// Temporary helper for serialization codepaths that still expect fixed-width values.
    ///
    /// This will assert if the value exceeds 8 bytes; future storage formats should
    /// be updated to handle variable-length payloads natively.
    pub fn to_le_bytes(&self) -> [u8; 8] {
        assert!(
            self.len() <= 8,
            "fixed-width serialization only supports up to 8 bytes (got {})",
            self.len()
        );
        let mut bytes = [0u8; 8];
        bytes[..self.len()].copy_from_slice(&self.0);
        bytes
    }

    /// Builds a value from a fixed-width little-endian representation.
    pub fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Value::from_vec(bytes.to_vec())
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Value").field(&self.0).finish()
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(num) = self.to_u64() {
            write!(f, "{num}")
        } else if self.is_delete() {
            write!(f, "0")
        } else {
            write!(f, "0x")?;
            for byte in self.as_slice() {
                write!(f, "{:02x}", byte)?;
            }
            Ok(())
        }
    }
}

impl Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::from_vec(value)
    }
}

impl From<&[u8]> for Value {
    fn from(slice: &[u8]) -> Self {
        Value::from_slice(slice)
    }
}

impl From<u64> for Value {
    fn from(number: u64) -> Self {
        Value::from_vec(number.to_le_bytes().to_vec())
    }
}

impl From<Value> for Vec<u8> {
    fn from(value: Value) -> Self {
        value.into_inner()
    }
}

impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq<u64> for Value {
    fn eq(&self, other: &u64) -> bool {
        self.to_u64() == Some(*other)
    }
}

impl PartialOrd<u64> for Value {
    fn partial_cmp(&self, other: &u64) -> Option<Ordering> {
        self.to_u64().and_then(|value| value.partial_cmp(other))
    }
}

/// Immutable shared buffer used by shards/state to avoid cloning.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct ValueBuf(Arc<[u8]>);

impl ValueBuf {
    pub fn from_arc(bytes: Arc<[u8]>) -> Self {
        Self(bytes)
    }

    pub fn from_slice(bytes: &[u8]) -> Self {
        Self(Arc::<[u8]>::from(bytes))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn is_delete(&self) -> bool {
        self.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_from_vec_rejects_oversized_payloads() {
        let oversized = vec![0u8; MAX_VALUE_BYTES + 1];
        let err = Value::try_from_vec(oversized).unwrap_err();
        match err {
            MhinStoreError::ValueTooLarge { actual, max } => {
                assert_eq!(actual, MAX_VALUE_BYTES + 1);
                assert_eq!(max, MAX_VALUE_BYTES);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

impl fmt::Debug for ValueBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ValueBuf").field(&self.0).finish()
    }
}

impl From<Value> for ValueBuf {
    fn from(value: Value) -> Self {
        let bytes: Vec<u8> = value.into();
        ValueBuf(Arc::from(bytes))
    }
}

impl From<&Value> for ValueBuf {
    fn from(value: &Value) -> Self {
        ValueBuf::from_slice(value.as_slice())
    }
}

impl From<ValueBuf> for Value {
    fn from(buf: ValueBuf) -> Self {
        Value::from_vec(buf.as_slice().to_vec())
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        Value::try_from_vec(bytes).map_err(de::Error::custom)
    }
}

impl AsRef<[u8]> for ValueBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

/// Block identifier used to track state versions.
///
/// Block heights must be monotonically increasing.
pub type BlockId = u64;

/// A user-requested operation on the store.
///
/// Operations are batched and applied atomically per block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    /// The key to operate on
    pub key: Key,
    /// The value to set. Empty values delete the key.
    pub value: Value,
}

impl Operation {
    #[inline]
    pub fn is_delete(&self) -> bool {
        self.value.is_delete()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardOp {
    pub key: Key,
    pub value: Value,
}

impl ShardOp {
    #[inline]
    pub fn is_delete(&self) -> bool {
        self.value.is_delete()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UndoEntry {
    pub key: Key,
    pub previous: Option<Value>,
    pub op: UndoOp,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum UndoOp {
    Inserted,
    Updated,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShardUndo {
    pub shard_index: usize,
    pub entries: Vec<UndoEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShardDelta {
    pub shard_index: usize,
    pub operations: Vec<ShardOp>,
    pub undo_entries: Vec<UndoEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BlockDelta {
    pub block_height: BlockId,
    pub shards: Vec<ShardDelta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BlockUndo {
    pub block_height: BlockId,
    pub shard_undos: Vec<ShardUndo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StateStats {
    pub operation_count: usize,
    pub modified_keys: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShardStats {
    pub keys: usize,
    pub tombstones: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JournalMeta {
    pub block_height: BlockId,
    pub offset: u64,
    pub compressed_len: u64,
    pub checksum: u32,
}
