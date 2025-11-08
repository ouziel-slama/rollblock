use serde::{Deserialize, Serialize};

/// Fixed-size 8-byte key used throughout the store.
///
/// Keys are typically derived from hashing larger identifiers.
pub type Key = [u8; 8];

/// 64-bit unsigned integer value stored for each key.
pub type Value = u64;

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
    /// The value to set.
    ///
    /// A value of zero indicates a delete.
    pub value: Value,
}

impl Operation {
    #[inline]
    pub fn is_delete(&self) -> bool {
        self.value == 0
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
        self.value == 0
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
