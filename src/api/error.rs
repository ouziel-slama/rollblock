use std::ops::RangeInclusive;
use std::path::PathBuf;

use thiserror::Error;

use crate::net::ServerError;
use crate::types::BlockId;

pub type StoreResult<T> = Result<T, MhinStoreError>;

#[derive(Debug, Error)]
pub enum MhinStoreError {
    #[error("heed error: {0}")]
    Heed(#[from] heed::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("thread pool build error: {0}")]
    ThreadPoolBuild(#[from] rayon::ThreadPoolBuildError),

    #[error("metadata value '{0}' is missing")]
    MissingMetadata(&'static str),

    #[error("invalid block range {start}..={end}")]
    InvalidBlockRange { start: BlockId, end: BlockId },

    #[error("metadata range {0:?} is empty")]
    EmptyRange(RangeInclusive<BlockId>),

    #[error("invalid journal header: {reason}")]
    InvalidJournalHeader { reason: &'static str },

    #[error("journal checksum mismatch for block {block}")]
    JournalChecksumMismatch { block: BlockId },

    #[error("journal block height mismatch (expected {expected}, found {found})")]
    JournalBlockIdMismatch { expected: BlockId, found: BlockId },

    #[error("no shards configured in state engine")]
    NoShardsConfigured,

    #[error("block delta block height mismatch (expected {expected}, found {found})")]
    BlockDeltaMismatch { expected: BlockId, found: BlockId },

    #[error("invalid shard index {shard_index} (available shards: {shard_count})")]
    InvalidShardIndex {
        shard_index: usize,
        shard_count: usize,
    },

    #[error("block {current} is still in progress")]
    BlockInProgress { current: BlockId },

    #[error("no block in progress")]
    NoBlockInProgress,

    #[error("rollback target {target} is ahead of current block {current}")]
    RollbackTargetAhead { target: BlockId, current: BlockId },

    #[error("journal entry missing for block {block}")]
    MissingJournalEntry { block: BlockId },

    #[error("snapshot corrupted at {path:?}: {reason}")]
    SnapshotCorrupted { path: PathBuf, reason: String },

    #[error("block height {block_height} must be greater than current block {current}")]
    BlockIdNotIncreasing {
        block_height: BlockId,
        current: BlockId,
    },

    #[error(
        "block height {block_height} has no operations (rollback target adjusted to {adjusted})"
    )]
    RollbackToEmptyBlock {
        block_height: BlockId,
        adjusted: BlockId,
    },

    #[error("{lock} lock poisoned")]
    LockPoisoned { lock: &'static str },

    #[error("configuration mismatch: {field} (stored: {stored}, requested: {requested})")]
    ConfigurationMismatch {
        field: &'static str,
        stored: usize,
        requested: usize,
    },

    #[error("missing shard config field: {field}")]
    MissingShardConfig { field: &'static str },

    #[error("missing shard layout in metadata at {path:?}")]
    MissingShardLayout { path: PathBuf },

    #[error("remote server configuration missing; call `with_remote_server` before enabling")]
    RemoteServerConfigMissing,

    #[error(
        "remote server requires explicit Basic Auth credentials; override `RemoteServerSettings::with_basic_auth` (or `with_auth_config`) before enabling"
    )]
    RemoteServerCredentialsMissing,

    #[error("invalid configuration: {field} must be at least {min}, got {value}")]
    InvalidConfiguration {
        field: &'static str,
        min: usize,
        value: usize,
    },

    #[error("data directory locked at {path:?} (requested: {requested})")]
    DataDirLocked {
        path: PathBuf,
        requested: &'static str,
    },

    #[error("durability failure for block {block}: {reason}")]
    DurabilityFailure { block: BlockId, reason: String },

    #[error("remote server error: {0}")]
    RemoteServer(#[from] ServerError),

    #[error("remote server task failed: {reason}")]
    RemoteServerTaskFailure { reason: String },

    #[error("value payload of {actual} bytes exceeds the {max}-byte limit")]
    ValueTooLarge { actual: usize, max: usize },

    #[error("unsupported operation: {reason}")]
    UnsupportedOperation { reason: String },
}
