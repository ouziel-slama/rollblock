use std::ops::RangeInclusive;

use crate::error::StoreResult;
use crate::types::{BlockId, JournalMeta};

pub mod lmdb;

pub use lmdb::LmdbMetadataStore;

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct ShardLayout {
    pub shards_count: usize,
    pub initial_capacity: usize,
}

pub trait MetadataStore: Send + Sync {
    fn current_block(&self) -> StoreResult<BlockId>;
    fn set_current_block(&self, block: BlockId) -> StoreResult<()>;
    fn put_journal_offset(&self, block: BlockId, meta: &JournalMeta) -> StoreResult<()>;
    fn get_journal_offsets(&self, range: RangeInclusive<BlockId>) -> StoreResult<Vec<JournalMeta>>;
    fn last_journal_offset_at_or_before(&self, block: BlockId) -> StoreResult<Option<JournalMeta>>;
    fn remove_journal_offsets_after(&self, block: BlockId) -> StoreResult<()> {
        let _ = block;
        Ok(())
    }

    fn record_block_commit(&self, block: BlockId, meta: &JournalMeta) -> StoreResult<()> {
        self.put_journal_offset(block, meta)?;
        self.set_current_block(block)
    }
}
