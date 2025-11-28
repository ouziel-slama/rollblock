use std::ops::RangeInclusive;
use std::path::PathBuf;

use crate::error::StoreResult;
use crate::storage::journal::{ChunkDeletionPlan, JournalPruneReport};
use crate::types::{BlockId, JournalMeta};

pub mod lmdb;

pub use lmdb::LmdbMetadataStore;

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct ShardLayout {
    pub shards_count: usize,
    pub initial_capacity: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GcWatermark {
    pub pruned_through: BlockId,
    pub chunk_plan: ChunkDeletionPlan,
    pub staged_index_path: PathBuf,
    pub baseline_entry_count: usize,
    pub report: JournalPruneReport,
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
    fn prune_journal_offsets_at_or_before(&self, block: BlockId) -> StoreResult<usize> {
        let _ = block;
        Ok(0)
    }
    fn load_gc_watermark(&self) -> StoreResult<Option<GcWatermark>> {
        Ok(None)
    }
    fn store_gc_watermark(&self, _watermark: &GcWatermark) -> StoreResult<()> {
        Ok(())
    }
    fn clear_gc_watermark(&self) -> StoreResult<()> {
        Ok(())
    }
    fn load_snapshot_watermark(&self) -> StoreResult<Option<BlockId>> {
        Ok(None)
    }
    fn store_snapshot_watermark(&self, _block: BlockId) -> StoreResult<()> {
        Ok(())
    }

    fn record_block_commit(&self, block: BlockId, meta: &JournalMeta) -> StoreResult<()> {
        self.put_journal_offset(block, meta)?;
        self.set_current_block(block)
    }

    fn record_block_commits(&self, entries: &[(BlockId, JournalMeta)]) -> StoreResult<()> {
        for (block, meta) in entries {
            self.record_block_commit(*block, meta)?;
        }
        Ok(())
    }
}
