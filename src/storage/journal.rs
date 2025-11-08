use crate::types::{BlockId, BlockUndo, JournalMeta, Operation};

use crate::error::StoreResult;
use serde::{Deserialize, Serialize};

pub mod file;
pub mod format;
pub mod iter;
pub mod maintenance;

pub use file::FileBlockJournal;
pub use iter::JournalIter;

pub trait BlockJournal: Send + Sync {
    fn append(
        &self,
        block: BlockId,
        undo: &BlockUndo,
        operations: &[Operation],
    ) -> StoreResult<JournalMeta>;
    fn iter_backwards(&self, from: BlockId, to: BlockId) -> StoreResult<JournalIter>;
    fn read_entry(&self, meta: &JournalMeta) -> StoreResult<JournalBlock>;
    fn list_entries(&self) -> StoreResult<Vec<JournalMeta>>;
    fn truncate_after(&self, block: BlockId) -> StoreResult<()>;
    fn rewrite_index(&self, metas: &[JournalMeta]) -> StoreResult<()> {
        let _ = metas;
        Ok(())
    }
    fn scan_entries(&self) -> StoreResult<Vec<JournalMeta>> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Clone)]
pub struct JournalOptions {
    pub compress: bool,
    pub compression_level: i32,
}

impl Default for JournalOptions {
    fn default() -> Self {
        Self {
            compress: true,
            compression_level: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalBlock {
    pub block_height: BlockId,
    pub operations: Vec<Operation>,
    pub undo: BlockUndo,
}
