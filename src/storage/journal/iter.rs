use std::fs::File;

use crate::error::StoreResult;
use crate::types::JournalMeta;

use super::format::read_journal_block;
use super::JournalBlock;

pub struct JournalIter {
    pub(crate) file: File,
    pub(crate) metas: Vec<JournalMeta>,
    position: usize,
}

impl JournalIter {
    pub(crate) fn new(file: File, metas: Vec<JournalMeta>) -> Self {
        Self {
            file,
            metas,
            position: 0,
        }
    }

    pub fn next_entry(&mut self) -> Option<StoreResult<JournalBlock>> {
        if self.position >= self.metas.len() {
            return None;
        }

        let meta = self.metas[self.position].clone();
        self.position += 1;

        Some(read_journal_block(&mut self.file, &meta))
    }
}
