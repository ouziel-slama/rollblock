use std::fs::File;
use std::path::PathBuf;

use crate::error::StoreResult;
use crate::types::JournalMeta;

use super::chunk::chunk_file_path;
use super::format::read_journal_block;
use super::JournalBlock;

pub struct JournalIter {
    pub(crate) metas: Vec<JournalMeta>,
    position: usize,
    chunk_dir: PathBuf,
    current_file: Option<(u32, File)>,
}

impl JournalIter {
    pub(crate) fn new(chunk_dir: PathBuf, metas: Vec<JournalMeta>) -> Self {
        Self {
            metas,
            position: 0,
            chunk_dir,
            current_file: None,
        }
    }

    pub fn next_entry(&mut self) -> Option<StoreResult<JournalBlock>> {
        if self.position >= self.metas.len() {
            return None;
        }

        let meta = self.metas[self.position].clone();
        self.position += 1;

        match self.file_for_chunk(meta.chunk_id) {
            Ok(file) => Some(read_journal_block(file, &meta)),
            Err(err) => Some(Err(err)),
        }
    }

    fn file_for_chunk(&mut self, chunk_id: u32) -> StoreResult<&mut File> {
        let needs_open = self
            .current_file
            .as_ref()
            .map(|(open_id, _)| *open_id != chunk_id)
            .unwrap_or(true);

        if needs_open {
            let path = chunk_file_path(&self.chunk_dir, chunk_id);
            let file = File::open(&path)?;
            self.current_file = Some((chunk_id, file));
        }

        Ok(self
            .current_file
            .as_mut()
            .map(|(_, file)| file)
            .expect("chunk file opened above"))
    }
}
