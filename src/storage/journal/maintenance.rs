use std::fs::OpenOptions;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{MhinStoreError, StoreResult};
use crate::storage::fs::sync_directory;
use crate::types::{BlockId, JournalMeta};

use super::format::{read_journal_block, JournalHeader, JOURNAL_HEADER_SIZE};

pub(crate) fn truncate_after(
    journal_path: &Path,
    index_path: &Path,
    block: BlockId,
    metas: Vec<JournalMeta>,
) -> StoreResult<()> {
    let retained: Vec<JournalMeta> = metas
        .into_iter()
        .filter(|meta| meta.block_height <= block)
        .collect();

    let new_len = if let Some(last) = retained.last() {
        last.offset
            .saturating_add(JOURNAL_HEADER_SIZE as u64)
            .saturating_add(last.compressed_len)
    } else {
        0
    };

    let journal = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(journal_path)?;
    journal.set_len(new_len)?;
    journal.sync_all()?;

    if let Some(parent) = journal_path.parent() {
        sync_directory(parent)?;
    }

    rewrite_index(index_path, &retained)
}

pub(crate) fn rewrite_index(index_path: &Path, metas: &[JournalMeta]) -> StoreResult<()> {
    let temp_path = index_path.with_extension("idx.tmp");

    if temp_path.exists() {
        std::fs::remove_file(&temp_path)?;
    }

    let mut index_tmp = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temp_path)?;

    for meta in metas {
        let bytes = bincode::serialize(meta)?;
        index_tmp.write_all(&bytes)?;
    }

    index_tmp.sync_all()?;
    drop(index_tmp);

    if index_path.exists() {
        std::fs::remove_file(index_path)?;
    }

    std::fs::rename(&temp_path, index_path)?;

    if let Some(parent) = index_path.parent() {
        sync_directory(parent)?;
    }

    Ok(())
}

pub(crate) fn scan_entries(journal_path: &Path) -> StoreResult<Vec<JournalMeta>> {
    if !journal_path.exists() {
        return Ok(Vec::new());
    }

    let mut file = OpenOptions::new().read(true).open(journal_path)?;
    let mut metas = Vec::new();
    let mut offset = 0u64;

    loop {
        file.seek(SeekFrom::Start(offset))?;

        let mut header_bytes = [0u8; JOURNAL_HEADER_SIZE];
        match file.read_exact(&mut header_bytes) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }

        let header = match JournalHeader::from_bytes(&header_bytes) {
            Ok(header) => header,
            Err(err) => {
                tracing::warn!(
                    offset,
                    ?err,
                    "Encountered invalid journal header while scanning; stopping recovery"
                );
                break;
            }
        };

        let meta = JournalMeta {
            block_height: header.block_height,
            offset,
            compressed_len: header.compressed_len,
            checksum: header.checksum,
        };

        match read_journal_block(&mut file, &meta) {
            Ok(_) => {
                metas.push(meta);
                offset = file.stream_position()?;
            }
            Err(MhinStoreError::Io(inner)) if inner.kind() == ErrorKind::UnexpectedEof => {
                tracing::warn!(
                    offset,
                    block_height = meta.block_height,
                    "Detected truncated journal payload while scanning; stopping recovery"
                );
                break;
            }
            Err(err) => {
                tracing::warn!(
                    offset,
                    block_height = meta.block_height,
                    ?err,
                    "Failed to validate journal entry while scanning; stopping recovery"
                );
                break;
            }
        }
    }

    Ok(metas)
}
