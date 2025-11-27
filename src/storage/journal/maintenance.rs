use std::fs::OpenOptions;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{MhinStoreError, StoreResult};
use crate::storage::fs::sync_directory;
use crate::types::{BlockId, JournalMeta};

use super::chunk::{chunk_file_path, enumerate_chunk_files};
use super::format::{read_journal_block, JournalHeader, JOURNAL_HEADER_SIZE};

struct ChunkScanOutcome {
    metas: Vec<JournalMeta>,
    last_good_end: u64,
}

pub(crate) fn repair_chunk_tail(path: &Path, chunk_id: u32) -> StoreResult<u64> {
    let outcome = scan_chunk_file(path, chunk_id, false)?;
    Ok(outcome.last_good_end)
}

pub(crate) fn truncate_after(
    chunk_dir: &Path,
    index_path: &Path,
    block: BlockId,
    metas: Vec<JournalMeta>,
) -> StoreResult<()> {
    let retained: Vec<JournalMeta> = metas
        .into_iter()
        .filter(|meta| meta.block_height <= block)
        .collect();

    if retained.is_empty() {
        for (_, path) in enumerate_chunk_files(chunk_dir)? {
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
        }
        if chunk_dir.exists() {
            sync_directory(chunk_dir)?;
        }
        return rewrite_index(index_path, &retained);
    }

    let last = retained.last().expect("retained is not empty");
    let new_len = last
        .chunk_offset
        .saturating_add(JOURNAL_HEADER_SIZE as u64)
        .saturating_add(last.compressed_len);
    let target_chunk = chunk_file_path(chunk_dir, last.chunk_id);

    if target_chunk.exists() {
        let chunk = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&target_chunk)?;
        chunk.set_len(new_len)?;
        chunk.sync_all()?;
    }

    for (chunk_id, path) in enumerate_chunk_files(chunk_dir)? {
        if chunk_id > last.chunk_id && path.exists() {
            std::fs::remove_file(&path)?;
        }
    }

    if chunk_dir.exists() {
        sync_directory(chunk_dir)?;
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

pub(crate) fn scan_entries(chunk_dir: &Path) -> StoreResult<Vec<JournalMeta>> {
    if !chunk_dir.exists() {
        return Ok(Vec::new());
    }

    let mut metas = Vec::new();
    for (chunk_id, path) in enumerate_chunk_files(chunk_dir)? {
        match scan_chunk_file(&path, chunk_id, true) {
            Ok(outcome) => metas.extend(outcome.metas),
            Err(err @ MhinStoreError::JournalChecksumMismatch { .. }) => return Err(err),
            Err(err) => {
                tracing::warn!(
                    ?path,
                    chunk_id,
                    ?err,
                    "Failed to scan journal chunk; skipping file"
                );
                continue;
            }
        }
    }

    Ok(metas)
}

fn scan_chunk_file(
    path: &Path,
    chunk_id: u32,
    collect_entries: bool,
) -> StoreResult<ChunkScanOutcome> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let original_len = file.metadata()?.len();
    let mut offset = 0u64;
    let mut last_good_end = 0u64;
    let mut encountered_tail_error = false;
    let mut metas = Vec::new();

    while offset < original_len {
        file.seek(SeekFrom::Start(offset))?;

        let mut header_bytes = [0u8; JOURNAL_HEADER_SIZE];
        match file.read_exact(&mut header_bytes) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                tracing::warn!(
                    chunk_id,
                    offset,
                    "Detected truncated journal header while scanning chunk; stopping chunk scan"
                );
                encountered_tail_error = true;
                break;
            }
            Err(err) => return Err(err.into()),
        }

        let header = match JournalHeader::from_bytes(&header_bytes) {
            Ok(header) => header,
            Err(err) => {
                tracing::warn!(
                    chunk_id,
                    offset,
                    ?err,
                    "Encountered invalid journal header while scanning chunk; stopping chunk scan"
                );
                encountered_tail_error = true;
                break;
            }
        };

        let meta = JournalMeta {
            block_height: header.block_height,
            chunk_id,
            chunk_offset: offset,
            compressed_len: header.compressed_len,
            checksum: header.checksum,
        };

        match read_journal_block(&mut file, &meta) {
            Ok(_) => {
                if collect_entries {
                    metas.push(meta);
                }
                offset = file.stream_position()?;
                last_good_end = offset;
            }
            Err(err @ MhinStoreError::JournalChecksumMismatch { .. }) => {
                tracing::error!(
                    chunk_id,
                    offset,
                    block_height = meta.block_height,
                    ?err,
                    "Detected journal checksum mismatch; aborting chunk scan"
                );
                return Err(err);
            }
            Err(MhinStoreError::Io(inner)) if inner.kind() == ErrorKind::UnexpectedEof => {
                tracing::warn!(
                    chunk_id,
                    offset,
                    block_height = meta.block_height,
                    "Detected truncated journal payload while scanning chunk; stopping chunk scan"
                );
                encountered_tail_error = true;
                break;
            }
            Err(err) => {
                tracing::warn!(
                    chunk_id,
                    offset,
                    block_height = meta.block_height,
                    ?err,
                    "Failed to validate journal entry while scanning chunk; stopping chunk scan"
                );
                encountered_tail_error = true;
                break;
            }
        }
    }

    if encountered_tail_error && last_good_end < original_len {
        file.set_len(last_good_end)?;
        file.sync_all()?;
        tracing::warn!(
            chunk_id,
            truncated_from = original_len,
            truncated_to = last_good_end,
            "Truncated corrupt journal chunk tail to last durable entry"
        );
    }

    Ok(ChunkScanOutcome {
        metas,
        last_good_end,
    })
}
