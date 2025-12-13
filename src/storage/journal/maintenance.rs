use std::fs::OpenOptions;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{StoreError, StoreResult};
use crate::storage::fs::sync_directory;
use crate::types::{BlockId, JournalMeta};

use super::chunk::{chunk_file_path, enumerate_chunk_files};
use super::format::{read_journal_block, JournalHeader, JOURNAL_HEADER_SIZE};

struct ChunkScanOutcome {
    metas: Vec<JournalMeta>,
    last_good_end: u64,
}

pub(crate) fn repair_chunk_tail(path: &Path, chunk_id: u32, key_bytes: usize) -> StoreResult<u64> {
    let outcome = scan_chunk_file(path, chunk_id, false, key_bytes)?;
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

pub(crate) fn scan_entries(chunk_dir: &Path, key_bytes: usize) -> StoreResult<Vec<JournalMeta>> {
    if !chunk_dir.exists() {
        return Ok(Vec::new());
    }

    let mut metas = Vec::new();
    for (chunk_id, path) in enumerate_chunk_files(chunk_dir)? {
        match scan_chunk_file(&path, chunk_id, true, key_bytes) {
            Ok(outcome) => metas.extend(outcome.metas),
            Err(err @ StoreError::JournalChecksumMismatch { .. }) => return Err(err),
            Err(err) if is_compatibility_error(&err) => return Err(err),
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
    key_bytes: usize,
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
            Err(err) if is_compatibility_error(&err) => {
                tracing::error!(
                    chunk_id,
                    offset,
                    ?err,
                    "Encountered incompatible journal header; aborting chunk scan"
                );
                return Err(err);
            }
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

        // If the header claims a payload that extends past the recorded chunk
        // length, treat it as a corrupted tail instead of a compatibility error.
        let claimed_end = offset
            .saturating_add(JOURNAL_HEADER_SIZE as u64)
            .saturating_add(meta.compressed_len);
        if claimed_end > original_len {
            tracing::warn!(
                chunk_id,
                offset,
                block_height = meta.block_height,
                compressed_len = meta.compressed_len,
                original_len,
                claimed_end,
                "Detected journal entry exceeding chunk length; stopping chunk scan"
            );
            encountered_tail_error = true;
            break;
        }

        match read_journal_block(&mut file, &meta, key_bytes) {
            Ok(_) => {
                if collect_entries {
                    metas.push(meta);
                }
                offset = file.stream_position()?;
                last_good_end = offset;
            }
            Err(err @ StoreError::JournalChecksumMismatch { .. }) => {
                tracing::error!(
                    chunk_id,
                    offset,
                    block_height = meta.block_height,
                    ?err,
                    "Detected journal checksum mismatch; aborting chunk scan"
                );
                return Err(err);
            }
            Err(err) if is_compatibility_error(&err) => {
                tracing::error!(
                    chunk_id,
                    offset,
                    block_height = meta.block_height,
                    ?err,
                    "Detected incompatible journal entry; aborting chunk scan"
                );
                return Err(err);
            }
            Err(StoreError::Io(inner)) if inner.kind() == ErrorKind::UnexpectedEof => {
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

fn is_compatibility_error(err: &StoreError) -> bool {
    matches!(err, StoreError::ConfigurationMismatch { .. })
        || matches!(
            err,
            StoreError::InvalidJournalHeader {
                reason: "unsupported version"
            } | StoreError::InvalidJournalHeader {
                reason: "invalid magic"
            } | StoreError::InvalidJournalHeader {
                reason: "key width below minimum"
            }
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::journal::chunk::chunk_file_path;
    use crate::storage::journal::format::{
        checksum_to_u32, serialize_journal_block, JournalHeader, JOURNAL_FLAG_UNCOMPRESSED,
    };
    use crate::types::{BlockUndo, Operation, StoreKey, Value};
    use std::io::Write;
    use tempfile::tempdir_in;

    #[test]
    fn scan_entries_rejects_mid_chunk_key_width_mismatch() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let chunk_path = chunk_file_path(tmp.path(), 0);

        // First entry with expected key width
        let block_height = 1;
        let operations = vec![Operation {
            key: StoreKey::from([0xAA; StoreKey::BYTES]),
            value: Value::from_slice(b"ok"),
        }];
        let undo = BlockUndo {
            block_height,
            shard_undos: Vec::new(),
        };
        let (payload, entry_count) =
            serialize_journal_block(block_height, &undo, &operations).unwrap();
        let checksum = checksum_to_u32(blake3::hash(&payload));
        let header = JournalHeader::new(
            block_height,
            entry_count,
            payload.len() as u64,
            payload.len() as u64,
            checksum,
            JOURNAL_FLAG_UNCOMPRESSED,
            StoreKey::BYTES,
        );

        // Second entry with mismatched key width to simulate incompatible writer
        let bad_block_height = 2;
        let bad_operations = vec![Operation {
            key: StoreKey::from([0xBB; StoreKey::BYTES]),
            value: Value::from_slice(b"bad"),
        }];
        let bad_undo = BlockUndo {
            block_height: bad_block_height,
            shard_undos: Vec::new(),
        };
        let (bad_payload, bad_entry_count) =
            serialize_journal_block(bad_block_height, &bad_undo, &bad_operations).unwrap();
        let bad_checksum = checksum_to_u32(blake3::hash(&bad_payload));
        let bad_header = JournalHeader::new(
            bad_block_height,
            bad_entry_count,
            bad_payload.len() as u64,
            bad_payload.len() as u64,
            bad_checksum,
            JOURNAL_FLAG_UNCOMPRESSED,
            StoreKey::BYTES + 1, // mismatch
        );

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&chunk_path)
            .unwrap();
        file.write_all(&header.to_bytes()).unwrap();
        file.write_all(&payload).unwrap();
        file.write_all(&bad_header.to_bytes()).unwrap();
        file.write_all(&bad_payload).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let err = scan_entries(tmp.path(), StoreKey::BYTES).expect_err("should fail fast");
        match err {
            StoreError::ConfigurationMismatch {
                field,
                stored,
                requested,
            } => {
                assert_eq!(field, "key_bytes");
                assert_eq!(stored, StoreKey::BYTES + 1);
                assert_eq!(requested, StoreKey::BYTES);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
