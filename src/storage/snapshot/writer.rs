use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{StoreError, StoreResult};
use crate::state_shard::StateShard;
use crate::storage::fs::sync_directory;
use crate::types::{BlockId, StoreKey as Key, ValueBuf, MAX_VALUE_BYTES};

use super::format::{checksum_from_reader, encode_header, SNAPSHOT_HEADER_SIZE};
use super::gc::snapshot_path;

pub(super) fn write_snapshot(
    root_dir: &Path,
    block: BlockId,
    shards: &[Arc<dyn StateShard>],
) -> StoreResult<PathBuf> {
    let snapshot_path = snapshot_path(root_dir, block);
    let tmp_path = snapshot_path.with_extension("tmp");
    let shard_count = shards.len() as u64;

    if tmp_path.exists() {
        std::fs::remove_file(&tmp_path)?;
    }

    let header = encode_header(block, shard_count, Key::BYTES);

    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;

    let mut writer = BufWriter::new(file);
    writer.write_all(&header)?;

    if shard_count > 0 {
        writer.write_all(&vec![0u8; (shard_count as usize) * 8])?;
    }

    let mut offsets = Vec::with_capacity(shards.len());
    let mut counts = Vec::with_capacity(shards.len());
    let mut current_offset =
        SNAPSHOT_HEADER_SIZE as u64 + shard_count * std::mem::size_of::<u64>() as u64;

    for shard in shards {
        offsets.push(current_offset);
        writer.write_all(&0u64.to_le_bytes())?;

        let mut count: u64 = 0;
        let mut section_bytes: u64 = 8; // entry count prefix
        let mut write_result: StoreResult<()> = Ok(());

        {
            let mut sink = |key: Key, value: ValueBuf| {
                if write_result.is_err() {
                    return;
                }

                if let Err(err) = writer.write_all(&key) {
                    write_result = Err(err.into());
                    return;
                }

                let value_bytes = value.as_slice();
                if value_bytes.len() > MAX_VALUE_BYTES {
                    write_result = Err(StoreError::SnapshotCorrupted {
                        path: tmp_path.clone(),
                        reason: format!(
                            "value length {} exceeds MAX_VALUE_BYTES",
                            value_bytes.len()
                        ),
                    });
                    return;
                }

                let len_bytes = (value_bytes.len() as u16).to_le_bytes();
                if let Err(err) = writer.write_all(&len_bytes) {
                    write_result = Err(err.into());
                    return;
                }

                if let Err(err) = writer.write_all(value_bytes) {
                    write_result = Err(err.into());
                    return;
                }

                let overflow_error = || StoreError::SnapshotCorrupted {
                    path: tmp_path.clone(),
                    reason: "snapshot size overflow while writing shard data".to_string(),
                };

                let entry_bytes =
                    match (Key::BYTES as u64 + 2u64).checked_add(value_bytes.len() as u64) {
                        Some(len) => len,
                        None => {
                            write_result = Err(overflow_error());
                            return;
                        }
                    };

                section_bytes = match section_bytes.checked_add(entry_bytes) {
                    Some(total) => total,
                    None => {
                        write_result = Err(overflow_error());
                        return;
                    }
                };

                match count.checked_add(1) {
                    Some(next) => {
                        count = next;
                    }
                    None => {
                        write_result = Err(StoreError::SnapshotCorrupted {
                            path: tmp_path.clone(),
                            reason: "entry count overflow while streaming shard data".to_string(),
                        });
                    }
                }
            };

            shard.visit_entries(&mut sink);
        }

        write_result?;

        let section_size = section_bytes;

        current_offset = current_offset.checked_add(section_size).ok_or_else(|| {
            StoreError::SnapshotCorrupted {
                path: tmp_path.clone(),
                reason: "snapshot size overflow while updating offsets".to_string(),
            }
        })?;

        counts.push(count);
    }

    writer.flush()?;
    let mut file = writer.into_inner().map_err(|err| err.into_error())?;

    for (offset, count) in offsets.iter().zip(counts.iter()) {
        file.seek(SeekFrom::Start(*offset))?;
        file.write_all(&count.to_le_bytes())?;
    }

    if !offsets.is_empty() {
        file.seek(SeekFrom::Start(SNAPSHOT_HEADER_SIZE as u64))?;
        for offset in &offsets {
            file.write_all(&offset.to_le_bytes())?;
        }
    }
    file.sync_data()?;

    let mut reader = BufReader::new(file.try_clone()?);
    reader.seek(SeekFrom::Start(SNAPSHOT_HEADER_SIZE as u64))?;
    let checksum = checksum_from_reader(&mut reader)?;
    drop(reader);

    file.seek(SeekFrom::Start(24))?;
    file.write_all(&checksum.to_le_bytes())?;
    file.sync_all()?;
    drop(file);

    if snapshot_path.exists() {
        std::fs::remove_file(&snapshot_path)?;
    }
    std::fs::rename(&tmp_path, &snapshot_path)?;
    sync_directory(root_dir)?;

    Ok(snapshot_path)
}
