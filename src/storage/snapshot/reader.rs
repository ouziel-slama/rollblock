use std::path::Path;
use std::sync::Arc;

use memmap2::Mmap;

use crate::error::{MhinStoreError, StoreResult};
use crate::state_shard::StateShard;
use crate::types::{BlockId, StoreKey as Key, ValueBuf, MAX_VALUE_BYTES};

use super::format::{checksum_to_u64, parse_header};

const VALUE_LEN_WIDTH: usize = 2;

pub(super) fn load_snapshot(path: &Path, shards: &[Arc<dyn StateShard>]) -> StoreResult<BlockId> {
    let file = std::fs::File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };

    let header = parse_header(path, &mmap)?;

    if header.key_bytes != Key::BYTES {
        return Err(MhinStoreError::ConfigurationMismatch {
            field: "key_bytes",
            stored: header.key_bytes,
            requested: Key::BYTES,
        });
    }

    if header.shard_count != shards.len() {
        return Err(MhinStoreError::SnapshotCorrupted {
            path: path.to_path_buf(),
            reason: format!(
                "shard count mismatch: snapshot has {}, but {} shards provided",
                header.shard_count,
                shards.len()
            ),
        });
    }

    if let Some(expected_checksum) = header.checksum {
        let computed = checksum_to_u64(blake3::hash(&mmap[header.header_size..]));
        if computed != expected_checksum {
            return Err(MhinStoreError::SnapshotCorrupted {
                path: path.to_path_buf(),
                reason: format!(
                    "checksum mismatch: expected {:016x}, got {:016x}",
                    expected_checksum, computed
                ),
            });
        }
    }

    let mut offsets = Vec::with_capacity(header.shard_count);
    let mut offset_pos = header.header_size;
    let offsets_end = offset_pos
        .checked_add(header.shard_count.saturating_mul(8))
        .ok_or_else(|| MhinStoreError::SnapshotCorrupted {
            path: path.to_path_buf(),
            reason: "offset section size overflow".to_string(),
        })?;

    if offsets_end > mmap.len() {
        return Err(MhinStoreError::SnapshotCorrupted {
            path: path.to_path_buf(),
            reason: "unexpected end of file while reading offsets".to_string(),
        });
    }

    for _ in 0..header.shard_count {
        let offset = u64::from_le_bytes([
            mmap[offset_pos],
            mmap[offset_pos + 1],
            mmap[offset_pos + 2],
            mmap[offset_pos + 3],
            mmap[offset_pos + 4],
            mmap[offset_pos + 5],
            mmap[offset_pos + 6],
            mmap[offset_pos + 7],
        ]) as usize;

        if offset < offsets_end {
            return Err(MhinStoreError::SnapshotCorrupted {
                path: path.to_path_buf(),
                reason: format!("offset for shard points inside header: {}", offset),
            });
        }

        offsets.push(offset);
        offset_pos += 8;
    }

    for (shard_idx, shard) in shards.iter().enumerate() {
        let data_offset = offsets[shard_idx];

        if data_offset + 8 > mmap.len() {
            return Err(MhinStoreError::SnapshotCorrupted {
                path: path.to_path_buf(),
                reason: format!("invalid offset for shard {}", shard_idx),
            });
        }

        let entry_count = u64::from_le_bytes([
            mmap[data_offset],
            mmap[data_offset + 1],
            mmap[data_offset + 2],
            mmap[data_offset + 3],
            mmap[data_offset + 4],
            mmap[data_offset + 5],
            mmap[data_offset + 6],
            mmap[data_offset + 7],
        ]) as usize;

        let entry_section_start = data_offset + 8;
        let remaining_bytes = mmap.len().saturating_sub(entry_section_start);
        // Invariant: header.key_bytes == Key::BYTES is validated at function entry.
        let max_entries_possible = remaining_bytes / (Key::BYTES + VALUE_LEN_WIDTH);
        let reserve = entry_count.min(max_entries_possible);
        let mut shard_entries = Vec::with_capacity(reserve);
        let mut entry_pos = entry_section_start;

        for _ in 0..entry_count {
            if entry_pos + Key::BYTES + VALUE_LEN_WIDTH > mmap.len() {
                return Err(MhinStoreError::SnapshotCorrupted {
                    path: path.to_path_buf(),
                    reason: format!(
                        "unexpected end of file while reading shard {} key length",
                        shard_idx
                    ),
                });
            }

            let mut key = Key::default();
            key.0
                .copy_from_slice(&mmap[entry_pos..entry_pos + Key::BYTES]);

            let len_offset = entry_pos + Key::BYTES;
            let value_len = u16::from_le_bytes([mmap[len_offset], mmap[len_offset + 1]]) as usize;

            if value_len > MAX_VALUE_BYTES {
                return Err(MhinStoreError::SnapshotCorrupted {
                    path: path.to_path_buf(),
                    reason: format!(
                        "value length {} exceeds MAX_VALUE_BYTES while reading shard {}",
                        value_len, shard_idx
                    ),
                });
            }

            entry_pos = len_offset + 2;

            if entry_pos + value_len > mmap.len() {
                return Err(MhinStoreError::SnapshotCorrupted {
                    path: path.to_path_buf(),
                    reason: format!(
                        "unexpected end of file while reading shard {} value payload",
                        shard_idx
                    ),
                });
            }

            let value = ValueBuf::from_slice(&mmap[entry_pos..entry_pos + value_len]);
            shard_entries.push((key, value));
            entry_pos += value_len;
        }

        shard.import_data(shard_entries);
    }

    Ok(header.block_height)
}
