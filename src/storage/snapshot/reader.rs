use std::path::Path;
use std::sync::Arc;

use memmap2::Mmap;

use crate::error::{MhinStoreError, StoreResult};
use crate::state_shard::StateShard;
use crate::types::{BlockId, Key, Value};

use super::format::{checksum_to_u64, parse_header};

pub(super) fn load_snapshot(path: &Path, shards: &[Arc<dyn StateShard>]) -> StoreResult<BlockId> {
    let file = std::fs::File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };

    let header = parse_header(path, &mmap)?;

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

        let mut shard_entries = Vec::with_capacity(entry_count);
        let mut entry_pos = data_offset + 8;

        for _ in 0..entry_count {
            if entry_pos + 16 > mmap.len() {
                return Err(MhinStoreError::SnapshotCorrupted {
                    path: path.to_path_buf(),
                    reason: format!(
                        "unexpected end of file while reading shard {} entries",
                        shard_idx
                    ),
                });
            }

            let mut key = Key::default();
            key.copy_from_slice(&mmap[entry_pos..entry_pos + 8]);

            let value = Value::from_le_bytes([
                mmap[entry_pos + 8],
                mmap[entry_pos + 9],
                mmap[entry_pos + 10],
                mmap[entry_pos + 11],
                mmap[entry_pos + 12],
                mmap[entry_pos + 13],
                mmap[entry_pos + 14],
                mmap[entry_pos + 15],
            ]);

            shard_entries.push((key, value));
            entry_pos += 16;
        }

        shard.import_data(shard_entries);
    }

    Ok(header.block_height)
}
