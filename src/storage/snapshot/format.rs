use std::path::Path;

use blake3::{Hash, Hasher};

use crate::error::{MhinStoreError, StoreResult};
use crate::types::BlockId;

pub(super) const SNAPSHOT_MAGIC: [u8; 4] = *b"MHIS";
pub(super) const SNAPSHOT_VERSION: u16 = 1;
pub(super) const SNAPSHOT_HEADER_RESERVED: u16 = 0;
pub(super) const SNAPSHOT_HEADER_SIZE_V2: usize = 32;

#[derive(Debug, Clone, Copy)]
pub(super) struct SnapshotHeader {
    pub(super) block_height: BlockId,
    pub(super) shard_count: usize,
    pub(super) checksum: Option<u64>,
    pub(super) header_size: usize,
}

pub(super) fn encode_header(block: BlockId, shard_count: u64) -> [u8; SNAPSHOT_HEADER_SIZE_V2] {
    let mut header = [0u8; SNAPSHOT_HEADER_SIZE_V2];
    header[0..4].copy_from_slice(&SNAPSHOT_MAGIC);
    header[4..6].copy_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&SNAPSHOT_HEADER_RESERVED.to_le_bytes());
    header[8..16].copy_from_slice(&block.to_le_bytes());
    header[16..24].copy_from_slice(&shard_count.to_le_bytes());
    header
}

pub(super) fn parse_header(path: &Path, bytes: &[u8]) -> StoreResult<SnapshotHeader> {
    if bytes.len() < SNAPSHOT_HEADER_SIZE_V2 {
        return Err(MhinStoreError::SnapshotCorrupted {
            path: path.to_path_buf(),
            reason: "file too small for snapshot header".to_string(),
        });
    }

    if bytes[0..4] != SNAPSHOT_MAGIC {
        return Err(MhinStoreError::SnapshotCorrupted {
            path: path.to_path_buf(),
            reason: format!("invalid magic bytes: {:?}", &bytes[0..4]),
        });
    }

    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != SNAPSHOT_VERSION {
        return Err(MhinStoreError::SnapshotCorrupted {
            path: path.to_path_buf(),
            reason: format!("unsupported version: {}", version),
        });
    }

    let reserved = u16::from_le_bytes([bytes[6], bytes[7]]);
    if reserved != SNAPSHOT_HEADER_RESERVED {
        return Err(MhinStoreError::SnapshotCorrupted {
            path: path.to_path_buf(),
            reason: format!("non-zero reserved field: {}", reserved),
        });
    }

    let block_height = u64::from_le_bytes(bytes[8..16].try_into().expect("slice length mismatch"));
    let shard_count =
        u64::from_le_bytes(bytes[16..24].try_into().expect("slice length mismatch")) as usize;
    let checksum = u64::from_le_bytes(bytes[24..32].try_into().expect("slice length mismatch"));

    Ok(SnapshotHeader {
        block_height,
        shard_count,
        checksum: Some(checksum),
        header_size: SNAPSHOT_HEADER_SIZE_V2,
    })
}

pub(super) fn checksum_to_u64(hash: Hash) -> u64 {
    let bytes = hash.as_bytes();
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

pub(super) fn checksum_from_reader<R>(mut reader: R) -> StoreResult<u64>
where
    R: std::io::Read,
{
    let mut hasher = Hasher::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(checksum_to_u64(hasher.finalize()))
}
