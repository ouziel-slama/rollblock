use std::convert::TryInto;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};

use blake3::Hash;

use crate::error::{MhinStoreError, StoreResult};
use crate::types::{BlockId, JournalMeta, ShardUndo};

use super::JournalBlock;

pub(crate) const JOURNAL_MAGIC: u32 = 0x4D_48_4A_31; // "MHJ1"
pub(crate) const JOURNAL_VERSION: u16 = 1;
pub(crate) const JOURNAL_HEADER_FLAG_NONE: u16 = 0;
pub(crate) const JOURNAL_FLAG_UNCOMPRESSED: u16 = 0x0001;
pub(crate) const JOURNAL_HEADER_SIZE: usize = 40;

#[derive(Debug, Clone)]
pub(crate) struct JournalHeader {
    pub magic: u32,
    pub version: u16,
    pub flags: u16,
    pub block_height: BlockId,
    pub entry_count: u32,
    pub compressed_len: u64,
    pub uncompressed_len: u64,
    pub checksum: u32,
}

impl JournalHeader {
    pub(crate) fn new(
        block_height: BlockId,
        entry_count: u32,
        compressed_len: u64,
        uncompressed_len: u64,
        checksum: u32,
        flags: u16,
    ) -> Self {
        Self {
            magic: JOURNAL_MAGIC,
            version: JOURNAL_VERSION,
            flags,
            block_height,
            entry_count,
            compressed_len,
            uncompressed_len,
            checksum,
        }
    }

    pub(crate) fn to_bytes(&self) -> [u8; JOURNAL_HEADER_SIZE] {
        let mut buffer = [0u8; JOURNAL_HEADER_SIZE];
        buffer[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buffer[4..6].copy_from_slice(&self.version.to_le_bytes());
        buffer[6..8].copy_from_slice(&self.flags.to_le_bytes());
        buffer[8..16].copy_from_slice(&self.block_height.to_le_bytes());
        buffer[16..20].copy_from_slice(&self.entry_count.to_le_bytes());
        buffer[20..28].copy_from_slice(&self.compressed_len.to_le_bytes());
        buffer[28..36].copy_from_slice(&self.uncompressed_len.to_le_bytes());
        buffer[36..40].copy_from_slice(&self.checksum.to_le_bytes());
        buffer
    }

    pub(crate) fn from_bytes(bytes: &[u8; JOURNAL_HEADER_SIZE]) -> StoreResult<Self> {
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if magic != JOURNAL_MAGIC {
            return Err(MhinStoreError::InvalidJournalHeader {
                reason: "invalid magic",
            });
        }

        let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        if version != JOURNAL_VERSION {
            return Err(MhinStoreError::InvalidJournalHeader {
                reason: "unsupported version",
            });
        }

        let flags = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
        let block_height = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let entry_count = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
        let compressed_len = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
        let uncompressed_len = u64::from_le_bytes(bytes[28..36].try_into().unwrap());
        let checksum = u32::from_le_bytes(bytes[36..40].try_into().unwrap());

        Ok(Self {
            magic,
            version,
            flags,
            block_height,
            entry_count,
            compressed_len,
            uncompressed_len,
            checksum,
        })
    }
}

pub(crate) fn read_journal_block(file: &mut File, meta: &JournalMeta) -> StoreResult<JournalBlock> {
    file.seek(SeekFrom::Start(meta.offset))?;

    let mut header_bytes = [0u8; JOURNAL_HEADER_SIZE];
    file.read_exact(&mut header_bytes)?;
    let header = JournalHeader::from_bytes(&header_bytes)?;

    if header.block_height != meta.block_height {
        return Err(MhinStoreError::JournalBlockIdMismatch {
            expected: meta.block_height,
            found: header.block_height,
        });
    }

    if header.compressed_len != meta.compressed_len {
        return Err(MhinStoreError::InvalidJournalHeader {
            reason: "compressed length mismatch",
        });
    }

    if header.checksum != meta.checksum {
        return Err(MhinStoreError::JournalChecksumMismatch {
            block: meta.block_height,
        });
    }

    let mut payload = vec![0u8; meta.compressed_len as usize];
    file.read_exact(&mut payload)?;

    let payload_checksum = checksum_to_u32(blake3::hash(&payload));
    if payload_checksum != meta.checksum {
        return Err(MhinStoreError::JournalChecksumMismatch {
            block: meta.block_height,
        });
    }

    let decompressed = if header.flags & JOURNAL_FLAG_UNCOMPRESSED != 0 {
        payload.clone()
    } else {
        zstd::stream::decode_all(Cursor::new(&payload))?
    };

    if decompressed.len() as u64 != header.uncompressed_len {
        return Err(MhinStoreError::InvalidJournalHeader {
            reason: "uncompressed length mismatch",
        });
    }

    let entry: JournalBlock = bincode::deserialize(&decompressed)?;

    if entry.block_height != meta.block_height {
        return Err(MhinStoreError::JournalBlockIdMismatch {
            expected: meta.block_height,
            found: entry.block_height,
        });
    }

    let actual_entries: usize = entry
        .undo
        .shard_undos
        .iter()
        .map(|shard| shard.entries.len())
        .sum();

    if actual_entries as u32 != header.entry_count {
        return Err(MhinStoreError::InvalidJournalHeader {
            reason: "entry count mismatch",
        });
    }

    Ok(entry)
}

pub(crate) fn total_entry_count(shards: &[ShardUndo]) -> usize {
    shards.iter().map(|shard| shard.entries.len()).sum()
}

pub(crate) fn checksum_to_u32(hash: Hash) -> u32 {
    let bytes = hash.as_bytes();
    u32::from_le_bytes(bytes[0..4].try_into().unwrap())
}
