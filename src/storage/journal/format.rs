use std::convert::TryInto;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};

use blake3::Hash;
use serde::{Deserialize, Serialize};

use crate::error::{MhinStoreError, StoreResult};
use crate::types::{BlockId, BlockUndo, JournalMeta, Operation, Value, MAX_VALUE_BYTES};

use super::JournalBlock;

pub(crate) const JOURNAL_MAGIC: u32 = 0x4D_48_4A_31; // "MHJ1"
pub(crate) const JOURNAL_VERSION: u16 = 1;
pub(crate) const JOURNAL_HEADER_FLAG_NONE: u16 = 0;
pub(crate) const JOURNAL_FLAG_UNCOMPRESSED: u16 = 0x0001;
pub(crate) const JOURNAL_HEADER_SIZE: usize = 40;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct JournalBlockPayload {
    pub block_height: BlockId,
    pub undo: BlockUndo,
    pub operations: Vec<u8>,
}

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

    let payload: JournalBlockPayload = bincode::deserialize(&decompressed)?;

    if payload.block_height != meta.block_height {
        return Err(MhinStoreError::JournalBlockIdMismatch {
            expected: meta.block_height,
            found: payload.block_height,
        });
    }

    let expected_count: usize =
        header
            .entry_count
            .try_into()
            .map_err(|_| MhinStoreError::InvalidJournalHeader {
                reason: "entry count overflow",
            })?;

    let operations = decode_operations(&payload.operations, expected_count)?;

    Ok(JournalBlock {
        block_height: payload.block_height,
        operations,
        undo: payload.undo,
    })
}

pub(crate) fn checksum_to_u32(hash: Hash) -> u32 {
    let bytes = hash.as_bytes();
    u32::from_le_bytes(bytes[0..4].try_into().unwrap())
}

fn decode_operations(bytes: &[u8], expected_count: usize) -> StoreResult<Vec<Operation>> {
    const ENTRY_OVERHEAD: usize = 8 + 2; // key + len

    if bytes.is_empty() && expected_count == 0 {
        return Ok(Vec::new());
    }

    if expected_count > 0 {
        let min_len = expected_count.checked_mul(ENTRY_OVERHEAD).ok_or(
            MhinStoreError::InvalidJournalHeader {
                reason: "operation payload overflow",
            },
        )?;
        if bytes.len() < min_len {
            return Err(MhinStoreError::InvalidJournalHeader {
                reason: "operation payload truncated",
            });
        }
    }

    let mut cursor = 0usize;
    let mut operations = Vec::with_capacity(expected_count);

    while cursor < bytes.len() {
        if cursor + ENTRY_OVERHEAD > bytes.len() {
            return Err(MhinStoreError::InvalidJournalHeader {
                reason: "operation payload truncated",
            });
        }

        let mut key = [0u8; 8];
        key.copy_from_slice(&bytes[cursor..cursor + 8]);
        cursor += 8;

        let value_len = u16::from_le_bytes([bytes[cursor], bytes[cursor + 1]]) as usize;
        cursor += 2;

        if value_len > MAX_VALUE_BYTES {
            return Err(MhinStoreError::InvalidJournalHeader {
                reason: "operation value exceeds limit",
            });
        }

        if cursor + value_len > bytes.len() {
            return Err(MhinStoreError::InvalidJournalHeader {
                reason: "operation payload truncated",
            });
        }

        let value = Value::from_slice(&bytes[cursor..cursor + value_len]);
        cursor += value_len;

        operations.push(Operation { key, value });
    }

    if cursor != bytes.len() {
        return Err(MhinStoreError::InvalidJournalHeader {
            reason: "operation payload trailing bytes",
        });
    }

    if operations.len() != expected_count {
        return Err(MhinStoreError::InvalidJournalHeader {
            reason: "entry count mismatch",
        });
    }

    Ok(operations)
}

pub(crate) fn serialize_journal_block(
    block_height: BlockId,
    undo: &BlockUndo,
    operations: &[Operation],
) -> StoreResult<(Vec<u8>, u32)> {
    let encoded_operations = encode_operations(operations)?;
    let payload = JournalBlockPayload {
        block_height,
        undo: undo.clone(),
        operations: encoded_operations,
    };

    let serialized = bincode::serialize(&payload)?;
    let operation_count: u32 =
        operations
            .len()
            .try_into()
            .map_err(|_| MhinStoreError::InvalidJournalHeader {
                reason: "operation count overflow",
            })?;

    Ok((serialized, operation_count))
}

fn encode_operations(operations: &[Operation]) -> StoreResult<Vec<u8>> {
    let mut total_len: usize = 0;

    for op in operations {
        let value_len = op.value.len();
        if value_len > MAX_VALUE_BYTES {
            return Err(MhinStoreError::InvalidJournalHeader {
                reason: "operation value exceeds limit",
            });
        }

        total_len = total_len.checked_add(8 + 2 + value_len).ok_or(
            MhinStoreError::InvalidJournalHeader {
                reason: "operation payload overflow",
            },
        )?;
    }

    let mut buffer = Vec::with_capacity(total_len);
    for op in operations {
        buffer.extend_from_slice(&op.key);
        let len = op.value.len() as u16;
        buffer.extend_from_slice(&len.to_le_bytes());
        buffer.extend_from_slice(op.value.as_slice());
    }

    Ok(buffer)
}
