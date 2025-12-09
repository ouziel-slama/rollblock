use std::convert::TryInto;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};

use blake3::Hash;
use serde::{Deserialize, Serialize};

use crate::error::{MhinStoreError, StoreResult};
use crate::types::{
    BlockId, BlockUndo, JournalMeta, Operation, StoreKey, Value, MAX_VALUE_BYTES, MIN_KEY_BYTES,
};

use super::JournalBlock;

pub(crate) const JOURNAL_MAGIC: u32 = 0x4D_48_4A_31; // "MHJ1"
pub(crate) const JOURNAL_VERSION: u16 = 2;
pub(crate) const JOURNAL_HEADER_FLAG_NONE: u16 = 0;
pub(crate) const JOURNAL_FLAG_UNCOMPRESSED: u16 = 0x0001;
pub(crate) const JOURNAL_HEADER_SIZE: usize = 42;

/// Per-entry overhead in the operations payload: key bytes + 2-byte value length.
const ENTRY_OVERHEAD: usize = StoreKey::BYTES + 2;

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
    pub key_bytes: u16,
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
        key_bytes: usize,
    ) -> Self {
        let key_bytes = u16::try_from(key_bytes).expect("key_bytes must fit in u16");

        Self {
            magic: JOURNAL_MAGIC,
            version: JOURNAL_VERSION,
            flags,
            key_bytes,
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
        buffer[8..10].copy_from_slice(&self.key_bytes.to_le_bytes());
        buffer[10..18].copy_from_slice(&self.block_height.to_le_bytes());
        buffer[18..22].copy_from_slice(&self.entry_count.to_le_bytes());
        buffer[22..30].copy_from_slice(&self.compressed_len.to_le_bytes());
        buffer[30..38].copy_from_slice(&self.uncompressed_len.to_le_bytes());
        buffer[38..42].copy_from_slice(&self.checksum.to_le_bytes());
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
        let key_bytes = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
        let block_height = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
        let entry_count = u32::from_le_bytes(bytes[18..22].try_into().unwrap());
        let compressed_len = u64::from_le_bytes(bytes[22..30].try_into().unwrap());
        let uncompressed_len = u64::from_le_bytes(bytes[30..38].try_into().unwrap());
        let checksum = u32::from_le_bytes(bytes[38..42].try_into().unwrap());

        if key_bytes < MIN_KEY_BYTES as u16 {
            return Err(MhinStoreError::InvalidJournalHeader {
                reason: "key width below minimum",
            });
        }

        Ok(Self {
            magic,
            version,
            flags,
            key_bytes,
            block_height,
            entry_count,
            compressed_len,
            uncompressed_len,
            checksum,
        })
    }
}

pub(crate) fn read_journal_block(
    file: &mut File,
    meta: &JournalMeta,
    expected_key_bytes: usize,
) -> StoreResult<JournalBlock> {
    file.seek(SeekFrom::Start(meta.chunk_offset))?;

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

    if header.key_bytes as usize != expected_key_bytes {
        return Err(MhinStoreError::ConfigurationMismatch {
            field: "key_bytes",
            stored: header.key_bytes as usize,
            requested: expected_key_bytes,
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

    // If the decompressed length does not match what the header recorded, the
    // entry was corrupted (e.g. a flipped header byte). Treat it as a checksum
    // failure so startup surfaces the corruption instead of silently truncating.
    if decompressed.len() as u64 != header.uncompressed_len {
        return Err(MhinStoreError::JournalChecksumMismatch {
            block: header.block_height,
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

        let mut key = [0u8; StoreKey::BYTES];
        key.copy_from_slice(&bytes[cursor..cursor + StoreKey::BYTES]);
        cursor += StoreKey::BYTES;

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

        operations.push(Operation {
            key: StoreKey::from(key),
            value,
        });
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

        total_len = total_len
            .checked_add(StoreKey::BYTES + 2 + value_len)
            .ok_or(MhinStoreError::InvalidJournalHeader {
                reason: "operation payload overflow",
            })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir_in;

    #[test]
    fn read_journal_block_rejects_key_width_mismatch() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let path = tmp.path().join("chunk_00000000.mhj");

        let block_height = 7;
        let operations = vec![Operation {
            key: StoreKey::from([0xAB; StoreKey::BYTES]),
            value: Value::from_slice(b"abc"),
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
            StoreKey::BYTES + 1,
        );

        {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)
                .unwrap();
            file.write_all(&header.to_bytes()).unwrap();
            file.write_all(&payload).unwrap();
            file.sync_all().unwrap();
        }

        let meta = JournalMeta {
            chunk_id: 0,
            chunk_offset: 0,
            block_height,
            compressed_len: payload.len() as u64,
            checksum,
        };

        let mut file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
        let err =
            read_journal_block(&mut file, &meta, StoreKey::BYTES).expect_err("should mismatch");

        match err {
            MhinStoreError::ConfigurationMismatch {
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
