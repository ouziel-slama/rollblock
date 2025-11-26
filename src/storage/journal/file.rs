use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::error::{MhinStoreError, StoreResult};
use crate::storage::fs::sync_directory;
use crate::types::{BlockId, BlockUndo, JournalMeta, Operation};

use super::format::{
    checksum_to_u32, read_journal_block, serialize_journal_block, JournalHeader,
    JOURNAL_FLAG_UNCOMPRESSED, JOURNAL_HEADER_FLAG_NONE, JOURNAL_HEADER_SIZE,
};
use super::{
    BlockJournal, JournalAppendOutcome, JournalBlock, JournalIter, JournalOptions, SyncPolicy,
};

pub struct FileBlockJournal {
    root_dir: PathBuf,
    journal_path: PathBuf,
    index_path: PathBuf,
    write_lock: Mutex<()>,
    options: JournalOptions,
    /// Sync policy stored separately for runtime modification.
    sync_policy: RwLock<SyncPolicy>,
    /// Lazily opened journal file handle shared across appends.
    journal_state: Mutex<Option<JournalFileState>>,
}

struct JournalFileState {
    file: File,
    current_offset: u64,
}

impl FileBlockJournal {
    const JOURNAL_FILE_NAME: &'static str = "journal.bin";
    const INDEX_FILE_NAME: &'static str = "journal.idx";

    pub fn new(root_dir: impl AsRef<Path>) -> StoreResult<Self> {
        Self::with_options(root_dir, JournalOptions::default())
    }

    pub fn with_options(root_dir: impl AsRef<Path>, options: JournalOptions) -> StoreResult<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&root_dir)?;
        let journal_path = root_dir.join(Self::JOURNAL_FILE_NAME);
        if !journal_path.exists() {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&journal_path)?;
        }
        let index_path = root_dir.join(Self::INDEX_FILE_NAME);
        let sync_policy = RwLock::new(options.sync_policy.clone());
        Ok(Self {
            root_dir,
            journal_path,
            index_path,
            write_lock: Mutex::new(()),
            options,
            sync_policy,
            journal_state: Mutex::new(None),
        })
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    fn open_index_for_append(&self) -> StoreResult<(File, bool)> {
        let existed = self.index_path.exists();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.index_path)?;
        Ok((file, !existed))
    }

    fn get_or_open_journal(&self) -> StoreResult<(MutexGuard<'_, Option<JournalFileState>>, bool)> {
        let mut guard = self.journal_state.lock();

        let was_empty = if guard.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&self.journal_path)?;
            let current_offset = file.metadata()?.len();
            let was_empty = current_offset == 0;

            *guard = Some(JournalFileState {
                file,
                current_offset,
            });
            was_empty
        } else {
            guard
                .as_ref()
                .map(|state| state.current_offset == 0)
                .unwrap_or(true)
        };

        Ok((guard, was_empty))
    }

    fn rewind_failed_append(state: &mut JournalFileState, offset: u64) -> StoreResult<()> {
        state.file.set_len(offset)?;
        state.file.seek(SeekFrom::Start(offset))?;
        state.current_offset = offset;
        Ok(())
    }

    fn load_index(&self) -> StoreResult<Vec<JournalMeta>> {
        if !self.index_path.exists() {
            return Ok(Vec::new());
        }

        let mut file = File::open(&self.index_path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let mut cursor = Cursor::new(data);
        let mut metas = Vec::new();

        while (cursor.position() as usize) < cursor.get_ref().len() {
            let start = cursor.position();
            match bincode::deserialize_from(&mut cursor) {
                Ok(meta) => metas.push(meta),
                Err(err) => {
                    // Tolerate a trailing partial entry caused by a crash mid-write.
                    if matches!(
                        *err,
                        bincode::ErrorKind::Io(ref io_err)
                            if io_err.kind() == std::io::ErrorKind::UnexpectedEof
                    ) {
                        tracing::warn!(
                            offset = start,
                            "Detected truncated journal index entry; ignoring trailing bytes"
                        );
                        break;
                    }
                    return Err(err.into());
                }
            }
        }

        Ok(metas)
    }
}

impl BlockJournal for FileBlockJournal {
    fn append(
        &self,
        block: BlockId,
        undo: &BlockUndo,
        operations: &[Operation],
    ) -> StoreResult<JournalAppendOutcome> {
        if undo.block_height != block {
            return Err(MhinStoreError::JournalBlockIdMismatch {
                expected: block,
                found: undo.block_height,
            });
        }

        let _guard = self.write_lock.lock();

        let (mut state_guard, journal_was_empty) = self.get_or_open_journal()?;
        let state = state_guard
            .as_mut()
            .expect("journal state should be initialized after open");
        let offset = state.current_offset;
        let mut journal_synced = false;
        let mut wrote_journal_entry = false;

        let result = (|| -> StoreResult<JournalAppendOutcome> {
            let (serialized, entry_count) = serialize_journal_block(block, undo, operations)?;
            let (payload, flags) = if self.options.compress {
                (
                    zstd::stream::encode_all(
                        serialized.as_slice(),
                        self.options.compression_level,
                    )?,
                    JOURNAL_HEADER_FLAG_NONE,
                )
            } else {
                (serialized.clone(), JOURNAL_FLAG_UNCOMPRESSED)
            };

            let checksum_hash = blake3::hash(&payload);
            let checksum = checksum_to_u32(checksum_hash);

            let header = JournalHeader::new(
                block,
                entry_count,
                payload.len() as u64,
                serialized.len() as u64,
                checksum,
                flags,
            );

            wrote_journal_entry = true;
            state.file.write_all(&header.to_bytes())?;
            state.file.write_all(&payload)?;
            let bytes_written = JOURNAL_HEADER_SIZE as u64 + payload.len() as u64;
            state.current_offset += bytes_written;

            // Sync according to the configured policy, but always sync the first entry so the
            // initial block is durable even before a policy-triggered fsync.
            let should_sync = self.sync_policy.read().should_sync();
            if should_sync || journal_was_empty {
                state.file.sync_data()?;
                journal_synced = true;
            }

            let meta = JournalMeta {
                block_height: block,
                offset,
                compressed_len: payload.len() as u64,
                checksum,
            };

            let (mut index, index_created) = self.open_index_for_append()?;
            let index_was_empty = index.metadata()?.len() == 0;
            let index_bytes = bincode::serialize(&meta)?;
            index.write_all(&index_bytes)?;

            // Sync according to the configured policy (reuse decision from journal sync)
            if should_sync || index_was_empty {
                index.sync_data()?;
            }

            // Always sync when the file was just created
            if index_created {
                index.sync_data()?;
                if let Some(parent) = self.index_path.parent() {
                    sync_directory(parent)?;
                }
            }

            Ok(JournalAppendOutcome {
                meta,
                synced: journal_synced,
            })
        })();

        match result {
            Ok(outcome) => {
                drop(state_guard);
                Ok(outcome)
            }
            Err(err) => {
                if wrote_journal_entry {
                    if let Err(rewind_err) = Self::rewind_failed_append(state, offset) {
                        drop(state_guard);
                        return Err(rewind_err);
                    }
                }
                drop(state_guard);
                Err(err)
            }
        }
    }

    fn force_sync(&self) -> StoreResult<()> {
        let _guard = self.write_lock.lock();

        // Sync journal file if it's already open.
        {
            let state_guard = self.journal_state.lock();
            if let Some(state) = state_guard.as_ref() {
                state.file.sync_data()?;
            }
        }

        // Sync index file
        if self.index_path.exists() {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&self.index_path)?;
            file.sync_data()?;
        }

        // Reset the sync counter so we don't skip the next required sync
        self.sync_policy.read().reset();

        Ok(())
    }

    fn set_sync_policy(&self, policy: SyncPolicy) {
        *self.sync_policy.write() = policy;
    }

    fn iter_backwards(&self, from: BlockId, to: BlockId) -> StoreResult<JournalIter> {
        if from < to {
            return Err(MhinStoreError::InvalidBlockRange {
                start: from,
                end: to,
            });
        }

        let metas = self.load_index()?;

        let filtered: Vec<JournalMeta> = metas
            .into_iter()
            .rev()
            .filter(|meta| meta.block_height <= from && meta.block_height >= to)
            .collect();

        let file = OpenOptions::new().read(true).open(&self.journal_path)?;

        Ok(JournalIter::new(file, filtered))
    }
    fn read_entry(&self, meta: &JournalMeta) -> StoreResult<JournalBlock> {
        let mut file = OpenOptions::new().read(true).open(&self.journal_path)?;
        read_journal_block(&mut file, meta)
    }

    fn list_entries(&self) -> StoreResult<Vec<JournalMeta>> {
        self.load_index()
    }

    fn truncate_after(&self, block: BlockId) -> StoreResult<()> {
        let _guard = self.write_lock.lock();

        {
            let mut state_guard = self.journal_state.lock();
            *state_guard = None;
        }

        if !self.journal_path.exists() {
            return Ok(());
        }

        let metas = self.load_index()?;
        super::maintenance::truncate_after(&self.journal_path, &self.index_path, block, metas)
    }

    fn rewrite_index(&self, metas: &[JournalMeta]) -> StoreResult<()> {
        let _guard = self.write_lock.lock();
        super::maintenance::rewrite_index(&self.index_path, metas)
    }

    fn scan_entries(&self) -> StoreResult<Vec<JournalMeta>> {
        super::maintenance::scan_entries(&self.journal_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::journal::format::JOURNAL_HEADER_SIZE;
    use crate::types::{Operation, ShardUndo, UndoEntry, UndoOp, Value};
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use tempfile::tempdir_in;

    fn sample_shard(shard_index: usize, key: [u8; 8], value: Value) -> ShardUndo {
        ShardUndo {
            shard_index,
            entries: vec![UndoEntry {
                key,
                previous: Some(value),
                op: UndoOp::Updated,
            }],
        }
    }

    fn sample_operations(block: BlockId) -> Vec<Operation> {
        vec![Operation {
            key: [block as u8; 8],
            value: block.into(),
        }]
    }

    fn sample_undo(block: BlockId) -> BlockUndo {
        BlockUndo {
            block_height: block,
            shard_undos: vec![sample_shard(0, [1u8; 8], 42.into())],
        }
    }

    #[test]
    fn append_and_read_single_block() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let journal = FileBlockJournal::new(tmp.path()).unwrap();

        let block_height = 7;
        let undo = sample_undo(block_height);
        let operations = sample_operations(block_height);

        let meta = journal
            .append(block_height, &undo, &operations)
            .unwrap()
            .meta;
        assert_eq!(meta.block_height, block_height);

        let mut iter = journal.iter_backwards(block_height, block_height).unwrap();
        let read_entry = iter.next_entry().unwrap().unwrap();
        assert_eq!(read_entry.block_height, block_height);
        assert_eq!(read_entry.undo.shard_undos.len(), 1);
        assert_eq!(read_entry.operations.len(), 1);
        assert_eq!(read_entry.operations[0].value, block_height);
        assert!(iter.next_entry().is_none());
    }

    #[test]
    fn append_block_with_variable_length_values() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let journal = FileBlockJournal::new(tmp.path()).unwrap();

        let block_height = 11;
        let undo = sample_undo(block_height);
        let mut large_value = vec![0xAB; crate::types::MAX_VALUE_BYTES];
        large_value[0] = 0xCD;

        let operations = vec![
            Operation {
                key: [0x01; 8],
                value: Value::from_vec(vec![1, 2, 3, 4, 5]),
            },
            Operation {
                key: [0x02; 8],
                value: Value::from_vec(large_value),
            },
            Operation {
                key: [0x03; 8],
                value: Value::empty(),
            },
        ];

        journal
            .append(block_height, &undo, &operations)
            .expect("append succeeds");

        let mut iter = journal.iter_backwards(block_height, block_height).unwrap();
        let entry = iter.next_entry().unwrap().unwrap();
        assert_eq!(entry.block_height, block_height);
        assert_eq!(entry.operations.len(), operations.len());
        assert_eq!(entry.operations[0].value.as_slice(), &[1, 2, 3, 4, 5]);
        assert_eq!(
            entry.operations[1].value.len(),
            crate::types::MAX_VALUE_BYTES
        );
        assert!(entry.operations[2].is_delete());
        assert!(iter.next_entry().is_none());
    }

    #[test]
    fn append_empty_block_round_trip() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let journal = FileBlockJournal::new(tmp.path()).unwrap();

        let block_height = 5;
        let undo = BlockUndo {
            block_height,
            shard_undos: Vec::new(),
        };

        let meta = journal.append(block_height, &undo, &[]).unwrap().meta;
        assert_eq!(meta.block_height, block_height);

        let mut iter = journal.iter_backwards(block_height, block_height).unwrap();
        let entry = iter.next_entry().unwrap().unwrap();
        assert_eq!(entry.block_height, block_height);
        assert!(entry.operations.is_empty());
        assert!(entry.undo.shard_undos.is_empty());
        assert!(iter.next_entry().is_none());
    }

    #[test]
    fn iterate_multiple_blocks_descending() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let journal = FileBlockJournal::new(tmp.path()).unwrap();

        for block_height in 1..=3 {
            let undo = sample_undo(block_height as BlockId);
            let operations = sample_operations(block_height as BlockId);
            journal
                .append(block_height as BlockId, &undo, &operations)
                .unwrap();
        }

        let mut iter = journal.iter_backwards(3, 1).unwrap();

        for expected in (1..=3).rev() {
            let entry = iter.next_entry().unwrap().unwrap();
            assert_eq!(entry.block_height, expected);
            assert_eq!(entry.undo.block_height, expected);
            assert_eq!(entry.operations.len(), 1);
        }

        assert!(iter.next_entry().is_none());
    }

    #[test]
    fn append_rejects_mismatched_block_height() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let journal = FileBlockJournal::new(tmp.path()).unwrap();

        let undo = sample_undo(2);
        let err = journal.append(1, &undo, &[]).unwrap_err();
        match err {
            MhinStoreError::JournalBlockIdMismatch { expected, found } => {
                assert_eq!(expected, 1);
                assert_eq!(found, 2);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn iter_backwards_rejects_invalid_range() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let journal = FileBlockJournal::new(tmp.path()).unwrap();

        match journal.iter_backwards(1, 2) {
            Err(MhinStoreError::InvalidBlockRange { start, end }) => {
                assert_eq!(start, 1);
                assert_eq!(end, 2);
            }
            Ok(_) => panic!("expected invalid block range error"),
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn iter_backwards_empty_journal_is_noop() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let journal = FileBlockJournal::new(tmp.path()).unwrap();
        std::fs::File::create(journal.root_dir().join("journal.bin")).unwrap();

        let mut iter = journal.iter_backwards(0, 0).unwrap();
        assert!(iter.next_entry().is_none());
    }

    #[test]
    fn corrupted_payload_results_in_checksum_error() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let journal = FileBlockJournal::new(tmp.path()).unwrap();

        let block_height = 5;
        let undo = sample_undo(block_height);
        let operations = sample_operations(block_height);
        let meta = journal
            .append(block_height, &undo, &operations)
            .unwrap()
            .meta;

        let journal_path = journal.root_dir().join("journal.bin");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(journal_path)
            .unwrap();
        file.seek(SeekFrom::Start(meta.offset + JOURNAL_HEADER_SIZE as u64))
            .unwrap();

        let mut byte = [0u8];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        file.seek(SeekFrom::Start(meta.offset + JOURNAL_HEADER_SIZE as u64))
            .unwrap();
        file.write_all(&byte).unwrap();
        file.flush().unwrap();

        let mut iter = journal.iter_backwards(block_height, block_height).unwrap();
        let err = iter.next_entry().unwrap().unwrap_err();
        match err {
            MhinStoreError::JournalChecksumMismatch { block } => assert_eq!(block, block_height),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
