use std::fs::{File, OpenOptions};
use std::io::{Cursor, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::error::{MhinStoreError, StoreResult};
use crate::storage::fs::sync_directory;
use crate::types::{
    BlockId, BlockUndo, JournalMeta, Operation, ShardUndo, UndoEntry, UndoOp, Value,
};

use super::format::{
    checksum_to_u32, read_journal_block, serialize_journal_block, JournalHeader,
    JOURNAL_FLAG_UNCOMPRESSED, JOURNAL_HEADER_FLAG_NONE, JOURNAL_HEADER_SIZE,
};
use super::{
    chunk::{chunk_file_path, enumerate_chunk_files},
    BlockJournal, JournalAppendOutcome, JournalBlock, JournalIter, JournalOptions, SyncPolicy,
};

pub struct FileBlockJournal {
    root_dir: PathBuf,
    index_path: PathBuf,
    write_lock: Mutex<()>,
    options: JournalOptions,
    /// Sync policy stored separately for runtime modification.
    sync_policy: RwLock<SyncPolicy>,
    /// Lazily opened journal file handle shared across appends.
    journal_state: Mutex<Option<JournalFileState>>,
}

struct JournalFileState {
    chunk_id: u32,
    file: File,
    current_offset: u64,
}

impl FileBlockJournal {
    const INDEX_FILE_NAME: &'static str = "journal.idx";

    pub fn new(root_dir: impl AsRef<Path>) -> StoreResult<Self> {
        Self::with_options(root_dir, JournalOptions::default())
    }

    pub fn with_options(root_dir: impl AsRef<Path>, options: JournalOptions) -> StoreResult<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&root_dir)?;
        let mut options = options;
        let min_entry_size = Self::minimum_entry_size_bytes(&options)?;
        options.max_chunk_size_bytes = options.max_chunk_size_bytes.max(min_entry_size);
        let index_path = root_dir.join(Self::INDEX_FILE_NAME);
        let sync_policy = RwLock::new(options.sync_policy.clone());
        Ok(Self {
            root_dir,
            index_path,
            write_lock: Mutex::new(()),
            options,
            sync_policy,
            journal_state: Mutex::new(None),
        })
    }

    fn minimum_entry_size_bytes(options: &JournalOptions) -> StoreResult<u64> {
        const MIN_BLOCK: BlockId = 0;
        const MIN_KEY: [u8; 8] = [0u8; 8];

        // Ensure the minimum chunk size can accommodate at least one operation plus its undo.
        let minimal_value = Value::from_le_bytes([0u8; 8]);
        let minimal_operations = [Operation {
            key: MIN_KEY,
            value: minimal_value.clone(),
        }];
        let minimal_undo = BlockUndo {
            block_height: MIN_BLOCK,
            shard_undos: vec![ShardUndo {
                shard_index: 0,
                entries: vec![UndoEntry {
                    key: MIN_KEY,
                    previous: Some(minimal_value),
                    op: UndoOp::Updated,
                }],
            }],
        };

        let (serialized, _) =
            serialize_journal_block(MIN_BLOCK, &minimal_undo, &minimal_operations)?;
        let payload = if options.compress {
            zstd::stream::encode_all(serialized.as_slice(), options.compression_level)?
        } else {
            serialized
        };
        Ok(JOURNAL_HEADER_SIZE as u64 + payload.len() as u64)
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

        if guard.is_none() {
            let state = self.bootstrap_active_chunk()?;
            *guard = Some(state);
        }

        let was_empty = guard
            .as_ref()
            .map(|state| state.current_offset == 0)
            .unwrap_or(true);

        Ok((guard, was_empty))
    }

    fn bootstrap_active_chunk(&self) -> StoreResult<JournalFileState> {
        std::fs::create_dir_all(&self.root_dir)?;
        let mut chunks = enumerate_chunk_files(&self.root_dir)?;

        while let Some((chunk_id, path)) = chunks.pop() {
            let metadata = std::fs::metadata(&path)?;
            if metadata.len() == 0 {
                std::fs::remove_file(&path)?;
                sync_directory(&self.root_dir)?;
                continue;
            }
            return self.open_existing_chunk(chunk_id);
        }

        self.create_chunk(1)
    }

    fn open_existing_chunk(&self, chunk_id: u32) -> StoreResult<JournalFileState> {
        let path = chunk_file_path(&self.root_dir, chunk_id);
        let current_offset = super::maintenance::repair_chunk_tail(&path, chunk_id)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(JournalFileState {
            chunk_id,
            file,
            current_offset,
        })
    }

    fn create_chunk(&self, chunk_id: u32) -> StoreResult<JournalFileState> {
        let path = chunk_file_path(&self.root_dir, chunk_id);
        match OpenOptions::new()
            .create_new(true)
            .append(true)
            .read(true)
            .open(&path)
        {
            Ok(file) => {
                file.sync_all()?;
                sync_directory(&self.root_dir)?;
                Ok(JournalFileState {
                    chunk_id,
                    file,
                    current_offset: 0,
                })
            }
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                self.open_existing_chunk(chunk_id)
            }
            Err(err) => Err(err.into()),
        }
    }

    fn rotate_chunk_if_needed(
        &self,
        state: &mut JournalFileState,
        bytes_required: u64,
    ) -> StoreResult<()> {
        let max_size = self.options.max_chunk_size_bytes;
        if bytes_required > max_size {
            return Err(MhinStoreError::UnsupportedOperation {
                reason: format!(
                    "journal chunk size {max_size} is smaller than required entry size {bytes_required}"
                ),
            });
        }

        if state.current_offset + bytes_required <= max_size {
            return Ok(());
        }

        state.file.sync_data()?;
        let next_chunk_id =
            state
                .chunk_id
                .checked_add(1)
                .ok_or_else(|| MhinStoreError::UnsupportedOperation {
                    reason: "exhausted journal chunk ids".to_string(),
                })?;
        *state = self.create_chunk(next_chunk_id)?;
        Ok(())
    }

    fn open_chunk_for_read(&self, chunk_id: u32) -> StoreResult<File> {
        let path = chunk_file_path(&self.root_dir, chunk_id);
        Ok(OpenOptions::new().read(true).open(path)?)
    }

    fn rewind_failed_append(state: &mut JournalFileState, chunk_offset: u64) -> StoreResult<()> {
        state.file.set_len(chunk_offset)?;
        state.file.seek(SeekFrom::Start(chunk_offset))?;
        state.current_offset = chunk_offset;
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

        let (mut state_guard, _) = self.get_or_open_journal()?;
        let state = state_guard
            .as_mut()
            .expect("journal state should be initialized after open");
        let mut journal_synced = false;
        let mut wrote_journal_entry = false;
        let mut chunk_offset = state.current_offset;

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

            let bytes_required = JOURNAL_HEADER_SIZE as u64 + payload.len() as u64;
            self.rotate_chunk_if_needed(state, bytes_required)?;
            chunk_offset = state.current_offset;
            let chunk_was_empty = state.current_offset == 0;

            wrote_journal_entry = true;
            state.file.write_all(&header.to_bytes())?;
            state.file.write_all(&payload)?;
            state.current_offset += bytes_required;

            // Sync according to the configured policy, but always sync the first entry so the
            // initial block in a chunk is durable even before a policy-triggered fsync.
            let should_sync = self.sync_policy.read().should_sync();
            if should_sync || chunk_was_empty {
                state.file.sync_data()?;
                journal_synced = true;
            }

            let meta = JournalMeta {
                block_height: block,
                chunk_id: state.chunk_id,
                chunk_offset,
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
                    if let Err(rewind_err) = Self::rewind_failed_append(state, chunk_offset) {
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

        Ok(JournalIter::new(self.root_dir.clone(), filtered))
    }
    fn read_entry(&self, meta: &JournalMeta) -> StoreResult<JournalBlock> {
        let mut file = self.open_chunk_for_read(meta.chunk_id)?;
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

        let metas = self.load_index()?;
        super::maintenance::truncate_after(&self.root_dir, &self.index_path, block, metas)
    }

    fn rewrite_index(&self, metas: &[JournalMeta]) -> StoreResult<()> {
        let _guard = self.write_lock.lock();
        super::maintenance::rewrite_index(&self.index_path, metas)
    }

    fn scan_entries(&self) -> StoreResult<Vec<JournalMeta>> {
        let _guard = self.write_lock.lock();
        super::maintenance::scan_entries(&self.root_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::journal::chunk::chunk_file_path;
    use crate::storage::journal::format::{
        checksum_to_u32, serialize_journal_block, JournalHeader, JOURNAL_FLAG_UNCOMPRESSED,
        JOURNAL_HEADER_FLAG_NONE, JOURNAL_HEADER_SIZE,
    };
    use crate::storage::journal::maintenance;
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

    fn chunk_options_fitting_one_entry(
        block: BlockId,
        undo: &BlockUndo,
        operations: &[Operation],
    ) -> JournalOptions {
        let (serialized, _) = serialize_journal_block(block, undo, operations).unwrap();
        JournalOptions {
            compress: false,
            max_chunk_size_bytes: JOURNAL_HEADER_SIZE as u64 + serialized.len() as u64,
            ..JournalOptions::default()
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

        let journal_path = chunk_file_path(journal.root_dir(), meta.chunk_id);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(journal_path)
            .unwrap();
        file.seek(SeekFrom::Start(
            meta.chunk_offset + JOURNAL_HEADER_SIZE as u64,
        ))
        .unwrap();

        let mut byte = [0u8];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        file.seek(SeekFrom::Start(
            meta.chunk_offset + JOURNAL_HEADER_SIZE as u64,
        ))
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

    #[test]
    fn rotates_chunk_when_limit_reached() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let block_one = 1;
        let undo_one = sample_undo(block_one);
        let operations_one = sample_operations(block_one);
        let options = chunk_options_fitting_one_entry(block_one, &undo_one, &operations_one);
        let journal = FileBlockJournal::with_options(tmp.path(), options).unwrap();

        let first_meta = journal
            .append(block_one, &undo_one, &operations_one)
            .unwrap()
            .meta;
        assert_eq!(first_meta.chunk_id, 1);
        assert_eq!(first_meta.chunk_offset, 0);

        let block_two = block_one + 1;
        let undo_two = sample_undo(block_two);
        let operations_two = sample_operations(block_two);
        let second_meta = journal
            .append(block_two, &undo_two, &operations_two)
            .unwrap()
            .meta;
        assert_eq!(second_meta.chunk_id, 2, "chunk should rotate after limit");
        assert_eq!(second_meta.chunk_offset, 0);

        assert!(chunk_file_path(journal.root_dir(), 1).exists());
        assert!(chunk_file_path(journal.root_dir(), 2).exists());
    }

    #[test]
    fn iterates_across_chunk_boundaries() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let block_one = 10;
        let undo_one = sample_undo(block_one);
        let operations_one = sample_operations(block_one);
        let options = chunk_options_fitting_one_entry(block_one, &undo_one, &operations_one);
        let journal = FileBlockJournal::with_options(tmp.path(), options).unwrap();

        for block in block_one..block_one + 3 {
            let undo = sample_undo(block);
            let operations = sample_operations(block);
            journal.append(block, &undo, &operations).unwrap();
        }

        let mut iter = journal
            .iter_backwards(block_one + 2, block_one)
            .expect("iterator should open");
        let mut seen = Vec::new();
        while let Some(entry) = iter.next_entry() {
            seen.push(entry.unwrap().block_height);
        }
        assert_eq!(seen, vec![block_one + 2, block_one + 1, block_one]);
    }

    #[test]
    fn truncate_removes_later_chunks() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let block_one = 20;
        let undo_one = sample_undo(block_one);
        let operations_one = sample_operations(block_one);
        let options = chunk_options_fitting_one_entry(block_one, &undo_one, &operations_one);
        let journal = FileBlockJournal::with_options(tmp.path(), options).unwrap();

        for block in block_one..block_one + 3 {
            let undo = sample_undo(block);
            let operations = sample_operations(block);
            journal.append(block, &undo, &operations).unwrap();
        }

        assert!(chunk_file_path(journal.root_dir(), 2).exists());
        journal.truncate_after(block_one).unwrap();
        assert!(
            !chunk_file_path(journal.root_dir(), 2).exists(),
            "truncate should remove newer chunk files"
        );

        let entries = journal.list_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].block_height, block_one);
    }

    #[test]
    fn tiny_chunk_size_is_clamped() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let options = JournalOptions {
            compress: false,
            max_chunk_size_bytes: 1,
            ..JournalOptions::default()
        };
        let expected_min =
            FileBlockJournal::minimum_entry_size_bytes(&options).expect("min size computable");
        let journal = FileBlockJournal::with_options(tmp.path(), options.clone()).unwrap();
        assert_eq!(journal.options.max_chunk_size_bytes, expected_min);

        let block_height = 50;
        let undo = sample_undo(block_height);
        let operations = sample_operations(block_height);
        journal
            .append(block_height, &undo, &operations)
            .expect("append succeeds despite tiny requested chunk");
    }

    #[test]
    fn bootstrap_truncates_partial_header_before_reuse() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let journal = FileBlockJournal::new(tmp.path()).unwrap();

        let block_one = 30;
        let undo_one = sample_undo(block_one);
        let operations_one = sample_operations(block_one);
        let first_meta = journal
            .append(block_one, &undo_one, &operations_one)
            .unwrap()
            .meta;
        let first_end =
            first_meta.chunk_offset + JOURNAL_HEADER_SIZE as u64 + first_meta.compressed_len;

        let chunk_path = chunk_file_path(journal.root_dir(), first_meta.chunk_id);
        {
            let mut file = OpenOptions::new().write(true).open(&chunk_path).unwrap();
            file.seek(SeekFrom::Start(first_end)).unwrap();
            let bogus_header =
                JournalHeader::new(block_one + 1, 0, 0, 0, 0, JOURNAL_HEADER_FLAG_NONE);
            let header_bytes = bogus_header.to_bytes();
            file.write_all(&header_bytes[..JOURNAL_HEADER_SIZE / 2])
                .unwrap();
            file.sync_all().unwrap();
        }

        drop(journal);

        let reopened = FileBlockJournal::new(tmp.path()).unwrap();
        let block_two = block_one + 1;
        let undo_two = sample_undo(block_two);
        let operations_two = sample_operations(block_two);
        let second_meta = reopened
            .append(block_two, &undo_two, &operations_two)
            .unwrap()
            .meta;

        assert_eq!(second_meta.chunk_id, first_meta.chunk_id);
        assert_eq!(
            second_meta.chunk_offset, first_end,
            "second append should start immediately after the last durable entry"
        );

        let chunk_path = chunk_file_path(reopened.root_dir(), second_meta.chunk_id);
        let final_len = std::fs::metadata(&chunk_path).unwrap().len();
        let expected_len =
            second_meta.chunk_offset + JOURNAL_HEADER_SIZE as u64 + second_meta.compressed_len;
        assert_eq!(final_len, expected_len);
    }

    #[test]
    fn scan_entries_truncates_corrupted_chunk_tail() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let options = JournalOptions {
            compress: false,
            ..JournalOptions::default()
        };
        let journal = FileBlockJournal::with_options(tmp.path(), options).unwrap();

        let block_one = 40;
        let undo_one = sample_undo(block_one);
        let operations_one = sample_operations(block_one);
        let first_meta = journal
            .append(block_one, &undo_one, &operations_one)
            .unwrap()
            .meta;
        let first_end =
            first_meta.chunk_offset + JOURNAL_HEADER_SIZE as u64 + first_meta.compressed_len;

        let block_two = block_one + 1;
        let undo_two = sample_undo(block_two);
        let operations_two = sample_operations(block_two);
        let (serialized, entry_count) =
            serialize_journal_block(block_two, &undo_two, &operations_two).unwrap();
        let payload = serialized.clone();
        let checksum = checksum_to_u32(blake3::hash(&payload));
        let header = JournalHeader::new(
            block_two,
            entry_count,
            payload.len() as u64,
            serialized.len() as u64,
            checksum,
            JOURNAL_FLAG_UNCOMPRESSED,
        );
        let header_bytes = header.to_bytes();

        let chunk_path = chunk_file_path(journal.root_dir(), first_meta.chunk_id);
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&chunk_path)
                .unwrap();
            file.seek(SeekFrom::Start(first_end)).unwrap();
            // Simulate crash-left header fragment followed by a valid entry that should become unreachable.
            file.write_all(&header_bytes[..8]).unwrap();
            file.write_all(&header_bytes).unwrap();
            file.write_all(&payload).unwrap();
            file.sync_all().unwrap();
        }

        drop(journal);

        let scanned = maintenance::scan_entries(tmp.path()).unwrap();
        assert_eq!(
            scanned.len(),
            1,
            "scanning should stop at the corruption point"
        );
        assert_eq!(scanned[0].block_height, block_one);

        let final_len = std::fs::metadata(chunk_file_path(tmp.path(), first_meta.chunk_id))
            .unwrap()
            .len();
        assert_eq!(final_len, first_end, "corrupted tail should be truncated");
    }
}
