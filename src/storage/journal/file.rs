use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, ErrorKind, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::error::{MhinStoreError, StoreResult};
use crate::storage::fs::sync_directory;
use crate::types::{
    BlockId, BlockUndo, JournalMeta, Operation, ShardUndo, StoreKey, UndoEntry, UndoOp, Value,
};

use super::format::{
    checksum_to_u32, read_journal_block, serialize_journal_block, JournalHeader,
    JOURNAL_FLAG_UNCOMPRESSED, JOURNAL_HEADER_FLAG_NONE, JOURNAL_HEADER_SIZE,
};
use super::{
    chunk::{chunk_file_path, enumerate_chunk_files},
    ActiveChunkResolver, BlockJournal, ChunkDeletionPlan, FilePlanContext, JournalAppendOutcome,
    JournalBlock, JournalIter, JournalOptions, JournalPrunePlan, JournalPruneReport,
    JournalSizingSnapshot, SyncPolicy,
};

pub struct FileBlockJournal {
    root_dir: PathBuf,
    index_path: PathBuf,
    write_lock: Arc<Mutex<()>>,
    options: JournalOptions,
    key_bytes: usize,
    /// Sync policy stored separately for runtime modification.
    sync_policy: RwLock<SyncPolicy>,
    /// Lazily opened journal file handle shared across appends.
    journal_state: Arc<Mutex<Option<JournalFileState>>>,
}

struct JournalFileState {
    chunk_id: u32,
    file: File,
    current_offset: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct ChunkSummary {
    max_block: BlockId,
    entry_count: usize,
}

#[derive(Debug, Clone)]
struct PruneCandidate {
    chunk_id: u32,
    max_block: BlockId,
    entries_removed: usize,
    bytes_freed: u64,
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
            write_lock: Arc::new(Mutex::new(())),
            options,
            key_bytes: StoreKey::BYTES,
            sync_policy,
            journal_state: Arc::new(Mutex::new(None)),
        })
    }

    fn minimum_entry_size_bytes(options: &JournalOptions) -> StoreResult<u64> {
        const MIN_BLOCK: BlockId = 0;
        let min_key = StoreKey::new([0u8; StoreKey::BYTES]);

        // Ensure the minimum chunk size can accommodate at least one operation plus its undo.
        let minimal_value = Value::from_le_bytes([0u8; 8]);
        let minimal_operations = [Operation {
            key: min_key,
            value: minimal_value.clone(),
        }];
        let minimal_undo = BlockUndo {
            block_height: MIN_BLOCK,
            shard_undos: vec![ShardUndo {
                shard_index: 0,
                entries: vec![UndoEntry {
                    key: min_key,
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

    fn staged_index_path(&self) -> PathBuf {
        self.index_path.with_extension("idx.gc")
    }

    fn active_chunk_resolver(&self) -> ActiveChunkResolver {
        let journal_state = Arc::clone(&self.journal_state);
        Arc::new(move || Ok(journal_state.lock().as_ref().map(|state| state.chunk_id)))
    }

    fn write_staged_index(&self, metas: &[JournalMeta]) -> StoreResult<PathBuf> {
        let staged_path = self.staged_index_path();
        if staged_path.exists() {
            fs::remove_file(&staged_path)?;
        }
        if let Some(parent) = staged_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&staged_path)?;
        for meta in metas {
            let bytes = bincode::serialize(meta)?;
            file.write_all(&bytes)?;
        }
        file.sync_all()?;
        if let Some(parent) = staged_path.parent() {
            sync_directory(parent)?;
        }
        Ok(staged_path)
    }

    fn summarize_chunk_stats(metas: &[JournalMeta]) -> HashMap<u32, ChunkSummary> {
        let mut stats = HashMap::new();
        for meta in metas {
            stats
                .entry(meta.chunk_id)
                .and_modify(|entry: &mut ChunkSummary| {
                    entry.max_block = entry.max_block.max(meta.block_height);
                    entry.entry_count += 1;
                })
                .or_insert(ChunkSummary {
                    max_block: meta.block_height,
                    entry_count: 1,
                });
        }
        stats
    }

    fn collect_prune_candidates(
        sealed_chunks: &[(u32, PathBuf)],
        chunk_stats: &HashMap<u32, ChunkSummary>,
        block: BlockId,
    ) -> StoreResult<Vec<PruneCandidate>> {
        let mut candidates = Vec::new();
        for (chunk_id, path) in sealed_chunks {
            let Some(stats) = chunk_stats.get(chunk_id) else {
                continue;
            };
            if stats.entry_count == 0 || stats.max_block > block {
                continue;
            }
            let bytes_freed = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            candidates.push(PruneCandidate {
                chunk_id: *chunk_id,
                max_block: stats.max_block,
                entries_removed: stats.entry_count,
                bytes_freed,
            });
        }
        Ok(candidates)
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn sizing_snapshot(&self, sample_window: usize) -> StoreResult<JournalSizingSnapshot> {
        let sample_len = sample_window.max(1);
        let mut recent = VecDeque::with_capacity(sample_len);
        self.visit_index(|meta| {
            if recent.len() == sample_len {
                recent.pop_front();
            }
            recent.push_back(meta);
        })?;
        let entry_sizes: Vec<u64> = recent
            .into_iter()
            .map(|meta| JOURNAL_HEADER_SIZE as u64 + meta.compressed_len)
            .collect();
        let chunks = enumerate_chunk_files(&self.root_dir)?;
        let chunk_count = chunks.len();
        let sealed_chunk_count = chunk_count.saturating_sub(1);
        let min_entry_size_bytes = Self::minimum_entry_size_bytes(&self.options)?;
        Ok(JournalSizingSnapshot {
            entry_sizes,
            sealed_chunk_count,
            chunk_count,
            min_entry_size_bytes,
            max_chunk_size_bytes: self.options.max_chunk_size_bytes,
        })
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
        let current_offset =
            super::maintenance::repair_chunk_tail(&path, chunk_id, self.key_bytes)?;
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
        let mut metas = Vec::new();
        self.visit_index(|meta| metas.push(meta))?;
        Ok(metas)
    }

    fn visit_index(&self, mut on_entry: impl FnMut(JournalMeta)) -> StoreResult<()> {
        if !self.index_path.exists() {
            return Ok(());
        }

        let file = File::open(&self.index_path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);

        loop {
            let entry_offset = reader.stream_position()?;
            match bincode::deserialize_from(&mut reader) {
                Ok(meta) => on_entry(meta),
                Err(err) => {
                    // Tolerate a trailing partial entry caused by a crash mid-write.
                    if matches!(
                        *err,
                        bincode::ErrorKind::Io(ref io_err)
                            if io_err.kind() == std::io::ErrorKind::UnexpectedEof
                    ) {
                        if entry_offset < file_len {
                            tracing::warn!(
                                offset = entry_offset,
                                "Detected truncated journal index entry; ignoring trailing bytes"
                            );
                        }
                        break;
                    }
                    return Err(err.into());
                }
            }
        }

        Ok(())
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
                self.key_bytes,
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

        Ok(JournalIter::new(
            self.root_dir.clone(),
            filtered,
            self.key_bytes,
        ))
    }
    fn read_entry(&self, meta: &JournalMeta) -> StoreResult<JournalBlock> {
        let mut file = self.open_chunk_for_read(meta.chunk_id)?;
        read_journal_block(&mut file, meta, self.key_bytes)
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
        super::maintenance::scan_entries(&self.root_dir, self.key_bytes)
    }

    fn plan_prune_chunks_ending_at_or_before(
        &self,
        block: BlockId,
    ) -> StoreResult<Option<JournalPrunePlan>> {
        if block == 0 {
            return Ok(None);
        }

        let metas = self.load_index()?;
        if metas.is_empty() {
            return Ok(None);
        }

        let chunk_listing = enumerate_chunk_files(&self.root_dir)?;
        if chunk_listing.len() <= 1 {
            return Ok(None);
        }

        let sealed_len = chunk_listing.len().saturating_sub(1);
        if sealed_len == 0 {
            return Ok(None);
        }
        let sealed_chunks = &chunk_listing[..sealed_len];

        let chunk_stats = Self::summarize_chunk_stats(&metas);
        let mut candidates = Self::collect_prune_candidates(sealed_chunks, &chunk_stats, block)?;
        if candidates.is_empty() {
            return Ok(None);
        }

        if sealed_len == 1 {
            return Ok(None);
        }

        if candidates.len() >= sealed_len {
            candidates.sort_by_key(|c| c.chunk_id);
            while candidates.len() >= sealed_len {
                candidates.pop();
            }
        }

        if candidates.is_empty() {
            return Ok(None);
        }

        let chunk_ids: Vec<u32> = candidates.iter().map(|c| c.chunk_id).collect();
        let removal_set: HashSet<u32> = chunk_ids.iter().copied().collect();
        let baseline_entry_count = metas.len();
        let retained: Vec<JournalMeta> = metas
            .into_iter()
            .filter(|meta| !removal_set.contains(&meta.chunk_id))
            .collect();

        let staged_index_path = self.write_staged_index(&retained)?;
        let pruned_through = candidates
            .iter()
            .map(|candidate| candidate.max_block)
            .max()
            .unwrap_or(block);
        let entries_removed: usize = candidates.iter().map(|c| c.entries_removed).sum();
        let bytes_freed: u64 = candidates.iter().map(|c| c.bytes_freed).sum();

        let report = JournalPruneReport {
            pruned_through,
            chunks_removed: chunk_ids.len(),
            entries_removed,
            bytes_freed,
        };

        let plan = JournalPrunePlan::file_plan(
            report,
            ChunkDeletionPlan { chunk_ids },
            staged_index_path,
            baseline_entry_count,
            FilePlanContext::new(
                self.root_dir.clone(),
                self.index_path.clone(),
                Arc::clone(&self.write_lock),
                self.active_chunk_resolver(),
            ),
        );
        Ok(Some(plan))
    }

    fn adopt_staged_prune_plan(
        &self,
        pruned_through: BlockId,
        chunk_plan: ChunkDeletionPlan,
        staged_index_path: PathBuf,
        baseline_entry_count: usize,
        mut report: JournalPruneReport,
    ) -> StoreResult<JournalPrunePlan> {
        if report.pruned_through == 0 {
            report.pruned_through = pruned_through;
        }
        if report.chunks_removed == 0 {
            report.chunks_removed = chunk_plan.chunk_ids.len();
        }
        Ok(JournalPrunePlan::recover_file_plan(
            report,
            chunk_plan,
            staged_index_path,
            baseline_entry_count,
            FilePlanContext::new(
                self.root_dir.clone(),
                self.index_path.clone(),
                Arc::clone(&self.write_lock),
                self.active_chunk_resolver(),
            ),
        ))
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
    use crate::types::{Operation, ShardUndo, StoreKey, UndoEntry, UndoOp, Value};
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use tempfile::tempdir_in;

    fn sample_shard(shard_index: usize, key: StoreKey, value: Value) -> ShardUndo {
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
            key: StoreKey::from([block as u8; StoreKey::BYTES]),
            value: block.into(),
        }]
    }

    fn sample_undo(block: BlockId) -> BlockUndo {
        BlockUndo {
            block_height: block,
            shard_undos: vec![sample_shard(
                0,
                StoreKey::from([1u8; StoreKey::BYTES]),
                42.into(),
            )],
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
                key: StoreKey::from([0x01; StoreKey::BYTES]),
                value: Value::from_vec(vec![1, 2, 3, 4, 5]),
            },
            Operation {
                key: StoreKey::from([0x02; StoreKey::BYTES]),
                value: Value::from_vec(large_value),
            },
            Operation {
                key: StoreKey::from([0x03; StoreKey::BYTES]),
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
    fn plan_prunes_sealed_chunks_and_preserves_new_entries() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let block_one = 1;
        let undo_one = sample_undo(block_one);
        let operations_one = sample_operations(block_one);
        let options = chunk_options_fitting_one_entry(block_one, &undo_one, &operations_one);
        let journal = FileBlockJournal::with_options(tmp.path(), options).unwrap();

        for block in 1..=4 {
            let undo = sample_undo(block);
            let operations = sample_operations(block);
            journal.append(block, &undo, &operations).unwrap();
        }

        let mut plan = journal
            .plan_prune_chunks_ending_at_or_before(2)
            .expect("plan succeeds")
            .expect("plan exists");

        assert_eq!(plan.report().chunks_removed, 2);
        assert_eq!(plan.chunk_plan().chunk_ids, vec![1, 2]);

        let block_five = 5;
        let undo_five = sample_undo(block_five);
        let ops_five = sample_operations(block_five);
        journal
            .append(block_five, &undo_five, &ops_five)
            .expect("append after plan succeeds");

        plan.mark_persisted();
        plan.commit().expect("commit succeeds");

        assert!(!chunk_file_path(journal.root_dir(), 1).exists());
        assert!(!chunk_file_path(journal.root_dir(), 2).exists());

        let remaining: Vec<BlockId> = journal
            .list_entries()
            .unwrap()
            .into_iter()
            .map(|meta| meta.block_height)
            .collect();
        assert_eq!(remaining, vec![3, 4, 5]);
    }

    #[test]
    fn plan_waits_when_only_one_historical_chunk_exists() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let block_one = 1;
        let undo_one = sample_undo(block_one);
        let operations_one = sample_operations(block_one);
        let options = chunk_options_fitting_one_entry(block_one, &undo_one, &operations_one);
        let journal = FileBlockJournal::with_options(tmp.path(), options).unwrap();

        for block in 1..=2 {
            let undo = sample_undo(block);
            let operations = sample_operations(block);
            journal.append(block, &undo, &operations).unwrap();
        }

        let plan = journal
            .plan_prune_chunks_ending_at_or_before(1)
            .expect("plan calculation succeeds");
        assert!(
            plan.is_none(),
            "should refuse pruning when only one sealed chunk is available"
        );
    }

    #[test]
    fn prune_plan_aborts_when_candidate_reopens_before_commit() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let block_one = 1;
        let undo_one = sample_undo(block_one);
        let operations_one = sample_operations(block_one);
        let options = chunk_options_fitting_one_entry(block_one, &undo_one, &operations_one);
        let journal = FileBlockJournal::with_options(tmp.path(), options).unwrap();

        for block in 1..=5 {
            let undo = sample_undo(block);
            let operations = sample_operations(block);
            journal.append(block, &undo, &operations).unwrap();
        }

        let mut plan = journal
            .plan_prune_chunks_ending_at_or_before(3)
            .expect("plan calculation succeeds")
            .expect("plan should exist");
        plan.mark_persisted();

        journal
            .truncate_after(3)
            .expect("truncate reopens the third chunk");

        plan.commit()
            .expect("commit should succeed but skip conflicting chunks");

        assert!(
            chunk_file_path(journal.root_dir(), 3).exists(),
            "reopened chunk should not be pruned"
        );
        assert!(
            chunk_file_path(journal.root_dir(), 1).exists(),
            "plan abort should preserve other candidates"
        );
        assert!(
            chunk_file_path(journal.root_dir(), 2).exists(),
            "plan abort should preserve other candidates"
        );
        assert!(
            !journal.staged_index_path().exists(),
            "aborted plan should discard staged index"
        );
    }

    #[test]
    fn plan_replay_is_idempotent_after_recovery() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let block_one = 1;
        let undo_one = sample_undo(block_one);
        let ops_one = sample_operations(block_one);
        let options = chunk_options_fitting_one_entry(block_one, &undo_one, &ops_one);
        let journal = FileBlockJournal::with_options(tmp.path(), options).unwrap();

        for block in 1..=5 {
            let undo = sample_undo(block);
            let operations = sample_operations(block);
            journal.append(block, &undo, &operations).unwrap();
        }

        let mut plan = journal
            .plan_prune_chunks_ending_at_or_before(2)
            .expect("plan calculation succeeds")
            .expect("plan exists");

        let pruned_through = plan.report().pruned_through;
        let chunk_plan = plan.chunk_plan().clone();
        let staged_index_path = plan.staged_index_path().to_path_buf();
        let baseline_entry_count = plan.baseline_entry_count();
        let report = plan.report().clone();

        plan.mark_persisted();
        drop(plan);

        // First replay simulates resuming immediately after a crash.
        let mut recovered = journal
            .adopt_staged_prune_plan(
                pruned_through,
                chunk_plan.clone(),
                staged_index_path.clone(),
                baseline_entry_count,
                report.clone(),
            )
            .expect("recovered plan builds");
        recovered.mark_persisted();
        recovered.commit().expect("recovered commit succeeds");

        assert!(
            !chunk_file_path(journal.root_dir(), 1).exists()
                && !chunk_file_path(journal.root_dir(), 2).exists(),
            "first two chunks should be deleted after recovery"
        );

        // A second adoption should be a no-op but must remain successful.
        let mut replay = journal
            .adopt_staged_prune_plan(
                pruned_through,
                chunk_plan,
                staged_index_path,
                baseline_entry_count,
                report,
            )
            .expect("idempotent plan builds");
        replay.mark_persisted();
        replay.commit().expect("idempotent commit succeeds");

        let remaining: Vec<BlockId> = journal
            .list_entries()
            .unwrap()
            .into_iter()
            .map(|meta| meta.block_height)
            .collect();
        assert_eq!(remaining, vec![3, 4, 5]);
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
            let bogus_header = JournalHeader::new(
                block_one + 1,
                0,
                0,
                0,
                0,
                JOURNAL_HEADER_FLAG_NONE,
                StoreKey::BYTES,
            );
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
            StoreKey::BYTES,
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

        let scanned = maintenance::scan_entries(tmp.path(), StoreKey::BYTES).unwrap();
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
