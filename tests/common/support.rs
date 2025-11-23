use std::collections::{BTreeMap, HashSet};
use std::env;
use std::io::Error;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::thread;
use std::time::Duration;

use rollblock::block_journal::{BlockJournal, JournalBlock, JournalIter};
use rollblock::error::{MhinStoreError, StoreResult};
use rollblock::metadata::MetadataStore;
use rollblock::snapshot::Snapshotter;
use rollblock::state_shard::StateShard;
use rollblock::types::{BlockId, JournalMeta, Key, Operation, Value};
use rollblock::FileBlockJournal;
static INIT_TESTDATA_ROOT: Once = Once::new();
use tempfile::{tempdir_in, TempDir};

use rollblock::orchestrator::durability::{DurabilityMode, PersistenceSettings};

#[derive(Default)]
pub struct MemoryMetadataStore {
    current: Mutex<BlockId>,
    offsets: Mutex<BTreeMap<BlockId, JournalMeta>>,
}

impl MemoryMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn has_offset(&self, block: BlockId) -> bool {
        self.offsets.lock().unwrap().contains_key(&block)
    }
}

pub struct FailingMetadataStore {
    inner: MemoryMetadataStore,
    fail_block: BlockId,
    failed: Mutex<bool>,
}

impl FailingMetadataStore {
    pub fn new(fail_block: BlockId) -> Self {
        Self {
            inner: MemoryMetadataStore::new(),
            fail_block,
            failed: Mutex::new(false),
        }
    }
}

impl MetadataStore for MemoryMetadataStore {
    fn current_block(&self) -> StoreResult<BlockId> {
        Ok(*self.current.lock().unwrap())
    }

    fn set_current_block(&self, block: BlockId) -> StoreResult<()> {
        *self.current.lock().unwrap() = block;
        Ok(())
    }

    fn put_journal_offset(&self, block: BlockId, meta: &JournalMeta) -> StoreResult<()> {
        self.offsets.lock().unwrap().insert(block, meta.clone());
        Ok(())
    }

    fn last_journal_offset_at_or_before(&self, block: BlockId) -> StoreResult<Option<JournalMeta>> {
        let offsets = self.offsets.lock().unwrap();
        Ok(offsets
            .range(..=block)
            .next_back()
            .map(|(_, meta)| meta.clone()))
    }

    fn get_journal_offsets(&self, range: RangeInclusive<BlockId>) -> StoreResult<Vec<JournalMeta>> {
        let start = *range.start();
        let end = *range.end();

        if start > end {
            return Err(MhinStoreError::InvalidBlockRange { start, end });
        }

        let offsets = self.offsets.lock().unwrap();
        Ok(offsets.range(range).map(|(_, meta)| meta.clone()).collect())
    }
}

impl MetadataStore for FailingMetadataStore {
    fn current_block(&self) -> StoreResult<BlockId> {
        <MemoryMetadataStore as MetadataStore>::current_block(&self.inner)
    }

    fn set_current_block(&self, block: BlockId) -> StoreResult<()> {
        <MemoryMetadataStore as MetadataStore>::set_current_block(&self.inner, block)
    }

    fn put_journal_offset(&self, block: BlockId, meta: &JournalMeta) -> StoreResult<()> {
        <MemoryMetadataStore as MetadataStore>::put_journal_offset(&self.inner, block, meta)
    }

    fn last_journal_offset_at_or_before(&self, block: BlockId) -> StoreResult<Option<JournalMeta>> {
        <MemoryMetadataStore as MetadataStore>::last_journal_offset_at_or_before(&self.inner, block)
    }

    fn get_journal_offsets(&self, range: RangeInclusive<BlockId>) -> StoreResult<Vec<JournalMeta>> {
        <MemoryMetadataStore as MetadataStore>::get_journal_offsets(&self.inner, range)
    }

    fn remove_journal_offsets_after(&self, block: BlockId) -> StoreResult<()> {
        <MemoryMetadataStore as MetadataStore>::remove_journal_offsets_after(&self.inner, block)
    }

    fn record_block_commit(&self, block: BlockId, meta: &JournalMeta) -> StoreResult<()> {
        let mut failed = self.failed.lock().unwrap();
        if block == self.fail_block && !*failed {
            *failed = true;
            return Err(MhinStoreError::Io(Error::other(
                "simulated metadata failure",
            )));
        }
        drop(failed);
        <MemoryMetadataStore as MetadataStore>::record_block_commit(&self.inner, block, meta)
    }
}

pub struct NoopSnapshotter;

impl Snapshotter for NoopSnapshotter {
    fn create_snapshot(
        &self,
        _block: BlockId,
        _shards: &[Arc<dyn StateShard>],
    ) -> StoreResult<PathBuf> {
        Ok(PathBuf::new())
    }

    fn load_snapshot(&self, _path: &Path, _shards: &[Arc<dyn StateShard>]) -> StoreResult<BlockId> {
        Ok(0)
    }
}

pub struct SlowSnapshotter {
    delay: Duration,
    in_progress: Arc<AtomicBool>,
}

impl SlowSnapshotter {
    pub fn new(delay: Duration) -> Self {
        Self {
            delay,
            in_progress: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn in_progress_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.in_progress)
    }
}

impl Snapshotter for SlowSnapshotter {
    fn create_snapshot(
        &self,
        _block: BlockId,
        _shards: &[Arc<dyn StateShard>],
    ) -> StoreResult<PathBuf> {
        self.in_progress.store(true, Ordering::Release);
        thread::sleep(self.delay);
        self.in_progress.store(false, Ordering::Release);
        Ok(PathBuf::new())
    }

    fn load_snapshot(&self, _path: &Path, _shards: &[Arc<dyn StateShard>]) -> StoreResult<BlockId> {
        Ok(0)
    }
}

pub struct FlakyJournal {
    inner: FileBlockJournal,
    fail_block: BlockId,
    failed: Mutex<bool>,
}

impl FlakyJournal {
    pub fn new(inner: FileBlockJournal, fail_block: BlockId) -> Self {
        Self {
            inner,
            fail_block,
            failed: Mutex::new(false),
        }
    }
}

impl BlockJournal for FlakyJournal {
    fn append(
        &self,
        block: BlockId,
        undo: &rollblock::types::BlockUndo,
        operations: &[Operation],
    ) -> StoreResult<JournalMeta> {
        if block == self.fail_block {
            let mut failed = self.failed.lock().unwrap();
            if !*failed {
                *failed = true;
                return Err(MhinStoreError::Io(Error::other(
                    "simulated persistence failure",
                )));
            }
        }

        self.inner.append(block, undo, operations)
    }

    fn iter_backwards(&self, from: BlockId, to: BlockId) -> StoreResult<JournalIter> {
        self.inner.iter_backwards(from, to)
    }

    fn read_entry(&self, meta: &JournalMeta) -> StoreResult<JournalBlock> {
        self.inner.read_entry(meta)
    }

    fn list_entries(&self) -> StoreResult<Vec<JournalMeta>> {
        self.inner.list_entries()
    }

    fn truncate_after(&self, block: BlockId) -> StoreResult<()> {
        self.inner.truncate_after(block)
    }

    fn rewrite_index(&self, metas: &[JournalMeta]) -> StoreResult<()> {
        self.inner.rewrite_index(metas)
    }

    fn scan_entries(&self) -> StoreResult<Vec<JournalMeta>> {
        self.inner.scan_entries()
    }
}

pub struct SlowJournal {
    inner: FileBlockJournal,
    slow_blocks: Mutex<HashSet<BlockId>>,
    delay: Duration,
}

impl SlowJournal {
    pub fn new(
        inner: FileBlockJournal,
        slow_blocks: impl IntoIterator<Item = BlockId>,
        delay: Duration,
    ) -> Self {
        Self {
            inner,
            slow_blocks: Mutex::new(slow_blocks.into_iter().collect()),
            delay,
        }
    }
}

impl BlockJournal for SlowJournal {
    fn append(
        &self,
        block: BlockId,
        undo: &rollblock::types::BlockUndo,
        operations: &[Operation],
    ) -> StoreResult<JournalMeta> {
        let should_delay = {
            let guard = self.slow_blocks.lock().unwrap();
            guard.contains(&block)
        };
        if should_delay {
            thread::sleep(self.delay);
        }
        self.inner.append(block, undo, operations)
    }

    fn iter_backwards(&self, from: BlockId, to: BlockId) -> StoreResult<JournalIter> {
        self.inner.iter_backwards(from, to)
    }

    fn read_entry(&self, meta: &JournalMeta) -> StoreResult<JournalBlock> {
        self.inner.read_entry(meta)
    }

    fn list_entries(&self) -> StoreResult<Vec<JournalMeta>> {
        self.inner.list_entries()
    }

    fn truncate_after(&self, block: BlockId) -> StoreResult<()> {
        self.inner.truncate_after(block)
    }

    fn rewrite_index(&self, metas: &[JournalMeta]) -> StoreResult<()> {
        self.inner.rewrite_index(metas)
    }

    fn scan_entries(&self) -> StoreResult<Vec<JournalMeta>> {
        self.inner.scan_entries()
    }
}

pub fn operation<V: Into<Value>>(key: Key, value: V) -> Operation {
    Operation {
        key,
        value: value.into(),
    }
}

pub fn wait_for_block(metadata: &Arc<MemoryMetadataStore>, target: BlockId) {
    let start = std::time::Instant::now();
    while metadata.current_block().unwrap() < target {
        if start.elapsed() > Duration::from_secs(2) {
            panic!("timeout waiting for metadata to reach block {}", target);
        }
        thread::sleep(Duration::from_millis(10));
    }
}

pub fn workspace_tmp() -> PathBuf {
    let path = env::current_dir()
        .unwrap()
        .join("target/testdata/orchestrator");
    INIT_TESTDATA_ROOT.call_once(|| {
        if env::var_os("ROLLBLOCK_KEEP_TESTDATA").is_none() {
            let _ = std::fs::remove_dir_all(&path);
        }
    });
    path
}

pub fn tempdir() -> TempDir {
    let workspace_tmp = workspace_tmp();
    std::fs::create_dir_all(&workspace_tmp).unwrap();
    tempdir_in(&workspace_tmp).unwrap()
}

pub fn synchronous_settings() -> PersistenceSettings {
    PersistenceSettings {
        durability_mode: DurabilityMode::Synchronous,
        snapshot_interval: Duration::from_secs(3600),
        max_snapshot_interval: Duration::from_secs(3600),
    }
}

pub fn async_settings(max_pending_blocks: usize) -> PersistenceSettings {
    PersistenceSettings {
        durability_mode: DurabilityMode::Async { max_pending_blocks },
        snapshot_interval: Duration::from_secs(3600),
        max_snapshot_interval: Duration::from_secs(3600),
    }
}
