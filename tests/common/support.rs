use std::collections::{BTreeMap, HashSet};
use std::env;
use std::io::Error;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::thread;
use std::time::Duration;

use rollblock::block_journal::{
    BlockJournal, JournalAppendOutcome, JournalBlock, JournalIter, SyncPolicy,
};
use rollblock::error::{MhinStoreError, StoreResult};
use rollblock::metadata::{GcWatermark, MetadataStore};
use rollblock::snapshot::Snapshotter;
use rollblock::state_shard::StateShard;
use rollblock::types::{BlockId, BlockUndo, JournalMeta, Operation, StoreKey as Key, Value};
use rollblock::FileBlockJournal;
static INIT_TESTDATA_ROOT: Once = Once::new();
use tempfile::{tempdir_in, TempDir};

use rollblock::orchestrator::durability::{DurabilityMode, PersistenceSettings};

#[derive(Default)]
pub struct MemoryMetadataStore {
    current: Mutex<BlockId>,
    offsets: Mutex<BTreeMap<BlockId, JournalMeta>>,
    gc_watermark: Mutex<Option<GcWatermark>>,
    snapshot_watermark: Mutex<Option<BlockId>>,
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

    fn remove_journal_offsets_after(&self, block: BlockId) -> StoreResult<()> {
        if block == BlockId::MAX {
            return Ok(());
        }

        let mut offsets = self.offsets.lock().unwrap();
        let start = block.saturating_add(1);
        let _ = offsets.split_off(&start);
        Ok(())
    }

    fn prune_journal_offsets_at_or_before(&self, block: BlockId) -> StoreResult<usize> {
        let mut offsets = self.offsets.lock().unwrap();
        if block == BlockId::MAX {
            let removed = offsets.len();
            offsets.clear();
            return Ok(removed);
        }

        let split_key = block.saturating_add(1);
        let retained = offsets.split_off(&split_key);
        let removed = offsets.len();
        *offsets = retained;
        Ok(removed)
    }

    fn load_gc_watermark(&self) -> StoreResult<Option<GcWatermark>> {
        Ok(self.gc_watermark.lock().unwrap().clone())
    }

    fn store_gc_watermark(&self, watermark: &GcWatermark) -> StoreResult<()> {
        *self.gc_watermark.lock().unwrap() = Some(watermark.clone());
        Ok(())
    }

    fn clear_gc_watermark(&self) -> StoreResult<()> {
        *self.gc_watermark.lock().unwrap() = None;
        Ok(())
    }

    fn load_snapshot_watermark(&self) -> StoreResult<Option<BlockId>> {
        Ok(*self.snapshot_watermark.lock().unwrap())
    }

    fn store_snapshot_watermark(&self, block: BlockId) -> StoreResult<()> {
        *self.snapshot_watermark.lock().unwrap() = Some(block);
        Ok(())
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

    fn prune_journal_offsets_at_or_before(&self, block: BlockId) -> StoreResult<usize> {
        let mut failed = self.failed.lock().unwrap();
        if block == self.fail_block && !*failed {
            *failed = true;
            return Err(MhinStoreError::Io(Error::other(
                "simulated metadata failure",
            )));
        }
        drop(failed);
        <MemoryMetadataStore as MetadataStore>::prune_journal_offsets_at_or_before(
            &self.inner,
            block,
        )
    }

    fn load_gc_watermark(&self) -> StoreResult<Option<GcWatermark>> {
        <MemoryMetadataStore as MetadataStore>::load_gc_watermark(&self.inner)
    }

    fn store_gc_watermark(&self, watermark: &GcWatermark) -> StoreResult<()> {
        <MemoryMetadataStore as MetadataStore>::store_gc_watermark(&self.inner, watermark)
    }

    fn clear_gc_watermark(&self) -> StoreResult<()> {
        <MemoryMetadataStore as MetadataStore>::clear_gc_watermark(&self.inner)
    }

    fn load_snapshot_watermark(&self) -> StoreResult<Option<BlockId>> {
        <MemoryMetadataStore as MetadataStore>::load_snapshot_watermark(&self.inner)
    }

    fn store_snapshot_watermark(&self, block: BlockId) -> StoreResult<()> {
        <MemoryMetadataStore as MetadataStore>::store_snapshot_watermark(&self.inner, block)
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
    ) -> StoreResult<JournalAppendOutcome> {
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

    fn force_sync(&self) -> StoreResult<()> {
        self.inner.force_sync()
    }

    fn set_sync_policy(&self, policy: SyncPolicy) {
        self.inner.set_sync_policy(policy);
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
    ) -> StoreResult<JournalAppendOutcome> {
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

    fn force_sync(&self) -> StoreResult<()> {
        self.inner.force_sync()
    }

    fn set_sync_policy(&self, policy: SyncPolicy) {
        self.inner.set_sync_policy(policy);
    }
}

pub fn operation<V: Into<Value>>(key: impl Into<Key>, value: V) -> Operation {
    Operation {
        key: key.into(),
        value: value.into(),
    }
}

pub fn padded_key(prefix: [u8; 8]) -> Key {
    Key::from_prefix(prefix)
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
        min_rollback_window: BlockId::MAX,
        prune_interval: Duration::from_secs(10),
    }
}

pub fn async_settings(max_pending_blocks: usize) -> PersistenceSettings {
    PersistenceSettings {
        durability_mode: DurabilityMode::Async { max_pending_blocks },
        snapshot_interval: Duration::from_secs(3600),
        max_snapshot_interval: Duration::from_secs(3600),
        min_rollback_window: BlockId::MAX,
        prune_interval: Duration::from_secs(10),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_undo(block: BlockId) -> BlockUndo {
        BlockUndo {
            block_height: block,
            shard_undos: Vec::new(),
        }
    }

    fn verify_journal_sync_forwarding<J: BlockJournal>(journal: &J) {
        journal.set_sync_policy(SyncPolicy::every_n_blocks(3));

        // First append must sync to ensure a brand-new journal is durable before batching kicks in.
        let first_outcome = journal
            .append(1, &empty_undo(1), &[operation([1u8; Key::BYTES], 10)])
            .expect("first append should succeed");
        assert!(
            first_outcome.synced,
            "first append to a new journal must always sync for durability"
        );

        for block in 2..=2 {
            let outcome = journal
                .append(
                    block,
                    &empty_undo(block),
                    &[operation([block as u8; Key::BYTES], block * 10)],
                )
                .expect("append should succeed");
            assert!(
                !outcome.synced,
                "every_n_blocks should skip sync until counter threshold"
            );
        }

        let outcome = journal
            .append(3, &empty_undo(3), &[operation(padded_key([3u8; 8]), 30)])
            .expect("append should succeed at threshold");
        assert!(
            outcome.synced,
            "counter threshold must trigger sync via forwarded policy"
        );

        journal.set_sync_policy(SyncPolicy::EveryBlock);
        let outcome = journal
            .append(4, &empty_undo(4), &[operation(padded_key([4u8; 8]), 40)])
            .expect("append should sync every block");
        assert!(outcome.synced, "every block policy must sync every append");

        journal.set_sync_policy(SyncPolicy::every_n_blocks(5));
        let outcome = journal
            .append(5, &empty_undo(5), &[operation(padded_key([5u8; 8]), 50)])
            .expect("append should succeed under relaxed mode");
        assert!(
            !outcome.synced,
            "relaxed mode should defer sync until force_sync/reset"
        );

        journal.force_sync().expect("force_sync should succeed");
        let outcome = journal
            .append(6, &empty_undo(6), &[operation(padded_key([6u8; 8]), 60)])
            .expect("append after force_sync should succeed");
        assert!(
            outcome.synced,
            "force_sync must reset counter so next append syncs"
        );
    }

    #[test]
    fn flaky_journal_forwards_runtime_sync_controls() {
        let tmp = tempdir();
        let journal = FlakyJournal::new(FileBlockJournal::new(tmp.path()).unwrap(), 9999);
        verify_journal_sync_forwarding(&journal);
    }

    #[test]
    fn slow_journal_forwards_runtime_sync_controls() {
        let tmp = tempdir();
        let journal = SlowJournal::new(
            FileBlockJournal::new(tmp.path()).unwrap(),
            [],
            Duration::ZERO,
        );
        verify_journal_sync_forwarding(&journal);
    }
}
