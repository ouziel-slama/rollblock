use super::*;
use crate::facade::recovery::{
    reconcile_metadata_with_journal, replay_committed_blocks, restore_existing_state,
};

mod recovery_tests {
    use super::*;
    use crate::block_journal::{BlockJournal, JournalBlock, JournalIter};
    use crate::metadata::MetadataStore;
    use crate::state_engine::ShardedStateEngine;
    use crate::state_shard::RawTableShard;
    use crate::types::{BlockUndo, JournalMeta, Operation};
    use crate::MhinStoreError;
    use std::ops::RangeInclusive;
    use std::sync::Arc;

    struct StubJournal;

    impl BlockJournal for StubJournal {
        fn append(
            &self,
            _block: BlockId,
            _undo: &crate::types::BlockUndo,
            _operations: &[Operation],
        ) -> StoreResult<JournalMeta> {
            panic!("append should not be called in tests");
        }

        fn iter_backwards(&self, _from: BlockId, _to: BlockId) -> StoreResult<JournalIter> {
            panic!("iter_backwards should not be called in tests");
        }

        fn read_entry(&self, meta: &JournalMeta) -> StoreResult<JournalBlock> {
            Ok(JournalBlock {
                block_height: meta.block_height,
                operations: Vec::<Operation>::new(),
                undo: BlockUndo {
                    block_height: meta.block_height,
                    shard_undos: Vec::new(),
                },
            })
        }

        fn list_entries(&self) -> StoreResult<Vec<JournalMeta>> {
            panic!("list_entries should not be called in tests");
        }

        fn truncate_after(&self, _block: BlockId) -> StoreResult<()> {
            panic!("truncate_after should not be called in tests");
        }

        fn rewrite_index(&self, _metas: &[JournalMeta]) -> StoreResult<()> {
            panic!("rewrite_index should not be called in tests");
        }

        fn scan_entries(&self) -> StoreResult<Vec<JournalMeta>> {
            panic!("scan_entries should not be called in tests");
        }
    }

    #[derive(Clone)]
    struct InMemoryMetadata {
        current: BlockId,
        offsets: Vec<JournalMeta>,
    }

    impl InMemoryMetadata {
        fn new(current: BlockId, offsets: Vec<JournalMeta>) -> Self {
            Self { current, offsets }
        }

        fn offsets_in_range(&self, range: RangeInclusive<BlockId>) -> Vec<JournalMeta> {
            self.offsets
                .iter()
                .filter(|meta| range.contains(&meta.block_height))
                .cloned()
                .collect()
        }
    }

    impl MetadataStore for InMemoryMetadata {
        fn current_block(&self) -> StoreResult<BlockId> {
            Ok(self.current)
        }

        fn set_current_block(&self, _block: BlockId) -> StoreResult<()> {
            panic!("set_current_block should not be called in tests");
        }

        fn put_journal_offset(&self, _block: BlockId, _meta: &JournalMeta) -> StoreResult<()> {
            panic!("put_journal_offset should not be called in tests");
        }

        fn get_journal_offsets(
            &self,
            range: RangeInclusive<BlockId>,
        ) -> StoreResult<Vec<JournalMeta>> {
            Ok(self.offsets_in_range(range))
        }

        fn last_journal_offset_at_or_before(
            &self,
            block: BlockId,
        ) -> StoreResult<Option<JournalMeta>> {
            Ok(self
                .offsets
                .iter()
                .rev()
                .find(|meta| meta.block_height <= block)
                .cloned())
        }
    }

    fn test_engine<M: MetadataStore + 'static>(metadata: Arc<M>) -> ShardedStateEngine<M> {
        let shard: Arc<dyn crate::state_shard::StateShard> = Arc::new(RawTableShard::new(0, 0));
        ShardedStateEngine::new(vec![shard], metadata)
    }

    #[test]
    fn replay_committed_blocks_errors_when_no_offsets() {
        let metadata = Arc::new(InMemoryMetadata::new(5, Vec::new()));
        let engine = test_engine(Arc::clone(&metadata));
        let err = replay_committed_blocks(&StubJournal, metadata.as_ref(), &engine, 2)
            .expect_err("expected missing offsets to produce an error");

        match err {
            MhinStoreError::MissingJournalEntry { block } => assert_eq!(block, 3),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn replay_committed_blocks_errors_on_gaps() {
        let offsets = vec![
            JournalMeta {
                block_height: 3,
                ..Default::default()
            },
            JournalMeta {
                block_height: 5,
                ..Default::default()
            },
        ];
        let metadata = Arc::new(InMemoryMetadata::new(5, offsets));
        let engine = test_engine(Arc::clone(&metadata));
        let err = replay_committed_blocks(&StubJournal, metadata.as_ref(), &engine, 2)
            .expect_err("expected gaps to produce an error");

        match err {
            MhinStoreError::MissingJournalEntry { block } => assert_eq!(block, 4),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

mod facade_tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs::{self, OpenOptions};
    use std::ops::RangeInclusive;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;

    use crate::block_journal::{BlockJournal, JournalBlock, JournalIter};
    use crate::metadata::{LmdbMetadataStore, MetadataStore};
    use crate::snapshot::Snapshotter;
    use crate::types::{BlockUndo, JournalMeta};
    use crate::{
        BlockOrchestrator, FileBlockJournal, MhinStoreError, MmapSnapshotter, RawTableShard,
        ShardedStateEngine, StateShard,
    };
    use tempfile::{tempdir, tempdir_in};

    #[derive(Default)]
    struct DummyOrchestrator {
        applied: Mutex<Vec<(BlockId, Vec<Operation>)>>,
        lookups: Mutex<Vec<Key>>,
        state: Mutex<HashMap<Key, Value>>,
        current_block: Mutex<BlockId>,
        revert_calls: Mutex<Vec<BlockId>>,
    }

    impl DummyOrchestrator {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }
    }

    #[derive(Default)]
    struct ShutdownTrackingOrchestrator {
        shutdowns: AtomicUsize,
    }

    impl ShutdownTrackingOrchestrator {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn shutdown_calls(&self) -> usize {
            self.shutdowns.load(Ordering::Acquire)
        }
    }

    impl BlockOrchestrator for ShutdownTrackingOrchestrator {
        fn apply_operations(
            &self,
            _block_height: BlockId,
            _ops: Vec<Operation>,
        ) -> StoreResult<()> {
            Ok(())
        }

        fn revert_to(&self, _block: BlockId) -> StoreResult<()> {
            Ok(())
        }

        fn fetch(&self, _key: Key) -> StoreResult<Value> {
            Ok(0)
        }

        fn metrics(&self) -> Option<&crate::metrics::StoreMetrics> {
            None
        }

        fn current_block(&self) -> StoreResult<BlockId> {
            Ok(0)
        }

        fn applied_block_height(&self) -> BlockId {
            0
        }

        fn durable_block_height(&self) -> StoreResult<BlockId> {
            Ok(0)
        }

        fn shutdown(&self) -> StoreResult<()> {
            self.shutdowns.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }

        fn ensure_healthy(&self) -> StoreResult<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct FailingOrchestrator;

    impl FailingOrchestrator {
        fn new() -> Arc<Self> {
            Arc::new(Self)
        }
    }

    fn wait_for_durable(store: &MhinStoreFacade, target: BlockId) {
        for _ in 0..100 {
            if store.durable_block().unwrap() >= target {
                return;
            }
            thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("durable block did not reach {target}");
    }

    impl BlockOrchestrator for DummyOrchestrator {
        fn apply_operations(&self, block_height: BlockId, ops: Vec<Operation>) -> StoreResult<()> {
            self.applied
                .lock()
                .unwrap()
                .push((block_height, ops.clone()));
            for op in ops {
                if op.value == 0 {
                    self.state.lock().unwrap().remove(&op.key);
                } else {
                    self.state.lock().unwrap().insert(op.key, op.value);
                }
            }
            *self.current_block.lock().unwrap() = block_height;
            Ok(())
        }

        fn revert_to(&self, block: BlockId) -> StoreResult<()> {
            self.revert_calls.lock().unwrap().push(block);
            *self.current_block.lock().unwrap() = block;
            Ok(())
        }

        fn fetch(&self, key: Key) -> StoreResult<Value> {
            self.lookups.lock().unwrap().push(key);
            Ok(self.state.lock().unwrap().get(&key).copied().unwrap_or(0))
        }

        fn metrics(&self) -> Option<&crate::metrics::StoreMetrics> {
            None
        }

        fn current_block(&self) -> StoreResult<BlockId> {
            Ok(*self.current_block.lock().unwrap())
        }

        fn applied_block_height(&self) -> BlockId {
            *self.current_block.lock().unwrap()
        }

        fn durable_block_height(&self) -> StoreResult<BlockId> {
            Ok(*self.current_block.lock().unwrap())
        }

        fn shutdown(&self) -> StoreResult<()> {
            Ok(())
        }

        fn ensure_healthy(&self) -> StoreResult<()> {
            Ok(())
        }
    }

    impl BlockOrchestrator for FailingOrchestrator {
        fn apply_operations(&self, block_height: BlockId, _ops: Vec<Operation>) -> StoreResult<()> {
            Err(MhinStoreError::BlockIdNotIncreasing {
                block_height,
                current: block_height,
            })
        }

        fn revert_to(&self, _block: BlockId) -> StoreResult<()> {
            Ok(())
        }

        fn fetch(&self, _key: Key) -> StoreResult<Value> {
            Ok(0)
        }

        fn metrics(&self) -> Option<&crate::metrics::StoreMetrics> {
            None
        }

        fn current_block(&self) -> StoreResult<BlockId> {
            Ok(0)
        }

        fn applied_block_height(&self) -> BlockId {
            0
        }

        fn durable_block_height(&self) -> StoreResult<BlockId> {
            Ok(0)
        }

        fn shutdown(&self) -> StoreResult<()> {
            Ok(())
        }

        fn ensure_healthy(&self) -> StoreResult<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct StubMetadata {
        current_block: Mutex<BlockId>,
        offsets: Mutex<HashMap<BlockId, JournalMeta>>,
    }

    impl MetadataStore for StubMetadata {
        fn current_block(&self) -> StoreResult<BlockId> {
            Ok(*self.current_block.lock().unwrap())
        }

        fn set_current_block(&self, block: BlockId) -> StoreResult<()> {
            *self.current_block.lock().unwrap() = block;
            Ok(())
        }

        fn put_journal_offset(&self, block: BlockId, meta: &JournalMeta) -> StoreResult<()> {
            self.offsets.lock().unwrap().insert(block, meta.clone());
            Ok(())
        }

        fn get_journal_offsets(
            &self,
            range: RangeInclusive<BlockId>,
        ) -> StoreResult<Vec<JournalMeta>> {
            let offsets = self.offsets.lock().unwrap();
            let mut metas: Vec<JournalMeta> = offsets
                .iter()
                .filter_map(|(&height, meta)| {
                    if range.contains(&height) {
                        Some(meta.clone())
                    } else {
                        None
                    }
                })
                .collect();
            metas.sort_by_key(|meta| meta.block_height);
            Ok(metas)
        }

        fn last_journal_offset_at_or_before(
            &self,
            block: BlockId,
        ) -> StoreResult<Option<JournalMeta>> {
            let offsets = self.offsets.lock().unwrap();
            Ok(offsets
                .iter()
                .filter(|(&height, _)| height <= block)
                .max_by_key(|(&height, _)| height)
                .map(|(_, meta)| meta.clone()))
        }

        fn remove_journal_offsets_after(&self, block: BlockId) -> StoreResult<()> {
            self.offsets
                .lock()
                .unwrap()
                .retain(|&height, _| height <= block);
            Ok(())
        }
    }

    #[derive(Default)]
    struct StubJournal;

    impl BlockJournal for StubJournal {
        fn append(
            &self,
            _block: BlockId,
            _undo: &BlockUndo,
            _operations: &[Operation],
        ) -> StoreResult<JournalMeta> {
            unreachable!("stub journal append should not be called");
        }

        fn iter_backwards(&self, _from: BlockId, to: BlockId) -> StoreResult<JournalIter> {
            Err(MhinStoreError::MissingJournalEntry { block: to })
        }

        fn read_entry(&self, meta: &JournalMeta) -> StoreResult<JournalBlock> {
            Err(MhinStoreError::MissingJournalEntry {
                block: meta.block_height,
            })
        }

        fn list_entries(&self) -> StoreResult<Vec<JournalMeta>> {
            Ok(Vec::new())
        }

        fn truncate_after(&self, _block: BlockId) -> StoreResult<()> {
            Ok(())
        }
    }

    fn sample_operation(value: Value) -> Operation {
        Operation {
            key: [value as u8; 8],
            value,
        }
    }

    #[test]
    fn store_config_generates_expected_paths() {
        let tmp = tempdir().unwrap();
        let config = StoreConfig::new(tmp.path(), 2, 16, 1, false);

        assert_eq!(config.data_dir, tmp.path());
        assert_eq!(config.shards_count, Some(2));
        assert_eq!(config.initial_capacity, Some(16));
        assert_eq!(config.thread_count, 1);
        assert_eq!(config.mode, StoreMode::ReadWrite);

        assert_eq!(config.metadata_dir(), tmp.path().join("metadata"));
        assert_eq!(config.journal_dir(), tmp.path().join("journal"));
        assert_eq!(config.snapshots_dir(), tmp.path().join("snapshots"));
    }

    #[test]
    fn reopening_with_mismatched_shards_count_fails() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let data_dir = tmp.path().join("store");

        let initial_config = StoreConfig::new(&data_dir, 2, 64, 1, false);
        let store = MhinStoreFacade::new(initial_config).expect("initial store should open");
        drop(store);

        let mismatched = StoreConfig::new(&data_dir, 3, 64, 1, false);
        let err = match MhinStoreFacade::new(mismatched) {
            Ok(_) => panic!("shard mismatch should error"),
            Err(err) => err,
        };
        match err {
            MhinStoreError::ConfigurationMismatch {
                field,
                stored,
                requested,
            } => {
                assert_eq!(field, "shards_count");
                assert_eq!(stored, 2);
                assert_eq!(requested, 3);
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let capacity_mismatch = StoreConfig::new(&data_dir, 2, 128, 1, false);
        let err = match MhinStoreFacade::new(capacity_mismatch) {
            Ok(_) => panic!("capacity mismatch should error"),
            Err(err) => err,
        };
        match err {
            MhinStoreError::ConfigurationMismatch {
                field,
                stored,
                requested,
            } => {
                assert_eq!(field, "initial_capacity");
                assert_eq!(stored, 64);
                assert_eq!(requested, 128);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn read_only_open_uses_persisted_layout_when_optional_fields_missing() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let data_dir = tmp.path().join("store");

        let initial_config = StoreConfig::new(&data_dir, 4, 32, 1, false);
        let store = MhinStoreFacade::new(initial_config).expect("initial store should open");
        drop(store);

        let read_only = StoreConfig::existing(&data_dir).with_mode(StoreMode::ReadOnly);
        let ro_store = MhinStoreFacade::new(read_only).expect("read-only store should open");

        assert_eq!(ro_store.current_block().unwrap(), 0);
    }

    #[test]
    fn facade_new_initializes_components_and_executes_operations() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let data_dir = tmp.path().join("store");

        let config = StoreConfig::new(&data_dir, 4, 32, 1, false);
        let facade = MhinStoreFacade::new(config).expect("facade should initialize");

        let key = [42u8; 8];
        facade
            .set(1, vec![Operation { key, value: 7 }])
            .expect("set should succeed");
        assert_eq!(facade.get(key).unwrap(), 7);

        facade.rollback(0).expect("rollback should succeed");

        assert!(data_dir.join("metadata").exists());
        assert!(data_dir.join("journal").exists());
        assert!(data_dir.join("snapshots").exists());

        facade.close().expect("close should succeed");
        drop(facade);

        let reopened =
            MhinStoreFacade::new(StoreConfig::existing(&data_dir)).expect("reopen should succeed");
        assert_eq!(reopened.get(key).unwrap(), 0);
    }

    #[test]
    fn facades_report_current_block() {
        let orchestrator = DummyOrchestrator::new();
        let facade = MhinStoreFacade::from_orchestrator(orchestrator.clone());
        let block_facade = MhinStoreBlockFacade::from_facade(facade.clone());

        assert_eq!(facade.current_block().unwrap(), 0);
        assert_eq!(block_facade.current_block().unwrap(), 0);

        facade
            .set(5, vec![sample_operation(1)])
            .expect("set should succeed");
        assert_eq!(facade.current_block().unwrap(), 5);
        assert_eq!(block_facade.current_block().unwrap(), 5);
    }

    #[test]
    fn facade_new_uses_parallel_orchestrator_when_requested() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let data_dir = tmp.path().join("parallel-store");
        let config = StoreConfig::new(&data_dir, 8, 64, 4, false);

        let facade = MhinStoreFacade::new(config).expect("parallel facade should initialize");
        let key_a = [1u8; 8];
        let key_b = [2u8; 8];

        facade
            .set(
                1,
                vec![
                    Operation {
                        key: key_a,
                        value: 10,
                    },
                    Operation {
                        key: key_b,
                        value: 11,
                    },
                ],
            )
            .expect("parallel set");
        assert_eq!(facade.get(key_a).unwrap(), 10);
        assert_eq!(facade.get(key_b).unwrap(), 11);

        facade
            .set(
                2,
                vec![Operation {
                    key: key_a,
                    value: 12,
                }],
            )
            .expect("second block should apply");
        assert_eq!(facade.get(key_a).unwrap(), 12);

        // Attempting to reuse a lower block height should error
        let err = facade
            .set(
                1,
                vec![
                    Operation {
                        key: key_b,
                        value: 20,
                    },
                    Operation {
                        key: [7u8; 8],
                        value: 30,
                    },
                ],
            )
            .expect_err("duplicate block height should be rejected");
        assert!(
            matches!(
                err,
                MhinStoreError::BlockIdNotIncreasing {
                    block_height: 1,
                    current: 2
                }
            ),
            "should surface BlockIdNotIncreasing error"
        );
        assert_eq!(facade.get(key_a).unwrap(), 12);
        assert_eq!(facade.get(key_b).unwrap(), 11);
        assert_eq!(facade.get([7u8; 8]).unwrap(), 0);
    }

    #[test]
    fn restore_existing_state_skips_corrupted_snapshot() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let metadata_tmp = tempdir_in(&workspace_tmp).unwrap();
        let snapshots_tmp = tempdir_in(&workspace_tmp).unwrap();

        let metadata = LmdbMetadataStore::new(metadata_tmp.path()).unwrap();

        let snapshotter = MmapSnapshotter::new(snapshots_tmp.path()).unwrap();

        let shards: Vec<Arc<dyn StateShard>> = (0..2)
            .map(|i| Arc::new(RawTableShard::new(i, 4)) as Arc<dyn StateShard>)
            .collect();

        let key = [1u8; 8];
        shards[0].import_data(vec![(key, 42)]);
        let valid_snapshot_path = snapshotter.create_snapshot(10, &shards).unwrap();
        assert!(valid_snapshot_path.exists());

        let corrupted_block = 11u64;
        let corrupted_path = snapshotter
            .root_dir()
            .join(format!("snapshot_{corrupted_block:016x}.bin"));
        fs::write(&corrupted_path, b"bad snapshot").unwrap();

        for shard in &shards {
            shard.import_data(vec![]);
        }
        assert_eq!(shards[0].get(&key), None);

        let restored =
            restore_existing_state(&snapshotter, &metadata, &shards).expect("restore should work");

        assert_eq!(restored, 10);
        assert_eq!(metadata.current_block().unwrap(), 10);
        assert!(
            !corrupted_path.exists(),
            "corrupted snapshot should be deleted"
        );
        assert_eq!(shards[0].get(&key), Some(42));
    }

    #[test]
    fn reconcile_metadata_with_journal_recovers_missing_entries() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let journal_dir = tmp.path().join("journal");
        let metadata_dir = tmp.path().join("metadata");

        let journal = FileBlockJournal::new(&journal_dir).expect("journal init");
        let metadata = LmdbMetadataStore::new(&metadata_dir).expect("metadata init");

        assert_eq!(metadata.current_block().unwrap(), 0);

        let block_height = 3;
        let undo = BlockUndo {
            block_height,
            shard_undos: Vec::new(),
        };

        let meta = journal
            .append(block_height, &undo, &[])
            .expect("journal append");

        // Simulate crash before metadata is updated
        assert_eq!(metadata.current_block().unwrap(), 0);

        let reconciled =
            reconcile_metadata_with_journal(&journal, &metadata).expect("reconciliation succeeds");

        assert_eq!(reconciled, block_height);
        assert_eq!(metadata.current_block().unwrap(), block_height);

        let offsets = metadata
            .get_journal_offsets(block_height..=block_height)
            .expect("offset fetch");
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].block_height, block_height);
        assert_eq!(offsets[0].offset, meta.offset);
        assert_eq!(offsets[0].compressed_len, meta.compressed_len);
    }

    #[test]
    fn replay_committed_blocks_errors_on_missing_journal_offsets() {
        let journal = StubJournal;
        let metadata = Arc::new(StubMetadata::default());

        let shards: Vec<Arc<dyn StateShard>> = (0..1)
            .map(|i| Arc::new(RawTableShard::new(i, 4)) as Arc<dyn StateShard>)
            .collect();
        let engine = ShardedStateEngine::new(shards, Arc::clone(&metadata));

        let block_height = 1;
        metadata
            .set_current_block(block_height)
            .expect("set current block without offsets");

        let result =
            replay_committed_blocks(&journal, metadata.as_ref(), &engine, 0 /* restored */);

        match result {
            Err(MhinStoreError::MissingJournalEntry { block }) => {
                assert_eq!(block, block_height);
            }
            other => panic!("expected MissingJournalEntry error, got {other:?}"),
        }
    }

    #[test]
    fn reconcile_metadata_with_journal_rebuilds_missing_index() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let journal_dir = tmp.path().join("journal");
        let metadata_dir = tmp.path().join("metadata");

        let journal = FileBlockJournal::new(&journal_dir).expect("journal init");
        let metadata = LmdbMetadataStore::new(&metadata_dir).expect("metadata init");

        let block_height = 7;
        let undo = BlockUndo {
            block_height,
            shard_undos: Vec::new(),
        };

        let meta = journal
            .append(block_height, &undo, &[])
            .expect("journal append");
        metadata
            .record_block_commit(block_height, &meta)
            .expect("metadata record");

        let index_path = journal.root_dir().join("journal.idx");
        assert!(index_path.exists(), "index should exist after append");
        std::fs::remove_file(&index_path).expect("remove index file");
        assert!(
            journal
                .list_entries()
                .expect("list entries after removal")
                .is_empty(),
            "index removal should yield empty list"
        );

        let reconciled =
            reconcile_metadata_with_journal(&journal, &metadata).expect("reconcile succeeds");
        assert_eq!(reconciled, block_height);

        assert!(
            index_path.exists(),
            "index should be rebuilt after reconciliation"
        );

        let entries = journal.list_entries().expect("list entries after rebuild");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].block_height, block_height);

        let offsets = metadata
            .get_journal_offsets(block_height..=block_height)
            .expect("offset fetch");
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].block_height, block_height);
        assert_eq!(offsets[0].offset, meta.offset);
        assert_eq!(metadata.current_block().unwrap(), block_height);
    }

    #[test]
    fn facade_from_orchestrator_delegates_calls() {
        let orchestrator = DummyOrchestrator::new();
        let facade = MhinStoreFacade::from_orchestrator(orchestrator.clone());

        facade.set(5, vec![sample_operation(9)]).unwrap();
        assert_eq!(
            orchestrator.applied.lock().unwrap().len(),
            1,
            "set should forward to orchestrator"
        );

        assert_eq!(facade.get([9u8; 8]).unwrap(), 9);
        assert_eq!(
            orchestrator.lookups.lock().unwrap().len(),
            1,
            "get should forward to orchestrator"
        );

        facade.rollback(0).unwrap();
        assert_eq!(
            orchestrator.revert_calls.lock().unwrap().as_slice(),
            &[0],
            "rollback should be forwarded"
        );
    }

    #[test]
    fn block_facade_stages_operations_before_commit() {
        let orchestrator = DummyOrchestrator::new();
        let base = MhinStoreFacade::from_orchestrator(orchestrator.clone());
        let facade = MhinStoreBlockFacade::from_facade(base);
        let key = [42u8; 8];

        facade.start_block(1).unwrap();
        facade.set(Operation { key, value: 5 }).unwrap();
        assert_eq!(
            facade.get(key).unwrap(),
            5,
            "staged value should be visible before commit"
        );

        // Underlying orchestrator has not been called yet.
        assert!(
            orchestrator.applied.lock().unwrap().is_empty(),
            "no operations should be applied before end_block"
        );

        facade.end_block().unwrap();
        assert_eq!(
            orchestrator.applied.lock().unwrap().len(),
            1,
            "operations should be forwarded on end_block"
        );
        assert_eq!(
            facade.get(key).unwrap(),
            5,
            "value should remain accessible after commit"
        );
    }

    #[test]
    fn block_facade_overrides_existing_values_in_staging() {
        let orchestrator = DummyOrchestrator::new();
        let base = MhinStoreFacade::from_orchestrator(orchestrator.clone());
        let facade = MhinStoreBlockFacade::from_facade(base);
        let key = [7u8; 8];

        // Seed orchestrator with existing value through regular set.
        facade
            .inner()
            .set(1, vec![Operation { key, value: 10 }])
            .unwrap();
        assert_eq!(facade.get(key).unwrap(), 10);

        facade.start_block(2).unwrap();
        facade.set(Operation { key, value: 99 }).unwrap();
        assert_eq!(
            facade.get(key).unwrap(),
            99,
            "staged set should shadow persisted value"
        );

        facade.set(Operation { key, value: 0 }).unwrap();
        assert_eq!(
            facade.get(key).unwrap(),
            0,
            "staged delete should hide value until commit"
        );

        facade.end_block().unwrap();
        assert_eq!(
            facade.get(key).unwrap(),
            0,
            "value should be removed after commit"
        );
    }

    #[test]
    fn block_facade_validates_block_lifecycle() {
        let orchestrator = DummyOrchestrator::new();
        let base = MhinStoreFacade::from_orchestrator(orchestrator);
        let facade = MhinStoreBlockFacade::from_facade(base);

        let err = facade.end_block().unwrap_err();
        assert!(
            matches!(err, MhinStoreError::NoBlockInProgress),
            "ending without start should error"
        );

        let err = facade
            .set(Operation {
                key: [0u8; 8],
                value: 1,
            })
            .unwrap_err();
        assert!(
            matches!(err, MhinStoreError::NoBlockInProgress),
            "set without start should error"
        );

        facade.start_block(10).unwrap();
        let err = facade.start_block(11).unwrap_err();
        assert!(
            matches!(err, MhinStoreError::BlockInProgress { current: 10 }),
            "starting a second block should fail"
        );
    }

    #[test]
    fn block_facade_prevents_rollback_with_pending_block() {
        let orchestrator = DummyOrchestrator::new();
        let base = MhinStoreFacade::from_orchestrator(orchestrator);
        let facade = MhinStoreBlockFacade::from_facade(base);

        facade.start_block(5).unwrap();
        let err = facade.rollback(2).unwrap_err();
        assert!(
            matches!(err, MhinStoreError::BlockInProgress { current: 5 }),
            "rollback should fail when block is staged"
        );
    }

    #[test]
    fn block_facade_restores_pending_when_commit_fails() {
        let orchestrator = FailingOrchestrator::new();
        let base = MhinStoreFacade::from_orchestrator(orchestrator);
        let facade = MhinStoreBlockFacade::from_facade(base);
        let key = [5u8; 8];

        facade.start_block(1).unwrap();
        facade.set(Operation { key, value: 21 }).unwrap();
        assert_eq!(facade.get(key).unwrap(), 21);

        let err = facade.end_block().unwrap_err();
        assert!(
            matches!(
                err,
                MhinStoreError::BlockIdNotIncreasing {
                    block_height: 1,
                    current: 1
                }
            ),
            "end_block should surface underlying error"
        );

        assert_eq!(
            facade.get(key).unwrap(),
            21,
            "staged value should still be visible after failed commit"
        );

        let err = facade.start_block(2).unwrap_err();
        assert!(
            matches!(err, MhinStoreError::BlockInProgress { current: 1 }),
            "pending block should remain active after failed commit"
        );
    }

    #[test]
    fn graceful_shutdown_persists_state_for_restart() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let data_dir = tmp.path().join("restartable-store");

        let config = StoreConfig::new(&data_dir, 4, 32, 1, false);
        let key = [0xABu8; 8];

        {
            let store = MhinStoreFacade::new(config.clone()).expect("store should initialize");
            store
                .set(1, vec![Operation { key, value: 99 }])
                .expect("set should succeed");
            store.close().expect("close should succeed");
        }

        let reopened = MhinStoreFacade::new(config).expect("store should restart");
        assert_eq!(reopened.get(key).unwrap(), 99);
    }

    #[test]
    fn restart_without_snapshot_rebuilds_state_from_journal() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let data_dir = tmp.path().join("crash-no-snapshot");

        let config = StoreConfig::new(&data_dir, 2, 8, 1, false);
        let key = [0xA5u8; 8];

        {
            let store = MhinStoreFacade::new(config.clone()).expect("store should initialize");
            store
                .set(1, vec![Operation { key, value: 7 }])
                .expect("set should succeed");
            // drop without calling close()
        }

        let reopened = MhinStoreFacade::new(config.clone())
            .expect("store should reopen even without snapshot");
        assert_eq!(
            reopened.get(key).unwrap(),
            7,
            "state should be rebuilt from journal even without snapshot"
        );
        drop(reopened);

        let metadata =
            LmdbMetadataStore::new(config.metadata_dir()).expect("metadata should reopen");
        assert_eq!(
            metadata.current_block().unwrap(),
            1,
            "current block should remain at the last committed block"
        );
    }

    #[test]
    fn restart_replays_blocks_beyond_snapshot() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let data_dir = tmp.path().join("crash-with-snapshot");

        let config = StoreConfig::new(&data_dir, 2, 8, 1, false);
        let key = [0xB6u8; 8];

        {
            let store = MhinStoreFacade::new(config.clone()).expect("store should initialize");
            store
                .set(1, vec![Operation { key, value: 10 }])
                .expect("first block should apply");
            store.close().expect("close should persist snapshot");
        }

        {
            let store =
                MhinStoreFacade::new(config.clone()).expect("store should reopen after snapshot");
            assert_eq!(
                store.get(key).unwrap(),
                10,
                "snapshot should restore first block"
            );
            store
                .set(2, vec![Operation { key, value: 99 }])
                .expect("second block should apply in-memory");
            wait_for_durable(&store, 2);
            // drop without close to simulate crash before snapshot
        }

        let reopened =
            MhinStoreFacade::new(config.clone()).expect("store should reopen after crash");
        assert_eq!(
            reopened.get(key).unwrap(),
            99,
            "state should reflect the latest committed block after journal replay"
        );
        drop(reopened);

        let metadata =
            LmdbMetadataStore::new(config.metadata_dir()).expect("metadata should reopen");
        assert_eq!(
            metadata.current_block().unwrap(),
            2,
            "current block should remain at the latest committed block after replay"
        );
    }

    #[test]
    fn rollback_removes_newer_snapshots() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let data_dir = tmp.path().join("rollback-prunes-snapshots");

        let config = StoreConfig::new(&data_dir, 2, 8, 1, false);
        let key = [0x11u8; 8];
        let snapshots_dir = data_dir.join("snapshots");
        let snapshot_path = |block: u64| snapshots_dir.join(format!("snapshot_{block:016x}.bin"));

        {
            let store = MhinStoreFacade::new(config.clone()).expect("store should initialize");
            store
                .set(1, vec![Operation { key, value: 5 }])
                .expect("first block should apply");
            store.close().expect("close should create snapshot");
        }
        assert!(
            snapshot_path(1).exists(),
            "snapshot for block 1 should exist"
        );

        {
            let store =
                MhinStoreFacade::new(config.clone()).expect("store should reopen with snapshot");
            store
                .set(2, vec![Operation { key, value: 50 }])
                .expect("second block should apply");
            store
                .close()
                .expect("close should create snapshot for block 2");
        }
        assert!(
            snapshot_path(2).exists(),
            "snapshot for block 2 should exist"
        );

        {
            let store =
                MhinStoreFacade::new(config.clone()).expect("store should reopen for rollback");
            store.rollback(1).expect("rollback should succeed");
            assert!(
                snapshot_path(1).exists(),
                "snapshot at rollback target should be retained"
            );
            assert!(
                !snapshot_path(2).exists(),
                "snapshot beyond rollback target should be removed"
            );
            assert_eq!(
                store.get(key).unwrap(),
                5,
                "state should reflect rolled back value"
            );
            store.close().expect("close after rollback should succeed");
        }

        let reopened =
            MhinStoreFacade::new(config).expect("store should reopen after rollback cleanup");
        assert_eq!(
            reopened.get(key).unwrap(),
            5,
            "state should restore the rolled back value on restart"
        );
    }

    #[test]
    fn rollback_persists_across_restart() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let data_dir = tmp.path().join("rollback-persists");

        let config = StoreConfig::new(&data_dir, 2, 16, 1, false);
        let key = [0x33u8; 8];

        {
            let store = MhinStoreFacade::new(config.clone()).expect("store should initialize");
            store
                .set(1, vec![Operation { key, value: 10 }])
                .expect("first block should apply");
            store
                .set(2, vec![Operation { key, value: 20 }])
                .expect("second block should apply");
            store.rollback(1).expect("rollback should succeed");
            assert_eq!(
                store.get(key).unwrap(),
                10,
                "rollback should restore block 1 value"
            );
            store.close().expect("close after rollback should succeed");
        }

        let reopened = MhinStoreFacade::new(config.clone()).expect("store should reopen");
        assert_eq!(
            reopened.get(key).unwrap(),
            10,
            "restart should keep rolled back state"
        );
        drop(reopened);

        let metadata =
            LmdbMetadataStore::new(config.metadata_dir()).expect("metadata should reopen");
        assert_eq!(
            metadata.current_block().unwrap(),
            1,
            "current block should remain at rollback target"
        );
    }

    #[test]
    fn restart_after_truncated_journal_tail_discards_corrupted_block() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let data_dir = tmp.path().join("truncated-tail");

        let config = StoreConfig::new(&data_dir, 2, 16, 1, false);
        let key = [0x44u8; 8];

        {
            let store = MhinStoreFacade::new(config.clone()).expect("store should initialize");
            store
                .set(1, vec![Operation { key, value: 11 }])
                .expect("first block should apply");
            store
                .set(2, vec![Operation { key, value: 22 }])
                .expect("second block should apply");
            wait_for_durable(&store, 2);
            // Drop without closing to leave journal as-is.
        }

        let journal_path = data_dir.join("journal").join("journal.bin");
        assert!(
            journal_path.exists(),
            "journal file should exist after sets"
        );
        let journal = OpenOptions::new()
            .write(true)
            .open(&journal_path)
            .expect("journal file should open");
        let len = journal.metadata().unwrap().len();
        journal
            .set_len(len.saturating_sub(5))
            .expect("should truncate journal tail");
        journal.sync_all().unwrap();
        drop(journal);

        let reopened = MhinStoreFacade::new(config.clone()).expect("store should reopen");
        assert_eq!(
            reopened.get(key).unwrap(),
            11,
            "corrupted tail block should be discarded"
        );
        drop(reopened);

        let metadata =
            LmdbMetadataStore::new(config.metadata_dir()).expect("metadata should reopen");
        assert_eq!(
            metadata.current_block().unwrap(),
            1,
            "metadata should reflect the last durable block"
        );
    }

    #[test]
    fn drop_triggers_shutdown_on_last_instance() {
        let orchestrator = ShutdownTrackingOrchestrator::new();
        let shutdown_state = Arc::new(AtomicBool::new(false));
        let facade = MhinStoreFacade::new_for_testing(
            orchestrator.clone(),
            Arc::clone(&shutdown_state),
            Arc::new(AtomicUsize::new(1)),
        );

        drop(facade);

        assert_eq!(
            orchestrator.shutdown_calls(),
            1,
            "dropping the final facade should shutdown orchestrator exactly once"
        );
        assert!(
            shutdown_state.load(Ordering::Acquire),
            "shutdown flag should be set after drop-triggered shutdown"
        );
    }

    #[test]
    fn drop_defers_shutdown_until_last_clone() {
        let orchestrator = ShutdownTrackingOrchestrator::new();
        let shutdown_state = Arc::new(AtomicBool::new(false));
        let facade = MhinStoreFacade::new_for_testing(
            orchestrator.clone(),
            Arc::clone(&shutdown_state),
            Arc::new(AtomicUsize::new(1)),
        );

        let clone = facade.clone();
        drop(facade);
        assert_eq!(
            orchestrator.shutdown_calls(),
            0,
            "shutdown should not run while clones remain"
        );
        assert!(
            !shutdown_state.load(Ordering::Acquire),
            "shutdown flag should remain unset while clones exist"
        );

        drop(clone);
        assert_eq!(
            orchestrator.shutdown_calls(),
            1,
            "shutdown should run when the last clone is dropped"
        );
        assert!(
            shutdown_state.load(Ordering::Acquire),
            "shutdown flag should be set after the final drop"
        );
    }

    #[test]
    fn close_prevents_drop_from_running_shutdown_twice() {
        let orchestrator = ShutdownTrackingOrchestrator::new();
        let shutdown_state = Arc::new(AtomicBool::new(false));
        let facade = MhinStoreFacade::new_for_testing(
            orchestrator.clone(),
            Arc::clone(&shutdown_state),
            Arc::new(AtomicUsize::new(1)),
        );

        facade.close().expect("close should succeed");
        assert_eq!(
            orchestrator.shutdown_calls(),
            1,
            "explicit close should invoke shutdown"
        );
        assert!(
            shutdown_state.load(Ordering::Acquire),
            "shutdown flag should be set after close"
        );

        drop(facade);
        assert_eq!(
            orchestrator.shutdown_calls(),
            1,
            "drop should not invoke shutdown again after close"
        );
    }

    #[test]
    fn block_facade_close_requires_no_pending_block() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let data_dir = tmp.path().join("block-close");

        let config = StoreConfig::new(&data_dir, 2, 8, 1, false);
        let facade = MhinStoreBlockFacade::new(config).expect("block facade should initialize");
        facade.start_block(7).unwrap();

        let err = facade.close().unwrap_err();
        match err {
            MhinStoreError::BlockInProgress { current } => assert_eq!(current, 7),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
