use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::Duration;

use rollblock::error::MhinStoreError;
use rollblock::orchestrator::{BlockOrchestrator, DefaultBlockOrchestrator};
use rollblock::state_engine::ShardedStateEngine;
use rollblock::state_shard::{RawTableShard, StateShard};
use rollblock::types::{Key, ShardOp, ShardStats, ShardUndo, Value, ValueBuf};
use rollblock::FileBlockJournal;
use rollblock::MetadataStore;

use super::support::{
    operation, synchronous_settings, tempdir, MemoryMetadataStore, NoopSnapshotter,
};

struct BlockingShard {
    inner: RawTableShard,
    apply_ready: Arc<Barrier>,
    apply_release: Arc<Barrier>,
    apply_calls: AtomicUsize,
}

impl BlockingShard {
    fn new(
        shard_index: usize,
        capacity: usize,
        apply_ready: Arc<Barrier>,
        apply_release: Arc<Barrier>,
    ) -> Self {
        Self {
            inner: RawTableShard::new(shard_index, capacity),
            apply_ready,
            apply_release,
            apply_calls: AtomicUsize::new(0),
        }
    }
}

impl StateShard for BlockingShard {
    fn apply(&self, ops: &[ShardOp]) -> ShardUndo {
        let call_index = self.apply_calls.fetch_add(1, Ordering::SeqCst);
        if call_index == 1 {
            self.apply_ready.wait();
            self.apply_release.wait();
        }
        self.inner.apply(ops)
    }

    fn revert(&self, undo: &ShardUndo) {
        self.inner.revert(undo);
    }

    fn get(&self, key: &Key) -> Option<ValueBuf> {
        self.inner.get(key)
    }

    fn stats(&self) -> ShardStats {
        self.inner.stats()
    }

    fn export_data(&self) -> Vec<(Key, ValueBuf)> {
        self.inner.export_data()
    }

    fn visit_entries(&self, visitor: &mut dyn FnMut(Key, ValueBuf)) {
        self.inner.visit_entries(visitor);
    }

    fn import_data(&self, data: Vec<(Key, ValueBuf)>) {
        self.inner.import_data(data);
    }
}

#[test]
fn apply_and_revert_flow() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(FileBlockJournal::new(&journal_path).unwrap());
    let snapshotter = Arc::new(NoopSnapshotter);

    let shards: Vec<Arc<dyn StateShard>> = (0..4)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        journal,
        snapshotter,
        Arc::clone(&metadata),
        synchronous_settings(),
    )
    .unwrap();
    let key_a = [1u8; 8];
    let key_b = [2u8; 8];

    orchestrator
        .apply_operations(1, vec![operation(key_a, 10)])
        .unwrap();
    assert_eq!(orchestrator.fetch(key_a).unwrap(), 10);
    assert_eq!(metadata.current_block().unwrap(), 1);

    orchestrator
        .apply_operations(2, vec![operation(key_a, 42), operation(key_b, 7)])
        .unwrap();
    assert_eq!(orchestrator.fetch(key_a).unwrap(), 42);
    assert_eq!(orchestrator.fetch(key_b).unwrap(), 7);
    assert_eq!(metadata.current_block().unwrap(), 2);

    orchestrator.revert_to(0).unwrap();
    assert_eq!(orchestrator.fetch(key_a).unwrap(), 0);
    assert_eq!(orchestrator.fetch(key_b).unwrap(), 0);
    assert_eq!(metadata.current_block().unwrap(), 0);
}

#[test]
fn reader_gate_blocks_fetch_during_pending_apply() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(FileBlockJournal::new(&journal_path).unwrap());
    let snapshotter = Arc::new(NoopSnapshotter);

    let apply_ready = Arc::new(Barrier::new(2));
    let apply_release = Arc::new(Barrier::new(2));

    let shard: Arc<dyn StateShard> = Arc::new(BlockingShard::new(
        0,
        32,
        Arc::clone(&apply_ready),
        Arc::clone(&apply_release),
    ));

    let engine = Arc::new(ShardedStateEngine::new(vec![shard], Arc::clone(&metadata)));

    let orchestrator = Arc::new(
        DefaultBlockOrchestrator::new(
            engine,
            journal,
            snapshotter,
            Arc::clone(&metadata),
            synchronous_settings(),
        )
        .unwrap(),
    );

    let key = [1u8; 8];
    orchestrator
        .apply_operations(1, vec![operation(key, 11)])
        .unwrap();

    let writer = {
        let orchestrator = Arc::clone(&orchestrator);
        thread::spawn(move || {
            orchestrator
                .apply_operations(2, vec![operation(key, 42)])
                .unwrap();
        })
    };

    apply_ready.wait();

    let fetch_start = Arc::new(Barrier::new(2));
    let fetch_finished = Arc::new(AtomicBool::new(false));
    let fetch_handle = {
        let orchestrator = Arc::clone(&orchestrator);
        let fetch_start = Arc::clone(&fetch_start);
        let fetch_finished = Arc::clone(&fetch_finished);
        thread::spawn(move || {
            fetch_start.wait();
            let value = orchestrator.fetch(key).unwrap();
            fetch_finished.store(true, Ordering::Release);
            value
        })
    };

    // Ensure the fetch thread is ready and waiting on the reader gate.
    fetch_start.wait();

    thread::sleep(Duration::from_millis(50));
    assert!(
        !fetch_finished.load(Ordering::Acquire),
        "fetch should still be blocked while apply is pending"
    );

    apply_release.wait();

    writer.join().unwrap();
    let fetched = fetch_handle.join().unwrap();
    assert_eq!(fetched, 42);
}

#[test]
fn parallel_apply_with_rayon() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(FileBlockJournal::new(&journal_path).unwrap());
    let snapshotter = Arc::new(NoopSnapshotter);

    // Create a rayon thread pool with 4 threads
    let thread_pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap(),
    );

    // Create 16 shards to maximize parallelism
    let shards: Vec<Arc<dyn StateShard>> = (0..16)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::with_thread_pool(
        shards,
        Arc::clone(&metadata),
        Some(thread_pool.clone()),
    ));

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        journal,
        snapshotter,
        Arc::clone(&metadata),
        synchronous_settings(),
    )
    .unwrap();

    // Create many operations that will be distributed across shards
    let mut ops = Vec::new();
    for i in 0..1000 {
        let key = [
            i as u8,
            (i >> 8) as u8,
            (i >> 16) as u8,
            (i >> 24) as u8,
            0,
            0,
            0,
            0,
        ];
        ops.push(operation(key, i as u64));
    }

    // Apply operations in parallel
    orchestrator.apply_operations(1, ops.clone()).unwrap();

    // Verify some keys were set
    for i in [0, 100, 500, 999] {
        let key = [
            i as u8,
            (i >> 8) as u8,
            (i >> 16) as u8,
            (i >> 24) as u8,
            0,
            0,
            0,
            0,
        ];
        assert_eq!(orchestrator.fetch(key).unwrap(), i as u64);
    }

    // Update operations
    let mut update_ops = Vec::new();
    for i in 0..500 {
        let key = [
            i as u8,
            (i >> 8) as u8,
            (i >> 16) as u8,
            (i >> 24) as u8,
            0,
            0,
            0,
            0,
        ];
        update_ops.push(operation(key, i as u64 + 1000));
    }

    orchestrator.apply_operations(2, update_ops).unwrap();

    // Verify updates
    let key_0 = [0, 0, 0, 0, 0, 0, 0, 0];
    assert_eq!(orchestrator.fetch(key_0).unwrap(), 1000);

    // Rollback and verify
    orchestrator.revert_to(1).unwrap();
    assert_eq!(orchestrator.fetch(key_0).unwrap(), 0);

    orchestrator.revert_to(0).unwrap();
    assert_eq!(orchestrator.fetch(key_0).unwrap(), 0);
}

#[test]
fn orchestrator_supports_thread_pools() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(FileBlockJournal::new(&journal_path).unwrap());
    let snapshotter = Arc::new(NoopSnapshotter);

    let shards: Vec<Arc<dyn StateShard>> = (0..8)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    // Build thread pool separately for StateEngine
    let thread_pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap(),
    );

    let engine = Arc::new(ShardedStateEngine::with_thread_pool(
        shards,
        Arc::clone(&metadata),
        Some(thread_pool),
    ));

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        journal,
        snapshotter,
        Arc::clone(&metadata),
        synchronous_settings(),
    )
    .unwrap();

    let key_a = [1u8; 8];
    orchestrator
        .apply_operations(1, vec![operation(key_a, 42)])
        .unwrap();

    assert_eq!(orchestrator.fetch(key_a).unwrap(), 42);
}

#[test]
fn empty_blocks_and_sparse_block_heights() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(FileBlockJournal::new(&journal_path).unwrap());
    let snapshotter = Arc::new(NoopSnapshotter);

    let shards: Vec<Arc<dyn StateShard>> = (0..4)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        journal,
        snapshotter,
        Arc::clone(&metadata),
        synchronous_settings(),
    )
    .unwrap();

    let key_a = [1u8; 8];
    let key_b = [2u8; 8];

    // Block 100: Set key_a
    orchestrator
        .apply_operations(100, vec![operation(key_a, 10)])
        .unwrap();
    assert_eq!(orchestrator.fetch(key_a).unwrap(), 10);
    assert_eq!(metadata.current_block().unwrap(), 100);

    // Block 105: Set key_b (blocks 101-104 are empty)
    orchestrator
        .apply_operations(105, vec![operation(key_b, 20)])
        .unwrap();
    assert_eq!(orchestrator.fetch(key_b).unwrap(), 20);
    assert_eq!(metadata.current_block().unwrap(), 105);

    // Block 110: Update key_a (blocks 106-109 are empty)
    orchestrator
        .apply_operations(110, vec![operation(key_a, 30)])
        .unwrap();
    assert_eq!(orchestrator.fetch(key_a).unwrap(), 30);
    assert_eq!(metadata.current_block().unwrap(), 110);

    // Rollback to 107 (empty block) - should rollback to state at block 105
    orchestrator.revert_to(107).unwrap();
    assert_eq!(orchestrator.fetch(key_a).unwrap(), 10); // From block 100
    assert_eq!(orchestrator.fetch(key_b).unwrap(), 20); // From block 105
    assert_eq!(metadata.current_block().unwrap(), 107);

    // Rollback to 102 (empty block) - should rollback to state at block 100
    orchestrator.revert_to(102).unwrap();
    assert_eq!(orchestrator.fetch(key_a).unwrap(), 10);
    assert_eq!(orchestrator.fetch(key_b).unwrap(), 0); // key_b set at 105
    assert_eq!(metadata.current_block().unwrap(), 102);

    // Rollback to 50 (before any blocks) - should rollback to state at block 0
    orchestrator.revert_to(50).unwrap();
    assert_eq!(orchestrator.fetch(key_a).unwrap(), 0);
    assert_eq!(orchestrator.fetch(key_b).unwrap(), 0);
    assert_eq!(metadata.current_block().unwrap(), 50);
}

#[test]
fn empty_block_only() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(FileBlockJournal::new(&journal_path).unwrap());
    let snapshotter = Arc::new(NoopSnapshotter);

    let shards: Vec<Arc<dyn StateShard>> = (0..4)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        journal,
        snapshotter,
        Arc::clone(&metadata),
        synchronous_settings(),
    )
    .unwrap();

    // Create an empty block (no operations)
    orchestrator.apply_operations(42, vec![]).unwrap();
    assert_eq!(metadata.current_block().unwrap(), 42);

    // Create another empty block
    orchestrator.apply_operations(100, vec![]).unwrap();
    assert_eq!(metadata.current_block().unwrap(), 100);

    // Rollback to the first empty block
    orchestrator.revert_to(42).unwrap();
    assert_eq!(metadata.current_block().unwrap(), 42);
}

#[test]
fn block_height_must_be_increasing() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(FileBlockJournal::new(&journal_path).unwrap());
    let snapshotter = Arc::new(NoopSnapshotter);

    let shards: Vec<Arc<dyn StateShard>> = (0..4)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        journal,
        snapshotter,
        Arc::clone(&metadata),
        synchronous_settings(),
    )
    .unwrap();

    let key_a = [1u8; 8];

    // Block 100: Set key_a
    orchestrator
        .apply_operations(100, vec![operation(key_a, 10)])
        .unwrap();

    // Try to use block 100 again (same as current)
    let result = orchestrator.apply_operations(100, vec![operation(key_a, 20)]);
    assert!(matches!(
        result,
        Err(MhinStoreError::BlockIdNotIncreasing { .. })
    ));

    // Try to use block 50 (less than current)
    let result = orchestrator.apply_operations(50, vec![operation(key_a, 20)]);
    assert!(matches!(
        result,
        Err(MhinStoreError::BlockIdNotIncreasing { .. })
    ));

    // Block 101 should work
    orchestrator
        .apply_operations(101, vec![operation(key_a, 20)])
        .unwrap();
    assert_eq!(orchestrator.fetch(key_a).unwrap(), 20);
}

#[test]
fn empty_value_sets_delete_keys() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(FileBlockJournal::new(&journal_path).unwrap());
    let snapshotter = Arc::new(NoopSnapshotter);

    let shards: Vec<Arc<dyn StateShard>> = (0..4)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        journal,
        snapshotter,
        Arc::clone(&metadata),
        synchronous_settings(),
    )
    .unwrap();

    let key_a = [1u8; 8];

    orchestrator
        .apply_operations(1, vec![operation(key_a, 10)])
        .unwrap();
    assert_eq!(orchestrator.fetch(key_a).unwrap(), 10);

    // Update with value 0 should translate to delete
    orchestrator
        .apply_operations(2, vec![operation(key_a, Value::empty())])
        .unwrap();

    assert_eq!(orchestrator.fetch(key_a).unwrap(), 0);
    assert_eq!(metadata.current_block().unwrap(), 2);
}
