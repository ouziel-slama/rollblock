use std::sync::Arc;

use rollblock::error::MhinStoreError;
use rollblock::orchestrator::{
    BlockOrchestrator, DefaultBlockOrchestrator, ReadOnlyBlockOrchestrator,
};
use rollblock::state_engine::ShardedStateEngine;
use rollblock::state_shard::{RawTableShard, StateShard};
use rollblock::FileBlockJournal;
use rollblock::MetadataStore;

use super::support::{
    operation, synchronous_settings, tempdir, MemoryMetadataStore, NoopSnapshotter,
};

#[test]
fn read_only_orchestrator_reports_restored_height_when_metadata_lags() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata.set_current_block(0).unwrap();

    let shards: Vec<Arc<dyn StateShard>> =
        vec![Arc::new(RawTableShard::new(0, 16)) as Arc<dyn StateShard>];
    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let restored_block = 42;
    let orchestrator =
        ReadOnlyBlockOrchestrator::new(engine, Arc::clone(&metadata), restored_block)
            .expect("read-only orchestrator should initialize");

    assert_eq!(orchestrator.applied_block_height(), restored_block);

    let metrics_snapshot = orchestrator.metrics().unwrap().snapshot();
    assert_eq!(metrics_snapshot.current_block_height, restored_block);
    assert_eq!(metrics_snapshot.durable_block_height, restored_block);

    // Metadata still reports height 0, but the orchestrator must surface restored height.
    assert_eq!(metadata.current_block().unwrap(), 0);
    assert_eq!(orchestrator.current_block().unwrap(), restored_block);

    // Once metadata catches up, the orchestrator should advance as well.
    metadata
        .set_current_block(restored_block + 1)
        .expect("metadata update should succeed");
    assert_eq!(orchestrator.current_block().unwrap(), restored_block + 1);
    assert_eq!(orchestrator.applied_block_height(), restored_block + 1);
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
fn zero_value_sets_delete_keys() {
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
        .apply_operations(2, vec![operation(key_a, 0)])
        .unwrap();

    assert_eq!(orchestrator.fetch(key_a).unwrap(), 0);
    assert_eq!(metadata.current_block().unwrap(), 2);
}
