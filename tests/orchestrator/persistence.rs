use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rollblock::block_journal::BlockJournal;
use rollblock::error::MhinStoreError;
use rollblock::orchestrator::{BlockOrchestrator, DefaultBlockOrchestrator};
use rollblock::state_engine::ShardedStateEngine;
use rollblock::state_shard::{RawTableShard, StateShard};
use rollblock::FileBlockJournal;
use rollblock::MetadataStore;

use super::support::{
    async_settings, operation, synchronous_settings, tempdir, wait_for_block, FailingMetadataStore,
    FlakyJournal, MemoryMetadataStore, NoopSnapshotter, SlowJournal, SlowSnapshotter,
};

#[test]
fn async_persistence_failure_is_fatal() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let inner_journal = FileBlockJournal::new(&journal_path).unwrap();
    let journal = Arc::new(FlakyJournal::new(inner_journal, 2));
    let snapshotter = Arc::new(NoopSnapshotter);

    let shards: Vec<Arc<dyn StateShard>> = (0..2)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let persistence_settings = async_settings(4);

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        journal,
        snapshotter,
        Arc::clone(&metadata),
        persistence_settings,
    )
    .unwrap();

    let key_a = [1u8; 8];
    orchestrator
        .apply_operations(1, vec![operation(key_a, 10)])
        .unwrap();

    let key_b = [2u8; 8];
    orchestrator
        .apply_operations(2, vec![operation(key_b, 20)])
        .unwrap();

    for _ in 0..100 {
        if orchestrator.ensure_healthy().is_err() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    let fatal = orchestrator.ensure_healthy().unwrap_err();
    match fatal {
        MhinStoreError::DurabilityFailure { block, .. } => assert_eq!(block, 2),
        other => panic!("unexpected error: {other:?}"),
    }

    let key_c = [3u8; 8];
    let err = orchestrator
        .apply_operations(3, vec![operation(key_c, 30)])
        .unwrap_err();
    match err {
        MhinStoreError::DurabilityFailure { block, .. } => assert_eq!(block, 2),
        other => panic!("unexpected error: {other:?}"),
    }

    let err = orchestrator.fetch(key_b).unwrap_err();
    match err {
        MhinStoreError::DurabilityFailure { block, .. } => assert_eq!(block, 2),
        other => panic!("unexpected error: {other:?}"),
    }

    let err = orchestrator.fetch(key_a).unwrap_err();
    match err {
        MhinStoreError::DurabilityFailure { block, .. } => assert_eq!(block, 2),
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(metadata.current_block().unwrap(), 1);

    let shutdown_err = orchestrator.shutdown().unwrap_err();
    match shutdown_err {
        MhinStoreError::DurabilityFailure { block, .. } => assert_eq!(block, 2),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn async_empty_block_eventually_becomes_durable() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(FileBlockJournal::new(&journal_path).unwrap());
    let snapshotter = Arc::new(NoopSnapshotter);

    let shards: Vec<Arc<dyn StateShard>> = (0..2)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let persistence_settings = async_settings(4);

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        journal,
        snapshotter,
        Arc::clone(&metadata),
        persistence_settings,
    )
    .unwrap();

    let key = [0xE1u8; 8];

    orchestrator
        .apply_operations(1, vec![operation(key, 11)])
        .unwrap();

    wait_for_block(&metadata, 1);

    orchestrator.apply_operations(2, Vec::new()).unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if orchestrator.durable_block_height().unwrap() >= 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for durable block to reach height 2"
        );
        thread::sleep(Duration::from_millis(5));
    }

    assert_eq!(orchestrator.applied_block_height(), 2);
    orchestrator.shutdown().unwrap();
}

#[test]
fn async_snapshots_do_not_block_queue() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(FileBlockJournal::new(&journal_path).unwrap());
    let snapshotter = Arc::new(SlowSnapshotter::new(Duration::from_millis(500)));
    let snapshot_in_progress = snapshotter.in_progress_flag();

    let shards: Vec<Arc<dyn StateShard>> = (0..2)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let mut persistence_settings = async_settings(4);
    persistence_settings.snapshot_interval = Duration::from_millis(5);
    persistence_settings.max_snapshot_interval = Duration::from_millis(5);

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        journal,
        snapshotter,
        Arc::clone(&metadata),
        persistence_settings,
    )
    .unwrap();

    orchestrator
        .apply_operations(1, vec![operation([0x11u8; 8], 11)])
        .unwrap();
    wait_for_block(&metadata, 1);

    let snapshot_deadline = Instant::now() + Duration::from_secs(2);
    while !snapshot_in_progress.load(Ordering::Acquire) {
        assert!(
            Instant::now() < snapshot_deadline,
            "snapshot worker never started"
        );
        thread::sleep(Duration::from_millis(5));
    }

    for block in 2..=5 {
        let key = [block as u8; 8];
        orchestrator
            .apply_operations(block, vec![operation(key, block * 10)])
            .unwrap();
    }

    let durable_deadline = Instant::now() + Duration::from_secs(2);
    let mut progressed_while_snapshot = false;
    loop {
        let durable = orchestrator.durable_block_height().unwrap();
        if durable >= 5 {
            if snapshot_in_progress.load(Ordering::Acquire) {
                progressed_while_snapshot = true;
            }
            break;
        }

        if snapshot_in_progress.load(Ordering::Acquire) && durable >= 3 {
            progressed_while_snapshot = true;
        }

        assert!(
            Instant::now() < durable_deadline,
            "timed out waiting for durability to reach block 5"
        );
        thread::sleep(Duration::from_millis(5));
    }

    assert!(
        progressed_while_snapshot,
        "durable height failed to advance while snapshot thread was running"
    );

    orchestrator.shutdown().unwrap();
}

#[test]
fn forces_snapshot_after_max_interval() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(FileBlockJournal::new(&journal_path).unwrap());
    let snapshotter = Arc::new(SlowSnapshotter::new(Duration::from_millis(25)));
    let snapshot_flag = snapshotter.in_progress_flag();

    let shards: Vec<Arc<dyn StateShard>> = (0..2)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let mut persistence_settings = async_settings(4);
    persistence_settings.snapshot_interval = Duration::from_secs(3600);
    persistence_settings.max_snapshot_interval = Duration::from_millis(30);

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        journal,
        Arc::clone(&snapshotter),
        Arc::clone(&metadata),
        persistence_settings,
    )
    .unwrap();

    let key_a = [0xA1u8; 8];
    orchestrator
        .apply_operations(1, vec![operation(key_a, 1)])
        .unwrap();
    wait_for_block(&metadata, 1);

    thread::sleep(Duration::from_millis(50));

    let monitor_flag = Arc::clone(&snapshot_flag);
    let monitor = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < deadline {
            if monitor_flag.load(Ordering::Acquire) {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        panic!("forced snapshot never started");
    });

    let key_b = [0xB2u8; 8];
    orchestrator
        .apply_operations(2, vec![operation(key_b, 2)])
        .unwrap();
    wait_for_block(&metadata, 2);

    monitor.join().unwrap();
    orchestrator.shutdown().unwrap();
}

#[test]
fn async_rollback_handles_inflight_persistence() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(SlowJournal::new(
        FileBlockJournal::new(&journal_path).unwrap(),
        [3],
        Duration::from_millis(200),
    ));
    let snapshotter = Arc::new(NoopSnapshotter);

    let shards: Vec<Arc<dyn StateShard>> = (0..2)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let persistence_settings = async_settings(4);

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        journal,
        snapshotter,
        Arc::clone(&metadata),
        persistence_settings,
    )
    .unwrap();

    let key_a = [1u8; 8];
    orchestrator
        .apply_operations(1, vec![operation(key_a, 10)])
        .unwrap();
    wait_for_block(&metadata, 1);

    let key_b = [2u8; 8];
    orchestrator
        .apply_operations(2, vec![operation(key_b, 20)])
        .unwrap();
    wait_for_block(&metadata, 2);

    let key_c = [3u8; 8];
    orchestrator
        .apply_operations(3, vec![operation(key_c, 30)])
        .unwrap();

    thread::sleep(Duration::from_millis(50));
    assert_eq!(orchestrator.durable_block_height().unwrap(), 2);

    orchestrator.revert_to(2).unwrap();

    assert_eq!(orchestrator.fetch(key_c).unwrap(), 0);
    assert_eq!(orchestrator.fetch(key_b).unwrap(), 20);
    assert_eq!(metadata.current_block().unwrap(), 2);
    assert_eq!(orchestrator.durable_block_height().unwrap(), 2);
    assert_eq!(orchestrator.applied_block_height(), 2);

    thread::sleep(Duration::from_millis(300));
    assert_eq!(orchestrator.durable_block_height().unwrap(), 2);
    assert_eq!(metadata.current_block().unwrap(), 2);
    assert_eq!(orchestrator.fetch(key_c).unwrap(), 0);

    orchestrator.shutdown().unwrap();
}

#[test]
fn async_rollback_discarded_inflight_persistence_is_skipped() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(SlowJournal::new(
        FileBlockJournal::new(&journal_path).unwrap(),
        [3],
        Duration::from_millis(200),
    ));
    let snapshotter = Arc::new(NoopSnapshotter);

    let shards: Vec<Arc<dyn StateShard>> = (0..2)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let persistence_settings = async_settings(4);

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        Arc::clone(&journal),
        snapshotter,
        Arc::clone(&metadata),
        persistence_settings,
    )
    .unwrap();

    let key_a = [21u8; 8];
    orchestrator
        .apply_operations(1, vec![operation(key_a, 10)])
        .unwrap();
    wait_for_block(&metadata, 1);

    let key_b = [22u8; 8];
    orchestrator
        .apply_operations(2, vec![operation(key_b, 20)])
        .unwrap();
    wait_for_block(&metadata, 2);

    let key_c = [23u8; 8];
    orchestrator
        .apply_operations(3, vec![operation(key_c, 30)])
        .unwrap();

    thread::sleep(Duration::from_millis(50));
    orchestrator.revert_to(2).unwrap();

    thread::sleep(Duration::from_millis(300));

    assert_eq!(metadata.current_block().unwrap(), 2);
    assert_eq!(orchestrator.durable_block_height().unwrap(), 2);
    assert_eq!(orchestrator.fetch(key_c).unwrap(), 0);
    assert!(!metadata.has_offset(3));
    let entries = journal.list_entries().unwrap();
    assert!(entries.iter().all(|meta| meta.block_height <= 2));

    orchestrator.shutdown().unwrap();
}

#[test]
fn async_rollback_to_non_persisted_target_keeps_state() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(MemoryMetadataStore::new());
    let journal = Arc::new(SlowJournal::new(
        FileBlockJournal::new(&journal_path).unwrap(),
        [3],
        Duration::from_millis(200),
    ));
    let snapshotter = Arc::new(NoopSnapshotter);

    let shards: Vec<Arc<dyn StateShard>> = (0..2)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let persistence_settings = async_settings(4);

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        journal,
        snapshotter,
        Arc::clone(&metadata),
        persistence_settings,
    )
    .unwrap();

    let key_a = [10u8; 8];
    orchestrator
        .apply_operations(1, vec![operation(key_a, 42)])
        .unwrap();
    wait_for_block(&metadata, 1);

    let key_b = [11u8; 8];
    orchestrator
        .apply_operations(2, vec![operation(key_b, 84)])
        .unwrap();
    wait_for_block(&metadata, 2);

    let key_c = [12u8; 8];
    orchestrator
        .apply_operations(3, vec![operation(key_c, 126)])
        .unwrap();

    thread::sleep(Duration::from_millis(50));
    assert_eq!(orchestrator.durable_block_height().unwrap(), 2);

    orchestrator.revert_to(3).unwrap();

    assert_eq!(orchestrator.fetch(key_c).unwrap(), 126);
    assert_eq!(orchestrator.applied_block_height(), 3);
    assert_eq!(orchestrator.durable_block_height().unwrap(), 2);
    assert_eq!(metadata.current_block().unwrap(), 2);

    wait_for_block(&metadata, 3);
    assert_eq!(orchestrator.durable_block_height().unwrap(), 3);
    assert_eq!(metadata.current_block().unwrap(), 3);
    assert_eq!(orchestrator.fetch(key_c).unwrap(), 126);

    orchestrator.shutdown().unwrap();
}

#[test]
fn synchronous_persistence_failure_is_fatal() {
    let tmp = tempdir();
    let journal_path = tmp.path().join("journal");

    let metadata = Arc::new(FailingMetadataStore::new(2));
    let journal = Arc::new(FileBlockJournal::new(&journal_path).unwrap());
    let snapshotter = Arc::new(NoopSnapshotter);

    let shards: Vec<Arc<dyn StateShard>> = (0..2)
        .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
        .collect();

    let engine = Arc::new(ShardedStateEngine::new(shards, Arc::clone(&metadata)));

    let orchestrator = DefaultBlockOrchestrator::new(
        engine,
        Arc::clone(&journal),
        snapshotter,
        Arc::clone(&metadata),
        synchronous_settings(),
    )
    .unwrap();

    let key_a = [1u8; 8];
    orchestrator
        .apply_operations(1, vec![operation(key_a, 10)])
        .unwrap();

    let key_b = [2u8; 8];
    let err = orchestrator
        .apply_operations(2, vec![operation(key_b, 20)])
        .unwrap_err();
    assert!(matches!(err, MhinStoreError::Io(_)));

    let err = orchestrator
        .apply_operations(3, vec![operation(key_b, 21)])
        .unwrap_err();
    match err {
        MhinStoreError::DurabilityFailure { block, .. } => assert_eq!(block, 2),
        other => panic!("unexpected error: {other:?}"),
    }

    let entries = journal.list_entries().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].block_height, 1);

    assert_eq!(metadata.current_block().unwrap(), 1);

    let err = orchestrator.fetch(key_a).unwrap_err();
    match err {
        MhinStoreError::DurabilityFailure { block, .. } => assert_eq!(block, 2),
        other => panic!("unexpected error: {other:?}"),
    }
}
