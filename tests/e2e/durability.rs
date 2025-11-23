use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime};

use rollblock::types::Operation;
use rollblock::{DurabilityMode, MhinStoreFacade, StoreConfig, StoreFacade, StoreResult};

use super::e2e_support::{
    apply_block, init_tracing, wait_for_durable, StoreHarness, DEFAULT_TIMEOUT,
    HARNESS_LMDB_MAP_SIZE,
};

#[test]
fn e2e_async_durability_queue() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("async-durability-queue")
        .durability_mode(DurabilityMode::Async {
            max_pending_blocks: 2,
        })
        .build();
    let store = harness.open()?;

    let key = [0xA1u8; 8];

    for block in 1..=4 {
        apply_block(&store, block, vec![Operation { key, value: block }])?;
    }

    let mut lag_observed = false;
    for _ in 0..20 {
        let durable = store.durable_block()?;
        let applied = store.applied_block()?;
        if applied > durable {
            lag_observed = true;
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    assert!(
        lag_observed,
        "expected asynchronous durability to introduce temporary lag"
    );

    store.ensure_healthy()?;

    wait_for_durable(&store, 4, DEFAULT_TIMEOUT)?;
    if let Some(metrics) = store.metrics() {
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.current_block_height, 4);
        assert_eq!(snapshot.durable_block_height, 4);
    }
    assert_eq!(store.get(key)?, 4);

    store.close()?;
    Ok(())
}

#[test]
fn e2e_async_empty_block_waits_for_persistence() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("async-empty-block")
        .durability_mode(DurabilityMode::Async {
            max_pending_blocks: 4,
        })
        .build();
    let store = harness.open()?;

    let key = [0xE1u8; 8];

    apply_block(&store, 1, vec![Operation { key, value: 11 }])?;
    apply_block(&store, 2, Vec::new())?;

    assert_eq!(store.applied_block()?, 2, "empty block should be applied");

    wait_for_durable(&store, 2, DEFAULT_TIMEOUT)?;

    assert_eq!(store.current_block()?, 2);
    assert_eq!(store.durable_block()?, 2);
    assert_eq!(store.applied_block()?, 2);
    assert_eq!(store.get(key)?, 11);

    store.ensure_healthy()?;
    store.close()?;
    drop(store);

    let reopened = harness.reopen()?;
    assert_eq!(reopened.current_block()?, 2);
    assert_eq!(reopened.durable_block()?, 2);
    assert_eq!(reopened.applied_block()?, 2);
    assert_eq!(reopened.get(key)?, 11);
    reopened.ensure_healthy()?;
    reopened.close()?;
    Ok(())
}

#[test]
fn e2e_sync_empty_block_crash_helper() -> StoreResult<()> {
    init_tracing();

    if env::var("ROLLBLOCK_SYNC_EMPTY_CRASH_TOKEN").is_err() {
        return Ok(());
    }

    let data_dir = PathBuf::from(
        env::var("ROLLBLOCK_SYNC_EMPTY_CRASH_DIR")
            .expect("ROLLBLOCK_SYNC_EMPTY_CRASH_DIR must be set when helper runs"),
    );

    if !data_dir.exists() {
        return Ok(());
    }

    // Apply an empty block and crash intentionally so the parent process can
    // assert that synchronous durability preserves the block height.
    let mut config = StoreConfig::new(&data_dir, 4, 64, 1, false)
        .with_lmdb_map_size(HARNESS_LMDB_MAP_SIZE)
        .without_remote_server();
    config.durability_mode = DurabilityMode::Synchronous;
    let store = MhinStoreFacade::new(config)?;

    let key = [0xF1u8; 8];

    apply_block(&store, 1, vec![Operation { key, value: 55 }])?;
    apply_block(&store, 2, Vec::new())?;
    store.ensure_healthy()?;

    std::process::abort();
}

#[test]
fn e2e_sync_empty_block_survives_crash() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("sync-empty-block-crash")
        .durability_mode(DurabilityMode::Synchronous)
        .build();

    let data_dir = harness.data_dir().to_path_buf();
    let token = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string();
    let helper_status =
        Command::new(std::env::current_exe().expect("failed to locate current test binary"))
            .arg("e2e_sync_empty_block_crash_helper")
            .env("ROLLBLOCK_SYNC_EMPTY_CRASH_TOKEN", &token)
            .env("ROLLBLOCK_SYNC_EMPTY_CRASH_DIR", &data_dir)
            .status()
            .expect("failed to run crash helper process");
    assert!(
        !helper_status.success(),
        "crash helper unexpectedly exited successfully"
    );

    // After reopening, the empty block must still be reported as durable.
    let reopened = harness.reopen()?;
    let key = [0xF1u8; 8];

    assert_eq!(reopened.current_block()?, 2);
    assert_eq!(reopened.durable_block()?, 2);
    assert_eq!(reopened.applied_block()?, 2);
    assert_eq!(reopened.get(key)?, 55);

    reopened.ensure_healthy()?;
    reopened.close()?;
    Ok(())
}

#[test]
fn e2e_sync_durability() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("sync-durability")
        .durability_mode(DurabilityMode::Synchronous)
        .build();
    let store = harness.open()?;

    let key = [0xB1u8; 8];

    apply_block(&store, 1, vec![Operation { key, value: 11 }])?;

    assert_eq!(store.current_block()?, 1);
    assert_eq!(store.applied_block()?, 1);
    assert_eq!(store.durable_block()?, 1);

    apply_block(&store, 2, vec![Operation { key, value: 22 }])?;

    assert_eq!(store.current_block()?, 2);
    assert_eq!(store.applied_block()?, 2);
    assert_eq!(store.durable_block()?, 2);
    assert_eq!(store.get(key)?, 22);

    store.ensure_healthy()?;
    store.close()?;
    drop(store);
    Ok(())
}

#[test]
fn e2e_restart_replays_journal() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("restart-replays-journal")
        .durability_mode(DurabilityMode::Async {
            max_pending_blocks: 4,
        })
        .build();
    let store = harness.open()?;

    let key = [0xC1u8; 8];

    apply_block(&store, 1, vec![Operation { key, value: 101 }])?;
    wait_for_durable(&store, 1, DEFAULT_TIMEOUT)?;

    apply_block(&store, 2, vec![Operation { key, value: 202 }])?;
    wait_for_durable(&store, 2, DEFAULT_TIMEOUT)?;
    store.ensure_healthy()?;

    drop(store);

    let reopened = harness.reopen()?;
    assert_eq!(reopened.current_block()?, 2);
    assert_eq!(reopened.applied_block()?, 2);
    assert_eq!(reopened.durable_block()?, 2);
    assert_eq!(reopened.get(key)?, 202);
    reopened.ensure_healthy()?;
    reopened.close()?;
    Ok(())
}

#[test]
fn e2e_snapshot_refresh() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("snapshot-refresh")
        .durability_mode(DurabilityMode::Async {
            max_pending_blocks: 2,
        })
        .snapshot_interval(Duration::from_secs(1))
        .max_snapshot_interval(Duration::from_secs(1))
        .build();

    let key = [0xD1u8; 8];
    {
        let store = harness.open()?;

        apply_block(&store, 1, vec![Operation { key, value: 301 }])?;
        wait_for_durable(&store, 1, DEFAULT_TIMEOUT)?;

        apply_block(&store, 2, vec![Operation { key, value: 404 }])?;
        wait_for_durable(&store, 2, DEFAULT_TIMEOUT)?;

        store.ensure_healthy()?;
        store.close()?;
    }
    thread::sleep(Duration::from_millis(10));

    let journal_dir = harness.data_dir().join("journal");
    if journal_dir.exists() {
        fs::remove_dir_all(&journal_dir)?;
        fs::create_dir_all(&journal_dir)?;
    }

    let reopened = harness.reopen()?;
    assert_eq!(reopened.current_block()?, 2);
    assert_eq!(reopened.applied_block()?, 2);
    assert_eq!(reopened.durable_block()?, 2);
    assert_eq!(reopened.get(key)?, 404);
    reopened.ensure_healthy()?;
    reopened.close()?;
    Ok(())
}
