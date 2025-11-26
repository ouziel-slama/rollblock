use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

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
        apply_block(
            &store,
            block,
            vec![Operation {
                key,
                value: (block).into(),
            }],
        )?;
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

    apply_block(
        &store,
        1,
        vec![Operation {
            key,
            value: 11.into(),
        }],
    )?;
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
fn e2e_async_relaxed_rollback_truncates_non_durable_blocks() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("async-relaxed-rollback")
        .durability_mode(DurabilityMode::AsyncRelaxed {
            max_pending_blocks: 8,
            sync_every_n_blocks: 50,
        })
        .build();
    let store = harness.open()?;

    let key = [0xAAu8; 8];
    store.enable_relaxed_mode(25)?;

    for block in 1u64..=5 {
        apply_block(
            &store,
            block,
            vec![Operation {
                key,
                value: block.into(),
            }],
        )?;
    }

    wait_for_blocks_committed(&store, 5)?;
    let applied = store.applied_block()?;
    assert_eq!(applied, 5);
    let durable = store.durable_block()?;
    assert!(
        durable < applied,
        "expected durable < applied under relaxed mode (durable={}, applied={})",
        durable,
        applied
    );

    store.rollback(4)?;

    // After rollback in relaxed mode, current_block is clamped to highest_committed which
    // could be 0 if no blocks were synced yet (sync every 25 blocks, only 5 applied).
    assert!(
        store.current_block()? <= 4,
        "current block should not exceed rollback target"
    );
    assert_eq!(store.applied_block()?, 4);
    assert_eq!(store.get(key)?, 4);
    assert!(
        store.durable_block()? <= 4,
        "durable block should not exceed rollback target"
    );

    store.disable_relaxed_mode()?;
    assert_eq!(store.durable_block()?, store.current_block()?);

    store.ensure_healthy()?;
    store.close()?;
    Ok(())
}

#[test]
fn e2e_async_relaxed_boot_respects_configured_policy() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("async-relaxed-boot-config")
        .durability_mode(DurabilityMode::AsyncRelaxed {
            max_pending_blocks: 8,
            sync_every_n_blocks: 4,
        })
        .build();
    let store = harness.open()?;
    let key = [0xACu8; 8];

    for block in 1u64..=3 {
        apply_block(
            &store,
            block,
            vec![Operation {
                key,
                value: block.into(),
            }],
        )?;
        wait_for_blocks_committed(&store, block)?;

        let durable = store.durable_block()?;
        if block == 1 {
            assert_eq!(
                durable, 1,
                "first block should sync because the journal is created"
            );
        } else {
            assert_eq!(
                durable, 1,
                "durability should not advance again before sync interval elapses (block {})",
                block
            );
        }
    }

    apply_block(
        &store,
        4,
        vec![Operation {
            key,
            value: 4.into(),
        }],
    )?;
    wait_for_blocks_committed(&store, 4)?;
    wait_for_durable(&store, 4, DEFAULT_TIMEOUT)?;

    assert_eq!(store.applied_block()?, 4);
    assert_eq!(store.durable_block()?, 4);
    assert_eq!(store.get(key)?, 4);

    store.ensure_healthy()?;
    store.close()?;
    drop(store);

    let reopened = harness.reopen()?;
    assert_eq!(reopened.durable_block()?, 4);
    assert_eq!(reopened.get(key)?, 4);
    reopened.ensure_healthy()?;
    reopened.close()?;
    Ok(())
}

#[test]
fn e2e_sync_relaxed_toggle_restores_sync_mode() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("sync-relaxed-toggle")
        .durability_mode(DurabilityMode::Synchronous)
        .build();
    let store = harness.open()?;
    let key = [0xB2u8; 8];

    store.enable_relaxed_mode(25)?;
    apply_block(
        &store,
        1,
        vec![Operation {
            key,
            value: 111.into(),
        }],
    )?;

    store.disable_relaxed_mode()?;
    store.ensure_healthy()?;
    store.close()?;
    drop(store);

    let reopened = harness.reopen()?;
    apply_block(
        &reopened,
        2,
        vec![Operation {
            key,
            value: 222.into(),
        }],
    )?;

    assert_eq!(
        reopened.applied_block()?,
        2,
        "second block should apply synchronously after toggling"
    );
    assert_eq!(
        reopened.durable_block()?,
        2,
        "durable height must match applied height for synchronous mode"
    );
    assert_eq!(reopened.get(key)?, 222);

    reopened.ensure_healthy()?;
    reopened.close()?;
    Ok(())
}

#[test]
fn e2e_sync_relaxed_shutdown_preserves_snapshot() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("sync-relaxed-shutdown-preserves-snapshot")
        .durability_mode(DurabilityMode::Synchronous)
        .build();

    let key = [0xB3u8; 8];

    {
        let store = harness.open()?;
        store.enable_relaxed_mode(25)?;

        for block in 1u64..=3 {
            apply_block(
                &store,
                block,
                vec![Operation {
                    key,
                    value: block.into(),
                }],
            )?;
        }

        assert_eq!(store.applied_block()?, 3);
        store.ensure_healthy()?;
        store.close()?;
    }

    let reopened = harness.reopen()?;
    assert_eq!(reopened.current_block()?, 3);
    assert_eq!(reopened.applied_block()?, 3);
    assert_eq!(reopened.durable_block()?, 3);
    assert_eq!(reopened.get(key)?, 3);

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

    apply_block(
        &store,
        1,
        vec![Operation {
            key,
            value: 55.into(),
        }],
    )?;
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
fn e2e_async_relaxed_crash_helper() -> StoreResult<()> {
    init_tracing();

    if env::var("ROLLBLOCK_ASYNC_RELAXED_CRASH_TOKEN").is_err() {
        return Ok(());
    }

    let data_dir = PathBuf::from(
        env::var("ROLLBLOCK_ASYNC_RELAXED_CRASH_DIR")
            .expect("ROLLBLOCK_ASYNC_RELAXED_CRASH_DIR must be set when helper runs"),
    );

    if !data_dir.exists() {
        return Ok(());
    }

    let mut config = StoreConfig::new(&data_dir, 4, 64, 1, false)
        .with_lmdb_map_size(HARNESS_LMDB_MAP_SIZE)
        .without_remote_server();
    config.durability_mode = DurabilityMode::Async {
        max_pending_blocks: 8,
    };

    let store = MhinStoreFacade::new(config)?;
    let key = [0xE2u8; 8];

    apply_block(
        &store,
        1,
        vec![Operation {
            key,
            value: 1.into(),
        }],
    )?;
    wait_for_durable(&store, 1, DEFAULT_TIMEOUT)?;

    store.enable_relaxed_mode(128)?;

    for block in 2u64..=5 {
        apply_block(
            &store,
            block,
            vec![Operation {
                key,
                value: block.into(),
            }],
        )?;
    }

    let durable_before_crash = store.durable_block()?;
    let applied_before_crash = store.applied_block()?;
    assert_eq!(applied_before_crash, 5);
    assert!(
        durable_before_crash < applied_before_crash,
        "expected relaxed async persistence to leave pending blocks before crash"
    );
    assert_eq!(store.get(key)?, applied_before_crash);

    let expectations_path = data_dir.join("async_relaxed_crash_expectations");
    let mut file = File::create(&expectations_path)
        .expect("failed to record async relaxed crash expectations");
    writeln!(file, "{durable_before_crash},{applied_before_crash}")
        .expect("failed to write async relaxed crash expectations");
    file.sync_all()
        .expect("failed to flush async relaxed crash expectations to disk");

    std::process::abort();
}

#[test]
fn e2e_async_relaxed_crash_recovers() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("async-relaxed-crash-recovers")
        .durability_mode(DurabilityMode::Async {
            max_pending_blocks: 8,
        })
        .build();

    let data_dir = harness.data_dir().to_path_buf();
    let token = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string();
    let helper_status =
        Command::new(std::env::current_exe().expect("failed to locate current test binary"))
            .arg("e2e_async_relaxed_crash_helper")
            .env("ROLLBLOCK_ASYNC_RELAXED_CRASH_TOKEN", &token)
            .env("ROLLBLOCK_ASYNC_RELAXED_CRASH_DIR", &data_dir)
            .status()
            .expect("failed to run async relaxed crash helper process");
    assert!(
        !helper_status.success(),
        "async relaxed crash helper unexpectedly exited successfully"
    );

    let expectations_path = data_dir.join("async_relaxed_crash_expectations");
    let expectations = fs::read_to_string(&expectations_path)
        .expect("failed to read async relaxed crash expectations after helper");
    let mut parts = expectations.trim().split(',');
    let durable_before: u64 = parts
        .next()
        .and_then(|value| value.parse().ok())
        .expect("missing durable expectation");
    let applied_before: u64 = parts
        .next()
        .and_then(|value| value.parse().ok())
        .expect("missing applied expectation");
    let _ = fs::remove_file(&expectations_path);

    let reopened = harness.reopen()?;
    let key = [0xE2u8; 8];

    let current = reopened.current_block()?;
    assert!(
        current >= durable_before && current <= applied_before,
        "recovered current block {} must be within recorded range [{}, {}]",
        current,
        durable_before,
        applied_before
    );
    assert_eq!(reopened.durable_block()?, current);
    assert_eq!(reopened.applied_block()?, current);
    assert_eq!(reopened.get(key)?, current);

    let next_block = current + 1;
    apply_block(
        &reopened,
        next_block,
        vec![Operation {
            key,
            value: next_block.into(),
        }],
    )?;

    let applied = reopened.applied_block()?;
    assert_eq!(applied, next_block);
    let durable = reopened.durable_block()?;
    assert!(
        durable < applied,
        "reopened store should remain in async-relaxed mode (durable={}, applied={})",
        durable,
        applied
    );

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

    apply_block(
        &store,
        1,
        vec![Operation {
            key,
            value: 11.into(),
        }],
    )?;

    assert_eq!(store.current_block()?, 1);
    assert_eq!(store.applied_block()?, 1);
    assert_eq!(store.durable_block()?, 1);

    apply_block(
        &store,
        2,
        vec![Operation {
            key,
            value: 22.into(),
        }],
    )?;

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

    apply_block(
        &store,
        1,
        vec![Operation {
            key,
            value: 101.into(),
        }],
    )?;
    wait_for_durable(&store, 1, DEFAULT_TIMEOUT)?;

    apply_block(
        &store,
        2,
        vec![Operation {
            key,
            value: 202.into(),
        }],
    )?;
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

        apply_block(
            &store,
            1,
            vec![Operation {
                key,
                value: 301.into(),
            }],
        )?;
        wait_for_durable(&store, 1, DEFAULT_TIMEOUT)?;

        apply_block(
            &store,
            2,
            vec![Operation {
                key,
                value: 404.into(),
            }],
        )?;
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

fn wait_for_blocks_committed(store: &MhinStoreFacade, expected: u64) -> StoreResult<()> {
    let deadline = Instant::now() + DEFAULT_TIMEOUT;

    loop {
        store.ensure_healthy()?;
        if let Some(metrics) = store.metrics() {
            let snapshot = metrics.snapshot();
            if snapshot.blocks_committed >= expected {
                return Ok(());
            }
        } else {
            panic!("store metrics are unavailable");
        }

        if Instant::now() >= deadline {
            panic!("timed out waiting for {} blocks to commit", expected);
        }

        thread::sleep(Duration::from_millis(5));
    }
}
