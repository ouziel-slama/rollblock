use std::fs;
use std::thread;
use std::time::{Duration, Instant};

use rollblock::metrics::HealthState;
use rollblock::types::{Operation, StoreKey as Key};
use rollblock::Value;
use rollblock::{DurabilityMode, MhinStoreError, StoreFacade, StoreResult};

use super::e2e_support::{
    apply_block, init_tracing, wait_for_durable, StoreHarness, DEFAULT_TIMEOUT,
};

#[test]
fn e2e_metrics_health() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("metrics-health")
        .durability_mode(DurabilityMode::Synchronous)
        .initial_capacity(32)
        .build();
    let store = harness.open()?;

    let key_a: Key = [0xA1u8; Key::BYTES].into();
    let key_b: Key = [0xB2u8; Key::BYTES].into();

    apply_block(
        &store,
        1,
        vec![
            Operation {
                key: key_a,
                value: 10.into(),
            },
            Operation {
                key: key_b,
                value: 20.into(),
            },
        ],
    )?;
    wait_for_durable(&store, 1, DEFAULT_TIMEOUT)?;

    apply_block(
        &store,
        2,
        vec![
            Operation {
                key: key_a,
                value: 15.into(),
            },
            Operation {
                key: key_b,
                value: Value::empty(),
            },
        ],
    )?;
    wait_for_durable(&store, 2, DEFAULT_TIMEOUT)?;

    let metrics = store.metrics().expect("metrics should be available");
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.blocks_committed, 2);
    assert_eq!(snapshot.operations_applied, 4);
    assert_eq!(snapshot.set_operations_applied, 3);
    assert_eq!(snapshot.zero_value_deletes_applied, 1);
    assert_eq!(snapshot.current_block_height, 2);
    assert_eq!(snapshot.durable_block_height, 2);
    assert_eq!(snapshot.failed_operations, 0);

    let health = store.health().expect("health should be available");
    assert_eq!(health.state, HealthState::Healthy);

    let journal_dir = harness.data_dir().join("journal");
    if journal_dir.exists() {
        fs::remove_dir_all(&journal_dir)?;
    }

    let failure = apply_block(
        &store,
        3,
        vec![Operation {
            key: key_a,
            value: 99.into(),
        }],
    )
    .expect_err("set should fail when journal directory is missing");

    match failure {
        MhinStoreError::Io(_) | MhinStoreError::DurabilityFailure { .. } => {}
        other => panic!("unexpected error: {other:?}"),
    }

    let health = store
        .health()
        .expect("health should remain available after failure");
    assert_eq!(health.state, HealthState::Unhealthy);

    let failure_snapshot = store.metrics().unwrap().snapshot();
    assert!(
        failure_snapshot.failed_operations >= 1,
        "expected failed operations to be recorded"
    );

    drop(store);
    Ok(())
}

#[test]
fn e2e_error_propagation() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("error-propagation")
        .durability_mode(DurabilityMode::Async {
            max_pending_blocks: 2,
        })
        .initial_capacity(32)
        .build();
    let store = harness.open()?;

    let key: Key = [0xC3u8; Key::BYTES].into();

    apply_block(
        &store,
        1,
        vec![Operation {
            key,
            value: 1.into(),
        }],
    )?;
    wait_for_durable(&store, 1, DEFAULT_TIMEOUT)?;

    let journal_dir = harness.data_dir().join("journal");
    let backup_dir = harness.data_dir().join("journal_backup");
    if backup_dir.exists() {
        fs::remove_dir_all(&backup_dir)?;
    }
    fs::rename(&journal_dir, &backup_dir)?;

    apply_block(
        &store,
        2,
        vec![Operation {
            key,
            value: 2.into(),
        }],
    )?;

    assert_eq!(store.get(key)?, 2);

    let deadline = Instant::now() + DEFAULT_TIMEOUT;
    let failure_err = loop {
        match store.ensure_healthy() {
            Ok(()) => {
                if Instant::now() >= deadline {
                    panic!("timed out waiting for durability failure to surface");
                }
                thread::sleep(Duration::from_millis(5));
            }
            Err(err) => break err,
        }
    };

    match failure_err {
        MhinStoreError::DurabilityFailure { block, .. } => assert_eq!(block, 2),
        other => panic!("unexpected error: {other:?}"),
    }

    fs::rename(&backup_dir, &journal_dir)?;

    drop(store);

    let reopened = harness.reopen()?;
    assert_eq!(reopened.current_block()?, 1);
    assert_eq!(reopened.get(key)?, 1);
    reopened.ensure_healthy()?;
    reopened.close()?;
    Ok(())
}
