use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use rollblock::types::Operation;
use rollblock::{DurabilityMode, MhinStoreError, MhinStoreFacade, StoreFacade, StoreResult};

use super::e2e_support::{
    apply_block, init_tracing, wait_for_durable, StoreHarness, DEFAULT_TIMEOUT,
};

const FINAL_BLOCK: u64 = 5;

#[test]
fn e2e_multi_reader() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("multi-reader")
        .initial_capacity(32)
        .durability_mode(DurabilityMode::Synchronous)
        .build();
    let store = harness.open()?;
    let key = [0xE1u8; 8];

    apply_block(&store, 1, vec![Operation { key, value: 1 }])?;
    wait_for_durable(&store, 1, DEFAULT_TIMEOUT)?;

    let barrier = Arc::new(Barrier::new(3));
    let done = Arc::new(AtomicBool::new(false));

    let writer_store = store.clone();
    let writer_barrier = Arc::clone(&barrier);
    let writer_done = Arc::clone(&done);
    let writer_key = key;
    let writer_handle = thread::spawn(move || -> StoreResult<()> {
        writer_barrier.wait();

        for block in 2..=FINAL_BLOCK {
            apply_block(
                &writer_store,
                block,
                vec![Operation {
                    key: writer_key,
                    value: block,
                }],
            )?;
            wait_for_durable(&writer_store, block, DEFAULT_TIMEOUT)?;
        }

        writer_store.ensure_healthy()?;
        writer_done.store(true, Ordering::SeqCst);
        Ok(())
    });

    let reader_loop = |reader_store: MhinStoreFacade| {
        let reader_barrier = Arc::clone(&barrier);
        let reader_done = Arc::clone(&done);
        thread::spawn(move || -> StoreResult<Vec<u64>> {
            reader_barrier.wait();

            let mut observed = Vec::new();
            let mut last_seen = 1;
            let deadline = Instant::now() + DEFAULT_TIMEOUT;

            loop {
                let value = reader_store.get(key)?;
                if value > last_seen {
                    last_seen = value;
                    observed.push(value);
                    if value == FINAL_BLOCK {
                        break;
                    }
                }

                if reader_done.load(Ordering::SeqCst) && Instant::now() >= deadline {
                    panic!("reader timed out waiting for final value");
                }

                thread::sleep(Duration::from_millis(2));
            }

            reader_store.ensure_healthy()?;
            Ok(observed)
        })
    };

    let reader_one_handle = reader_loop(store.clone());
    let reader_two_handle = reader_loop(store.clone());

    writer_handle.join().expect("writer thread panicked")?;

    let reader_one_observed = reader_one_handle.join().expect("reader one panicked")?;
    let reader_two_observed = reader_two_handle.join().expect("reader two panicked")?;

    assert!(
        reader_one_observed.contains(&FINAL_BLOCK),
        "reader one did not observe final value"
    );
    assert!(
        reader_two_observed.contains(&FINAL_BLOCK),
        "reader two did not observe final value"
    );

    assert!(
        reader_one_observed
            .iter()
            .all(|value| *value >= 2 && *value <= FINAL_BLOCK),
        "reader one observed unexpected values: {:?}",
        reader_one_observed
    );
    assert!(
        reader_two_observed
            .iter()
            .all(|value| *value >= 2 && *value <= FINAL_BLOCK),
        "reader two observed unexpected values: {:?}",
        reader_two_observed
    );

    store.ensure_healthy()?;
    store.close()?;
    Ok(())
}

#[test]
fn e2e_lock_contention() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("lock-contention")
        .initial_capacity(16)
        .build();
    let store = harness.open()?;
    let config_locked = harness.config();

    let err = match MhinStoreFacade::new(config_locked.clone()) {
        Ok(_) => panic!("second writer should fail"),
        Err(err) => err,
    };
    match err {
        MhinStoreError::DataDirLocked { requested, .. } => {
            assert_eq!(requested, "exclusive");
        }
        other => panic!("unexpected error: {other:?}"),
    }

    store.ensure_healthy()?;
    store.close()?;
    drop(store);

    let reopened = MhinStoreFacade::new(config_locked)?;
    reopened.ensure_healthy()?;
    reopened.close()?;
    Ok(())
}

#[test]
fn e2e_parallel_execution() -> StoreResult<()> {
    init_tracing();

    const BATCH_SIZE: usize = 48;
    const BLOCK_COUNT: u64 = 4;

    let harness = StoreHarness::builder("parallel-execution")
        .thread_count(4)
        .initial_capacity(256)
        .build();
    let store = harness.open()?;

    let keys: Vec<[u8; 8]> = (0..BATCH_SIZE).map(|i| (i as u64).to_le_bytes()).collect();

    let initial_operations: Vec<Operation> = keys
        .iter()
        .enumerate()
        .map(|(i, &key)| Operation {
            key,
            value: 100 + i as u64,
        })
        .collect();
    apply_block(&store, 1, initial_operations)?;
    wait_for_durable(&store, 1, DEFAULT_TIMEOUT)?;

    for block in 2..=BLOCK_COUNT {
        let block_operations: Vec<Operation> = keys
            .iter()
            .enumerate()
            .map(|(i, &key)| Operation {
                key,
                value: block * 1_000 + i as u64,
            })
            .collect();
        apply_block(&store, block, block_operations)?;
        wait_for_durable(&store, block, DEFAULT_TIMEOUT)?;
    }

    store.ensure_healthy()?;

    assert_eq!(store.current_block()?, BLOCK_COUNT);
    assert_eq!(store.applied_block()?, BLOCK_COUNT);
    assert_eq!(store.durable_block()?, BLOCK_COUNT);

    for (i, &key) in keys.iter().enumerate() {
        let expected = BLOCK_COUNT * 1_000 + i as u64;
        assert_eq!(store.get(key)?, expected);
    }

    if let Some(metrics) = store.metrics() {
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.blocks_committed, BLOCK_COUNT);
        assert_eq!(
            snapshot.operations_applied,
            (BATCH_SIZE as u64) * BLOCK_COUNT
        );
        assert_eq!(snapshot.current_block_height, BLOCK_COUNT);
        assert_eq!(snapshot.durable_block_height, BLOCK_COUNT);
    }

    store.close()?;
    Ok(())
}
