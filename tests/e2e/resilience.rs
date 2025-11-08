use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

use rollblock::types::Operation;
use rollblock::{DurabilityMode, MhinStoreError, StoreFacade, StoreResult};

use super::e2e_support::{apply_block, init_tracing, wait_for_durable, StoreHarness, DEFAULT_TIMEOUT};

#[test]
fn e2e_checksum_corruption() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("checksum-corruption")
        .durability_mode(DurabilityMode::Synchronous)
        .compress_journal(false)
        .initial_capacity(64)
        .build();
    let store = harness.open()?;

    let key = [0xD1u8; 8];
    apply_block(&store, 1, vec![Operation { key, value: 123 }])?;
    wait_for_durable(&store, 1, DEFAULT_TIMEOUT)?;
    store.close()?;
    drop(store);

    let journal_path = harness.data_dir().join("journal").join("journal.bin");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&journal_path)?;
    let mut header = [0u8; 40];
    file.read_exact(&mut header)?;
    header[36] ^= 0xFF;
    file.seek(SeekFrom::Start(36))?;
    file.write_all(&header[36..40])?;
    file.sync_all()?;
    drop(file);

    match harness.reopen() {
        Err(MhinStoreError::JournalChecksumMismatch { block }) => assert_eq!(block, 1),
        Err(other) => panic!("unexpected error when reopening: {other:?}"),
        Ok(store) => {
            store.close()?;
            panic!("expected checksum mismatch during reopen");
        }
    }

    Ok(())
}

#[test]
#[cfg_attr(
    not(feature = "slow-tests"),
    ignore = "enable with --features slow-tests to include slow benchmarks"
)]
fn e2e_large_batch_bounds() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("large-batch-bounds")
        .durability_mode(DurabilityMode::Async {
            max_pending_blocks: 4,
        })
        .thread_count(4)
        .initial_capacity(8_192)
        .compress_journal(false)
        .build();
    let store = harness.open()?;

    let operation_count: usize = 5_000;
    let mut operations = Vec::with_capacity(operation_count);
    for i in 0..operation_count {
        let key = (i as u64).to_le_bytes();
        operations.push(Operation {
            key,
            // Zero-value operations are treated as deletes; offset by 1 to ensure insertion.
            value: i as u64 + 1,
        });
    }

    let start = Instant::now();
    apply_block(&store, 1, operations)?;
    wait_for_durable(&store, 1, Duration::from_secs(5))?;
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(5),
        "large batch apply took {elapsed:?}"
    );

    store.ensure_healthy()?;

    let metrics = store.metrics().expect("metrics should be available");
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.blocks_committed, 1);
    assert_eq!(snapshot.operations_applied, operation_count as u64);
    assert_eq!(snapshot.set_operations_applied, operation_count as u64);
    assert_eq!(snapshot.total_keys_stored, operation_count);
    assert_eq!(snapshot.checksum_errors, 0);

    store.close()?;

    Ok(())
}
