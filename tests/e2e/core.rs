use rollblock::types::Operation;
use rollblock::{StoreFacade, StoreResult};

use super::e2e_support::{apply_block, init_tracing, wait_for_durable, StoreHarness, DEFAULT_TIMEOUT};

#[test]
fn e2e_basic_lifecycle() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("basic-lifecycle")
        .initial_capacity(32)
        .build();
    let store = harness.open()?;

    let key = [1u8; 8];

    let current = store.current_block()?;
    let applied = store.applied_block();
    let durable = store.durable_block()?;
    assert_eq!(current, 0);
    assert_eq!(applied, 0);
    assert_eq!(durable, 0);

    apply_block(&store, 1, vec![Operation { key, value: 100 }])?;
    wait_for_durable(&store, 1, DEFAULT_TIMEOUT)?;

    let current = store.current_block()?;
    let applied = store.applied_block();
    let durable = store.durable_block()?;
    let value = store.get(key)?;
    assert_eq!(current, 1);
    assert_eq!(applied, 1);
    assert_eq!(durable, 1);
    assert_eq!(value, 100);

    apply_block(&store, 2, vec![Operation { key, value: 200 }])?;
    wait_for_durable(&store, 2, DEFAULT_TIMEOUT)?;

    let current = store.current_block()?;
    let applied = store.applied_block();
    let durable = store.durable_block()?;
    let value = store.get(key)?;
    assert_eq!(current, 2);
    assert_eq!(applied, 2);
    assert_eq!(durable, 2);
    assert_eq!(value, 200);

    apply_block(&store, 3, vec![Operation { key, value: 0 }])?;
    wait_for_durable(&store, 3, DEFAULT_TIMEOUT)?;

    let current = store.current_block()?;
    let applied = store.applied_block();
    let durable = store.durable_block()?;
    let value = store.get(key)?;
    assert_eq!(current, 3);
    assert_eq!(applied, 3);
    assert!(durable >= 3);
    assert_eq!(value, 0);

    store.rollback(1)?;
    let current = store.current_block()?;
    let applied = store.applied_block();
    let value = store.get(key)?;
    assert_eq!(current, 1);
    assert_eq!(applied, 1);
    assert_eq!(value, 100);

    store.close()?;
    Ok(())
}

#[test]
fn e2e_zero_value_auto_delete() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("zero-value-auto-delete").build();
    let store = harness.open()?;

    let key = [2u8; 8];

    apply_block(&store, 1, vec![Operation { key, value: 7 }])?;
    wait_for_durable(&store, 1, DEFAULT_TIMEOUT)?;
    let value = store.get(key)?;
    assert_eq!(value, 7);

    apply_block(&store, 2, vec![Operation { key, value: 0 }])?;
    wait_for_durable(&store, 2, DEFAULT_TIMEOUT)?;

    let current = store.current_block()?;
    let applied = store.applied_block();
    let value = store.get(key)?;
    assert_eq!(current, 2);
    assert_eq!(applied, 2);
    assert_eq!(value, 0);

    store.close()?;
    Ok(())
}

#[test]
fn e2e_sparse_blocks() -> StoreResult<()> {
    init_tracing();

    let harness = StoreHarness::builder("sparse-blocks")
        .initial_capacity(64)
        .build();
    let store = harness.open()?;

    let key_a = [10u8; 8];
    let key_b = [11u8; 8];
    let key_c = [12u8; 8];
    let key_d = [13u8; 8];

    apply_block(
        &store,
        100,
        vec![Operation {
            key: key_a,
            value: 100,
        }],
    )?;
    wait_for_durable(&store, 100, DEFAULT_TIMEOUT)?;

    apply_block(
        &store,
        500,
        vec![Operation {
            key: key_b,
            value: 500,
        }],
    )?;
    wait_for_durable(&store, 500, DEFAULT_TIMEOUT)?;

    apply_block(
        &store,
        1000,
        vec![Operation {
            key: key_c,
            value: 1000,
        }],
    )?;
    wait_for_durable(&store, 1000, DEFAULT_TIMEOUT)?;

    apply_block(
        &store,
        1500,
        vec![Operation {
            key: key_a,
            value: 1500,
        }],
    )?;
    wait_for_durable(&store, 1500, DEFAULT_TIMEOUT)?;

    let current = store.current_block()?;
    let value_a = store.get(key_a)?;
    let value_b = store.get(key_b)?;
    let value_c = store.get(key_c)?;
    assert_eq!(current, 1500);
    assert_eq!(value_a, 1500);
    assert_eq!(value_b, 500);
    assert_eq!(value_c, 1000);

    store.rollback(750)?;
    let current = store.current_block()?;
    let value_a = store.get(key_a)?;
    let value_b = store.get(key_b)?;
    let value_c = store.get(key_c)?;
    assert_eq!(current, 750);
    assert_eq!(value_a, 100);
    assert_eq!(value_b, 500);
    assert_eq!(value_c, 0);

    store.rollback(300)?;
    let current = store.current_block()?;
    let value_a = store.get(key_a)?;
    let value_b = store.get(key_b)?;
    let value_c = store.get(key_c)?;
    assert_eq!(current, 300);
    assert_eq!(value_a, 100);
    assert_eq!(value_b, 0);
    assert_eq!(value_c, 0);

    apply_block(&store, 2000, vec![])?;
    wait_for_durable(&store, 2000, DEFAULT_TIMEOUT)?;
    let current = store.current_block()?;
    assert_eq!(current, 2000);

    apply_block(&store, 3000, vec![])?;
    wait_for_durable(&store, 3000, DEFAULT_TIMEOUT)?;
    let current = store.current_block()?;
    assert_eq!(current, 3000);

    apply_block(
        &store,
        4000,
        vec![Operation {
            key: key_d,
            value: 4000,
        }],
    )?;
    wait_for_durable(&store, 4000, DEFAULT_TIMEOUT)?;
    let value_d = store.get(key_d)?;
    assert_eq!(value_d, 4000);

    store.rollback(3000)?;
    let current = store.current_block()?;
    let value_d = store.get(key_d)?;
    let value_a = store.get(key_a)?;
    assert_eq!(current, 3000);
    assert_eq!(value_d, 0);
    assert_eq!(value_a, 100);

    store.close()?;
    Ok(())
}
