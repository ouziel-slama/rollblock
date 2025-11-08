use std::fs;
use std::path::PathBuf;

use rollblock::types::Operation;
use rollblock::{MhinStoreError, MhinStoreFacade, StoreConfig, StoreFacade, StoreMode};
use tempfile::tempdir_in;

fn temp_store_dir(name: &str) -> PathBuf {
    let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
    fs::create_dir_all(&workspace_tmp).unwrap();
    let tmp = tempdir_in(&workspace_tmp).unwrap();
    tmp.path().join(name)
}

#[test]
fn second_writer_fails_fast() {
    let data_dir = temp_store_dir("writer-lock");
    let config = StoreConfig::new(&data_dir, 2, 16, 1, false);
    let writer = MhinStoreFacade::new(config.clone()).expect("first writer");

    let err = match MhinStoreFacade::new(config.clone()) {
        Ok(_) => panic!("second writer should fail"),
        Err(err) => err,
    };
    match err {
        MhinStoreError::DataDirLocked { requested, .. } => {
            assert_eq!(requested, "read-write");
        }
        other => panic!("unexpected error: {other:?}"),
    }

    drop(writer);
    MhinStoreFacade::new(config).expect("writer should reopen after drop");
}

#[test]
fn multiple_readers_can_share_lock() {
    let data_dir = temp_store_dir("shared-readers");
    let writer_config = StoreConfig::new(&data_dir, 2, 16, 1, false);
    let key = [0x42u8; 8];

    {
        let writer = MhinStoreFacade::new(writer_config.clone()).expect("writer init");
        writer
            .set(1, vec![Operation { key, value: 7 }])
            .expect("write should succeed");
        writer.close().expect("writer close");
    }

    let reader_config = writer_config.clone().with_mode(StoreMode::ReadOnly);
    let reader_one = MhinStoreFacade::new(reader_config.clone()).expect("first reader");
    let reader_two = MhinStoreFacade::new(reader_config).expect("second reader");

    assert_eq!(reader_one.get(key).unwrap(), 7);
    assert_eq!(reader_two.get(key).unwrap(), 7);

    let err = reader_one
        .set(2, vec![Operation { key, value: 9 }])
        .expect_err("read-only set should fail");
    match err {
        MhinStoreError::ReadOnlyOperation { operation } => {
            assert_eq!(operation, "apply_operations");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn existing_config_recovers_lmdb_map_size() {
    let data_dir = temp_store_dir("map-size-recovery");
    let custom_map_size = 128usize << 20;

    {
        let config =
            StoreConfig::new(&data_dir, 2, 16, 1, false).with_lmdb_map_size(custom_map_size);
        let store = MhinStoreFacade::new(config).expect("initial writer");
        store.close().expect("initial store close");
    }

    let existing_config = StoreConfig::existing(&data_dir);
    assert_eq!(
        existing_config.lmdb_map_size, custom_map_size,
        "StoreConfig::existing should recover the configured LMDB map size"
    );

    let reopened = MhinStoreFacade::new(existing_config).expect("reopen with recovered map size");
    reopened.close().expect("reopened store close");
}
