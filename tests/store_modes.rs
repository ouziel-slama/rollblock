use std::fs;
use std::path::PathBuf;
use std::sync::Once;

use rollblock::types::Operation;
use rollblock::{MhinStoreError, MhinStoreFacade, StoreConfig, StoreFacade};
use tempfile::tempdir_in;

static INIT_TESTDATA_ROOT: Once = Once::new();

fn temp_store_dir(name: &str) -> PathBuf {
    let workspace_tmp = std::env::current_dir()
        .unwrap()
        .join("target/testdata/store_modes");
    INIT_TESTDATA_ROOT.call_once(|| {
        if std::env::var_os("ROLLBLOCK_KEEP_TESTDATA").is_none() {
            let _ = fs::remove_dir_all(&workspace_tmp);
        }
    });
    fs::create_dir_all(&workspace_tmp).unwrap();
    let tmp = tempdir_in(&workspace_tmp).unwrap();
    tmp.path().join(name)
}

#[test]
fn second_writer_fails_fast() {
    let data_dir = temp_store_dir("writer-lock");
    let config = StoreConfig::new(&data_dir, 2, 16, 1, false).without_remote_server();
    let writer = MhinStoreFacade::new(config.clone()).expect("first writer");

    let err = match MhinStoreFacade::new(config.clone()) {
        Ok(_) => panic!("second writer should fail"),
        Err(err) => err,
    };
    match err {
        MhinStoreError::DataDirLocked { requested, .. } => {
            assert_eq!(requested, "exclusive");
        }
        other => panic!("unexpected error: {other:?}"),
    }

    drop(writer);
    MhinStoreFacade::new(config).expect("writer should reopen after drop");
}

#[test]
fn active_handle_blocks_second_open_even_for_reads() {
    let data_dir = temp_store_dir("exclusive-lock");
    let config = StoreConfig::new(&data_dir, 2, 16, 1, false).without_remote_server();
    let key = [0xAAu8; 8];

    let writer = MhinStoreFacade::new(config.clone()).expect("initial writer");
    writer
        .set(
            1,
            vec![Operation {
                key,
                value: 11.into(),
            }],
        )
        .expect("write should succeed");
    assert_eq!(writer.get(key).unwrap(), 11);

    let err = match MhinStoreFacade::new(config.clone()) {
        Ok(_) => panic!("second handle should fail"),
        Err(err) => err,
    };
    match err {
        MhinStoreError::DataDirLocked { requested, .. } => {
            assert_eq!(requested, "exclusive");
        }
        other => panic!("unexpected error: {other:?}"),
    }

    drop(writer);
    let reopened = MhinStoreFacade::new(config).expect("reopen after drop");
    assert_eq!(reopened.get(key).unwrap(), 11);
    reopened.close().expect("reopened close");
}

#[test]
fn existing_config_recovers_lmdb_map_size() {
    let data_dir = temp_store_dir("map-size-recovery");
    let custom_map_size = 128usize << 20;

    {
        let config = StoreConfig::new(&data_dir, 2, 16, 1, false)
            .with_lmdb_map_size(custom_map_size)
            .without_remote_server();
        let store = MhinStoreFacade::new(config).expect("initial writer");
        store.close().expect("initial store close");
    }

    let existing_config = StoreConfig::existing_with_lmdb_map_size(&data_dir, custom_map_size)
        .without_remote_server();
    assert_eq!(
        existing_config.lmdb_map_size, custom_map_size,
        "StoreConfig::existing should recover the configured LMDB map size"
    );

    let reopened = MhinStoreFacade::new(existing_config).expect("reopen with recovered map size");
    reopened.close().expect("reopened store close");
}
