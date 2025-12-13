use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::time::{Duration, Instant};

use rollblock::block_journal::FileBlockJournal;
use rollblock::storage::metadata::{GcWatermark, LmdbMetadataStore};
use rollblock::types::{Operation, StoreKey as Key, Value};
use rollblock::{
    BlockJournal, DurabilityMode, MetadataStore, SimpleStoreFacade, StoreConfig, StoreError,
    StoreFacade,
};
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
    let config = StoreConfig::new(&data_dir, 2, 16, 1, false)
        .expect("valid config")
        .without_remote_server();
    let writer = SimpleStoreFacade::new(config.clone()).expect("first writer");

    let err = match SimpleStoreFacade::new(config.clone()) {
        Ok(_) => panic!("second writer should fail"),
        Err(err) => err,
    };
    match err {
        StoreError::DataDirLocked { requested, .. } => {
            assert_eq!(requested, "exclusive");
        }
        other => panic!("unexpected error: {other:?}"),
    }

    drop(writer);
    SimpleStoreFacade::new(config).expect("writer should reopen after drop");
}

#[test]
fn active_handle_blocks_second_open_even_for_reads() {
    let data_dir = temp_store_dir("exclusive-lock");
    let config = StoreConfig::new(&data_dir, 2, 16, 1, false)
        .expect("valid config")
        .without_remote_server();
    let key: Key = [0xAAu8; Key::BYTES].into();

    let writer = SimpleStoreFacade::new(config.clone()).expect("initial writer");
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

    let err = match SimpleStoreFacade::new(config.clone()) {
        Ok(_) => panic!("second handle should fail"),
        Err(err) => err,
    };
    match err {
        StoreError::DataDirLocked { requested, .. } => {
            assert_eq!(requested, "exclusive");
        }
        other => panic!("unexpected error: {other:?}"),
    }

    drop(writer);
    let reopened = SimpleStoreFacade::new(config).expect("reopen after drop");
    assert_eq!(reopened.get(key).unwrap(), 11);
    reopened.close().expect("reopened close");
}

#[test]
fn existing_config_recovers_lmdb_map_size() {
    let data_dir = temp_store_dir("map-size-recovery");
    let custom_map_size = 128usize << 20;

    {
        let config = StoreConfig::new(&data_dir, 2, 16, 1, false)
            .expect("valid config")
            .with_lmdb_map_size(custom_map_size)
            .without_remote_server();
        let store = SimpleStoreFacade::new(config).expect("initial writer");
        store.close().expect("initial store close");
    }

    let existing_config = StoreConfig::existing_with_lmdb_map_size(&data_dir, custom_map_size)
        .without_remote_server();
    assert_eq!(
        existing_config.lmdb_map_size, custom_map_size,
        "StoreConfig::existing should recover the configured LMDB map size"
    );

    let reopened = SimpleStoreFacade::new(existing_config).expect("reopen with recovered map size");
    reopened.close().expect("reopened store close");
}

#[test]
fn background_pruner_removes_old_chunks_and_caps_rollback() {
    let data_dir = temp_store_dir("pruner-removal");
    let config = pruning_config(&data_dir);
    let store = SimpleStoreFacade::new(config.clone()).expect("store opens");
    eprintln!("store opened for pruner-removal");

    write_blocks(&store, 1, 20).expect("blocks persisted");
    eprintln!("blocks written");

    assert!(
        wait_for_condition(Duration::from_secs(5), || chunk_paths(config.journal_dir())
            .len()
            >= 3),
        "expected multiple sealed chunks before pruning runs"
    );
    eprintln!("enough chunks observed");

    let oldest = chunk_paths(config.journal_dir())
        .into_iter()
        .min()
        .expect("chunks exist");
    assert!(
        wait_for_condition(Duration::from_secs(5), || !oldest.exists()),
        "expected oldest chunk to be pruned"
    );
    eprintln!("oldest chunk pruned");

    store.close().expect("store closed");
    eprintln!("store closed after pruning");

    let metadata = LmdbMetadataStore::new_with_map_size(config.metadata_dir(), 32 << 20)
        .expect("metadata reopens");
    let retained = metadata
        .get_journal_offsets(0..=1)
        .expect("read pruned offsets");
    assert!(
        retained.is_empty(),
        "expected metadata to drop pruned history, found {retained:?}"
    );
}

#[test]
fn pruning_survives_restart_and_limits_history() {
    let data_dir = temp_store_dir("pruner-restart");
    let config = pruning_config(&data_dir);
    let store = SimpleStoreFacade::new(config.clone()).expect("store opens");
    let final_block = 20;

    write_blocks(&store, 1, final_block).expect("blocks persisted");
    assert!(
        wait_for_condition(Duration::from_secs(5), || chunk_paths(config.journal_dir())
            .len()
            >= 3),
        "expected multiple chunks before pruning"
    );
    let first_chunk = chunk_paths(config.journal_dir())
        .into_iter()
        .min()
        .expect("chunks exist");
    assert!(
        wait_for_condition(Duration::from_secs(5), || !first_chunk.exists()),
        "expected pruning before restart"
    );
    store.close().expect("initial close");

    let reopen_config =
        StoreConfig::existing_with_lmdb_map_size(&data_dir, 32 << 20).without_remote_server();
    let reopened = SimpleStoreFacade::new(reopen_config).expect("store reopens");
    assert_eq!(
        reopened.current_block().expect("current block loads"),
        final_block
    );

    reopened.close().expect("reopened close");

    let metadata =
        LmdbMetadataStore::new_with_map_size(config.metadata_dir(), 32 << 20).expect("metadata");
    let earliest_after_prune = metadata.get_journal_offsets(0..=5).expect("read offsets");
    assert!(
        earliest_after_prune.is_empty(),
        "expected pruned metadata to be empty, found {earliest_after_prune:?}"
    );
}

#[test]
fn pending_prune_plan_replays_on_restart() {
    let data_dir = temp_store_dir("pruner-replay");
    let config = pruning_config(&data_dir)
        .with_prune_interval(Duration::from_secs(60))
        .expect("custom interval");
    let store = SimpleStoreFacade::new(config.clone()).expect("store opens");
    write_blocks(&store, 1, 24).expect("blocks persisted");
    store.close().expect("initial close");

    let journal_dir = config.journal_dir();
    let metadata_dir = config.metadata_dir();
    let journal = FileBlockJournal::new(&journal_dir).expect("journal reopens");
    let metadata =
        LmdbMetadataStore::new_with_map_size(&metadata_dir, 32 << 20).expect("metadata reopens");

    let current = metadata.current_block().expect("current block loads");
    let prune_target = current.saturating_sub(6).max(1);
    let mut plan = journal
        .plan_prune_chunks_ending_at_or_before(prune_target)
        .expect("plan calculation succeeds")
        .expect("plan should exist with multiple chunks");

    let chunk_plan = plan.chunk_plan().clone();
    let staged_index_path = plan.staged_index_path().to_path_buf();
    let baseline_entry_count = plan.baseline_entry_count();
    let report = plan.report().clone();

    let watermark = GcWatermark {
        pruned_through: report.pruned_through,
        chunk_plan: chunk_plan.clone(),
        staged_index_path: staged_index_path.clone(),
        baseline_entry_count,
        report: report.clone(),
    };
    metadata
        .store_gc_watermark(&watermark)
        .expect("watermark persisted");
    plan.mark_persisted();
    metadata
        .prune_journal_offsets_at_or_before(report.pruned_through)
        .expect("metadata prune succeeds");
    drop(plan);
    drop(journal);
    drop(metadata);

    let reopen_config =
        StoreConfig::existing_with_lmdb_map_size(&data_dir, 32 << 20).without_remote_server();
    let reopened = SimpleStoreFacade::new(reopen_config).expect("store reopens");
    reopened.close().expect("reopened close");

    let metadata_check =
        LmdbMetadataStore::new_with_map_size(&metadata_dir, 32 << 20).expect("metadata reopens");
    assert!(
        metadata_check
            .load_gc_watermark()
            .expect("watermark loads")
            .is_none(),
        "pending plan watermark should be cleared after replay"
    );
    drop(metadata_check);

    for chunk_id in chunk_plan.chunk_ids {
        let path = journal_dir.join(chunk_filename(chunk_id));
        assert!(
            !path.exists(),
            "chunk {chunk_id} should be deleted after recovery replay"
        );
    }
    assert!(
        !staged_index_path.exists(),
        "staged index should be removed after replay"
    );
}

fn write_blocks(store: &dyn StoreFacade, start: u64, count: u64) -> rollblock::StoreResult<()> {
    for offset in 0..count {
        let height = start + offset;
        store.set(height, vec![heavy_operation(height)])?;
    }
    Ok(())
}

fn heavy_operation(height: u64) -> Operation {
    let key = Key::from_u64_le(height);
    let mut payload = vec![0u8; 256];
    payload[0] = (height & 0xFF) as u8;
    Operation {
        key,
        value: Value::from(payload),
    }
}

fn pruning_config(data_dir: &Path) -> StoreConfig {
    StoreConfig::new(data_dir, 2, 16, 1, false)
        .expect("valid config")
        .with_lmdb_map_size(32 << 20)
        .with_journal_chunk_size(1 << 10)
        .with_min_rollback_window(8)
        .expect("min window set")
        .with_prune_interval(Duration::from_millis(100))
        .expect("interval set")
        .with_bootstrap_block_profile(1)
        .expect("bootstrap profile set")
        .with_durability_mode(DurabilityMode::Synchronous)
        .without_remote_server()
}

fn chunk_paths(dir: PathBuf) -> Vec<PathBuf> {
    if !dir.exists() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("journal.") && name.ends_with(".bin") {
                    chunks.push(entry.path());
                }
            }
        }
    }
    chunks.sort();
    chunks
}

fn chunk_filename(chunk_id: u32) -> String {
    format!("journal.{chunk_id:08}.bin")
}

fn wait_for_condition<F>(timeout: Duration, mut predicate: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}
