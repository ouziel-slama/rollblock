use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::StoreResult;
use crate::state_shard::StateShard;
use crate::types::BlockId;

mod format;
mod gc;
mod reader;
mod writer;

pub trait Snapshotter: Send + Sync {
    fn create_snapshot(
        &self,
        block: BlockId,
        shards: &[Arc<dyn StateShard>],
    ) -> StoreResult<PathBuf>;
    fn load_snapshot(&self, path: &Path, shards: &[Arc<dyn StateShard>]) -> StoreResult<BlockId>;

    fn prune_snapshots_after(&self, _block: BlockId) -> StoreResult<()> {
        Ok(())
    }
}

pub struct MmapSnapshotter {
    root_dir: PathBuf,
}

impl MmapSnapshotter {
    pub fn new(root_dir: impl AsRef<Path>) -> StoreResult<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&root_dir)?;
        Ok(Self { root_dir })
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn latest_snapshot(&self) -> StoreResult<Option<(PathBuf, BlockId)>> {
        gc::latest_snapshot(&self.root_dir)
    }

    pub fn snapshots_desc(&self) -> StoreResult<Vec<(PathBuf, BlockId)>> {
        gc::snapshots_desc(&self.root_dir)
    }
}

impl Snapshotter for MmapSnapshotter {
    fn create_snapshot(
        &self,
        block: BlockId,
        shards: &[Arc<dyn StateShard>],
    ) -> StoreResult<PathBuf> {
        if let Some((existing_path, existing_block)) = self.latest_snapshot()? {
            if existing_block >= block {
                tracing::info!(
                    requested_block = block,
                    latest_block = existing_block,
                    path = ?existing_path,
                    "Skipping snapshot creation; durable height has not advanced"
                );
                return Ok(existing_path);
            }
        }

        let snapshot_path = writer::write_snapshot(&self.root_dir, block, shards)?;
        gc::cleanup_old_snapshots(&self.root_dir, block);
        Ok(snapshot_path)
    }

    fn load_snapshot(&self, path: &Path, shards: &[Arc<dyn StateShard>]) -> StoreResult<BlockId> {
        reader::load_snapshot(path, shards)
    }

    fn prune_snapshots_after(&self, block: BlockId) -> StoreResult<()> {
        gc::prune_after(&self.root_dir, block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MhinStoreError;
    use crate::state_shard::RawTableShard;
    use crate::types::{Key, Value, ValueBuf, MAX_VALUE_BYTES};
    use format::{
        checksum_to_u64, SNAPSHOT_HEADER_RESERVED, SNAPSHOT_HEADER_SIZE_V2, SNAPSHOT_VERSION,
    };
    use memmap2::Mmap;
    use std::env;
    use std::fs::{self, File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use tempfile::tempdir_in;

    fn buf(value: u64) -> ValueBuf {
        ValueBuf::from(Value::from(value))
    }

    fn opt_value(buf: Option<ValueBuf>) -> Option<Value> {
        buf.map(Value::from)
    }

    #[test]
    fn create_and_load_snapshot_with_empty_shards() {
        let workspace_tmp = env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let snapshotter = MmapSnapshotter::new(tmp.path()).unwrap();

        let shards: Vec<Arc<dyn StateShard>> = (0..4)
            .map(|i| Arc::new(RawTableShard::new(i, 16)) as Arc<dyn StateShard>)
            .collect();

        let block_height = 42;
        let snapshot_path = snapshotter.create_snapshot(block_height, &shards).unwrap();

        assert!(snapshot_path.exists());

        let loaded_block = snapshotter.load_snapshot(&snapshot_path, &shards).unwrap();
        assert_eq!(loaded_block, block_height);
    }

    #[test]
    fn snapshot_round_trip_variable_length_values() {
        let workspace_tmp = env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let snapshotter = MmapSnapshotter::new(tmp.path()).unwrap();

        let shards: Vec<Arc<dyn StateShard>> = vec![Arc::new(RawTableShard::new(0, 8))];

        let expected_entries: Vec<(Key, Vec<u8>)> = vec![
            ([0u8; 8], vec![1, 2, 3, 4, 5, 6, 7]),
            ([1u8; 8], vec![0xAA; 32]),
            ([2u8; 8], vec![0x55; MAX_VALUE_BYTES]),
        ];

        shards[0].import_data(
            expected_entries
                .iter()
                .map(|(key, bytes)| (*key, ValueBuf::from_slice(bytes)))
                .collect(),
        );

        let snapshot_path = snapshotter
            .create_snapshot(123, &shards)
            .expect("snapshot creation succeeds");

        let restore: Vec<Arc<dyn StateShard>> = vec![Arc::new(RawTableShard::new(0, 4))];
        snapshotter
            .load_snapshot(&snapshot_path, &restore)
            .expect("snapshot loads");

        let mut restored = restore[0].export_data();
        restored.sort_by(|(a, _), (b, _)| a.cmp(b));

        for ((expected_key, expected_bytes), (restored_key, restored_value)) in
            expected_entries.iter().zip(restored.iter())
        {
            assert_eq!(expected_key, restored_key);
            assert_eq!(expected_bytes.as_slice(), restored_value.as_slice());
        }
    }

    #[test]
    fn load_snapshot_detects_checksum_mismatch() {
        let workspace_tmp = env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let snapshotter = MmapSnapshotter::new(tmp.path()).unwrap();

        let shards: Vec<Arc<dyn StateShard>> = (0..2)
            .map(|i| Arc::new(RawTableShard::new(i, 8)) as Arc<dyn StateShard>)
            .collect();

        let key = [7u8; 8];
        shards[0].import_data(vec![(key, buf(77))]);

        let block_height = 55;
        let snapshot_path = snapshotter.create_snapshot(block_height, &shards).unwrap();

        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&snapshot_path)
                .unwrap();
            let len = file.metadata().unwrap().len();
            file.seek(SeekFrom::Start(len - 1)).unwrap();
            let mut byte = [0u8];
            file.read_exact(&mut byte).unwrap();
            byte[0] ^= 0xFF;
            file.seek(SeekFrom::Start(len - 1)).unwrap();
            file.write_all(&byte).unwrap();
            file.sync_all().unwrap();
        }

        let err = snapshotter
            .load_snapshot(&snapshot_path, &shards)
            .unwrap_err();

        match err {
            MhinStoreError::SnapshotCorrupted { reason, .. } => {
                assert!(reason.contains("checksum mismatch"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn create_and_load_snapshot_with_data() {
        let workspace_tmp = env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let snapshotter = MmapSnapshotter::new(tmp.path()).unwrap();

        let shards: Vec<Arc<dyn StateShard>> = (0..4)
            .map(|i| Arc::new(RawTableShard::new(i, 16)) as Arc<dyn StateShard>)
            .collect();

        let test_data = [
            vec![
                ([1, 0, 0, 0, 0, 0, 0, 0], buf(100)),
                ([2, 0, 0, 0, 0, 0, 0, 0], buf(200)),
            ],
            vec![([3, 0, 0, 0, 0, 0, 0, 0], buf(300))],
            vec![],
            vec![
                ([4, 0, 0, 0, 0, 0, 0, 0], buf(400)),
                ([5, 0, 0, 0, 0, 0, 0, 0], buf(500)),
            ],
        ];

        for (i, data) in test_data.iter().enumerate() {
            shards[i].import_data(data.clone());
        }

        assert_eq!(
            opt_value(shards[0].get(&[1, 0, 0, 0, 0, 0, 0, 0])),
            Some(100.into())
        );
        assert_eq!(
            opt_value(shards[1].get(&[3, 0, 0, 0, 0, 0, 0, 0])),
            Some(300.into())
        );
        assert_eq!(
            opt_value(shards[3].get(&[5, 0, 0, 0, 0, 0, 0, 0])),
            Some(500.into())
        );

        let block_height = 123;
        let snapshot_path = snapshotter.create_snapshot(block_height, &shards).unwrap();

        for shard in &shards {
            shard.import_data(vec![]);
        }

        let loaded_block = snapshotter.load_snapshot(&snapshot_path, &shards).unwrap();
        assert_eq!(loaded_block, block_height);

        assert_eq!(
            opt_value(shards[0].get(&[1, 0, 0, 0, 0, 0, 0, 0])),
            Some(100.into())
        );
        assert_eq!(
            opt_value(shards[0].get(&[2, 0, 0, 0, 0, 0, 0, 0])),
            Some(200.into())
        );
        assert_eq!(
            opt_value(shards[1].get(&[3, 0, 0, 0, 0, 0, 0, 0])),
            Some(300.into())
        );
        assert_eq!(
            opt_value(shards[3].get(&[4, 0, 0, 0, 0, 0, 0, 0])),
            Some(400.into())
        );
        assert_eq!(
            opt_value(shards[3].get(&[5, 0, 0, 0, 0, 0, 0, 0])),
            Some(500.into())
        );
    }

    #[test]
    fn create_snapshot_skips_when_height_unchanged() {
        let workspace_tmp = env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let snapshotter = MmapSnapshotter::new(tmp.path()).unwrap();
        let shards: Vec<Arc<dyn StateShard>> = (0..2)
            .map(|i| Arc::new(RawTableShard::new(i, 8)) as Arc<dyn StateShard>)
            .collect();

        let key = [1u8; 8];
        shards[0].import_data(vec![(key, buf(42))]);

        let block_height = 7;
        let first_path = snapshotter
            .create_snapshot(block_height, &shards)
            .expect("initial snapshot succeeds");

        assert!(first_path.exists());

        let entries: Vec<_> = fs::read_dir(tmp.path())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries.len(), 1);

        let second_path = snapshotter
            .create_snapshot(block_height, &shards)
            .expect("repeat snapshot is skipped");

        assert_eq!(first_path, second_path);

        let entries_after: Vec<_> = fs::read_dir(tmp.path())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries_after.len(), 1);
    }

    #[test]
    fn load_snapshot_validates_shard_count() {
        let workspace_tmp = env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let snapshotter = MmapSnapshotter::new(tmp.path()).unwrap();

        let shards_4: Vec<Arc<dyn StateShard>> = (0..4)
            .map(|i| Arc::new(RawTableShard::new(i, 16)) as Arc<dyn StateShard>)
            .collect();

        let shards_8: Vec<Arc<dyn StateShard>> = (0..8)
            .map(|i| Arc::new(RawTableShard::new(i, 16)) as Arc<dyn StateShard>)
            .collect();

        let block_height = 99;
        let snapshot_path = snapshotter
            .create_snapshot(block_height, &shards_4)
            .unwrap();

        let result = snapshotter.load_snapshot(&snapshot_path, &shards_8);
        assert!(result.is_err());

        match result {
            Err(crate::error::MhinStoreError::SnapshotCorrupted { reason, .. }) => {
                assert!(reason.contains("shard count mismatch"));
            }
            _ => panic!("Expected SnapshotCorrupted error"),
        }
    }

    #[test]
    fn snapshot_file_format_is_correct() {
        let workspace_tmp = env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let snapshotter = MmapSnapshotter::new(tmp.path()).unwrap();

        let shards: Vec<Arc<dyn StateShard>> = (0..2)
            .map(|i| Arc::new(RawTableShard::new(i, 16)) as Arc<dyn StateShard>)
            .collect();

        let block_height = 777;
        let snapshot_path = snapshotter.create_snapshot(block_height, &shards).unwrap();

        let file = File::open(&snapshot_path).unwrap();
        let mmap = unsafe { Mmap::map(&file).unwrap() };

        assert_eq!(&mmap[0..4], b"MHIS");

        let version = u16::from_le_bytes([mmap[4], mmap[5]]);
        assert_eq!(version, SNAPSHOT_VERSION);

        let reserved = u16::from_le_bytes([mmap[6], mmap[7]]);
        assert_eq!(reserved, SNAPSHOT_HEADER_RESERVED);

        let stored_block = u64::from_le_bytes([
            mmap[8], mmap[9], mmap[10], mmap[11], mmap[12], mmap[13], mmap[14], mmap[15],
        ]);
        assert_eq!(stored_block, block_height);

        let shard_count = u64::from_le_bytes([
            mmap[16], mmap[17], mmap[18], mmap[19], mmap[20], mmap[21], mmap[22], mmap[23],
        ]);
        assert_eq!(shard_count, 2);

        let stored_checksum = u64::from_le_bytes([
            mmap[24], mmap[25], mmap[26], mmap[27], mmap[28], mmap[29], mmap[30], mmap[31],
        ]);
        let expected_checksum = checksum_to_u64(blake3::hash(&mmap[SNAPSHOT_HEADER_SIZE_V2..]));
        assert_eq!(stored_checksum, expected_checksum);
    }

    #[test]
    fn prune_snapshots_after_removes_newer_files() {
        let workspace_tmp = env::current_dir().unwrap().join("target/testdata");
        std::fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let snapshotter = MmapSnapshotter::new(tmp.path()).unwrap();

        let shards: Vec<Arc<dyn StateShard>> = (0..2)
            .map(|i| Arc::new(RawTableShard::new(i, 4)) as Arc<dyn StateShard>)
            .collect();

        snapshotter.create_snapshot(5, &shards).unwrap();
        snapshotter.create_snapshot(10, &shards).unwrap();

        let snapshot_dir = snapshotter.root_dir();
        let snapshot_5 = snapshot_dir.join("snapshot_0000000000000005.bin");
        let snapshot_10 = snapshot_dir.join("snapshot_000000000000000a.bin");

        assert!(snapshot_5.exists());
        assert!(snapshot_10.exists());

        snapshotter.prune_snapshots_after(6).unwrap();

        assert!(snapshot_5.exists());
        assert!(!snapshot_10.exists());
    }
}
