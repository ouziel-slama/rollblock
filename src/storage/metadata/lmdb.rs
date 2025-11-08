use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use heed::byteorder::BigEndian;
use heed::types::{Bytes, SerdeBincode, Str, U64};
use heed::{Database, Env};

use crate::error::{MhinStoreError, StoreResult};
use crate::orchestrator::DurabilityMode;
use crate::storage::metadata::{MetadataStore, ShardLayout};
use crate::types::{BlockId, JournalMeta};

mod durability;
mod env;
mod layout;

use env::EnvHandles;

pub struct LmdbMetadataStore {
    env: Arc<Env>,
    path: PathBuf,
    state_db: Database<Str, SerdeBincode<BlockId>>,
    config_db: Database<Str, Bytes>,
    journal_offsets_db: Database<U64<BigEndian>, SerdeBincode<JournalMeta>>,
}

impl LmdbMetadataStore {
    const CURRENT_BLOCK_KEY: &'static str = "current_block";
    const SHARD_LAYOUT_KEY: &'static str = "shard_layout";
    const DURABILITY_MODE_KEY: &'static str = "durability_mode";
    const LMDB_MAP_SIZE_KEY: &'static str = "lmdb_map_size";
    const DEFAULT_MAP_SIZE: usize = env::DEFAULT_MAP_SIZE;

    /// Creates a new LMDB metadata store with the default map size.
    ///
    /// The default size (2GB) is sufficient for Bitcoin mainnet and all testnets.
    pub fn new(path: impl AsRef<Path>) -> StoreResult<Self> {
        Self::new_with_map_size(path, Self::DEFAULT_MAP_SIZE)
    }

    /// Creates a new LMDB metadata store with a custom map size.
    ///
    /// # Arguments
    ///
    /// * `path` - Directory path for the LMDB environment
    /// * `map_size` - Maximum size in bytes for the LMDB database
    ///
    /// # Map Size Guidelines
    ///
    /// - **Bitcoin (all networks)**: 2GB (default) - handles ~35M blocks
    /// - **High-frequency chains**: 10GB - handles ~178M blocks
    /// - **Extreme cases**: 50GB+ - handles ~895M+ blocks
    ///
    /// Note: On 64-bit systems, reserving virtual address space is free until actually used.
    pub fn new_with_map_size(path: impl AsRef<Path>, map_size: usize) -> StoreResult<Self> {
        let handles = env::open_rw(path.as_ref(), map_size)?;
        let store = Self::from_handles(handles);

        let actual_map_size = store.effective_map_size();
        store.ensure_lmdb_map_size(actual_map_size)?;

        Ok(store)
    }

    pub fn env(&self) -> Arc<Env> {
        Arc::clone(&self.env)
    }

    pub fn effective_map_size(&self) -> usize {
        self.env.info().map_size
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn from_handles(handles: EnvHandles) -> Self {
        Self {
            env: Arc::new(handles.env),
            path: handles.path,
            state_db: handles.state_db,
            config_db: handles.config_db,
            journal_offsets_db: handles.journal_offsets_db,
        }
    }

    /// Opens an existing LMDB metadata store in read-only mode with the default map size.
    pub fn open_read_only(path: impl AsRef<Path>) -> StoreResult<Self> {
        Self::open_read_only_with_map_size(path, Self::DEFAULT_MAP_SIZE)
    }

    /// Opens an existing LMDB metadata store in read-only mode with a custom map size.
    ///
    /// # Arguments
    ///
    /// * `path` - Directory path for the LMDB environment
    /// * `map_size` - Maximum size in bytes for the LMDB database
    pub fn open_read_only_with_map_size(
        path: impl AsRef<Path>,
        map_size: usize,
    ) -> StoreResult<Self> {
        let handles = env::open_ro(path.as_ref(), map_size)?;
        Ok(Self::from_handles(handles))
    }

    pub fn load_shard_layout(&self) -> StoreResult<Option<ShardLayout>> {
        layout::load(self.env.as_ref(), &self.config_db, Self::SHARD_LAYOUT_KEY)
    }

    pub fn store_shard_layout(&self, layout: &ShardLayout) -> StoreResult<()> {
        layout::store(
            self.env.as_ref(),
            &self.config_db,
            Self::SHARD_LAYOUT_KEY,
            layout,
        )
    }

    pub fn load_durability_mode(&self) -> StoreResult<Option<DurabilityMode>> {
        durability::load_mode(
            self.env.as_ref(),
            &self.config_db,
            Self::DURABILITY_MODE_KEY,
        )
    }

    pub fn store_durability_mode(&self, mode: &DurabilityMode) -> StoreResult<()> {
        durability::store_mode(
            self.env.as_ref(),
            &self.config_db,
            Self::DURABILITY_MODE_KEY,
            mode,
        )
    }

    pub fn load_lmdb_map_size(&self) -> StoreResult<Option<usize>> {
        durability::load_map_size(self.env.as_ref(), &self.config_db, Self::LMDB_MAP_SIZE_KEY)
    }

    pub fn store_lmdb_map_size(&self, map_size: usize) -> StoreResult<()> {
        durability::store_map_size(
            self.env.as_ref(),
            &self.config_db,
            Self::LMDB_MAP_SIZE_KEY,
            map_size,
        )
    }

    pub fn ensure_lmdb_map_size(&self, map_size: usize) -> StoreResult<()> {
        durability::ensure_map_size(
            self.env.as_ref(),
            &self.config_db,
            Self::LMDB_MAP_SIZE_KEY,
            map_size,
        )
    }
}

impl Clone for LmdbMetadataStore {
    fn clone(&self) -> Self {
        Self {
            env: Arc::clone(&self.env),
            path: self.path.clone(),
            state_db: self.state_db,
            config_db: self.config_db,
            journal_offsets_db: self.journal_offsets_db,
        }
    }
}

impl MetadataStore for LmdbMetadataStore {
    fn current_block(&self) -> StoreResult<BlockId> {
        let txn = self.env.read_txn()?;
        let value = self.state_db.get(&txn, Self::CURRENT_BLOCK_KEY)?;
        Ok(value.unwrap_or(0))
    }

    fn set_current_block(&self, block: BlockId) -> StoreResult<()> {
        let mut txn = self.env.write_txn()?;
        self.state_db
            .put(&mut txn, Self::CURRENT_BLOCK_KEY, &block)?;
        txn.commit()?;
        Ok(())
    }

    fn put_journal_offset(&self, block: BlockId, meta: &JournalMeta) -> StoreResult<()> {
        let mut txn = self.env.write_txn()?;
        self.journal_offsets_db.put(&mut txn, &block, meta)?;
        txn.commit()?;
        Ok(())
    }

    fn get_journal_offsets(&self, range: RangeInclusive<BlockId>) -> StoreResult<Vec<JournalMeta>> {
        let start = *range.start();
        let end = *range.end();

        if start > end {
            return Err(MhinStoreError::InvalidBlockRange { start, end });
        }

        let txn = self.env.read_txn()?;
        let iter = self.journal_offsets_db.range(&txn, &(start..=end))?;
        let mut metas = Vec::new();

        for result in iter {
            let (_block, meta) = result?;
            metas.push(meta);
        }

        Ok(metas)
    }

    fn last_journal_offset_at_or_before(&self, block: BlockId) -> StoreResult<Option<JournalMeta>> {
        let txn = self.env.read_txn()?;
        let range = self.journal_offsets_db.range(&txn, &(0..=block))?;
        let mut last_meta = None;

        for result in range {
            let (_key, meta) = result?;
            last_meta = Some(meta);
        }

        Ok(last_meta)
    }

    fn remove_journal_offsets_after(&self, block: BlockId) -> StoreResult<()> {
        if block == BlockId::MAX {
            return Ok(());
        }

        let mut txn = self.env.write_txn()?;
        let range_start = block
            .checked_add(1)
            .ok_or(MhinStoreError::InvalidBlockRange {
                start: block,
                end: block,
            })?;

        let iter = self
            .journal_offsets_db
            .range(&txn, &(range_start..=BlockId::MAX))?;
        let mut to_remove = Vec::new();

        for result in iter {
            let (key, _) = result?;
            to_remove.push(key);
        }

        for key in to_remove {
            self.journal_offsets_db.delete(&mut txn, &key)?;
        }

        txn.commit()?;
        Ok(())
    }

    fn record_block_commit(&self, block: BlockId, meta: &JournalMeta) -> StoreResult<()> {
        let mut txn = self.env.write_txn()?;
        self.journal_offsets_db.put(&mut txn, &block, meta)?;
        self.state_db
            .put(&mut txn, Self::CURRENT_BLOCK_KEY, &block)?;
        txn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Arc;

    use tempfile::tempdir_in;

    use crate::error::MhinStoreError;

    fn sample_meta(block_height: BlockId, offset: u64) -> JournalMeta {
        JournalMeta {
            block_height,
            offset,
            compressed_len: 16,
            checksum: 1234 + block_height as u32,
        }
    }

    #[test]
    fn current_block_defaults_to_zero_and_updates() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let store = LmdbMetadataStore::new(tmp.path()).expect("metadata store should initialize");

        assert_eq!(store.current_block().unwrap(), 0);

        store.set_current_block(42).unwrap();
        assert_eq!(store.current_block().unwrap(), 42);
        assert!(store.path().exists());
    }

    #[test]
    fn journal_offsets_round_trip() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let store = LmdbMetadataStore::new(tmp.path()).unwrap();

        let meta1 = sample_meta(1, 0);
        let meta3 = sample_meta(3, 128);

        store.put_journal_offset(1, &meta1).unwrap();
        store.put_journal_offset(3, &meta3).unwrap();

        let result = store.get_journal_offsets(1..=3).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].block_height, 1);
        assert_eq!(result[0].offset, 0);
        assert_eq!(result[0].compressed_len, 16);
        assert_eq!(result[0].checksum, 1235);
        assert_eq!(result[1].block_height, 3);
        assert_eq!(result[1].offset, 128);
        assert_eq!(result[1].checksum, 1237);

        let empty = store.get_journal_offsets(4..=6).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    #[allow(clippy::reversed_empty_ranges)]
    fn journal_offsets_invalid_range_errors() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let store = LmdbMetadataStore::new(tmp.path()).unwrap();

        let err = store.get_journal_offsets(5..=3).unwrap_err();
        match err {
            MhinStoreError::InvalidBlockRange { start, end } => {
                assert_eq!(start, 5);
                assert_eq!(end, 3);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn clone_shares_environment_and_path() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();
        let store = LmdbMetadataStore::new(tmp.path()).unwrap();

        store.set_current_block(3).unwrap();
        let meta = sample_meta(2, 256);
        store.put_journal_offset(2, &meta).unwrap();

        let cloned = store.clone();
        assert!(Arc::ptr_eq(&store.env(), &cloned.env()));
        assert_eq!(store.path(), cloned.path());

        cloned.set_current_block(8).unwrap();
        assert_eq!(store.current_block().unwrap(), 8);

        let offsets = cloned.get_journal_offsets(1..=5).unwrap();
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].block_height, 2);
    }

    #[test]
    fn custom_map_size_creates_store() {
        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        // Create with custom 10GB map size
        let custom_size = 10 << 30; // 10GB
        let store = LmdbMetadataStore::new_with_map_size(tmp.path(), custom_size).unwrap();

        store.set_current_block(42).unwrap();
        assert_eq!(store.current_block().unwrap(), 42);

        let meta = sample_meta(1, 100);
        store.put_journal_offset(1, &meta).unwrap();

        let offsets = store.get_journal_offsets(1..=1).unwrap();
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].block_height, 1);
    }

    #[test]
    fn default_map_size_is_sufficient_for_bitcoin() {
        // Bitcoin mainnet: ~870k blocks (Dec 2024)
        // Growth: ~52,560 blocks/year
        // 10-year projection: ~1.4M blocks
        // At ~60 bytes/block: ~84 MB
        // Default 2GB can handle: ~35M blocks
        // Safety factor: 25x current needs ✅

        let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
        fs::create_dir_all(&workspace_tmp).unwrap();
        let tmp = tempdir_in(&workspace_tmp).unwrap();

        let store = LmdbMetadataStore::new(tmp.path()).unwrap();

        // Simulate 10 years of Bitcoin blocks (write sample blocks)
        let test_blocks = [1, 10_000, 100_000, 500_000, 1_000_000, 1_400_000];

        for block in test_blocks.iter() {
            let meta = sample_meta(*block, block * 100);
            store.put_journal_offset(*block, &meta).unwrap();
        }

        store.set_current_block(1_400_000).unwrap();
        assert_eq!(store.current_block().unwrap(), 1_400_000);

        // Verify we can read the blocks
        let offsets = store.get_journal_offsets(1..=1_400_000).unwrap();
        assert_eq!(offsets.len(), test_blocks.len());

        // Verify specific block
        let offsets = store.get_journal_offsets(1_000_000..=1_000_000).unwrap();
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].block_height, 1_000_000);
    }
}
