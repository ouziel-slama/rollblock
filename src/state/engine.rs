use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use rayon::ThreadPool;

use crate::error::{StoreError, StoreResult};
use crate::metadata::MetadataStore;
use crate::state::shard::StateShard;
#[cfg(test)]
use crate::types::Value;
use crate::types::{
    BlockDelta, BlockId, BlockUndo, Operation, StateStats, StoreKey as Key, ValueBuf,
};

mod executor;
mod hashing;
mod planner;
mod replay;

pub use hashing::SHARD_HASH_VERSION;

use executor::{commit_block, revert_block};
use planner::plan_block_delta;
use replay::plan_replay_delta;

pub trait StateEngine: Send + Sync {
    fn prepare_journal(&self, block_height: BlockId, ops: &[Operation]) -> StoreResult<BlockDelta>;
    fn commit(
        &self,
        block_height: BlockId,
        delta: BlockDelta,
    ) -> StoreResult<(StateStats, BlockUndo)>;
    fn revert(&self, block_height: BlockId, undo: BlockUndo) -> StoreResult<()>;
    fn lookup(&self, key: &Key) -> Option<ValueBuf>;
    fn lookup_many(&self, keys: &[Key]) -> Vec<Option<ValueBuf>> {
        keys.iter().map(|key| self.lookup(key)).collect()
    }
    fn snapshot_shards(&self) -> Vec<Arc<dyn StateShard>>;
    fn total_keys(&self) -> usize {
        self.snapshot_shards()
            .iter()
            .map(|shard| shard.stats().keys)
            .sum()
    }
}

pub struct ShardedStateEngine<M>
where
    M: MetadataStore + 'static,
{
    shards: Vec<Arc<dyn StateShard>>,
    lookup_scratch: Vec<Mutex<Vec<Option<ValueBuf>>>>,
    _metadata: PhantomData<Arc<M>>,
    thread_pool: Option<Arc<ThreadPool>>,
}

impl<M> ShardedStateEngine<M>
where
    M: MetadataStore + 'static,
{
    pub fn new(shards: Vec<Arc<dyn StateShard>>, _metadata: Arc<M>) -> Self {
        let lookup_scratch = Self::build_lookup_scratch(shards.len());
        Self {
            shards,
            lookup_scratch,
            _metadata: PhantomData,
            thread_pool: None,
        }
    }

    pub fn with_thread_pool(
        shards: Vec<Arc<dyn StateShard>>,
        _metadata: Arc<M>,
        thread_pool: Option<Arc<ThreadPool>>,
    ) -> Self {
        let lookup_scratch = Self::build_lookup_scratch(shards.len());
        Self {
            shards,
            lookup_scratch,
            _metadata: PhantomData,
            thread_pool,
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn build_lookup_scratch(shard_count: usize) -> Vec<Mutex<Vec<Option<ValueBuf>>>> {
        (0..shard_count).map(|_| Mutex::new(Vec::new())).collect()
    }

    fn shard_for_key(&self, key: &Key) -> Option<&Arc<dyn StateShard>> {
        self.shard_index_for_key(key)
            .and_then(|index| self.shards.get(index))
    }

    fn shard_index_for_key(&self, key: &Key) -> Option<usize> {
        if self.shards.is_empty() {
            return None;
        }

        let shard_count = self.shards.len() as u64;
        let hash = hashing::shard_hash(key);
        Some((hash % shard_count) as usize)
    }

    pub fn apply_replayed_block(
        &self,
        block_height: BlockId,
        operations: &[Operation],
        undo: &BlockUndo,
    ) -> StoreResult<()> {
        if self.shards.is_empty() {
            return Err(StoreError::NoShardsConfigured);
        }

        if undo.block_height != block_height {
            return Err(StoreError::BlockDeltaMismatch {
                expected: block_height,
                found: undo.block_height,
            });
        }

        let delta = plan_replay_delta(block_height, operations, undo, |key| {
            self.shard_index_for_key(key)
                .ok_or(StoreError::NoShardsConfigured)
        })?;

        if let Some(block_delta) = delta {
            let _ = self.commit(block_height, block_delta)?;
        }
        Ok(())
    }
}

impl<M> StateEngine for ShardedStateEngine<M>
where
    M: MetadataStore + 'static,
{
    fn total_keys(&self) -> usize {
        self.shards.iter().map(|shard| shard.stats().keys).sum()
    }

    fn prepare_journal(&self, block_height: BlockId, ops: &[Operation]) -> StoreResult<BlockDelta> {
        if self.shards.is_empty() {
            return Err(StoreError::NoShardsConfigured);
        }

        plan_block_delta(&self.shards, block_height, ops)
    }

    fn commit(
        &self,
        block_height: BlockId,
        delta: BlockDelta,
    ) -> StoreResult<(StateStats, BlockUndo)> {
        if self.shards.is_empty() {
            return Err(StoreError::NoShardsConfigured);
        }

        commit_block(&self.shards, self.thread_pool.as_ref(), block_height, delta)
    }

    fn revert(&self, block_height: BlockId, undo: BlockUndo) -> StoreResult<()> {
        if self.shards.is_empty() {
            return Err(StoreError::NoShardsConfigured);
        }

        revert_block(&self.shards, self.thread_pool.as_ref(), block_height, undo)
    }

    fn lookup_many(&self, keys: &[Key]) -> Vec<Option<ValueBuf>> {
        if keys.is_empty() {
            return Vec::new();
        }

        if self.shards.is_empty() {
            return vec![None; keys.len()];
        }

        struct ShardBatch {
            positions: Vec<usize>,
            keys: Vec<Key>,
        }

        impl ShardBatch {
            fn new() -> Self {
                Self {
                    positions: Vec::new(),
                    keys: Vec::new(),
                }
            }
        }

        let mut batches: Vec<ShardBatch> =
            (0..self.shards.len()).map(|_| ShardBatch::new()).collect();

        for (idx, key) in keys.iter().copied().enumerate() {
            if let Some(shard_idx) = self.shard_index_for_key(&key) {
                batches[shard_idx].positions.push(idx);
                batches[shard_idx].keys.push(key);
            }
        }

        let mut results = vec![None; keys.len()];
        for (shard_idx, batch) in batches.into_iter().enumerate() {
            if batch.keys.is_empty() {
                continue;
            }

            if let (Some(shard), Some(scratch_mutex)) = (
                self.shards.get(shard_idx),
                self.lookup_scratch.get(shard_idx),
            ) {
                let mut scratch = scratch_mutex.lock().unwrap();
                if scratch.len() < batch.keys.len() {
                    scratch.resize(batch.keys.len(), None);
                } else {
                    for slot in scratch.iter_mut().take(batch.keys.len()) {
                        *slot = None;
                    }
                }

                let slice = &mut scratch[..batch.keys.len()];
                shard.get_many(&batch.keys, slice);
                for (pos, slot) in batch.positions.into_iter().zip(slice.iter_mut()) {
                    results[pos] = slot.take();
                }
            }
        }

        results
    }

    fn lookup(&self, key: &Key) -> Option<ValueBuf> {
        self.shard_for_key(key).and_then(|shard| shard.get(key))
    }

    fn snapshot_shards(&self) -> Vec<Arc<dyn StateShard>> {
        self.shards.iter().map(Arc::clone).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::engine::hashing::shard_hash;
    use crate::types::DEFAULT_KEY_BYTES;
    use std::collections::BTreeMap;
    use std::ops::RangeInclusive;
    use std::sync::Mutex;
    use xxhash_rust::xxh3::xxh3_64;

    use crate::error::StoreError;
    use crate::metadata::GcWatermark;
    use crate::state::shard::{RawTableShard, StateShard};
    use crate::types::{JournalMeta, ShardDelta, ShardOp, ShardUndo, UndoOp};

    #[derive(Default)]
    struct MemoryMetadataStore {
        current: Mutex<BlockId>,
        offsets: Mutex<BTreeMap<BlockId, JournalMeta>>,
        gc_watermark: Mutex<Option<GcWatermark>>,
        snapshot_watermark: Mutex<Option<BlockId>>,
    }

    impl MetadataStore for MemoryMetadataStore {
        fn current_block(&self) -> StoreResult<BlockId> {
            Ok(*self.current.lock().unwrap())
        }

        fn set_current_block(&self, block: BlockId) -> StoreResult<()> {
            *self.current.lock().unwrap() = block;
            Ok(())
        }

        fn put_journal_offset(&self, block: BlockId, meta: &JournalMeta) -> StoreResult<()> {
            self.offsets.lock().unwrap().insert(block, meta.clone());
            Ok(())
        }

        fn last_journal_offset_at_or_before(
            &self,
            block: BlockId,
        ) -> StoreResult<Option<JournalMeta>> {
            let offsets = self.offsets.lock().unwrap();
            Ok(offsets
                .range(..=block)
                .next_back()
                .map(|(_, meta)| meta.clone()))
        }

        fn get_journal_offsets(
            &self,
            range: RangeInclusive<BlockId>,
        ) -> StoreResult<Vec<JournalMeta>> {
            let start = *range.start();
            let end = *range.end();
            if start > end {
                return Err(StoreError::InvalidBlockRange { start, end });
            }

            Ok(self
                .offsets
                .lock()
                .unwrap()
                .range(range)
                .map(|(_, meta)| meta.clone())
                .collect())
        }

        fn prune_journal_offsets_at_or_before(&self, block: BlockId) -> StoreResult<usize> {
            let mut offsets = self.offsets.lock().unwrap();
            if block == BlockId::MAX {
                let removed = offsets.len();
                offsets.clear();
                return Ok(removed);
            }
            let split_key = block.saturating_add(1);
            let retained = offsets.split_off(&split_key);
            let removed = offsets.len();
            *offsets = retained;
            Ok(removed)
        }

        fn load_gc_watermark(&self) -> StoreResult<Option<GcWatermark>> {
            Ok(self.gc_watermark.lock().unwrap().clone())
        }

        fn store_gc_watermark(&self, watermark: &GcWatermark) -> StoreResult<()> {
            *self.gc_watermark.lock().unwrap() = Some(watermark.clone());
            Ok(())
        }

        fn clear_gc_watermark(&self) -> StoreResult<()> {
            *self.gc_watermark.lock().unwrap() = None;
            Ok(())
        }

        fn load_snapshot_watermark(&self) -> StoreResult<Option<BlockId>> {
            Ok(*self.snapshot_watermark.lock().unwrap())
        }

        fn store_snapshot_watermark(&self, block: BlockId) -> StoreResult<()> {
            *self.snapshot_watermark.lock().unwrap() = Some(block);
            Ok(())
        }
    }

    fn op(key: impl Into<Key>, value: Value) -> Operation {
        Operation {
            key: key.into(),
            value,
        }
    }

    fn shard_op(key: impl Into<Key>, value: Value) -> ShardOp {
        ShardOp {
            key: key.into(),
            value,
        }
    }

    fn val(num: u64) -> Value {
        Value::from(num)
    }

    fn del() -> Value {
        Value::empty()
    }

    fn op_num(key: impl Into<Key> + Copy, num: u64) -> Operation {
        op(key, val(num))
    }

    fn op_delete(key: impl Into<Key> + Copy) -> Operation {
        op(key, del())
    }

    fn shard_op_num(key: impl Into<Key> + Copy, num: u64) -> ShardOp {
        shard_op(key, val(num))
    }

    /// Creates a test key from a u64, using a specific pattern for predictable distribution.
    fn test_key(i: u64) -> Key {
        // Use a pattern that differs from from_u64_le to test shard distribution
        Key::from_prefix([
            (i & 0xFF) as u8,
            ((i >> 8) & 0xFF) as u8,
            ((i >> 16) & 0xFF) as u8,
            ((i >> 24) & 0xFF) as u8,
            0,
            0,
            0,
            i as u8,
        ])
    }

    #[test]
    fn prepare_journal_requires_shards() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let engine = ShardedStateEngine::new(Vec::new(), metadata);
        let err = engine.prepare_journal(1, &[]).unwrap_err();
        assert!(matches!(err, StoreError::NoShardsConfigured));
    }

    #[test]
    fn prepare_journal_collects_expected_undo_entries() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let shard = Arc::new(RawTableShard::new(0, 8)) as Arc<dyn StateShard>;
        shard.apply(&[shard_op_num([1u8; Key::BYTES], 3)]);

        let engine = ShardedStateEngine::new(vec![Arc::clone(&shard)], Arc::clone(&metadata));
        let key: Key = [1u8; Key::BYTES].into();

        let ops = vec![op_num(key, 10), op_delete(key)];

        let delta = engine.prepare_journal(2, &ops).unwrap();
        assert_eq!(delta.block_height, 2);
        assert_eq!(delta.shards.len(), 1);

        let shard_delta = &delta.shards[0];
        assert_eq!(shard_delta.operations.len(), 2);
        assert_eq!(shard_delta.undo_entries.len(), 2);
        assert_eq!(shard_delta.undo_entries[0].previous, Some(val(3)));
        assert_eq!(shard_delta.undo_entries[0].op, UndoOp::Updated);
        assert_eq!(shard_delta.undo_entries[1].previous, Some(val(10)));
        assert_eq!(shard_delta.undo_entries[1].op, UndoOp::Deleted);
    }

    #[test]
    fn commit_and_revert_round_trip() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let shard = Arc::new(RawTableShard::new(0, 8)) as Arc<dyn StateShard>;
        let engine = ShardedStateEngine::new(vec![Arc::clone(&shard)], Arc::clone(&metadata));
        let key: Key = [7u8; Key::BYTES].into();

        let ops = vec![op_num(key, 5), op_num(key, 9)];

        let delta = engine.prepare_journal(1, &ops).unwrap();
        let (stats, undo) = engine.commit(1, delta).unwrap();

        assert_eq!(stats.operation_count, 2);
        assert_eq!(stats.modified_keys, 2);
        assert_eq!(engine.lookup(&key).map(Value::from), Some(val(9)));
        assert_eq!(undo.block_height, 1);
        assert_eq!(undo.shard_undos.len(), 1);
        assert_eq!(undo.shard_undos[0].entries.len(), 2);

        engine.revert(1, undo).unwrap();
        assert_eq!(engine.lookup(&key), None);
    }

    #[test]
    fn lookup_many_returns_values_in_original_order() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let raw_shards: Vec<Arc<RawTableShard>> = (0..2)
            .map(|index| Arc::new(RawTableShard::new(index, 8)))
            .collect();
        let shards: Vec<Arc<dyn StateShard>> = raw_shards
            .iter()
            .map(|shard| Arc::clone(shard) as Arc<dyn StateShard>)
            .collect();

        let engine = ShardedStateEngine::new(shards, metadata);

        let key_a: Key = [1u8; Key::BYTES].into();
        let key_b: Key = [2u8; Key::BYTES].into();
        let shard_idx_a = engine.shard_index_for_key(&key_a).unwrap();
        raw_shards[shard_idx_a].apply(&[shard_op_num(key_a, 10)]);
        let shard_idx_b = engine.shard_index_for_key(&key_b).unwrap();
        raw_shards[shard_idx_b].apply(&[shard_op_num(key_b, 20)]);

        let keys = vec![key_b, Key::from([3u8; Key::BYTES]), key_a];
        let results = engine.lookup_many(&keys);

        let actual: Vec<Option<Value>> = results
            .into_iter()
            .map(|opt| opt.map(Value::from))
            .collect();
        assert_eq!(actual, vec![Some(val(20)), None, Some(val(10))]);
    }

    #[test]
    fn shard_hash_uses_xxh3_over_key_bytes() {
        let key = Key::from([0u8; Key::BYTES]);
        let hash = shard_hash(&key);
        assert_eq!(hash, xxh3_64(&[0u8; DEFAULT_KEY_BYTES]));
    }

    #[test]
    fn shard_index_for_key_reference_distribution() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let shards: Vec<Arc<dyn StateShard>> = (0..4)
            .map(|index| Arc::new(RawTableShard::new(index, 8)) as Arc<dyn StateShard>)
            .collect();
        let engine = ShardedStateEngine::new(shards, metadata);
        let key = Key::from([0u8; Key::BYTES]);
        let expected =
            (xxhash_rust::xxh3::xxh3_64(key.as_slice()) % engine.shard_count() as u64) as usize;
        assert_eq!(engine.shard_index_for_key(&key), Some(expected));
    }

    #[test]
    fn commit_handles_noop_operations() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let shard = Arc::new(RawTableShard::new(0, 8)) as Arc<dyn StateShard>;
        let engine = ShardedStateEngine::new(vec![Arc::clone(&shard)], metadata);
        let key = Key::from([2u8; Key::BYTES]);

        let ops = vec![op_delete(key)];
        let delta = engine.prepare_journal(5, &ops).unwrap();
        let (stats, undo) = engine.commit(5, delta).unwrap();

        assert_eq!(stats.operation_count, 1);
        assert_eq!(stats.modified_keys, 0);
        assert!(undo.shard_undos.is_empty());
        assert_eq!(engine.lookup(&key), None);
    }

    #[test]
    fn commit_block_height_mismatch_errors() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let shard = Arc::new(RawTableShard::new(0, 8)) as Arc<dyn StateShard>;
        let engine = ShardedStateEngine::new(vec![Arc::clone(&shard)], metadata);

        let delta = BlockDelta {
            block_height: 3,
            shards: Vec::new(),
        };

        let err = engine.commit(2, delta).unwrap_err();
        assert!(matches!(
            err,
            StoreError::BlockDeltaMismatch {
                expected: 2,
                found: 3
            }
        ));
    }

    #[test]
    fn revert_block_height_mismatch_errors() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let shard = Arc::new(RawTableShard::new(0, 8)) as Arc<dyn StateShard>;
        let engine = ShardedStateEngine::new(vec![Arc::clone(&shard)], metadata);

        let undo = BlockUndo {
            block_height: 4,
            shard_undos: Vec::new(),
        };

        let err = engine.revert(1, undo).unwrap_err();
        assert!(matches!(
            err,
            StoreError::BlockDeltaMismatch {
                expected: 1,
                found: 4
            }
        ));
    }

    #[test]
    fn commit_errors_on_invalid_shard_index() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let shard = Arc::new(RawTableShard::new(0, 8)) as Arc<dyn StateShard>;
        let engine = ShardedStateEngine::new(vec![Arc::clone(&shard)], metadata);
        let key = Key::from([9u8; Key::BYTES]);

        let delta = BlockDelta {
            block_height: 1,
            shards: vec![ShardDelta {
                shard_index: 5,
                operations: vec![ShardOp {
                    key,
                    value: 1.into(),
                }],
                undo_entries: Vec::new(),
            }],
        };

        let err = engine.commit(1, delta).unwrap_err();
        assert!(matches!(
            err,
            StoreError::InvalidShardIndex {
                shard_index: 5,
                shard_count: 1
            }
        ));
    }

    #[test]
    fn revert_errors_on_invalid_shard_index() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let shard = Arc::new(RawTableShard::new(0, 8)) as Arc<dyn StateShard>;
        let engine = ShardedStateEngine::new(vec![Arc::clone(&shard)], metadata);

        let undo = BlockUndo {
            block_height: 1,
            shard_undos: vec![ShardUndo {
                shard_index: 3,
                entries: Vec::new(),
            }],
        };

        let err = engine.revert(1, undo).unwrap_err();
        assert!(matches!(
            err,
            StoreError::InvalidShardIndex {
                shard_index: 3,
                shard_count: 1
            }
        ));
    }

    #[test]
    fn lookup_without_shards_returns_none() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let engine = ShardedStateEngine::new(Vec::new(), metadata);
        let key = Key::from([1u8; Key::BYTES]);
        assert!(engine.lookup(&key).is_none());
        assert!(engine.shard_index_for_key(&key).is_none());
    }

    #[test]
    fn commit_without_shards_errors() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let engine = ShardedStateEngine::new(Vec::new(), metadata);
        let delta = BlockDelta {
            block_height: 1,
            shards: Vec::new(),
        };

        let err = engine.commit(1, delta).unwrap_err();
        assert!(matches!(err, StoreError::NoShardsConfigured));
    }

    #[test]
    fn revert_without_shards_errors() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let engine = ShardedStateEngine::new(Vec::new(), metadata);
        let undo = BlockUndo {
            block_height: 1,
            shard_undos: Vec::new(),
        };

        let err = engine.revert(1, undo).unwrap_err();
        assert!(matches!(err, StoreError::NoShardsConfigured));
    }

    #[test]
    fn commit_and_revert_with_thread_pool() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let shards: Vec<Arc<dyn StateShard>> = (0..4)
            .map(|index| Arc::new(RawTableShard::new(index, 32)) as Arc<dyn StateShard>)
            .collect();

        let thread_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );

        let engine =
            ShardedStateEngine::with_thread_pool(shards, Arc::clone(&metadata), Some(thread_pool));

        let mut ops = Vec::new();
        for i in 0..64u64 {
            let key = test_key(i);
            if i == 0 {
                ops.push(op_delete(key));
            } else {
                ops.push(op_num(key, i));
            }
        }

        let delta = engine.prepare_journal(1, &ops).unwrap();
        let (stats, undo) = engine.commit(1, delta).unwrap();
        assert_eq!(stats.operation_count, ops.len());
        let zero_deletes = ops.iter().filter(|op| op.value.is_delete()).count();
        assert_eq!(stats.modified_keys, ops.len() - zero_deletes);

        for i in [0u64, 1, 10, 63] {
            let key = test_key(i);
            let expected = if i == 0 { None } else { Some(val(i)) };
            assert_eq!(engine.lookup(&key).map(Value::from), expected);
        }

        engine.revert(1, undo).unwrap();
        for i in [0u64, 1, 10, 63] {
            let key = test_key(i);
            assert_eq!(engine.lookup(&key), None);
        }
    }
}
