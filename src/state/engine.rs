use std::marker::PhantomData;
use std::sync::Arc;

use rayon::ThreadPool;

use crate::error::{MhinStoreError, StoreResult};
use crate::metadata::MetadataStore;
use crate::state::shard::StateShard;
use crate::types::{BlockDelta, BlockId, BlockUndo, Key, Operation, StateStats, Value};

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
    fn lookup(&self, key: &Key) -> Option<Value>;
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
    _metadata: PhantomData<Arc<M>>,
    thread_pool: Option<Arc<ThreadPool>>,
}

impl<M> ShardedStateEngine<M>
where
    M: MetadataStore + 'static,
{
    pub fn new(shards: Vec<Arc<dyn StateShard>>, _metadata: Arc<M>) -> Self {
        Self {
            shards,
            _metadata: PhantomData,
            thread_pool: None,
        }
    }

    pub fn with_thread_pool(
        shards: Vec<Arc<dyn StateShard>>,
        _metadata: Arc<M>,
        thread_pool: Option<Arc<ThreadPool>>,
    ) -> Self {
        Self {
            shards,
            _metadata: PhantomData,
            thread_pool,
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
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
            return Err(MhinStoreError::NoShardsConfigured);
        }

        if undo.block_height != block_height {
            return Err(MhinStoreError::BlockDeltaMismatch {
                expected: block_height,
                found: undo.block_height,
            });
        }

        let delta = plan_replay_delta(block_height, operations, undo, |key| {
            self.shard_index_for_key(key)
                .ok_or(MhinStoreError::NoShardsConfigured)
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
            return Err(MhinStoreError::NoShardsConfigured);
        }

        plan_block_delta(&self.shards, block_height, ops)
    }

    fn commit(
        &self,
        block_height: BlockId,
        delta: BlockDelta,
    ) -> StoreResult<(StateStats, BlockUndo)> {
        if self.shards.is_empty() {
            return Err(MhinStoreError::NoShardsConfigured);
        }

        commit_block(&self.shards, self.thread_pool.as_ref(), block_height, delta)
    }

    fn revert(&self, block_height: BlockId, undo: BlockUndo) -> StoreResult<()> {
        if self.shards.is_empty() {
            return Err(MhinStoreError::NoShardsConfigured);
        }

        revert_block(&self.shards, self.thread_pool.as_ref(), block_height, undo)
    }

    fn lookup(&self, key: &Key) -> Option<Value> {
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
    use std::collections::BTreeMap;
    use std::ops::RangeInclusive;
    use std::sync::Mutex;

    use crate::error::MhinStoreError;
    use crate::state::shard::{RawTableShard, StateShard};
    use crate::types::{JournalMeta, ShardDelta, ShardOp, ShardUndo, UndoOp};

    #[derive(Default)]
    struct MemoryMetadataStore {
        current: Mutex<BlockId>,
        offsets: Mutex<BTreeMap<BlockId, JournalMeta>>,
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
                return Err(MhinStoreError::InvalidBlockRange { start, end });
            }

            Ok(self
                .offsets
                .lock()
                .unwrap()
                .range(range)
                .map(|(_, meta)| meta.clone())
                .collect())
        }
    }

    fn op(key: Key, value: Value) -> Operation {
        Operation { key, value }
    }

    fn shard_op(key: Key, value: Value) -> ShardOp {
        ShardOp { key, value }
    }

    #[test]
    fn prepare_journal_requires_shards() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let engine = ShardedStateEngine::new(Vec::new(), metadata);
        let err = engine.prepare_journal(1, &[]).unwrap_err();
        assert!(matches!(err, MhinStoreError::NoShardsConfigured));
    }

    #[test]
    fn prepare_journal_collects_expected_undo_entries() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let shard = Arc::new(RawTableShard::new(0, 8)) as Arc<dyn StateShard>;
        shard.apply(&[shard_op([1u8; 8], 3)]);

        let engine = ShardedStateEngine::new(vec![Arc::clone(&shard)], Arc::clone(&metadata));
        let key = [1u8; 8];

        let ops = vec![op(key, 10), op(key, 0)];

        let delta = engine.prepare_journal(2, &ops).unwrap();
        assert_eq!(delta.block_height, 2);
        assert_eq!(delta.shards.len(), 1);

        let shard_delta = &delta.shards[0];
        assert_eq!(shard_delta.operations.len(), 2);
        assert_eq!(shard_delta.undo_entries.len(), 2);
        assert_eq!(shard_delta.undo_entries[0].previous, Some(3));
        assert_eq!(shard_delta.undo_entries[0].op, UndoOp::Updated);
        assert_eq!(shard_delta.undo_entries[1].previous, Some(10));
        assert_eq!(shard_delta.undo_entries[1].op, UndoOp::Deleted);
    }

    #[test]
    fn commit_and_revert_round_trip() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let shard = Arc::new(RawTableShard::new(0, 8)) as Arc<dyn StateShard>;
        let engine = ShardedStateEngine::new(vec![Arc::clone(&shard)], Arc::clone(&metadata));
        let key = [7u8; 8];

        let ops = vec![op(key, 5), op(key, 9)];

        let delta = engine.prepare_journal(1, &ops).unwrap();
        let (stats, undo) = engine.commit(1, delta).unwrap();

        assert_eq!(stats.operation_count, 2);
        assert_eq!(stats.modified_keys, 2);
        assert_eq!(engine.lookup(&key), Some(9));
        assert_eq!(undo.block_height, 1);
        assert_eq!(undo.shard_undos.len(), 1);
        assert_eq!(undo.shard_undos[0].entries.len(), 2);

        engine.revert(1, undo).unwrap();
        assert_eq!(engine.lookup(&key), None);
    }

    #[test]
    fn shard_hash_reference_value_is_stable() {
        let key = [0u8; 8];
        let hash = shard_hash(&key);
        assert_eq!(hash, 253443271424304431);
    }

    #[test]
    fn shard_index_for_key_reference_distribution() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let shards: Vec<Arc<dyn StateShard>> = (0..4)
            .map(|index| Arc::new(RawTableShard::new(index, 8)) as Arc<dyn StateShard>)
            .collect();
        let engine = ShardedStateEngine::new(shards, metadata);
        let key = [0u8; 8];
        assert_eq!(engine.shard_index_for_key(&key), Some(3));
    }

    #[test]
    fn commit_handles_noop_operations() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let shard = Arc::new(RawTableShard::new(0, 8)) as Arc<dyn StateShard>;
        let engine = ShardedStateEngine::new(vec![Arc::clone(&shard)], metadata);
        let key = [2u8; 8];

        let ops = vec![op(key, 0)];
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
            MhinStoreError::BlockDeltaMismatch {
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
            MhinStoreError::BlockDeltaMismatch {
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
        let key = [9u8; 8];

        let delta = BlockDelta {
            block_height: 1,
            shards: vec![ShardDelta {
                shard_index: 5,
                operations: vec![ShardOp { key, value: 1 }],
                undo_entries: Vec::new(),
            }],
        };

        let err = engine.commit(1, delta).unwrap_err();
        assert!(matches!(
            err,
            MhinStoreError::InvalidShardIndex {
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
            MhinStoreError::InvalidShardIndex {
                shard_index: 3,
                shard_count: 1
            }
        ));
    }

    #[test]
    fn lookup_without_shards_returns_none() {
        let metadata = Arc::new(MemoryMetadataStore::default());
        let engine = ShardedStateEngine::new(Vec::new(), metadata);
        let key = [1u8; 8];
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
        assert!(matches!(err, MhinStoreError::NoShardsConfigured));
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
        assert!(matches!(err, MhinStoreError::NoShardsConfigured));
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
            let key = [
                (i & 0xFF) as u8,
                ((i >> 8) & 0xFF) as u8,
                ((i >> 16) & 0xFF) as u8,
                ((i >> 24) & 0xFF) as u8,
                0,
                0,
                0,
                i as u8,
            ];
            ops.push(op(key, i));
        }

        let delta = engine.prepare_journal(1, &ops).unwrap();
        let (stats, undo) = engine.commit(1, delta).unwrap();
        assert_eq!(stats.operation_count, ops.len());
        let zero_deletes = ops.iter().filter(|op| op.value == 0).count();
        assert_eq!(stats.modified_keys, ops.len() - zero_deletes);

        for i in [0u64, 1, 10, 63] {
            let key = [
                (i & 0xFF) as u8,
                ((i >> 8) & 0xFF) as u8,
                ((i >> 16) & 0xFF) as u8,
                ((i >> 24) & 0xFF) as u8,
                0,
                0,
                0,
                i as u8,
            ];
            let expected = if i == 0 { None } else { Some(i) };
            assert_eq!(engine.lookup(&key), expected);
        }

        engine.revert(1, undo).unwrap();
        for i in [0u64, 1, 10, 63] {
            let key = [
                (i & 0xFF) as u8,
                ((i >> 8) & 0xFF) as u8,
                ((i >> 16) & 0xFF) as u8,
                ((i >> 24) & 0xFF) as u8,
                0,
                0,
                0,
                i as u8,
            ];
            assert_eq!(engine.lookup(&key), None);
        }
    }
}
