use std::collections::HashMap;

use crate::error::StoreResult;
use crate::types::{
    BlockDelta, BlockId, BlockUndo, Key, Operation, ShardDelta, ShardOp, ShardUndo,
};

pub(crate) fn plan_replay_delta(
    block_height: BlockId,
    operations: &[Operation],
    undo: &BlockUndo,
    mut shard_for_key: impl FnMut(&Key) -> StoreResult<usize>,
) -> StoreResult<Option<BlockDelta>> {
    if operations.is_empty() {
        return Ok(None);
    }

    let mut shard_deltas: HashMap<usize, ShardDelta> = HashMap::new();
    for op in operations {
        let shard_index = shard_for_key(&op.key)?;
        let delta = shard_deltas
            .entry(shard_index)
            .or_insert_with(|| ShardDelta {
                shard_index,
                operations: Vec::new(),
                undo_entries: Vec::new(),
            });

        delta.operations.push(ShardOp {
            key: op.key,
            value: op.value.clone(),
        });
    }

    if !undo.shard_undos.is_empty() {
        let mut undo_lookup: HashMap<usize, &ShardUndo> = HashMap::new();
        for shard_undo in &undo.shard_undos {
            undo_lookup.insert(shard_undo.shard_index, shard_undo);
        }

        for delta in shard_deltas.values_mut() {
            if let Some(shard_undo) = undo_lookup.get(&delta.shard_index) {
                delta.undo_entries = shard_undo.entries.clone();
            }
        }
    }

    let mut shards: Vec<ShardDelta> = shard_deltas.into_values().collect();
    shards.sort_by_key(|delta| delta.shard_index);

    if shards.is_empty() {
        return Ok(None);
    }

    Ok(Some(BlockDelta {
        block_height,
        shards,
    }))
}
