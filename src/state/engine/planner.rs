use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{StoreError, StoreResult};
use crate::state::shard::StateShard;
use crate::types::{
    BlockDelta, BlockId, Operation, ShardDelta, ShardOp, StoreKey as Key, UndoEntry, UndoOp, Value,
    ValueBuf,
};

pub(crate) fn plan_block_delta(
    shards: &[Arc<dyn StateShard>],
    block_height: BlockId,
    ops: &[Operation],
) -> StoreResult<BlockDelta> {
    if ops.is_empty() {
        return Ok(BlockDelta {
            block_height,
            shards: Vec::new(),
        });
    }

    let shard_count = shards.len();

    let mut deltas: Vec<ShardDelta> = (0..shard_count)
        .map(|index| ShardDelta {
            shard_index: index,
            operations: Vec::new(),
            undo_entries: Vec::new(),
        })
        .collect();
    let mut planned_states: Vec<HashMap<Key, Option<ValueBuf>>> =
        (0..shard_count).map(|_| HashMap::new()).collect();

    for op in ops {
        let shard_index = shard_for_key(shards, &op.key)?;
        let shard = shards
            .get(shard_index)
            .ok_or(StoreError::NoShardsConfigured)?;

        let shard_delta = &mut deltas[shard_index];
        shard_delta.operations.push(ShardOp {
            key: op.key,
            value: op.value.clone(),
        });

        let plan = &mut planned_states[shard_index];
        let previous_state = if let Some(state) = plan.get(&op.key) {
            state.clone()
        } else {
            shard.get(&op.key)
        };

        if op.is_delete() {
            if let Some(ref previous) = previous_state {
                shard_delta.undo_entries.push(UndoEntry {
                    key: op.key,
                    previous: Some(Value::from(previous.clone())),
                    op: UndoOp::Deleted,
                });
            }
            plan.insert(op.key, None);
        } else {
            let undo_op = if previous_state.is_some() {
                UndoOp::Updated
            } else {
                UndoOp::Inserted
            };

            shard_delta.undo_entries.push(UndoEntry {
                key: op.key,
                previous: previous_state.as_ref().map(|buf| Value::from(buf.clone())),
                op: undo_op,
            });

            plan.insert(op.key, Some(ValueBuf::from(&op.value)));
        }
    }

    deltas.retain(|delta| !delta.operations.is_empty());

    Ok(BlockDelta {
        block_height,
        shards: deltas,
    })
}

fn shard_for_key(shards: &[Arc<dyn StateShard>], key: &Key) -> StoreResult<usize> {
    if shards.is_empty() {
        return Err(StoreError::NoShardsConfigured);
    }

    let shard_count = shards.len();
    let hash = super::hashing::shard_hash(key);
    Ok((hash % shard_count as u64) as usize)
}
