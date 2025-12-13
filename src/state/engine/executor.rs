use std::sync::Arc;

use rayon::ThreadPool;

use crate::error::{StoreError, StoreResult};
use crate::state::shard::StateShard;
use crate::types::{BlockDelta, BlockId, BlockUndo, ShardDelta, ShardUndo, StateStats};

pub(crate) fn commit_block(
    shards: &[Arc<dyn StateShard>],
    thread_pool: Option<&Arc<ThreadPool>>,
    block_height: BlockId,
    delta: BlockDelta,
) -> StoreResult<(StateStats, BlockUndo)> {
    if delta.block_height != block_height {
        return Err(StoreError::BlockDeltaMismatch {
            expected: block_height,
            found: delta.block_height,
        });
    }

    let results: Vec<_> = match thread_pool {
        Some(pool) => pool.install(|| {
            use rayon::prelude::*;
            delta
                .shards
                .par_iter()
                .map(|shard_delta| process_shard_commit(shards, shard_delta))
                .collect()
        }),
        None => delta
            .shards
            .iter()
            .map(|shard_delta| process_shard_commit(shards, shard_delta))
            .collect(),
    };

    let mut total_ops = 0usize;
    let mut modified_keys = 0usize;
    let mut block_undo = BlockUndo {
        block_height,
        shard_undos: Vec::new(),
    };

    for result in results {
        let (ops, keys, shard_undo_opt) = result?;
        total_ops += ops;
        modified_keys += keys;
        if let Some(shard_undo) = shard_undo_opt {
            block_undo.shard_undos.push(shard_undo);
        }
    }

    Ok((
        StateStats {
            operation_count: total_ops,
            modified_keys,
        },
        block_undo,
    ))
}

pub(crate) fn revert_block(
    shards: &[Arc<dyn StateShard>],
    thread_pool: Option<&Arc<ThreadPool>>,
    block_height: BlockId,
    undo: BlockUndo,
) -> StoreResult<()> {
    if undo.block_height != block_height {
        return Err(StoreError::BlockDeltaMismatch {
            expected: block_height,
            found: undo.block_height,
        });
    }

    let results: Vec<_> = match thread_pool {
        Some(pool) => pool.install(|| {
            use rayon::prelude::*;
            undo.shard_undos
                .par_iter()
                .map(|shard_undo| process_shard_revert(shards, shard_undo))
                .collect()
        }),
        None => undo
            .shard_undos
            .iter()
            .map(|shard_undo| process_shard_revert(shards, shard_undo))
            .collect(),
    };

    for result in results {
        result?;
    }

    Ok(())
}

fn process_shard_commit(
    shards: &[Arc<dyn StateShard>],
    shard_delta: &ShardDelta,
) -> StoreResult<(usize, usize, Option<ShardUndo>)> {
    let shard = shards
        .get(shard_delta.shard_index)
        .ok_or(StoreError::InvalidShardIndex {
            shard_index: shard_delta.shard_index,
            shard_count: shards.len(),
        })?;

    let mut shard_undo = shard.apply(&shard_delta.operations);
    let ops_count = shard_delta.operations.len();
    let entry_count = shard_undo.entries.len();

    debug_assert_eq!(
        entry_count,
        shard_delta.undo_entries.len(),
        "planned undo entries diverged from shard undo entries",
    );

    if entry_count > 0 {
        if shard_undo.shard_index != shard_delta.shard_index {
            shard_undo.shard_index = shard_delta.shard_index;
        }
        Ok((ops_count, entry_count, Some(shard_undo)))
    } else {
        Ok((ops_count, 0, None))
    }
}

fn process_shard_revert(shards: &[Arc<dyn StateShard>], shard_undo: &ShardUndo) -> StoreResult<()> {
    let shard = shards
        .get(shard_undo.shard_index)
        .ok_or(StoreError::InvalidShardIndex {
            shard_index: shard_undo.shard_index,
            shard_count: shards.len(),
        })?;
    shard.revert(shard_undo);
    Ok(())
}
