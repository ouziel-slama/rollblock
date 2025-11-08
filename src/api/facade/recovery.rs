use std::sync::Arc;

use crate::block_journal::BlockJournal;
use crate::error::{MhinStoreError, StoreResult};
use crate::metadata::{LmdbMetadataStore, MetadataStore, ShardLayout};
use crate::snapshot::{MmapSnapshotter, Snapshotter};
use crate::state_engine::ShardedStateEngine;
use crate::state_shard::StateShard;
use crate::types::BlockId;

use super::config::StoreConfig;

pub(crate) fn restore_existing_state(
    snapshotter: &MmapSnapshotter,
    metadata: &LmdbMetadataStore,
    shards: &[Arc<dyn StateShard>],
) -> StoreResult<BlockId> {
    let recorded_current = metadata.current_block()?;
    let snapshots = snapshotter.snapshots_desc()?;

    let mut prune_newer_snapshots = recorded_current != 0;
    if !prune_newer_snapshots {
        if let Some(start) = recorded_current.checked_add(1) {
            let newer_offsets = metadata.get_journal_offsets(start..=u64::MAX)?;
            prune_newer_snapshots = !newer_offsets.is_empty();
        }
    }

    for (path, snapshot_block) in snapshots {
        if prune_newer_snapshots && snapshot_block > recorded_current {
            tracing::info!(
                block_height = snapshot_block,
                current_block = recorded_current,
                path = ?path,
                "Removing snapshot newer than recorded metadata"
            );
            if let Err(err) = std::fs::remove_file(&path) {
                tracing::warn!(
                    block_height = snapshot_block,
                    path = ?path,
                    ?err,
                    "Failed to delete snapshot newer than metadata"
                );
            }
            continue;
        }

        tracing::info!(
            block_height = snapshot_block,
            path = ?path,
            "Loading snapshot during initialization"
        );

        match snapshotter.load_snapshot(&path, shards) {
            Ok(loaded_block) => {
                if loaded_block != snapshot_block {
                    tracing::warn!(
                        expected = snapshot_block,
                        actual = loaded_block,
                        "Snapshot block height mismatch; using snapshot-reported block height"
                    );
                }

                if recorded_current < loaded_block {
                    tracing::info!(
                        current_block = recorded_current,
                        restored_block = loaded_block,
                        "Updating metadata current block to match snapshot"
                    );
                    metadata.set_current_block(loaded_block)?;
                } else if recorded_current > loaded_block {
                    tracing::info!(
                        current_block = recorded_current,
                        restored_block = loaded_block,
                        "Snapshot is behind metadata; pending blocks will be replayed from journal"
                    );
                }

                return Ok(loaded_block);
            }
            Err(MhinStoreError::SnapshotCorrupted { reason, .. }) => {
                tracing::warn!(
                    block_height = snapshot_block,
                    path = ?path,
                    %reason,
                    "Snapshot corrupted during load; attempting fallback"
                );
                if let Err(err) = std::fs::remove_file(&path) {
                    tracing::warn!(
                        block_height = snapshot_block,
                        path = ?path,
                        ?err,
                        "Failed to delete corrupted snapshot file"
                    );
                }
            }
            Err(err) => return Err(err),
        }
    }

    if recorded_current != 0 {
        tracing::warn!(
            current_block = recorded_current,
            "Metadata indicates prior state but no usable snapshot was found; rebuilding from journal"
        );
    } else {
        tracing::info!("No snapshot found; starting from empty state at block 0");
    }

    Ok(0)
}

pub(crate) fn restore_existing_state_read_only(
    snapshotter: &MmapSnapshotter,
    metadata: &LmdbMetadataStore,
    shards: &[Arc<dyn StateShard>],
) -> StoreResult<BlockId> {
    let recorded_current = metadata.current_block()?;
    let snapshots = snapshotter.snapshots_desc()?;

    for (path, snapshot_block) in snapshots {
        if snapshot_block > recorded_current && recorded_current != 0 {
            tracing::info!(
                block_height = snapshot_block,
                current_block = recorded_current,
                path = ?path,
                "Skipping snapshot newer than metadata in read-only mode"
            );
            continue;
        }

        match snapshotter.load_snapshot(&path, shards) {
            Ok(loaded_block) => {
                if loaded_block != snapshot_block {
                    tracing::warn!(
                        expected = snapshot_block,
                        actual = loaded_block,
                        "Snapshot block mismatch while opening in read-only mode"
                    );
                }
                return Ok(loaded_block);
            }
            Err(MhinStoreError::SnapshotCorrupted { reason, .. }) => {
                tracing::warn!(
                    block_height = snapshot_block,
                    path = ?path,
                    %reason,
                    "Encountered corrupted snapshot while opening in read-only mode"
                );
                continue;
            }
            Err(err) => return Err(err),
        }
    }

    Ok(0)
}

pub(crate) fn replay_committed_blocks<J, M>(
    journal: &J,
    metadata: &M,
    engine: &ShardedStateEngine<M>,
    restored_block: BlockId,
) -> StoreResult<()>
where
    J: BlockJournal,
    M: MetadataStore + 'static,
{
    let target_block = metadata.current_block()?;
    if target_block <= restored_block {
        tracing::debug!(
            target_block,
            restored_block,
            "No journal replay required; snapshot already up to date"
        );
        return Ok(());
    }

    let start_block = restored_block.saturating_add(1);
    let mut metas = metadata.get_journal_offsets(start_block..=target_block)?;
    if metas.is_empty() {
        tracing::error!(
            start_block,
            target_block,
            restored_block,
            "Metadata current block is ahead of snapshot but no journal offsets are available"
        );
        return Err(MhinStoreError::MissingJournalEntry { block: start_block });
    }

    metas.sort_by_key(|meta| meta.block_height);
    metas.dedup_by_key(|meta| meta.block_height);

    let mut expected_block = start_block;
    let mut missing_blocks = Vec::new();

    for meta in &metas {
        if meta.block_height < start_block {
            continue;
        }

        while expected_block < meta.block_height && expected_block <= target_block {
            missing_blocks.push(expected_block);
            expected_block = expected_block.saturating_add(1);
        }

        if meta.block_height == expected_block {
            expected_block = expected_block.saturating_add(1);
        }
    }

    while expected_block <= target_block {
        missing_blocks.push(expected_block);
        expected_block = expected_block.saturating_add(1);
    }

    if let Some(missing) = missing_blocks.first().copied() {
        tracing::error!(
            start_block,
            target_block,
            restored_block,
            missing_block = missing,
            missing_blocks = ?missing_blocks,
            "Detected gaps between metadata and journal during recovery"
        );
        return Err(MhinStoreError::MissingJournalEntry { block: missing });
    }

    for meta in metas {
        let entry = journal.read_entry(&meta)?;
        if entry.block_height <= restored_block {
            continue;
        }

        tracing::info!(
            block_height = entry.block_height,
            "Replaying committed block from journal"
        );
        engine.apply_replayed_block(entry.block_height, &entry.operations, &entry.undo)?;
    }

    Ok(())
}

pub(crate) fn reconcile_metadata_with_journal<J, M>(
    journal: &J,
    metadata: &M,
) -> StoreResult<BlockId>
where
    J: BlockJournal,
    M: MetadataStore,
{
    let current_block = metadata.current_block()?;

    let mut index_entries = journal.list_entries()?;
    index_entries.sort_by_key(|meta| meta.block_height);
    index_entries.dedup_by_key(|meta| meta.block_height);

    let mut scanned_entries = journal.scan_entries()?;
    scanned_entries.sort_by_key(|meta| meta.block_height);
    scanned_entries.dedup_by_key(|meta| meta.block_height);

    let mut entries = if !scanned_entries.is_empty() {
        let mut rewrite_required = index_entries.len() != scanned_entries.len();

        if !rewrite_required {
            for (indexed, scanned) in index_entries.iter().zip(scanned_entries.iter()) {
                if indexed.block_height != scanned.block_height
                    || indexed.offset != scanned.offset
                    || indexed.compressed_len != scanned.compressed_len
                    || indexed.checksum != scanned.checksum
                {
                    rewrite_required = true;
                    break;
                }
            }
        }

        if rewrite_required {
            journal.rewrite_index(&scanned_entries)?;
        }

        scanned_entries
    } else {
        index_entries
    };

    if entries.is_empty() {
        tracing::warn!(
            current_block,
            "Journal index empty during startup; attempting recovery"
        );

        let mut recovered = Vec::new();

        if current_block > 0 {
            match metadata.get_journal_offsets(0..=current_block) {
                Ok(mut metas) => recovered.append(&mut metas),
                Err(err) => tracing::warn!(
                    current_block,
                    ?err,
                    "Failed to load metadata offsets during journal recovery"
                ),
            }
        }

        if recovered.is_empty() {
            recovered = journal.scan_entries()?;
        }

        if !recovered.is_empty() {
            recovered.sort_by_key(|meta| meta.block_height);
            recovered.dedup_by_key(|meta| meta.block_height);

            let mut validated = Vec::with_capacity(recovered.len());
            for meta in recovered.into_iter() {
                match journal.read_entry(&meta) {
                    Ok(_) => {
                        metadata.put_journal_offset(meta.block_height, &meta)?;
                        validated.push(meta);
                    }
                    Err(err) => {
                        tracing::warn!(
                            block_height = meta.block_height,
                            ?err,
                            "Failed to validate recovered journal entry; stopping recovery"
                        );
                        break;
                    }
                }
            }

            if !validated.is_empty() {
                let durable_block = validated.last().unwrap().block_height;
                journal.rewrite_index(&validated)?;
                metadata.remove_journal_offsets_after(durable_block)?;
                metadata.set_current_block(durable_block)?;
                entries = validated;
            }
        }

        if entries.is_empty() {
            if current_block > 0 {
                tracing::warn!(
                    current_block,
                    "No durable journal entries could be recovered; resetting to block 0"
                );
                journal.truncate_after(0)?;
                metadata.remove_journal_offsets_after(0)?;
                metadata.set_current_block(0)?;
            }
            return Ok(0);
        }
    }

    entries.sort_by_key(|meta| meta.block_height);
    let tail_block = entries.last().map(|meta| meta.block_height);
    let mut latest_verified = 0u64;
    let mut pruned_tail = false;

    for meta in entries {
        let block_height = meta.block_height;
        if block_height <= latest_verified {
            continue;
        }

        match journal.read_entry(&meta) {
            Ok(_) => {
                latest_verified = block_height;
                if block_height > current_block {
                    tracing::info!(
                        block_height,
                        current_block,
                        "Metadata behind journal entry; reconciling during startup"
                    );
                    metadata.record_block_commit(block_height, &meta)?;
                }
            }
            Err(err) if Some(block_height) == tail_block => {
                if matches!(err, MhinStoreError::JournalChecksumMismatch { .. }) {
                    return Err(err);
                }
                tracing::warn!(
                    block_height,
                    ?err,
                    latest_durable_block = latest_verified,
                    "Failed to load tail journal entry; truncating to last durable block"
                );
                journal.truncate_after(latest_verified)?;
                metadata.remove_journal_offsets_after(latest_verified)?;
                metadata.set_current_block(latest_verified)?;
                pruned_tail = true;
                break;
            }
            Err(err) => return Err(err),
        }
    }

    if !pruned_tail && latest_verified < current_block {
        journal.truncate_after(latest_verified)?;
        metadata.remove_journal_offsets_after(latest_verified)?;
        metadata.set_current_block(latest_verified)?;
    }

    metadata.current_block()
}

pub(crate) fn resolve_shard_layout(
    metadata: &LmdbMetadataStore,
    config: &StoreConfig,
    allow_persist: bool,
) -> StoreResult<ShardLayout> {
    if let Some(stored) = metadata.load_shard_layout()? {
        if let Some(requested) = config.shards_count {
            if requested != stored.shards_count {
                return Err(MhinStoreError::ConfigurationMismatch {
                    field: "shards_count",
                    stored: stored.shards_count,
                    requested,
                });
            }
        }

        if let Some(requested) = config.initial_capacity {
            if requested != stored.initial_capacity {
                return Err(MhinStoreError::ConfigurationMismatch {
                    field: "initial_capacity",
                    stored: stored.initial_capacity,
                    requested,
                });
            }
        }

        return Ok(stored);
    }

    let shards_count = match config.shards_count {
        Some(value) => value,
        None if allow_persist => {
            return Err(MhinStoreError::MissingShardConfig {
                field: "shards_count",
            })
        }
        None => {
            return Err(MhinStoreError::MissingShardLayout {
                path: metadata.path().to_path_buf(),
            })
        }
    };

    let initial_capacity = match config.initial_capacity {
        Some(value) => value,
        None if allow_persist => {
            return Err(MhinStoreError::MissingShardConfig {
                field: "initial_capacity",
            })
        }
        None => {
            return Err(MhinStoreError::MissingShardLayout {
                path: metadata.path().to_path_buf(),
            })
        }
    };

    let layout = ShardLayout {
        shards_count,
        initial_capacity,
    };

    if allow_persist {
        metadata.store_shard_layout(&layout)?;
    }

    Ok(layout)
}
