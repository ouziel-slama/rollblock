use std::path::{Path, PathBuf};

use crate::error::StoreResult;
use crate::types::BlockId;

pub(super) fn snapshot_path(root_dir: &Path, block: BlockId) -> PathBuf {
    root_dir.join(format!("snapshot_{:016x}.bin", block))
}

pub(super) fn latest_snapshot(root_dir: &Path) -> StoreResult<Option<(PathBuf, BlockId)>> {
    Ok(snapshots_desc(root_dir)?.into_iter().next())
}

pub(super) fn snapshots_desc(root_dir: &Path) -> StoreResult<Vec<(PathBuf, BlockId)>> {
    if !root_dir.exists() {
        return Ok(Vec::new());
    }

    let mut snapshots: Vec<(BlockId, PathBuf)> = Vec::new();

    for entry in std::fs::read_dir(root_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        let Some(hex_block) = file_name
            .strip_prefix("snapshot_")
            .and_then(|rest| rest.strip_suffix(".bin"))
        else {
            continue;
        };

        let Ok(block_height) = u64::from_str_radix(hex_block, 16) else {
            continue;
        };

        snapshots.push((block_height, entry.path()));
    }

    snapshots.sort_by(|a, b| b.0.cmp(&a.0));

    Ok(snapshots
        .into_iter()
        .map(|(block, path)| (path, block))
        .collect())
}

pub(super) fn cleanup_old_snapshots(root_dir: &Path, keep_block: BlockId) {
    const MAX_RETAINED: usize = 2;

    match snapshots_desc(root_dir) {
        Ok(snapshots) => {
            if snapshots.len() <= MAX_RETAINED {
                return;
            }

            for (index, (path, block_height)) in snapshots.into_iter().enumerate() {
                if index < MAX_RETAINED {
                    continue;
                }

                if block_height < keep_block {
                    if let Err(err) = std::fs::remove_file(&path) {
                        tracing::warn!(
                            block = block_height,
                            keep_block,
                            path = ?path,
                            ?err,
                            "Failed to remove stale snapshot"
                        );
                    } else {
                        tracing::info!(
                            removed_block = block_height,
                            keep_block,
                            path = ?path,
                            "Removed stale snapshot after creating new snapshot"
                        );
                    }
                }
            }
        }
        Err(err) => {
            tracing::warn!(?err, "Failed to enumerate snapshots for cleanup");
        }
    }
}

pub(super) fn prune_after(root_dir: &Path, block: BlockId) -> StoreResult<()> {
    if !root_dir.exists() {
        return Ok(());
    }

    for entry in std::fs::read_dir(root_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        let Some(hex_block) = file_name
            .strip_prefix("snapshot_")
            .and_then(|rest| rest.strip_suffix(".bin"))
        else {
            continue;
        };

        let Ok(snapshot_block) = u64::from_str_radix(hex_block, 16) else {
            continue;
        };

        if snapshot_block > block {
            let path = entry.path();
            tracing::info!(
                snapshot_block,
                rollback_target = block,
                path = ?path,
                "Deleting snapshot beyond rollback target"
            );
            std::fs::remove_file(path)?;
        }
    }

    Ok(())
}
