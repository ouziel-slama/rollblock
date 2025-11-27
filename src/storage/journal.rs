use crate::types::{BlockId, BlockUndo, JournalMeta, Operation};

use crate::error::StoreResult;
use serde::{Deserialize, Serialize};

pub mod chunk;
pub mod file;
pub mod format;
pub mod iter;
pub mod maintenance;

pub use file::FileBlockJournal;
pub use iter::JournalIter;

pub trait BlockJournal: Send + Sync {
    fn append(
        &self,
        block: BlockId,
        undo: &BlockUndo,
        operations: &[Operation],
    ) -> StoreResult<JournalAppendOutcome>;
    fn iter_backwards(&self, from: BlockId, to: BlockId) -> StoreResult<JournalIter>;
    fn read_entry(&self, meta: &JournalMeta) -> StoreResult<JournalBlock>;
    fn list_entries(&self) -> StoreResult<Vec<JournalMeta>>;
    fn truncate_after(&self, block: BlockId) -> StoreResult<()>;
    fn rewrite_index(&self, metas: &[JournalMeta]) -> StoreResult<()> {
        let _ = metas;
        Ok(())
    }
    fn scan_entries(&self) -> StoreResult<Vec<JournalMeta>> {
        Ok(Vec::new())
    }
    /// Forces an immediate sync of all pending writes to disk.
    /// Called during shutdown or when explicit durability is required.
    fn force_sync(&self) -> StoreResult<()> {
        Ok(()) // Default no-op for implementations that always sync
    }

    /// Changes the sync policy at runtime.
    /// Useful for switching between relaxed mode (during initial sync)
    /// and strict mode (when at chain tip).
    fn set_sync_policy(&self, _policy: SyncPolicy) {
        // Default no-op for implementations that don't support runtime changes
    }
}

/// Controls when the journal syncs data to disk.
#[derive(Debug)]
pub enum SyncPolicy {
    /// Sync after every block (safest, slowest).
    EveryBlock,
    /// Sync every N blocks (trade-off between safety and speed).
    EveryNBlocks {
        n: usize,
        counter: std::sync::atomic::AtomicUsize,
    },
    /// Never sync automatically (fastest, caller must sync manually).
    Manual,
}

impl Clone for SyncPolicy {
    fn clone(&self) -> Self {
        match self {
            Self::EveryBlock => Self::EveryBlock,
            Self::EveryNBlocks { n, counter } => Self::EveryNBlocks {
                n: *n,
                counter: std::sync::atomic::AtomicUsize::new(
                    counter.load(std::sync::atomic::Ordering::Relaxed),
                ),
            },
            Self::Manual => Self::Manual,
        }
    }
}

impl Default for SyncPolicy {
    fn default() -> Self {
        Self::EveryBlock
    }
}

impl SyncPolicy {
    /// Creates a policy that syncs every N blocks.
    pub fn every_n_blocks(n: usize) -> Self {
        Self::EveryNBlocks {
            n: n.max(1),
            counter: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Returns true if we should sync now, and updates internal counter.
    pub fn should_sync(&self) -> bool {
        match self {
            Self::EveryBlock => true,
            Self::EveryNBlocks { n, counter } => {
                let current = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let next = current.wrapping_add(1);
                next % n == 0
            }
            Self::Manual => false,
        }
    }

    /// Forces the counter to trigger a sync on next call.
    pub fn reset(&self) {
        if let Self::EveryNBlocks { n, counter } = self {
            // Set counter to n-1 so next increment triggers sync
            counter.store(n.saturating_sub(1), std::sync::atomic::Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone)]
pub struct JournalOptions {
    pub compress: bool,
    pub compression_level: i32,
    pub sync_policy: SyncPolicy,
    pub max_chunk_size_bytes: u64,
}

impl Default for JournalOptions {
    fn default() -> Self {
        Self {
            compress: true,
            compression_level: 0,
            sync_policy: SyncPolicy::default(),
            max_chunk_size_bytes: 128 << 20,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalBlock {
    pub block_height: BlockId,
    pub operations: Vec<Operation>,
    pub undo: BlockUndo,
}

/// Result of appending a block to the journal.
#[derive(Debug, Clone)]
pub struct JournalAppendOutcome {
    /// Metadata describing where the block was stored.
    pub meta: JournalMeta,
    /// Whether this append triggered a filesystem sync, guaranteeing durability.
    pub synced: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_policy_every_block_always_syncs() {
        let policy = SyncPolicy::EveryBlock;
        assert!(policy.should_sync());
        assert!(policy.should_sync());
        assert!(policy.should_sync());
    }

    #[test]
    fn sync_policy_every_n_blocks_syncs_periodically() {
        let policy = SyncPolicy::every_n_blocks(3);
        assert!(!policy.should_sync()); // 1
        assert!(!policy.should_sync()); // 2
        assert!(policy.should_sync()); // 3 - sync!
        assert!(!policy.should_sync()); // 1
        assert!(!policy.should_sync()); // 2
        assert!(policy.should_sync()); // 3 - sync!
    }

    #[test]
    fn sync_policy_manual_never_syncs() {
        let policy = SyncPolicy::Manual;
        for _ in 0..100 {
            assert!(!policy.should_sync());
        }
    }

    #[test]
    fn sync_policy_reset_triggers_next_sync() {
        let policy = SyncPolicy::every_n_blocks(10);
        assert!(!policy.should_sync()); // 1
        policy.reset();
        assert!(policy.should_sync()); // Should sync after reset
    }

    #[test]
    fn sync_policy_every_n_blocks_with_n_equals_one() {
        let policy = SyncPolicy::every_n_blocks(1);
        assert!(policy.should_sync());
        assert!(policy.should_sync());
        assert!(policy.should_sync());
    }

    #[test]
    fn sync_policy_counter_wraps_without_panicking() {
        let policy = SyncPolicy::every_n_blocks(2);
        if let SyncPolicy::EveryNBlocks { counter, .. } = &policy {
            counter.store(usize::MAX, std::sync::atomic::Ordering::Relaxed);
        } else {
            panic!("expected EveryNBlocks policy");
        }
        // Should not panic even though the counter overflows.
        policy.should_sync();
    }

    #[test]
    fn sync_policy_clone_preserves_counter() {
        let policy = SyncPolicy::every_n_blocks(5);
        assert!(!policy.should_sync()); // 1
        assert!(!policy.should_sync()); // 2

        let cloned = policy.clone();
        // Cloned policy should have the same counter state
        assert!(!cloned.should_sync()); // 3
        assert!(!cloned.should_sync()); // 4
        assert!(cloned.should_sync()); // 5 - sync!
    }
}
