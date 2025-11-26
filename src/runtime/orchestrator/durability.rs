//! Durability configuration primitives shared by orchestrators and persistence runtime.
//!
//! `DurabilityMode` determines whether persistence happens synchronously on the
//! calling thread or asynchronously via the background runtime. `PersistenceSettings`
//! groups all persistence related configuration knobs and provides sane defaults.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Operating mode for block durability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurabilityMode {
    /// Persist every block inline with the mutation path.
    Synchronous,
    /// Persist blocks synchronously but relax fsync cadence.
    /// Syncs every `sync_every_n_blocks` blocks.
    SynchronousRelaxed { sync_every_n_blocks: usize },
    /// Persist blocks asynchronously, allowing up to `max_pending_blocks` to queue.
    Async { max_pending_blocks: usize },
    /// Persist blocks asynchronously with relaxed sync guarantees.
    /// Syncs to disk every `sync_every_n_blocks` blocks instead of every block.
    /// This improves throughput but increases the window of potential data loss.
    AsyncRelaxed {
        max_pending_blocks: usize,
        /// Number of blocks between fsync calls. Higher = faster but riskier.
        sync_every_n_blocks: usize,
    },
}

impl DurabilityMode {
    /// Maximum number of blocks that may be pending persistence for this mode.
    pub fn max_pending_blocks(&self) -> usize {
        match self {
            DurabilityMode::Synchronous => 1,
            DurabilityMode::SynchronousRelaxed { .. } => 1,
            DurabilityMode::Async { max_pending_blocks } => *max_pending_blocks,
            DurabilityMode::AsyncRelaxed {
                max_pending_blocks, ..
            } => *max_pending_blocks,
        }
    }

    /// Helper to determine if persistence happens off-thread.
    pub fn is_async(&self) -> bool {
        matches!(
            self,
            DurabilityMode::Async { .. } | DurabilityMode::AsyncRelaxed { .. }
        )
    }

    /// Returns the sync interval in blocks. 0 means sync every block.
    pub fn sync_every_n_blocks(&self) -> usize {
        match self {
            DurabilityMode::Synchronous => 0,
            DurabilityMode::SynchronousRelaxed {
                sync_every_n_blocks,
            } => *sync_every_n_blocks,
            DurabilityMode::Async { .. } => 0,
            DurabilityMode::AsyncRelaxed {
                sync_every_n_blocks,
                ..
            } => *sync_every_n_blocks,
        }
    }

    /// Returns true if this mode uses relaxed sync semantics.
    pub fn is_relaxed(&self) -> bool {
        matches!(
            self,
            DurabilityMode::SynchronousRelaxed { .. } | DurabilityMode::AsyncRelaxed { .. }
        )
    }

    /// Creates a relaxed async mode with sensible defaults.
    /// Syncs every 100 blocks by default.
    pub fn async_relaxed() -> Self {
        Self::AsyncRelaxed {
            max_pending_blocks: 1024,
            sync_every_n_blocks: 100,
        }
    }
}

impl Default for DurabilityMode {
    fn default() -> Self {
        DurabilityMode::Async {
            max_pending_blocks: 1024,
        }
    }
}

/// High level persistence configuration for orchestrators.
#[derive(Debug, Clone)]
pub struct PersistenceSettings {
    pub durability_mode: DurabilityMode,
    pub snapshot_interval: Duration,
    pub max_snapshot_interval: Duration,
}

impl Default for PersistenceSettings {
    fn default() -> Self {
        Self {
            durability_mode: DurabilityMode::default(),
            snapshot_interval: Duration::from_secs(3600),
            max_snapshot_interval: Duration::from_secs(3600),
        }
    }
}
