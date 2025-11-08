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
    /// Persist blocks asynchronously, allowing up to `max_pending_blocks` to queue.
    Async { max_pending_blocks: usize },
}

impl DurabilityMode {
    /// Maximum number of blocks that may be pending persistence for this mode.
    pub fn max_pending_blocks(&self) -> usize {
        match self {
            DurabilityMode::Synchronous => 1,
            DurabilityMode::Async { max_pending_blocks } => *max_pending_blocks,
        }
    }

    /// Helper to determine if persistence happens off-thread.
    pub fn is_async(&self) -> bool {
        matches!(self, DurabilityMode::Async { .. })
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
}

impl Default for PersistenceSettings {
    fn default() -> Self {
        Self {
            durability_mode: DurabilityMode::default(),
            snapshot_interval: Duration::from_secs(3600),
        }
    }
}
