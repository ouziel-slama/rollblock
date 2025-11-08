//! Filesystem helpers shared by persistence components.

pub mod store_lock;

pub use store_lock::StoreLockGuard;

use crate::error::StoreResult;
use std::path::Path;

/// Sync directory entries to disk to guarantee metadata durability.
pub fn sync_directory(path: &Path) -> StoreResult<()> {
    // Opening directories is platform-dependent. `std::fs::File::open` works on Linux
    // and macOS as long as the path exists.
    let dir = std::fs::File::open(path)?;
    dir.sync_all()?;
    Ok(())
}
