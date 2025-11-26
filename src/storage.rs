//! Persistence backends: filesystem helpers, journals, metadata, snapshots.

pub mod fs;
pub mod journal;
pub mod metadata;
pub mod snapshot;

pub mod prelude {
    pub use super::fs::store_lock::StoreLockGuard;
    pub use super::journal::{
        BlockJournal, FileBlockJournal, JournalBlock, JournalIter, JournalOptions, SyncPolicy,
    };
    pub use super::metadata::{LmdbMetadataStore, MetadataStore, ShardLayout};
    pub use super::snapshot::{MmapSnapshotter, Snapshotter};
}
