use crate::storage::fs::sync_directory;
use crate::types::{BlockId, BlockUndo, JournalMeta, Operation};

use crate::error::StoreResult;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::warn;

pub mod chunk;
pub mod file;
pub mod format;
pub mod iter;
pub mod maintenance;
pub mod pruner;

pub use file::FileBlockJournal;
pub use iter::JournalIter;
pub use pruner::{JournalPruneObserver, JournalPruner};

pub(crate) type ActiveChunkResolver = Arc<dyn Fn() -> StoreResult<Option<u32>> + Send + Sync>;

#[derive(Clone)]
pub(crate) struct FilePlanContext {
    chunk_dir: PathBuf,
    index_path: PathBuf,
    write_lock: Arc<Mutex<()>>,
    active_chunk: ActiveChunkResolver,
}

impl FilePlanContext {
    pub(crate) fn new(
        chunk_dir: PathBuf,
        index_path: PathBuf,
        write_lock: Arc<Mutex<()>>,
        active_chunk: ActiveChunkResolver,
    ) -> Self {
        Self {
            chunk_dir,
            index_path,
            write_lock,
            active_chunk,
        }
    }

    fn chunk_dir(&self) -> &Path {
        &self.chunk_dir
    }

    fn index_path(&self) -> &Path {
        &self.index_path
    }

    fn write_lock(&self) -> &Arc<Mutex<()>> {
        &self.write_lock
    }

    fn active_chunk(&self) -> &ActiveChunkResolver {
        &self.active_chunk
    }
}

#[derive(Debug, Clone)]
pub struct JournalSizingSnapshot {
    pub entry_sizes: Vec<u64>,
    pub sealed_chunk_count: usize,
    pub chunk_count: usize,
    pub min_entry_size_bytes: u64,
    pub max_chunk_size_bytes: u64,
}

impl JournalSizingSnapshot {
    pub fn sample_size(&self) -> usize {
        self.entry_sizes.len()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ChunkDeletionPlan {
    pub chunk_ids: Vec<u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalPruneReport {
    pub pruned_through: BlockId,
    pub chunks_removed: usize,
    pub entries_removed: usize,
    pub bytes_freed: u64,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JournalPrunePlanOutcome {
    Applied,
    Conflicted,
}

pub struct JournalPrunePlan {
    report: JournalPruneReport,
    chunk_plan: ChunkDeletionPlan,
    staged_index_path: PathBuf,
    baseline_entry_count: usize,
    cleanup_on_drop: bool,
    kind: JournalPrunePlanKind,
}

enum JournalPrunePlanKind {
    NoOp,
    File { context: FilePlanContext },
}

impl JournalPrunePlan {
    pub(crate) fn file_plan(
        report: JournalPruneReport,
        chunk_plan: ChunkDeletionPlan,
        staged_index_path: PathBuf,
        baseline_entry_count: usize,
        context: FilePlanContext,
    ) -> Self {
        Self {
            report,
            chunk_plan,
            staged_index_path,
            baseline_entry_count,
            cleanup_on_drop: true,
            kind: JournalPrunePlanKind::File { context },
        }
    }

    pub fn no_op(report: JournalPruneReport) -> Self {
        Self {
            report,
            chunk_plan: ChunkDeletionPlan {
                chunk_ids: Vec::new(),
            },
            staged_index_path: PathBuf::new(),
            baseline_entry_count: 0,
            cleanup_on_drop: false,
            kind: JournalPrunePlanKind::NoOp,
        }
    }

    pub fn report(&self) -> &JournalPruneReport {
        &self.report
    }

    pub fn chunk_plan(&self) -> &ChunkDeletionPlan {
        &self.chunk_plan
    }

    pub fn staged_index_path(&self) -> &Path {
        &self.staged_index_path
    }

    pub fn baseline_entry_count(&self) -> usize {
        self.baseline_entry_count
    }

    pub fn mark_persisted(&mut self) {
        self.cleanup_on_drop = false;
    }

    pub(crate) fn recover_file_plan(
        report: JournalPruneReport,
        chunk_plan: ChunkDeletionPlan,
        staged_index_path: PathBuf,
        baseline_entry_count: usize,
        context: FilePlanContext,
    ) -> Self {
        let mut plan = Self::file_plan(
            report,
            chunk_plan,
            staged_index_path,
            baseline_entry_count,
            context,
        );
        plan.cleanup_on_drop = false;
        plan
    }

    pub fn commit_with_hooks<Pre, Post, Conflict>(
        mut self,
        pre_finalize: Pre,
        post_finalize: Post,
        on_conflict: Conflict,
    ) -> StoreResult<JournalPrunePlanOutcome>
    where
        Pre: FnOnce() -> StoreResult<()>,
        Post: FnOnce() -> StoreResult<()>,
        Conflict: FnOnce() -> StoreResult<()>,
    {
        self.cleanup_on_drop = false;

        match &self.kind {
            JournalPrunePlanKind::NoOp => {
                pre_finalize()?;
                post_finalize()?;
                Ok(JournalPrunePlanOutcome::Applied)
            }
            JournalPrunePlanKind::File { context } => Self::finalize_file_plan_with_hooks(
                context,
                &self.staged_index_path,
                self.baseline_entry_count,
                &self.chunk_plan.chunk_ids,
                pre_finalize,
                post_finalize,
                on_conflict,
            ),
        }
    }

    pub fn commit(self) -> StoreResult<()> {
        let _ = self.commit_with_hooks(|| Ok(()), || Ok(()), || Ok(()))?;
        Ok(())
    }

    fn finalize_file_plan_with_hooks<Pre, Post, Conflict>(
        context: &FilePlanContext,
        staged_index_path: &Path,
        baseline_entry_count: usize,
        chunk_ids: &[u32],
        pre_finalize: Pre,
        post_finalize: Post,
        on_conflict: Conflict,
    ) -> StoreResult<JournalPrunePlanOutcome>
    where
        Pre: FnOnce() -> StoreResult<()>,
        Post: FnOnce() -> StoreResult<()>,
        Conflict: FnOnce() -> StoreResult<()>,
    {
        let _guard = context.write_lock().lock();

        if !chunk_ids.is_empty() && !staged_index_path.exists() {
            warn!(
                path = ?staged_index_path,
                "staged_journal_index_missing_aborting_prune"
            );
            on_conflict()?;
            return Ok(JournalPrunePlanOutcome::Conflicted);
        }

        if Self::plan_conflicts_with_active_chunk(
            context.chunk_dir(),
            chunk_ids,
            context.active_chunk(),
        )? {
            if staged_index_path.exists() {
                fs::remove_file(staged_index_path)?;
            }
            on_conflict()?;
            return Ok(JournalPrunePlanOutcome::Conflicted);
        }

        pre_finalize()?;

        if staged_index_path.exists() {
            Self::append_new_entries(
                context.index_path(),
                staged_index_path,
                baseline_entry_count,
                chunk_ids,
            )?;

            Self::swap_index_files(context.index_path(), staged_index_path)?;
        }

        for chunk_id in chunk_ids {
            let path =
                crate::storage::journal::chunk::chunk_file_path(context.chunk_dir(), *chunk_id);
            if path.exists() {
                fs::remove_file(&path)?;
            }
        }
        if context.chunk_dir().exists() {
            sync_directory(context.chunk_dir())?;
        }

        post_finalize()?;

        Ok(JournalPrunePlanOutcome::Applied)
    }

    fn plan_conflicts_with_active_chunk(
        chunk_dir: &Path,
        chunk_ids: &[u32],
        active_chunk: &ActiveChunkResolver,
    ) -> StoreResult<bool> {
        if chunk_ids.is_empty() {
            return Ok(false);
        }

        let chunk_listing = crate::storage::journal::chunk::enumerate_chunk_files(chunk_dir)?;
        if chunk_listing.is_empty() {
            return Ok(false);
        }

        let existing: HashSet<u32> = chunk_listing
            .iter()
            .map(|(chunk_id, _)| *chunk_id)
            .collect();
        if existing.is_empty() {
            return Ok(false);
        }

        let from_state = active_chunk()?;
        let from_listing = chunk_listing.last().map(|(chunk_id, _)| *chunk_id);
        let open_chunk_id = match (from_state, from_listing) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        let Some(open_chunk_id) = open_chunk_id else {
            return Ok(false);
        };

        Ok(chunk_ids
            .iter()
            .filter(|chunk_id| existing.contains(chunk_id))
            .any(|chunk_id| *chunk_id >= open_chunk_id))
    }

    fn append_new_entries(
        index_path: &Path,
        staged_index_path: &Path,
        baseline_entry_count: usize,
        chunk_ids: &[u32],
    ) -> StoreResult<()> {
        if !index_path.exists() {
            return Ok(());
        }
        let removal_set: HashSet<u32> = chunk_ids.iter().copied().collect();
        let mut reader = fs::File::open(index_path)?;
        let mut staged = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(staged_index_path)?;
        let mut seen = 0usize;

        loop {
            match bincode::deserialize_from::<_, JournalMeta>(&mut reader) {
                Ok(meta) => {
                    if seen >= baseline_entry_count && !removal_set.contains(&meta.chunk_id) {
                        let bytes = bincode::serialize(&meta)?;
                        staged.write_all(&bytes)?;
                    }
                    seen += 1;
                }
                Err(err) => {
                    if let bincode::ErrorKind::Io(ref io_err) = *err {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            break;
                        }
                    }
                    return Err(err.into());
                }
            }
        }

        staged.sync_all()?;
        Ok(())
    }

    fn swap_index_files(index_path: &Path, staged_index_path: &Path) -> StoreResult<()> {
        if index_path.exists() {
            fs::remove_file(index_path)?;
        }
        fs::rename(staged_index_path, index_path)?;
        if let Some(parent) = index_path.parent() {
            sync_directory(parent)?;
        }
        Ok(())
    }
}

impl Drop for JournalPrunePlan {
    fn drop(&mut self) {
        if self.cleanup_on_drop
            && matches!(self.kind, JournalPrunePlanKind::File { .. })
            && self.staged_index_path.exists()
        {
            let _ = fs::remove_file(&self.staged_index_path);
        }
    }
}

#[cfg(test)]
mod prune_plan_tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::tempdir;

    #[test]
    fn missing_staged_index_aborts_plan_before_side_effects() {
        let tmp = tempdir().expect("tempdir");
        let chunk_dir = tmp.path().join("chunks");
        let index_path = tmp.path().join("journal.idx");
        std::fs::create_dir_all(&chunk_dir).expect("chunk dir");

        let context = FilePlanContext::new(
            chunk_dir,
            index_path,
            Arc::new(Mutex::new(())),
            Arc::new(|| Ok(None)),
        );

        let staged_index_path = tmp.path().join("journal.idx.gc");
        assert!(
            !staged_index_path.exists(),
            "staged index should start absent for test"
        );

        let plan = JournalPrunePlan::file_plan(
            JournalPruneReport {
                pruned_through: 10,
                chunks_removed: 1,
                entries_removed: 5,
                bytes_freed: 1,
            },
            ChunkDeletionPlan { chunk_ids: vec![1] },
            staged_index_path,
            0,
            context,
        );

        let pre_called = Arc::new(AtomicBool::new(false));
        let conflict_called = Arc::new(AtomicBool::new(false));
        let pre_flag = Arc::clone(&pre_called);
        let conflict_flag = Arc::clone(&conflict_called);

        let outcome = plan
            .commit_with_hooks(
                move || {
                    pre_flag.store(true, Ordering::SeqCst);
                    Ok(())
                },
                || Ok(()),
                move || {
                    conflict_flag.store(true, Ordering::SeqCst);
                    Ok(())
                },
            )
            .expect("commit succeeds");

        assert_eq!(outcome, JournalPrunePlanOutcome::Conflicted);
        assert!(
            !pre_called.load(Ordering::SeqCst),
            "metadata pruning must not run when staged index is missing"
        );
        assert!(
            conflict_called.load(Ordering::SeqCst),
            "conflict hook must run so metadata can clear pending watermark"
        );
    }
}

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

    fn plan_prune_chunks_ending_at_or_before(
        &self,
        block: BlockId,
    ) -> StoreResult<Option<JournalPrunePlan>> {
        let _ = block;
        Ok(None)
    }

    fn adopt_staged_prune_plan(
        &self,
        pruned_through: BlockId,
        chunk_plan: ChunkDeletionPlan,
        staged_index_path: PathBuf,
        baseline_entry_count: usize,
        report: JournalPruneReport,
    ) -> StoreResult<JournalPrunePlan> {
        let _ = (
            pruned_through,
            chunk_plan,
            staged_index_path,
            baseline_entry_count,
            report,
        );
        Ok(JournalPrunePlan::no_op(JournalPruneReport::default()))
    }
}

/// Controls when the journal syncs data to disk.
#[derive(Debug, Default)]
pub enum SyncPolicy {
    /// Sync after every block (safest, slowest).
    #[default]
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
