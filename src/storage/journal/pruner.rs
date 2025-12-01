use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tracing::{error, info};

use crate::error::StoreResult;
use crate::storage::journal::{BlockJournal, JournalPrunePlanOutcome, JournalPruneReport};
use crate::storage::metadata::{GcWatermark, MetadataStore};
use crate::types::BlockId;

pub trait JournalPruneObserver: Send + Sync {
    fn record_iteration(&self, report: &JournalPruneReport);
    fn record_noop(&self);
}

impl JournalPruneObserver for () {
    fn record_iteration(&self, _report: &JournalPruneReport) {}
    fn record_noop(&self) {}
}

pub struct JournalPruner<J, M> {
    inner: Arc<JournalPrunerInner<J, M>>,
}

impl<J, M> Clone for JournalPruner<J, M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct JournalPrunerInner<J, M> {
    journal: Arc<J>,
    metadata: Arc<M>,
    min_window: BlockId,
    interval: Duration,
    current_block_fn: Arc<dyn Fn() -> BlockId + Send + Sync>,
    observer: Option<Arc<dyn JournalPruneObserver>>,
    stop: AtomicBool,
    started: AtomicBool,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    run_lock: Mutex<()>,
}

#[derive(Copy, Clone, Debug)]
enum PruneReason {
    Background,
    Manual,
    Recovery,
}

impl<J, M> JournalPruner<J, M>
where
    J: BlockJournal + 'static,
    M: MetadataStore + 'static,
{
    pub fn spawn(
        journal: Arc<J>,
        metadata: Arc<M>,
        min_window: BlockId,
        interval: Duration,
        current_block_fn: impl Fn() -> BlockId + Send + Sync + 'static,
        observer: Option<Arc<dyn JournalPruneObserver>>,
    ) -> Self {
        let inner = Arc::new(JournalPrunerInner {
            journal,
            metadata,
            min_window,
            interval,
            current_block_fn: Arc::new(current_block_fn),
            observer,
            stop: AtomicBool::new(false),
            started: AtomicBool::new(false),
            worker: Mutex::new(None),
            run_lock: Mutex::new(()),
        });
        Self { inner }
    }

    pub fn resume_pending_work(&self) -> StoreResult<()> {
        {
            let _guard = self.inner.run_lock.lock();
            if let Some(watermark) = self.inner.metadata.load_gc_watermark()? {
                match self.replay_plan(&watermark)? {
                    Some(report) => {
                        self.record_success(
                            PruneReason::Recovery,
                            &report,
                            Duration::from_millis(0),
                        );
                    }
                    None => self.record_noop(PruneReason::Recovery),
                }
            }
        }
        self.ensure_background_thread();
        Ok(())
    }

    pub fn prune_now(&self) -> StoreResult<Option<JournalPruneReport>> {
        self.execute_once(PruneReason::Manual)
    }

    pub fn shutdown(&self) {
        self.inner.stop.store(true, Ordering::SeqCst);
        join_worker(&self.inner.worker);
    }

    fn ensure_background_thread(&self) {
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        const MAX_IMMEDIATE_RETRIES: usize = 3;
        let worker = self.clone();
        let handle = thread::spawn(move || {
            let mut consecutive_failures = 0usize;
            while !worker.inner.stop.load(Ordering::SeqCst) {
                let started = Instant::now();
                let mut should_wait = true;
                match worker.execute_once(PruneReason::Background) {
                    Ok(_) => consecutive_failures = 0,
                    Err(err) => {
                        error!(?err, "journal_pruner_iteration_failed");
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        if consecutive_failures <= MAX_IMMEDIATE_RETRIES {
                            should_wait = false;
                        } else {
                            consecutive_failures = MAX_IMMEDIATE_RETRIES;
                        }
                    }
                }
                if worker.inner.stop.load(Ordering::SeqCst) {
                    break;
                }
                if should_wait {
                    worker.wait_for_next_iteration(started.elapsed());
                }
            }
        });
        *self.inner.worker.lock() = Some(handle);
    }

    fn wait_for_next_iteration(&self, elapsed: Duration) {
        if self.inner.interval <= elapsed {
            return;
        }
        let remaining = self.inner.interval - elapsed;
        thread::park_timeout(remaining);
    }

    fn execute_once(&self, reason: PruneReason) -> StoreResult<Option<JournalPruneReport>> {
        let _guard = self.inner.run_lock.lock();
        if let Some(watermark) = self.inner.metadata.load_gc_watermark()? {
            match self.replay_plan(&watermark)? {
                Some(report) => {
                    self.record_success(PruneReason::Recovery, &report, Duration::from_millis(0));
                    return Ok(Some(report));
                }
                None => {
                    self.record_noop(PruneReason::Recovery);
                    return Ok(None);
                }
            }
        }

        let started = Instant::now();
        match self.plan_and_execute()? {
            Some(report) => {
                self.record_success(reason, &report, started.elapsed());
                Ok(Some(report))
            }
            None => {
                self.record_noop(reason);
                Ok(None)
            }
        }
    }

    fn plan_and_execute(&self) -> StoreResult<Option<JournalPruneReport>> {
        if self.inner.min_window == BlockId::MAX {
            return Ok(None);
        }

        let current_block = (self.inner.current_block_fn)();
        if current_block <= self.inner.min_window {
            return Ok(None);
        }

        let mut prune_target = current_block.saturating_sub(self.inner.min_window);
        if prune_target == 0 {
            return Ok(None);
        }

        if let Some(snapshot_block) = self.inner.metadata.load_snapshot_watermark()? {
            if snapshot_block < prune_target {
                info!(
                    snapshot_block,
                    unclamped_target = prune_target,
                    "pruner_clamped_by_snapshot"
                );
                prune_target = snapshot_block;
            }
        }

        if prune_target == 0 {
            return Ok(None);
        }

        let mut plan = match self
            .inner
            .journal
            .plan_prune_chunks_ending_at_or_before(prune_target)?
        {
            Some(plan) => plan,
            None => return Ok(None),
        };

        let watermark = GcWatermark {
            pruned_through: plan.report().pruned_through,
            chunk_plan: plan.chunk_plan().clone(),
            staged_index_path: plan.staged_index_path().to_path_buf(),
            baseline_entry_count: plan.baseline_entry_count(),
            report: plan.report().clone(),
        };
        self.inner.metadata.store_gc_watermark(&watermark)?;
        plan.mark_persisted();

        let report = plan.report().clone();
        let pruned_through = report.pruned_through;
        let metadata_for_prune = Arc::clone(&self.inner.metadata);
        let metadata_for_post = Arc::clone(&self.inner.metadata);
        let metadata_for_conflict = Arc::clone(&self.inner.metadata);

        let outcome = plan.commit_with_hooks(
            move || {
                metadata_for_prune.prune_journal_offsets_at_or_before(pruned_through)?;
                Ok(())
            },
            move || metadata_for_post.clear_gc_watermark(),
            move || metadata_for_conflict.clear_gc_watermark(),
        )?;

        match outcome {
            JournalPrunePlanOutcome::Applied => Ok(Some(report)),
            JournalPrunePlanOutcome::Conflicted => Ok(None),
        }
    }

    fn replay_plan(&self, watermark: &GcWatermark) -> StoreResult<Option<JournalPruneReport>> {
        let mut plan = self.inner.journal.adopt_staged_prune_plan(
            watermark.pruned_through,
            watermark.chunk_plan.clone(),
            watermark.staged_index_path.clone(),
            watermark.baseline_entry_count,
            watermark.report.clone(),
        )?;
        plan.mark_persisted();
        let metadata_for_prune = Arc::clone(&self.inner.metadata);
        let metadata_for_post = Arc::clone(&self.inner.metadata);
        let metadata_for_conflict = Arc::clone(&self.inner.metadata);
        let pruned_through = watermark.pruned_through;
        let report = watermark.report.clone();

        let outcome = plan.commit_with_hooks(
            move || {
                metadata_for_prune.prune_journal_offsets_at_or_before(pruned_through)?;
                Ok(())
            },
            move || metadata_for_post.clear_gc_watermark(),
            move || metadata_for_conflict.clear_gc_watermark(),
        )?;

        match outcome {
            JournalPrunePlanOutcome::Applied => Ok(Some(report)),
            JournalPrunePlanOutcome::Conflicted => Ok(None),
        }
    }

    fn record_success(&self, reason: PruneReason, report: &JournalPruneReport, duration: Duration) {
        info!(
            pruned_through = report.pruned_through,
            chunks_removed = report.chunks_removed,
            entries_removed = report.entries_removed,
            bytes_freed = report.bytes_freed,
            duration_ms = duration.as_millis(),
            reason = ?reason,
            "journal_prune_success"
        );
        if let Some(observer) = &self.inner.observer {
            observer.record_iteration(report);
        }
    }

    fn record_noop(&self, reason: PruneReason) {
        info!(reason = ?reason, "journal_pruner_nop");
        if let Some(observer) = &self.inner.observer {
            observer.record_noop();
        }
    }
}

impl<J, M> Drop for JournalPruner<J, M> {
    fn drop(&mut self) {
        self.inner.stop.store(true, Ordering::SeqCst);
        join_worker(&self.inner.worker);
    }
}

fn join_worker(worker: &Mutex<Option<thread::JoinHandle<()>>>) {
    let handle = {
        let mut guard = worker.lock();
        guard.take()
    };
    if let Some(handle) = handle {
        handle.thread().unpark();
        let _ = handle.join();
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ChunkDeletionPlan, JournalPrunePlan};
    use super::PruneReason;
    use super::*;
    use parking_lot::Mutex;
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::time::Instant;

    #[derive(Default)]
    struct MockJournal {
        plans: Mutex<VecDeque<JournalPruneReport>>,
        adopt_calls: AtomicUsize,
    }

    impl MockJournal {
        fn new(reports: Vec<JournalPruneReport>) -> Self {
            Self {
                plans: Mutex::new(reports.into()),
                adopt_calls: AtomicUsize::new(0),
            }
        }

        fn adopt_invocations(&self) -> usize {
            self.adopt_calls.load(Ordering::SeqCst)
        }
    }

    impl BlockJournal for MockJournal {
        fn append(
            &self,
            _block: BlockId,
            _undo: &crate::types::BlockUndo,
            _operations: &[crate::types::Operation],
        ) -> StoreResult<crate::storage::journal::JournalAppendOutcome> {
            unreachable!("append not used in tests");
        }

        fn iter_backwards(
            &self,
            _from: BlockId,
            _to: BlockId,
        ) -> StoreResult<crate::storage::journal::JournalIter> {
            unreachable!("iter not used in tests");
        }

        fn read_entry(
            &self,
            _meta: &crate::types::JournalMeta,
        ) -> StoreResult<crate::storage::journal::JournalBlock> {
            unreachable!("read not used in tests");
        }

        fn list_entries(&self) -> StoreResult<Vec<crate::types::JournalMeta>> {
            Ok(Vec::new())
        }

        fn truncate_after(&self, _block: BlockId) -> StoreResult<()> {
            Ok(())
        }

        fn rewrite_index(&self, _metas: &[crate::types::JournalMeta]) -> StoreResult<()> {
            Ok(())
        }

        fn scan_entries(&self) -> StoreResult<Vec<crate::types::JournalMeta>> {
            Ok(Vec::new())
        }

        fn plan_prune_chunks_ending_at_or_before(
            &self,
            _block: BlockId,
        ) -> StoreResult<Option<JournalPrunePlan>> {
            let mut plans = self.plans.lock();
            Ok(plans.pop_front().map(JournalPrunePlan::no_op))
        }

        fn adopt_staged_prune_plan(
            &self,
            _pruned_through: BlockId,
            _chunk_plan: ChunkDeletionPlan,
            _staged_index_path: PathBuf,
            _baseline_entry_count: usize,
            report: JournalPruneReport,
        ) -> StoreResult<JournalPrunePlan> {
            self.adopt_calls.fetch_add(1, Ordering::SeqCst);
            Ok(JournalPrunePlan::no_op(report))
        }
    }

    #[derive(Default)]
    struct MockMetadata {
        watermark: Mutex<Option<GcWatermark>>,
        snapshot: Mutex<Option<BlockId>>,
        prune_calls: Mutex<Vec<BlockId>>,
    }

    impl MockMetadata {
        fn prune_history(&self) -> Vec<BlockId> {
            self.prune_calls.lock().clone()
        }
    }

    impl MetadataStore for MockMetadata {
        fn current_block(&self) -> StoreResult<BlockId> {
            Ok(0)
        }

        fn set_current_block(&self, _block: BlockId) -> StoreResult<()> {
            Ok(())
        }

        fn put_journal_offset(
            &self,
            _block: BlockId,
            _meta: &crate::types::JournalMeta,
        ) -> StoreResult<()> {
            Ok(())
        }

        fn get_journal_offsets(
            &self,
            _range: std::ops::RangeInclusive<BlockId>,
        ) -> StoreResult<Vec<crate::types::JournalMeta>> {
            Ok(Vec::new())
        }

        fn last_journal_offset_at_or_before(
            &self,
            _block: BlockId,
        ) -> StoreResult<Option<crate::types::JournalMeta>> {
            Ok(None)
        }

        fn remove_journal_offsets_after(&self, _block: BlockId) -> StoreResult<()> {
            Ok(())
        }

        fn prune_journal_offsets_at_or_before(&self, block: BlockId) -> StoreResult<usize> {
            self.prune_calls.lock().push(block);
            Ok(0)
        }

        fn load_gc_watermark(&self) -> StoreResult<Option<GcWatermark>> {
            Ok(self.watermark.lock().clone())
        }

        fn store_gc_watermark(&self, watermark: &GcWatermark) -> StoreResult<()> {
            *self.watermark.lock() = Some(watermark.clone());
            Ok(())
        }

        fn clear_gc_watermark(&self) -> StoreResult<()> {
            *self.watermark.lock() = None;
            Ok(())
        }

        fn load_snapshot_watermark(&self) -> StoreResult<Option<BlockId>> {
            Ok(*self.snapshot.lock())
        }

        fn store_snapshot_watermark(&self, block: BlockId) -> StoreResult<()> {
            *self.snapshot.lock() = Some(block);
            Ok(())
        }
    }

    fn sample_report(pruned: BlockId) -> JournalPruneReport {
        JournalPruneReport {
            pruned_through: pruned,
            chunks_removed: 1,
            entries_removed: 10,
            bytes_freed: 1024,
        }
    }

    #[test]
    fn prune_now_executes_single_iteration() {
        let journal = Arc::new(MockJournal::new(vec![sample_report(8)]));
        let metadata = Arc::new(MockMetadata::default());
        let pruner = JournalPruner::spawn(
            Arc::clone(&journal),
            Arc::clone(&metadata),
            1,
            Duration::from_secs(1),
            || 100,
            None,
        );

        let report = pruner.prune_now().unwrap().expect("report present");
        assert_eq!(report.pruned_through, 8);
        assert_eq!(metadata.prune_history(), vec![8]);

        pruner.shutdown();
    }

    #[test]
    fn background_iteration_consumes_plan() {
        let journal = Arc::new(MockJournal::new(vec![sample_report(12)]));
        let metadata = Arc::new(MockMetadata::default());
        let pruner = JournalPruner::spawn(
            Arc::clone(&journal),
            Arc::clone(&metadata),
            1,
            Duration::from_secs(1),
            || 100,
            None,
        );

        let result = pruner.execute_once(PruneReason::Background).unwrap();
        assert!(result.is_some());
        assert_eq!(metadata.prune_history(), vec![12]);
        pruner.shutdown();
    }

    #[test]
    fn replay_plan_applies_watermark() {
        let journal = Arc::new(MockJournal::new(Vec::new()));
        let metadata = Arc::new(MockMetadata::default());
        let pruner = JournalPruner::spawn(
            Arc::clone(&journal),
            Arc::clone(&metadata),
            1,
            Duration::from_secs(1),
            || 100,
            None,
        );

        let watermark = GcWatermark {
            pruned_through: 5,
            chunk_plan: ChunkDeletionPlan {
                chunk_ids: Vec::new(),
            },
            staged_index_path: PathBuf::new(),
            baseline_entry_count: 0,
            report: sample_report(5),
        };

        let report = pruner
            .replay_plan(&watermark)
            .unwrap()
            .expect("replay report present");
        assert_eq!(report.pruned_through, 5);
        assert_eq!(metadata.prune_history(), vec![5]);
        assert_eq!(journal.adopt_invocations(), 1);

        pruner.shutdown();
    }

    #[test]
    fn shutdown_waits_for_inflight_iteration() {
        let journal = Arc::new(MockJournal::new(Vec::new()));
        let metadata = Arc::new(MockMetadata::default());
        let pruner = JournalPruner::spawn(
            Arc::clone(&journal),
            Arc::clone(&metadata),
            1,
            Duration::from_millis(5),
            || 100,
            None,
        );

        let (tx, rx) = channel();
        *pruner.inner.worker.lock() = Some(thread::spawn(move || {
            let _ = rx.recv();
        }));

        let completed = Arc::new(AtomicBool::new(false));
        let completed_flag = Arc::clone(&completed);
        let pruner_for_shutdown = pruner.clone();
        let handle = thread::spawn(move || {
            pruner_for_shutdown.shutdown();
            completed_flag.store(true, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(50));
        assert!(
            !completed.load(Ordering::SeqCst),
            "shutdown should wait for worker completion"
        );

        tx.send(()).expect("release worker");
        handle.join().expect("shutdown join succeeds");
    }

    #[test]
    fn prune_now_overlapping_shutdown_is_serialized() {
        let journal = Arc::new(MockJournal::new(Vec::new()));
        let metadata = Arc::new(MockMetadata::default());
        let pruner = JournalPruner::spawn(
            Arc::clone(&journal),
            Arc::clone(&metadata),
            1,
            Duration::from_millis(200),
            || 100,
            None,
        );

        let manual_guard = pruner.inner.run_lock.lock();

        let worker_acquired = Arc::new(AtomicBool::new(false));
        let (worker_release_tx, worker_release_rx) = channel();
        let inner_for_worker = Arc::clone(&pruner.inner);
        let acquired_flag = Arc::clone(&worker_acquired);
        *pruner.inner.worker.lock() = Some(thread::spawn(move || {
            let _guard = inner_for_worker.run_lock.lock();
            acquired_flag.store(true, Ordering::SeqCst);
            let _ = worker_release_rx.recv();
        }));

        let shutdown_finished = Arc::new(AtomicBool::new(false));
        let shutdown_flag = Arc::clone(&shutdown_finished);
        let shutdown_pruner = pruner.clone();
        let shutdown_handle = thread::spawn(move || {
            shutdown_pruner.shutdown();
            shutdown_flag.store(true, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(50));
        assert!(
            !shutdown_finished.load(Ordering::SeqCst),
            "shutdown must wait while the run lock is held"
        );

        drop(manual_guard);

        assert!(
            wait_for(Duration::from_millis(200), || worker_acquired
                .load(Ordering::SeqCst)),
            "worker never acquired run lock"
        );
        worker_release_tx.send(()).expect("release worker lock");
        shutdown_handle.join().expect("shutdown join succeeds");
    }

    fn wait_for<F>(timeout: Duration, mut predicate: F) -> bool
    where
        F: FnMut() -> bool,
    {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return true;
            }
            thread::sleep(Duration::from_millis(5));
        }
        false
    }
}
