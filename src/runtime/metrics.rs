//! Metrics and observability for the store.
//!
//! This module provides runtime metrics and health information about the store's
//! operations, performance, and state.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::types::BlockId;

pub mod health;
pub mod snapshot;

pub use health::{HealthState, HealthStatus};
pub use snapshot::MetricsSnapshot;

/// Store-wide metrics collected at runtime.
///
/// Metrics are thread-safe and can be accessed concurrently.
/// All counters are monotonically increasing.
#[derive(Debug, Clone)]
pub struct StoreMetrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug)]
struct MetricsInner {
    operations_applied: AtomicU64,
    set_operations_applied: AtomicU64,
    zero_value_deletes_applied: AtomicU64,
    blocks_committed: AtomicU64,
    rollbacks_executed: AtomicU64,
    lookups_performed: AtomicU64,
    total_apply_time_us: AtomicU64,
    total_rollback_time_us: AtomicU64,
    total_lookup_time_us: AtomicU64,
    applied_block_height: AtomicU64,
    durable_block_height: AtomicU64,
    total_keys_stored: AtomicUsize,
    failed_operations: AtomicU64,
    checksum_errors: AtomicU64,
    recent_operations: RwLock<RecentOperations>,
}

#[derive(Debug, Default)]
struct RecentOperations {
    last_100_apply_times_us: VecDeque<u64>,
    last_100_rollback_times_us: VecDeque<u64>,
    last_operation_timestamp: Option<Instant>,
}

impl StoreMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                operations_applied: AtomicU64::new(0),
                set_operations_applied: AtomicU64::new(0),
                zero_value_deletes_applied: AtomicU64::new(0),
                blocks_committed: AtomicU64::new(0),
                rollbacks_executed: AtomicU64::new(0),
                lookups_performed: AtomicU64::new(0),
                total_apply_time_us: AtomicU64::new(0),
                total_rollback_time_us: AtomicU64::new(0),
                total_lookup_time_us: AtomicU64::new(0),
                applied_block_height: AtomicU64::new(0),
                durable_block_height: AtomicU64::new(0),
                total_keys_stored: AtomicUsize::new(0),
                failed_operations: AtomicU64::new(0),
                checksum_errors: AtomicU64::new(0),
                recent_operations: RwLock::new(RecentOperations::default()),
            }),
        }
    }

    pub fn record_apply(
        &self,
        block_height: BlockId,
        ops_count: usize,
        set_count: usize,
        zero_delete_count: usize,
        duration: Duration,
    ) {
        self.inner
            .operations_applied
            .fetch_add(ops_count as u64, Ordering::Relaxed);
        self.inner
            .set_operations_applied
            .fetch_add(set_count as u64, Ordering::Relaxed);
        self.inner
            .zero_value_deletes_applied
            .fetch_add(zero_delete_count as u64, Ordering::Relaxed);
        self.inner.blocks_committed.fetch_add(1, Ordering::Relaxed);

        let duration_us = duration.as_micros() as u64;
        self.inner
            .total_apply_time_us
            .fetch_add(duration_us, Ordering::Relaxed);

        self.inner
            .applied_block_height
            .store(block_height, Ordering::Relaxed);

        let mut recent = self.inner.recent_operations.write();
        recent.last_operation_timestamp = Some(Instant::now());
        if recent.last_100_apply_times_us.len() >= 100 {
            recent.last_100_apply_times_us.pop_front();
        }
        recent.last_100_apply_times_us.push_back(duration_us);
    }

    pub fn record_rollback(&self, target_block: BlockId, duration: Duration) {
        self.inner
            .rollbacks_executed
            .fetch_add(1, Ordering::Relaxed);

        let duration_us = duration.as_micros() as u64;
        self.inner
            .total_rollback_time_us
            .fetch_add(duration_us, Ordering::Relaxed);

        self.inner
            .applied_block_height
            .store(target_block, Ordering::Relaxed);
        self.inner
            .durable_block_height
            .store(target_block, Ordering::Relaxed);

        let mut recent = self.inner.recent_operations.write();
        recent.last_operation_timestamp = Some(Instant::now());
        if recent.last_100_rollback_times_us.len() >= 100 {
            recent.last_100_rollback_times_us.pop_front();
        }
        recent.last_100_rollback_times_us.push_back(duration_us);
    }

    pub fn record_lookup(&self, duration: Duration) {
        self.record_lookup_batch(1, duration);
    }

    pub fn record_lookup_batch(&self, key_count: usize, duration: Duration) {
        if key_count == 0 {
            return;
        }

        self.inner
            .lookups_performed
            .fetch_add(key_count as u64, Ordering::Relaxed);

        let duration_us = duration.as_micros() as u64;
        self.inner
            .total_lookup_time_us
            .fetch_add(duration_us, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.inner.failed_operations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_checksum_error(&self) {
        self.inner.checksum_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_durable_block(&self, block_height: BlockId) {
        self.inner
            .durable_block_height
            .store(block_height, Ordering::Relaxed);
    }

    pub fn update_applied_block(&self, block_height: BlockId) {
        self.inner
            .applied_block_height
            .store(block_height, Ordering::Relaxed);
    }

    pub fn update_key_count(&self, count: usize) {
        self.inner.total_keys_stored.store(count, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let operations_applied = self.inner.operations_applied.load(Ordering::Relaxed);
        let set_operations_applied = self.inner.set_operations_applied.load(Ordering::Relaxed);
        let zero_value_deletes_applied = self
            .inner
            .zero_value_deletes_applied
            .load(Ordering::Relaxed);
        let blocks_committed = self.inner.blocks_committed.load(Ordering::Relaxed);
        let rollbacks_executed = self.inner.rollbacks_executed.load(Ordering::Relaxed);
        let lookups_performed = self.inner.lookups_performed.load(Ordering::Relaxed);

        let total_apply_time_us = self.inner.total_apply_time_us.load(Ordering::Relaxed);
        let total_rollback_time_us = self.inner.total_rollback_time_us.load(Ordering::Relaxed);
        let total_lookup_time_us = self.inner.total_lookup_time_us.load(Ordering::Relaxed);

        let applied_block_height = self.inner.applied_block_height.load(Ordering::Relaxed);
        let durable_block_height = self.inner.durable_block_height.load(Ordering::Relaxed);
        let total_keys_stored = self.inner.total_keys_stored.load(Ordering::Relaxed);

        let failed_operations = self.inner.failed_operations.load(Ordering::Relaxed);
        let checksum_errors = self.inner.checksum_errors.load(Ordering::Relaxed);

        let avg_apply_time_us = if blocks_committed > 0 {
            total_apply_time_us / blocks_committed
        } else {
            0
        };

        let avg_rollback_time_us = if rollbacks_executed > 0 {
            total_rollback_time_us / rollbacks_executed
        } else {
            0
        };

        let avg_lookup_time_us = if lookups_performed > 0 {
            total_lookup_time_us / lookups_performed
        } else {
            0
        };

        let recent = self.inner.recent_operations.read();
        let apply_p50 = snapshot::calculate_percentile(&recent.last_100_apply_times_us, 50);
        let apply_p95 = snapshot::calculate_percentile(&recent.last_100_apply_times_us, 95);
        let apply_p99 = snapshot::calculate_percentile(&recent.last_100_apply_times_us, 99);

        let rollback_p50 = snapshot::calculate_percentile(&recent.last_100_rollback_times_us, 50);
        let rollback_p95 = snapshot::calculate_percentile(&recent.last_100_rollback_times_us, 95);
        let rollback_p99 = snapshot::calculate_percentile(&recent.last_100_rollback_times_us, 99);

        let last_operation_secs = recent
            .last_operation_timestamp
            .map(|t| t.elapsed().as_secs());

        drop(recent);

        MetricsSnapshot {
            operations_applied,
            set_operations_applied,
            zero_value_deletes_applied,
            blocks_committed,
            rollbacks_executed,
            lookups_performed,
            avg_apply_time_us,
            avg_rollback_time_us,
            avg_lookup_time_us,
            apply_p50_us: apply_p50,
            apply_p95_us: apply_p95,
            apply_p99_us: apply_p99,
            rollback_p50_us: rollback_p50,
            rollback_p95_us: rollback_p95,
            rollback_p99_us: rollback_p99,
            current_block_height: applied_block_height,
            applied_block_height,
            durable_block_height,
            total_keys_stored,
            failed_operations,
            checksum_errors,
            last_operation_secs,
        }
    }

    pub fn health(&self) -> HealthStatus {
        let snapshot = self.snapshot();
        health::derive_health(&snapshot)
    }
}

impl Default for StoreMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::metrics::snapshot;

    #[test]
    fn metrics_new_has_zero_values() {
        let metrics = StoreMetrics::new();
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.operations_applied, 0);
        assert_eq!(snapshot.set_operations_applied, 0);
        assert_eq!(snapshot.zero_value_deletes_applied, 0);
        assert_eq!(snapshot.blocks_committed, 0);
        assert_eq!(snapshot.rollbacks_executed, 0);
        assert_eq!(snapshot.lookups_performed, 0);
        assert_eq!(snapshot.current_block_height, 0);
        assert_eq!(snapshot.applied_block_height, 0);
        assert_eq!(snapshot.durable_block_height, 0);
        assert_eq!(snapshot.failed_operations, 0);
    }

    #[test]
    fn record_apply_updates_metrics() {
        let metrics = StoreMetrics::new();
        metrics.record_apply(1, 5, 4, 1, Duration::from_millis(10));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.operations_applied, 5);
        assert_eq!(snapshot.set_operations_applied, 4);
        assert_eq!(snapshot.zero_value_deletes_applied, 1);
        assert_eq!(snapshot.blocks_committed, 1);
        assert_eq!(snapshot.current_block_height, 1);
        assert_eq!(snapshot.applied_block_height, 1);
        assert_eq!(snapshot.durable_block_height, 0);
        assert!(snapshot.avg_apply_time_us > 0);
    }

    #[test]
    fn record_rollback_updates_metrics() {
        let metrics = StoreMetrics::new();
        metrics.record_rollback(5, Duration::from_millis(20));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rollbacks_executed, 1);
        assert_eq!(snapshot.current_block_height, 5);
        assert_eq!(snapshot.applied_block_height, 5);
        assert_eq!(snapshot.durable_block_height, 5);
        assert!(snapshot.avg_rollback_time_us > 0);
    }

    #[test]
    fn health_reports_idle_initially() {
        let metrics = StoreMetrics::new();
        let health = metrics.health();

        assert_eq!(health.state, HealthState::Idle);
        assert_eq!(health.failed_operations, 0);
        assert_eq!(health.checksum_errors, 0);
        assert_eq!(health.current_block, 0);
        assert_eq!(health.durable_block, 0);
    }

    #[test]
    fn health_reports_healthy_after_operation() {
        let metrics = StoreMetrics::new();
        metrics.record_apply(1, 5, 4, 1, Duration::from_millis(10));

        let health = metrics.health();
        assert_eq!(health.state, HealthState::Healthy);
        assert_eq!(health.failed_operations, 0);
        assert_eq!(health.checksum_errors, 0);
        assert_eq!(health.durable_block, 0);
    }

    #[test]
    fn health_reports_unhealthy_after_checksum_error() {
        let metrics = StoreMetrics::new();
        metrics.record_checksum_error();

        let health = metrics.health();
        assert_eq!(health.state, HealthState::Degraded);
        assert_eq!(health.checksum_errors, 1);
        assert_eq!(health.durable_block, 0);
    }

    #[test]
    fn percentile_calculation() {
        use std::collections::VecDeque;

        let values: VecDeque<u64> = vec![1, 2, 3, 4, 5].into();
        assert_eq!(snapshot::calculate_percentile(&values, 50), 3);

        let values: VecDeque<u64> = vec![1, 2, 3, 4, 5].into();
        assert_eq!(snapshot::calculate_percentile(&values, 95), 5);

        let values: VecDeque<u64> = vec![1].into();
        assert_eq!(snapshot::calculate_percentile(&values, 50), 1);

        let values: VecDeque<u64> = VecDeque::new();
        assert_eq!(snapshot::calculate_percentile(&values, 50), 0);
    }

    #[test]
    fn recent_operations_tracks_last_100() {
        let metrics = StoreMetrics::new();

        // Record 150 operations
        for i in 0..150 {
            metrics.record_apply(i, 1, 1, 0, Duration::from_micros(i));
        }

        let recent = metrics.inner.recent_operations.read();
        assert_eq!(recent.last_100_apply_times_us.len(), 100);
        // Should have the last 100 (50-149)
        assert_eq!(recent.last_100_apply_times_us[0], 50);
        assert_eq!(recent.last_100_apply_times_us[99], 149);
    }
}
