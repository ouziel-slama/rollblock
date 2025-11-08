use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::types::BlockId;

/// A snapshot of metrics at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub operations_applied: u64,
    pub set_operations_applied: u64,
    pub zero_value_deletes_applied: u64,
    pub blocks_committed: u64,
    pub rollbacks_executed: u64,
    pub lookups_performed: u64,
    pub avg_apply_time_us: u64,
    pub avg_rollback_time_us: u64,
    pub avg_lookup_time_us: u64,
    pub apply_p50_us: u64,
    pub apply_p95_us: u64,
    pub apply_p99_us: u64,
    pub rollback_p50_us: u64,
    pub rollback_p95_us: u64,
    pub rollback_p99_us: u64,
    pub current_block_height: BlockId,
    pub applied_block_height: BlockId,
    pub durable_block_height: BlockId,
    pub total_keys_stored: usize,
    pub failed_operations: u64,
    pub checksum_errors: u64,
    pub last_operation_secs: Option<u64>,
}

pub(crate) fn calculate_percentile(values: &VecDeque<u64>, percentile: u8) -> u64 {
    if values.is_empty() {
        return 0;
    }

    let mut sorted: Vec<_> = values.iter().copied().collect();
    sorted.sort_unstable();

    let index = ((percentile as f64 / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}
