use serde::{Deserialize, Serialize};

use crate::types::BlockId;

use super::MetricsSnapshot;

/// Health status of the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub state: HealthState,
    pub current_block: BlockId,
    pub durable_block: BlockId,
    pub total_operations: u64,
    pub failed_operations: u64,
    pub checksum_errors: u64,
    pub last_operation_secs: Option<u64>,
}

/// Health state enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthState {
    Healthy,
    Idle,
    Degraded,
    Unhealthy,
}

impl std::fmt::Display for HealthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthState::Healthy => write!(f, "HEALTHY"),
            HealthState::Idle => write!(f, "IDLE"),
            HealthState::Degraded => write!(f, "DEGRADED"),
            HealthState::Unhealthy => write!(f, "UNHEALTHY"),
        }
    }
}

pub(crate) fn derive_health(snapshot: &MetricsSnapshot) -> HealthStatus {
    let is_healthy = snapshot.failed_operations == 0 && snapshot.checksum_errors == 0;
    let has_activity = snapshot.last_operation_secs.is_some();
    let is_responsive = snapshot
        .last_operation_secs
        .map(|secs| secs < 60)
        .unwrap_or(false);

    let state = if !is_healthy && snapshot.checksum_errors > 0 {
        HealthState::Degraded
    } else if !is_healthy {
        HealthState::Unhealthy
    } else if !has_activity {
        HealthState::Idle
    } else if is_responsive {
        HealthState::Healthy
    } else {
        HealthState::Idle
    };

    HealthStatus {
        state,
        current_block: snapshot.applied_block_height,
        durable_block: snapshot.durable_block_height,
        total_operations: snapshot.operations_applied,
        failed_operations: snapshot.failed_operations,
        checksum_errors: snapshot.checksum_errors,
        last_operation_secs: snapshot.last_operation_secs,
    }
}
