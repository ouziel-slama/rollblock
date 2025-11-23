//! Runtime protocols: orchestration, durability and metrics collection.

pub mod metrics;
pub mod orchestrator;

pub mod prelude {
    pub use super::metrics::{HealthState, HealthStatus, MetricsSnapshot, StoreMetrics};
    pub use super::orchestrator::{
        BlockOrchestrator, DefaultBlockOrchestrator, DurabilityMode, PersistenceSettings,
    };
}
