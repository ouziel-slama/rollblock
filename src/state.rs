//! State management primitives (shards, engines, planners).

pub mod engine;
pub mod shard;

pub mod prelude {
    pub use super::engine::{ShardedStateEngine, StateEngine};
    pub use super::shard::{RawTableShard, StateShard};
}
