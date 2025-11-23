//! Public API surface for Rollblock consumers.
//!
//! This module groups lightweight types, error definitions, and facade traits so
//! downstream crates can interact with the store without pulling in the heavy
//! runtime/state/storage implementation details.

pub mod error;
pub mod facade;
pub mod types;

pub mod prelude {
    pub use super::error::{MhinStoreError, StoreResult};
    pub use super::facade::{MhinStoreBlockFacade, MhinStoreFacade, StoreConfig, StoreFacade};
    pub use super::types::*;
}
