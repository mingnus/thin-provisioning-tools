//! Device Mapper abstraction layer
//!
//! This module provides either the external devicemapper-rs implementation (default)
//! or an internal implementation when the 'internal-dm' feature is enabled.
//!
//! Usage:
//! - `cargo build` -> uses external devicemapper-rs
//! - `cargo build --features internal-dm` -> uses internal implementation (no external deps)
//!
//! When internal-dm is enabled, it takes priority over the external implementation.

// Re-export the appropriate implementation based on feature flags
// internal-dm takes priority when enabled
#[cfg(feature = "internal-dm")]
pub use devicemapper_internal::*;

#[cfg(not(feature = "internal-dm"))]
pub use devicemapper_external::*;
