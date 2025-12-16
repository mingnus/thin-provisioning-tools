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

#[cfg(feature = "internal-dm")]
pub mod ioctl;

#[cfg(feature = "internal-dm")]
pub mod internal;

#[cfg(not(feature = "internal-dm"))]
pub mod external;

// Re-export the appropriate implementation based on feature flags
// internal-dm takes priority when enabled
#[cfg(feature = "internal-dm")]
pub use internal::*;

#[cfg(not(feature = "internal-dm"))]
pub use external::*;
