//! External devicemapper-rs wrapper
//!
//! This module re-exports devicemapper-rs types and provides any necessary
//! compatibility wrappers for the existing API.
//!
//! This module is only compiled when internal-dm feature is NOT enabled.

// Compile-time check: this module should not be compiled when internal-dm is active
#[cfg(feature = "internal-dm")]
compile_error!("External devicemapper module should not be compiled when internal-dm is enabled");

// Re-export all devicemapper types that are currently used
pub use devicemapper::{DevId, Device, DeviceInfo, DmFlags, DmName, DmNameBuf, DmOptions, DM};
