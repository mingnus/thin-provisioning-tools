pub mod internal;
pub mod ioctl;
pub mod ioctl_base;

// Re-export the main types for compatibility with devicemapper-rs API
pub use internal::{
    DevId, Device, DeviceEntry, DeviceInfo, DmFlags, DmName, DmNameBuf, DmNameType, DmOptions,
    DmUuid, TargetStatus, DM,
};
