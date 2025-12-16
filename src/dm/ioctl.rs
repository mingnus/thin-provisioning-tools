//! Device mapper ioctl bindings
//!
//! This module provides low-level ioctl interface for communicating with the
//! Linux device mapper kernel subsystem.

use std::{
    ffi::CStr,
    mem::{size_of, zeroed},
    os::raw::{c_char, c_int},
    ptr,
};

use crate::{ioctl::*, request_code_readwrite};
use nix::libc;

/// Device mapper ioctl structure
#[repr(C)]
pub struct DmIoctl {
    /// Interface version [major, minor, patchlevel]
    pub version: [u32; 3],
    /// Size of data including this header
    pub data_size: u32,
    /// Offset to start of data after header
    pub data_start: u32,
    /// Number of target specs following
    pub target_count: u32,
    /// Number of times opened
    pub open_count: i32,
    /// Event flags
    pub flags: u32,
    /// Event number
    pub event_nr: u32,
    /// Padding
    pub padding: u32,
    /// Device number
    pub dev: u64,
    /// Device name (128 bytes)
    pub name: [c_char; 128],
    /// Device UUID (129 bytes)
    pub uuid: [c_char; 129],
    /// Start of target data
    pub data: [c_char; 7],
}

/// Device mapper name list structure
#[repr(C)]
pub struct DmNameList {
    pub dev: u64,
    pub next: u32,
    // Name follows this structure
}

/// Device mapper target specification
#[repr(C)]
pub struct DmTargetSpec {
    pub sector_start: u64,
    pub length: u64,
    pub next: u32,
    pub status: i32,
    pub target_type: [c_char; 16],
}

// Device mapper ioctl commands
pub const DM_LIST_DEVICES_CMD: u8 = 2;
pub const DM_DEV_STATUS_CMD: u8 = 7;
pub const DM_TABLE_STATUS_CMD: u8 = 12;

// Device mapper version
const DM_VERSION_MAJOR: u32 = 4;
const DM_VERSION_MINOR: u32 = 49;
const DM_VERSION_PATCHLEVEL: u32 = 0;

// Device mapper magic number
const DM_IOCTL: u32 = 0xfd;

// udev shift for event_nr
pub const DM_UDEV_FLAGS_SHIFT: u32 = 16;

impl Default for DmIoctl {
    fn default() -> Self {
        unsafe { zeroed() }
    }
}

impl DmIoctl {
    /// Create a new ioctl header
    pub fn new() -> Self {
        DmIoctl {
            version: [DM_VERSION_MAJOR, DM_VERSION_MINOR, DM_VERSION_PATCHLEVEL],
            data_size: size_of::<DmIoctl>() as u32,
            data_start: size_of::<DmIoctl>() as u32,
            ..Default::default()
        }
    }

    /// Set device name
    pub fn set_name(&mut self, name: &str) -> Result<(), String> {
        if name.len() >= 128 {
            return Err("Name too long".to_string());
        }

        self.name.fill(0);
        let name_bytes = name.as_bytes();
        for (i, &byte) in name_bytes.iter().enumerate() {
            self.name[i] = byte as c_char;
        }
        Ok(())
    }

    /// Set device UUID
    pub fn set_uuid(&mut self, uuid: &str) -> Result<(), String> {
        if uuid.len() >= 129 {
            return Err("UUID too long".to_string());
        }

        self.uuid.fill(0);
        let uuid_bytes = uuid.as_bytes();
        for (i, &byte) in uuid_bytes.iter().enumerate() {
            self.uuid[i] = byte as c_char;
        }
        Ok(())
    }

    /// Get name as string
    pub fn get_name(&self) -> Result<String, String> {
        unsafe {
            let name_cstr = CStr::from_ptr(self.name.as_ptr());
            name_cstr
                .to_str()
                .map(|s| s.to_string())
                .map_err(|_| "Invalid UTF-8 in name".to_string())
        }
    }

    /// Get UUID as string
    pub fn get_uuid(&self) -> Result<Option<String>, String> {
        unsafe {
            if self.uuid[0] == 0 {
                return Ok(None);
            }
            let uuid_cstr = CStr::from_ptr(self.uuid.as_ptr());
            match uuid_cstr.to_str() {
                Ok(s) if !s.is_empty() => Ok(Some(s.to_string())),
                Ok(_) => Ok(None),
                Err(_) => Err("Invalid UTF-8 in UUID".to_string()),
            }
        }
    }
}

/// Perform device mapper ioctl
#[allow(clippy::missing_safety_doc)]
pub unsafe fn dm_ioctl(fd: c_int, cmd: u8, hdr: *mut DmIoctl) -> Result<(), nix::errno::Errno> {
    let request = request_code_readwrite!(DM_IOCTL, cmd, DmIoctl);
    let result = libc::ioctl(fd, request as libc::c_ulong, hdr);

    if result < 0 {
        Err(nix::errno::Errno::last())
    } else {
        Ok(())
    }
}

/// Parse C string from bytes
pub fn parse_cstring(bytes: &[u8]) -> Result<String, String> {
    let null_pos = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..null_pos])
        .map(|s| s.to_string())
        .map_err(|_| "Invalid UTF-8".to_string())
}

/// Parse target spec from data
pub fn parse_target_spec(data: &[u8], offset: usize) -> Result<(DmTargetSpec, usize), String> {
    if offset + size_of::<DmTargetSpec>() > data.len() {
        return Err("Not enough data for target spec".to_string());
    }

    let spec: DmTargetSpec = unsafe { ptr::read(data[offset..].as_ptr() as *const DmTargetSpec) };

    let next_offset = if spec.next == 0 {
        data.len()
    } else {
        offset + spec.next as usize
    };

    Ok((spec, next_offset))
}
