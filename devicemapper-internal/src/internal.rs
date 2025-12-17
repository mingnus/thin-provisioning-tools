//! Internal Device Mapper implementation
//!
//! This module provides internal implementations of the Device Mapper types
//! and operations, compatible with the devicemapper-rs API surface.

use anyhow::{anyhow, Result};
use std::{fmt, fs::File, mem::size_of, os::unix::io::AsRawFd};

use super::ioctl;

use ioctl::*;

// Type aliases and compatible structures
pub type DmNameBuf = String;
pub type DmName = str;

/// Device mapper device name (max 127 chars + null terminator)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DmNameType(String);

impl DmNameType {
    /// Create a new device name
    pub fn new(name: &str) -> Result<Self, String> {
        if name.is_empty() || name.len() > 127 {
            return Err("Name must be 1-127 characters".to_string());
        }
        if name.contains('\0') {
            return Err("Name cannot contain null bytes".to_string());
        }
        Ok(DmNameType(name.to_string()))
    }

    /// Get the name as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Get the name as bytes
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for DmNameType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Device mapper UUID (max 128 chars + null terminator)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DmUuid(String);

impl DmUuid {
    /// Create a new UUID
    pub fn new(uuid: &str) -> Result<Self, String> {
        if uuid.len() > 128 {
            return Err("UUID must be max 128 characters".to_string());
        }
        if uuid.contains('\0') {
            return Err("UUID cannot contain null bytes".to_string());
        }
        Ok(DmUuid(uuid.to_string()))
    }

    /// Get the UUID as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Get the UUID as bytes
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for DmUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Device {
    pub major: u32,
    pub minor: u32,
}

impl Device {
    pub fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }
}

/// Information about a device mapper device
#[derive(Debug)]
pub struct DeviceInfo {
    device: Device,
    name: Option<String>,
    uuid: Option<String>,
    open_count: i32,
    target_count: u32,
    flags: DmFlags,
    dev: u64,
}

impl DeviceInfo {
    /// Create new device info
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        name: String,
        uuid: Option<String>,
        major: u32,
        minor: u32,
        open_count: i32,
        target_count: u32,
        flags: DmFlags,
        dev: u64,
    ) -> Self {
        Self {
            device: Device::new(major, minor),
            name: Some(name),
            uuid,
            open_count,
            target_count,
            flags,
            dev,
        }
    }

    pub fn device(&self) -> Device {
        self.device
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn uuid(&self) -> Option<&str> {
        self.uuid.as_deref()
    }

    pub fn open_count(&self) -> i32 {
        self.open_count
    }

    pub fn target_count(&self) -> u32 {
        self.target_count
    }

    pub fn event_nr(&self) -> u32 {
        0 // Not used in the ioctl implementation, but required for compatibility
    }

    pub fn flags(&self) -> DmFlags {
        self.flags
    }

    /// Get device number
    pub fn dev(&self) -> u64 {
        self.dev
    }
}

/// A device mapper device (simplified version)
#[derive(Debug, Clone)]
pub struct DeviceEntry {
    name: String,
    uuid: Option<String>,
    major: u32,
    minor: u32,
}

impl DeviceEntry {
    /// Create new device
    pub(crate) fn new(name: String, uuid: Option<String>, major: u32, minor: u32) -> Self {
        Self {
            name,
            uuid,
            major,
            minor,
        }
    }

    /// Get device name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get device UUID
    pub fn uuid(&self) -> Option<&str> {
        self.uuid.as_deref()
    }

    /// Get device major number
    pub fn major(&self) -> u32 {
        self.major
    }

    /// Get device minor number
    pub fn minor(&self) -> u32 {
        self.minor
    }
}

/// Target status information
#[derive(Debug, Clone)]
pub struct TargetStatus {
    /// Start sector
    pub start: u64,
    /// Length in sectors
    pub length: u64,
    /// Target type
    pub target_type: String,
    /// Target parameters
    pub params: String,
}

#[derive(Debug, Clone)]
pub enum DevId<'a> {
    Name(&'a DmName),
    Uuid(&'a str),
    Device(Device),
}

bitflags::bitflags! {
    /// Device mapper ioctl flags
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct DmFlags: u32 {
        /// Device is suspended
        const DM_SUSPEND = 0x01;
        /// Device is read-only
        const DM_READONLY = 0x02;
        /// Device has active table
        const DM_ACTIVE_PRESENT = 0x04;
        /// Device has inactive table
        const DM_INACTIVE_PRESENT = 0x08;
        /// Return table status instead of device status
        const DM_STATUS_TABLE_FLAG = 0x10;
        const DM_STATUS_TABLE = 0x10;  // Alias for compatibility
        /// Device has deferred remove
        const DM_DEFERRED_REMOVE = 0x20;
        /// Device is internally suspended
        const DM_INTERNAL_SUSPEND = 0x40;
        /// Buffer was too small for response
        const DM_BUFFER_FULL_FLAG = 0x100;
        /// Don't flush
        const DM_NOFLUSH_FLAG = 0x800;
        /// Query inactive table
        const DM_QUERY_INACTIVE_TABLE_FLAG = 0x1000;
        /// Identify device by UUID instead of name
        const DM_UUID_FLAG = 0x4000;
    }
}

impl Default for DmFlags {
    fn default() -> Self {
        DmFlags::empty()
    }
}

/// Device mapper options for ioctl operations
#[derive(Debug, Default)]
pub struct DmOptions {
    flags: DmFlags,
    udev_flags: u32,
}

impl DmOptions {
    /// Create new options with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set flags
    pub fn set_flags(mut self, flags: DmFlags) -> Self {
        self.flags = flags;
        self
    }

    /// Get flags
    pub fn flags(&self) -> DmFlags {
        self.flags
    }

    /// Set udev flags
    pub fn set_udev_flags(mut self, flags: u32) -> Self {
        self.udev_flags = flags;
        self
    }

    /// Get udev flags
    pub fn udev_flags(&self) -> u32 {
        self.udev_flags
    }
}

/// Internal Device Mapper implementation
pub struct DM {
    file: File,
}

impl DM {
    /// Create a new DM instance
    pub fn new() -> Result<Self> {
        let file = File::open("/dev/mapper/control")
            .map_err(|e| anyhow!("Failed to open /dev/mapper/control: {}", e))?;
        Ok(DM { file })
    }

    /// List all device mapper devices
    ///
    /// Returns a vector of (name, device, info) tuples for compatibility with devicemapper-rs
    pub fn list_devices(&mut self) -> Result<Vec<(DmNameBuf, Device, DeviceInfo)>> {
        let devices = self.list_devices_raw()?;
        let mut result = Vec::new();

        for device in devices {
            let device_struct = Device::new(device.major(), device.minor());
            let device_info = DeviceInfo::new(
                device.name().to_string(),
                device.uuid().map(|s| s.to_string()),
                device.major(),
                device.minor(),
                0, // open_count not available from list_devices
                0, // target_count not available from list_devices
                DmFlags::empty(),
                ((device.major() as u64) << 8) | (device.minor() as u64),
            );
            result.push((device.name().to_string(), device_struct, device_info));
        }

        Ok(result)
    }

    /// List all device mapper devices (raw implementation)
    fn list_devices_raw(&self) -> Result<Vec<DeviceEntry>> {
        let mut hdr = DmIoctl::new();

        // Start with a reasonable buffer size
        let mut buf_size = 4096;
        loop {
            hdr.data_size = buf_size;

            // Allocate buffer
            let mut buffer = vec![0u8; buf_size as usize];

            // Copy header to start of buffer
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &hdr as *const DmIoctl as *const u8,
                    buffer.as_mut_ptr(),
                    size_of::<DmIoctl>(),
                );
            }

            // Perform ioctl
            let result = unsafe {
                dm_ioctl(
                    self.file.as_raw_fd(),
                    DM_LIST_DEVICES_CMD,
                    buffer.as_mut_ptr() as *mut DmIoctl,
                )
            };

            match result {
                Ok(()) => {
                    // Read back the header to check if buffer was large enough
                    let hdr_out: DmIoctl =
                        unsafe { std::ptr::read(buffer.as_ptr() as *const DmIoctl) };

                    if (hdr_out.flags & DmFlags::DM_BUFFER_FULL_FLAG.bits()) != 0 {
                        // Buffer was too small, try again with larger buffer
                        buf_size *= 2;
                        continue;
                    }

                    // Parse the device list
                    return self.parse_device_list(&buffer[hdr_out.data_start as usize..]);
                }
                Err(errno) => return Err(anyhow!("Device mapper ioctl failed: {}", errno)),
            }
        }
    }

    // FIXME: avoid returning anyhow::Result to suppress warnings
    /// Get device information
    pub fn device_info(&mut self, dev: &DevId) -> Result<DeviceInfo> {
        let mut hdr = DmIoctl::new();

        // Set device identifier
        match dev {
            DevId::Name(name) => {
                hdr.set_name(name)
                    .map_err(|e| anyhow!("Invalid device name: {}", e))?;
            }
            DevId::Uuid(uuid) => {
                hdr.set_uuid(uuid)
                    .map_err(|e| anyhow!("Invalid UUID: {}", e))?;
                hdr.flags |= DmFlags::DM_UUID_FLAG.bits();
            }
            DevId::Device(_) => {
                return Err(anyhow!("DevId::Device not supported in device_info"));
            }
        }

        // Perform ioctl
        let result = unsafe {
            dm_ioctl(
                self.file.as_raw_fd(),
                DM_DEV_STATUS_CMD,
                &mut hdr as *mut DmIoctl,
            )
        };

        match result {
            Ok(()) => self.parse_device_info(&hdr),
            Err(errno) => Err(anyhow!("Device info ioctl failed: {}", errno)),
        }
    }

    /// Get table status for a device
    #[allow(clippy::type_complexity)]
    pub fn table_status(
        &mut self,
        dev: &DevId,
        options: DmOptions,
    ) -> Result<(DeviceInfo, Vec<(u64, u64, String, String)>)> {
        let mut hdr = DmIoctl::new();

        // Set flags from options
        hdr.flags = options.flags().bits() | DmFlags::DM_STATUS_TABLE_FLAG.bits();
        hdr.event_nr = options.udev_flags() << DM_UDEV_FLAGS_SHIFT;

        // Set device identifier
        match dev {
            DevId::Name(name) => {
                hdr.set_name(name)
                    .map_err(|e| anyhow!("Invalid device name: {}", e))?;
            }
            DevId::Uuid(uuid) => {
                hdr.set_uuid(uuid)
                    .map_err(|e| anyhow!("Invalid UUID: {}", e))?;
                hdr.flags |= DmFlags::DM_UUID_FLAG.bits();
            }
            DevId::Device(_) => {
                return Err(anyhow!("DevId::Device not supported in table_status"));
            }
        }

        // Start with a reasonable buffer size
        let mut buf_size = 4096;
        loop {
            hdr.data_size = buf_size;

            // Allocate buffer
            let mut buffer = vec![0u8; buf_size as usize];

            // Copy header to start of buffer
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &hdr as *const DmIoctl as *const u8,
                    buffer.as_mut_ptr(),
                    size_of::<DmIoctl>(),
                );
            }

            // Perform ioctl
            let result = unsafe {
                dm_ioctl(
                    self.file.as_raw_fd(),
                    DM_TABLE_STATUS_CMD,
                    buffer.as_mut_ptr() as *mut DmIoctl,
                )
            };

            match result {
                Ok(()) => {
                    // Read back the header
                    let hdr_out: DmIoctl =
                        unsafe { std::ptr::read(buffer.as_ptr() as *const DmIoctl) };

                    if (hdr_out.flags & DmFlags::DM_BUFFER_FULL_FLAG.bits()) != 0 {
                        // Buffer was too small, try again with larger buffer
                        buf_size *= 2;
                        continue;
                    }

                    // Parse device info and table status
                    let device_info = self.parse_device_info(&hdr_out)?;
                    let table_status = self.parse_table_status(
                        hdr_out.target_count,
                        &buffer[hdr_out.data_start as usize..],
                    )?;

                    // Convert TargetStatus to tuple format expected by devicemapper-rs API
                    let table_tuples: Vec<(u64, u64, String, String)> = table_status
                        .into_iter()
                        .map(|ts| (ts.start, ts.length, ts.target_type, ts.params))
                        .collect();

                    return Ok((device_info, table_tuples));
                }
                Err(errno) => return Err(anyhow!("Table status ioctl failed: {}", errno)),
            }
        }
    }

    /// Parse device list from ioctl response
    fn parse_device_list(&self, data: &[u8]) -> Result<Vec<DeviceEntry>> {
        let mut devices = Vec::new();
        let mut offset = 0;

        while offset < data.len() {
            if offset + size_of::<DmNameList>() > data.len() {
                break; // Not enough data for another entry
            }

            // Read the name list entry
            let name_list: DmNameList =
                unsafe { std::ptr::read(data[offset..].as_ptr() as *const DmNameList) };

            // Parse the device name
            let name_start = offset + size_of::<DmNameList>();
            if name_start >= data.len() {
                break;
            }

            let name_data = &data[name_start..];
            let name = parse_cstring(name_data)
                .map_err(|e| anyhow!("Failed to parse device name: {}", e))?;

            if !name.is_empty() {
                // Extract major/minor from dev
                let major = (name_list.dev >> 8) as u32;
                let minor = (name_list.dev & 0xff) as u32;

                devices.push(DeviceEntry::new(name, None, major, minor));
            }

            // Move to next entry
            if name_list.next == 0 {
                break; // Last entry
            }
            offset = name_list.next as usize;
        }

        Ok(devices)
    }

    /// Parse device info from ioctl header
    fn parse_device_info(&self, hdr: &DmIoctl) -> Result<DeviceInfo> {
        let name = hdr
            .get_name()
            .map_err(|e| anyhow!("Failed to parse device name: {}", e))?;

        let uuid = match hdr.get_uuid() {
            Ok(Some(uuid_str)) => Some(uuid_str),
            Ok(None) => None,
            Err(e) => return Err(anyhow!("Failed to parse UUID: {}", e)),
        };

        // Extract major/minor from dev
        let major = (hdr.dev >> 8) as u32;
        let minor = (hdr.dev & 0xff) as u32;

        let flags = DmFlags::from_bits_truncate(hdr.flags);

        Ok(DeviceInfo::new(
            name,
            uuid,
            major,
            minor,
            hdr.open_count,
            hdr.target_count,
            flags,
            hdr.dev,
        ))
    }

    /// Parse table status from ioctl response data
    fn parse_table_status(&self, target_count: u32, data: &[u8]) -> Result<Vec<TargetStatus>> {
        let mut targets = Vec::new();
        let mut offset = 0;

        for _ in 0..target_count {
            if offset >= data.len() {
                break;
            }

            let (spec, next_offset) = parse_target_spec(data, offset)
                .map_err(|e| anyhow!("Failed to parse target spec: {}", e))?;

            // Parse target type
            let target_type = unsafe {
                let type_ptr = spec.target_type.as_ptr();
                let type_cstr = std::ffi::CStr::from_ptr(type_ptr);
                type_cstr
                    .to_str()
                    .map_err(|_| anyhow!("Invalid UTF-8 in target type"))?
                    .to_string()
            };

            // Parse parameters (everything after the target spec)
            let params_start = offset + size_of::<DmTargetSpec>();
            let params_end = if next_offset > data.len() {
                data.len()
            } else {
                next_offset
            };

            let params = if params_start < params_end {
                let params_data = &data[params_start..params_end];
                parse_cstring(params_data).unwrap_or_else(|_| String::new())
            } else {
                String::new()
            };

            targets.push(TargetStatus {
                start: spec.sector_start,
                length: spec.length,
                target_type,
                params,
            });

            offset = next_offset;
            if offset >= data.len() {
                break;
            }
        }

        Ok(targets)
    }
}
