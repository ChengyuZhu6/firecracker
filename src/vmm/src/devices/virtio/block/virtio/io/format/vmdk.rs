// Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! VMDK disk image backend using the imago library.
//!
//! This module provides a synchronous block device engine for VMDK disk images,
//! backed by imago's `SyncFormatAccess`. VMDK images are read-only.

use std::fs::File;
use std::io;

use imago::SyncFormatAccess;
use imago::file::File as ImagoFile;
use imago::format::gate::PermissiveImplicitOpenGate;
use imago::vmdk::Vmdk;
use imago::FormatDriverBuilder;
use vm_memory::GuestMemoryError;

use crate::vstate::memory::{Bytes, GuestAddress, GuestMemory, GuestMemoryMmap};

/// Errors specific to the VMDK IO engine.
#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum VmdkIoError {
    /// Failed to open VMDK image: {0}
    Open(io::Error),
    /// VMDK read error: {0}
    Read(io::Error),
    /// VMDK write not supported (read-only image)
    WriteNotSupported,
    /// Guest memory error: {0}
    GuestMemory(GuestMemoryError),
    /// VMDK flush error: {0}
    Flush(io::Error),
}

/// A synchronous VMDK disk engine using imago's `SyncFormatAccess`.
///
/// This engine wraps imago's VMDK reader and provides a synchronous read interface
/// compatible with Firecracker's block device I/O path. VMDK images are always read-only.
pub struct VmdkFileEngine {
    access: SyncFormatAccess<ImagoFile>,
    disk_size: u64,
}

// SyncFormatAccess does not derive Debug, so we implement it manually.
impl std::fmt::Debug for VmdkFileEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VmdkFileEngine")
            .field("disk_size", &self.disk_size)
            .finish_non_exhaustive()
    }
}

// SAFETY: SyncFormatAccess wraps a tokio Runtime + FormatAccess which are Send.
// The underlying ImagoFile uses RwLock internally.
unsafe impl Send for VmdkFileEngine {}

impl VmdkFileEngine {
    /// Create a new `VmdkFileEngine` from a host file descriptor.
    ///
    /// The provided `File` should point to a VMDK descriptor file. The engine will
    /// parse the VMDK metadata and open any referenced extent files.
    pub fn from_file(file: File) -> Result<Self, VmdkIoError> {
        // Convert std::fs::File to imago's File type.
        let imago_file: ImagoFile = file.try_into().map_err(VmdkIoError::Open)?;

        // Open the VMDK image (read-only) using synchronous builder.
        let vmdk = Vmdk::<ImagoFile>::builder(imago_file)
            .write(false)
            .open_sync(PermissiveImplicitOpenGate())
            .map_err(VmdkIoError::Open)?;

        // Create a SyncFormatAccess wrapper.
        let access = SyncFormatAccess::new(vmdk).map_err(VmdkIoError::Open)?;

        // Get the virtual disk size.
        let disk_size = access.size();

        Ok(Self { access, disk_size })
    }

    /// Returns the virtual disk size in bytes.
    pub fn disk_size(&self) -> u64 {
        self.disk_size
    }

    /// Read data from the VMDK image at the given byte offset into guest memory.
    pub fn read(
        &self,
        offset: u64,
        mem: &GuestMemoryMmap,
        addr: GuestAddress,
        count: u32,
    ) -> Result<u32, VmdkIoError> {
        let count_usize = count as usize;
        let mut buf = vec![0u8; count_usize];

        // Read from the VMDK image into our buffer.
        self.access
            .read(&mut buf[..], offset)
            .map_err(VmdkIoError::Read)?;

        // Write the data from our buffer into guest memory.
        mem.write_slice(&buf, addr)
            .map_err(VmdkIoError::GuestMemory)?;

        Ok(count)
    }

    /// Writing to VMDK is not supported (read-only format).
    pub fn write(
        &self,
        _offset: u64,
        _mem: &GuestMemoryMmap,
        _addr: GuestAddress,
        _count: u32,
    ) -> Result<u32, VmdkIoError> {
        Err(VmdkIoError::WriteNotSupported)
    }

    /// Flush the VMDK engine (no-op for read-only images).
    pub fn flush(&self) -> Result<(), VmdkIoError> {
        self.access.flush().map_err(VmdkIoError::Flush)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use vmm_sys_util::tempfile::TempFile;

    use super::super::{DiskImageFormat, VMDK4_MAGIC, detect_disk_format};
    use super::*;

    /// Create a minimal VMDK descriptor file that references a flat extent.
    /// Returns (descriptor_file, extent_file) as TempFiles.
    fn create_test_vmdk() -> (TempFile, TempFile) {
        // Create the flat extent file (raw data backing).
        let extent_file = TempFile::new().unwrap();
        let extent_size: u64 = 1024 * 1024; // 1 MB
        extent_file.as_file().set_len(extent_size).unwrap();

        // Write some test data at the beginning of the extent.
        let test_data = b"Hello VMDK from Firecracker!";
        std::io::Write::write_all(&mut &*extent_file.as_file(), test_data).unwrap();

        let extent_path = extent_file.as_path().to_str().unwrap().to_string();
        let extent_sectors = extent_size / 512;

        // Create the VMDK descriptor file.
        let descriptor_file = TempFile::new().unwrap();
        let descriptor_content = format!(
            r#"# Disk DescriptorFile
version=1
CID=fffffffe
parentCID=ffffffff
createType="monolithicFlat"

# Extent description
RW {extent_sectors} FLAT "{extent_path}" 0

# The Disk Data Base
#DDB
"#
        );
        descriptor_file
            .as_file()
            .write_all(descriptor_content.as_bytes())
            .unwrap();

        (descriptor_file, extent_file)
    }

    #[test]
    fn test_detect_raw_format() {
        let f = TempFile::new().unwrap();
        f.as_file().set_len(4096).unwrap();

        let file = std::fs::File::open(f.as_path()).unwrap();
        let format = detect_disk_format(&file).unwrap();
        assert_eq!(format, DiskImageFormat::Raw);
    }

    #[test]
    fn test_detect_vmdk_descriptor_format() {
        let f = TempFile::new().unwrap();
        f.as_file()
            .write_all(b"# Disk DescriptorFile\nversion=1\n")
            .unwrap();

        let file = std::fs::File::open(f.as_path()).unwrap();
        let format = detect_disk_format(&file).unwrap();
        assert_eq!(format, DiskImageFormat::Vmdk);
    }

    #[test]
    fn test_detect_vmdk_createtype_format() {
        let f = TempFile::new().unwrap();
        f.as_file()
            .write_all(b"version=1\ncreateType=\"monolithicFlat\"\n")
            .unwrap();

        let file = std::fs::File::open(f.as_path()).unwrap();
        let format = detect_disk_format(&file).unwrap();
        assert_eq!(format, DiskImageFormat::Vmdk);
    }

    #[test]
    fn test_detect_vmdk_sparse_magic() {
        let f = TempFile::new().unwrap();
        // Write VMDK4 sparse magic: 'KDMV' = 0x564d444b in LE
        let magic_bytes: [u8; 4] = VMDK4_MAGIC.to_le_bytes();
        f.as_file().write_all(&magic_bytes).unwrap();
        f.as_file().set_len(4096).unwrap();

        let file = std::fs::File::open(f.as_path()).unwrap();
        let format = detect_disk_format(&file).unwrap();
        assert_eq!(format, DiskImageFormat::Vmdk);
    }

    #[test]
    fn test_detect_empty_file_as_raw() {
        let f = TempFile::new().unwrap();
        // Don't write anything - empty file

        let file = std::fs::File::open(f.as_path()).unwrap();
        let format = detect_disk_format(&file).unwrap();
        assert_eq!(format, DiskImageFormat::Raw);
    }

    #[test]
    fn test_vmdk_engine_open_and_read() {
        let (descriptor, _extent) = create_test_vmdk();

        let file = std::fs::File::open(descriptor.as_path()).unwrap();
        let engine = VmdkFileEngine::from_file(file).unwrap();

        // Check that disk size matches the extent size.
        assert_eq!(engine.disk_size(), 1024 * 1024);

        // Read the first 512 bytes using a temporary buffer (no guest memory needed for this
        // low-level test).
        let mut buf = vec![0u8; 512];
        engine.access.read(&mut buf[..], 0).unwrap();
        assert_eq!(&buf[..28], b"Hello VMDK from Firecracker!");
    }

    #[test]
    fn test_vmdk_engine_write_returns_error() {
        let (descriptor, _extent) = create_test_vmdk();

        let file = std::fs::File::open(descriptor.as_path()).unwrap();
        let engine = VmdkFileEngine::from_file(file).unwrap();

        // Writing should always fail.
        use crate::vmm_config::machine_config::HugePageConfig;
        use crate::vstate::memory::{self, GuestRegionMmapExt};

        let mem = crate::vstate::memory::GuestMemoryMmap::from_regions(
            memory::anonymous(
                [(GuestAddress(0), 4096)].into_iter(),
                true,
                HugePageConfig::None,
            )
            .unwrap()
            .into_iter()
            .map(|region| GuestRegionMmapExt::dram_from_mmap_region(region, 0))
            .collect(),
        )
        .unwrap();

        let result = engine.write(0, &mem, GuestAddress(0), 512);
        assert!(matches!(result, Err(VmdkIoError::WriteNotSupported)));
    }

    #[test]
    fn test_vmdk_engine_flush() {
        let (descriptor, _extent) = create_test_vmdk();

        let file = std::fs::File::open(descriptor.as_path()).unwrap();
        let engine = VmdkFileEngine::from_file(file).unwrap();

        // Flush should succeed (no-op for read-only).
        engine.flush().unwrap();
    }

    #[test]
    fn test_vmdk_engine_debug() {
        let (descriptor, _extent) = create_test_vmdk();

        let file = std::fs::File::open(descriptor.as_path()).unwrap();
        let engine = VmdkFileEngine::from_file(file).unwrap();

        let debug_str = format!("{:?}", engine);
        assert!(debug_str.contains("VmdkFileEngine"));
        assert!(debug_str.contains("disk_size"));
    }
}
