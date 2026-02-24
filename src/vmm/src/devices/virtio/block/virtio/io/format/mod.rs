// Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Disk image format detection and format-specific drivers.
//!
//! This module provides format detection for disk images (Raw vs VMDK)
//! and hosts format-specific I/O engines. Raw format requires no special
//! driver — sync_io and async_io handle it directly.

pub mod vmdk;

use std::fs::File;
use std::io;

pub use self::vmdk::{VmdkFileEngine, VmdkIoError};

/// VMDK4 sparse file magic: 'KDMV' in little-endian.
const VMDK4_MAGIC: u32 = 0x564d_444b;

/// The detected disk image format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskImageFormat {
    /// Raw disk image (no format header detected).
    Raw,
    /// VMDK disk image.
    Vmdk,
}

/// Detects the format of a disk image by reading its first bytes.
///
/// Returns `DiskImageFormat::Vmdk` if the file appears to be a VMDK descriptor or sparse file,
/// otherwise returns `DiskImageFormat::Raw`.
pub fn detect_disk_format(file: &File) -> io::Result<DiskImageFormat> {
    use std::os::unix::fs::FileExt;

    // Read the first 512 bytes to check for VMDK signatures.
    let mut header = [0u8; 512];
    let bytes_read = file.read_at(&mut header, 0)?;
    if bytes_read < 4 {
        return Ok(DiskImageFormat::Raw);
    }

    // Check for VMDK4 sparse magic (binary VMDK).
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    if magic == VMDK4_MAGIC {
        return Ok(DiskImageFormat::Vmdk);
    }

    // Check for VMDK descriptor file (text-based).
    if let Ok(text) = std::str::from_utf8(&header[..bytes_read]) {
        if text.contains("# Disk DescriptorFile")
            || (text.contains("version") && text.contains("createType"))
        {
            return Ok(DiskImageFormat::Vmdk);
        }
    }

    Ok(DiskImageFormat::Raw)
}
