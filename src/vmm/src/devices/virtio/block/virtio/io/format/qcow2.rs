// Copyright (c) 2026 Tencent. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fs::File;
use std::io;
use std::path::Path;

use imago::file::File as ImagoFile;
use imago::format::access::FormatAccess;
use imago::io_buffers::IoVectorMut;
use imago::qcow2::Qcow2;
use imago::{DenyImplicitOpenGate, FormatDriverBuilder, StorageOpenOptions};
use vm_memory::{GuestMemoryBackend, GuestMemoryError};

use crate::vstate::memory::{GuestAddress, GuestMemoryMmap};

/// Errors specific to the QCOW2 IO engine.
#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum Qcow2IoError {
    /// Failed to open QCOW2 image: {0}
    Open(io::Error),
    /// QCOW2 backend does not support the async file engine.
    AsyncNotSupported,
    /// QCOW2 read error: {0}
    Read(io::Error),
    /// QCOW2 write not supported (read-only image)
    WriteNotSupported,
    /// QCOW2 backend requires is_read_only=true.
    RequiresReadOnly,
    /// Guest memory error: {0}
    GuestMemory(GuestMemoryError),
    /// QCOW2 flush error: {0}
    Flush(io::Error),
}

#[derive(Debug)]
pub struct Qcow2FileEngine {
    access: FormatAccess<ImagoFile>,
}

impl Qcow2FileEngine {
    pub fn from_file(file: File, path: &Path) -> Result<Self, Qcow2IoError> {
        let storage = ImagoFile::from_open_file(
            file,
            StorageOpenOptions::new()
                .filename(path)
                .write(false)
                .direct(false),
        )
        .map_err(Qcow2IoError::Open)?;
        let qcow2 = Qcow2::<ImagoFile>::builder(storage)
            .write(false)
            .open(DenyImplicitOpenGate::default())
            .map_err(Qcow2IoError::Open)?;

        Ok(Self {
            access: FormatAccess::new(qcow2),
        })
    }

    pub fn disk_size(&self) -> u64 {
        self.access.size()
    }

    pub fn read(
        &self,
        offset: u64,
        mem: &GuestMemoryMmap,
        addr: GuestAddress,
        count: u32,
    ) -> Result<u32, Qcow2IoError> {
        let slice = mem
            .get_slice(addr, count as usize)
            .map_err(Qcow2IoError::GuestMemory)?;
        let slices = [slice];
        let (bufv, _guard) = IoVectorMut::from_volatile_slice(&slices);
        self.access
            .readv(bufv, offset)
            .map_err(Qcow2IoError::Read)?;

        Ok(count)
    }

    pub fn write(
        &self,
        _offset: u64,
        _mem: &GuestMemoryMmap,
        _addr: GuestAddress,
        _count: u32,
    ) -> Result<u32, Qcow2IoError> {
        Err(Qcow2IoError::WriteNotSupported)
    }

    pub fn flush(&self) -> Result<(), Qcow2IoError> {
        self.access.flush().map_err(Qcow2IoError::Flush)?;
        self.access.sync().map_err(Qcow2IoError::Flush)
    }
}
