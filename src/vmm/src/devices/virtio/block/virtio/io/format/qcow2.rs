// Copyright (c) 2026 Tencent. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fs::File;
use std::io;
use std::path::Path;

use imago::file::File as ImagoFile;
use imago::format::access::FormatAccess;
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
    /// QCOW2 write error: {0}
    Write(io::Error),
    /// QCOW2 discard is not supported.
    DiscardNotSupported,
    /// The QCOW2 backend cannot be re-pointed at another file after being opened.
    UnsupportedOperation,
    /// Guest memory error: {0}
    GuestMemory(GuestMemoryError),
    /// QCOW2 flush error: {0}
    Flush(io::Error),
}

const READ_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Debug)]
pub struct Qcow2FileEngine {
    access: FormatAccess<ImagoFile>,
    read_buffer: Box<[u8]>,
}

impl Qcow2FileEngine {
    pub fn from_file(file: File, path: &Path, read_only: bool) -> Result<Self, Qcow2IoError> {
        let storage = ImagoFile::from_open_file(
            file,
            StorageOpenOptions::new()
                .filename(path)
                .write(!read_only)
                .direct(false),
        )
        .map_err(Qcow2IoError::Open)?;
        let qcow2 = Qcow2::<ImagoFile>::builder(storage)
            .write(!read_only)
            .open(DenyImplicitOpenGate::default())
            .map_err(Qcow2IoError::Open)?;

        Ok(Self {
            access: FormatAccess::new(qcow2),
            read_buffer: vec![0; READ_BUFFER_SIZE].into_boxed_slice(),
        })
    }

    pub fn disk_size(&self) -> u64 {
        self.access.size()
    }

    pub fn read(
        &mut self,
        offset: u64,
        mem: &GuestMemoryMmap,
        addr: GuestAddress,
        count: u32,
    ) -> Result<u32, Qcow2IoError> {
        let slice = mem
            .get_slice(addr, count as usize)
            .map_err(Qcow2IoError::GuestMemory)?;
        offset.checked_add(u64::from(count)).ok_or_else(|| {
            Qcow2IoError::Read(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QCOW2 read offset overflow",
            ))
        })?;

        for start in (0..slice.len()).step_by(READ_BUFFER_SIZE) {
            let len = (slice.len() - start).min(READ_BUFFER_SIZE);
            self.access
                .read(&mut self.read_buffer[..len], offset + start as u64)
                .map_err(Qcow2IoError::Read)?;
            slice
                .subslice(start, len)
                .map_err(|err| Qcow2IoError::GuestMemory(err.into()))?
                .copy_from(&self.read_buffer[..len]);
        }

        Ok(count)
    }

    pub fn write(
        &self,
        offset: u64,
        mem: &GuestMemoryMmap,
        addr: GuestAddress,
        count: u32,
    ) -> Result<u32, Qcow2IoError> {
        let slice = mem
            .get_slice(addr, count as usize)
            .map_err(Qcow2IoError::GuestMemory)?;
        offset.checked_add(u64::from(count)).ok_or_else(|| {
            Qcow2IoError::Write(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QCOW2 write offset overflow",
            ))
        })?;

        let mut buffer = vec![0; READ_BUFFER_SIZE.min(slice.len())];
        for start in (0..slice.len()).step_by(READ_BUFFER_SIZE) {
            let len = (slice.len() - start).min(READ_BUFFER_SIZE);
            slice
                .subslice(start, len)
                .map_err(|err| Qcow2IoError::GuestMemory(err.into()))?
                .copy_to(&mut buffer[..len]);
            self.access
                .write(&buffer[..len], offset + start as u64)
                .map_err(Qcow2IoError::Write)?;
        }

        Ok(count)
    }

    pub fn flush(&self) -> Result<(), Qcow2IoError> {
        self.access.flush().map_err(Qcow2IoError::Flush)?;
        self.access.sync().map_err(Qcow2IoError::Flush)
    }
}
