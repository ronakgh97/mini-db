use anyhow::Result;
use bytes::Bytes;
use crc32fast::Hasher;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::protocol::Operation;

pub const HEADER_LEN: usize = 1 + 4 + 4; // op + klen + vlen
pub const CRC_LEN: usize = 4; // CRC32 of (op, klen, vlen, key, value)

/// Single-file WAL owned by a single DB worker.
/// Entry format: `(1 B OP | 4B KLEN | 4B VLEN | key | value | 4B CRC32)`.
pub struct Wal {
    file: File,
    path: PathBuf,
    next_offset: u64,
    entry_count: u64,
    crc32_hasher: Hasher,
}

impl Wal {
    /// Open or create WAL file.
    pub async fn init(path: PathBuf) -> Result<Self> {
        let mut file = OpenOptions::default()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await?;
        let next_offset = file.seek(SeekFrom::End(0)).await?;
        let crc32_hasher = Hasher::default();

        Ok(Self {
            file,
            path,
            next_offset,
            entry_count: 0,
            crc32_hasher,
        })
    }

    pub async fn append(&mut self, op: Operation, key: Bytes, value: Bytes) -> Result<u64> {}

    pub async fn index() -> HashMap<Bytes, Bytes> {}

    pub async fn sync(&mut self) -> Result<()> {
        self.file.flush().await?;
        self.file.sync_all().await?;
        Ok(())
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[inline(always)]
fn crc_hash(mut hasher: Hasher, op: u8, klen: usize, vlen: usize, key: &[u8], value: &[u8]) -> u32 {
    hasher.update(&[op]);
    hasher.update(&(klen as u32).to_le_bytes());
    hasher.update(&(vlen as u32).to_le_bytes());
    hasher.update(key);
    hasher.update(value);
    hasher.finalize()
}
