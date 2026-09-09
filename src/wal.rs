use anyhow::Result;
use bytes::{Bytes, BytesMut};
use crc32fast::Hasher;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

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
    write_buf: BytesMut,
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
            write_buf: BytesMut::zeroed(24 << 20), // 24 MB
        })
    }

    /// Write new entry to WAL file, returns the offset of the new entry.
    /// `fsync` is not performed here, call `sync()` to ensure durability.
    pub async fn append(&mut self, op: u8, key: Bytes, value: Bytes) -> Result<u64> {
        let klen = key.len();
        let vlen = value.len();
        let total_len = HEADER_LEN + klen + vlen + CRC_LEN;
        if self.write_buf.capacity() < total_len {
            self.write_buf.reserve(total_len);
        }

        // batch header and payload
        self.write_buf.extend_from_slice(&[op]);
        self.write_buf
            .extend_from_slice(&(klen as u32).to_le_bytes());
        self.write_buf
            .extend_from_slice(&(vlen as u32).to_le_bytes());
        self.write_buf.extend_from_slice(&key);
        self.write_buf.extend_from_slice(&value);

        // compute crc32 of current entry (op, klen, vlen, key, value)
        let crc = crc_hash(&mut self.crc32_hasher, op, klen, vlen, &key, &value);
        self.write_buf.extend_from_slice(&crc.to_le_bytes());

        // write once
        self.file.write_all(&self.write_buf).await?;
        self.write_buf.clear(); // clear buf for next entry

        // update state
        let offset = self.next_offset;
        self.next_offset += total_len as u64;
        self.entry_count += 1;

        Ok(offset)
    }

    /// Read all entries from WAL file and return a memory index.
    pub async fn index(&mut self) -> HashMap<Bytes, Bytes> {
        let data = match tokio::fs::read(&self.path).await {
            Ok(d) => Bytes::from(d),
            Err(_) => return HashMap::with_capacity(1 << 20),
        };

        let mut offset = 0;
        let len = data.len();

        // preallocate with least 15 bytes per entry
        // (1B op + 4B klen + 4B vlen + 1B key + 1B value + 4B crc)
        let map_index = HashMap::with_capacity(len / 15);

        map_index
    }

    /// Flush and sync WAL file to disk.
    pub async fn sync(&mut self) -> Result<()> {
        self.file.flush().await?;
        self.file.sync_all().await?;
        Ok(())
    }

    /// Get the offset of the next entry to be written.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Get the number of entries written to the WAL file.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Get the path of the WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[inline(always)]
fn crc_hash(
    hasher: &mut Hasher,
    op: u8,
    klen: usize,
    vlen: usize,
    key: &[u8],
    value: &[u8],
) -> u32 {
    hasher.update(&[op]);
    hasher.update(&(klen as u32).to_le_bytes());
    hasher.update(&(vlen as u32).to_le_bytes());
    hasher.update(key);
    hasher.update(value);
    hasher.clone().finalize()
}
