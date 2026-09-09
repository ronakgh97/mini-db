use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use crc32fast::Hasher;
use memmap2::Mmap;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

pub const HEADER_LEN: usize = 1 + 4 + 4; // op + klen + vlen
pub const CRC_LEN: usize = 4; // CRC32 of (op, klen, vlen, key, value)

const OP_SET: u8 = 1;
const OP_DELETE: u8 = 2;

/// Single-file WAL owned by a single DB worker.
/// Entry format: `(1 B OP | 4B KLEN | 4B VLEN | key | value | 4B CRC32)`.
pub struct Wal {
    file: File,
    file_path: PathBuf,
    file_len: u64,
    entry_count: u64,
    crc32_hasher: Hasher,
    write_buf: BytesMut,
}

// TODO:
//  We are assuming that 'mid-file-corruption' will NOT HAPPEN, if it does.. well we are fucked!!!,
//  'end-of-file-corruption' is handled by truncating the file to the last valid entry,
//  which get overwritten on next append.

impl Wal {
    /// Open or create WAL file, recover from torn/corrupt tail, and return WAL handle and rebuilt memory index.
    pub async fn init(file_path: PathBuf) -> Result<(Self, HashMap<Bytes, Bytes>)> {
        if let Some(parent) = file_path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let mut file = OpenOptions::default()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .await?;

        let file_len = file.seek(SeekFrom::End(0)).await?;
        if file_len == 0 {
            return Ok((
                Self {
                    file,
                    file_path,
                    file_len: 0,
                    entry_count: 0,
                    crc32_hasher: Hasher::default(),
                    write_buf: BytesMut::with_capacity(24 << 20),
                },
                HashMap::with_capacity(1 << 20),
            ));
        }

        let (valid_file_len, valid_entry_count, crc32_hasher, map_index) =
            tokio::task::spawn_blocking({
                let path = file_path.clone();
                // this performs runtime cpu feature detection, so we can't spam it in loop
                let mut crc32_hasher = Hasher::default();

                move || -> Result<(u64, u64, Hasher, HashMap<Bytes, Bytes>)> {
                    let file = std::fs::File::open(&path)?;
                    let mmap = unsafe { Mmap::map(&file)? };

                    let data = &mmap[..];
                    let len = data.len();
                    let mut offset = 0usize;
                    let mut count = 0u64;

                    // least ~15 bytes min per entry (1 op + 4 klen + 4 vlen + 1 key + 1 val + 4 crc)
                    let mut map_index: HashMap<Bytes, Bytes> = HashMap::with_capacity(len / 15);

                    while offset + (HEADER_LEN + CRC_LEN) <= len {
                        // parse headers
                        let op = data[offset];

                        // parse read lengths
                        let klen = u32::from_le_bytes([
                            data[offset + 1],
                            data[offset + 2],
                            data[offset + 3],
                            data[offset + 4],
                        ]) as usize;
                        let vlen = u32::from_le_bytes([
                            data[offset + 5],
                            data[offset + 6],
                            data[offset + 7],
                            data[offset + 8],
                        ]) as usize;
                        let total_len = HEADER_LEN + klen + vlen + CRC_LEN;

                        // trailing incomplete unexpected entry
                        if offset + total_len > len {
                            break;
                        }

                        // get len and slice for key, value, and crc
                        let key_start = offset + HEADER_LEN;
                        let value_start = key_start + klen;
                        let crc_start = value_start + vlen;

                        let key = Bytes::copy_from_slice(&data[key_start..value_start]);
                        let value = Bytes::copy_from_slice(&data[value_start..crc_start]);

                        // validate crc32 of entry
                        let stored_crc = u32::from_le_bytes([
                            data[crc_start],
                            data[crc_start + 1],
                            data[crc_start + 2],
                            data[crc_start + 3],
                        ]);
                        let computed_crc =
                            crc_hash(&mut crc32_hasher, op, klen, vlen, &key, &value);

                        // file corruption, stop here and discard everything after
                        if stored_crc != computed_crc {
                            break;
                        }

                        // build in-memory index
                        match op {
                            OP_SET => {
                                map_index.insert(key, value);
                            }
                            OP_DELETE => {
                                map_index.remove(&key as &[u8]);
                            }
                            _ => { /* ignore op */ }
                        }
                        offset += total_len;
                        count += 1;
                    }
                    Ok((offset as u64, count, crc32_hasher, map_index))
                }
            })
            .await??;

        // truncate torn/corrupt tail to last valid entry, and seek to end of valid entries for next append.
        if valid_file_len < file_len {
            file.set_len(valid_file_len).await?;
            file.seek(SeekFrom::Start(valid_file_len)).await?;
        }

        Ok((
            Self {
                file,
                file_path,
                file_len: valid_file_len,
                entry_count: valid_entry_count,
                crc32_hasher,
                write_buf: BytesMut::with_capacity(24 << 20),
            },
            map_index,
        ))
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

        // write to WAL and flush (fsync is caller's responsibility)
        self.file
            .write_all(&self.write_buf)
            .await
            .context("wal: write")?;
        self.file.flush().await.context("wal: flush")?;
        self.write_buf.clear(); // clear for next entry

        // update state
        let offset = self.file_len;
        self.file_len += total_len as u64;
        self.entry_count += 1;

        Ok(offset)
    }

    /// Flush and sync WAL file to disk.
    pub async fn sync(&mut self) -> Result<()> {
        self.file.flush().await.context("wal: flush")?;
        self.file.sync_all().await.context("wal: fsync")?;
        Ok(())
    }

    /// Get the offset of the next entry to be written.
    pub fn next_offset(&self) -> u64 {
        self.file_len
    }

    /// Get the number of entries written to the WAL file.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Get the path of the WAL file.
    pub fn path(&self) -> &Path {
        &self.file_path
    }
}

/// Compute CRC32 of a WAL entry (op, klen, vlen, key, value) without allocating.
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
    let crc = hasher.clone().finalize();
    hasher.reset(); // reset for next entry
    crc
}
