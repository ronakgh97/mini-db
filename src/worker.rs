use crate::protocol::Response;
use crate::wal::{OP_DELETE, OP_SET, Wal};
use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use rustc_hash::FxHashMap;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;

/// Represents a database query operation that can be performed by the worker.
/// Holds the query data required, and a oneshot channel for to send the result back to the caller.
#[repr(C)]
pub enum DatabaseQueryOperation {
    Info {
        tx: Sender<Result<Response>>,
    },
    GET {
        key: Bytes,
        tx: Sender<Result<Response>>,
    },
    SET {
        key: Bytes,
        value: Bytes,
        tx: Sender<Result<Response>>,
    },
    DELETE {
        key: Bytes,
        tx: Sender<Result<Response>>,
    },
}

/// Represents a worker that handles database operations, maintains an in-memory index,
/// and writes to a write-ahead log (WAL).
pub struct DatabaseWorker {
    wal: Wal,
    fsync_interval: usize,
    memory_index: FxHashMap<Bytes, Bytes>,
    db_handler: Receiver<DatabaseQueryOperation>,
}

impl DatabaseWorker {
    pub fn init(
        wal: Wal,
        fsync_interval: usize,
        memory_index: FxHashMap<Bytes, Bytes>,
        db_handler: Receiver<DatabaseQueryOperation>,
    ) -> Self {
        Self {
            wal,
            fsync_interval,
            memory_index,
            db_handler,
        }
    }

    /// Executes database operations received from the channel.
    /// This will keep waiting and executing operations until the main sender drops.
    pub async fn execute_queries(&mut self) -> Result<()> {
        let mut writes_executed: usize = 0;
        let interval = self.fsync_interval.max(1);
        while let Some(op) = self.db_handler.recv().await {
            match op {
                DatabaseQueryOperation::Info { tx } => {
                    // Info contain
                    // - index_size_disk
                    // - index_size_memory
                    // - key by (min, mean and max byte size)
                    // - key_count

                    let size_on_disk = self.wal.size_on_disk() as u32;
                    let size_in_memory = size_of_val(&self.memory_index) as u32;

                    // iter n calculate the sizes of the keys
                    let (min_key_size, max_key_size, total_key_size) = self
                        .memory_index
                        .keys()
                        .map(|k| k.len())
                        .fold((usize::MAX, 0usize, 0usize), |(min, max, sum), len| {
                            (min.min(len), max.max(len), sum + len)
                        });

                    let key_count = self.memory_index.len() as u32;
                    let min_key_size = if key_count > 0 {
                        min_key_size as u32
                    } else {
                        0
                    };
                    let max_key_size = if key_count > 0 {
                        max_key_size as u32
                    } else {
                        0
                    };
                    let mean_key_size =
                        total_key_size.checked_div(key_count as usize).unwrap_or(0) as u32;

                    let info_packet = {
                        let mut packet = BytesMut::with_capacity(24);
                        packet.put_u32(size_on_disk);
                        packet.put_u32(size_in_memory);
                        packet.put_u32(min_key_size);
                        packet.put_u32(mean_key_size);
                        packet.put_u32(max_key_size);
                        packet.put_u32(key_count);
                        packet
                    };

                    let _ = tx.send(Ok(Response::Ok(info_packet.freeze())));
                }
                DatabaseQueryOperation::GET { key, tx } => {
                    if let Some(value) = self.memory_index.get(&key) {
                        // let _ = because the receiver might have been dropped,
                        let _ = tx.send(Ok(Response::KeyValue(value.clone())));
                    } else {
                        let _ = tx.send(Ok(Response::KeyNotFound(Bytes::from_static(
                            b"Key not found",
                        ))));
                    }
                }

                DatabaseQueryOperation::SET { key, value, tx } => {
                    if let Err(e) = self.wal.append(OP_SET, &key, &value).await {
                        let _ = tx.send(Err(e));
                        continue;
                    }
                    writes_executed += 1;
                    if writes_executed.is_multiple_of(interval)
                        && let Err(e) = self.wal.fsync().await
                    {
                        let _ = tx.send(Err(e));
                        continue;
                    }
                    // incoming key/value are split_to() slices,
                    // sharing the connection's reusable read buffer, moving them
                    // into the index would pin that whole buffer per key
                    self.memory_index
                        .insert(Bytes::copy_from_slice(&key), Bytes::copy_from_slice(&value));
                    let _ = tx.send(Ok(Response::Ok(Bytes::from_static(b"OK"))));
                }

                DatabaseQueryOperation::DELETE { key, tx } => {
                    if let Err(e) = self
                        .wal
                        .append(OP_DELETE, &key, &Bytes::from_static(b""))
                        .await
                    {
                        let _ = tx.send(Err(e));
                        continue;
                    }
                    writes_executed += 1;
                    if writes_executed.is_multiple_of(interval)
                        && let Err(e) = self.wal.fsync().await
                    {
                        let _ = tx.send(Err(e));
                        continue;
                    }
                    if self.memory_index.remove(&key).is_some() {
                        let _ = tx.send(Ok(Response::Ok(Bytes::from_static(b"OK"))));
                    } else {
                        let _ = tx.send(Ok(Response::KeyNotFound(Bytes::from_static(
                            b"Key not found",
                        ))));
                    }
                }
            }
        }

        // final fsync before returning
        let _ = self.wal.fsync().await;
        Ok(())
    }
}
