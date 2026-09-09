use crate::protocol::Response;
use crate::wal::{OP_DELETE, OP_SET, Wal};
use anyhow::Result;
use bytes::Bytes;
use std::collections::HashMap;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;

/// Represents a database operation that can be performed by the worker.
/// Holds a key, value (if applicable), and a channel to send the result back to the caller.
#[repr(C)]
pub enum DatabaseOperation {
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
    memory_index: HashMap<Bytes, Bytes>,
    db_handler: Receiver<DatabaseOperation>,
}

impl DatabaseWorker {
    pub fn init(
        wal: Wal,
        fsync_interval: usize,
        memory_index: HashMap<Bytes, Bytes>,
        db_handler: Receiver<DatabaseOperation>,
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
    pub async fn execute_operations(&mut self) -> Result<()> {
        let mut writes_executed: usize = 0;
        // interval==0 would panic/diverge on modulo — treat as fsync-every-write.
        let interval = self.fsync_interval.max(1);
        while let Some(op) = self.db_handler.recv().await {
            match op {
                DatabaseOperation::GET { key, tx } => {
                    if let Some(value) = self.memory_index.get(&key) {
                        // let _ = because the receiver might have been dropped,
                        let _ = tx.send(Ok(Response::KeyValue(value.clone())));
                    } else {
                        let _ = tx.send(Ok(Response::KeyNotFound(Bytes::from("Key not found"))));
                    }
                }

                DatabaseOperation::SET { key, value, tx } => {
                    if let Err(e) = self.wal.append(OP_SET, key.clone(), value.clone()).await {
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
                    self.memory_index.insert(key, value);
                    let _ = tx.send(Ok(Response::Ok(Bytes::from("OK"))));
                }

                DatabaseOperation::DELETE { key, tx } => {
                    if let Err(e) = self.wal.append(OP_DELETE, key.clone(), Bytes::new()).await {
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
                        let _ = tx.send(Ok(Response::Ok(Bytes::from("OK"))));
                    } else {
                        let _ = tx.send(Ok(Response::KeyNotFound(Bytes::from("Key not found"))));
                    }
                }
            }
        }
        // Final fsync so the last partial batch (< interval) is durable on shutdown.
        let _ = self.wal.fsync().await;
        Ok(())
    }
}
