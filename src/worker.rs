use crate::wal::Wal;
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
        tx: Sender<Result<Bytes>>,
    },
    SET {
        key: Bytes,
        value: Bytes,
        tx: Sender<Result<Bytes>>,
    },
    DELETE {
        key: Bytes,
        tx: Sender<Result<Bytes>>,
    },
}

/// Represents a worker that handles database operations, maintains an in-memory index,
/// and writes to a write-ahead log (WAL).
pub struct DatabaseWorker {
    wal: Wal,
    memory_index: HashMap<Bytes, Bytes>,
    db_handler: Receiver<DatabaseOperation>,
}

impl DatabaseWorker {
    pub fn init(
        wal: Wal,
        memory_index: HashMap<Bytes, Bytes>,
        db_handler: Receiver<DatabaseOperation>,
    ) -> Self {
        Self {
            wal,
            memory_index,
            db_handler,
        }
    }

    /// Executes database operations received from the channel.
    /// This will keep waiting and executing operations until the main sender drops.
    pub async fn execute_operations(&mut self) -> Result<(u8, Bytes)> {
        while let Some(op) = self.db_handler.recv().await {
            match op {
                DatabaseOperation::GET { key, tx } => {
                    todo!()
                }
                DatabaseOperation::SET { key, value, tx } => {
                    todo!()
                }
                DatabaseOperation::DELETE { key, tx } => {
                    todo!()
                }
            }
        }
        todo!()
    }
}
