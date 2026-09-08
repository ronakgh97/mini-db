use crate::wal::Wal;
use anyhow::Result;
use bytes::Bytes;
use std::collections::HashMap;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;

#[repr(C)]
pub enum DatabaseOperation {
    GET {
        key: Bytes,
        response: Sender<Result<Bytes>>,
    },
    SET {
        key: Bytes,
        value: Bytes,
        response: Sender<Result<Bytes>>,
    },
    DELETE {
        key: Bytes,
        response: Sender<Result<Bytes>>,
    },
}

struct DatabaseWorker {
    wal: Wal,
    memory_index: HashMap<Bytes, Bytes>,
    db_handler: Receiver<DatabaseOperation>,
}

impl DatabaseWorker {
    fn init(
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

    pub async fn execute_database_operation(&mut self) -> Result<(u8, Bytes)> {
        while let Some(op) = self.db_handler.recv().await {}
        unimplemented!()
    }
}
