use crate::protocol::Response;
use crate::wal::Wal;
use crate::worker::{DatabaseQueryOperation, DatabaseWorker};
use crate::{DEFAULT_DB_NAME, debug, fmt_bytes};
use anyhow::Result;
use arc_swap::ArcSwap;
use bytes::Bytes;
use rustc_hash::FxHashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Notify, mpsc};

/// Handle to a running database worker.
#[derive(Clone)]
struct DBHandle {
    worker_handle: mpsc::Sender<DatabaseQueryOperation>,
    worker_exit: Arc<Notify>,
}

/// Manages multiple database instances, each backed by its own WAL file and worker task.
pub struct DbManager {
    wal_dir: PathBuf,
    fsync_interval: usize,
    max_queue_size: usize,
    db_map: Arc<ArcSwap<FxHashMap<String, DBHandle>>>,
    write_lock: tokio::sync::Mutex<()>, // tiny write lock for NOT risking race conditions like lost-update load
}

impl DbManager {
    /// Create wal_dir if needed, scan existing `*.log` files, build memory index,
    /// spawn a worker for each DB, and ensure `default.log` exists
    pub async fn init(
        wal_dir: PathBuf,
        fsync_interval: usize,
        max_queue_size: usize,
    ) -> Result<Self> {
        tokio::fs::create_dir_all(&wal_dir).await?;
        let mut dbs: FxHashMap<String, DBHandle> = FxHashMap::default();

        // iter over wal entries, init memory index, spawn worker for each DB,
        // finally them into the map with exit notifiers
        let start_time = std::time::Instant::now();
        let mut wal_entries = tokio::fs::read_dir(&wal_dir).await?;
        debug!("Indexing WAL entries from: {:?}", wal_dir);
        while let Some(entry) = wal_entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("log") {
                let file_name = match path.file_stem().and_then(|n| n.to_str()) {
                    Some(name) => name,
                    None => return Err(anyhow::anyhow!("Invalid log file name")), // TODO; should we continue or return Err?
                };
                let file_metadata = entry.metadata().await?;
                let file_size = file_metadata.len();

                // init WAL + memory index
                debug!(
                    "Initializing WAL/Index and spawning DbWorkers for: '{}' (size: {})",
                    file_name,
                    fmt_bytes(file_size as usize)
                );
                let (wal, memory_index) = Wal::init(path.clone()).await?;
                let (query_tx, query_rx) = mpsc::channel(max_queue_size);
                let worker_drop_notifier = Arc::new(Notify::new());

                // spawn worker task, store handle
                let mut worker = DatabaseWorker::init(wal, fsync_interval, memory_index, query_rx);
                let worker_drop_notifier_clone = worker_drop_notifier.clone();
                tokio::spawn(async move {
                    let result = worker.execute_queries().await;
                    worker_drop_notifier_clone.notify_one();
                    result
                });
                dbs.insert(
                    file_name.to_string(),
                    DBHandle {
                        worker_handle: query_tx,
                        worker_exit: worker_drop_notifier,
                    },
                );
            }
        }
        debug!(
            "Indexed {} WAL entries in {:.2?}",
            dbs.len(),
            start_time.elapsed()
        );

        // ensure default.log exists
        if !dbs.contains_key(DEFAULT_DB_NAME) {
            Self::spawn_db_worker(
                &wal_dir,
                DEFAULT_DB_NAME,
                fsync_interval,
                max_queue_size,
                &mut dbs,
            )
            .await?;
        }

        Ok(Self {
            wal_dir,
            fsync_interval,
            max_queue_size,
            db_map: Arc::new(ArcSwap::from_pointee(dbs)),
            write_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Create a database
    pub async fn create_db(&self, name: &str) -> Result<Response> {
        let _guard = self.write_lock.lock().await;
        let mut dbs = (*self.db_map.load_full()).clone();
        if dbs.contains_key(name) {
            return Ok(Response::DbAlreadyExists(Bytes::from_static(
                b"Database already exists",
            )));
        }

        // spawn task and store handle in db_map
        Self::spawn_db_worker(
            &self.wal_dir,
            name,
            self.fsync_interval,
            self.max_queue_size,
            &mut dbs,
        )
        .await?;
        self.db_map.store(Arc::new(dbs));

        Ok(Response::Ok(Bytes::from_static(
            b"Database created successfully",
        )))
    }

    /// Drop a database
    pub async fn drop_db(&self, name: &str) -> Result<Response> {
        let _guard = self.write_lock.lock().await;
        if name == DEFAULT_DB_NAME {
            return Ok(Response::InvalidRequest(Bytes::from_static(
                b"Cannot drop the default database",
            )));
        }

        let mut dbs = (*self.db_map.load_full()).clone();
        let handle = dbs
            .remove(name)
            .ok_or_else(|| anyhow::anyhow!("database not found: {name}"))?;
        self.db_map.store(Arc::new(dbs));

        // drop main sender -> worker exits loop -> final fsync -> notify_one -> task returns
        // TODO; in-flight queries to this db, may be dropped or hanged!!!
        drop(handle.worker_handle);
        handle.worker_exit.notified().await;

        let wal_path = self.wal_dir.join(format!("{name}.log"));
        tokio::fs::remove_file(&wal_path).await?;

        Ok(Response::Ok(Bytes::from_static(
            b"Database dropped successfully",
        )))
    }

    /// Get the mpsc handle for a database worker, if it exists
    #[inline(always)]
    pub fn get_db_handle(
        &self,
        name: &str,
    ) -> Result<mpsc::Sender<DatabaseQueryOperation>, Response> {
        match self.db_map.load().get(name) {
            Some(handle) => Ok(handle.worker_handle.clone()),
            None => Err(Response::DbNotFound(Bytes::from_static(
                b"Database not found",
            ))),
        }
    }

    /// Check if a database exists
    /// DO NOT USE THIS, could be racy, use `get_db_handle` instead
    #[inline(always)]
    pub fn db_exists(&self, name: &str) -> bool {
        self.db_map.load().contains_key(name)
    }

    /// Number of active databases
    #[inline(always)]
    pub fn db_count(&self) -> usize {
        self.db_map.load().len()
    }

    /// Shutdown every worker: drop all main senders, wait for all tasks to finish + final fsync
    pub async fn shutdown_all_workers(&self) {
        let workers = self.db_map.swap(Arc::new(FxHashMap::default()));

        // collect all worker notifiers before awaiting
        let to_notify: Vec<Arc<Notify>> = workers.values().map(|h| h.worker_exit.clone()).collect();

        // drop all main senders to unblock workers
        drop(workers);

        // notify each worker task to return
        for notify in to_notify {
            notify.notified().await;
        }
    }

    /// Init WAL + spawn worker + store in map, all I/O happens here, caller owns the DB Map mutation
    async fn spawn_db_worker(
        wal_dir: &Path,
        name: &str,
        fsync_interval: usize,
        max_queue_size: usize,
        dbs: &mut FxHashMap<String, DBHandle>,
    ) -> Result<()> {
        let wal_path = wal_dir.join(format!("{name}.log"));
        let (wal, memory_index) = Wal::init(wal_path).await?;

        let (query_tx, query_rx) = mpsc::channel(max_queue_size);
        let done = Arc::new(Notify::new());

        let mut worker = DatabaseWorker::init(wal, fsync_interval, memory_index, query_rx);
        let done_clone = done.clone();
        let _task = tokio::spawn(async move {
            let r = worker.execute_queries().await;
            done_clone.notify_one();
            r
        });

        dbs.insert(
            name.to_string(),
            DBHandle {
                worker_handle: query_tx,
                worker_exit: done,
            },
        );

        Ok(())
    }
}
