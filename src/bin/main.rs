use anyhow::Result;
use chrono::Local;
use clap::{Parser, Subcommand};
use mini_db::log::{LOG_LEVEL, Level};
use mini_db::protocol::Response;
use mini_db::wal::Wal;
use mini_db::worker::{DatabaseOperation, DatabaseWorker};
use mini_db::{START_TIME, debug, error, info};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc, oneshot};

#[derive(Parser)]
#[command(
    name = "mini-db",
    version = "v1.0.0",
    about = "High-performance WAL-based key-value database",
    long_about = "High-performance WAL-based key-value database"
)]
struct Cli {
    #[command(subcommand)]
    command: CliArgs,
}

#[derive(Subcommand)]
enum CliArgs {
    Start {
        /// Address of the server to bind to (format: ip:port)
        #[arg(long, default_value = "0.0.0.0:8787")]
        server_addr: String,

        /// Number of database operation that single worker can handle concurrently
        #[arg(long, default_value = "1024")]
        max_queue_size: usize,

        /// Maximum size of a key in bytes
        #[arg(long, default_value = "4096")]
        max_key_size: usize,

        /// Maximum size of a value in bytes
        #[arg(long, default_value = "16384")]
        max_value_size: usize,

        /// Path to the write-ahead log file
        #[arg(long, default_value = "wal.log")]
        wal_path: PathBuf,

        /// Level of verbosity for logging
        #[arg(long, default_value = "info")]
        log_level: Level,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        CliArgs::Start {
            server_addr,
            max_key_size,
            max_value_size,
            max_queue_size,
            wal_path,
            log_level,
        } => {
            run_server(
                server_addr,
                max_key_size,
                max_value_size,
                max_queue_size,
                wal_path,
                log_level,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_server(
    addr: String,
    max_key_size: usize,
    max_value_size: usize,
    max_queue_size: usize,
    wal_path: PathBuf,
    log_level: Level,
) -> Result<()> {
    START_TIME
        .set(Local::now())
        .expect("Failed to set START_TIME");
    LOG_LEVEL.set(log_level).expect("Failed to set LOG_LEVEL");

    info!("Performing necessary initialization...");
    // init wal and build memory index if available
    let (wal, map_index) = Wal::init(wal_path).await?;

    // init database worker(s) and mpsc channel for communication
    let (client_handler, db_handler) = mpsc::channel::<DatabaseOperation>(max_queue_size);
    let mut db_worker = DatabaseWorker::init(wal, map_index, db_handler);

    tokio::spawn(async move {
        if let Err(e) = db_worker.execute_operations().await {
            error!("Database worker encountered an error: {:?}", e);
        }
    });

    // init necessary shared state for graceful shutdown
    let shutdown_notifier = Arc::new(Notify::new());
    let active_connections = Arc::new(AtomicU32::new(0));

    let max_key_size = Arc::new(max_key_size);
    let max_value_size = Arc::new(max_value_size);

    // finally start the server after all initialization is done
    let listener = TcpListener::bind(&addr).await?;
    let addr = listener.local_addr()?;

    info!("Server listening on {}", addr);

    loop {
        // TODO: Add rate limiting and max connection limit later

        // wait for either a new connection or a shutdown signal
        tokio::select! {
            _ = ctrl_c_handler() => {
                info!("Shutdown signal received, stopping server...");
                shutdown_notifier.notify_waiters();
                break;
            }
            res = listener.accept() => {
                match res {
                    Ok((socket, addr)) => {
                        debug!("Accepted connections from {}", addr);
                        active_connections.fetch_add(1, Ordering::AcqRel); // inc active connections count
                        let active_connections = active_connections.clone(); // clone for the spawned task, dec when returns

                        let client_handler = client_handler.clone();
                        let max_key_size = max_key_size.clone();
                        let max_value_size = max_value_size.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_client(socket, max_key_size, max_value_size, client_handler).await {
                                error!("Error handling client (ip: {}): {:?}", addr, e);
                            };
                            active_connections.fetch_sub(1, Ordering::AcqRel);
                        });
                    }
                    Err(e) => {
                        error!("Error accepting connection from ({}): {:?}", addr, e);
                    }
                }
            }
        }
    }

    drop(listener); // drop to stop accepting new connections
    info!(
        "Waiting for {} active connections to complete...",
        active_connections.load(Ordering::Acquire)
    );

    // wait for all active connections to finish before shutting down (returning to main)
    while active_connections.load(Ordering::Acquire) != 0 {
        tokio::task::yield_now().await;
    }
    info!("Server has stopped");

    Ok(())
}

pub async fn handle_client(
    socket: TcpStream,
    max_key_size: Arc<usize>,
    max_value_size: Arc<usize>,
    client_handler: mpsc::Sender<DatabaseOperation>,
) -> Result<()> {
    let (worker_result, client_response) = oneshot::channel::<Response>();
    socket.nodelay()?;
    todo!()
}

/// Cross-platform Ctrl+C handler that also handles SIGTERM on Unix systems.
async fn ctrl_c_handler() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed Ctrl+C handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
