use anyhow::Result;
use chrono::Local;
use clap::{Parser, Subcommand};
use mini_db::log::Level;
use mini_db::{START_TIME, debug, error, info};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};

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
        worker_queue_size: usize,

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
            worker_queue_size,
            log_level,
        } => {
            run_server(server_addr, worker_queue_size, log_level).await?;
        }
    }

    Ok(())
}

async fn run_server(addr: String, worker_queue_size: usize, log_level: Level) -> Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    let addr = listener.local_addr()?;

    START_TIME
        .set(Local::now())
        .expect("Failed to set START_TIME");

    let shutdown_notifier = Arc::new(Notify::new());
    let active_connections = Arc::new(AtomicU32::new(0));

    let (db_handler, client_handler) = mpsc::channel(worker_queue_size);

    loop {
        // Wait for either a new connection or a shutdown signal
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
                        let db_handler = db_handler.clone();
                        let active_connections = active_connections.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_client(socket, db_handler).await {
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

    // wait for all active connections to finish before shutting down (returning from main)
    while active_connections.load(Ordering::Acquire) != 0 {
        tokio::task::yield_now().await;
    }
    info!("Server has stopped.");

    Ok(())
}

pub async fn handle_client(
    socket: TcpStream,
    db_handler: mpsc::Sender<mini_db::worker::ClientRequest>,
) -> Result<()> {
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
