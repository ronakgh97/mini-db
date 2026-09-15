use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use chrono::Local;
use clap::{Parser, Subcommand};
use colored::Colorize;
use mini_db::log::{LOG_LEVEL, Level};
use mini_db::manager::DbManager;
use mini_db::protocol::{Operation, Response};
use mini_db::wal::HEADER_LEN;
use mini_db::worker::DatabaseQueryOperation;
use mini_db::{MAX_DB_NAME_LEN, START_TIME, debug, error, get_uptime_hrs, info, trace};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

#[derive(Parser)]
#[command(
    name = "mini-db",
    author = "ronakgh97 <ronakgh97@gmail.com>",
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
        #[arg(long, default_value = "127.0.0.1:8787")]
        server_addr: String,

        /// Number of database operation that single worker can handle concurrently
        #[arg(long, default_value = "2048")]
        max_queue_size: usize,

        /// Maximum size of a key in bytes
        #[arg(long, default_value = "4096")]
        max_key_size: usize,

        /// Maximum size of a value in bytes
        #[arg(long, default_value = "16384")]
        max_value_size: usize,

        /// Interval for syncing the write-ahead log to disk (in number of write-operations)
        #[arg(long, default_value = "16")]
        fsync_interval: usize,

        /// Whether to enable lazy indexing (default: false)
        #[arg(long, default_value = "false")]
        lazy_indexing: bool,

        /// Path to the write-ahead log directory (default: "./mini-logs/")
        #[arg(long, default_value = "./mini-logs/")]
        wal_dir: PathBuf,

        /// Level of verbosity for logging
        #[arg(long, default_value = "debug")]
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
            fsync_interval,
            wal_dir,
            lazy_indexing,
            log_level,
        } => {
            print_ascii_art();
            run_server(
                server_addr,
                max_key_size,
                max_value_size,
                max_queue_size,
                fsync_interval,
                lazy_indexing,
                wal_dir,
                log_level,
            )
            .await?;
        }
    }

    Ok(())
}

fn print_ascii_art() {
    let art = r#"
                               ▄▄ ▄▄
         ▀▀        ▀▀          ██ ██
███▄███▄ ██  ████▄ ██       ▄████ ████▄
██ ██ ██ ██  ██ ██ ██ ▀▀▀▀▀ ██ ██ ██ ██
██ ██ ██ ██▄ ██ ██ ██▄      ▀████ ████▀
"#;
    print!("{}", art.bold().cyan());
}

#[allow(clippy::too_many_arguments)]
async fn run_server(
    addr: String,
    max_key_size: usize,
    max_value_size: usize,
    max_queue_size: usize,
    fsync_interval: usize,
    #[allow(unused)] lazy_indexing: bool, // TODO; implement lazy indexing managed by DManager
    wal_dir: PathBuf,
    log_level: Level,
) -> Result<()> {
    START_TIME
        .set(Local::now())
        .expect("Failed to set START_TIME");
    LOG_LEVEL.set(log_level).expect("Failed to set LOG_LEVEL");

    info!("Performing necessary initialization...");
    let db_manager = Arc::new(DbManager::init(wal_dir, fsync_interval, max_queue_size).await?);

    // // init wal and build memory index if available
    // let (wal, map_index) = Wal::init(wal_path).await?;
    //
    // // init database worker(s) and mpsc channel for communication
    // let (client_handler, db_handler) = mpsc::channel::<DatabaseOperation>(max_queue_size);
    // let mut db_worker = DatabaseWorker::init(wal, fsync_interval, map_index, db_handler);
    //
    // tokio::spawn(async move {
    //     if let Err(e) = db_worker.execute_operations().await {
    //         error!("Database worker encountered an error: {:?}", e);
    //     }
    // });

    // init necessary shared state for graceful shutdown
    // let shutdown_notifier = Arc::new(Notify::new());
    let active_connections = Arc::new(AtomicU32::new(0));

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
                // shutdown_notifier.notify_waiters();
                break;
            }
            res = listener.accept() => {
                match res {
                    Ok((socket, addr)) => {
                        trace!("Accepted connections from {}", addr);
                        active_connections.fetch_add(1, Ordering::AcqRel); // inc active connections count

                        let db_manager_clone = db_manager.clone(); // client needs to send queries to DB manager
                        let active_connections_clone = active_connections.clone(); // clone for dec on task completion
                        let active_connections_task_clone = active_connections_clone.clone(); // clone for stats operation

                        // spawn per client connection task
                        tokio::spawn(async move {
                            if let Err(e) = handle_client(
                                socket,
                                max_key_size,
                                max_value_size,
                                active_connections_task_clone,
                                db_manager_clone
                            ).await {
                                if is_connection_error(&e) {
                                    debug!("Client (ip: {}) disconnected", addr);
                                } else {
                                    error!("Error handling client (ip: {}): {:?}", addr, e);
                                }
                            };
                            active_connections_clone.fetch_sub(1, Ordering::AcqRel);
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
    // TODO: polling is fine for small active connections
    while active_connections.load(Ordering::Acquire) != 0 {
        tokio::task::yield_now().await;
    }
    db_manager.shutdown_all_workers().await; // shutdown all workers gracefully
    info!("Server has stopped");

    Ok(())
}

#[inline(always)]
fn is_connection_error(e: &anyhow::Error) -> bool {
    if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
        return matches!(
            io_err.kind(),
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof
        );
    }
    false
}

pub async fn handle_client(
    mut socket: TcpStream,
    max_key_size: usize,
    max_value_size: usize,
    active_connections: Arc<AtomicU32>,
    db_manager: Arc<DbManager>,
) -> Result<()> {
    socket.nodelay()?; // disable Nagle's algorithm

    // reusable buffer for reading from socket, to avoid repeated allocations inside loop
    let mut read_buf = BytesMut::with_capacity(HEADER_LEN + max_key_size + max_value_size);

    loop {
        let (db_result, client_response) = oneshot::channel::<Result<Response>>();

        let op = match socket.read_u8().await {
            Ok(op) => op,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // client sends FIN, close the connection gracefully
                break;
            }
            Err(e) => {
                return Err(e.into());
            }
        };

        match Operation::from_u8(op) {
            Some(op) => match op {
                Operation::Stats => {
                    // Stats contains
                    // - server uptime in hours
                    // - number of databases
                    // - number of active connections

                    let mut stats_packet = BytesMut::with_capacity(12);
                    stats_packet.put_f64_le(get_uptime_hrs());
                    stats_packet.put_u32_le(db_manager.db_count() as u32);
                    stats_packet.put_u32_le(active_connections.load(Ordering::Acquire));

                    let rsp = Response::Ok(stats_packet.freeze());
                    rsp.send_response(&mut socket, &mut read_buf).await?;
                }

                Operation::Ping => {
                    // send back 'PONG' response with the same payload
                    let rsp = Response::Pong(Bytes::from_static(b"PONG"));
                    rsp.send_response(&mut socket, &mut read_buf).await?;
                }

                Operation::Close => {
                    // client wants to close the connection, break the loop and close gracefully
                    break;
                }

                Operation::Create => {
                    let db_name_len = socket.read_u8().await? as usize;

                    read_exact_reuse_buf(&mut socket, db_name_len, &mut read_buf).await?; // reuse buffer for later operation
                    let db_name_buf = read_buf.split_to(db_name_len).freeze();
                    let db_name =
                        validate_db_name(&db_name_buf, &mut socket, &mut read_buf).await?;

                    let rsp = db_manager.create_db(db_name).await?; // holds TINY write lock, read path are lock-free (atomic swap)
                    rsp.send_response(&mut socket, &mut read_buf).await?;
                }

                Operation::Info => {
                    let db_name_len = socket.read_u8().await? as usize;

                    read_exact_reuse_buf(&mut socket, db_name_len, &mut read_buf).await?; // reuse buffer for later operation
                    let db_name_buf = read_buf.split_to(db_name_len).freeze();
                    let db_name =
                        validate_db_name(&db_name_buf, &mut socket, &mut read_buf).await?;

                    let db_handler = match db_manager.get_db_handle(db_name) {
                        Ok(handle) => handle,
                        Err(error_rsp) => {
                            error_rsp.send_response(&mut socket, &mut read_buf).await?;
                            return Err(anyhow::anyhow!("Database {} not found", db_name));
                        }
                    };

                    db_handler
                        .send(DatabaseQueryOperation::Info { tx: db_result })
                        .await?;
                    send_db_result_to_client(client_response, &mut socket, &mut read_buf).await?;
                }

                Operation::Drop => {
                    let db_name_len = socket.read_u8().await? as usize;

                    read_exact_reuse_buf(&mut socket, db_name_len, &mut read_buf).await?; // reuse buffer for later operation
                    let db_name_buf = read_buf.split_to(db_name_len).freeze();
                    let db_name =
                        validate_db_name(&db_name_buf, &mut socket, &mut read_buf).await?;

                    let rsp = db_manager.drop_db(db_name).await?;
                    rsp.send_response(&mut socket, &mut read_buf).await?;
                }

                Operation::Get => {
                    let db_name_len = socket.read_u8().await? as usize;
                    let key_size = socket.read_u32_le().await? as usize;

                    // perform all validation required
                    read_exact_reuse_buf(&mut socket, db_name_len, &mut read_buf).await?; // reuse buffer for later operation
                    let db_name_buf = read_buf.split_to(db_name_len).freeze();
                    let db_name =
                        validate_db_name(&db_name_buf, &mut socket, &mut read_buf).await?;

                    validate_key(key_size, max_key_size, &mut socket, &mut read_buf).await?;
                    read_exact_reuse_buf(&mut socket, key_size, &mut read_buf).await?;
                    let key = read_buf.split_to(key_size).freeze();

                    // send to DB manager which router to that DB worker via MPSC queue
                    // TODO; do sharding here later for better performance

                    let db_handler = match db_manager.get_db_handle(db_name) {
                        Ok(handle) => handle,
                        Err(error_rsp) => {
                            error_rsp.send_response(&mut socket, &mut read_buf).await?;
                            return Err(anyhow::anyhow!("Database {} not found", db_name));
                        }
                    };

                    // send to DB worker
                    db_handler
                        .send(DatabaseQueryOperation::GET { key, tx: db_result })
                        .await?;

                    // wait for db to process result from DB worker and send response back to client
                    send_db_result_to_client(client_response, &mut socket, &mut read_buf).await?;
                }

                Operation::Set => {
                    let db_name_len = socket.read_u8().await? as usize;
                    let key_size = socket.read_u32_le().await? as usize;
                    let value_size = socket.read_u32_le().await? as usize;

                    // validate database name
                    read_exact_reuse_buf(&mut socket, db_name_len, &mut read_buf).await?;
                    let db_name_buf = read_buf.split_to(db_name_len).freeze();
                    let db_name =
                        validate_db_name(&db_name_buf, &mut socket, &mut read_buf).await?;

                    // validate key and value
                    validate_key(key_size, max_key_size, &mut socket, &mut read_buf).await?;
                    validate_value(value_size, max_value_size, &mut socket, &mut read_buf).await?;

                    read_exact_reuse_buf(&mut socket, key_size, &mut read_buf).await?;
                    let key = read_buf.split_to(key_size).freeze();
                    read_exact_reuse_buf(&mut socket, value_size, &mut read_buf).await?;
                    let value = read_buf.split_to(value_size).freeze();

                    // send to DB worker
                    let db_handler = match db_manager.get_db_handle(db_name) {
                        Ok(handle) => handle,
                        Err(error_rsp) => {
                            error_rsp.send_response(&mut socket, &mut read_buf).await?;
                            return Err(anyhow::anyhow!("Database {} not found", db_name));
                        }
                    };

                    db_handler
                        .send(DatabaseQueryOperation::SET {
                            key,
                            value,
                            tx: db_result,
                        })
                        .await?;

                    // wait for response from DB worker and send back to client
                    send_db_result_to_client(client_response, &mut socket, &mut read_buf).await?;
                }

                Operation::Delete => {
                    let db_name_len = socket.read_u8().await? as usize;
                    let key_size = socket.read_u32_le().await? as usize;

                    // validate database name
                    read_exact_reuse_buf(&mut socket, db_name_len, &mut read_buf).await?;
                    let db_name_buf = read_buf.split_to(db_name_len).freeze();
                    let db_name =
                        validate_db_name(&db_name_buf, &mut socket, &mut read_buf).await?;

                    // validate key
                    validate_key(key_size, max_key_size, &mut socket, &mut read_buf).await?;
                    read_exact_reuse_buf(&mut socket, key_size, &mut read_buf).await?;
                    let key = read_buf.split_to(key_size).freeze();

                    // send to DB worker(s)
                    let db_handler = match db_manager.get_db_handle(db_name) {
                        Ok(handle) => handle,
                        Err(error_rsp) => {
                            error_rsp.send_response(&mut socket, &mut read_buf).await?;
                            return Err(anyhow::anyhow!("Database {} not found", db_name));
                        }
                    };

                    db_handler
                        .send(DatabaseQueryOperation::DELETE { key, tx: db_result })
                        .await?;

                    // wait for response from DB worker and send back to client
                    send_db_result_to_client(client_response, &mut socket, &mut read_buf).await?;
                }
            },
            None => {
                let rsp = Response::InvalidRequest(Bytes::from_static(b"Invalid operation code"));
                rsp.send_response(&mut socket, &mut read_buf).await?;
                return Err(anyhow::anyhow!("Invalid operation code"));
            }
        }
    }

    Ok(())
}

#[inline(always)]
async fn read_exact_reuse_buf(
    socket: &mut TcpStream,
    len: usize,
    buf: &mut BytesMut,
) -> Result<()> {
    buf.clear();
    buf.reserve(len);
    // reserve guarantees capacity,
    // read_exact overwrites all `len` bytes on success,
    // on error .clear() so uninit never escapes.
    unsafe { buf.set_len(len) };
    match socket.read_exact(buf).await {
        Ok(_) => Ok(()),
        Err(e) => {
            buf.clear();
            Err(e.into())
        }
    }
}

#[inline(always)]
async fn validate_db_name<'a>(
    name_buf: &'a [u8],
    socket: &mut TcpStream,
    buf: &mut BytesMut,
) -> Result<&'a str> {
    if name_buf.is_empty() {
        let rsp = Response::InvalidRequest(Bytes::from_static(b"Database name cannot be empty"));
        rsp.send_response(socket, buf).await?;
        return Err(anyhow::anyhow!("Database name cannot be empty"));
    }
    if name_buf.len() > MAX_DB_NAME_LEN {
        let rsp = Response::PayloadTooLarge(Bytes::from_static(b"Database name too long"));
        rsp.send_response(socket, buf).await?;
        return Err(anyhow::anyhow!(format!(
            "Database name {} exceeds maximum allowed length of {} bytes",
            String::from_utf8_lossy(name_buf),
            MAX_DB_NAME_LEN
        )));
    }

    // check for file_name safety (path-injection prevention)
    if !name_buf
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-')
    {
        let rsp =
            Response::InvalidRequest(Bytes::from_static(b"Invalid characters in database name"));
        rsp.send_response(socket, buf).await?;
        return Err(anyhow::anyhow!("Invalid characters in database name"));
    }

    match core::str::from_utf8(name_buf) {
        Ok(s) => Ok(s),
        Err(_) => {
            let rsp =
                Response::InvalidRequest(Bytes::from_static(b"Database name must be valid UTF-8"));
            rsp.send_response(socket, buf).await?;
            Err(anyhow::anyhow!("Database name must be valid UTF-8"))
        }
    }
}

#[inline(always)]
async fn validate_key(
    key_size: usize,
    max_key_size: usize,
    socket: &mut TcpStream,
    buf: &mut BytesMut,
) -> Result<()> {
    if key_size == 0 {
        let rsp = Response::InvalidRequest(Bytes::from_static(b"Key size cannot be zero"));
        rsp.send_response(socket, buf).await?;
        return Err(anyhow::anyhow!("Key cannot be empty"));
    }
    if key_size > max_key_size {
        let rsp = Response::PayloadTooLarge(Bytes::from_static(b"Key too large"));
        rsp.send_response(socket, buf).await?;
        return Err(anyhow::anyhow!(format!(
            "Key size {} exceeds maximum allowed size of {} bytes",
            key_size, max_key_size
        )));
    }

    Ok(())
}

#[inline(always)]
async fn validate_value(
    value_size: usize,
    max_value_size: usize,
    socket: &mut TcpStream,
    buf: &mut BytesMut,
) -> Result<()> {
    if value_size == 0 {
        let rsp = Response::InvalidRequest(Bytes::from_static(b"Value cannot be empty"));
        rsp.send_response(socket, buf).await?;
        return Err(anyhow::anyhow!("Value cannot be empty"));
    }
    if value_size > max_value_size {
        let rsp = Response::PayloadTooLarge(Bytes::from_static(b"Value too large"));
        rsp.send_response(socket, buf).await?;
        return Err(anyhow::anyhow!(format!(
            "Value size {} exceeds maximum allowed size of {} bytes",
            value_size, max_value_size
        )));
    }
    Ok(())
}

#[inline(always)]
async fn send_db_result_to_client(
    client_response: oneshot::Receiver<Result<Response>>,
    client_socket: &mut TcpStream,
    buf: &mut BytesMut,
) -> Result<()> {
    match client_response.await {
        Ok(Ok(response)) => response.send_response(client_socket, buf).await,
        Ok(Err(e)) => {
            let rsp = Response::InternalError(Bytes::from(format!(
                "Something went terribly wrong: {}",
                e
            )));
            rsp.send_response(client_socket, buf).await?;
            Err(anyhow::anyhow!("Internal error: {}", e))
        }
        Err(_) => {
            let rsp = Response::InternalError(Bytes::from_static(
                b"Failed to receive response from worker",
            ));
            rsp.send_response(client_socket, buf).await?;
            Err(anyhow::anyhow!("Failed to receive response from worker"))
        }
    }
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
