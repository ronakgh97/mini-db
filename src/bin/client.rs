use anyhow::Result;
use clap::Parser;
use mini_db::fmt_bytes;
use mini_db::protocol::{InfoPacket, Operation, Response, StatsPacket};
use std::io::{self, Write};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[derive(Parser)]
#[command(
    name = "mini-client",
    version = "v1.0.0",
    about = "REPL client for mini-db server"
)]
struct CliArgs {
    /// Address of the mini-db server to connect to.
    #[arg(long, default_value = "127.0.0.1:8787")]
    server_addr: String,

    /// Database name to operate on.
    #[arg(long, default_value = "default")]
    db_name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CliArgs::parse();
    let mut stream = TcpStream::connect(&args.server_addr).await?;
    stream.set_nodelay(true)?;

    // do ping health check
    send_ping(&mut stream).await?;
    if let Err(e) = Response::read_response(&mut stream).await {
        eprintln!(
            "Failed to ping mini-db server at {}: {}",
            args.server_addr, e
        );
        return Ok(());
    }

    println!("Connected to mini-db at {}", args.server_addr);
    println!("Commands:");
    println!("  GET <key>             - Get a value by key");
    println!("  SET <key> <value>     - Set a key-value pair");
    println!("  DEL <key>             - Delete a key");
    println!("  PING                  - Ping the server");
    println!("  STATS                 - Show server statistics");
    println!("  INFO                  - Show current database info");
    println!("  USE <db_name>         - Switch to a different database");
    println!("  CREATE <db_name>      - Create a new database");
    println!("  INFO <db_name>        - Show database info");
    println!("  DROP <db_name>        - Drop a database");
    println!("  QUIT                  - Close connection");
    println!();

    let mut current_db = args.db_name.clone();

    loop {
        print!("mini-db({})> ", current_db);
        io::stdout().flush()?;

        let mut input = String::with_capacity(1024);
        io::stdin().read_line(&mut input)?;
        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        // split in at most 3 parts: command, key, value (value may contain spaces)
        let parts: Vec<&str> = input.splitn(3, ' ').collect();
        let cmd = parts[0].to_uppercase();

        match cmd.as_str() {
            "QUIT" => {
                send_close(&mut stream).await?;
                break;
            }

            "PING" => {
                let start = Instant::now();
                send_ping(&mut stream).await?;
                let resp = Response::read_response(&mut stream).await?;
                let elapsed = start.elapsed();
                print_response(&resp, elapsed);
            }

            "GET" => {
                if parts.len() < 2 {
                    println!("Usage: GET <key>");
                    continue;
                }
                let key = parts[1].as_bytes();

                let start = Instant::now();
                send_get(&mut stream, &current_db, key).await?;
                let resp = Response::read_response(&mut stream).await?;
                let elapsed = start.elapsed();
                print_response(&resp, elapsed);
            }

            "SET" => {
                if parts.len() < 3 {
                    println!("Usage: SET <key> <value>");
                    continue;
                }
                let key = parts[1].as_bytes();
                let value = parts[2].as_bytes();

                let start = Instant::now();
                send_set(&mut stream, &current_db, key, value).await?;
                let resp = Response::read_response(&mut stream).await?;
                let elapsed = start.elapsed();
                print_response(&resp, elapsed);
            }

            "DEL" => {
                if parts.len() < 2 {
                    println!("Usage: DEL <key>");
                    continue;
                }
                let key = parts[1].as_bytes();

                let start = Instant::now();
                send_delete(&mut stream, &current_db, key).await?;
                let resp = Response::read_response(&mut stream).await?;
                let elapsed = start.elapsed();
                print_response(&resp, elapsed);
            }

            "STATS" => {
                let start = Instant::now();
                send_stats(&mut stream).await?;
                let resp = Response::read_response(&mut stream).await?;
                let elapsed = start.elapsed();
                print_stats_response(&resp, elapsed);
            }

            "CREATE" => {
                if parts.len() < 2 {
                    println!("Usage: CREATE <db_name>");
                    continue;
                }
                let db_name = parts[1];
                if db_name.len() > 255 {
                    println!("Error: db_name must be at most 255 bytes");
                    continue;
                }

                let start = Instant::now();
                send_create(&mut stream, db_name).await?;
                let resp = Response::read_response(&mut stream).await?;
                let elapsed = start.elapsed();
                print_response(&resp, elapsed);
            }

            "INFO" => {
                let db_name = if parts.len() >= 2 {
                    parts[1].to_string()
                } else {
                    current_db.clone()
                };
                if db_name.len() > 255 {
                    println!("Error: db_name must be at most 255 bytes");
                    continue;
                }

                let start = Instant::now();
                send_info(&mut stream, &db_name).await?;
                let resp = Response::read_response(&mut stream).await?;
                let elapsed = start.elapsed();
                print_info_response(&resp, elapsed, &db_name);
            }

            "DROP" => {
                if parts.len() < 2 {
                    println!("Usage: DROP <db_name>");
                    continue;
                }
                let db_name = parts[1];
                if db_name.len() > 255 {
                    println!("Error: db_name must be at most 255 bytes");
                    continue;
                }
                // if db_name == "default" {
                //     println!("Error: cannot drop the default database");
                //     continue;
                // }

                let start = Instant::now();
                send_drop(&mut stream, db_name).await?;
                let resp = Response::read_response(&mut stream).await?;
                let elapsed = start.elapsed();
                print_response(&resp, elapsed);
            }

            "USE" => {
                if parts.len() < 2 {
                    println!("Usage: USE <db_name>");
                    continue;
                }
                current_db = parts[1].to_string();
                println!("Switched to database '{}'", current_db);
            }

            _ => {
                println!("Unknown command: {}", parts[0]);
                println!(
                    "Commands: GET | SET | DEL | PING | STATS | CREATE | INFO | DROP | USE | QUIT"
                );
            }
        }
    }

    stream.flush().await?;
    stream.shutdown().await?;

    Ok(())
}

#[inline(always)]
async fn send_ping(stream: &mut TcpStream) -> Result<()> {
    stream.write_u8(Operation::Ping.to_u8()).await?;
    Ok(())
}

#[inline(always)]
async fn send_close(stream: &mut TcpStream) -> Result<()> {
    stream.write_u8(Operation::Close.to_u8()).await?;
    Ok(())
}

#[inline(always)]
async fn send_stats(stream: &mut TcpStream) -> Result<()> {
    stream.write_u8(Operation::Stats.to_u8()).await?;
    Ok(())
}

#[inline(always)]
async fn send_db_op(stream: &mut TcpStream, op: Operation, db_name: &str) -> Result<()> {
    stream.write_u8(op.to_u8()).await?;
    stream.write_u8(db_name.len() as u8).await?;
    stream.write_all(db_name.as_bytes()).await?;
    Ok(())
}

#[inline(always)]
async fn send_create(stream: &mut TcpStream, db_name: &str) -> Result<()> {
    send_db_op(stream, Operation::Create, db_name).await
}

#[inline(always)]
async fn send_info(stream: &mut TcpStream, db_name: &str) -> Result<()> {
    send_db_op(stream, Operation::Info, db_name).await
}

#[inline(always)]
async fn send_drop(stream: &mut TcpStream, db_name: &str) -> Result<()> {
    send_db_op(stream, Operation::Drop, db_name).await
}

#[inline(always)]
async fn send_get(stream: &mut TcpStream, db_name: &str, key: &[u8]) -> Result<()> {
    stream.write_u8(Operation::Get.to_u8()).await?;
    stream.write_u8(db_name.len() as u8).await?;
    stream.write_u32_le(key.len() as u32).await?;
    stream.write_all(db_name.as_bytes()).await?;
    stream.write_all(key).await?;
    Ok(())
}

#[inline(always)]
async fn send_set(stream: &mut TcpStream, db_name: &str, key: &[u8], value: &[u8]) -> Result<()> {
    stream.write_u8(Operation::Set.to_u8()).await?;
    stream.write_u8(db_name.len() as u8).await?;
    stream.write_u32_le(key.len() as u32).await?;
    stream.write_u32_le(value.len() as u32).await?;
    stream.write_all(db_name.as_bytes()).await?;
    stream.write_all(key).await?;
    stream.write_all(value).await?;
    Ok(())
}

#[inline(always)]
async fn send_delete(stream: &mut TcpStream, db_name: &str, key: &[u8]) -> Result<()> {
    stream.write_u8(Operation::Delete.to_u8()).await?;
    stream.write_u8(db_name.len() as u8).await?;
    stream.write_u32_le(key.len() as u32).await?;
    stream.write_all(db_name.as_bytes()).await?;
    stream.write_all(key).await?;
    Ok(())
}

#[inline(always)]
fn print_response(resp: &Response, elapsed: Duration) {
    let ms = elapsed.as_secs_f64() * 1000.0;
    match resp {
        Response::Ok(payload) => {
            let msg = String::from_utf8_lossy(payload);
            if msg.is_empty() {
                println!("OK ({:.2}ms)", ms);
            } else {
                println!("{} ({:.2}ms)", msg, ms);
            }
        }
        Response::Pong(_) => println!("Pong ({:.2}ms)", ms),
        Response::KeyValue(val) => {
            let val_str = String::from_utf8_lossy(val);
            println!("\"{}\" ({:.2}ms)", val_str, ms);
        }
        Response::KeyNotFound(msg) => {
            let msg = String::from_utf8_lossy(msg);
            println!("(n/a) {} ({:.2}ms)", msg, ms);
        }
        Response::DbNotFound(msg) => {
            let msg = String::from_utf8_lossy(msg);
            println!("Error: {} ({:.2}ms)", msg, ms);
        }
        Response::DbAlreadyExists(msg) => {
            let msg = String::from_utf8_lossy(msg);
            println!("Error: {} ({:.2}ms)", msg, ms);
        }
        Response::InvalidRequest(msg) => {
            let msg = String::from_utf8_lossy(msg);
            println!("Error: {} ({:.2}ms)", msg, ms);
        }
        Response::PayloadTooLarge(msg) => {
            let msg = String::from_utf8_lossy(msg);
            println!("Error: {} ({:.2}ms)", msg, ms);
        }
        Response::InternalError(msg) => {
            let msg = String::from_utf8_lossy(msg);
            println!("Error: {} ({:.2}ms)", msg, ms);
        }
    }
}

#[inline]
fn print_stats_response(resp: &Response, elapsed: Duration) {
    let ms = elapsed.as_secs_f64() * 1000.0;
    if let Response::Ok(payload) = resp {
        let stats = StatsPacket::from_bytes(&mut payload.clone());

        println!("Server Stats ({:.2}ms):", ms);
        println!("  uptime:           {:.2} hours", stats.uptime_hrs);
        println!("  databases:        {}", stats.total_dbs);
        println!("  active conns:     {}", stats.total_active_connections);
        return;
    }
    print_response(resp, elapsed);
}

#[inline]
fn print_info_response(resp: &Response, elapsed: Duration, db_name: &str) {
    let ms = elapsed.as_secs_f64() * 1000.0;
    if let Response::Ok(payload) = resp {
        let info = InfoPacket::from_bytes(&mut payload.clone());

        println!("DB '{}' Info ({:.2}ms):", db_name, ms);
        println!("  keys:             {}", info.key_count);
        println!(
            "  size on disk:     {}",
            fmt_bytes(info.size_on_disk as usize)
        );
        println!(
            "  size in memory:   {}",
            fmt_bytes(info.size_in_memory as usize)
        );
        println!("  key size (min):   {} bytes", info.min_key_size);
        println!("  key size (mean):  {} bytes", info.mean_key_size);
        println!("  key size (max):   {} bytes", info.max_key_size);
        return;
    }
    print_response(resp, elapsed);
}
