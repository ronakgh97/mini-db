use anyhow::Result;
use clap::Parser;
use mini_db::protocol::{Operation, Response};
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
    #[arg(long, default_value = "127.0.0.1:8787")]
    server_addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CliArgs::parse();
    let mut stream = TcpStream::connect(&args.server_addr).await?;
    stream.set_nodelay(true)?;

    // do ping health check
    send_request(&mut stream, Operation::Ping, &[], &[]).await?;
    if let Err(e) = Response::read_response(&mut stream).await {
        eprintln!(
            "Failed to ping mini-db server at {}: {}",
            args.server_addr, e
        );
        return Ok(());
    }

    println!("Connected to mini-db at {}", args.server_addr);
    println!("Commands: GET <key> | SET <key> <value> | DEL <key> | PING | QUIT");
    println!();

    loop {
        print!("mini-db> ");
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
                send_request(&mut stream, Operation::Close, &[], &[]).await?;
                break;
            }

            "PING" => {
                let start = Instant::now();
                send_request(&mut stream, Operation::Ping, &[], &[]).await?;
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
                let value = &[];

                let start = Instant::now();
                send_request(&mut stream, Operation::Get, key, value).await?;
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
                send_request(&mut stream, Operation::Set, key, value).await?;
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
                let value = &[];

                let start = Instant::now();
                send_request(&mut stream, Operation::Delete, key, value).await?;
                let resp = Response::read_response(&mut stream).await?;
                let elapsed = start.elapsed();
                print_response(&resp, elapsed);
            }
            _ => {
                println!("Unknown command: {}", parts[0]);
                println!("Commands: GET <key> | SET <key> <value> | DEL <key> | PING | QUIT");
            }
        }
    }

    stream.flush().await?;
    stream.shutdown().await?;

    Ok(())
}

#[inline(always)]
async fn send_request(
    stream: &mut TcpStream,
    op: Operation,
    key: &[u8],
    value: &[u8],
) -> Result<()> {
    stream.write_u8(op.to_u8()).await?;
    if !key.is_empty() || matches!(op, Operation::Get | Operation::Set | Operation::Delete) {
        stream.write_u32_le(key.len() as u32).await?;
    }
    if matches!(op, Operation::Set) {
        stream.write_u32_le(value.len() as u32).await?;
    }
    if !key.is_empty() {
        stream.write_all(key).await?;
    }
    if !value.is_empty() {
        stream.write_all(value).await?;
    }
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
