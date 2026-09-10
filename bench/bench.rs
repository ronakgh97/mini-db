use anyhow::{Result, ensure};
use clap::{Parser, ValueEnum};
use rand::rngs::SmallRng;
use rand::{Rng, RngExt, SeedableRng, rng};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Barrier;

const OP_GET: u8 = 0;
const OP_SET: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Workload {
    Set,
    Get,
    ReadOverwrite,
}

#[derive(Debug, Parser)]
#[command(
    name = "mini-bench",
    version = "1.0.0",
    about = "End-to-end benchmark client for mini-db"
)]
struct Args {
    /// Address of an already-running mini-db server.
    #[arg(long, default_value = "127.0.0.1:8787")]
    server_addr: String,

    /// Number of persistent concurrent TCP clients.
    #[arg(long, default_value_t = 32)]
    clients: usize,

    /// Total number of operations across all clients.
    #[arg(long, default_value_t = 256_000)]
    operations: usize,

    /// Number of distinct keys used by the workload.
    #[arg(long, default_value_t = 16_384)]
    keyspace: usize,

    /// Key size in bytes.
    #[arg(long, default_value_t = 16)]
    key_size: usize,

    /// Value size in bytes.
    #[arg(long, default_value_t = 128)]
    value_size: usize,

    /// Untimed operations performed before measurement.
    #[arg(long, default_value_t = 1024)]
    warmup: usize,

    /// Workload to execute.
    #[arg(long, value_enum, default_value_t = Workload::ReadOverwrite)]
    workload: Workload,

    /// Probability of GET operations in the mixed workload.
    #[arg(long, default_value_t = 80)]
    read_percent: u8,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    validate(&args)?;
    print_config(&args);

    let mut kv_space = KeySpace::init(args.keyspace, args.key_size, args.value_size)?;

    // prefill the database server before starting the benchmark
    println!("Prefilling {} keys...", args.keyspace);
    kv_space.fill_db_server(&args.server_addr).await?;

    // TODO:

    Ok(())
}

fn print_config(args: &Args) {
    println!("mini-db e2e benchmark");
    println!("  server:    {}", args.server_addr);
    println!("  workload:  {:?}", args.workload);
    println!("  clients:   {}", args.clients);
    println!("  ops:       {}", args.operations);
    println!("  keyspace:  {}", args.keyspace);
    println!("  key/value: {}/{} bytes", args.key_size, args.value_size);
    if args.workload == Workload::ReadOverwrite {
        println!(
            "  mix:       {}% GET / {}% SET",
            args.read_percent,
            100 - args.read_percent
        );
    }
    println!("  warmup:    {}", args.warmup);
    println!();
}

fn validate(args: &Args) -> Result<()> {
    ensure!(args.clients > 0, "--clients must be greater than zero");
    ensure!(args.operations >= 128, "--operations must be at least 128");
    ensure!(args.keyspace >= 4096, "--keyspace must be at least 4096");
    ensure!(args.key_size > 0, "--key-size must be greater than zero");
    ensure!(
        args.value_size > 0,
        "--value-size must be greater than zero"
    );
    ensure!(
        args.read_percent < 100 && args.read_percent > 0,
        "--read-percent must be 0..99"
    );
    Ok(())
}

struct KeySpace {
    keys: Box<[u8]>,
    key_size: usize,
    values: Box<[u8]>,
    value_size: usize,
    rng: SmallRng,
}

impl KeySpace {
    fn init(keyspace: usize, key_size: usize, value_size: usize) -> Result<Self> {
        let mut keys = vec![0u8; keyspace * key_size];
        let mut values = vec![0u8; keyspace * value_size];

        rng().fill_bytes(&mut keys);
        rng().fill_bytes(&mut values);

        Ok(Self {
            keys: keys.into_boxed_slice(),
            key_size,
            values: values.into_boxed_slice(),
            value_size,
            rng: SmallRng::from_rng(&mut rng()),
        })
    }

    #[inline(always)]
    fn get_random_key(&mut self) -> &[u8] {
        let idx = self.rng.random_range(0..self.keys.len() / self.key_size);
        let start = idx * self.key_size;
        let end = start + self.key_size;
        &self.keys[start..end]
    }

    #[inline(always)]
    fn get_random_value(&mut self) -> &[u8] {
        let idx = self
            .rng
            .random_range(0..self.values.len() / self.value_size);
        let start = idx * self.value_size;
        let end = start + self.value_size;
        &self.values[start..end]
    }

    async fn fill_db_server(&mut self, server_addr: &str) -> Result<()> {
        let mut socket = TcpStream::connect(server_addr).await?;
        socket.set_nodelay(true)?;

        // fill db with all keys and values in the keyspace
        for i in 0..self.keys.len() {
            let key_start = i * self.key_size;
            let key_end = key_start + self.key_size;
            let key = &self.keys[key_start..key_end];

            let value_start = i * self.value_size;
            let value_end = value_start + self.value_size;
            let value = &self.values[value_start..value_end];

            socket.write_u8(OP_SET).await?;
            socket.write_u32_le(self.key_size as u32).await?;
            socket.write_all(key).await?;
            socket.write_u32_le(self.value_size as u32).await?;
            socket.write_all(value).await?;
        }

        Ok(())
    }
}
