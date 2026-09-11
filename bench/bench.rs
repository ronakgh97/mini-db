use anyhow::{Context, Result, ensure};
use clap::{Parser, ValueEnum};
use mini_db::protocol::Response;
use mini_db::wal::HEADER_LEN;
use rand::rngs::SmallRng;
use rand::{Rng, RngExt, SeedableRng, rng};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

const OP_GET: u8 = 0;
const OP_SET: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Workload {
    Read,
    Write,
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

    let kv_space = Arc::new(KeySpace::init(
        args.keyspace,
        args.key_size,
        args.value_size,
    )?);

    // assume fresh DB, so SET does not need to prefill keys, it's not ReadOverwrite
    if args.workload != Workload::Write {
        print!("Prefilling {} keys...", args.keyspace);
        let start = Instant::now();
        kv_space
            .fill_db_server(&args.server_addr)
            .await
            .context("failed to prefill db")?;
        println!(" done in {:.2?}", start.elapsed());
    }

    let mut clients = Vec::with_capacity(args.clients);
    for _ in 0..args.clients {
        clients.push(Client::connect(&args.server_addr, &kv_space).await?);
    }

    let workload = args.workload;
    let read_percent = args.read_percent;

    print!("Warming up with {} operations...", args.warmup);
    let start = Instant::now();
    let mut warmup_tasks = JoinSet::new();
    for (id, mut client) in clients.into_iter().enumerate() {
        let space = Arc::clone(&kv_space);
        let operations = share_workload(args.warmup, args.clients, id);

        warmup_tasks.spawn(async move {
            let _ = client
                .run(&space, workload, read_percent, operations)
                .await
                .with_context(|| format!("warming up client {id}"))?;

            Ok::<_, anyhow::Error>((id, client))
        });
    }

    let mut ready_clients = Vec::with_capacity(args.clients);
    while let Some(result) = warmup_tasks.join_next().await {
        ready_clients.push(result.context("warmup task panicked")??);
    }
    println!(" done in {:.2?}", start.elapsed());

    // wait for all clients to complete the warmup phase before starting the benchmark
    let start_barrier = Arc::new(Barrier::new(args.clients + 1));
    let mut tasks = JoinSet::new();

    for (id, mut client) in ready_clients {
        let space = Arc::clone(&kv_space);
        let barrier = Arc::clone(&start_barrier);
        let operations = share_workload(args.operations, args.clients, id);

        tasks.spawn(async move {
            barrier.wait().await;

            let lat_ns = client
                .run(&space, workload, read_percent, operations)
                .await
                .with_context(|| format!("running client {id}"))?;

            Ok::<_, anyhow::Error>((operations, lat_ns, Instant::now()))
        });
    }

    println!("Running {} sampled operations", args.operations);

    // start measured run
    let started = Instant::now();
    start_barrier.wait().await;

    let mut completed = 0;
    let mut finished = started;
    let mut lat_ns = Vec::with_capacity(args.operations);

    while let Some(result) = tasks.join_next().await {
        let (operations, mut task_lat, client_finished) =
            result.context("benchmark task panicked")??;

        completed += operations;
        finished = finished.max(client_finished);
        lat_ns.append(&mut task_lat);
    }

    let elapsed = finished.duration_since(started);
    ensure!(
        lat_ns.len() == completed,
        "recorded {} ops, expected {}",
        lat_ns.len(),
        completed
    );
    lat_ns.sort_unstable();

    let total: u128 = lat_ns.iter().map(|&n| u128::from(n)).sum();
    let mean = total / lat_ns.len() as u128;

    println!();
    println!("Results");
    println!("  completed:  {completed}");
    println!("  elapsed:    {:.3} s", elapsed.as_secs_f64());
    println!(
        "  throughput: {:.0} ops/s",
        completed as f64 / elapsed.as_secs_f64()
    );
    println!("  min:        {}", fmt_ns(u128::from(lat_ns[0])));
    println!("  mean:       {}", fmt_ns(mean));
    println!(
        "  p50:        {}",
        fmt_ns(u128::from(percentile(&lat_ns, 0.50)))
    );
    println!(
        "  p90:        {}",
        fmt_ns(u128::from(percentile(&lat_ns, 0.90)))
    );
    println!(
        "  p95:        {}",
        fmt_ns(u128::from(percentile(&lat_ns, 0.95)))
    );
    println!(
        "  p99:        {}",
        fmt_ns(u128::from(percentile(&lat_ns, 0.99)))
    );
    println!(
        "  p99.9:      {}",
        fmt_ns(u128::from(percentile(&lat_ns, 0.999)))
    );
    println!(
        "  max:        {}",
        fmt_ns(u128::from(*lat_ns.last().expect("latencies are non-empty")))
    );

    Ok(())
}

fn print_config(args: &Args) {
    println!("Mini-db e2e benchmark");
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

#[inline(always)]
fn share_workload(total: usize, clients: usize, id: usize) -> usize {
    total / clients + usize::from(id < total % clients)
}

struct KeySpace {
    keys: Box<[u8]>,
    key_size: usize,
    values: Box<[u8]>,
    value_size: usize,
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
        })
    }

    #[inline(always)]
    fn get_key(&self, index: usize) -> &[u8] {
        let start = index * self.key_size;
        &self.keys[start..start + self.key_size]
    }

    #[inline(always)]
    fn get_value(&self, index: usize) -> &[u8] {
        let start = index * self.value_size;
        &self.values[start..start + self.value_size]
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.keys.len() / self.key_size
    }

    async fn fill_db_server(&self, server_addr: &str) -> Result<()> {
        let mut socket = TcpStream::connect(server_addr).await?;
        socket.set_nodelay(true)?;

        for i in 0..self.len() {
            let key = self.get_key(i);
            let value = self.get_value(i);

            socket.write_u8(OP_SET).await?;
            socket.write_u32_le(key.len() as u32).await?;
            socket.write_u32_le(value.len() as u32).await?;
            socket.write_all(key).await?;
            socket.write_all(value).await?;

            match Response::read_response(&mut socket).await? {
                Response::Ok(_) => {}
                response => anyhow::bail!("prefill SET {i} failed: {response:?}"),
            }
        }

        Ok(())
    }
}

struct Client {
    socket: TcpStream,
    rng: SmallRng,
    read_buf: Vec<u8>,
}

impl Client {
    async fn connect(server_addr: &str, space: &KeySpace) -> Result<Self> {
        let socket = TcpStream::connect(server_addr).await?;
        socket.set_nodelay(true)?;

        Ok(Self {
            socket,
            rng: SmallRng::from_rng(&mut rng()),
            read_buf: Vec::with_capacity(HEADER_LEN + space.key_size + space.value_size),
        })
    }

    async fn run(
        &mut self,
        space: &KeySpace,
        workload: Workload,
        read_percent: u8,
        operations: usize,
    ) -> Result<Vec<u64>> {
        let mut lat_ns = Vec::with_capacity(operations);

        for _ in 0..operations {
            let key_index = self.rng.random_range(0..space.len());
            let key = space.get_key(key_index);

            let is_get = match workload {
                Workload::Read => true,
                Workload::Write => false,
                Workload::ReadOverwrite => self.rng.random_range(0u8..100) < read_percent,
            };

            if is_get {
                encode_get(&mut self.read_buf, key);
            } else {
                let value_index = self.rng.random_range(0..space.len());
                encode_set(&mut self.read_buf, key, space.get_value(value_index));
            }

            let started = Instant::now();
            self.socket.write_all(&self.read_buf).await?;
            check_response(&mut self.socket, is_get).await?;
            lat_ns.push(started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
        }

        Ok(lat_ns)
    }
}

#[inline(always)]
fn encode_get(buffer: &mut Vec<u8>, key: &[u8]) {
    buffer.clear();
    buffer.push(OP_GET);
    buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buffer.extend_from_slice(key);
}

#[inline(always)]
fn encode_set(buffer: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    buffer.clear();
    buffer.push(OP_SET);
    buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buffer.extend_from_slice(key);
    buffer.extend_from_slice(value);
}

#[inline(always)]
async fn check_response(socket: &mut TcpStream, is_get: bool) -> Result<()> {
    match Response::read_response(socket).await? {
        Response::Ok(_) if !is_get => Ok(()),
        Response::KeyValue(_) if is_get => Ok(()),
        response => anyhow::bail!(
            "{} failed: {response:?}",
            if is_get { "GET" } else { "SET" }
        ),
    }
}

#[inline(always)]
fn percentile(sorted: &[u64], q: f64) -> u64 {
    let rank = (q * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[inline(always)]
fn fmt_ns(ns: u128) -> String {
    if ns >= 1_000_000_000 {
        format!("{:.3} s", ns as f64 / 1_000_000_000.0)
    } else if ns >= 1_000_000 {
        format!("{:.3} ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.3} us", ns as f64 / 1_000.0)
    } else {
        format!("{ns} ns")
    }
}
