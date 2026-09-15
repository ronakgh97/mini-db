### High-performance Async Write-Ahead Log DB

> Note: I AM BUILDING THIS PROJECT, BECAUSE I FUMBLED TO ANSWER INTERVIEW QUESTIONS ABOUT TOKIO, SO FUCK IT, I BALL!!!.

**High-level design**

![Design](mini-db-design.png)

**Network protocol**

- Request

```rust
#[repr(C)]
pub enum Operation {
    Stats,
    Ping,
    Close,
    Create,
    Info,
    Drop,
    Get,
    Set,
    Delete,
}
```

STATS/PING/CLOSE/CREATE/INFO/DROP/GET/SET/DELETE = 0x00/0x01/0x02/0x03/0x04/0x05/0x06/0x07/0x08 -
`(1B op|1B db_name_len|4B key_length|4B value_length|db_name|key|value|)` (Little Endian)

- Response

```rust
#[repr(C)]
pub enum Response {
    Ok(Bytes),
    Pong(Bytes),
    DbNotFound(Bytes),
    DbAlreadyExists(Bytes),
    KeyValue(Bytes),
    KeyNotFound(Bytes),
    InvalidRequest(Bytes),
    PayloadTooLarge(Bytes),
    InternalError(Bytes),
}
```

OK/Pong/DbNotFound/DbAlreadyExists/KeyValue/KeyNotFound/InvalidRequest/PayloadTooLarge/InternalError =
0x00/0x01/0x02/0x03/0x04/0x05/0x06/0x07/0x08 -
`(1B status|4B value_length|value|)` (Little Endian)

**Benchmarks**

```terminaloutput
mini-bench --workload read-overwrite

Mini-db e2e benchmark
  server:    127.0.0.1:8787
  db:        default
  workload:  ReadOverwrite
  clients:   32
  ops:       256000
  keyspace:  16384
  key/value: 16/128 bytes
  mix:       80% GET / 20% SET
  warmup:    1024

Prefilling 16384 keys... done in 1.94s
Warming up with 1024 operations... done in 14.02ms
Running 256000 sampled operations

Results
  completed:  256000
  elapsed:    3.677 s
  throughput: 69620 ops/s
  min:        18.400 us
  mean:       458.567 us
  p50:        278.600 us
  p90:        875.400 us
  p95:        929.900 us
  p99:        1.083 ms
  p99.9:      1.352 ms
  max:        2.178 ms
```

**TODO**

- Multiple DB workers (Sharding and Synchronization)
- Faster to memory index for read operations (Rwlock)
- Multiple segmented WALs
- Fsync infrequently and group commits
- Multi-tenancy and proper userspace