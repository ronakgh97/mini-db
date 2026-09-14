### High-performance Async Write-Ahead Log DB

> Note: I AM BUILDING THIS PROJECT, BECAUSE I FUMBLED TO ANSWER INTERVIEW QUESTIONS ABOUT TOKIO, SO FUCK IT, I BALL!!!.

**High-level design**

![Design](mini-db-design.png)

**Network protocol**

- Request

```rust
#[repr(C)]
pub enum Operation {
    Get,
    Set,
    Delete,
    Ping,
    Close,
}
```

GET/SET/DELETE/PING/CLOSE = 0x00/0x01/0x02/0x03/0x04 -
`(1B op|4B key_length|4B value_length|key|value|)` (All are Little Endian)

- Response

```rust
#[repr(C)]
pub enum Response {
    Ok(Bytes),
    Pong(Bytes),
    KeyValue(Bytes),
    KeyNotFound(Bytes),
    InvalidRequest(Bytes),
    PayloadTooLarge(Bytes),
    InternalError(Bytes),
}
```

OK/Pong/KeyValue/KeyNotFound/InvalidRequest/PayloadTooLarge/InternalError = 0x00/0x01/0x02/0x03/0x04/0x05/0x06 -
`(1B status|4B value_length|value|)` (All are Little Endian)

**Benchmarks**

```terminaloutput
mini-bench --workload read-overwrite

Mini-db e2e benchmark
  server:    127.0.0.1:8787
  workload:  ReadOverwrite
  clients:   32
  ops:       256000
  keyspace:  16384
  key/value: 16/128 bytes
  mix:       80% GET / 20% SET
  warmup:    1024

Prefilling 16384 keys... done in 2.49s
Warming up with 1024 operations... done in 26.90ms
Running 256000 sampled operations

Results
  completed:  256000
  elapsed:    5.968 s
  throughput: 42896 ops/s
  min:        22.000 us
  mean:       744.475 us
  p50:        707.900 us
  p90:        1.215 ms
  p95:        1.277 ms
  p99:        1.422 ms
  p99.9:      1.808 ms
  max:        11.157 ms
```

**TODO**    

- Multiple DB workers (Sharding and Synchronization)
- Faster to memory index for read operations (Rwlock)
- Multiple segmented WALs
- Fsync infrequently and group commits
- Multi-tenancy and proper userspace