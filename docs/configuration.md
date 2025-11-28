# Configuration Reference

## StoreConfig

| Field | Default | Builder Method | Description |
|-------|---------|----------------|-------------|
| `data_dir` | — | `new(path, ...)` | Base directory for metadata, journal, and snapshots |
| `shards_count` | — | `new(..., shards, ...)` | Number of parallel shards |
| `initial_capacity` | — | `new(..., capacity, ...)` | Initial entries per shard |
| `thread_count` | — | `new(..., threads, ...)` | 1 = sequential, >1 = parallel |
| `compress_journal` | `false` | `with_journal_compression(bool)` | Enable zstd compression |
| `journal_compression_level` | `0` | `with_journal_compression_level(i32)` | Zstd level (0–22) |
| `journal_chunk_size_bytes` | 128 MiB | `with_journal_chunk_size(u64)` | Max size before chunk rotation |
| `durability_mode` | `Async { 1024 }` | `with_durability_mode(mode)` | See durability modes below |
| `snapshot_interval` | 1 hour | `with_snapshot_interval(Duration)` | Target interval between snapshots |
| `max_snapshot_interval` | 1 hour | `with_max_snapshot_interval(Duration)` | Force snapshot after this delay |
| `lmdb_map_size` | 2 GiB | `with_lmdb_map_size(usize)` | LMDB metadata database limit |
| `min_rollback_window` | `100` | `with_min_rollback_window(BlockId)?` | Blocks kept for rollback |
| `prune_interval` | 10 s | `with_prune_interval(Duration)?` | Background pruner cadence |
| `bootstrap_block_profile` | chunk ÷ 4 KiB | `with_bootstrap_block_profile(u64)?` | Blocks-per-chunk estimate |
| `enable_server` | `false` | `enable_remote_server()?` | Start embedded TCP server |
| `remote_server` | See below | `with_remote_server(settings)` | Server configuration |

### Constructors

| Method | Usage |
|--------|-------|
| `StoreConfig::new(path, shards, capacity, threads, compress)?` | Create new store |
| `StoreConfig::existing(path)` | Reopen existing store (loads layout from metadata) |
| `StoreConfig::existing_with_lmdb_map_size(path, size)` | Reopen with explicit LMDB map size |

### Durability Modes

| Mode | Builder | Behaviour |
|------|---------|-----------|
| `Synchronous` | `with_durability_mode(DurabilityMode::Synchronous)` | fsync every block |
| `SynchronousRelaxed` | `with_durability_mode(DurabilityMode::SynchronousRelaxed { sync_every_n_blocks })` | fsync every N blocks |
| `Async` | `with_async_max_pending(n)` | Queue up to N blocks, async fsync |
| `AsyncRelaxed` | `with_async_relaxed(pending, sync_n)` | Queue + relaxed fsync cadence |

---

## RemoteServerSettings

| Field | Default | Builder Method |
|-------|---------|----------------|
| `bind_address` | `127.0.0.1:9443` | `with_bind_address(SocketAddr)` / `with_bind_port(u16)` |
| `tls` | `None` | `with_tls(cert_path, key_path)` / `without_tls()` |
| `auth` | `proto:proto` | `with_basic_auth(user, pass)` / `with_auth_config(BasicAuthConfig)` |
| `max_connections` | `512` | `with_max_connections(usize)` |
| `request_timeout` | 2 s | `with_request_timeout(Duration)` |
| `client_idle_timeout` | 10 s | `with_client_idle_timeout(Duration)` |
| `worker_threads` | CPU cores | `with_worker_threads(usize)` |

> ⚠️ The embedded server refuses to start until you override the placeholder `proto`/`proto` credentials. Call `with_basic_auth` (or `with_auth_config`) with production values before enabling it.

---

## Choosing Parameters

### shards_count — Number of Shards

Shards partition the keyspace and enable parallel writes. Each shard is an independent hash table protected by its own lock.

| Workload | Recommended | Rationale |
|----------|-------------|-----------|
| Single-threaded / small dataset | 4–8 | Low overhead, simple debugging |
| Multi-threaded server | 1–2 × CPU cores | Reduces lock contention |
| High-frequency ingestion | 32–64 | Spreads writes across more buckets |

> **Rule of thumb:** start with `num_cpus` shards and benchmark. Double if you observe lock contention in traces.

### initial_capacity — Capacity per Shard

This pre-allocates the internal hash table. Undersizing causes rehashing under load; oversizing wastes RAM.

```
capacity_per_shard = (expected_active_keys × 1.2) / shards_count
```

- **1.2×** accounts for load factor headroom (~80% fill).
- Round up to a power of two if you want predictable memory layout.

| Active Keys | Shards | Capacity per Shard |
|-------------|--------|--------------------|
| 1 M | 8 | 150 000 |
| 10 M | 16 | 750 000 |
| 100 M | 32 | 3 750 000 |

### thread_count — Parallelism

- `1` — sequential mode; no thread pool, lowest latency for small batches.
- `>1` — parallel mode; distributes shard updates across a Rayon pool.

Set to the number of CPU cores available to the process. Going higher than core count adds context-switch overhead without benefit.

### compress_journal — Journal Compression

| Setting | Trade-off |
|---------|-----------|
| `false` (default) | Maximum write throughput; larger disk footprint |
| `true` | ~30–50% smaller journal; higher CPU usage on writes |

Enable compression when disk I/O is the bottleneck (e.g., spinning disks, cloud volumes with IOPS limits). Disable on fast NVMe where CPU would become the limiter.

### min_rollback_window — Rollback Depth

Defines how many blocks the pruner must retain. Set this to at least your chain's maximum reorg depth plus a safety margin.

| Chain Profile | Recommended Window |
|---------------|--------------------|
| Bitcoin-like (rare reorgs) | 100–200 |
| Fast finality (< 10 blocks) | 50 |
| Long reorgs possible | 1 000+ |

Setting `BlockId::MAX` disables pruning entirely (useful for archival nodes or deterministic tests).

### durability_mode — Crash Safety vs Throughput

| Mode | Data-loss window | Throughput |
|------|------------------|------------|
| `Synchronous` | 0 blocks | Lowest |
| `SynchronousRelaxed { 10 }` | up to 10 blocks | Medium |
| `Async { 1024 }` | up to 1 024 blocks | High |
| `AsyncRelaxed { 1024, 100 }` | up to 1 024 queued **plus** the sync batch (≈100) | Highest |

Choose based on your durability requirements. Most blockchain indexers can afford `Async` because the chain itself is the source of truth and can be replayed. `AsyncRelaxed` adds the `sync_every_n_blocks` window on top of the queued blocks, so size both knobs according to your data-loss tolerance.

---

## Example: Production Configuration

80 M active keys on a 32-core host with NVMe storage:

```rust
use std::time::Duration;
use rollblock::{MhinStoreFacade, StoreConfig};
use rollblock::orchestrator::DurabilityMode;

let config = StoreConfig::new(
    "/var/lib/rollblock",
    32,          // 32 shards → ~2.5 M keys each
    3_000_000,   // capacity per shard (80M × 1.2 / 32)
    16,          // 16 worker threads
    false,       // no compression
)?
.with_durability_mode(DurabilityMode::Async { max_pending_blocks: 4096 })
.with_journal_chunk_size(256 << 20)         // 256 MiB chunks
.with_min_rollback_window(1000)?            // 1 000 blocks for reorgs
.with_snapshot_interval(Duration::from_secs(1800)); // snapshot every 30 min

let store = MhinStoreFacade::new(config)?;
```

---

## See Also

- [Examples](examples.md) — code samples for each facade
- [Architecture](architecture.md) — internal design, journal, pruning, snapshots

