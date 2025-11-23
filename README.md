# Rollblock

[![Crates.io](https://img.shields.io/crates/v/rollblock.svg)](https://crates.io/crates/rollblock)
[![Docs.rs](https://docs.rs/rollblock/badge.svg)](https://docs.rs/rollblock)
[![Tests](https://github.com/ouziel-slama/rollblock/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/ouziel-slama/rollblock/actions/workflows/test.yml)
[![Coverage](https://codecov.io/gh/ouziel-slama/rollblock/branch/main/graph/badge.svg)](https://codecov.io/gh/ouziel-slama/rollblock)
[![Clippy](https://github.com/ouziel-slama/rollblock/actions/workflows/clippy.yml/badge.svg?branch=main)](https://github.com/ouziel-slama/rollblock/actions/workflows/clippy.yml)
[![Rustfmt](https://github.com/ouziel-slama/rollblock/actions/workflows/fmt.yml/badge.svg?branch=main)](https://github.com/ouziel-slama/rollblock/actions/workflows/fmt.yml)

A super-fast, rollbackable block-oriented key-value store.

## Features

- Nearly as fast as a hashbrown HashMap in RAM and purpose-built for blockchain-style workloads.
- Block-scoped, atomic updates keyed by monotonically increasing `BlockId`s.
- zstd-compressed undo journal entries protected by Blake3 checksums.
- Memory-mapped snapshots with checksum validation for fast restarts.
- Configurable sharding with optional Rayon-based parallel execution.
- Zero-as-delete semantics and a block-staging facade for complex workflows.
- Built-in metrics, health reporting, and structured tracing hooks.
- Authenticated remote server (TLS optional) and a zero-allocation Rust client for remote queries.

## Benchmark Snapshot (Nov 2025)

Benchmark: ~1.52B operations replayed via `block_benchmark` on an Apple M4 Mac (24 GB RAM).

- `Async, multi-threads` (reference): ≈1.40M ops/s.
- `Async, single-threaded`: ≈1.27M ops/s (1.1x slower than reference).
- `Synchronous, multi-threads`: ≈0.80M ops/s (1.7x slower).
- `Synchronous, single-threaded`: ≈0.82M ops/s (1.7x slower).
- `LMDB baseline`: ≈0.04M ops/s (35x slower).

See `docs/block_benchmark.md` for the complete methodology, hardware specs, and additional notes.

## Data Model

- `Key = [u8; 8]`: fixed-size keys (hash higher-level identifiers if needed).
- `Value = u64`: 64-bit values stored per key.
- `Operation` batches carry `key` and `value`; setting `value = 0` removes a key.
- `BlockId = u64`: block heights must strictly increase; each block is applied atomically.

## Installation

Pull it directly from crates.io (or add via `cargo add rollblock`):

```toml
[dependencies]
rollblock = "0.2"
```

## Quick Start

```rust
use rollblock::{MhinStoreFacade, StoreConfig, StoreResult};
use rollblock::types::Operation;

fn main() -> StoreResult<()> {
    let config = StoreConfig::new("./data/basic", 4, 1000, 1, false);
    let store = MhinStoreFacade::new(config)?;

    let key = [1u8, 0, 0, 0, 0, 0, 0, 0];
    store.set(
        1,
        vec![Operation {
            key,
            value: 100,
        }],
    )?;

    let value = store.get(key)?;
    if value > 0 {
        println!("Value at block 1: {value}");
    }

    // Prefer multi_get when requesting more than one key.
    let batch = store.multi_get(&[key, [2u8; 8]])?;
    println!("Batch read returned {:?}", batch);

    store.rollback(0)?;
    store.close()?;
    Ok(())
}
```

`multi_get` shares the same zero-fill semantics as `get` but amortizes lock
contention and remote round-trips. Prefer it whenever you have more than one key.

⚠️ Remote access is opt-in. Call `.enable_remote_server()` (or
`.with_remote_server(...)`) before building the store if you want the embedded
server. It binds to `127.0.0.1:9443` with plaintext `proto`/`proto` credentials
unless you override the `RemoteServerSettings`.

## Block-Staged Workflow

Use `MhinStoreBlockFacade` to stage operations while exposing intermediate reads:

```rust
use rollblock::{MhinStoreBlockFacade, StoreConfig, StoreResult};
use rollblock::types::Operation;

fn staged_block() -> StoreResult<()> {
    let facade = MhinStoreBlockFacade::new(StoreConfig::new("./data/staged", 4, 1000, 1, false))?;

    facade.start_block(42)?;
    facade.set(Operation {
        key: [9u8; 8],
        value: 77,
    })?;
    assert_eq!(facade.get([9u8; 8])?, 77);
    assert_eq!(facade.multi_get(&[[9u8; 8], [1u8; 8]])?, vec![77, 0]);
    facade.end_block()?;
    Ok(())
}
```

> **Failure mode:** Any error returned by `end_block` is treated as a fatal
> durability failure. The staged block is dropped, the underlying facade is
> marked unhealthy, and every subsequent call fails until you reopen the store.
> **That includes application-level `pre_process` / `process` hooks: if they
> bubble up an error, the store is deliberately bricked. Make sure they only
> fail when stopping the store is the correct outcome.**

`MhinStoreBlockFacade::multi_get` mirrors the base facade, but merges staged
operations before falling back to the committed state.

## Configuration


- `data_dir`: base directory containing `metadata/`, `journal/`, and `snapshots/`.
- `shards`: number of in-memory shards (4–16 is a good starting point).
- `initial_capacity`: initial per-shard capacity; growth is automatic afterward.
- `thread_count`: `1` for sequential execution, `>1` to enable Rayon-backed parallelism.
- `use_compression`: enable zstd compression for the journal (default `false`).
- `enable_server`: opt-in flag that starts the embedded remote server (default `false`).
  `.with_remote_server(...)` flips this on automatically.
- `remote_server`: optional `RemoteServerSettings` describing the embedded server
  (defaults to `127.0.0.1:9443`, plaintext, `proto`/`proto` credentials) and only
  take effect when `enable_server` is `true`.

### Advanced Configuration

For high-throughput blockchains, you can customize the LMDB metadata size:

```rust
let config = StoreConfig::new("./data", 4, 1000, 1, false)
    .with_lmdb_map_size(10 << 30); // 10GB for high-frequency chains
```

**Default**: 2GB (sufficient for Bitcoin mainnet and all testnets)
**Recommended for Ethereum/Polygon**: 10GB
**High-frequency chains**: 20GB+

### Remote Server Settings

```rust
use rollblock::RemoteServerSettings;

let tls_config = RemoteServerSettings::default()
    .with_bind_address("0.0.0.0:9443".parse()?)
    .with_basic_auth("replica", "super-secret")
    .with_tls("/etc/rollblock/server.crt", "/etc/rollblock/server.key")
    .with_worker_threads(4);

let config = StoreConfig::new("./data", 4, 1000, 1, false)
    .with_remote_server(tls_config);

// Use default settings but still expose the server
let default_server = StoreConfig::new("./data", 4, 1000, 1, false).enable_remote_server()?;

// Remove settings entirely for tests
let headless = StoreConfig::new("./data", 4, 1000, 1, false).without_remote_server();
```

If you do not supply certificate/key paths, the server listens over plaintext.
Whenever TLS paths are set, HTTPS is enforced automatically.

`with_worker_threads(N)` controls how many Tokio worker threads the embedded
server dedicates to TLS handshakes, accept loops, and connection tasks. The
default matches `std::thread::available_parallelism()` (minimum `1`), so the
server scales with the host's CPU count but can be tuned lower for constrained
environments or higher when the remote server is the primary workload.

> ⚠️ **Security warning:** calling `.enable_remote_server()` without overriding
> `RemoteServerSettings` starts a plaintext server on `127.0.0.1:9443` with the
> default `proto` / `proto` credentials. Always change the credentials and turn
> on TLS before exposing it outside local development.

### Deployment Notes

- Only **one** process can open a data directory at a time. `rollblock.lock` is now enforced as an exclusive file lock—spawn additional read replicas through the remote server instead of opening the directory again.
- Remote consumers should use the remote server + client described below so that the primary writer maintains authoritative metadata. TLS is recommended on any untrusted network; plaintext mode should be limited to trusted, internal deployments.

## Remote Server & Client

### Binary Protocol

- Requests start with a single `u8` header containing the number of keys (`1‥=255`).
- Payload = `N × 8` bytes (`[Key; N]`): keys are raw `[u8; 8]`. Bytes are not interpreted—hash or encode upstream IDs into 8 bytes.
- Responses = `N × 8` bytes (`[u64; N]`) encoded little-endian (`Value == 0` means “absent”).
- Error handling: the server replies with a single byte code and immediately closes the connection. Current codes are `1 = invalid header`, `2 = invalid payload`, `3 = store failure`, `4 = unauthorized`, `5 = timeout`.

> **Example:** sending `0x02` as the header means “two keys are about to follow”. Exactly `2 × 8 = 16` payload bytes must be transmitted before the server can respond.

### Security & Basic Authentication

- Certificates/keys are standard PEM. Feed their paths into `RemoteServerSettings::with_tls` (see below), or export them via environment variables and construct the settings from those values.

```
ROLLBLOCK_REMOTE_CERT=/etc/rollblock/server.crt
ROLLBLOCK_REMOTE_KEY=/etc/rollblock/server.key
ROLLBLOCK_REMOTE_USER=replica
ROLLBLOCK_REMOTE_PASSWORD=super-secret
```

- Basic Auth uses the canonical `Authorization: Basic base64("user:password")` header. The server validates the first ASCII line (after the optional TLS handshake) before it accepts binary requests and replies with a single `0x00` “ready” byte. Any other byte at this stage is an error code and the connection is closed immediately.
- TLS can be disabled by leaving `RemoteServerSettings::tls` unset (the default). Plaintext mode should be confined to trusted networks.

### Embedded Server (Opt-In)

```rust
use rollblock::{MhinStoreFacade, RemoteServerSettings, StoreConfig};

let server_settings = RemoteServerSettings::default()
    .with_bind_address("0.0.0.0:9443".parse()?)
    .with_basic_auth("replica", "super-secret")
    .with_tls("/etc/rollblock/server.crt", "/etc/rollblock/server.key");

let config = StoreConfig::existing("./data")
    .with_remote_server(server_settings);
let store = MhinStoreFacade::new(config)?;
```

Once `enable_server` is `true` (either via `.enable_remote_server()` or
`.with_remote_server(...)`), `MhinStoreFacade::new` spins up a dedicated Tokio
runtime, starts the listener, and shuts it down from `close()`/`Drop`. Call
`store.remote_server_metrics()` to grab `ServerMetricsSnapshot` on demand. Leave
the flag off (the default) or call `.without_remote_server()` for networking-free
unit tests.

### Rust Client

```rust
use std::time::Duration;
use rollblock::client::{ClientConfig, RemoteStoreClient};
use rollblock::net::BasicAuthConfig;

let auth = BasicAuthConfig::new("replica", "super-secret");
let config = ClientConfig::new("my-server.local", "./tls/root-ca.pem", auth)
    .with_timeout(Duration::from_secs(2));

let mut client = RemoteStoreClient::connect("my-server.local:9443", config)?;
let balance = client.get_one([0u8; 8])?;
let batch = client.get(&[[0u8; 8], [1u8; 8]])?;
client.close()?;
```

```rust
// Plaintext example for trusted networks only
let auth = BasicAuthConfig::new("proto", "proto"); // matches the defaults
let config = ClientConfig::without_tls(auth).with_timeout(Duration::from_secs(2));
let mut client = RemoteStoreClient::connect("127.0.0.1:9444", config)?;
```

The client automatically reuses request/response buffers, performs Basic Auth, and enforces an optional read/write timeout via `set_{read,write}_timeout`.

### Manual Control (Optional)

If you prefer to manage the runtime yourself, the lower-level `RemoteStoreServer`
API remains available:

```rust
use rollblock::net::{BasicAuthConfig, RemoteServerConfig, RemoteServerSecurity, RemoteStoreServer};

let server = RemoteStoreServer::new(store.clone(), RemoteServerConfig {
    bind_address: "0.0.0.0:9443".parse()?,
    security: RemoteServerSecurity::Plain,
    auth: BasicAuthConfig::new("replica", "pass"),
    max_connections: 256,
    request_timeout: Duration::from_secs(2),
    client_idle_timeout: Duration::from_secs(10),
})?;
tokio::spawn(async move {
    server.run_until_shutdown(tokio::signal::ctrl_c()).await.unwrap();
});
```

## API Surface

- `MhinStoreFacade`: thread-safe facade for committing blocks (`set`, `rollback`, `get`, `metrics`, `health`, `current_block`, `close`).
- `MhinStoreBlockFacade`: staging facade exposing `start_block`, `set`, `end_block`, staged reads, and rollback.
- `StoreFacade`: trait implemented by both facades for dependency injection and testing.
- `MhinStoreError` / `StoreResult`: error handling primitives returned by all fallible operations.
- Embedded remote server controls via `RemoteServerSettings`, plus `RemoteStoreServer` / `RemoteServerConfig` for advanced manual hosting.
- `RemoteStoreClient` / `ClientConfig`: blocking client that speaks the binary protocol with automatic Basic Auth and optional TLS validation.

## Persistence Pipeline

- Undo journal entries are compressed with zstd and verified with Blake3 checksums.
- Snapshots are memory-mapped binary images with embedded Blake3 checksum validation.
- Startup flow: load latest snapshot (if any), replay remaining journal entries, resume at last committed block.
- `close()` triggers a fresh snapshot so the next start avoids journal replay.

## Observability

- `store.metrics()` (always `Some` for the default orchestrator) exposes `StoreMetrics::snapshot()` with counters, averages, and P50/P95/P99 latencies.
- `store.health()` provides `HealthStatus` with `HealthState::{Healthy, Idle, Degraded, Unhealthy}` for alerting.
- `store.remote_server_metrics()` returns `ServerMetricsSnapshot` whenever the embedded server is enabled, so you can scrape remote connection stats without wiring a custom handle.
- Enable structured tracing with `RUST_LOG=rollblock=debug` to see block application, rollback, and snapshot events.

## Migration Guide

- `StoreMode::ReadOnly` and the shared-lock path have been removed. All deployments must either (a) run a full writer instance or (b) proxy reads through the remote server (TLS recommended) and the new `RemoteStoreClient`.
- The filesystem lock (`rollblock.lock`) is always exclusive. Clone `MhinStoreFacade` handles inside the same process if you need multiple threads, but never start a second process against the same directory.
- Replace embedded read-only workers with the networking stack: run the primary writer locally, expose it via `RemoteStoreServer`, and point remote processes to it with `RemoteStoreClient`.

## Examples

```
cargo run --example basic_usage
cargo run --example blockchain_reorg
cargo run --example parallel_processing --release
cargo run --example sparse_blocks
cargo run --example observability
cargo run --example network_client
```

Each example is documented in `examples/README.md` and covers CRUD operations, chain reorganizations, sparse blocks, parallel workloads, and observability tooling. Example runs create data under `./data/`; remove it with `rm -rf data/`.

## Development

```
cargo fmt
cargo clippy --all-targets --all-features
cargo test --all-targets
cargo check --examples
```

## License

Licensed under either of MIT or Apache-2.0 at your option.