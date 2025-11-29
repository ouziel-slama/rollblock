# Rollblock

[![Tests](https://github.com/ouziel-slama/rollblock/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/ouziel-slama/rollblock/actions/workflows/test.yml)
[![Coverage](https://codecov.io/gh/ouziel-slama/rollblock/branch/main/graph/badge.svg)](https://codecov.io/gh/ouziel-slama/rollblock)
[![Clippy](https://github.com/ouziel-slama/rollblock/actions/workflows/clippy.yml/badge.svg?branch=main)](https://github.com/ouziel-slama/rollblock/actions/workflows/clippy.yml)
[![Rustfmt](https://github.com/ouziel-slama/rollblock/actions/workflows/fmt.yml/badge.svg?branch=main)](https://github.com/ouziel-slama/rollblock/actions/workflows/fmt.yml)
[![Crates.io](https://img.shields.io/crates/v/rollblock.svg)](https://crates.io/crates/rollblock)
[![Docs.rs](https://docs.rs/rollblock/badge.svg)](https://docs.rs/rollblock)


**A super-fast, block-oriented and rollbackable key-value store.**

---

⚡ **Super Fast** — The entire dataset lives in RAM. Under the hood, [hashbrown](https://github.com/rust-lang/hashbrown)'s `RawTable` delivers O(1) reads and writes with minimal overhead. On an Apple M4, Rollblock sustains ~1.4M ops/sec—35× faster than LMDB.

📦 **Block-Oriented** — Every mutation belongs to a block identified by a `BlockId`. When you commit, all operations succeed together or fail together. No partial state, no corruption—like SQL transactions, but designed for sequential workflows.

⏪ **Rollbackable** — An undo journal on disk lets you rewind to any previous state with a single call. Whether you roll back one block or a thousand, the store returns to that exact point in time: `store.rollback(height)?`

Built for blockchain nodes, event sourcing, game state, and any system that needs atomic commits with time-travel capabilities.

---

## Documentation

- **[Architecture](docs/architecture.md)** — Internal design, data flow, and component overview
- **[Configuration](docs/configuration.md)** — All settings, durability modes, and tuning options
- **[Examples](docs/examples.md)** — Annotated code samples for common use cases
- **[Observability](docs/observability.md)** — Metrics, health checks, and monitoring integration
- **[Benchmark](docs/benchmark.md)** — Performance methodology and results

---

## Quick Examples

### Server: Read & Write Operations

```rust
use rollblock::{MhinStoreFacade, StoreConfig, StoreFacade};
use rollblock::types::{Operation, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new store
    let config = StoreConfig::new(
        "./data/mystore",  // Data directory
        4,                 // Number of shards
        10_000,            // Initial capacity per shard
        1,                 // Thread count (1 = sequential)
        false,             // Journal compression
    )?;
    let store = MhinStoreFacade::new(config)?;

    // Write a block with multiple operations
    let key_alice = [0x01, 0, 0, 0, 0, 0, 0, 0];
    let key_bob   = [0x02, 0, 0, 0, 0, 0, 0, 0];

    store.set(1, vec![
        Operation { key: key_alice, value: Value::from_slice(b"balance:1000") },
        Operation { key: key_bob,   value: Value::from_slice(b"balance:500") },
    ])?;

    // Read values back
    let alice_balance = store.get(key_alice)?;
    println!("Alice: {}", String::from_utf8_lossy(alice_balance.as_slice()));

    // Update in a new block
    store.set(2, vec![
        Operation { key: key_alice, value: Value::from_slice(b"balance:900") },
        Operation { key: key_bob,   value: Value::from_slice(b"balance:600") },
    ])?;

    // Oops! Roll back to block 1
    store.rollback(1)?;

    // Alice's balance is restored
    let restored = store.get(key_alice)?;
    assert_eq!(restored.as_slice(), b"balance:1000");

    store.close()?;
    Ok(())
}
```

### Client: Remote Reads via Socket

Rollblock includes an embedded TCP server for read-only remote access. The client is zero-allocation and speaks a compact binary protocol.

```rust
use std::time::Duration;
use rollblock::client::{ClientConfig, RemoteStoreClient};
use rollblock::net::BasicAuthConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to a running Rollblock server
    let auth = BasicAuthConfig::new("reader", "secret-token");
    let config = ClientConfig::without_tls(auth)
        .with_timeout(Duration::from_secs(2));

    let mut client = RemoteStoreClient::connect("127.0.0.1:9443", config)?;

    // Fetch a single key
    let key = [0x01, 0, 0, 0, 0, 0, 0, 0];
    let value = client.get_one(key)?;

    if value.is_empty() {
        println!("Key not found");
    } else {
        println!("Value: {} bytes", value.len());
    }

    // Batch fetch multiple keys
    let keys = [[0x01; 8], [0x02; 8], [0x03; 8]];
    let values = client.get(&keys)?;

    for (i, v) in values.iter().enumerate() {
        println!("Key {}: {} bytes", i, v.len());
    }

    client.close()?;
    Ok(())
}
```

To enable the server on the writer side:

```rust
use rollblock::RemoteServerSettings;

let settings = RemoteServerSettings::default()
    .with_bind_address("0.0.0.0:9443".parse()?)
    .with_basic_auth("reader", "secret-token");

let config = StoreConfig::new("./data", 4, 10_000, 1, false)?
    .with_remote_server(settings);
```

---

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `data_dir` | — | Base directory for metadata, journal, and snapshots |
| `shards` | — | Number of in-memory shards (4–16 recommended) |
| `thread_count` | `1` | Set to `>1` to enable parallel execution via Rayon |
| `compress_journal` | `false` | Enable zstd compression for the undo journal |
| `min_rollback_window` | `100` | Minimum number of blocks retained for rollback |
| `lmdb_map_size` | 2 GB | LMDB metadata size (increase for high-frequency chains) |

### Durability Modes

```rust
use rollblock::orchestrator::DurabilityMode;

// Async (default): fast, acknowledges before fsync
let config = config.with_durability_mode(DurabilityMode::Async {
    max_pending_blocks: 1024
});

// Synchronous: fsync after every block (slower but safer)
let config = config.with_durability_mode(DurabilityMode::Synchronous);
```

---

## Data Model

| Type | Definition | Notes |
|------|------------|-------|
| `Key` | `[u8; 8]` | Fixed 8-byte identifier. Hash your keys upstream if needed. |
| `Value` | `Vec<u8>` | Up to 65,535 bytes. An empty value represents a deletion. |
| `BlockId` | `u64` | Block height. Must be strictly increasing. |
| `Operation` | `{ key, value }` | A single mutation within a block. |

Keys are sharded using their raw bytes interpreted as little-endian `u64`: `shard = key_as_u64 % shard_count`. Design your key space accordingly for even distribution.

---

## Limitations

Rollblock makes deliberate trade-offs. Know them before you commit:

| Limitation | Implication |
|------------|-------------|
| **Full in-RAM** | Your dataset must fit in memory. OS swap will kill performance. |
| **Single writer** | Only one process can open a data directory at a time. |
| **Read-only clients** | The remote protocol only supports `GET` operations. All writes go through the primary. |
| **Fixed key size** | Keys are exactly 8 bytes. Hash or truncate larger identifiers. |
| **Value size cap** | Maximum 65,535 bytes per value. |

---

## Running the Examples

```bash
# Basic CRUD operations
cargo run --example basic_usage

# Chain reorganization simulation
cargo run --example blockchain_reorg

# Parallel processing (release mode recommended)
cargo run --example parallel_processing --release

# Remote client/server demo
cargo run --example network_client

# Metrics and health monitoring
cargo run --example observability
```

Examples store data in `./data/`. Clean up with:

```bash
rm -rf data/
```

---

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.

