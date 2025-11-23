# Rollblock Usage Guide

## Installation

Rollblock is published at [crates.io/crates/rollblock](https://crates.io/crates/rollblock) with API docs on [docs.rs/rollblock](https://docs.rs/rollblock).

Add it via `cargo`:
```shell
cargo add rollblock
```

Or edit `Cargo.toml` manually:
```toml
[dependencies]
rollblock = "0.2"
```

> Developing inside this repository? Keep using the workspace path
> dependency (`rollblock = { path = "../rollblock" }`) so local changes are
> picked up automatically.

## Initialization

### Basic Configuration

```rust
use rollblock::*;

// Create configuration
let config = StoreConfig::new(
    "./data",     // data_dir: base directory
    4,            // shards_count: number of shards
    1000,         // initial_capacity: initial capacity per shard
    1,            // thread_count: 1 = sequential mode
    false,        // use_compression: disable compression by default
);

// Create store
let store = MhinStoreFacade::new(config)?;
```

The store automatically creates these subdirectories:
- `./data/metadata`: LMDB database for metadata
- `./data/journal`: block journal (undo + redo payloads)
- `./data/snapshots`: state snapshots

### Parallel Configuration

```rust
// Enable parallel mode by setting thread_count > 1
let config = StoreConfig::new(
    "./data",
    4,      // 4 shards
    1000,   // initial capacity
    4,      // 4 threads (parallel mode)
    false,  // compression disabled by default
);

let store = MhinStoreFacade::new(config)?;
```

## Configuration Parameters

Choose parameters based on your workload:

### 1. shards_count - Number of Shards

- **CPU cores**: Use 1-2 shards per core for optimal parallelism
- **Expected entries**: More shards = better distribution, less contention
- **Typical values**: 8, 16, or 32 shards for production

### 2. initial_capacity - Initial Capacity per Shard

- **Calculate**: `max_active_entries / shards_count`
- **Add margin**: 20-50% to avoid reallocations
- **Trade-off**: Higher = fewer reallocations but more initial RAM

### 3. thread_count - Parallelism

- `1`: Sequential mode (simple, no overhead)
- `>1`: Parallel mode (recommended for large workloads)
- Set to number of CPU cores for maximum parallelism

### 4. use_compression - Journal Compression

- `false` (default): Writes journal payloads uncompressed for maximum throughput
- `true`: Enables zstd compression to reduce disk usage; increases CPU usage
- Toggle per environment depending on available I/O vs CPU budgets

### 5. Zero-value semantics

- `Operation { value: 0 }` removes the key if it exists.
- `store.get(key)` returns `0` when the key is absent.
- Use non-zero values for persisted data; zero acts as a tombstone.

### Example: 80M Active Keys (Production)

**Scenario**: 800M operations total, ~80M live keys after compaction  
- **Target host**: dedicated 32 GB RAM, NVMe SSD  
- **Throughput goal**: absorb bursts around 100k ops/s without rehashing

**Recommended configuration**:

```rust
let config = StoreConfig::new(
    "/var/lib/rollblock",
    32,            // 32 shards → ~2.5M keys per shard
    3_000_000,     // 3M capacity per shard (≈20% headroom)
    16,            // 16 worker threads (match CPU cores)
    false,         // leave journal uncompressed for throughput
)
.with_journal_compression(false)
.with_durability_mode(DurabilityMode::Async {
    max_pending_blocks: 4_096,
});
```

**Why these values?**
- **32 shards** keep lock contention low while fitting comfortably in memory.
- **3M capacity** matches `(80M × 1.2) / 32`, preventing mid-flight rehashing.
- **16 threads** utilize a 16–24 core node without oversubscribing.
- **No compression** favors raw throughput; flip it on only if disk becomes the bottleneck.
- **Async depth 4 096** lets the persistence pipeline absorb spikes without stalling producers.

## Remote Server Opt-In

`StoreConfig::new` still carries default `RemoteServerSettings`, but the embedded
server now starts **only** when you flip the new `enable_server` flag (or call
`.with_remote_server(...)`, which enables it automatically).

```rust
use rollblock::{MhinStoreFacade, RemoteServerSettings, StoreConfig};

let server = RemoteServerSettings::default()
    .with_bind_address("0.0.0.0:9443".parse().unwrap())
    .with_basic_auth("replica", "super-secret")
    .with_tls("/etc/rollblock/server.crt", "/etc/rollblock/server.key");

let config = StoreConfig::new("./data", 4, 1000, 1, false)
    .with_remote_server(server);

// Or rely on the defaults while still exposing the server:
let default_server = StoreConfig::new("./data", 4, 1000, 1, false).enable_remote_server()?;

let store = MhinStoreFacade::new(config)?;
```

Need to keep networking off (e.g. for unit tests)? Simply skip the `.enable_remote_server()`
call—the server remains disabled. `StoreConfig::without_remote_server()` removes
the settings entirely so later builders can't accidentally opt in.

Metrics become available through `store.remote_server_metrics()` once the server
is enabled. The `ServerMetricsSnapshot` reports active connection counts, total
requests, failures, and average latency.

## Basic Operations

### SET (new key)

```rust
use rollblock::types::Operation;

let key = [1, 2, 3, 4, 5, 6, 7, 8];
let value = 42;

let op = Operation {
    key,
    value: value,
};

store.set(1, vec![op])?;
```

### SET (existing key)

```rust
let op = Operation {
    key,
    value: 100,
};

store.set(2, vec![op])?;
```

### DELETE

```rust
let op = Operation {
    key,
    value: 0, // value not required for delete
};

store.set(3, vec![op])?;
```

### GET

```rust
let value = store.get(key)?;
if value == 0 {
    println!("Key not found");
} else {
    println!("Value: {}", value);
}
```

### MULTI_GET (batched reads)

```rust
let keys = [[1u8; 8], [2u8; 8], [3u8; 8]];
let values = store.multi_get(&keys)?;
assert_eq!(values, vec![10, 0, 27]);
```

`multi_get` returns values in the same order as the provided keys and substitutes
`0` for missing rows. Internally it only acquires the read gate once, so prefer
it for any request that touches more than one key.

## Block-Staged Updates

For workflows that require staging multiple operations before committing them as a block, use `MhinStoreBlockFacade`. It exposes a transactional API with intermediate reads that reflect pending changes:

```rust
use rollblock::{MhinStoreBlockFacade, StoreConfig};
use rollblock::types::Operation;

let config = StoreConfig::new("./data", 4, 1000, 1, false);
let block_store = MhinStoreBlockFacade::new(config)?;

// Stage a block
block_store.start_block(100)?;
block_store.set(Operation {
    key: [1, 2, 3, 4, 5, 6, 7, 8],
    value: 42,
})?;

// Intermediate reads reflect staged operations
assert_eq!(block_store.get([1, 2, 3, 4, 5, 6, 7, 8])?, 42);
assert_eq!(
    block_store.multi_get(&[[1, 2, 3, 4, 5, 6, 7, 8], [9, 9, 9, 9, 9, 9, 9, 9]])?,
    vec![42, 0]
);

// Commit the staged block
block_store.end_block()?;

// Rollbacks are still available (requires no block in progress)
block_store.rollback(50)?;
```

> **Fatal failures:** If `end_block` returns an error, the pending block is
> discarded and the facade records a fatal durability error. All subsequent
> operations (including `start_block`, `set`, `get`, `rollback`, and `close`)
> will return `MhinStoreError::DurabilityFailure` until the store is reopened.

`block_store.multi_get(...)` mirrors the base facade call but merges staged
operations before consulting the committed state, so it is safe to batch reads
even while a block is in progress.

### Lifecycle Guarantees

- `start_block` fails if another block is already staged.
- `set` and `end_block` require an active block.
- `rollback` fails when a block is staged; call `end_block` first.

## Batch Operations

```rust
let operations = vec![
    Operation {
        key: [1, 0, 0, 0, 0, 0, 0, 0],
        value: 10,
    },
    Operation {
        key: [2, 0, 0, 0, 0, 0, 0, 0],
        value: 20,
    },
    Operation {
        key: [1, 0, 0, 0, 0, 0, 0, 0],
        value: 15,
    },
    Operation {
        key: [2, 0, 0, 0, 0, 0, 0, 0],
        value: 0,
    },
];

// block_height must be strictly greater than current block
store.set(1, operations)?;
```

## Rollback

### Rollback to Previous Block

```rust
// Current state: block 5
store.rollback(3)?; // Rollback to block 3
```

### Rollback to Beginning

```rust
store.rollback(0)?; // Undo all operations
```

### Sparse Blocks and Rollback

The store supports empty blocks (gaps in IDs):

```rust
use rollblock::types::Operation;

let key_a = [0xAA; 8];
let key_b = [0xBB; 8];

// Block 100: Set key_a
store.set(
    100,
    vec![Operation {
        key: key_a,
        value: 7,
    }],
)?;

// Block 105: Set key_b (blocks 101-104 are empty)
store.set(
    105,
    vec![Operation {
        key: key_b,
        value: 9,
    }],
)?;

// Rollback to block 102 (empty block)
// Store automatically rolls back to last block with operations <= 102
// i.e., block 100
store.rollback(102)?;

// State: key_a present, key_b absent (block 105 undone)
```

## Zero Value Deletes (Default)

Zero values are always interpreted as delete operations. No additional configuration is required.

### Example Workflow

```rust
let config = StoreConfig::new(
    "./data",
    4,
    1000,
    1,
    false,
);

let store = MhinStoreFacade::new(config)?;
```

### Use Case: Zero-As-Delete Streams

```rust
use rollblock::types::Operation;

let key_a = [1u8; 8];

// Ingest upstream mutation
let initial_set = Operation {
    key: key_a,
    value: 10,
};
store.set(1, vec![initial_set])?;

// Upstream emits a "set to zero" to signal deletion
let delete_marker = Operation {
    key: key_a,
    value: 0,
};
store.set(2, vec![delete_marker])?;

assert_eq!(store.get(key_a)?, 0); // treated as delete
```

### Detailed Behavior

- Setting `value = 0` removes the key; non-zero values are persisted as data.
- The delete translation happens before journaling, so checkpoints and rollbacks observe delete semantics.
- Metrics split zero-based deletes from non-zero sets for observability.

## Understanding Block Heights

Rollblock exposes three related notions of “current block”:

- `current_block()` – the last block height durably recorded in metadata. This is what reopen/recovery will start from.
- `durable_block()` – the highest block whose journal + metadata writes have completed. It should match `current_block()` but is obtained from the orchestrator and therefore reflects any in-flight flush.
- `applied_block()` – the highest block already applied in memory. In asynchronous durability modes this value can be **ahead** of the durable height while persistence catches up.

Use them together to drive your control loop:

```rust
let applied = store.applied_block()?; // highest block the state machine has executed
let durable = store.durable_block()?; // highest block safely persisted

let next_block = applied + 1;
if durable < applied {
    tracing::debug!(
        "persistence is catching up (applied {}, durable {})",
        applied,
        durable
    );
}
```

When deciding the next block height to submit, prefer `applied_block()` so you never attempt to reapply an in-memory block that is waiting for durability.

## Error Handling

```rust
use rollblock::error::MhinStoreError;

match store.set(block_height, ops) {
    Ok(()) => println!("Success at block: {}", block_height),
    Err(MhinStoreError::NoShardsConfigured) => {
        eprintln!("Error: no shards configured");
    }
    Err(MhinStoreError::BlockIdNotIncreasing { block_height, current }) => {
        eprintln!("Block height {} must be > current block {}", block_height, current);
    }
    Err(MhinStoreError::JournalChecksumMismatch { block }) => {
        eprintln!("Corruption detected at block {}", block);
    }
    Err(MhinStoreError::RollbackTargetAhead { target, current }) => {
        eprintln!("Cannot rollback forward: {} > {}", target, current);
    }
    Err(e) => eprintln!("Error: {}", e),
}
```

## Best Practices

### 1. Shard Sizing

```rust
// For 850M keys, use 64-256 shards
let shard_count = 128;
let keys_per_shard = 850_000_000 / shard_count; // ~6.6M
let capacity = keys_per_shard / 2; // load factor 0.5
```

### 2. Batch vs Single Operations

- **Batch**: Prefer for maximum throughput
- **Single**: Acceptable for minimal latency

### 3. Parallelism

```rust
// Effective if: num_shards >= num_threads
// Optimal: 8-16 threads for 64+ shards
```

### 4. Periodic Snapshots

```rust
// Snapshot every N blocks to reduce rollback time
if block_height % 10_000 == 0 {
    snapshotter.create_snapshot(block_height, &shards)?;
}
```

- In asynchronous mode the persistence queue only asks for a snapshot once it drains; a dedicated snapshot worker captures the snapshot off-thread so new blocks continue to flow.
- Because snapshots run off the hot path, keep the interval aggressive without risking durability stalls.

### 5. Graceful Shutdown and Recovery

```rust
// Flush in-memory state and capture a fresh snapshot
store.close()?;
```

- `close()` triggers a snapshot so the next restart can skip journal replay.
- On restart, the store loads the latest snapshot **and** replays every fully committed block from the journal to reach the most recent `current_block`.
- Blocks that never finished journaling are ignored, ensuring only complete blocks are restored.
- Even if the process crashes before calling `close()`, committed blocks persist thanks to the redo data stored in the journal; the replay cost grows with the number of unsnapshotted blocks.

## Complete Example

```rust
use rollblock::*;
use rollblock::types::Operation;

fn main() -> StoreResult<()> {
    // Setup
    let config = StoreConfig::new("./data", 4, 1000, 1, false);
    let store = MhinStoreFacade::new(config)?;
    
    let key = [1, 0, 0, 0, 0, 0, 0, 0];
    
    // SET (new key)
    store.set(1, vec![Operation {
        key,
        value: 100,
    }])?;
    println!("Block 1: Set key to 100");
    
    // SET (existing key)
    store.set(2, vec![Operation {
        key,
        value: 200,
    }])?;
    println!("Block 2: Updated key to 200");
    
    // GET
    let value = store.get(key)?;
    if value == 0 {
        println!("Key not found");
    } else {
        println!("Current value: {}", value); // 200
    }
    
    // ROLLBACK
    store.rollback(1)?;
    let value = store.get(key)?;
    if value == 0 {
        println!("Key not found");
    } else {
        println!("Value after rollback: {}", value); // 100
    }
    
    Ok(())
}
```
