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
rollblock = "0.1"
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

    store.rollback(0)?;
    store.close()?;
    Ok(())
}
```

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
    facade.end_block()?;
    Ok(())
}
```

## Configuration


- `data_dir`: base directory containing `metadata/`, `journal/`, and `snapshots/`.
- `shards`: number of in-memory shards (4–16 is a good starting point).
- `initial_capacity`: initial per-shard capacity; growth is automatic afterward.
- `thread_count`: `1` for sequential execution, `>1` to enable Rayon-backed parallelism.
- `use_compression`: enable zstd compression for the journal (default `false`).

### Advanced Configuration

For high-throughput blockchains, you can customize the LMDB metadata size:

```rust
let config = StoreConfig::new("./data", 4, 1000, 1, false)
    .with_lmdb_map_size(10 << 30); // 10GB for high-frequency chains
```

**Default**: 2GB (sufficient for Bitcoin mainnet and all testnets)
**Recommended for Ethereum/Polygon**: 10GB
**High-frequency chains**: 20GB+

### Read-only Mode Caveats

When opening an existing store in `StoreMode::ReadOnly`, the startup path trusts the metadata that was previously persisted. It does **not** rebuild the journal index the way the read/write path does. If the writer crashed after appending to `journal.bin` but before committing the matching offset to LMDB, those final blocks will be invisible to read-only consumers. For critical replicas, run a read/write reconciliation (or shut down cleanly) before handing the directory to a read-only process.

## API Surface

- `MhinStoreFacade`: thread-safe facade for committing blocks (`set`, `rollback`, `get`, `metrics`, `health`, `current_block`, `close`).
- `MhinStoreBlockFacade`: staging facade exposing `start_block`, `set`, `end_block`, staged reads, and rollback.
- `StoreFacade`: trait implemented by both facades for dependency injection and testing.
- `MhinStoreError` / `StoreResult`: error handling primitives returned by all fallible operations.

## Persistence Pipeline

- Undo journal entries are compressed with zstd and verified with Blake3 checksums.
- Snapshots are memory-mapped binary images with embedded Blake3 checksum validation.
- Startup flow: load latest snapshot (if any), replay remaining journal entries, resume at last committed block.
- `close()` triggers a fresh snapshot so the next start avoids journal replay.

## Observability

- `store.metrics()` (always `Some` for the default orchestrator) exposes `StoreMetrics::snapshot()` with counters, averages, and P50/P95/P99 latencies.
- `store.health()` provides `HealthStatus` with `HealthState::{Healthy, Idle, Degraded, Unhealthy}` for alerting.
- Enable structured tracing with `RUST_LOG=rollblock=debug` to see block application, rollback, and snapshot events.

## Examples

```
cargo run --example basic_usage
cargo run --example blockchain_reorg
cargo run --example parallel_processing --release
cargo run --example sparse_blocks
cargo run --example observability
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