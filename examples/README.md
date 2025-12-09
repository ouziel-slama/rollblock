# Rollblock Examples

This directory contains practical examples demonstrating various features of Rollblock.

## Running Examples

All examples can be run with:

```bash
cargo run --example <example_name>
```

For optimal performance with the parallel processing example:

```bash
cargo run --example parallel_processing --release
```

## Available Examples

### 1. Basic Usage (`basic_usage.rs`)

Demonstrates fundamental operations:
- Setting key-value pairs (create or update)
- Updating existing values
- Batch operations
- Deleting keys
- Rolling back to previous states

**Run with:**
```bash
cargo run --example basic_usage
```

**Key concepts:**
- Sequential block application
- Simple CRUD operations
- Rollback mechanics

---

### 2. Blockchain Reorganization (`blockchain_reorg.rs`)

Shows how to handle blockchain reorganizations (forks) with explicit rollback:
- Building an initial chain
- Detecting and handling chain reorganizations
- Manual state rollback using `rollback()`
- Applying alternative chain history

**Run with:**
```bash
cargo run --example blockchain_reorg
```

**Key concepts:**
- Explicit rollback for reorg handling
- Fork handling
- Chain reorganizations

---

### 3. Parallel Processing (`parallel_processing.rs`)

Performance benchmarks with parallel processing:
- Large batch set operations (100,000 operations)
- Random lookups (10,000 queries)
- Batch updates (50,000 operations)
- Batch deletes (30,000 operations)
- Rollback performance

**Run with:**
```bash
cargo run --example parallel_processing --release
```

**Key concepts:**
- Multi-threaded processing with Rayon
- Shard distribution
- Performance optimization
- Throughput measurement

---

### 4. Sparse Blocks (`sparse_blocks.rs`)

Demonstrates non-sequential block heights:
- Large gaps in block numbering
- Empty blocks (no operations)
- Rollback to empty block heights
- Explicit empty block creation

**Run with:**
```bash
cargo run --example sparse_blocks
```

**Key concepts:**
- Non-sequential block heights
- Empty blocks
- Smart rollback to last operation

---

### 5. Network Client (`network_client.rs`)

Demonstrates remote store access via the built-in TCP/TLS protocol:
- Configuring the embedded TCP server with TLS and Basic Auth
- Generating self-signed TLS certificates
- Connecting a remote client
- Reading data remotely
- Accessing server metrics

**Run with:**
```bash
cargo run --example network_client
```

**Key concepts:**
- Remote store access
- TLS encryption
- Basic authentication
- Server metrics

---

### 6. Block Throughput Benchmark (`block_benchmark.rs`)

Benchmarks sequential blocks (default 50,000):
- Each block inserts 10,000 keys and deletes 9,000 keys (`value = 0`)
- Net growth of 1,000 keys per block (50,000,000 keys total by default)
- Runs scenarios from fastest (async relaxed + 4 threads) to slowest (sync + single thread)
- Compares synchronous vs. asynchronous durability modes
- Measures single-threaded vs. parallel (4-thread) execution
- Includes LMDB baseline comparison
- Reports total time per scenario
- Logs progress every 100 blocks

**Run with:**
```bash
cargo run --example block_benchmark --release
```
You can override the total number of blocks (default 50,000):
```bash
cargo run --example block_benchmark --release -- <scenario_number> [total_blocks]
# Example: cargo run --example block_benchmark --release -- 1 10000
```

**Key concepts:**
- High-volume block application
- Durability mode comparison
- Thread-scaling impact
- Throughput measurement

---

### 7. Observability (`observability.rs`)

Demonstrates metrics collection and structured logging:
- Initializing tracing subscriber for structured logs
- Tracking operations (sets, deletes, lookups)
- Accessing performance metrics (avg, p50, p95, p99)
- Health status monitoring
- Exporting metrics as JSON

**Run with:**
```bash
cargo run --example observability
# For detailed traces:
RUST_LOG=debug cargo run --example observability
```

**Key concepts:**
- Tracing integration
- Metrics collection and percentiles
- Health monitoring
- JSON export for monitoring systems

---

## Understanding the Output

Each example includes detailed output showing:
- Configuration parameters
- Operations being performed
- Current state after each operation
- Performance metrics (where applicable)
- Verification of expected behavior

## Cleaning Up

Examples create data directories under `./data/`. To clean up:

```bash
rm -rf data/
```

## Further Reading

- [Usage Guide](../docs/store_usage.md) - Detailed usage documentation
- [Architecture](../docs/store_architecture.md) - Internal design and components
- [README](../README.md) - Project overview and quick start

