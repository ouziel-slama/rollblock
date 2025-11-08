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

Shows how to handle blockchain reorganizations (forks) with auto-rollback:
- Building an initial chain
- Detecting and handling chain reorganizations
- Automatic state rollback
- Applying alternative chain history

**Run with:**
```bash
cargo run --example blockchain_reorg
```

**Key concepts:**
- Auto-rollback feature
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

### 5. Block Throughput Benchmark (`block_benchmark.rs`)

Benchmarks 10,000 sequential blocks:
- Each block inserts 1,000 keys and deletes 900 keys (`value = 0`)
- Net growth of 100 keys per block (1,000,000 keys total)
- Runs scenarios from fastest (async + 8 threads) to slowest (sync + single thread)
- Compares synchronous vs. asynchronous durability
- Measures single-threaded vs. parallel (8-thread) execution
- Reports total time per scenario
- Logs progress every 100 blocks

**Run with:**
```bash
cargo run --example block_benchmark --release
```
You can override the total number of blocks (default 10,000):
```bash
cargo run --example block_benchmark --release -- 2500
```

**Key concepts:**
- High-volume block application
- Durability mode comparison
- Thread-scaling impact
- Throughput measurement

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

