# Block Benchmark (November 2025)

This document captures the latest `block_benchmark` run performed on November 15, 2025. It records the hardware/software environment, workload parameters, and measured throughput so future runs can be compared apples-to-apples.

## Hardware & OS

- Model number: MC6V4FN/A (model identifier reported as `Unknown`)
- Chip: Apple M4 (10 cores: 4 performance + 6 efficiency)
- Memory: 24 GB unified
- Operating system: macOS 15 (Darwin 24.6.0)

> Serial numbers and UUIDs have been intentionally omitted.

## Toolchain & Command

- `rustc 1.90.0 (1159e78c4 2025-09-14)`
- Command: `cargo run --example block_benchmark --release`

## Workload Constants

| Setting | Value |
| --- | --- |
| Total blocks | 80,000 |
| Insertions per block | 10,000 |
| Deletions per block | 9,000 |
| Net keys per block | 1,000 |
| Total ops per block | 19,000 |
| Expected final keys | 80,000,000 |
| Expected total ops | 1,520,000,000 |
| Shards | 16 |
| Initial capacity per shard | 6,000,000 |
| Parallel thread count | 4 |
| LMDB map size | 16 GiB |

## Results

Reference scenario: `Async, multi-threads`.

| Scenario | Duration | Blocks/s | Ops/s | Relative to reference |
| --- | --- | --- | --- | --- |
| Async, multi-threads | 18m8s | 73.5 | 1,395,992 | reference |
| Async, single-threaded | 19m59s | 66.7 | 1,267,602 | 1.1x slower |
| Synchronous, multi-threads | 31m33s | 42.3 | 802,834 | 1.7x slower |
| Synchronous, single-threaded | 30m44s | 43.4 | 823,930 | 1.7x slower |
| LMDB baseline | 10h44m17s | 2.1 | 39,320 | 35x slower |

## Interpretation & Notes

- Asynchronous durability with four worker threads sustained **~73 blocks/s** (≈1.4 million ops/s) while staying crash-safe.
- Dropping to a single thread reduces throughput by roughly 9% due to the absence of parallel staging.
- Synchronous durability roughly halves throughput versus async, highlighting the cost of forcing journal fsync per block even with parallel workers.
- The LMDB baseline demonstrates that a naive per-block LMDB usage is roughly **35x slower** under this workload despite generous map sizing.
- Each run reset its target data directory before execution; ensure the `./data/` tree is disposable before re-running the benchmark.

