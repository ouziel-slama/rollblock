# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-11-15

### Highlights
- First public release of Rollblock with a production-ready state store, rollback-aware runtime, and documentation set.
- Supports sequential and sparse block heights, deterministic rollbacks, and high-throughput ingestion via sharded in-memory state.
- Ships dual licensing (MIT OR Apache-2.0) plus complete README, architecture notes, and usage guides.

### Storage Engine & API
- `StoreConfig` builders for LMDB map sizing, allowing deployments to set metadata databases well beyond the default 2 GB to avoid `MDB_MAP_FULL`.
- Simplified mutation semantics: `Operation` now carries only `key` and `value`, with `value = 0` acting as a delete tombstone; `get()` returns `0` when a key is absent.
- Comprehensive examples (`basic_usage.rs`, `blockchain_reorg.rs`, `parallel_processing.rs`, `sparse_blocks.rs`) to showcase ingestion, reorg handling, and performance tuning.
- Rich Rustdoc comments and module-level overviews for every public interface.

### Durability, Recovery & Observability
- Default LMDB metadata map size increased from 1 GB to 2 GB so Bitcoin mainnet + testnets fit without manual tuning.
- Asynchronous persistence pipeline with rollback barriers, snapshot integration, and `PersistenceSettings` presets for sync/async durability.
- Blake3-backed integrity checks, compressed zstd snapshots, and automatic rollback handling for blockchain reorganizations.
- Store metrics with latency percentiles, health classification (`Healthy`, `Idle`, `Degraded`, `Unhealthy`), and tracing instrumentation across hot paths.

### Tooling & CI
- GitHub Actions workflows for `cargo fmt`, `cargo clippy --all-targets --all-features`, unit tests, and coverage uploads to Codecov.
- Comprehensive `.gitignore`, crate metadata (`Cargo.toml` license, description, keywords, categories), and repository badges.

### Documentation
- End-to-end docs across `docs/` (usage, architecture, observability, testing, and E2E quick start).
- README with feature highlights, CI badges, and example links.
- Safety comments on all `unsafe` blocks to capture invariants and threat models.

