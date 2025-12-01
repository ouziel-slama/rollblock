# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1] - 2025-12-01

### Fixed
- Remote server start/shutdown now happens on a dedicated thread, avoiding nested `Runtime::block_on` panics when rollblock is embedded inside an existing Tokio runtime.

## [0.3.0] - 2025-11-28

### Added
- `StoreFacade::pop(block_height, key)` removes a key while returning its previous value under the same orchestration lock. `MhinStoreFacade`, `MhinStoreBlockFacade`, all orchestrators, docs, and tests have been updated accordingly, and `DefaultBlockOrchestrator` now reuses undo metadata to avoid redundant lookups.

## [0.2.1] - 2025-11-28

### Fixed
- fix tests

## [0.2.0] - 2025-11-28

### Breaking Changes
- **Read-only mode removed**: `StoreMode::ReadOnly`, `ReadOnlyBlockOrchestrator`, and all `open_read_only` variants have been removed. Use the new client/server model for read replicas instead.
- **Values are now byte arrays**: Values changed from `u64` to arbitrary `Vec<u8>` (max 65,535 bytes). Empty vectors represent deletes, missing keys return empty vectors.
- **Journal layout revamped**: `JournalMeta` now records `(chunk_id, chunk_offset)` pairs and the on-disk journal lives in chunked files (`journal.XXXXXXXX.bin`). Existing single-file journals are not compatible.

### Added
- **Remote server**: New `RemoteStoreServer` with TLS (via `rustls`), Basic Auth, configurable timeouts, and connection limits. Embeddable via `StoreConfig::with_remote_server(...)`.
- **Remote client**: New `RemoteStoreClient` for read replicas with automatic TLS verification, auth headers, and buffer reuse.
- **Plaintext transport**: Optional `RemoteServerSecurity::Plain` and `ClientConfig::without_tls(...)` for trusted environments.
- **Relaxed durability mode**: New `enable_relaxed_mode(n)` and `disable_relaxed_mode()` APIs for faster initial sync or catch-up. Syncs every N blocks instead of every block, significantly improving throughput at the cost of a larger crash-loss window.
- **Journal pruning**: Background pruner automatically removes old journal chunks beyond the configured rollback window. Configure via `with_min_rollback_window(blocks)` and `with_prune_interval(duration)`. Crash-safe with GC watermarks.
- **Small Value Optimization (SVO)**: Values ≤ 8 bytes are now stored inline without heap allocation, significantly improving performance for small payloads like `u64`.
- `network_client` example for client/server usage.
- Journal chunking with atomic rotation, directory fsyncs, and crash-safe discovery.
- `StoreConfig::with_journal_chunk_size(bytes)` for configuring the per-chunk size (default 128 MiB).

### Fixed
- Snapshot scheduling under sustained load: new `max_snapshot_interval` ensures snapshots are forced when the persistence queue never fully drains.

### Changed
- Store directories now use exclusive locking only (no more shared locks).
- `end_block` failures are now fatal: the store becomes unhealthy and requires reopening.
- Internal value buffers now use `Arc<[u8]>` instead of `Vec<u8>` for cheaper cloning.
- Documentation reorganized: `docs/architecture.md`, `docs/configuration.md`, `docs/examples.md`, `docs/benchmark.md`, and `docs/observability.md`.

## [0.1.0] - 2025-11-15

### Highlights
- First public release of Rollblock with a production-ready state store, rollback-aware runtime, and documentation set.
- Supports sequential and sparse block heights, deterministic rollbacks, and high-throughput ingestion via sharded in-memory state.
- Ships dual licensing (MIT OR Apache-2.0) plus complete README, architecture notes, and usage guides.

### Storage Engine & API
- `StoreConfig` builders for LMDB map sizing, allowing deployments to set metadata databases well beyond the default 2 GB to avoid `MDB_MAP_FULL`.
- Simplified mutation semantics: `Operation` now carries only `key` and `value`, with `value = 0` acting as a delete tombstone; `get()` returns `0` when a key is absent.
- Comprehensive examples (`basic_usage.rs`, `blockchain_reorg.rs`, `parallel_processing.rs`, `sparse_blocks.rs`) to showcase ingestion, reorg handling, and performance tuning.
- Rich Rustdoc comments and module-level overviews for every public interface.

### Durability, Recovery & Observability
- Default LMDB metadata map size increased from 1 GB to 2 GB so Bitcoin mainnet + testnets fit without manual tuning.
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
