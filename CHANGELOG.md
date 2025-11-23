# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Embedded remote server runtime now exposes a configurable worker thread count via `RemoteServerSettings::with_worker_threads(...)` and defaults to `std::thread::available_parallelism()` so TLS handshakes, accept loops, and client tasks no longer contend on a single Tokio worker.
- `MhinStoreFacade::applied_block()` now returns `StoreResult<BlockId>` and verifies the store health before reporting the in-memory height, aligning it with the `StoreFacade` contract.
- Documentation clarifies the difference between applied vs durable block heights and updates observability metrics to spell out that `current_block_height` reflects the applied (potentially non-durable) height.
- `StoreConfig` now carries an `enable_server` flag (default `false`). The embedded remote server only starts when this flag is set (or when `.with_remote_server(...)` is used), and `MhinStoreFacade::new` still handles the Tokio runtime lifecycle automatically. Defaults bind to `127.0.0.1:9443` with Basic Auth `proto`/`proto` and plaintext transport whenever TLS material is not provided.
- Added `StoreConfig::with_remote_server(...)`, `.without_remote_server()`, and `RemoteServerSettings` builder helpers to customize bind address, TLS, Basic Auth, timeouts, and connection limits.
- `MhinStoreFacade::remote_server_metrics()` exposes the server-side counters without requiring callers to hold a separate `RemoteServerHandle`.
- `MhinStoreError` reports remote server initialization/join failures via dedicated variants so callers get actionable diagnostics.
- `MhinStoreBlockFacade::end_block` now treats every failure as a fatal durability error: the staged block is dropped, the orchestrator is marked unhealthy, and all future operations return `MhinStoreError::DurabilityFailure` until the store is reopened. Documentation and tests cover the new behavior, plus a regression e2e test ensures `BlockIdNotIncreasing` halts the facade.
- `StoreConfig::enable_remote_server()` now returns `StoreResult<StoreConfig>` and surfaces `MhinStoreError::RemoteServerConfigMissing` when the remote server settings were removed via `.without_remote_server()`, preventing a silent "enabled without parameters" state.
- Snapshot scheduling now enforces a new `StoreConfig::max_snapshot_interval` (defaulting to the configured `snapshot_interval`). When asynchronous persistence runs under sustained load and queue drains never fully idle, the orchestrator blocks new writes long enough to flush pending work and force a snapshot so the journal cannot grow unbounded.

## [0.2.0] - 2025-11-21

### Highlights
- Introduced an authenticated remote server (`RemoteStoreServer`) plus a zero-allocation Rust client (`RemoteStoreClient`) for remote read replicas with TLS enabled by default.
- Simplified the core store life cycle by removing the `StoreMode::ReadOnly` branch; data directories now use a single exclusive lock, eliminating undefined states between writers and shadow readers.

### Storage & API
- Removed `StoreMode`, `ReadOnlyBlockOrchestrator`, and every `open_read_only` variant. `StoreConfig::existing` now always opens stores in read/write mode with exclusive locking.
- `StoreLockGuard` no longer differentiates shared/exclusive modes and always holds the directory lock exclusively. Tests adjusted to reflect the new semantics.
- Added `net::server` and `client` modules, including `RemoteServerConfig`, `RemoteServerHandle`, `ClientConfig`, and `ClientError`, all re-exported from the crate root.
- `MhinStoreError` dropped the `ReadOnlyOperation` variant; protocol violations now surface through the remote server error codes.

### Networking
- Defined a compact binary protocol (1-byte header, `N × 8` byte payload/response) with deterministic error codes and end-of-connection semantics.
- Server supports TLS via `rustls`, Basic Authentication, configurable timeouts, per-connection metrics, and a concurrency limiter.
- Client automatically handles TLS verification, Basic Auth headers, reusable buffers, and read/write timeouts.
- Added integration tests (`tests/network.rs`) that validate TLS handshakes, happy-path reads, and authentication failures using self-signed certificates.
- Added optional plaintext transport for trusted environments via `RemoteServerSecurity::Plain` and `ClientConfig::without_tls(...)`.

### Tooling & Docs
- README now documents the protocol, TLS/auth configuration, server deployment snippet, client usage, and a migration guide covering the removal of read-only mode.
- Added a `network_client` example demonstrating end-to-end server/client wiring.
- `Cargo.toml` now includes `tokio`, `tokio-rustls`, `rustls`, `rustls-pemfile`, `base64`, and `rcgen` (dev) to support the networking stack.

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

