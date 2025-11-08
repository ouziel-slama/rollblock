# Testing Rollblock

This guide covers how to execute the automated test suite for Rollblock with a focus on the end-to-end (E2E) scenarios introduced in the rollout plan.

## Quick Start

- Run all fast checks: `cargo test`
- Run the full E2E integration crate with captured logs: `cargo test --test e2e -- --nocapture`
- Include the heavier benchmarks when needed: `cargo test --test e2e --features slow-tests -- --nocapture`

## End-to-End Scenarios

- Helpers live in `tests/common/e2e_support.rs`; they provide temporary store fixtures, tracing, and durability polling.
- The `tests/e2e` integration crate (`tests/e2e/mod.rs`) wires themed modules such as `core.rs`, `durability.rs`, `concurrency.rs`, `observability.rs`, and `resilience.rs`. Cargo still executes individual modules in parallel.
- Run every test in a module: `cargo test --test e2e core:: -- --nocapture`
- Run an individual scenario: `cargo test --test e2e core::e2e_basic_lifecycle -- --nocapture`

## Slow & Optional Scenarios

- The `slow-tests` Cargo feature gates long-running checks such as `e2e_large_batch_bounds`.
- These tests are ignored by default; enable them explicitly with `cargo test --test e2e --features slow-tests -- --nocapture`.

## Logging & Diagnostics

- Tracing is enabled on demand via `RUST_LOG` (e.g., `RUST_LOG=rollblock=debug cargo test --test e2e core:: -- --nocapture`).
- Temporary data directories are created under `target/testdata` and cleaned up automatically when each test fixture drops.

## Troubleshooting

- **Permission denied or read-only journal paths**: confirm the working directory is writable and that the `journal` subdirectory under `target/testdata` inherits correct permissions.
- **`DataDirLocked` errors**: ensure no other test or process is holding the store open. Removing stale directories under `target/testdata` typically clears the lock.
- **Slow test flakiness**: when running with `--features slow-tests`, rerun with `RUST_LOG=rollblock=debug` for more insight or temporarily raise `DEFAULT_TIMEOUT` in `tests/common/e2e_support.rs`.

