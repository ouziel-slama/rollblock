# E2E Quick Start for New Contributors

## Purpose

Launch the end-to-end (E2E) suite quickly, understand how scenarios are laid out, and diagnose the most common failures without having to read every helper file.

## Prerequisites

- Rust toolchain `1.81` or newer with `cargo`.
- `zstd` and `pkg-config` installed (required for compression and native bindings); on macOS: `brew install zstd pkg-config`.
- Ensure your working tree is clean or commit/stash unrelated changes before running multi-process tests.

## Test Layout

- Harness utilities live in `tests/common/e2e_support.rs`.
- All scenarios compile into the single `tests/e2e` integration crate (`tests/e2e/mod.rs` wires the themed modules such as `core`, `durability`, `concurrency`, etc.).
- Run subsets by filtering module paths, e.g. `cargo test --test e2e core:: -- --nocapture` for every test in `tests/e2e/core.rs` or `cargo test --test e2e core::e2e_basic_lifecycle -- --nocapture` for a single scenario.
- Temporary data directories are created under `target/testdata`.

## Fast Path

- Run formatting, linting, and unit tests:
  - `cargo fmt`
  - `cargo clippy --all-targets --all-features`
  - `cargo test --lib`
- Smoke-test a single E2E scenario (≈10 s):
  - `cargo test --test e2e core::e2e_basic_lifecycle -- --nocapture`

## Full E2E Sweep

- Standard E2E integration suite (≈3 min on a 12‑core laptop):
  - `cargo test --test e2e -- --nocapture`
- Enable verbose tracing when chasing race conditions:
  - `RUST_LOG=rollblock=debug cargo test --test e2e -- --nocapture`

## Cleanup

- Tests automatically drop their temporary directories. If a run is interrupted, remove leftovers: `rm -rf target/testdata`.
- A stuck `DataDirLocked` error usually resolves after deleting the relevant directory under `target/testdata`.

## Common Failures & Fixes

| Message / Symptom | Likely Cause | Suggested Fix |
| --- | --- | --- |
| `DataDirLocked` on startup | Another test process still holds the store lock or a previous run crashed | Terminate lingering `cargo test` processes; delete the matching folder in `target/testdata`. |
| `Permission denied` when writing the journal | Temp directory inherited read-only permissions | Verify the working tree location is writable; recreate `target/testdata`; rerun with `sudo` **only** if your workflow requires it. |
| `health().state == Unhealthy` in metrics checks | Durability worker surfaced an error (often missing directory cleanup) | Inspect logs with `RUST_LOG=rollblock=debug`; confirm journal and snapshot paths exist; rerun after cleanup. |
| `ChecksumMismatch` during restart replay | Intentionally injected corruption or interrupted test left a partial journal | Remove the corrupted directory under `target/testdata`; rerun the scenario; ensure tests finish cleanly. |
| Suite hangs during async durability tests | Background flush queue saturated while `ensure_healthy()` waits | Increase the `max_pending_blocks` parameter in the test or run with lower CPU load; for debugging temporarily relax the assertions or raise `DEFAULT_TIMEOUT` in `tests/common/e2e_support.rs`. |

## When to Ask for Help

- The failure reproduces consistently even after cleanup.
- You see a new error message not covered above.
- You suspect a regression introduced by your changes.

Document the command you ran, attach the last 20 lines of output, and mention any local tweaks (e.g., custom data directory) before escalating. This context keeps triage fast for reviewers.

