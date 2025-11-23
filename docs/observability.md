# Observability in Rollblock

This guide covers the observability features built into Rollblock, including metrics collection, health monitoring, and structured logging.

## Overview

Rollblock provides comprehensive observability through:
- **Real-time metrics** - Performance counters and statistics
- **Health monitoring** - Store health status tracking
- **Structured logging** - Tracing integration for detailed insights

## Metrics

### Accessing Metrics

Get metrics from the store facade:

```rust
use rollblock::{MhinStoreFacade, StoreConfig};

let config = StoreConfig::new("./data", 4, 1000, 1, false).enable_remote_server()?;
let store = MhinStoreFacade::new(config)?;

// Access metrics
if let Some(metrics) = store.metrics() {
    let snapshot = metrics.snapshot();
    println!("Operations: {}", snapshot.operations_applied);
}

// Remote server metrics (only available when `enable_server` was set)
if let Some(remote) = store.remote_server_metrics() {
    println!("Active remote connections: {}", remote.active_connections);
}
```

### Available Metrics

#### Counters
- `operations_applied` - Total number of operations applied
- `set_operations_applied` - Total number of set operations applied
- `zero_value_deletes_applied` - Total number of empty-value (`Value::empty()`) delete operations applied
- `blocks_committed` - Number of blocks successfully committed
- `rollbacks_executed` - Number of rollback operations
- `lookups_performed` - Total read operations
- `failed_operations` - Number of failed operations
- `checksum_errors` - Number of data integrity errors

#### Timing Metrics (microseconds)
- `avg_apply_time_us` - Average time to apply a block
- `avg_rollback_time_us` - Average time to rollback
- `avg_lookup_time_us` - Average time for lookups

#### Percentile Metrics (microseconds)
- `apply_p50_us`, `apply_p95_us`, `apply_p99_us` - Apply latency percentiles
- `rollback_p50_us`, `rollback_p95_us`, `rollback_p99_us` - Rollback latency percentiles

#### State Metrics
- `current_block_height` *(a.k.a. `applied_block_height`)* - Highest block applied **in memory**. In asynchronous durability it can be ahead of the on-disk state.
- `durable_block_height` - Highest block durably persisted to journal + metadata.
- `total_keys_stored` - Number of keys in the store
- `last_operation_secs` - Seconds since last operation

### Metrics Snapshot

Capture a point-in-time snapshot of all metrics:

```rust
let snapshot = metrics.snapshot();

// Serialize to JSON for monitoring systems
let json = serde_json::to_string_pretty(&snapshot)?;
println!("{}", json);
```

Output example:
```json
{
  "operations_applied": 1000,
  "set_operations_applied": 900,
  "zero_value_deletes_applied": 100,
  "blocks_committed": 100,
  "rollbacks_executed": 2,
  "avg_apply_time_us": 15000,
  "apply_p95_us": 25000,
  "current_block_height": 120,
  "durable_block_height": 100,
  "failed_operations": 0
}
```

## Health Monitoring

### Health States

Rollblock reports four health states:

- **Healthy** ✅ - Store operating normally with recent activity
- **Idle** 💤 - Store is functional but has no recent operations
- **Degraded** ⚠️ - Store has experienced errors but is operational
- **Unhealthy** ❌ - Store has critical issues

### Checking Health

```rust
if let Some(health) = store.health() {
    match health.state {
        HealthState::Healthy => {
            println!("✅ Store is healthy");
            println!("Current block: {}", health.current_block);
        }
        HealthState::Degraded => {
            println!("⚠️ Store has issues:");
            println!("  Failed operations: {}", health.failed_operations);
            println!("  Checksum errors: {}", health.checksum_errors);
        }
        HealthState::Unhealthy => {
            println!("❌ Store is unhealthy - investigate immediately");
        }
        HealthState::Idle => {
            println!("💤 Store is idle");
        }
    }
}
```

### Health Criteria

A store is considered:
- **Healthy** if: No errors AND recent activity (< 60 seconds)
- **Idle** if: No errors AND no recent activity
- **Degraded** if: Has checksum errors but otherwise functional
- **Unhealthy** if: Has failed operations

## Structured Logging with Tracing

Rollblock uses the `tracing` crate for structured, leveled logging.

### Enabling Tracing

Initialize the tracing subscriber in your application:

```rust
fn main() {
    // Basic setup
    tracing_subscriber::fmt::init();

    // Or with environment filter
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    // Your code here
}
```

### Log Levels

Control verbosity with the `RUST_LOG` environment variable:

```bash
# Info level (default) - Major operations only
RUST_LOG=rollblock=info cargo run

# Debug level - Detailed operation traces
RUST_LOG=rollblock=debug cargo run

# Trace level - Very detailed, including lookups
RUST_LOG=rollblock=trace cargo run
```

### Log Output Examples

**INFO level** - Block operations:
```
2025-11-07T22:47:07.328991Z INFO apply_operations: Block applied successfully 
  block_height=1 operations=10 modified_keys=10 duration_ms=25
```

**DEBUG level** - Internal steps:
```
2025-11-07T22:47:07.328991Z DEBUG apply_operations: Acquiring mutation mutex
2025-11-07T22:47:07.329123Z DEBUG apply_operations: Current block retrieved current_block=0
2025-11-07T22:47:07.329234Z DEBUG apply_operations: Preparing journal
2025-11-07T22:47:07.329567Z DEBUG apply_operations: State committed operations=10 modified_keys=10
```

**TRACE level** - Individual lookups:
```
2025-11-07T22:47:07.364123Z TRACE fetch: Lookup performed 
  key=[1,0,0,0,0,0,0,0] found=true duration_us=42
```

### Structured Fields

All logs include structured fields that can be parsed by log aggregators:

- `block_height` - Block number
- `ops_count` - Number of operations
- `operations` - Operations processed
- `modified_keys` - Keys affected
- `duration_ms` / `duration_us` - Timing information
- `target_block` - Rollback target
- `offset` - Journal offset
- `compressed_len` - Compressed data size

### JSON Output

For production environments, output logs as JSON:

```rust
tracing_subscriber::fmt()
    .json()
    .with_env_filter("rollblock=info")
    .init();
```

Output:
```json
{
  "timestamp": "2025-11-07T22:47:07.328991Z",
  "level": "INFO",
  "fields": {
    "message": "Block applied successfully",
    "block_height": 1,
    "operations": 10,
    "modified_keys": 10,
    "duration_ms": 25
  },
  "target": "rollblock::orchestrator"
}
```

## Integration with Monitoring Systems

### Prometheus

Export metrics to Prometheus:

```rust
use rollblock::metrics::MetricsSnapshot;

fn export_prometheus(snapshot: &MetricsSnapshot) -> String {
    format!(
        r#"
# HELP rollblock_operations_total Total operations applied
# TYPE rollblock_operations_total counter
rollblock_operations_total {{}} {}

# HELP rollblock_blocks_total Total blocks committed
# TYPE rollblock_blocks_total counter
rollblock_blocks_total {{}} {}

# HELP rollblock_apply_duration_microseconds Block apply duration
# TYPE rollblock_apply_duration_microseconds summary
rollblock_apply_duration_microseconds{{quantile="0.5"}} {}
rollblock_apply_duration_microseconds{{quantile="0.95"}} {}
rollblock_apply_duration_microseconds{{quantile="0.99"}} {}
"#,
        snapshot.operations_applied,
        snapshot.blocks_committed,
        snapshot.apply_p50_us,
        snapshot.apply_p95_us,
        snapshot.apply_p99_us
    )
}
```

### Health Check Endpoint

Implement a health check endpoint:

```rust
use rollblock::metrics::HealthState;

fn health_check(store: &MhinStoreFacade) -> (u16, &'static str) {
    match store.health() {
        Some(health) => match health.state {
            HealthState::Healthy => (200, "OK"),
            HealthState::Idle => (200, "OK - Idle"),
            HealthState::Degraded => (503, "Service Degraded"),
            HealthState::Unhealthy => (503, "Service Unhealthy"),
        },
        None => (500, "Metrics Unavailable"),
    }
}
```

## Performance Considerations

### Metrics Overhead

Metrics collection has minimal overhead:
- Counter increments: ~5-10 nanoseconds
- Timestamp recording: ~50-100 nanoseconds
- Percentile calculation: Only on snapshot (not per-operation)

### Tracing Overhead

Tracing overhead varies by level:
- **INFO**: <1% overhead (only major operations)
- **DEBUG**: ~1-2% overhead
- **TRACE**: ~5-10% overhead (logs every lookup)

For production, use INFO or DEBUG level only.

### Best Practices

1. **Sample traces in production** - Use `RUST_LOG=rollblock=info` by default
2. **Export metrics periodically** - Take snapshots every 10-60 seconds
3. **Monitor health continuously** - Check health status every 5-10 seconds
4. **Alert on degradation** - Set up alerts for `Degraded` or `Unhealthy` states
5. **Track percentiles** - P95 and P99 latencies reveal performance issues

## Example: Complete Observability Setup

```rust
use rollblock::{MhinStoreFacade, StoreConfig};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("rollblock=info")
        .init();

    // Create store
    let config = StoreConfig::new("./data", 4, 1000, 1, false);
    let store = MhinStoreFacade::new(config)?;

    // Start metrics exporter thread
    let store_clone = store.clone();
    std::thread::spawn(move || {
        loop {
            if let Some(metrics) = store_clone.metrics() {
                let snapshot = metrics.snapshot();
                println!("Metrics: ops={} blocks={} p95={}μs",
                    snapshot.operations_applied,
                    snapshot.blocks_committed,
                    snapshot.apply_p95_us);
            }
            std::thread::sleep(Duration::from_secs(10));
        }
    });

    // Start health checker thread
    let store_clone = store.clone();
    std::thread::spawn(move || {
        loop {
            if let Some(health) = store_clone.health() {
                if health.state != rollblock::metrics::HealthState::Healthy {
                    eprintln!("⚠️ Health alert: {:?}", health);
                }
            }
            std::thread::sleep(Duration::from_secs(5));
        }
    });

    // Your application logic here
    Ok(())
}
```

## Troubleshooting

### No Metrics Available

If `store.metrics()` returns `None`, check:
- You're using `MhinStoreFacade` (custom orchestrators may not have metrics)
- The store was initialized correctly

### Unexpected Health State

**Degraded state:**
- Check `checksum_errors` counter
- Verify disk integrity
- Review journal files for corruption

**Unhealthy state:**
- Check `failed_operations` counter
- Review error logs with `RUST_LOG=debug`
- Ensure sufficient disk space

### High Latencies

If P95/P99 latencies are high:
1. Check operation batch sizes
2. Verify shard count is appropriate
3. Consider enabling parallelism
4. Monitor disk I/O performance

## See Also

- [Architecture Documentation](store_architecture.md)
- [Usage Guide](store_usage.md)
- [Observability Example](../examples/observability.rs)

