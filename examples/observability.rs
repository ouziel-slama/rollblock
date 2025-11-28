//! Observability example demonstrating metrics and tracing
//!
//! Run with: cargo run --example observability
//! For JSON logs: RUST_LOG=rollblock=info cargo run --example observability

use rollblock::metrics::HealthState;
use rollblock::types::{Operation, Value};
use rollblock::{MhinStoreFacade, StoreConfig, StoreFacade};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber for structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    println!("🔍 Rollblock Observability Example\n");
    println!("This example demonstrates metrics collection and tracing.");
    println!("Set RUST_LOG=debug for detailed traces.\n");

    // Create store configuration
    let config = StoreConfig::new(
        "./data/observability_example",
        4,     // 4 shards
        1000,  // initial capacity
        1,     // single thread
        false, // disable compression for metrics demo
    )?
    .without_remote_server();

    let store = MhinStoreFacade::new(config)?;

    // Check initial health
    println!("📊 Initial Health Check");
    if let Some(health) = store.health() {
        print_health(&health);
    }
    println!();

    // Perform some operations
    println!("✏️  Performing operations...");

    // Block 1: Set operations (new keys)
    let mut operations = Vec::new();
    for i in 0..10 {
        operations.push(Operation {
            key: [i, 0, 0, 0, 0, 0, 0, 0],
            value: (i as u64 * 100).into(),
        });
    }
    store.set(1, operations)?;
    println!("✓ Block 1: Set 10 keys");

    // Block 2: Set operations (existing keys)
    let mut operations = Vec::new();
    for i in 0..5 {
        operations.push(Operation {
            key: [i, 0, 0, 0, 0, 0, 0, 0],
            value: (i as u64 * 200).into(),
        });
    }
    store.set(2, operations)?;
    println!("✓ Block 2: Updated 5 keys");

    // Block 3: Delete operations
    let mut operations = Vec::new();
    for i in 0..3 {
        operations.push(Operation {
            key: [i, 0, 0, 0, 0, 0, 0, 0],
            value: Value::empty(),
        });
    }
    store.set(3, operations)?;
    println!("✓ Block 3: Deleted 3 keys");

    // Perform some lookups
    println!("\n🔎 Performing lookups...");
    for i in 0..10 {
        let key = [i, 0, 0, 0, 0, 0, 0, 0];
        let _ = store.get(key)?;
    }
    println!("✓ Performed 10 lookups");

    // Rollback to block 1
    println!("\n⏪ Rolling back to block 1...");
    store.rollback(1)?;
    println!("✓ Rollback complete");

    // Display metrics
    println!("\n📈 Metrics Report");
    println!("═══════════════════════════════════════════════════");

    if let Some(metrics) = store.metrics() {
        let snapshot = metrics.snapshot();

        println!("Operations:");
        println!(
            "  • Total operations applied: {}",
            snapshot.operations_applied
        );
        println!(
            "  • Sets: {} ({} empty-value deletes)",
            snapshot.set_operations_applied, snapshot.zero_value_deletes_applied
        );
        println!("  • Blocks committed: {}", snapshot.blocks_committed);
        println!("  • Rollbacks executed: {}", snapshot.rollbacks_executed);
        println!("  • Lookups performed: {}", snapshot.lookups_performed);

        println!("\nPerformance (Averages):");
        println!("  • Apply time: {} μs", snapshot.avg_apply_time_us);
        println!("  • Rollback time: {} μs", snapshot.avg_rollback_time_us);
        println!("  • Lookup time: {} μs", snapshot.avg_lookup_time_us);

        println!("\nPerformance (Percentiles):");
        println!("  • Apply P50: {} μs", snapshot.apply_p50_us);
        println!("  • Apply P95: {} μs", snapshot.apply_p95_us);
        println!("  • Apply P99: {} μs", snapshot.apply_p99_us);
        println!("  • Rollback P50: {} μs", snapshot.rollback_p50_us);
        println!("  • Rollback P95: {} μs", snapshot.rollback_p95_us);
        println!("  • Rollback P99: {} μs", snapshot.rollback_p99_us);

        println!("\nState:");
        println!("  • Current block: {}", snapshot.current_block_height);
        println!("  • Total keys stored: {}", snapshot.total_keys_stored);

        println!("\nErrors:");
        println!("  • Failed operations: {}", snapshot.failed_operations);
        println!("  • Checksum errors: {}", snapshot.checksum_errors);

        if let Some(secs) = snapshot.last_operation_secs {
            println!("\nActivity:");
            println!("  • Last operation: {} seconds ago", secs);
        }
    }

    // Final health check
    println!("\n🏥 Final Health Check");
    println!("═══════════════════════════════════════════════════");
    if let Some(health) = store.health() {
        print_health(&health);
    }

    // Export metrics as JSON (demonstration)
    if let Some(metrics) = store.metrics() {
        let snapshot = metrics.snapshot();
        let json = serde_json::to_string_pretty(&snapshot)?;
        println!("\n📄 Metrics JSON:");
        println!("{}", json);
    }

    println!("\n✅ Example completed successfully!");
    println!("\nTip: Run with RUST_LOG=debug to see detailed tracing information.");

    Ok(())
}

fn print_health(health: &rollblock::metrics::HealthStatus) {
    let status_emoji = match health.state {
        HealthState::Healthy => "✅",
        HealthState::Idle => "💤",
        HealthState::Degraded => "⚠️",
        HealthState::Unhealthy => "❌",
    };

    println!("  {} Status: {}", status_emoji, health.state);
    println!("  • Applied block: {}", health.current_block);
    println!("  • Durable block: {}", health.durable_block);
    println!("  • Total operations: {}", health.total_operations);
    println!("  • Failed operations: {}", health.failed_operations);
    println!("  • Checksum errors: {}", health.checksum_errors);

    if let Some(secs) = health.last_operation_secs {
        println!("  • Last operation: {} seconds ago", secs);
    } else {
        println!("  • Last operation: never");
    }
}
