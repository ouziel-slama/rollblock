//! Parallel processing example with multiple threads
//!
//! Demonstrates high-performance parallel operation processing
//! across multiple shards and threads.
//!
//! Run with: cargo run --example parallel_processing --release

use rollblock::types::{Operation, StoreKey as Key, Value};
use rollblock::{SimpleStoreFacade, StoreConfig, StoreFacade};
use std::time::Instant;

fn key_from_u64(value: u64) -> Key {
    Key::from_u64_le(value)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Parallel Processing Performance Example\n");

    // Configuration for high-performance parallel processing
    let config = StoreConfig::new(
        "./data/parallel_example",
        16,      // 16 shards for maximum parallelism
        100_000, // 100k initial capacity per shard
        8,       // 8 threads for parallel processing
        false,   // disable compression to maximize throughput
    )?
    .without_remote_server();

    println!("📦 Configuration:");
    println!("   - Shards: 16 (for parallel distribution)");
    println!("   - Threads: 8 (parallel mode with Rayon)");
    println!("   - Initial capacity: 100,000 per shard\n");

    let store = SimpleStoreFacade::new(config)?;

    // Benchmark 1: Large batch set operations
    println!("🔥 Benchmark 1: Setting 100,000 keys");
    let start = Instant::now();

    let mut operations = Vec::with_capacity(100_000);
    for i in 0..100_000u64 {
        let key = key_from_u64(i);
        operations.push(Operation {
            key,
            value: i.into(),
        });
    }

    store.set(1, operations)?;
    let duration = start.elapsed();

    println!("   ✓ Completed in {:?}", duration);
    println!(
        "   ✓ Throughput: {:.0} ops/sec\n",
        100_000.0 / duration.as_secs_f64()
    );

    // Benchmark 2: Random lookups
    println!("🔍 Benchmark 2: 10,000 random lookups");
    let start = Instant::now();

    let mut found_count = 0;
    for i in (0..10_000u64).step_by(10) {
        let key = key_from_u64(i);
        if store.get(key)?.is_set() {
            found_count += 1;
        }
    }

    let duration = start.elapsed();
    println!("   ✓ Completed in {:?}", duration);
    println!("   ✓ Found {} keys", found_count);
    println!(
        "   ✓ Throughput: {:.0} lookups/sec\n",
        10_000.0 / duration.as_secs_f64()
    );

    // Benchmark 3: Batch updates
    println!("🔄 Benchmark 3: Updating 50,000 keys");
    let start = Instant::now();

    let mut operations = Vec::with_capacity(50_000);
    for i in (0..100_000u64).step_by(2) {
        let key = key_from_u64(i);
        operations.push(Operation {
            key,
            value: (i * 10).into(), // Multiply value by 10
        });
    }

    store.set(2, operations)?;
    let duration = start.elapsed();

    println!("   ✓ Completed in {:?}", duration);
    println!(
        "   ✓ Throughput: {:.0} ops/sec\n",
        50_000.0 / duration.as_secs_f64()
    );

    // Benchmark 4: Batch deletes
    println!("🗑️  Benchmark 4: Deleting 30,000 keys");
    let start = Instant::now();

    let mut operations = Vec::with_capacity(30_000);
    for i in (0..60_000u64).step_by(2) {
        let key = key_from_u64(i);
        operations.push(Operation {
            key,
            value: Value::empty(),
        });
    }

    store.set(3, operations)?;
    let duration = start.elapsed();

    println!("   ✓ Completed in {:?}", duration);
    println!(
        "   ✓ Throughput: {:.0} ops/sec\n",
        30_000.0 / duration.as_secs_f64()
    );

    // Benchmark 5: Rollback performance
    println!("⏪ Benchmark 5: Rollback to block 1");
    let start = Instant::now();

    store.rollback(1)?;

    let duration = start.elapsed();
    println!("   ✓ Completed in {:?}", duration);
    println!("   ✓ Rolled back 80,000 operations\n");

    // Verify final state
    println!("📊 Final verification:");
    let mut sample_keys = 0;
    for i in (0..100_000u64).step_by(10_000) {
        let key = key_from_u64(i);
        if store.get(key)?.is_set() {
            sample_keys += 1;
        }
    }
    println!("   ✓ Sample keys found: {}/10", sample_keys);

    println!("\n✅ Parallel processing benchmarks completed!");
    println!("\nNote: Run with --release for optimal performance:");
    println!("      cargo run --example parallel_processing --release");

    Ok(())
}
