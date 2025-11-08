//! Basic usage example demonstrating core operations
//!
//! Run with: cargo run --example basic_usage

use rollblock::types::Operation;
use rollblock::{MhinStoreFacade, StoreConfig, StoreFacade};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Rollblock Basic Usage Example\n");

    // Setup: Create a simple configuration
    let config = StoreConfig::new(
        "./data/basic_example", // data directory
        4,                      // 4 shards
        1000,                   // initial capacity per shard
        1,                      // single thread (sequential mode)
        false,                  // disable compression for demo
    );

    println!("📦 Creating store with configuration:");
    println!("   - Data dir: ./data/basic_example");
    println!("   - Shards: 4");
    println!("   - Initial capacity: 1000 per shard\n");

    let store = MhinStoreFacade::new(config)?;

    // Example 1: SET a new key-value pair
    println!("✏️  Block 1: Setting key [1,0,0,0,0,0,0,0] = 100");
    let key = [1, 0, 0, 0, 0, 0, 0, 0];
    store.set(1, vec![Operation { key, value: 100 }])?;

    // Verify the value
    let value = store.get(key)?;
    if value == 0 {
        println!("   ✗ Key not found\n");
    } else {
        println!("   ✓ Value confirmed: {}\n", value);
    }

    // Example 2: SET an existing key
    println!("✏️  Block 2: Setting key [1,0,0,0,0,0,0,0] = 200");
    store.set(2, vec![Operation { key, value: 200 }])?;

    let value = store.get(key)?;
    if value == 0 {
        println!("   ✗ Key not found\n");
    } else {
        println!("   ✓ Updated value: {}\n", value);
    }

    // Example 3: Batch operations
    println!("✏️  Block 3: Batch set multiple keys");
    let operations = vec![
        Operation {
            key: [2, 0, 0, 0, 0, 0, 0, 0],
            value: 300,
        },
        Operation {
            key: [3, 0, 0, 0, 0, 0, 0, 0],
            value: 400,
        },
        Operation {
            key: [4, 0, 0, 0, 0, 0, 0, 0],
            value: 500,
        },
    ];
    store.set(3, operations)?;
    println!("   ✓ Set 3 keys\n");

    // Verify all keys
    for i in 1..=4 {
        let k = [i, 0, 0, 0, 0, 0, 0, 0];
        let v = store.get(k)?;
        if v > 0 {
            println!("   Key {:?} = {}", k, v);
        } else {
            println!("   Key {:?} = (not found)", k);
        }
    }
    println!();

    // Example 4: ROLLBACK to previous state
    println!("⏪ Rolling back to block 2...");
    store.rollback(2)?;
    println!("   ✓ Rollback complete\n");

    // Verify state after rollback
    println!("📊 State after rollback:");
    for i in 1..=4 {
        let k = [i, 0, 0, 0, 0, 0, 0, 0];
        let v = store.get(k)?;
        if v > 0 {
            println!("   Key {:?} = {}", k, v);
        } else {
            println!("   Key {:?} = (not found)", k);
        }
    }
    println!();

    // Example 5: DELETE operation
    println!("✏️  Block 4: Deleting key [1,0,0,0,0,0,0,0]");
    store.set(4, vec![Operation { key, value: 0 }])?;

    if store.get(key)? == 0 {
        println!("   ✓ Key deleted successfully\n");
    } else {
        println!("   ✗ Key still exists\n");
    }

    // Final rollback to clean state
    println!("⏪ Rolling back to block 0 (initial state)...");
    store.rollback(0)?;
    println!("   ✓ Rollback complete\n");

    println!("✅ Example completed successfully!");

    Ok(())
}
