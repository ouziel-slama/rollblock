//! Basic usage example demonstrating core operations
//!
//! Run with: cargo run --example basic_usage

use rollblock::types::{Operation, StoreKey as Key, Value};
use rollblock::{SimpleStoreFacade, StoreConfig, StoreFacade};

fn key(byte: u8) -> Key {
    Key::from_prefix([byte, 0, 0, 0, 0, 0, 0, 0])
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Rollblock Basic Usage Example\n");

    // Setup: Create a simple configuration
    let config = StoreConfig::new(
        "./data/basic_example", // data directory
        4,                      // 4 shards
        1000,                   // initial capacity per shard
        1,                      // single thread (sequential mode)
        false,                  // disable compression for demo
    )?
    .without_remote_server();

    println!("📦 Creating store with configuration:");
    println!("   - Data dir: ./data/basic_example");
    println!("   - Shards: 4");
    println!("   - Initial capacity: 1000 per shard\n");

    let store = SimpleStoreFacade::new(config)?;

    // Example 1: SET a new key-value pair
    println!("✏️  Block 1: Setting key [1,0,0,0,0,0,0,0] = 100");
    let key1 = key(1);
    store.set(
        1,
        vec![Operation {
            key: key1,
            value: 100.into(),
        }],
    )?;

    // Verify the value
    let value = store.get(key1)?;
    if value.is_delete() {
        println!("   ✗ Key not found\n");
    } else {
        println!("   ✓ Value confirmed: {:?}\n", value.as_slice());
    }

    // Example 2: SET an existing key
    println!("✏️  Block 2: Setting key [1,0,0,0,0,0,0,0] = 200");
    store.set(
        2,
        vec![Operation {
            key: key1,
            value: 200.into(),
        }],
    )?;

    let value = store.get(key1)?;
    if value.is_delete() {
        println!("   ✗ Key not found\n");
    } else {
        println!("   ✓ Updated value: {:?}\n", value.as_slice());
    }

    // Example 3: Batch operations
    println!("✏️  Block 3: Batch set multiple keys");
    let operations = vec![
        Operation {
            key: key(2),
            value: 300.into(),
        },
        Operation {
            key: key(3),
            value: 400.into(),
        },
        Operation {
            key: key(4),
            value: 500.into(),
        },
    ];
    store.set(3, operations)?;
    println!("   ✓ Set 3 keys\n");

    // Verify all keys
    for i in 1..=4 {
        let k = key(i);
        let v = store.get(k)?;
        if v.is_set() {
            println!("   Key {:?} = {:?}", k, v.as_slice());
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
        let k = key(i);
        let v = store.get(k)?;
        if v.is_set() {
            println!("   Key {:?} = {:?}", k, v.as_slice());
        } else {
            println!("   Key {:?} = (not found)", k);
        }
    }
    println!();

    // Example 5: DELETE operation
    println!("✏️  Block 4: Deleting key [1,0,0,0,0,0,0,0]");
    store.set(
        4,
        vec![Operation {
            key: key1,
            value: Value::empty(),
        }],
    )?;

    if store.get(key1)?.is_delete() {
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
