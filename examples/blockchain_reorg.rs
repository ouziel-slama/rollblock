//! Blockchain reorganization example without auto-rollback
//!
//! Demonstrates how to handle chain reorganizations (forks) manually
//! by issuing explicit rollbacks.
//!
//! Run with: cargo run --example blockchain_reorg

use rollblock::types::Operation;
use rollblock::{MhinStoreFacade, StoreConfig, StoreFacade};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔗 Blockchain Reorganization Example\n");

    // Use default configuration
    let config = StoreConfig::new(
        "./data/blockchain_example",
        4,     // 4 shards
        1000,  // initial capacity
        1,     // single thread
        false, // disable compression for clarity
    )
    .without_remote_server();

    println!("📦 Creating store with manual reorg handling\n");
    let store = MhinStoreFacade::new(config)?;

    // Simulate initial blockchain state
    println!("📖 Building initial chain:\n");

    // Block 100: Genesis transaction
    println!("  Block 100: Alice receives 1000 coins");
    let alice = [1, 0, 0, 0, 0, 0, 0, 0];
    store.set(
        100,
        vec![Operation {
            key: alice,
            value: 1000,
        }],
    )?;

    // Block 101: Alice sends 200 to Bob
    println!("  Block 101: Alice sends 200 to Bob");
    let bob = [2, 0, 0, 0, 0, 0, 0, 0];
    store.set(
        101,
        vec![
            Operation {
                key: alice,
                value: 800, // Alice: 1000 - 200
            },
            Operation {
                key: bob,
                value: 200, // Bob: +200
            },
        ],
    )?;

    // Block 102: Bob sends 50 to Charlie
    println!("  Block 102: Bob sends 50 to Charlie");
    let charlie = [3, 0, 0, 0, 0, 0, 0, 0];
    store.set(
        102,
        vec![
            Operation {
                key: bob,
                value: 150, // Bob: 200 - 50
            },
            Operation {
                key: charlie,
                value: 50, // Charlie: +50
            },
        ],
    )?;

    // Check current state
    println!("\n📊 Current state (at block 102):");
    println!("  Alice:   {} coins", store.get(alice)?);
    println!("  Bob:     {} coins", store.get(bob)?);
    println!("  Charlie: {} coins", store.get(charlie)?);

    // Simulate a blockchain reorganization (fork)
    println!("\n⚠️  CHAIN REORGANIZATION DETECTED!\n");
    println!("  Alternative chain fork starting from block 101");
    println!("  (Different transaction history)\n");

    println!("  Rolling back to block 100 before applying alternative history...");
    store.rollback(100)?;
    println!("  ✓ Rollback completed\n");

    println!("  Block 101 (new): Alice sends 300 to David instead");
    let david = [4, 0, 0, 0, 0, 0, 0, 0];
    store.set(
        101,
        vec![
            Operation {
                key: alice,
                value: 700, // Alice: 1000 - 300
            },
            Operation {
                key: david,
                value: 300, // David: +300
            },
        ],
    )?;

    println!("  ✓ New block 101 applied\n");

    // Check state after reorganization
    println!("📊 State after reorganization (at block 101):");
    println!("  Alice:   {} coins", store.get(alice)?);
    println!("  Bob:     {} coins (should be 0)", store.get(bob)?);
    println!("  Charlie: {} coins (should be 0)", store.get(charlie)?);
    println!("  David:   {} coins", store.get(david)?);

    // Continue building on the new chain
    println!("\n  Block 102 (new): David sends 100 to Eve");
    let eve = [5, 0, 0, 0, 0, 0, 0, 0];
    store.set(
        102,
        vec![
            Operation {
                key: david,
                value: 200, // David: 300 - 100
            },
            Operation {
                key: eve,
                value: 100, // Eve: +100
            },
        ],
    )?;

    println!("\n📊 Final state (at block 102 on new chain):");
    println!("  Alice:   {} coins", store.get(alice)?);
    println!("  David:   {} coins", store.get(david)?);
    println!("  Eve:     {} coins", store.get(eve)?);

    println!("\n✅ Reorganization handled with explicit rollback!");
    println!("\nTip: Attempting to apply block 101 without rolling back first");
    println!("would return a BlockIdNotIncreasing error.");

    Ok(())
}
