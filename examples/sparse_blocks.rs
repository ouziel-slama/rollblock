//! Sparse blocks example
//!
//! Demonstrates support for non-sequential block heights and
//! empty blocks (gaps in block numbering).
//!
//! Run with: cargo run --example sparse_blocks

use rollblock::types::Operation;
use rollblock::{MhinStoreFacade, StoreConfig, StoreFacade};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🎯 Sparse Blocks Example\n");

    let config = StoreConfig::new(
        "./data/sparse_example",
        4,     // 4 shards
        1000,  // initial capacity
        1,     // single thread
        false, // compression disabled for example
    );

    let store = MhinStoreFacade::new(config)?;

    println!("📖 Scenario: Blocks with large gaps in numbering\n");

    // Block 100: First operation
    println!("  Block 100: Set key_a = 100");
    let key_a = [1, 0, 0, 0, 0, 0, 0, 0];
    store.set(
        100,
        vec![Operation {
            key: key_a,
            value: 100,
        }],
    )?;

    // Block 500: Large gap (blocks 101-499 are empty/non-existent)
    println!("  Block 500: Set key_b = 500");
    let key_b = [2, 0, 0, 0, 0, 0, 0, 0];
    store.set(
        500,
        vec![Operation {
            key: key_b,
            value: 500,
        }],
    )?;

    // Block 1000: Another large gap
    println!("  Block 1000: Set key_c = 1000");
    let key_c = [3, 0, 0, 0, 0, 0, 0, 0];
    store.set(
        1000,
        vec![Operation {
            key: key_c,
            value: 1000,
        }],
    )?;

    // Block 1500: Final operation
    println!("  Block 1500: Set key_a = 1500");
    store.set(
        1500,
        vec![Operation {
            key: key_a,
            value: 1500,
        }],
    )?;

    println!("\n📊 Current state (at block 1500):");
    println!("  key_a: {}", store.get(key_a)?);
    println!("  key_b: {}", store.get(key_b)?);
    println!("  key_c: {}", store.get(key_c)?);

    // Rollback to an empty block height
    println!("\n⏪ Rollback to block 750 (empty block, between 500 and 1000)");
    store.rollback(750)?;

    println!("   ✓ Automatic rollback to last block with operations <= 750");
    println!("   ✓ State restored to block 500\n");

    println!("📊 State after rollback:");
    println!("  key_a: {} (from block 100)", store.get(key_a)?);
    println!("  key_b: {} (from block 500)", store.get(key_b)?);
    println!(
        "  key_c: {} (should be 0 - value was set at block 1000)",
        store.get(key_c)?
    );

    // Rollback to another empty block
    println!("\n⏪ Rollback to block 300 (empty block, between 100 and 500)");
    store.rollback(300)?;

    println!("   ✓ State restored to block 100\n");

    println!("📊 State after second rollback:");
    println!("  key_a: {} (from block 100)", store.get(key_a)?);
    println!(
        "  key_b: {} (should be 0 - value was set at block 500)",
        store.get(key_b)?
    );
    println!(
        "  key_c: {} (should be 0 - value was set at block 1000)",
        store.get(key_c)?
    );

    // Demonstrate empty block creation
    println!("\n✏️  Creating explicit empty blocks:");
    println!("  Block 2000: Empty block (no operations)");
    store.set(2000, vec![])?;
    println!("   ✓ Empty block created");

    println!("\n  Block 3000: Empty block (no operations)");
    store.set(3000, vec![])?;
    println!("   ✓ Empty block created");

    println!("\n  Block 4000: Set key_d = 4000");
    let key_d = [4, 0, 0, 0, 0, 0, 0, 0];
    store.set(
        4000,
        vec![Operation {
            key: key_d,
            value: 4000,
        }],
    )?;

    println!("\n⏪ Rollback to block 3000 (empty block)");
    store.rollback(3000)?;

    println!("   ✓ State preserved up to block 2000 (last non-empty block)\n");

    println!("📊 Final state:");
    println!("  key_a: {}", store.get(key_a)?);
    println!(
        "  key_d: {} (should be 0 - value was set at block 4000)",
        store.get(key_d)?
    );

    println!("\n✅ Sparse blocks example completed!");
    println!("\nKey takeaways:");
    println!("  • Block heights don't need to be sequential");
    println!("  • Gaps in block numbering are supported");
    println!("  • Empty blocks can be created explicitly");
    println!("  • Rollback to empty blocks works automatically");

    Ok(())
}
