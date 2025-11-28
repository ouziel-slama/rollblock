//! Block throughput benchmark applying 10,000 blocks under different modes.
//!
//! Each block inserts 10,000 unique keys and deletes 9,000 keys (using value = 0),
//! resulting in a net growth of 1,000 keys per block and 20,000,000 keys total.

use heed::types::Bytes;
use heed::{EnvFlags, EnvOpenOptions};
use rollblock::types::Operation;
use rollblock::Value;
use rollblock::{DurabilityMode, MhinStoreFacade, StoreConfig, StoreFacade};
use std::env;
use std::error::Error;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::time::{Duration, Instant};

const DEFAULT_TOTAL_BLOCKS: u64 = 50_000;
const PROGRESS_INTERVAL: u64 = 100;
const INSERT_PER_BLOCK: usize = 10_000;
const DELETE_PER_BLOCK: usize = 9_000;
const NET_KEYS_PER_BLOCK: usize = INSERT_PER_BLOCK - DELETE_PER_BLOCK;
const TOTAL_OPS_PER_BLOCK: usize = INSERT_PER_BLOCK + DELETE_PER_BLOCK;
const SHARDS: usize = 16;
const INITIAL_CAPACITY_PER_SHARD: usize = 5_250_000;
const PARALLEL_THREAD_COUNT: usize = 4;
const LMDB_DATA_DIR: &str = "./data/block_benchmark_lmdb";
const LMDB_ENTRY_BYTES_ESTIMATE: usize = 128;
const REFERENCE_SCENARIO_NAME: &str = "Async relaxed, multi-threads";
const LMDB_SCENARIO_NAME: &str = "LMDB baseline";
const BYTES_PER_GIB: f64 = (1u64 << 30) as f64;

fn format_with_separator<T>(value: T) -> String
where
    T: Into<u128>,
{
    let digits = value.into().to_string();
    let len = digits.len();
    let mut formatted = String::with_capacity(len + len / 3);
    for (idx, ch) in digits.chars().enumerate() {
        if idx > 0 && (len - idx) % 3 == 0 {
            formatted.push(',');
        }
        formatted.push(ch);
    }
    formatted
}

fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    let mut result = String::new();

    if hours > 0 {
        result.push_str(&format!("{hours}h"));
    }
    if hours > 0 || minutes > 0 {
        result.push_str(&format!("{minutes}m"));
    }

    if total_seconds == 0 {
        let millis = duration.subsec_millis();
        if millis > 0 {
            result.push_str(&format!("{millis}ms"));
            return result;
        }
    }

    result.push_str(&format!("{seconds}s"));
    if result.is_empty() {
        result.push_str("0s");
    }

    result
}

struct Scenario {
    data_dir: &'static str,
    thread_count: usize,
    durability_mode: DurabilityMode,
}

enum ScenarioKind {
    Rollblock(Scenario),
    Lmdb,
}

struct ScenarioEntry {
    name: &'static str,
    kind: ScenarioKind,
}

fn build_scenarios() -> Vec<ScenarioEntry> {
    vec![
        ScenarioEntry {
            name: "Async relaxed, multi-threads",
            kind: ScenarioKind::Rollblock(Scenario {
                data_dir: "./data/block_benchmark_async_relaxed_parallel",
                thread_count: PARALLEL_THREAD_COUNT,
                durability_mode: DurabilityMode::AsyncRelaxed {
                    max_pending_blocks: 1024,
                    sync_every_n_blocks: 100,
                },
            }),
        },
        ScenarioEntry {
            name: "Async, multi-threads",
            kind: ScenarioKind::Rollblock(Scenario {
                data_dir: "./data/block_benchmark_async_parallel",
                thread_count: PARALLEL_THREAD_COUNT,
                durability_mode: DurabilityMode::Async {
                    max_pending_blocks: 1024,
                },
            }),
        },
        ScenarioEntry {
            name: "Async, single-threaded",
            kind: ScenarioKind::Rollblock(Scenario {
                data_dir: "./data/block_benchmark_async_single",
                thread_count: 1,
                durability_mode: DurabilityMode::Async {
                    max_pending_blocks: 1024,
                },
            }),
        },
        ScenarioEntry {
            name: "Synchronous, multi-threads",
            kind: ScenarioKind::Rollblock(Scenario {
                data_dir: "./data/block_benchmark_sync_parallel",
                thread_count: PARALLEL_THREAD_COUNT,
                durability_mode: DurabilityMode::Synchronous,
            }),
        },
        ScenarioEntry {
            name: "Synchronous, single-threaded",
            kind: ScenarioKind::Rollblock(Scenario {
                data_dir: "./data/block_benchmark_sync_single",
                thread_count: 1,
                durability_mode: DurabilityMode::Synchronous,
            }),
        },
        ScenarioEntry {
            name: LMDB_SCENARIO_NAME,
            kind: ScenarioKind::Lmdb,
        },
    ]
}

fn print_scenario_menu(scenarios: &[ScenarioEntry]) {
    println!("🚀 Block throughput benchmark\n");
    println!("Available scenarios:");
    for (idx, entry) in scenarios.iter().enumerate() {
        let marker = if entry.name == REFERENCE_SCENARIO_NAME {
            " (reference)"
        } else {
            ""
        };
        println!("   {}. {}{}", idx + 1, entry.name, marker);
    }
    println!();
    println!(
        "Usage: cargo run --release --example block_benchmark -- <scenario_number> [total_blocks]"
    );
    println!();
    println!("Enter scenario number (1-{}): ", scenarios.len());
}

fn main() -> Result<(), Box<dyn Error>> {
    let scenarios = build_scenarios();
    let args: Vec<String> = env::args().collect();

    let scenario_index = match args.get(1) {
        Some(arg) => {
            let index: usize = arg.parse().map_err(|_| {
                format!(
                    "Invalid scenario number `{}` (expected 1-{})",
                    arg,
                    scenarios.len()
                )
            })?;
            if index == 0 || index > scenarios.len() {
                return Err(
                    format!("Scenario number must be between 1 and {}", scenarios.len()).into(),
                );
            }
            index - 1
        }
        None => {
            print_scenario_menu(&scenarios);
            io::stdout().flush()?;

            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let index: usize = input.trim().parse().map_err(|_| {
                format!(
                    "Invalid input `{}` (expected 1-{})",
                    input.trim(),
                    scenarios.len()
                )
            })?;
            if index == 0 || index > scenarios.len() {
                return Err(
                    format!("Scenario number must be between 1 and {}", scenarios.len()).into(),
                );
            }
            index - 1
        }
    };

    let total_blocks = match args.get(2) {
        Some(arg) => {
            let value: u64 = arg.parse().map_err(|_| {
                format!(
                    "Invalid total block count `{}` (expected positive integer)",
                    arg
                )
            })?;
            if value == 0 {
                return Err("Total blocks must be greater than zero".into());
            }
            value
        }
        None => DEFAULT_TOTAL_BLOCKS,
    };

    let selected = &scenarios[scenario_index];
    let expected_final_keys = expected_final_keys(total_blocks);
    let total_expected_ops = (total_blocks as usize).saturating_mul(TOTAL_OPS_PER_BLOCK);
    let lmdb_map_size_bytes = lmdb_map_size_bytes(total_blocks);

    println!("\n🚀 Block throughput benchmark\n");
    println!("Constants:");
    println!(
        "   • DEFAULT_TOTAL_BLOCKS: {}",
        format_with_separator(DEFAULT_TOTAL_BLOCKS)
    );
    println!(
        "   • PROGRESS_INTERVAL: {}",
        format_with_separator(PROGRESS_INTERVAL)
    );
    println!(
        "   • INSERT_PER_BLOCK: {}",
        format_with_separator(INSERT_PER_BLOCK as u128)
    );
    println!(
        "   • DELETE_PER_BLOCK: {}",
        format_with_separator(DELETE_PER_BLOCK as u128)
    );
    println!(
        "   • NET_KEYS_PER_BLOCK: {}",
        format_with_separator(NET_KEYS_PER_BLOCK as u128)
    );
    println!(
        "   • TOTAL_OPS_PER_BLOCK: {}",
        format_with_separator(TOTAL_OPS_PER_BLOCK as u128)
    );
    println!("   • SHARDS: {}", format_with_separator(SHARDS as u128));
    println!(
        "   • INITIAL_CAPACITY_PER_SHARD: {}",
        format_with_separator(INITIAL_CAPACITY_PER_SHARD as u128)
    );
    println!(
        "   • PARALLEL_THREAD_COUNT: {}\n",
        format_with_separator(PARALLEL_THREAD_COUNT as u128)
    );
    println!("Target blocks: {}", format_with_separator(total_blocks));
    println!(
        "Expected final keys: {}",
        format_with_separator(expected_final_keys as u128)
    );
    println!(
        "Expected total operations: {}\n",
        format_with_separator(total_expected_ops as u128)
    );

    if matches!(selected.kind, ScenarioKind::Lmdb) {
        println!(
            "LMDB map size: {:.2} GiB ({} bytes)\n",
            (lmdb_map_size_bytes as f64) / BYTES_PER_GIB,
            format_with_separator(lmdb_map_size_bytes as u128)
        );
    }

    println!("== {} ==", selected.name);

    let duration = match &selected.kind {
        ScenarioKind::Rollblock(scenario) => run_scenario(scenario, total_blocks)?,
        ScenarioKind::Lmdb => run_lmdb_benchmark(total_blocks, lmdb_map_size_bytes)?,
    };

    let seconds = duration.as_secs_f64();
    let blocks_per_second = total_blocks as f64 / seconds;
    let ops_per_second = blocks_per_second * (TOTAL_OPS_PER_BLOCK as f64);

    println!("   • Duration: {}", format_duration(duration));
    println!("   • Throughput: {:.1} blocks/s", blocks_per_second);
    println!("   • Throughput: {:.0} ops/s", ops_per_second);
    println!();

    Ok(())
}

fn run_scenario(scenario: &Scenario, total_blocks: u64) -> Result<Duration, Box<dyn Error>> {
    clean_data_dir(scenario.data_dir)?;

    let mut config = StoreConfig::new(
        scenario.data_dir,
        SHARDS,
        INITIAL_CAPACITY_PER_SHARD,
        scenario.thread_count,
        false,
    )?
    .without_remote_server();
    config = config.with_durability_mode(scenario.durability_mode.clone());

    let store = MhinStoreFacade::new(config)?;

    let mut next_key: u64 = 0;
    let mut delete_cursor: u64 = 0;

    let start = Instant::now();

    for block in 1..=total_blocks {
        let mut operations = Vec::with_capacity(TOTAL_OPS_PER_BLOCK);

        for _ in 0..INSERT_PER_BLOCK {
            let key = next_key.to_le_bytes();
            operations.push(Operation {
                key,
                value: next_key.into(),
            });
            next_key += 1;
        }

        for _ in 0..DELETE_PER_BLOCK {
            debug_assert!(
                delete_cursor < next_key,
                "attempted to delete more keys than inserted"
            );
            let key = delete_cursor.to_le_bytes();
            delete_cursor += 1;
            operations.push(Operation {
                key,
                value: Value::empty(),
            });
        }

        store.set(block, operations)?;

        if block % PROGRESS_INTERVAL == 0 || block == total_blocks {
            let elapsed = start.elapsed();
            let blocks_per_second = (block as f64) / elapsed.as_secs_f64().max(f64::EPSILON);
            print!(
                "\r\x1b[K   • Progress: block {}/{} ({:.1} blocks/s)",
                block, total_blocks, blocks_per_second
            );
            io::stdout().flush()?;
        }
    }
    println!();

    store.ensure_healthy()?;
    store.close()?;

    let duration = start.elapsed();
    let remaining = next_key - delete_cursor;
    debug_assert_eq!(remaining as usize, expected_final_keys(total_blocks));

    Ok(duration)
}

fn run_lmdb_benchmark(
    total_blocks: u64,
    map_size_bytes: usize,
) -> Result<Duration, Box<dyn Error>> {
    clean_data_dir(LMDB_DATA_DIR)?;
    fs::create_dir_all(LMDB_DATA_DIR)?;

    let mut options = EnvOpenOptions::new();
    options.map_size(map_size_bytes);
    options.max_dbs(1);
    unsafe {
        options.flags(
            EnvFlags::WRITE_MAP | EnvFlags::MAP_ASYNC | EnvFlags::NO_SYNC | EnvFlags::NO_META_SYNC,
        );
    }
    let env = unsafe { options.open(Path::new(LMDB_DATA_DIR))? };

    let mut init_txn = env.write_txn()?;
    let db = env.create_database::<Bytes, Bytes>(&mut init_txn, Some("kv"))?;
    init_txn.commit()?;

    let mut next_key: u64 = 0;
    let mut delete_cursor: u64 = 0;

    let start = Instant::now();

    for block in 1..=total_blocks {
        let mut txn = env.write_txn()?;

        for _ in 0..INSERT_PER_BLOCK {
            let key = next_key.to_le_bytes();
            let value = next_key.to_le_bytes();
            db.put(&mut txn, &key, &value)?;
            next_key += 1;
        }

        for _ in 0..DELETE_PER_BLOCK {
            debug_assert!(
                delete_cursor < next_key,
                "attempted to delete more keys than inserted"
            );
            let key = delete_cursor.to_le_bytes();
            delete_cursor += 1;
            db.delete(&mut txn, &key)?;
        }

        txn.commit()?;

        if block % PROGRESS_INTERVAL == 0 || block == total_blocks {
            let elapsed = start.elapsed();
            let blocks_per_second = (block as f64) / elapsed.as_secs_f64().max(f64::EPSILON);
            print!(
                "\r\x1b[K   • Progress: block {}/{} ({:.1} blocks/s)",
                block, total_blocks, blocks_per_second
            );
            io::stdout().flush()?;
        }
    }
    println!();

    let duration = start.elapsed();
    let remaining = next_key - delete_cursor;
    debug_assert_eq!(remaining as usize, expected_final_keys(total_blocks));

    Ok(duration)
}

fn lmdb_map_size_bytes(total_blocks: u64) -> usize {
    let expected_keys = expected_final_keys(total_blocks).max(1);
    let needed_bytes = expected_keys.saturating_mul(LMDB_ENTRY_BYTES_ESTIMATE);
    needed_bytes
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
}

fn clean_data_dir(path: &str) -> Result<(), Box<dyn Error>> {
    if Path::new(path).exists() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

fn expected_final_keys(total_blocks: u64) -> usize {
    (total_blocks as usize) * NET_KEYS_PER_BLOCK
}
