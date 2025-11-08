# Rollblock Architecture

## Overview

Rollblock is a high-performance key-value storage system designed to handle hundreds of millions of operations with rollback guarantees. The architecture consists of 7 core components organized in layers.

## Components

Rollblock is organised as a set of layered crates inside `src/`:

- `api/`: errors, public types, facade modules
- `runtime/`: orchestrators and metrics slices
- `state/`: shard engine, planners and executors
- `storage/`: persistence backends (journal, metadata, snapshot, filesystem helpers)

Each layer only depends on the ones below it, which keeps rebuild surfaces small and testing targets focused.

### 1. Facade (`api/facade/`)
Single entry point for users. Implements the `StoreFacade` trait with core methods:
- `set(block_height, operations)` - applies a block of mutations
- `rollback(block_height)`
- `get(key) -> Value` (returns `0` when the key is missing)

Delegates everything to the `BlockOrchestrator`.

### 2. Orchestrator (`runtime/orchestrator/`)
Central coordinator that orchestrates operations between components:
- Manages transactions with mutex to avoid race conditions
- Coordinates: StateEngine → Journal → Metadata
- Ensures atomicity: automatic rollback on failure
- Parallelism support via Rayon (optional)

### 3. State Engine (`state/engine/`)
Operation distribution engine across shards:
- **Sharding**: Key distribution by Blake3 (seeded v1) hash across N shards
- **Prepare-Commit**: First generates a `BlockDelta`, then applies
- **Undo Planning**: Calculates undo before application to guarantee rollback
- Operation parallelization across shards (optional with Rayon)

### 4. State Shard (`state/shard/`)
Atomic storage unit:
- **Implementation**: `RawTableShard` based on `hashbrown::RawTable`
- **O(1) Complexity**: Set, Delete, Get
- **Thread-safe**: `RwLock` for concurrent access
- **Export/Import**: Full snapshot support

### 5. Block Journal (`storage/journal/`)
Operation persistence for rollback:
- `format.rs`: shared header definitions and checksum helpers
- `file.rs`: on-disk implementation with zstd compression
- `iter.rs`: reversible iterators over entries
- `maintenance.rs`: truncate/rewrite utilities for keeping journal tidy

Header structure stays unchanged:
```
magic(4) | version(2) | reserved(2) | block_height(8) |
entry_count(4) | compressed_len(8) | uncompressed_len(8) | checksum(4)
```

### 6. Metadata Store (`storage/metadata/`)
Metadata management via LMDB:
- `metadata.rs`: defines the `MetadataStore` trait plus `ShardLayout`
- `lmdb/env.rs`: environment bootstrap and database handles
- `lmdb/layout.rs`: shard layout serialization helpers
- `lmdb/durability.rs`: durability mode + map-size tracking
- `lmdb.rs`: concrete store wiring everything together with LMDB’s ACID guarantees

### 7. Snapshotter (`storage/snapshot/`)
Complete state backup:
- `format.rs`: header layout, checksum utilities
- `writer.rs`: streaming snapshot creation
- `reader.rs`: mmap-based restore path with validation
- `gc.rs`: listing, pruning and cleanup helpers shared by the runtime
- `snapshot.rs`: `Snapshotter` trait + `MmapSnapshotter` that composes the pieces

## Data Flows

### SET Flow
```
User → Facade.set(block_height, ops)
  → Orchestrator.apply_operations(block_height, ops)
    → Validation: block_height > current_block
    → StateEngine.prepare_journal(block_height, ops) → BlockDelta
    → StateEngine.commit(block_height, delta) → BlockUndo
    → BlockJournal.append(block_height, undo, ops) → JournalMeta
    → Metadata.put_journal_offset(block_height, meta)
    → Metadata.set_current_block(block_height)
```

**Key features**:
- `block_height` is provided by the caller and must be strictly greater than the current height
- Empty blocks support: if `ops` is empty, only `current_block` is updated
- Zero-value operations are treated as deletes before persistence; non-zero values are stored verbatim

On error at any step after commit:
- `StateEngine.revert(block_height, undo)` is called automatically

### ROLLBACK Flow
```
User → Facade.rollback(target_block)
  → Orchestrator.revert_to(target_block)
    → Metadata.get_journal_offsets(0..=target_block)
    → Find last block with operations <= target (actual_target)
    → Journal.iter_backwards(current, actual_target+1)
    → For each undo: StateEngine.revert(block_height, undo)
    → Metadata.set_current_block(target_block)
```

**Key features**:
- Empty block support during rollback
- If `target_block` is empty, rollback to last block with operations <= target
- Example: rollback(102) with ops at blocks 100 and 105 → reverts to state at block 100

### GET Flow
```
User → Facade.get(key)
  → Orchestrator.fetch(key)
    → StateEngine.lookup(key)
      → shard = select_shard(hash(key))
      → shard.get(key)
    ← Value (0 when the key is absent)
```

## Guarantees

### Performance
- **O(1)**: All hash table operations
- **Scalability**: Sharding avoids contention
- **Parallelism**: Rayon for simultaneous multi-shard processing

### Reliability
- **Atomicity**: Automatic rollback on error
- **Durability**: Journal + LMDB persistence
- **Integrity**: Blake3 checksums at all levels
- **Validation**: Verification of block_height, counts, lengths

### Concurrency
- **Mutex** on set: Write serialization
- **RwLock** per shard: Parallel reads possible
- **Thread-safe**: All components are Send + Sync

## Core Types

```rust
Key = [u8; 8]              // Fixed 8-byte hash
Value = u64                // 64-bit integer
BlockId = u64              // Block height (user-controlled)

Operation {                // User operation
  key: Key
  value: Value             // value == 0 deletes the key
}

BlockDelta {               // Execution plan
  block_height: BlockId
  shards: Vec<ShardDelta>
}

BlockUndo {                // Rollback information
  block_height: BlockId
  shard_undos: Vec<ShardUndo>
}
```

## Extensibility

All critical components are traits:
- `StoreFacade`: Public interface
- `BlockOrchestrator`: Coordination logic
- `StateEngine`: Execution engine
- `StateShard`: Storage unit
- `BlockJournal`: Persistence system
- `MetadataStore`: Metadata store
- `Snapshotter`: Backup system

This allows replacing any component with an alternative implementation.
