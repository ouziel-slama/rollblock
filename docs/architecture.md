# Rollblock Architecture

## What is Rollblock?

Rollblock is a **block-oriented, rollbackable key-value store** optimized for high-throughput blockchain and event-sourcing workloads. It maintains the entire dataset in RAM for O(1) access while persisting an undo journal to disk for crash recovery and rollback.

```
┌─────────────────────────────────────────────────────────────────┐
│                         Your Application                        │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                       StoreFacade (API)                         │
│              set() · pop() · get() · rollback()                 │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BlockOrchestrator (Runtime)                  │
│            Coordinates mutations, durability & rollback         │
└────────┬─────────────────────┬──────────────────────┬───────────┘
         │                     │                      │
         ▼                     ▼                      ▼
┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────┐
│   StateEngine   │  │   BlockJournal   │  │    MetadataStore     │
│   (in-memory)   │  │  (undo on disk)  │  │   (LMDB offsets)     │
└────────┬────────┘  └──────────────────┘  └──────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Shards (N×)                             │
│           RawTable hashmap per shard · O(1) operations          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Core Concepts

### Data Model

| Type | Size | Description |
|------|------|-------------|
| `Key` | Compile-time width (≥ 8 bytes) | Fixed-size identifier, persisted in metadata. Client/server builds must agree on the width. |
| `Value` | 0–65,535 bytes | Arbitrary payload. Empty = deletion marker. |
| `BlockId` | u64 | Block height. Strictly increasing, caller-controlled. |
| `Operation` | Key + Value | A single mutation within a block. |

### Sharding Strategy

Keys are routed using a fast hash of their raw bytes:

```
shard_index = xxh3_64(key_bytes) % shard_count
```

No additional hashing—your key bytes directly control shard placement. Design keys for even distribution.

---

## Layers

Rollblock is organized in four layers, each depending only on the ones below:

```
┌──────────────────────────────────────────────┐
│  api/         Public interface & config      │  ← You call this
├──────────────────────────────────────────────┤
│  runtime/     Orchestration & metrics        │
├──────────────────────────────────────────────┤
│  state/       In-memory shards & execution   │
├──────────────────────────────────────────────┤
│  storage/     Journal, metadata, snapshots   │  ← Persisted to disk
└──────────────────────────────────────────────┘
```

### Layer 1: API (`src/api/`)

The public surface. Users interact exclusively with the `StoreFacade` trait:

```rust
pub trait StoreFacade {
    fn set(&self, block_height: BlockId, ops: Vec<Operation>) -> Result<()>;
    fn pop(&self, block_height: BlockId, key: Key) -> Result<Value>;
    fn get(&self, key: Key) -> Result<Value>;
    fn rollback(&self, target: BlockId) -> Result<()>;
    fn close(&self) -> Result<()>;
}
```

`pop` removes a single key under the same mutation lock that `set` uses and returns the previously committed value (or `Value::empty()` if the key was absent). This avoids coordinating an extra `get` + `set` round trip just to discover what was deleted.

**Implementation**: `MhinStoreFacade` wraps everything and manages the embedded remote server.

### Layer 2: Runtime (`src/runtime/`)

**Orchestrator** — The central coordinator. Guarantees:

- **Atomicity**: All operations in a block succeed or fail together
- **Serialization**: A mutex prevents concurrent mutations
- **Auto-recovery**: Reverts in-memory state if persistence fails

**Metrics** — Real-time counters for ops/sec, latency, health status.

### Layer 3: State (`src/state/`)

**StateEngine** — Distributes operations across shards:

1. `prepare_journal()` — Plans mutations and captures undo info *before* applying
2. `commit()` — Applies the delta to shards, returns `BlockUndo`
3. `revert()` — Restores previous state using the undo

**StateShard** — Atomic storage unit (`RawTableShard`):

- Built on `hashbrown::RawTable` for minimal overhead
- `RwLock` for concurrent reads, exclusive writes
- O(1) get/set/delete

### Layer 4: Storage (`src/storage/`)

| Component | Purpose | Persistence |
|-----------|---------|-------------|
| **Journal** | Undo log for rollback | Chunked files + index |
| **Metadata** | Block heights, offsets | LMDB (ACID) |
| **Snapshot** | Full state backup | Single file per snapshot |

---

## Write Flow

When you call `store.set(block_height, operations)`:

```
┌──────────────────────────────────────────────────────────────────┐
│ 1. VALIDATE                                                      │
│    └─ block_height > current_block? ──no──▶ Error                │
└──────────────────────────────────────────────────────────────────┘
                              │ yes
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│ 2. PREPARE                                                       │
│    └─ StateEngine.prepare_journal(ops)                           │
│       • Route each op to its shard                               │
│       • Capture current values for undo                          │
│       • Return BlockDelta (execution plan)                       │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│ 3. COMMIT                                                        │
│    └─ StateEngine.commit(delta)                                  │
│       • Apply operations to shards (parallel if configured)      │
│       • Return BlockUndo                                         │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│ 4. PERSIST                                                       │
│    ├─ Journal.append(block_height, undo, ops)                    │
│    ├─ Metadata.put_journal_offset(block_height, meta)            │
│    └─ Metadata.set_current_block(block_height)                   │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                           SUCCESS

On persistence failure:
  └─ StateEngine.revert(block_height, undo)  ◀── automatic recovery
```

**Empty blocks**: If `ops` is empty, only `current_block` advances. This supports sparse block heights.

**Single-key deletes with return values**: `pop(block_height, key)` synthesizes a delete operation, runs through the same pipeline, and inspects the `BlockUndo` that `StateEngine` emits to return the previous value without issuing a separate `get`.

---

## Rollback Flow

When you call `store.rollback(target_block)`:

```
current = 105, target = 100

┌─────────────────────────────────────────────────────────────────┐
│ 1. Find journal entries between target+1 and current            │
│    └─ Blocks 101, 102, 103, 104, 105                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. Iterate backwards, apply each BlockUndo                      │
│    └─ 105 → 104 → 103 → 102 → 101                               │
│       For each: StateEngine.revert(block, undo)                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. Truncate journal after target                                │
│ 4. Update metadata: current_block = 100                         │
│ 5. Prune snapshots newer than target                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                     State restored to block 100
```

---

## Read Flow

Reads acquire the shared side of the global `reader_gate` `RwLock`, so multiple lookups can proceed concurrently while still yielding whenever a writer holds the exclusive lock. Once inside the gate, each shard only needs a read guard, which keeps lookups on different shards non-blocking.

```
store.get(key)
    │
    ▼
StateEngine.lookup(key)
    │
    ├─ shard_index = xxh3_64(key_bytes) % shard_count
    │
    └─ shard.get(key)  ◀── RwLock::read(), O(1) lookup
           │
           ▼
     Value or empty
```

---

## Durability Modes

| Mode | Sync Behavior | Throughput | Risk |
|------|---------------|------------|------|
| `Synchronous` | fsync every block | Lowest | Safest |
| `SynchronousRelaxed` | fsync every N blocks | Medium | N blocks at risk |
| `Async` | Background thread, fsync every block | High | In-flight blocks at risk |
| `AsyncRelaxed` | Background thread, fsync every N blocks | Highest | N + queue at risk |

**Default**: `Async { max_pending_blocks: 1024 }`

The `applied_block` may be ahead of `durable_block`. On crash, recovery replays from the last durable state.

---

## Storage Formats

### Journal Entry

Each journal entry stores operations and undo data for one block:

```
┌──────────────────────── HEADER (42 bytes) ────────────────────────┐
│ magic(4) │ version(2) │ flags(2) │ key_bytes(2) │ block_height(8) │
│ entry_count(4) │ compressed_len(8) │ uncompressed_len(8)          │
│ checksum(4)                                                       │
└───────────────────────────────────────────────────────────────────┘
┌──────────────────────── PAYLOAD (zstd compressed) ────────────────┐
│ bincode-encoded BlockUndo                                         │
│ + operation stream:                                               │
│   [key(key_bytes)][len(u16)][value bytes...] × entry_count        │
└───────────────────────────────────────────────────────────────────┘
```

- **Chunked files**: Journal rotates when a chunk exceeds `max_chunk_size_bytes` (default 128 MiB)
- **Index file**: Maps block heights to chunk + offset for O(1) lookup
- **Checksum**: Blake3 hash of compressed payload

### Snapshot

Full state export for fast recovery:

```
┌──────────────────────── HEADER (32 bytes) ────────────────────────┐
│ magic(4) "MHIS" │ version(2) │ key_bytes(2) │ block_height(8)     │
│ shard_count(8) │ checksum(8)                                      │
└───────────────────────────────────────────────────────────────────┘
┌──────────────────────── SHARD DATA (per shard) ───────────────────┐
│ entry_count(u64)                                                  │
│ repeat entry_count times:                                         │
│   [key(key_bytes)][len(u16)][value bytes...]                      │
└───────────────────────────────────────────────────────────────────┘
```

---

## LMDB Metadata

The `MetadataStore` tracks persistent state using LMDB:

| Key | Value | Purpose |
|-----|-------|---------|
| `current_block` | BlockId | Latest committed block height |
| `journal_offsets/{block}` | JournalMeta | Chunk ID + offset for each block |
| `gc_watermark` | GcWatermark | Pending prune state for crash recovery |
| `snapshot_watermark` | BlockId | Latest snapshot block for pruning |

LMDB provides ACID guarantees, but metadata is recorded *after* the journal append succeeds. If the process crashes after writing the journal entry but before LMDB catches up, recovery replays the durable journal blocks and re-derives the missing metadata.

---

## Pruning

Old journal chunks are pruned to bound disk usage:

```
min_rollback_window = 100
current_block = 1000

┌──────────────────────────────────────────────────────────────────┐
│ Blocks 0–900: eligible for pruning                               │
│ Blocks 901–1000: protected (within rollback window)              │
└──────────────────────────────────────────────────────────────────┘
```

Pruning runs in the background at `prune_interval`. The pruner:

1. Scans sealed chunks (not the active write chunk)
2. Deletes chunks whose highest block ≤ `current_block - min_rollback_window`
3. Updates the journal index atomically

---

## Concurrency Model

```
┌─────────────────────────────────────────────────────────────────┐
│                        update_mutex                             │
│  • Serializes all mutations (set, rollback)                     │
│  • Held for the entire write path                               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        reader_gate (RwLock)                     │
│  • Writes acquire exclusive lock                                │
│  • Reads acquire shared lock                                    │
│  • Ensures consistent view during mutations                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              Shard RwLocks (one per shard)                      │
│  • Fine-grained: reads to different shards don't block          │
│  • Writes acquire exclusive per-shard lock                      │
└─────────────────────────────────────────────────────────────────┘
```

**Thread-safety**: All components are `Send + Sync`.

---

## Parallelism

When `thread_count > 1`, Rollblock uses Rayon for:

- Preparing deltas across shards
- Committing operations to shards
- Reverting undo entries

The speedup is most noticeable with many shards and large batches.

---

## Traits & Extensibility

Every major component is a trait. Swap implementations without touching the rest:

| Trait | Default Implementation | Role |
|-------|------------------------|------|
| `StoreFacade` | `MhinStoreFacade` | Public API |
| `BlockOrchestrator` | `DefaultBlockOrchestrator` | Coordination |
| `StateEngine` | `ShardedStateEngine` | Shard management |
| `StateShard` | `RawTableShard` | In-memory storage |
| `BlockJournal` | `FileBlockJournal` | Undo persistence |
| `MetadataStore` | `LmdbMetadataStore` | Block tracking |
| `Snapshotter` | `MmapSnapshotter` | State backup |

---

## Directory Layout

```
data_dir/
├── metadata/           # LMDB environment
│   ├── data.mdb
│   └── lock.mdb
├── journal/            # Undo log
│   ├── journal.00000001.bin
│   ├── journal.00000002.bin
│   └── journal.idx
├── snapshots/          # Full state exports
│   └── snapshot_00000000000003e8.bin
└── rollblock.lock      # Single-writer lock
```

---

## Guarantees

| Category | Guarantee |
|----------|-----------|
| **Atomicity** | All ops in a block succeed or fail together |
| **Durability** | Committed blocks survive crash (per mode) |
| **Integrity** | Blake3 checksums on journal and snapshots |
| **Isolation** | Reads see consistent state; writes are serialized |
| **Performance** | O(1) get/set/delete via hashmap shards |

---

## Quick Reference

```rust
use rollblock::types::StoreKey as Key;

// Create store
let config = StoreConfig::new("./data", 4, 10_000, 1, false)?;
let store = MhinStoreFacade::new(config)?;

// Write a block
store.set(
    1,
    vec![Operation {
        key: [1u8; Key::BYTES].into(),
        value: 42.into(),
    }],
)?;

// Read
let value = store.get([1u8; Key::BYTES].into())?;

// Rollback
store.rollback(0)?;

// Graceful shutdown
store.close()?;
```
