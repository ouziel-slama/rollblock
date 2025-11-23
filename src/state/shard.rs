use crate::types::{Key, ShardOp, ShardStats, ShardUndo, Value};

pub mod raw_table;

pub use raw_table::RawTableShard;

pub trait StateShard: Send + Sync {
    fn apply(&self, ops: &[ShardOp]) -> ShardUndo;
    fn revert(&self, undo: &ShardUndo);
    fn get(&self, key: &Key) -> Option<Value>;
    /// Fetches multiple keys while allowing implementations to amortize locking.
    fn get_many(&self, keys: &[Key], out: &mut [Option<Value>]) {
        debug_assert_eq!(
            keys.len(),
            out.len(),
            "get_many requires matching keys/out lengths"
        );

        for (key, slot) in keys.iter().zip(out.iter_mut()) {
            *slot = self.get(key);
        }
    }
    fn stats(&self) -> ShardStats;

    /// Export all key-value pairs from this shard
    fn export_data(&self) -> Vec<(Key, Value)>;

    /// Visit all key-value pairs without allocating an intermediate buffer.
    ///
    /// Implementations should provide a streaming traversal to avoid
    /// materialising large shards in memory. The default implementation falls
    /// back to `export_data` for metadata-only shards used in tests.
    fn visit_entries(&self, visitor: &mut dyn FnMut(Key, Value)) {
        for (key, value) in self.export_data() {
            visitor(key, value);
        }
    }

    /// Clear all data and import the provided key-value pairs
    fn import_data(&self, data: Vec<(Key, Value)>);
}
