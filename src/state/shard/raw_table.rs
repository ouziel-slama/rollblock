use std::hash::BuildHasher;

use hashbrown::hash_map::DefaultHashBuilder;
use hashbrown::raw::RawTable;
use parking_lot::RwLock;

use super::StateShard;
use crate::types::{Key, ShardOp, ShardStats, ShardUndo, UndoEntry, UndoOp, Value};

pub struct RawTableShard {
    shard_index: usize,
    table: RwLock<RawTable<(Key, Value)>>,
    build_hasher: DefaultHashBuilder,
}

impl RawTableShard {
    pub fn new(shard_index: usize, capacity: usize) -> Self {
        let build_hasher = DefaultHashBuilder::default();
        let mut table = RawTable::new();
        if capacity > 0 {
            table.reserve(capacity, |(key, _)| Self::hash_with(&build_hasher, key));
        }

        Self {
            shard_index,
            table: RwLock::new(table),
            build_hasher,
        }
    }

    #[inline]
    fn hash_key(&self, key: &Key) -> u64 {
        Self::hash_with(&self.build_hasher, key)
    }

    #[inline]
    fn hash_with(builder: &DefaultHashBuilder, key: &Key) -> u64 {
        builder.hash_one(key)
    }

    #[inline]
    fn shard_index(&self) -> usize {
        self.shard_index
    }
}

impl Default for RawTableShard {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

impl StateShard for RawTableShard {
    fn apply(&self, ops: &[ShardOp]) -> ShardUndo {
        let mut table = self.table.write();
        let mut undo = ShardUndo {
            shard_index: self.shard_index(),
            entries: Vec::new(),
        };

        for op in ops {
            if op.is_delete() {
                let hash = self.hash_key(&op.key);
                if let Some((key, previous)) =
                    table.remove_entry(hash, |(candidate, _)| candidate == &op.key)
                {
                    undo.entries.push(UndoEntry {
                        key,
                        previous: Some(previous),
                        op: UndoOp::Deleted,
                    });
                }
            } else {
                let value = op.value;
                let hash = self.hash_key(&op.key);
                if let Some((_, existing_value)) =
                    table.get_mut(hash, |(candidate, _)| candidate == &op.key)
                {
                    let previous = *existing_value;
                    if previous != value {
                        *existing_value = value;
                    }
                    undo.entries.push(UndoEntry {
                        key: op.key,
                        previous: Some(previous),
                        op: UndoOp::Updated,
                    });
                } else {
                    table.insert(hash, (op.key, value), |(key, _)| {
                        Self::hash_with(&self.build_hasher, key)
                    });
                    undo.entries.push(UndoEntry {
                        key: op.key,
                        previous: None,
                        op: UndoOp::Inserted,
                    });
                }
            }
        }

        undo
    }

    fn revert(&self, undo: &ShardUndo) {
        let mut table = self.table.write();
        for entry in undo.entries.iter().rev() {
            let hash = self.hash_key(&entry.key);
            match entry.op {
                UndoOp::Inserted => {
                    table.remove_entry(hash, |(key, _)| key == &entry.key);
                }
                UndoOp::Updated => {
                    if let Some(previous) = entry.previous {
                        if let Some((_, value)) = table.get_mut(hash, |(key, _)| key == &entry.key)
                        {
                            *value = previous;
                        } else {
                            table.insert(hash, (entry.key, previous), |(key, _)| {
                                Self::hash_with(&self.build_hasher, key)
                            });
                        }
                    } else {
                        table.remove_entry(hash, |(key, _)| key == &entry.key);
                    }
                }
                UndoOp::Deleted => {
                    if let Some(previous) = entry.previous {
                        table.insert(hash, (entry.key, previous), |(key, _)| {
                            Self::hash_with(&self.build_hasher, key)
                        });
                    }
                }
            }
        }
    }

    fn get(&self, key: &Key) -> Option<Value> {
        let table = self.table.read();
        let hash = self.hash_key(key);
        table
            .get(hash, |(candidate, _)| candidate == key)
            .map(|(_, value)| *value)
    }

    fn get_many(&self, keys: &[Key], out: &mut [Option<Value>]) {
        debug_assert_eq!(keys.len(), out.len());
        let table = self.table.read();
        for (key, slot) in keys.iter().zip(out.iter_mut()) {
            let hash = self.hash_key(key);
            *slot = table
                .get(hash, |(candidate, _)| candidate == key)
                .map(|(_, value)| *value);
        }
    }

    fn stats(&self) -> ShardStats {
        let table = self.table.read();

        ShardStats {
            keys: table.len(),
            tombstones: 0,
        }
    }

    fn export_data(&self) -> Vec<(Key, Value)> {
        let table = self.table.read();
        let mut data = Vec::with_capacity(table.len());

        // SAFETY: RawTable::iter() provides valid bucket references for the lifetime
        // of the read guard. Each bucket contains a valid (Key, Value) tuple that was
        // properly inserted via the StateShard trait methods. The data is Copy, so
        // dereferencing is safe.
        unsafe {
            for bucket in table.iter() {
                let (key, value) = bucket.as_ref();
                data.push((*key, *value));
            }
        }

        data
    }

    fn visit_entries(&self, visitor: &mut dyn FnMut(Key, Value)) {
        let table = self.table.read();

        unsafe {
            for bucket in table.iter() {
                let (key, value) = bucket.as_ref();
                visitor(*key, *value);
            }
        }
    }

    fn import_data(&self, data: Vec<(Key, Value)>) {
        let mut table = self.table.write();

        table.clear();

        if !data.is_empty() {
            table.reserve(data.len(), |(key, _)| {
                Self::hash_with(&self.build_hasher, key)
            });
        }

        for (key, value) in data {
            let hash = Self::hash_with(&self.build_hasher, &key);
            table.insert(hash, (key, value), |(k, _)| {
                Self::hash_with(&self.build_hasher, k)
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard_op(key: Key, value: Value) -> ShardOp {
        ShardOp { key, value }
    }

    #[test]
    fn insert_update_delete_cycle() {
        let shard = RawTableShard::new(3, 16);
        let key = [0u8; 8];

        let insert_undo = shard.apply(&[shard_op(key, 10)]);
        assert_eq!(insert_undo.shard_index, 3);
        assert_eq!(insert_undo.entries.len(), 1);
        assert_eq!(insert_undo.entries[0].op, UndoOp::Inserted);
        assert_eq!(shard.get(&key), Some(10));

        let update_undo = shard.apply(&[shard_op(key, 42)]);
        assert_eq!(update_undo.entries.len(), 1);
        assert_eq!(update_undo.entries[0].op, UndoOp::Updated);
        assert_eq!(update_undo.entries[0].previous, Some(10));
        assert_eq!(shard.get(&key), Some(42));

        let delete_undo = shard.apply(&[shard_op(key, 0)]);
        assert_eq!(delete_undo.entries.len(), 1);
        assert_eq!(delete_undo.entries[0].op, UndoOp::Deleted);
        assert_eq!(shard.get(&key), None);

        shard.revert(&delete_undo);
        assert_eq!(shard.get(&key), Some(42));

        shard.revert(&update_undo);
        assert_eq!(shard.get(&key), Some(10));

        shard.revert(&insert_undo);
        assert_eq!(shard.get(&key), None);
    }

    #[test]
    fn revert_restores_state_after_batch_operations() {
        let shard = RawTableShard::new(1, 8);
        let key_a = [1u8; 8];
        let key_b = [2u8; 8];

        let undo = shard.apply(&[
            shard_op(key_a, 5),
            shard_op(key_b, 7),
            shard_op(key_a, 9),
            shard_op(key_b, 0),
        ]);

        assert_eq!(shard.get(&key_a), Some(9));
        assert_eq!(shard.get(&key_b), None);
        assert_eq!(undo.entries.len(), 4);

        shard.revert(&undo);

        assert_eq!(shard.get(&key_a), None);
        assert_eq!(shard.get(&key_b), None);
        assert_eq!(shard.stats().keys, 0);
    }

    #[test]
    fn export_data_returns_all_pairs() {
        let shard = RawTableShard::new(0, 4);
        let key_a = [3u8; 8];
        let key_b = [4u8; 8];

        shard.apply(&[shard_op(key_a, 21), shard_op(key_b, 34)]);

        let mut data = shard.export_data();
        data.sort_by(|(a, _), (b, _)| a.cmp(b));
        assert_eq!(data, vec![(key_a, 21), (key_b, 34)]);
    }

    #[test]
    fn import_data_replaces_existing_state() {
        let shard = RawTableShard::new(2, 2);
        let old_key = [5u8; 8];
        shard.apply(&[shard_op(old_key, 99)]);
        assert_eq!(shard.get(&old_key), Some(99));

        let new_key_a = [8u8; 8];
        let new_key_b = [9u8; 8];
        shard.import_data(vec![(new_key_a, 1), (new_key_b, 2)]);

        assert_eq!(shard.get(&old_key), None);
        assert_eq!(shard.get(&new_key_a), Some(1));
        assert_eq!(shard.get(&new_key_b), Some(2));
        assert_eq!(shard.stats().keys, 2);
    }

    #[test]
    fn revert_updated_entry_with_no_previous_value_removes_key() {
        let shard = RawTableShard::new(3, 4);
        let key = [6u8; 8];
        shard.apply(&[shard_op(key, 55)]);
        assert_eq!(shard.get(&key), Some(55));

        let undo = ShardUndo {
            shard_index: 3,
            entries: vec![UndoEntry {
                key,
                previous: None,
                op: UndoOp::Updated,
            }],
        };

        shard.revert(&undo);
        assert_eq!(shard.get(&key), None);
    }
}
