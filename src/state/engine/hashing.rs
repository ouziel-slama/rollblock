use crate::types::Key;

/// Version identifier for the shard hashing algorithm.
///
/// This must be bumped any time the mapping between keys and shard indexes
/// changes so that migrations can detect incompatible shard layouts.
pub const SHARD_HASH_VERSION: u32 = 1;

#[inline]
pub(crate) fn shard_hash(key: &Key) -> u64 {
    // Keys are already 8-byte identifiers, so use them directly instead of
    // re-hashing to the same width.
    u64::from_le_bytes(*key)
}
