use crate::types::Key;

/// Version identifier for the shard hashing algorithm.
///
/// Bump this when changing `SHARD_HASH_KEY_V1` or the hashing strategy so that
/// migrations can detect incompatible shard layouts.
pub const SHARD_HASH_VERSION: u32 = 1;

const SHARD_HASH_KEY_V1: [u8; 32] = *b"RollblockShardHashSeedV1-2024-10";

#[inline]
pub(crate) fn shard_hash(key: &Key) -> u64 {
    let hash = blake3::keyed_hash(&SHARD_HASH_KEY_V1, key);
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}
