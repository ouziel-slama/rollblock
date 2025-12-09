use crate::types::Key;
use xxhash_rust::xxh3::xxh3_64;

/// Version identifier for the shard hashing algorithm.
///
/// This must be bumped any time the mapping between keys and shard indexes
/// changes so that migrations can detect incompatible shard layouts.
pub const SHARD_HASH_VERSION: u32 = 2;

#[inline]
pub(crate) fn shard_hash<const N: usize>(key: &Key<N>) -> u64 {
    xxh3_64(key.as_slice())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_hash_accepts_8_byte_keys() {
        let key = Key::<8>::from([0u8; 8]);
        assert_eq!(shard_hash(&key), xxh3_64(&[0u8; 8]));
    }

    #[test]
    fn shard_hash_accepts_16_byte_keys() {
        let key = Key::<16>::from([1u8; 16]);
        assert_eq!(shard_hash(&key), xxh3_64(&[1u8; 16]));
    }
}
