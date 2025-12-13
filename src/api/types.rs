use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::api::error::StoreError;

// Key width constants included from build-time generated files.
// These are `pub const` in the included files, so they're automatically
// exported and accessible via `rollblock::types::{MIN_KEY_BYTES, MAX_KEY_BYTES, DEFAULT_KEY_BYTES}`.
include!(concat!(env!("CARGO_MANIFEST_DIR"), "/build/key_limits.rs"));
include!(concat!(env!("OUT_DIR"), "/key_width.rs"));

/// Fixed-size key used throughout the store (const-generic over the width).
///
/// Keys are typically derived from hashing larger identifiers upstream.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Key<const N: usize>(pub [u8; N]);

/// Default key type bound to [`DEFAULT_KEY_BYTES`].
pub type StoreKey = Key<DEFAULT_KEY_BYTES>;

impl<const N: usize> Key<N> {
    pub const BYTES: usize = N;

    #[inline]
    pub const fn new(bytes: [u8; N]) -> Self {
        Self(bytes)
    }

    #[inline]
    pub const fn into_inner(self) -> [u8; N] {
        self.0
    }

    #[inline]
    pub const fn as_array(&self) -> &[u8; N] {
        &self.0
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Build a key from a prefix, padding with zeros when the configured key
    /// width is larger than the prefix length.
    ///
    /// # Panics
    ///
    /// Panics if the prefix is longer than the configured key width to avoid
    /// silent truncation.
    #[inline]
    pub fn from_prefix<const M: usize>(prefix: [u8; M]) -> Self {
        assert!(
            prefix.len() <= N,
            "prefix length {} exceeds key width {}",
            prefix.len(),
            N
        );

        let mut bytes = [0u8; N];
        bytes[..prefix.len()].copy_from_slice(&prefix);
        Key(bytes)
    }

    /// Build a key from a little-endian `u64`, padding or truncating to the
    /// configured key width.
    #[inline]
    pub fn from_u64_le(value: u64) -> Self {
        Self::from_prefix(value.to_le_bytes())
    }
}

impl<const N: usize> From<[u8; N]> for Key<N> {
    fn from(value: [u8; N]) -> Self {
        Key(value)
    }
}

impl StoreKey {
    /// Fallible constructor that enforces the configured key width.
    ///
    /// Returns an error if the input slice length does not match
    /// `DEFAULT_KEY_BYTES`, preventing accidental truncation when the store
    /// is compiled with a larger key width.
    pub fn try_from_slice(bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() != DEFAULT_KEY_BYTES {
            return Err(StoreError::ConfigurationMismatch {
                field: "key_bytes",
                stored: bytes.len(),
                requested: DEFAULT_KEY_BYTES,
            });
        }

        let mut buf = [0u8; DEFAULT_KEY_BYTES];
        buf.copy_from_slice(bytes);
        Ok(Key(buf))
    }
}

impl<const N: usize> From<Key<N>> for [u8; N] {
    fn from(key: Key<N>) -> Self {
        key.0
    }
}

impl<const N: usize> Deref for Key<N> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<const N: usize> AsRef<[u8]> for Key<N> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<const N: usize> Borrow<[u8]> for Key<N> {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<const N: usize> fmt::Debug for Key<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Key").field(&self.as_slice()).finish()
    }
}

impl<const N: usize> Default for Key<N> {
    fn default() -> Self {
        Self([0u8; N])
    }
}

impl<const N: usize> Serialize for Key<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeTuple;
        // Serialize as a fixed-width tuple to avoid length prefixes in binary formats.
        // This matches the deserialize_tuple call in Deserialize impl.
        let mut tuple = serializer.serialize_tuple(N)?;
        for byte in &self.0 {
            tuple.serialize_element(byte)?;
        }
        tuple.end()
    }
}

impl<'de, const N: usize> Deserialize<'de> for Key<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct KeyVisitor<const N: usize>;

        impl<'de, const N: usize> de::Visitor<'de> for KeyVisitor<N> {
            type Value = Key<N>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a byte array of length {}", N)
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if v.len() != N {
                    return Err(E::invalid_length(v.len(), &self));
                }
                let mut bytes = [0u8; N];
                bytes.copy_from_slice(v);
                Ok(Key(bytes))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut bytes = [0u8; N];
                for (i, byte) in bytes.iter_mut().enumerate() {
                    *byte = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(i, &self))?;
                }
                Ok(Key(bytes))
            }
        }

        deserializer.deserialize_tuple(N, KeyVisitor)
    }
}

/// Maximum number of bytes allowed per value payload.
pub const MAX_VALUE_BYTES: usize = 65_535;

/// Maximum number of bytes that can be stored inline (Small Value Optimization).
const SVO_MAX_LEN: usize = 8;

/// Internal representation for Small Value Optimization.
///
/// Values ≤ 8 bytes are stored inline to avoid heap allocation,
/// which significantly improves performance for small payloads like u64.
#[derive(Clone)]
enum ValueInner {
    /// Inline storage for values up to 8 bytes. No heap allocation.
    Inline { data: [u8; SVO_MAX_LEN], len: u8 },
    /// Heap-allocated storage for larger values.
    Heap(Arc<[u8]>),
}

impl Default for ValueInner {
    #[inline]
    fn default() -> Self {
        ValueInner::Inline {
            data: [0; SVO_MAX_LEN],
            len: 0,
        }
    }
}

impl PartialEq for ValueInner {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for ValueInner {}

impl ValueInner {
    #[inline]
    fn from_slice(bytes: &[u8]) -> Self {
        if bytes.len() <= SVO_MAX_LEN {
            let mut data = [0u8; SVO_MAX_LEN];
            data[..bytes.len()].copy_from_slice(bytes);
            ValueInner::Inline {
                data,
                len: bytes.len() as u8,
            }
        } else {
            ValueInner::Heap(Arc::from(bytes))
        }
    }

    #[inline]
    fn from_vec(bytes: Vec<u8>) -> Self {
        if bytes.len() <= SVO_MAX_LEN {
            let mut data = [0u8; SVO_MAX_LEN];
            data[..bytes.len()].copy_from_slice(&bytes);
            ValueInner::Inline {
                data,
                len: bytes.len() as u8,
            }
        } else {
            ValueInner::Heap(Arc::from(bytes))
        }
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        match self {
            ValueInner::Inline { data, len } => &data[..*len as usize],
            ValueInner::Heap(arc) => arc,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            ValueInner::Inline { len, .. } => *len as usize,
            ValueInner::Heap(arc) => arc.len(),
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Converts to Arc<[u8]>, allocating if currently inline.
    fn into_arc(self) -> Arc<[u8]> {
        match self {
            ValueInner::Inline { data, len } => Arc::from(&data[..len as usize]),
            ValueInner::Heap(arc) => arc,
        }
    }

    /// Returns Arc if heap-allocated, or creates one from inline data.
    fn to_arc(&self) -> Arc<[u8]> {
        match self {
            ValueInner::Inline { data, len } => Arc::from(&data[..*len as usize]),
            ValueInner::Heap(arc) => arc.clone(),
        }
    }
}

/// Owned value payload stored for each key.
///
/// Empty values represent deletes across the stack.
///
/// Uses Small Value Optimization (SVO): values ≤ 8 bytes are stored inline
/// without heap allocation, significantly improving performance for small
/// payloads like u64 counters.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Value(ValueInner);

impl Value {
    #[inline]
    fn ensure_len_within_limit(len: usize) -> Result<(), StoreError> {
        if len > MAX_VALUE_BYTES {
            Err(StoreError::ValueTooLarge {
                actual: len,
                max: MAX_VALUE_BYTES,
            })
        } else {
            Ok(())
        }
    }

    /// Creates a value from owned bytes while validating the payload length.
    pub fn try_from_vec(bytes: Vec<u8>) -> Result<Self, StoreError> {
        Self::ensure_len_within_limit(bytes.len())?;
        Ok(Self(ValueInner::from_vec(bytes)))
    }

    /// Clones the provided slice into a new value while validating the length.
    pub fn try_from_slice(bytes: &[u8]) -> Result<Self, StoreError> {
        Self::ensure_len_within_limit(bytes.len())?;
        Ok(Self(ValueInner::from_slice(bytes)))
    }

    /// Ensures the current payload stays within the configured limit.
    pub fn ensure_within_limit(&self) -> Result<(), StoreError> {
        Self::ensure_len_within_limit(self.len())
    }

    /// Creates a value from owned bytes.
    ///
    /// # Panics
    ///
    /// Panics if `bytes.len()` exceeds [`MAX_VALUE_BYTES`]. Use
    /// [`Value::try_from_vec`] to handle validation errors explicitly.
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self::try_from_vec(bytes).expect("value exceeds MAX_VALUE_BYTES; use try_from_vec")
    }

    /// Clones the provided slice into a new value.
    ///
    /// # Panics
    ///
    /// Panics if `bytes.len()` exceeds [`MAX_VALUE_BYTES`]. Use
    /// [`Value::try_from_slice`] to handle validation errors explicitly.
    pub fn from_slice(bytes: &[u8]) -> Self {
        Self::try_from_slice(bytes).expect("value exceeds MAX_VALUE_BYTES; use try_from_slice")
    }

    /// Creates an empty (delete) marker.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Returns true when the payload has zero bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns true when this value represents a delete.
    #[inline]
    pub fn is_delete(&self) -> bool {
        self.is_empty()
    }

    /// Borrows the raw bytes.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Consumes the value and returns the shared buffer.
    ///
    /// Note: For inline values (≤ 8 bytes), this allocates a new Arc.
    pub fn into_inner(self) -> Arc<[u8]> {
        self.0.into_arc()
    }

    /// Number of bytes contained in this value.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true when the payload is non-empty.
    pub fn is_set(&self) -> bool {
        !self.is_delete()
    }

    /// Interprets the value as a little-endian `u64` if it fits.
    #[inline]
    pub fn to_u64(&self) -> Option<u64> {
        if self.len() > 8 {
            return None;
        }
        let mut buf = [0u8; 8];
        buf[..self.len()].copy_from_slice(self.as_slice());
        Some(u64::from_le_bytes(buf))
    }

    /// Temporary helper for serialization codepaths that still expect fixed-width values.
    ///
    /// This will assert if the value exceeds 8 bytes; future storage formats should
    /// be updated to handle variable-length payloads natively.
    #[inline]
    pub fn to_le_bytes(&self) -> [u8; 8] {
        assert!(
            self.len() <= 8,
            "fixed-width serialization only supports up to 8 bytes (got {})",
            self.len()
        );
        let mut bytes = [0u8; 8];
        bytes[..self.len()].copy_from_slice(self.as_slice());
        bytes
    }

    /// Builds a value from a fixed-width little-endian representation.
    ///
    /// This is optimized for SVO and will store the value inline.
    #[inline]
    pub fn from_le_bytes(bytes: [u8; 8]) -> Self {
        // Store all 8 bytes inline - no heap allocation
        Value(ValueInner::Inline {
            data: bytes,
            len: 8,
        })
    }

    /// Returns true if this value is stored inline (≤ 8 bytes).
    #[inline]
    pub fn is_inline(&self) -> bool {
        matches!(self.0, ValueInner::Inline { .. })
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Value").field(&self.as_slice()).finish()
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(num) = self.to_u64() {
            write!(f, "{num}")
        } else if self.is_delete() {
            write!(f, "0")
        } else {
            write!(f, "0x")?;
            for byte in self.as_slice() {
                write!(f, "{:02x}", byte)?;
            }
            Ok(())
        }
    }
}

impl Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::from_vec(value)
    }
}

impl From<&[u8]> for Value {
    fn from(slice: &[u8]) -> Self {
        Value::from_slice(slice)
    }
}

impl From<u64> for Value {
    #[inline]
    fn from(number: u64) -> Self {
        // Directly inline - no heap allocation
        Value(ValueInner::Inline {
            data: number.to_le_bytes(),
            len: 8,
        })
    }
}

impl From<Value> for Vec<u8> {
    fn from(value: Value) -> Self {
        let bytes = value.into_inner();
        bytes.as_ref().to_vec()
    }
}

impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq<u64> for Value {
    fn eq(&self, other: &u64) -> bool {
        self.to_u64() == Some(*other)
    }
}

impl PartialOrd<u64> for Value {
    fn partial_cmp(&self, other: &u64) -> Option<Ordering> {
        self.to_u64().and_then(|value| value.partial_cmp(other))
    }
}

/// Immutable shared buffer used by shards/state to avoid cloning.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct ValueBuf(Arc<[u8]>);

impl ValueBuf {
    pub fn from_arc(bytes: Arc<[u8]>) -> Self {
        Self(bytes)
    }

    pub fn from_slice(bytes: &[u8]) -> Self {
        Self(Arc::<[u8]>::from(bytes))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn is_delete(&self) -> bool {
        self.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_supports_default_width() {
        let key: StoreKey = Key::from([1u8; DEFAULT_KEY_BYTES]);
        assert_eq!(key.as_slice(), &[1u8; DEFAULT_KEY_BYTES]);
    }

    #[test]
    fn key_supports_max_width() {
        let key: Key<MAX_KEY_BYTES> = Key::from([7u8; MAX_KEY_BYTES]);
        assert_eq!(key.as_slice().len(), MAX_KEY_BYTES);
        assert_eq!(key.as_array(), &[7u8; MAX_KEY_BYTES]);
    }

    #[test]
    fn key_supports_larger_widths() {
        let key: Key<16> = Key::from([9u8; 16]);
        assert_eq!(key.as_slice().len(), 16);
        assert_eq!(key.as_array(), &[9u8; 16]);
    }

    #[test]
    fn key_bincode_roundtrip_default_width() {
        let original = StoreKey::from([0xABu8; DEFAULT_KEY_BYTES]);
        let encoded = bincode::serialize(&original).unwrap();
        let decoded: StoreKey = bincode::deserialize(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn key_bincode_roundtrip_larger_width() {
        let original = Key::<16>::from([0xCDu8; 16]);
        let encoded = bincode::serialize(&original).unwrap();
        let decoded: Key<16> = bincode::deserialize(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn key_bincode_rejects_wrong_length() {
        let short_bytes = vec![1u8; 4]; // Too short for an 8-byte key
        let err = bincode::deserialize::<StoreKey>(&short_bytes);
        assert!(err.is_err(), "should reject undersized payload");
    }

    #[test]
    fn key_try_from_slice_rejects_mismatched_length() {
        let too_short = [0u8; MIN_KEY_BYTES - 1];
        let err = StoreKey::try_from_slice(&too_short).unwrap_err();
        match err {
            StoreError::ConfigurationMismatch {
                field,
                stored,
                requested,
            } => {
                assert_eq!(field, "key_bytes");
                assert_eq!(stored, MIN_KEY_BYTES - 1);
                assert_eq!(requested, DEFAULT_KEY_BYTES);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn key_try_from_slice_accepts_exact_width() {
        let bytes = [0xEEu8; DEFAULT_KEY_BYTES];
        let key = StoreKey::try_from_slice(&bytes).expect("valid width");
        assert_eq!(key.as_slice(), bytes);
    }

    #[test]
    #[should_panic(expected = "prefix length")]
    fn key_from_prefix_rejects_overlong_prefix() {
        let _ = StoreKey::from_prefix([0xAAu8; DEFAULT_KEY_BYTES + 1]);
    }

    #[test]
    fn try_from_vec_rejects_oversized_payloads() {
        let oversized = vec![0u8; MAX_VALUE_BYTES + 1];
        let err = Value::try_from_vec(oversized).unwrap_err();
        match err {
            StoreError::ValueTooLarge { actual, max } => {
                assert_eq!(actual, MAX_VALUE_BYTES + 1);
                assert_eq!(max, MAX_VALUE_BYTES);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    // ========== SVO (Small Value Optimization) Tests ==========

    #[test]
    fn svo_u64_is_inline() {
        let value: Value = 42u64.into();
        assert!(value.is_inline(), "u64 values should be stored inline");
        assert_eq!(value.len(), 8);
        assert_eq!(value.to_u64(), Some(42));
    }

    #[test]
    fn svo_small_slice_is_inline() {
        let value = Value::from_slice(&[1, 2, 3, 4]);
        assert!(value.is_inline(), "4-byte values should be stored inline");
        assert_eq!(value.as_slice(), &[1, 2, 3, 4]);
    }

    #[test]
    fn svo_8_bytes_is_inline() {
        let value = Value::from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        assert!(value.is_inline(), "8-byte values should be stored inline");
        assert_eq!(value.len(), 8);
    }

    #[test]
    fn svo_9_bytes_is_heap() {
        let value = Value::from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert!(!value.is_inline(), "9-byte values should be heap-allocated");
        assert_eq!(value.len(), 9);
    }

    #[test]
    fn svo_empty_is_inline() {
        let value = Value::empty();
        assert!(value.is_inline(), "empty values should be stored inline");
        assert!(value.is_empty());
        assert!(value.is_delete());
    }

    #[test]
    fn svo_from_le_bytes_is_inline() {
        let bytes = 12345u64.to_le_bytes();
        let value = Value::from_le_bytes(bytes);
        assert!(
            value.is_inline(),
            "from_le_bytes should produce inline value"
        );
        assert_eq!(value.to_u64(), Some(12345));
    }

    #[test]
    fn svo_clone_preserves_inline() {
        let original: Value = 999u64.into();
        let cloned = original.clone();
        assert!(
            cloned.is_inline(),
            "cloned inline value should remain inline"
        );
        assert_eq!(cloned.to_u64(), Some(999));
    }

    #[test]
    fn svo_into_inner_works_for_inline() {
        let value: Value = 42u64.into();
        let arc = value.into_inner();
        assert_eq!(arc.as_ref(), &42u64.to_le_bytes());
    }

    #[test]
    fn svo_equality_works() {
        let a: Value = 100u64.into();
        let b: Value = 100u64.into();
        let c: Value = 200u64.into();
        assert_eq!(a, b);
        assert_ne!(a, c);

        // Inline vs heap with same content
        let small = Value::from_slice(&[1, 2, 3]);
        let small2 = Value::from_slice(&[1, 2, 3]);
        assert_eq!(small, small2);
    }

    #[test]
    fn svo_valuebuf_roundtrip() {
        let original: Value = 42u64.into();
        let buf: ValueBuf = (&original).into();
        let restored: Value = buf.into();
        assert_eq!(original, restored);
        assert!(restored.is_inline(), "restored value should be inline");
    }
}

impl fmt::Debug for ValueBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ValueBuf").field(&self.0).finish()
    }
}

impl From<Value> for ValueBuf {
    fn from(value: Value) -> Self {
        ValueBuf(value.into_inner())
    }
}

impl From<&Value> for ValueBuf {
    fn from(value: &Value) -> Self {
        ValueBuf(value.0.to_arc())
    }
}

impl From<ValueBuf> for Value {
    fn from(buf: ValueBuf) -> Self {
        Value(ValueInner::from_slice(&buf.0))
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_newtype_struct("Value", &self.as_slice())
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        Value::try_from_vec(bytes).map_err(de::Error::custom)
    }
}

impl AsRef<[u8]> for ValueBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

/// Block identifier used to track state versions.
///
/// Block heights must be monotonically increasing.
pub type BlockId = u64;

pub type Operation = OperationForKey<DEFAULT_KEY_BYTES>;
pub type ShardOp = ShardOpForKey<DEFAULT_KEY_BYTES>;
pub type UndoEntry = UndoEntryForKey<DEFAULT_KEY_BYTES>;
pub type ShardUndo = ShardUndoForKey<DEFAULT_KEY_BYTES>;
pub type ShardDelta = ShardDeltaForKey<DEFAULT_KEY_BYTES>;
pub type BlockDelta = BlockDeltaForKey<DEFAULT_KEY_BYTES>;
pub type BlockUndo = BlockUndoForKey<DEFAULT_KEY_BYTES>;

/// A user-requested operation on the store.
///
/// Operations are batched and applied atomically per block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationForKey<const N: usize> {
    /// The key to operate on
    pub key: Key<N>,
    /// The value to set. Empty values delete the key.
    pub value: Value,
}

impl<const N: usize> OperationForKey<N> {
    #[inline]
    pub fn is_delete(&self) -> bool {
        self.value.is_delete()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardOpForKey<const N: usize> {
    pub key: Key<N>,
    pub value: Value,
}

impl<const N: usize> ShardOpForKey<N> {
    #[inline]
    pub fn is_delete(&self) -> bool {
        self.value.is_delete()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UndoEntryForKey<const N: usize> {
    pub key: Key<N>,
    pub previous: Option<Value>,
    pub op: UndoOp,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum UndoOp {
    Inserted,
    Updated,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShardUndoForKey<const N: usize> {
    pub shard_index: usize,
    pub entries: Vec<UndoEntryForKey<N>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShardDeltaForKey<const N: usize> {
    pub shard_index: usize,
    pub operations: Vec<ShardOpForKey<N>>,
    pub undo_entries: Vec<UndoEntryForKey<N>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BlockDeltaForKey<const N: usize> {
    pub block_height: BlockId,
    pub shards: Vec<ShardDeltaForKey<N>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BlockUndoForKey<const N: usize> {
    pub block_height: BlockId,
    pub shard_undos: Vec<ShardUndoForKey<N>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StateStats {
    pub operation_count: usize,
    pub modified_keys: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShardStats {
    pub keys: usize,
    pub tombstones: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct JournalMeta {
    /// Block recorded by this journal entry.
    pub block_height: BlockId,
    /// Identifier of the chunk file that holds the entry.
    pub chunk_id: u32,
    /// Byte offset within the chunk file.
    pub chunk_offset: u64,
    /// Length of the compressed payload (excludes header).
    pub compressed_len: u64,
    /// Blake3 checksum persisted alongside the header.
    pub checksum: u32,
}
