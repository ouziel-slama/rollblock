/// Minimum number of bytes for store keys.
///
/// This is the lower bound enforced by `build.rs` when parsing `ROLLBLOCK_KEY_BYTES`.
pub const MIN_KEY_BYTES: usize = 8;

/// Maximum number of bytes for store keys.
///
/// Explicitly capped at 64 bytes to keep keys small while still fitting the
/// network handshake and on-disk header formats (which use `u16`).
pub const MAX_KEY_BYTES: usize = 64;

