//! Minimal object-store abstraction for shared WAL tiering.
//!
//! This trait is defined in `helix-wal` to avoid a circular dependency:
//! `helix-tier` depends on `helix-wal`, so `helix-wal` cannot depend on
//! `helix-tier`. Implementors (e.g., `SimulatedObjectStorage`,
//! `FilesystemObjectStorage`, AWS S3) live in `helix-tier`.
//!
//! The interface is intentionally minimal — only the operations needed by
//! `SharedWalCoordinator` for segment upload, download, and listing.

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::{WalError, WalResult};

/// Minimal object-store interface used by `SharedWalCoordinator` for tiering.
///
/// Implementors must be `Send + Sync + 'static` so they can be stored in the
/// coordinator's `Arc<CoordinatorInner<S>>` and shared across async tasks.
///
/// # Key Format
///
/// Keys are opaque strings. The coordinator uses:
/// `shared/{pool_index}/{segment_id:08x}.wal`
///
/// prefixed with a pod-scoped prefix supplied at `configure_tiering()` time.
#[async_trait]
pub trait WalSegmentStore: Send + Sync + 'static {
    /// Uploads `data` to `key`, overwriting any previous value.
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` on upload failure.
    async fn put(&self, key: &str, data: Bytes) -> WalResult<()>;

    /// Downloads the object at `key`.
    ///
    /// # Errors
    ///
    /// Returns `WalError::SegmentNotFound` if the key does not exist.
    /// Returns `WalError::Io` on download failure.
    async fn get(&self, key: &str) -> WalResult<Bytes>;

    /// Deletes the object at `key`.
    ///
    /// Must be idempotent: deleting a non-existent key is not an error.
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` on unexpected deletion failure.
    async fn delete(&self, key: &str) -> WalResult<()>;

    /// Lists all keys that start with `prefix`.
    ///
    /// Returns the full key strings (not just the suffix after the prefix).
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` on listing failure.
    async fn list(&self, prefix: &str) -> WalResult<Vec<String>>;
}

/// A no-op [`WalSegmentStore`] that discards all writes and returns empty lists.
///
/// Useful for testing configurations where tiering is disabled at the
/// coordinator level but the generic parameter still needs a concrete type.
pub struct NoopSegmentStore;

#[async_trait]
impl WalSegmentStore for NoopSegmentStore {
    async fn put(&self, _key: &str, _data: Bytes) -> WalResult<()> {
        Ok(())
    }

    async fn get(&self, key: &str) -> WalResult<Bytes> {
        Err(WalError::SegmentNotFound {
            segment_id: parse_segment_id_from_key(key).unwrap_or(0),
        })
    }

    async fn delete(&self, _key: &str) -> WalResult<()> {
        Ok(())
    }

    async fn list(&self, _prefix: &str) -> WalResult<Vec<String>> {
        Ok(Vec::new())
    }
}

/// Parses a segment ID from the trailing `{segment_id:08x}.wal` component
/// of a WAL segment key.
///
/// Returns `None` if the key does not match the expected suffix format.
#[must_use]
pub fn parse_segment_id_from_key(key: &str) -> Option<u64> {
    // Key format: ".../{segment_id:08x}.wal"
    // Strip the ".wal" suffix, then parse the last path component as hex.
    let stem = key.strip_suffix(".wal")?;
    let hex_part = stem.rsplit('/').next()?;
    u64::from_str_radix(hex_part, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_segment_id_from_key() {
        assert_eq!(
            parse_segment_id_from_key("shared/0/0000001a.wal"),
            Some(0x1a)
        );
        assert_eq!(
            parse_segment_id_from_key("prefix/shared/3/deadbeef.wal"),
            Some(0xdead_beef)
        );
        assert_eq!(parse_segment_id_from_key("not-a-wal-key"), None);
        assert_eq!(parse_segment_id_from_key("segment.txt"), None);
    }
}
