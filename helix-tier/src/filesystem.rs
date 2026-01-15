//! Filesystem-based object storage for local development and testing.
//!
//! This backend stores objects as files on the local filesystem, providing
//! a simple way to test tiering without requiring S3 or network access.
//!
//! # Directory Structure
//!
//! Objects are stored at: `{base_path}/{topic_id}/{partition_id}/segment-{segment_id:08x}.wal`
//!
//! # Use Cases
//!
//! - Local development without AWS credentials
//! - Integration testing without network dependencies
//! - CI environments without S3 access

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::{TierError, TierResult};
use crate::storage::{ObjectKey, ObjectStorage};

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

/// Configuration for filesystem-based object storage.
#[derive(Debug, Clone)]
pub struct FilesystemConfig {
    /// Base directory for all objects.
    pub base_path: PathBuf,

    /// Whether to call fsync after writes for durability.
    ///
    /// Default: `true` for safety. Set to `false` for faster tests.
    pub sync_on_write: bool,

    /// Create base directory if it doesn't exist.
    ///
    /// Default: `true`.
    pub create_if_missing: bool,
}

impl FilesystemConfig {
    /// Creates a new configuration with the given base path.
    ///
    /// Uses safe defaults: `sync_on_write = true`, `create_if_missing = true`.
    #[must_use]
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
            sync_on_write: true,
            create_if_missing: true,
        }
    }

    /// Creates a configuration optimized for testing (no fsync).
    #[must_use]
    pub fn for_testing(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
            sync_on_write: false,
            create_if_missing: true,
        }
    }
}

// -----------------------------------------------------------------------------
// FilesystemObjectStorage
// -----------------------------------------------------------------------------

/// Filesystem-based object storage implementation.
///
/// Implements the [`ObjectStorage`] trait using local filesystem operations.
/// Suitable for development and testing scenarios.
#[derive(Debug, Clone)]
pub struct FilesystemObjectStorage {
    config: FilesystemConfig,
}

impl FilesystemObjectStorage {
    /// Creates a new filesystem storage with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if `create_if_missing` is true and directory creation fails.
    ///
    /// # Panics
    ///
    /// Panics if `base_path` is empty.
    pub async fn new(config: FilesystemConfig) -> TierResult<Self> {
        assert!(
            !config.base_path.as_os_str().is_empty(),
            "base_path must not be empty"
        );

        if config.create_if_missing {
            tokio::fs::create_dir_all(&config.base_path)
                .await
                .map_err(|e| TierError::Io {
                    operation: "create_base_dir",
                    message: format!(
                        "failed to create base directory '{}': {e}",
                        config.base_path.display()
                    ),
                })?;
        }

        Ok(Self { config })
    }

    /// Returns the full filesystem path for an object key.
    fn object_path(&self, key: &ObjectKey) -> PathBuf {
        self.config.base_path.join(key.as_str())
    }

    /// Ensures parent directories exist for a path.
    async fn ensure_parent_dirs(&self, path: &Path) -> TierResult<()> {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| TierError::Io {
                        operation: "create_parent_dirs",
                        message: format!(
                            "failed to create parent directories for '{}': {e}",
                            path.display()
                        ),
                    })?;
            }
        }
        Ok(())
    }

    /// Removes all objects from storage.
    ///
    /// Useful for test cleanup. Recreates the base directory after clearing.
    ///
    /// # Errors
    ///
    /// Returns an error if directory operations fail.
    pub async fn clear_all(&self) -> TierResult<()> {
        if self.config.base_path.exists() {
            tokio::fs::remove_dir_all(&self.config.base_path)
                .await
                .map_err(|e| TierError::Io {
                    operation: "clear_all",
                    message: format!(
                        "failed to remove directory '{}': {e}",
                        self.config.base_path.display()
                    ),
                })?;
        }

        tokio::fs::create_dir_all(&self.config.base_path)
            .await
            .map_err(|e| TierError::Io {
                operation: "clear_all",
                message: format!(
                    "failed to recreate directory '{}': {e}",
                    self.config.base_path.display()
                ),
            })?;

        Ok(())
    }

    /// Returns the total size in bytes of all stored objects.
    ///
    /// # Errors
    ///
    /// Returns an error if listing or stat operations fail.
    pub async fn total_size(&self) -> TierResult<u64> {
        let keys = self.list("").await?;
        let mut total = 0u64;

        for key in keys {
            let path = self.object_path(&key);
            if let Ok(metadata) = tokio::fs::metadata(&path).await {
                total += metadata.len();
            }
        }

        Ok(total)
    }

    /// Returns the number of stored objects.
    ///
    /// # Errors
    ///
    /// Returns an error if listing fails.
    pub async fn object_count(&self) -> TierResult<usize> {
        let keys = self.list("").await?;
        Ok(keys.len())
    }
}

// -----------------------------------------------------------------------------
// ObjectStorage Implementation
// -----------------------------------------------------------------------------

#[async_trait]
impl ObjectStorage for FilesystemObjectStorage {
    async fn put(&self, key: &ObjectKey, data: Bytes) -> TierResult<()> {
        assert!(!key.as_str().is_empty(), "key must not be empty");

        let path = self.object_path(key);
        self.ensure_parent_dirs(&path).await?;

        // Write to temp file first, then rename for atomicity.
        let temp_path = path.with_extension("tmp");

        // Write data to temp file.
        let write_result = async {
            let mut file = tokio::fs::File::create(&temp_path)
                .await
                .map_err(|e| TierError::UploadFailed {
                    key: key.to_string(),
                    message: format!("failed to create temp file: {e}"),
                })?;

            tokio::io::AsyncWriteExt::write_all(&mut file, &data)
                .await
                .map_err(|e| TierError::UploadFailed {
                    key: key.to_string(),
                    message: format!("failed to write data: {e}"),
                })?;

            if self.config.sync_on_write {
                file.sync_all().await.map_err(|e| TierError::UploadFailed {
                    key: key.to_string(),
                    message: format!("failed to sync file: {e}"),
                })?;
            }

            Ok::<(), TierError>(())
        }
        .await;

        // Clean up temp file on error.
        if let Err(e) = write_result {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(e);
        }

        // Atomic rename.
        tokio::fs::rename(&temp_path, &path)
            .await
            .map_err(|e| TierError::UploadFailed {
                key: key.to_string(),
                message: format!("failed to rename temp file: {e}"),
            })?;

        Ok(())
    }

    async fn get(&self, key: &ObjectKey) -> TierResult<Bytes> {
        assert!(!key.as_str().is_empty(), "key must not be empty");

        let path = self.object_path(key);

        let data = tokio::fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                TierError::NotFound {
                    key: key.to_string(),
                }
            } else {
                TierError::DownloadFailed {
                    key: key.to_string(),
                    message: format!("failed to read file: {e}"),
                }
            }
        })?;

        Ok(Bytes::from(data))
    }

    async fn delete(&self, key: &ObjectKey) -> TierResult<()> {
        assert!(!key.as_str().is_empty(), "key must not be empty");

        let path = self.object_path(key);

        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            // Idempotent: deleting non-existent key succeeds.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(TierError::Io {
                operation: "delete",
                message: format!("failed to delete '{}': {e}", path.display()),
            }),
        }
    }

    async fn list(&self, prefix: &str) -> TierResult<Vec<ObjectKey>> {
        if !self.config.base_path.exists() {
            return Ok(Vec::new());
        }

        let mut keys = Vec::new();
        let mut stack = vec![self.config.base_path.clone()];

        while let Some(dir) = stack.pop() {
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(TierError::Io {
                        operation: "list",
                        message: format!("failed to read directory '{}': {e}", dir.display()),
                    })
                }
            };

            while let Some(entry) = entries.next_entry().await.map_err(|e| TierError::Io {
                operation: "list",
                message: format!("failed to read directory entry: {e}"),
            })? {
                let path = entry.path();
                let file_type = entry.file_type().await.map_err(|e| TierError::Io {
                    operation: "list",
                    message: format!("failed to get file type for '{}': {e}", path.display()),
                })?;

                if file_type.is_dir() {
                    stack.push(path);
                } else if file_type.is_file() {
                    // Skip temp files.
                    if path.extension().is_some_and(|ext| ext == "tmp") {
                        continue;
                    }

                    // Convert to relative key.
                    if let Ok(relative) = path.strip_prefix(&self.config.base_path) {
                        let key_str = relative.to_string_lossy();
                        if key_str.starts_with(prefix) {
                            keys.push(ObjectKey::new(key_str.as_ref()));
                        }
                    }
                }
            }
        }

        // Sort for deterministic ordering.
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));

        Ok(keys)
    }

    async fn exists(&self, key: &ObjectKey) -> TierResult<bool> {
        assert!(!key.as_str().is_empty(), "key must not be empty");

        let path = self.object_path(key);

        match tokio::fs::metadata(&path).await {
            Ok(metadata) => Ok(metadata.is_file()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(TierError::Io {
                operation: "exists",
                message: format!("failed to check '{}': {e}", path.display()),
            }),
        }
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_storage() -> (FilesystemObjectStorage, TempDir) {
        let temp_dir = TempDir::new().expect("create temp dir");
        let config = FilesystemConfig::for_testing(temp_dir.path());
        let storage = FilesystemObjectStorage::new(config)
            .await
            .expect("create storage");
        (storage, temp_dir)
    }

    #[tokio::test]
    async fn test_put_get_roundtrip() {
        let (storage, _temp) = create_test_storage().await;

        let key = ObjectKey::from_segment(1, 0, 42);
        let data = Bytes::from(vec![1, 2, 3, 4, 5]);

        storage.put(&key, data.clone()).await.expect("put");
        let retrieved = storage.get(&key).await.expect("get");

        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_put_overwrites() {
        let (storage, _temp) = create_test_storage().await;

        let key = ObjectKey::from_segment(1, 0, 42);
        let data1 = Bytes::from(vec![1, 2, 3]);
        let data2 = Bytes::from(vec![4, 5, 6, 7, 8]);

        storage.put(&key, data1).await.expect("put 1");
        storage.put(&key, data2.clone()).await.expect("put 2");

        let retrieved = storage.get(&key).await.expect("get");
        assert_eq!(retrieved, data2);
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let key = ObjectKey::from_segment(1, 0, 999);
        let result = storage.get(&key).await;

        assert!(matches!(result, Err(TierError::NotFound { .. })));
    }

    #[tokio::test]
    async fn test_delete_existing() {
        let (storage, _temp) = create_test_storage().await;

        let key = ObjectKey::from_segment(1, 0, 42);
        let data = Bytes::from(vec![1, 2, 3]);

        storage.put(&key, data).await.expect("put");
        assert!(storage.exists(&key).await.expect("exists before"));

        storage.delete(&key).await.expect("delete");
        assert!(!storage.exists(&key).await.expect("exists after"));
    }

    #[tokio::test]
    async fn test_delete_idempotent() {
        let (storage, _temp) = create_test_storage().await;

        let key = ObjectKey::from_segment(1, 0, 999);

        // Delete non-existent key should succeed.
        storage.delete(&key).await.expect("delete non-existent");
        storage.delete(&key).await.expect("delete again");
    }

    #[tokio::test]
    async fn test_list_with_prefix() {
        let (storage, _temp) = create_test_storage().await;

        // Create objects in different partitions.
        let key1 = ObjectKey::from_segment(1, 0, 1);
        let key2 = ObjectKey::from_segment(1, 0, 2);
        let key3 = ObjectKey::from_segment(1, 1, 1);
        let key4 = ObjectKey::from_segment(2, 0, 1);
        let data = Bytes::from(vec![1, 2, 3]);

        storage.put(&key1, data.clone()).await.expect("put 1");
        storage.put(&key2, data.clone()).await.expect("put 2");
        storage.put(&key3, data.clone()).await.expect("put 3");
        storage.put(&key4, data).await.expect("put 4");

        // List all.
        let all = storage.list("").await.expect("list all");
        assert_eq!(all.len(), 4);

        // List topic 1.
        let topic1 = storage.list("1/").await.expect("list topic 1");
        assert_eq!(topic1.len(), 3);

        // List topic 1, partition 0.
        let t1p0 = storage.list("1/0/").await.expect("list t1p0");
        assert_eq!(t1p0.len(), 2);

        // List topic 2.
        let topic2 = storage.list("2/").await.expect("list topic 2");
        assert_eq!(topic2.len(), 1);
    }

    #[tokio::test]
    async fn test_list_empty_dir() {
        let (storage, _temp) = create_test_storage().await;

        let keys = storage.list("").await.expect("list empty");
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_exists() {
        let (storage, _temp) = create_test_storage().await;

        let key = ObjectKey::from_segment(1, 0, 42);
        let data = Bytes::from(vec![1, 2, 3]);

        assert!(!storage.exists(&key).await.expect("exists before"));
        storage.put(&key, data).await.expect("put");
        assert!(storage.exists(&key).await.expect("exists after"));
    }

    #[tokio::test]
    async fn test_nested_directories() {
        let (storage, _temp) = create_test_storage().await;

        // Keys with multiple path components.
        let key = ObjectKey::new("deeply/nested/path/to/segment.wal");
        let data = Bytes::from(vec![1, 2, 3]);

        storage.put(&key, data.clone()).await.expect("put");
        let retrieved = storage.get(&key).await.expect("get");

        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_clear_all() {
        let (storage, _temp) = create_test_storage().await;

        let key1 = ObjectKey::from_segment(1, 0, 1);
        let key2 = ObjectKey::from_segment(1, 0, 2);
        let data = Bytes::from(vec![1, 2, 3]);

        storage.put(&key1, data.clone()).await.expect("put 1");
        storage.put(&key2, data).await.expect("put 2");
        assert_eq!(storage.object_count().await.expect("count"), 2);

        storage.clear_all().await.expect("clear");
        assert_eq!(storage.object_count().await.expect("count after"), 0);
    }

    #[tokio::test]
    async fn test_total_size() {
        let (storage, _temp) = create_test_storage().await;

        let key1 = ObjectKey::from_segment(1, 0, 1);
        let key2 = ObjectKey::from_segment(1, 0, 2);
        let data1 = Bytes::from(vec![1, 2, 3]); // 3 bytes
        let data2 = Bytes::from(vec![4, 5, 6, 7, 8]); // 5 bytes

        storage.put(&key1, data1).await.expect("put 1");
        storage.put(&key2, data2).await.expect("put 2");

        let total = storage.total_size().await.expect("total size");
        assert_eq!(total, 8);
    }
}
