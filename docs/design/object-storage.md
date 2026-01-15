# S3 Storage Backend Design

**Status**: Design Phase
**Created**: 2025-01-14

## Overview

This document details the design for three `ObjectStorage` implementations:

1. **S3ObjectStorage** - Real AWS S3 for production
2. **FilesystemObjectStorage** - Local filesystem for development/testing
3. **SimulatedObjectStorage** (enhanced) - DST-compatible for simulation testing

All implementations conform to the existing `ObjectStorage` trait:

```rust
#[async_trait]
pub trait ObjectStorage: Send + Sync {
    async fn put(&self, key: &ObjectKey, data: Bytes) -> TierResult<()>;
    async fn get(&self, key: &ObjectKey) -> TierResult<Bytes>;
    async fn delete(&self, key: &ObjectKey) -> TierResult<()>;
    async fn list(&self, prefix: &str) -> TierResult<Vec<ObjectKey>>;
    async fn exists(&self, key: &ObjectKey) -> TierResult<bool>;
}
```

---

## Design Principles

### Segment Size and Alignment

**Recommended segment size: 4 MiB**

Object storage systems (S3, GCS, Azure Blob) are optimized for objects in the 1-16 MiB range:
- Smaller objects incur higher per-request overhead
- Larger objects increase latency for partial reads
- 4 MiB provides good balance for both upload and download efficiency

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Target segment size | 4 MiB | Optimal S3 GET performance |
| Multipart upload threshold | 8 MiB | Use multipart for larger segments |
| Multipart part size | 8 MiB | Balance parallelism vs overhead |

### Shared WAL Considerations

With shared WALs (multiple partitions per WAL), segments contain interleaved data:

```
Shared WAL Segment:
┌──────────────────────────────────────────────────────┐
│ P0:100 │ P2:50 │ P0:101 │ P3:75 │ P1:200 │ ...      │
└──────────────────────────────────────────────────────┘
  (entries interleaved by arrival time)
```

**Read amplification**: When reading a single partition from tiered storage, the entire
segment must be downloaded and filtered. With 4 partitions per shared WAL, this results
in ~4x read amplification for single-partition reads.

### Compaction Strategy (v1: None)

**Decision: No compaction in v1.**

Rationale:
- Tiered data is cold - reads are infrequent
- Hot data lives on local SSD, not S3
- Shared WAL segments are already reasonably sized (~4 MiB)
- Accepting read amplification simplifies the system

**Future considerations** (if read patterns change):
- Per-partition reorganization: Split shared segments into per-partition files
- Size-based merging: Combine small segments into larger objects
- Index files: Create partition→byte-range indexes for S3 Range GETs

Metrics to monitor:
- Single-partition vs all-partition read ratio for tiered data
- S3 GET request count and bytes transferred
- Consumer lag duration (time spent reading tiered data)

---

## 1. S3ObjectStorage (Production)

### 1.1 Configuration

```rust
/// Configuration for S3 object storage.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3 bucket name (required)
    pub bucket: String,

    /// Key prefix for all objects (default: "helix/segments/")
    pub key_prefix: String,

    /// AWS region (default: from environment/config)
    pub region: Option<String>,

    /// Custom endpoint URL (for S3-compatible services like MinIO, LocalStack)
    pub endpoint_url: Option<String>,

    /// Force path-style addressing (required for MinIO/LocalStack)
    pub force_path_style: bool,

    /// Request timeout in seconds (default: 30)
    pub timeout_secs: u64,

    /// Number of retry attempts (default: 3, uses AWS SDK exponential backoff)
    pub max_retries: u32,

    /// Storage class for uploads (default: STANDARD)
    pub storage_class: S3StorageClass,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum S3StorageClass {
    #[default]
    Standard,
    StandardIa,        // Infrequent Access
    IntelligentTiering,
    GlacierInstantRetrieval,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            key_prefix: "helix/segments/".to_string(),
            region: None,
            endpoint_url: None,
            force_path_style: false,
            timeout_secs: 30,
            max_retries: 3,
            storage_class: S3StorageClass::Standard,
        }
    }
}
```

### 1.2 Implementation Structure

```rust
/// Real AWS S3 object storage implementation.
///
/// # Key Format
///
/// Objects are stored at: `{key_prefix}{topic_id}/{partition_id}/segment-{segment_id:08x}.wal`
///
/// Example: `helix/segments/1/0/segment-00000003.wal`
#[derive(Clone)]
pub struct S3ObjectStorage {
    client: aws_sdk_s3::Client,
    config: S3Config,
}

impl S3ObjectStorage {
    /// Create a new S3 storage from configuration.
    ///
    /// Loads AWS credentials from the standard credential chain:
    /// 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    /// 2. Shared credentials file (~/.aws/credentials)
    /// 3. IAM role (for EC2/ECS/Lambda)
    pub async fn new(config: S3Config) -> Result<Self, S3InitError> {
        assert!(!config.bucket.is_empty(), "bucket name required");

        let mut aws_config = aws_config::defaults(BehaviorVersion::latest());

        if let Some(region) = &config.region {
            aws_config = aws_config.region(Region::new(region.clone()));
        }

        let aws_config = aws_config.load().await;

        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
            .retry_config(
                RetryConfig::standard()
                    .with_max_attempts(config.max_retries)
            );

        if let Some(endpoint) = &config.endpoint_url {
            s3_config = s3_config.endpoint_url(endpoint);
        }

        if config.force_path_style {
            s3_config = s3_config.force_path_style(true);
        }

        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        Ok(Self { client, config })
    }

    /// Create from an existing AWS SDK client (for testing or custom config).
    pub fn from_client(client: aws_sdk_s3::Client, config: S3Config) -> Self {
        Self { client, config }
    }

    /// Build the full S3 key from an ObjectKey.
    fn full_key(&self, key: &ObjectKey) -> String {
        format!("{}{}", self.config.key_prefix, key.as_str())
    }
}
```

### 1.3 Trait Implementation

```rust
#[async_trait]
impl ObjectStorage for S3ObjectStorage {
    async fn put(&self, key: &ObjectKey, data: Bytes) -> TierResult<()> {
        assert!(!key.as_str().is_empty(), "key must not be empty");

        let full_key = self.full_key(key);
        let body = ByteStream::from(data);

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .body(body)
            .storage_class(self.config.storage_class.into())
            .send()
            .await
            .map_err(|e| TierError::UploadFailed {
                key: key.to_string(),
                message: format!("S3 PutObject failed: {e}"),
            })?;

        Ok(())
    }

    async fn get(&self, key: &ObjectKey) -> TierResult<Bytes> {
        assert!(!key.as_str().is_empty(), "key must not be empty");

        let full_key = self.full_key(key);

        let response = self.client
            .get_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| {
                // Check if it's a NotFound error
                if is_not_found_error(&e) {
                    TierError::NotFound { key: key.to_string() }
                } else {
                    TierError::DownloadFailed {
                        key: key.to_string(),
                        message: format!("S3 GetObject failed: {e}"),
                    }
                }
            })?;

        let bytes = response.body
            .collect()
            .await
            .map_err(|e| TierError::DownloadFailed {
                key: key.to_string(),
                message: format!("Failed to read S3 response body: {e}"),
            })?
            .into_bytes();

        Ok(bytes)
    }

    async fn delete(&self, key: &ObjectKey) -> TierResult<()> {
        assert!(!key.as_str().is_empty(), "key must not be empty");

        let full_key = self.full_key(key);

        // S3 delete is idempotent - doesn't error if key doesn't exist
        self.client
            .delete_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| TierError::Io {
                operation: "delete",
                message: format!("S3 DeleteObject failed: {e}"),
            })?;

        Ok(())
    }

    async fn list(&self, prefix: &str) -> TierResult<Vec<ObjectKey>> {
        let full_prefix = format!("{}{}", self.config.key_prefix, prefix);
        let prefix_len = self.config.key_prefix.len();

        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self.client
                .list_objects_v2()
                .bucket(&self.config.bucket)
                .prefix(&full_prefix);

            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let response = request.send().await.map_err(|e| TierError::Io {
                operation: "list",
                message: format!("S3 ListObjectsV2 failed: {e}"),
            })?;

            if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        // Strip our prefix to get the ObjectKey
                        if key.len() > prefix_len {
                            keys.push(ObjectKey::new(&key[prefix_len..]));
                        }
                    }
                }
            }

            if response.is_truncated == Some(true) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(keys)
    }

    async fn exists(&self, key: &ObjectKey) -> TierResult<bool> {
        assert!(!key.as_str().is_empty(), "key must not be empty");

        let full_key = self.full_key(key);

        match self.client
            .head_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                if is_not_found_error(&e) {
                    Ok(false)
                } else {
                    Err(TierError::Io {
                        operation: "exists",
                        message: format!("S3 HeadObject failed: {e}"),
                    })
                }
            }
        }
    }
}

/// Check if an AWS SDK error is a "not found" error.
fn is_not_found_error<E>(err: &SdkError<E>) -> bool {
    matches!(
        err,
        SdkError::ServiceError(service_err)
            if service_err.raw().status().as_u16() == 404
    )
}
```

### 1.4 Feature Flag

```toml
# helix-tier/Cargo.toml
[features]
default = []
s3 = ["aws-sdk-s3", "aws-config", "aws-smithy-runtime-api"]

[dependencies]
aws-sdk-s3 = { version = "1.65", optional = true }
aws-config = { version = "1.5", optional = true }
aws-smithy-runtime-api = { version = "1.7", optional = true }
```

---

## 2. FilesystemObjectStorage (Local Development)

### 2.1 Configuration

```rust
/// Configuration for filesystem-based object storage.
#[derive(Debug, Clone)]
pub struct FilesystemConfig {
    /// Base directory for all objects (required)
    pub base_path: PathBuf,

    /// Whether to call fsync after writes (default: true)
    pub sync_on_write: bool,

    /// Create base directory if it doesn't exist (default: true)
    pub create_if_missing: bool,
}

impl FilesystemConfig {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
            sync_on_write: true,
            create_if_missing: true,
        }
    }
}
```

### 2.2 Implementation Structure

```rust
/// Filesystem-based object storage for local development and testing.
///
/// # Directory Structure
///
/// Objects are stored at: `{base_path}/{topic_id}/{partition_id}/segment-{segment_id:08x}.wal`
///
/// Example: `/tmp/helix-tier/1/0/segment-00000003.wal`
///
/// # Use Cases
///
/// - Local development without AWS credentials
/// - Integration testing without network dependencies
/// - CI environments without S3 access
#[derive(Clone)]
pub struct FilesystemObjectStorage {
    config: FilesystemConfig,
}

impl FilesystemObjectStorage {
    /// Create a new filesystem storage.
    pub async fn new(config: FilesystemConfig) -> TierResult<Self> {
        assert!(!config.base_path.as_os_str().is_empty(), "base_path required");

        if config.create_if_missing {
            tokio::fs::create_dir_all(&config.base_path)
                .await
                .map_err(|e| TierError::Io {
                    operation: "create_base_dir",
                    message: format!("Failed to create base directory: {e}"),
                })?;
        }

        Ok(Self { config })
    }

    /// Get the full filesystem path for an ObjectKey.
    fn object_path(&self, key: &ObjectKey) -> PathBuf {
        self.config.base_path.join(key.as_str())
    }

    /// Ensure parent directories exist for a path.
    async fn ensure_parent_dirs(&self, path: &Path) -> TierResult<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| TierError::Io {
                    operation: "create_parent_dirs",
                    message: format!("Failed to create parent directories: {e}"),
                })?;
        }
        Ok(())
    }
}
```

### 2.3 Trait Implementation

```rust
#[async_trait]
impl ObjectStorage for FilesystemObjectStorage {
    async fn put(&self, key: &ObjectKey, data: Bytes) -> TierResult<()> {
        assert!(!key.as_str().is_empty(), "key must not be empty");

        let path = self.object_path(key);
        self.ensure_parent_dirs(&path).await?;

        // Write to temp file first, then rename for atomicity
        let temp_path = path.with_extension("tmp");

        let mut file = tokio::fs::File::create(&temp_path)
            .await
            .map_err(|e| TierError::UploadFailed {
                key: key.to_string(),
                message: format!("Failed to create file: {e}"),
            })?;

        tokio::io::AsyncWriteExt::write_all(&mut file, &data)
            .await
            .map_err(|e| TierError::UploadFailed {
                key: key.to_string(),
                message: format!("Failed to write data: {e}"),
            })?;

        if self.config.sync_on_write {
            file.sync_all()
                .await
                .map_err(|e| TierError::UploadFailed {
                    key: key.to_string(),
                    message: format!("Failed to sync file: {e}"),
                })?;
        }

        drop(file);

        // Atomic rename
        tokio::fs::rename(&temp_path, &path)
            .await
            .map_err(|e| TierError::UploadFailed {
                key: key.to_string(),
                message: format!("Failed to rename temp file: {e}"),
            })?;

        Ok(())
    }

    async fn get(&self, key: &ObjectKey) -> TierResult<Bytes> {
        assert!(!key.as_str().is_empty(), "key must not be empty");

        let path = self.object_path(key);

        let data = tokio::fs::read(&path)
            .await
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    TierError::NotFound { key: key.to_string() }
                } else {
                    TierError::DownloadFailed {
                        key: key.to_string(),
                        message: format!("Failed to read file: {e}"),
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
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()), // Idempotent
            Err(e) => Err(TierError::Io {
                operation: "delete",
                message: format!("Failed to delete file: {e}"),
            }),
        }
    }

    async fn list(&self, prefix: &str) -> TierResult<Vec<ObjectKey>> {
        let search_path = self.config.base_path.join(prefix);

        // Determine the directory to search
        let search_dir = if search_path.is_dir() {
            search_path.clone()
        } else if let Some(parent) = search_path.parent() {
            parent.to_path_buf()
        } else {
            self.config.base_path.clone()
        };

        if !search_dir.exists() {
            return Ok(Vec::new());
        }

        let mut keys = Vec::new();
        let mut stack = vec![search_dir];

        while let Some(dir) = stack.pop() {
            let mut entries = tokio::fs::read_dir(&dir)
                .await
                .map_err(|e| TierError::Io {
                    operation: "list",
                    message: format!("Failed to read directory: {e}"),
                })?;

            while let Some(entry) = entries.next_entry().await.map_err(|e| TierError::Io {
                operation: "list",
                message: format!("Failed to read directory entry: {e}"),
            })? {
                let path = entry.path();
                let file_type = entry.file_type().await.map_err(|e| TierError::Io {
                    operation: "list",
                    message: format!("Failed to get file type: {e}"),
                })?;

                if file_type.is_dir() {
                    stack.push(path);
                } else if file_type.is_file() {
                    // Convert path to relative key
                    if let Ok(relative) = path.strip_prefix(&self.config.base_path) {
                        let key_str = relative.to_string_lossy();
                        if key_str.starts_with(prefix) {
                            keys.push(ObjectKey::new(key_str.as_ref()));
                        }
                    }
                }
            }
        }

        Ok(keys)
    }

    async fn exists(&self, key: &ObjectKey) -> TierResult<bool> {
        assert!(!key.as_str().is_empty(), "key must not be empty");

        let path = self.object_path(key);
        Ok(path.exists())
    }
}
```

### 2.4 Cleanup Utility

```rust
impl FilesystemObjectStorage {
    /// Remove all objects (for testing cleanup).
    pub async fn clear_all(&self) -> TierResult<()> {
        if self.config.base_path.exists() {
            tokio::fs::remove_dir_all(&self.config.base_path)
                .await
                .map_err(|e| TierError::Io {
                    operation: "clear_all",
                    message: format!("Failed to remove directory: {e}"),
                })?;

            tokio::fs::create_dir_all(&self.config.base_path)
                .await
                .map_err(|e| TierError::Io {
                    operation: "clear_all",
                    message: format!("Failed to recreate directory: {e}"),
                })?;
        }
        Ok(())
    }

    /// Get total size of all stored objects in bytes.
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
}
```

---

## 3. SimulatedObjectStorage Enhancements (DST)

The existing `SimulatedObjectStorage` is already well-designed. Here are targeted enhancements:

### 3.1 Latency Injection

```rust
/// Extended fault configuration with latency injection.
#[derive(Debug, Clone)]
pub struct ObjectStorageFaultConfig {
    // Existing fields...
    pub put_fail_rate: f64,
    pub get_fail_rate: f64,
    pub get_corruption_rate: f64,
    pub delete_fail_rate: f64,
    pub exists_fail_rate: f64,
    pub force_put_fail: bool,
    pub force_get_fail: bool,
    pub force_get_corruption: bool,
    pub force_delete_fail: bool,
    pub force_exists_fail: bool,

    // NEW: Latency injection (in simulated microseconds)
    pub put_latency_us: LatencyConfig,
    pub get_latency_us: LatencyConfig,
    pub delete_latency_us: LatencyConfig,
    pub list_latency_us: LatencyConfig,
    pub exists_latency_us: LatencyConfig,
}

/// Configurable latency distribution.
#[derive(Debug, Clone, Default)]
pub struct LatencyConfig {
    /// Base latency in microseconds (always added)
    pub base_us: u64,

    /// Additional random latency range [0, jitter_us)
    pub jitter_us: u64,

    /// Probability of adding "spike" latency (0.0-1.0)
    pub spike_rate: f64,

    /// Spike latency in microseconds (added when spike triggers)
    pub spike_us: u64,
}

impl LatencyConfig {
    pub fn none() -> Self {
        Self::default()
    }

    /// Realistic S3 latency profile.
    pub fn s3_realistic() -> Self {
        Self {
            base_us: 10_000,      // 10ms base
            jitter_us: 20_000,   // +0-20ms jitter
            spike_rate: 0.01,    // 1% spike chance
            spike_us: 500_000,   // 500ms spike
        }
    }

    /// High latency for stress testing.
    pub fn high_latency() -> Self {
        Self {
            base_us: 100_000,    // 100ms base
            jitter_us: 100_000,  // +0-100ms jitter
            spike_rate: 0.05,    // 5% spike chance
            spike_us: 2_000_000, // 2s spike
        }
    }
}
```

### 3.2 Bandwidth Throttling

```rust
/// Bandwidth throttling configuration.
#[derive(Debug, Clone)]
pub struct BandwidthConfig {
    /// Maximum bytes per second for uploads (0 = unlimited)
    pub upload_bytes_per_sec: u64,

    /// Maximum bytes per second for downloads (0 = unlimited)
    pub download_bytes_per_sec: u64,
}

impl BandwidthConfig {
    pub fn unlimited() -> Self {
        Self {
            upload_bytes_per_sec: 0,
            download_bytes_per_sec: 0,
        }
    }

    /// Simulate a slow network connection.
    pub fn slow_network() -> Self {
        Self {
            upload_bytes_per_sec: 1_000_000,   // 1 MB/s
            download_bytes_per_sec: 5_000_000, // 5 MB/s
        }
    }
}

impl SimulatedObjectStorage {
    /// Calculate simulated delay based on data size and bandwidth config.
    fn bandwidth_delay_us(&self, data_len: usize, is_upload: bool) -> u64 {
        let config = self.bandwidth_config.lock().expect("bandwidth lock");
        let bytes_per_sec = if is_upload {
            config.upload_bytes_per_sec
        } else {
            config.download_bytes_per_sec
        };

        if bytes_per_sec == 0 {
            return 0;
        }

        // Time = size / rate, convert to microseconds
        (data_len as u64 * 1_000_000) / bytes_per_sec
    }
}
```

### 3.3 Partial Failure Mode

```rust
/// Configuration for partial upload failures.
#[derive(Debug, Clone)]
pub struct PartialFailureConfig {
    /// Probability that an upload fails midway (0.0-1.0)
    pub partial_upload_fail_rate: f64,

    /// When partial failure occurs, what fraction of data is "uploaded" (0.0-1.0)
    pub partial_upload_fraction: f64,

    /// Enable "ghost objects" - objects that exist() returns true but get() fails
    pub ghost_object_rate: f64,
}
```

### 3.4 Operation Metrics

```rust
/// Metrics tracked by SimulatedObjectStorage for testing validation.
#[derive(Debug, Clone, Default)]
pub struct StorageMetrics {
    pub put_count: u64,
    pub put_bytes: u64,
    pub put_failures: u64,

    pub get_count: u64,
    pub get_bytes: u64,
    pub get_failures: u64,
    pub get_corruptions: u64,

    pub delete_count: u64,
    pub delete_failures: u64,

    pub list_count: u64,
    pub list_failures: u64,

    pub exists_count: u64,
    pub exists_failures: u64,

    /// Total simulated latency accumulated (microseconds)
    pub total_latency_us: u64,
}

impl SimulatedObjectStorage {
    /// Get current metrics snapshot.
    pub fn metrics(&self) -> StorageMetrics {
        self.metrics.lock().expect("metrics lock").clone()
    }

    /// Reset metrics to zero.
    pub fn reset_metrics(&self) {
        *self.metrics.lock().expect("metrics lock") = StorageMetrics::default();
    }
}
```

### 3.5 Preset Configurations

```rust
impl ObjectStorageFaultConfig {
    /// No faults - for basic correctness tests.
    pub fn none() -> Self { /* existing */ }

    /// Realistic S3 failure rates.
    pub fn flaky() -> Self { /* existing */ }

    /// Aggressive faults for stress testing.
    pub fn chaos() -> Self {
        Self {
            put_fail_rate: 0.10,      // 10% put failures
            get_fail_rate: 0.10,      // 10% get failures
            get_corruption_rate: 0.02, // 2% corruption
            delete_fail_rate: 0.05,   // 5% delete failures
            exists_fail_rate: 0.10,   // 10% exists failures
            put_latency_us: LatencyConfig::high_latency(),
            get_latency_us: LatencyConfig::high_latency(),
            ..Self::none()
        }
    }

    /// Network partition simulation - very high failure rates.
    pub fn partitioned() -> Self {
        Self {
            put_fail_rate: 0.90,
            get_fail_rate: 0.90,
            delete_fail_rate: 0.90,
            exists_fail_rate: 0.90,
            ..Self::none()
        }
    }
}
```

---

## 4. Factory and Runtime Selection

### 4.1 Storage Factory

```rust
/// Storage backend type selection.
#[derive(Debug, Clone)]
pub enum ObjectStorageBackend {
    /// In-memory simulated storage (for testing/DST)
    Simulated {
        seed: u64,
        fault_config: Option<ObjectStorageFaultConfig>,
    },

    /// Local filesystem storage (for development)
    Filesystem {
        config: FilesystemConfig,
    },

    /// AWS S3 storage (for production)
    #[cfg(feature = "s3")]
    S3 {
        config: S3Config,
    },
}

/// Create an ObjectStorage instance from configuration.
pub async fn create_object_storage(
    backend: ObjectStorageBackend,
) -> TierResult<Box<dyn ObjectStorage>> {
    match backend {
        ObjectStorageBackend::Simulated { seed, fault_config } => {
            let storage = match fault_config {
                Some(config) => SimulatedObjectStorage::with_faults(seed, config),
                None => SimulatedObjectStorage::new(seed),
            };
            Ok(Box::new(storage))
        }

        ObjectStorageBackend::Filesystem { config } => {
            let storage = FilesystemObjectStorage::new(config).await?;
            Ok(Box::new(storage))
        }

        #[cfg(feature = "s3")]
        ObjectStorageBackend::S3 { config } => {
            let storage = S3ObjectStorage::new(config).await
                .map_err(|e| TierError::Io {
                    operation: "create_s3_storage",
                    message: e.to_string(),
                })?;
            Ok(Box::new(storage))
        }
    }
}
```

### 4.2 Environment-Based Configuration

```rust
impl S3Config {
    /// Load configuration from environment variables.
    ///
    /// Environment variables:
    /// - HELIX_S3_BUCKET (required)
    /// - HELIX_S3_PREFIX (default: "helix/segments/")
    /// - HELIX_S3_REGION (optional)
    /// - HELIX_S3_ENDPOINT (optional, for MinIO/LocalStack)
    /// - HELIX_S3_FORCE_PATH_STYLE (optional, "true"/"false")
    pub fn from_env() -> Result<Self, ConfigError> {
        let bucket = std::env::var("HELIX_S3_BUCKET")
            .map_err(|_| ConfigError::MissingEnv("HELIX_S3_BUCKET"))?;

        Ok(Self {
            bucket,
            key_prefix: std::env::var("HELIX_S3_PREFIX")
                .unwrap_or_else(|_| "helix/segments/".to_string()),
            region: std::env::var("HELIX_S3_REGION").ok(),
            endpoint_url: std::env::var("HELIX_S3_ENDPOINT").ok(),
            force_path_style: std::env::var("HELIX_S3_FORCE_PATH_STYLE")
                .map(|v| v == "true")
                .unwrap_or(false),
            ..Self::default()
        })
    }
}
```

---

## 5. Testing Strategy

### 5.1 Unit Tests (per backend)

| Backend | Test Focus |
|---------|------------|
| `SimulatedObjectStorage` | Fault injection, determinism, metrics |
| `FilesystemObjectStorage` | File operations, atomicity, cleanup |
| `S3ObjectStorage` | Error mapping, pagination, key formatting |

### 5.2 Integration Tests

```rust
// tests/s3_integration.rs

/// Run against LocalStack or MinIO.
///
/// Start LocalStack: docker run -p 4566:4566 localstack/localstack
///
/// Environment:
///   HELIX_S3_BUCKET=test-bucket
///   HELIX_S3_ENDPOINT=http://localhost:4566
///   HELIX_S3_FORCE_PATH_STYLE=true
#[tokio::test]
#[ignore] // Run with: cargo test --features s3 -- --ignored
async fn test_s3_roundtrip() {
    let config = S3Config::from_env().expect("S3 config from env");
    let storage = S3ObjectStorage::new(config).await.expect("create storage");

    // Create bucket if needed (LocalStack)
    // ...

    let key = ObjectKey::from_segment(1, 0, 42);
    let data = Bytes::from(vec![1, 2, 3, 4, 5]);

    storage.put(&key, data.clone()).await.expect("put");
    assert!(storage.exists(&key).await.expect("exists"));

    let retrieved = storage.get(&key).await.expect("get");
    assert_eq!(retrieved, data);

    storage.delete(&key).await.expect("delete");
    assert!(!storage.exists(&key).await.expect("exists after delete"));
}
```

### 5.3 DST Tests

```rust
// helix-tests/src/tier_s3_dst.rs

#[test]
fn test_tiering_with_s3_faults() {
    for seed in 0..500 {
        let storage = SimulatedObjectStorage::with_faults(
            seed,
            ObjectStorageFaultConfig::chaos(),
        );

        let metadata = InMemoryMetadataStore::new();
        let manager = TieringManager::new(
            Arc::new(storage.clone()),
            Arc::new(metadata),
            TieringConfig::default(),
        );

        // Run tiering operations with fault injection
        // Verify invariants hold despite faults
        // Check metrics for expected failure counts
    }
}
```

---

## 6. Implementation Order

### Phase 1: FilesystemObjectStorage (1-2 days)
1. Implement `FilesystemObjectStorage` struct
2. Implement all 5 trait methods
3. Add unit tests
4. Add to factory

### Phase 2: S3ObjectStorage (2-3 days)
1. Update Cargo.toml dependencies
2. Implement `S3Config` and `S3ObjectStorage`
3. Implement trait methods with error mapping
4. Add LocalStack integration tests
5. Add to factory

### Phase 3: SimulatedObjectStorage Enhancements (1-2 days)
1. Add `LatencyConfig` structure
2. Add `StorageMetrics` tracking
3. Add preset configurations
4. Update DST tests to use new features

### Phase 4: Integration (1 day)
1. Wire into `helix-server` configuration
2. Add CLI flags for storage backend selection
3. Update documentation

---

## 7. Configuration Examples

### Production (AWS S3)

```bash
export HELIX_S3_BUCKET=my-helix-tier-bucket
export HELIX_S3_REGION=us-west-2
export HELIX_S3_PREFIX=prod/segments/

helix-server --storage-backend s3
```

### Local Development

```bash
helix-server --storage-backend filesystem --tier-path /tmp/helix-tier
```

### Testing with LocalStack

```bash
docker run -d -p 4566:4566 localstack/localstack
aws --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket

export HELIX_S3_BUCKET=test-bucket
export HELIX_S3_ENDPOINT=http://localhost:4566
export HELIX_S3_FORCE_PATH_STYLE=true

cargo test --features s3 -- --ignored
```

### DST

```rust
let storage = SimulatedObjectStorage::with_faults(
    42, // deterministic seed
    ObjectStorageFaultConfig::flaky(),
);
```
