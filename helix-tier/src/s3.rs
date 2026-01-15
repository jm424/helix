//! S3-based object storage for production tiered storage.
//!
//! This backend stores objects in AWS S3 or S3-compatible services
//! (`MinIO`, `LocalStack`, etc.).
//!
//! # Configuration
//!
//! Configuration can be provided programmatically or loaded from environment
//! variables using [`S3Config::from_env`].
//!
//! # Example
//!
//! ```ignore
//! use helix_tier::{S3Config, S3ObjectStorage};
//!
//! let config = S3Config::from_env()?;
//! let storage = S3ObjectStorage::new(config).await?;
//! ```

use std::env;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::Response as HttpResponse;
use bytes::Bytes;

use crate::error::{TierError, TierResult};
use crate::storage::{ObjectKey, ObjectStorage};

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

/// S3 storage class for uploaded objects.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum S3StorageClass {
    /// Standard S3 storage (default).
    #[default]
    Standard,
    /// Infrequent Access - lower cost, retrieval fee.
    StandardIa,
    /// Intelligent Tiering - automatic cost optimization.
    IntelligentTiering,
    /// Glacier - archive storage for infrequently accessed data.
    Glacier,
}

impl S3StorageClass {
    /// Returns the AWS SDK storage class value.
    const fn to_sdk_class(self) -> aws_sdk_s3::types::StorageClass {
        match self {
            Self::Standard => aws_sdk_s3::types::StorageClass::Standard,
            Self::StandardIa => aws_sdk_s3::types::StorageClass::StandardIa,
            Self::IntelligentTiering => aws_sdk_s3::types::StorageClass::IntelligentTiering,
            Self::Glacier => aws_sdk_s3::types::StorageClass::Glacier,
        }
    }
}

/// Configuration for S3 object storage.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3 bucket name (required).
    pub bucket: String,

    /// Key prefix for all objects.
    ///
    /// Default: `"helix/segments/"`.
    pub key_prefix: String,

    /// AWS region.
    ///
    /// If not set, uses the default region from environment/config.
    pub region: Option<String>,

    /// Custom endpoint URL for S3-compatible services.
    ///
    /// Use this for `MinIO`, `LocalStack`, or other S3-compatible services.
    pub endpoint_url: Option<String>,

    /// Force path-style addressing.
    ///
    /// Required for `MinIO` and `LocalStack`. AWS S3 uses virtual-hosted style.
    pub force_path_style: bool,

    /// Request timeout in seconds.
    ///
    /// Default: 30 seconds.
    pub timeout_secs: u64,

    /// Maximum retry attempts.
    ///
    /// Default: 3. Uses AWS SDK exponential backoff.
    pub max_retries: u32,

    /// Storage class for uploaded objects.
    ///
    /// Default: `Standard`.
    pub storage_class: S3StorageClass,
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

/// Error during S3 configuration or initialization.
#[derive(Debug, Clone, thiserror::Error)]
pub enum S3ConfigError {
    /// Required environment variable is missing.
    #[error("missing required environment variable: {0}")]
    MissingEnv(&'static str),

    /// Invalid configuration value.
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

impl S3Config {
    /// Creates a new configuration with the given bucket name.
    #[must_use]
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            ..Self::default()
        }
    }

    /// Loads configuration from environment variables.
    ///
    /// # Environment Variables
    ///
    /// - `HELIX_S3_BUCKET` (required): S3 bucket name
    /// - `HELIX_S3_PREFIX`: Key prefix (default: `"helix/segments/"`)
    /// - `HELIX_S3_REGION`: AWS region
    /// - `HELIX_S3_ENDPOINT`: Custom endpoint URL
    /// - `HELIX_S3_FORCE_PATH_STYLE`: Set to `"true"` for path-style addressing
    ///
    /// # Errors
    ///
    /// Returns an error if `HELIX_S3_BUCKET` is not set.
    pub fn from_env() -> Result<Self, S3ConfigError> {
        let bucket =
            env::var("HELIX_S3_BUCKET").map_err(|_| S3ConfigError::MissingEnv("HELIX_S3_BUCKET"))?;

        Ok(Self {
            bucket,
            key_prefix: env::var("HELIX_S3_PREFIX")
                .unwrap_or_else(|_| "helix/segments/".to_string()),
            region: env::var("HELIX_S3_REGION").ok(),
            endpoint_url: env::var("HELIX_S3_ENDPOINT").ok(),
            force_path_style: env::var("HELIX_S3_FORCE_PATH_STYLE")
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            ..Self::default()
        })
    }

    /// Sets the key prefix.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = prefix.into();
        self
    }

    /// Sets a custom endpoint URL.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint_url = Some(endpoint.into());
        self
    }

    /// Enables path-style addressing.
    #[must_use]
    pub const fn with_path_style(mut self) -> Self {
        self.force_path_style = true;
        self
    }

    /// Sets the region.
    #[must_use]
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Sets the storage class.
    #[must_use]
    pub const fn with_storage_class(mut self, class: S3StorageClass) -> Self {
        self.storage_class = class;
        self
    }
}

// -----------------------------------------------------------------------------
// S3ObjectStorage
// -----------------------------------------------------------------------------

/// S3-based object storage implementation.
///
/// Implements the [`ObjectStorage`] trait using AWS S3 or S3-compatible services.
#[derive(Clone)]
pub struct S3ObjectStorage {
    client: aws_sdk_s3::Client,
    config: S3Config,
}

impl S3ObjectStorage {
    /// Creates a new S3 storage from configuration.
    ///
    /// Loads AWS credentials from the standard credential chain:
    /// 1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
    /// 2. Shared credentials file (`~/.aws/credentials`)
    /// 3. IAM role (for EC2/ECS/Lambda)
    ///
    /// # Errors
    ///
    /// Returns an error if the AWS client cannot be initialized.
    ///
    /// # Panics
    ///
    /// Panics if the bucket name is empty.
    pub async fn new(config: S3Config) -> Result<Self, S3ConfigError> {
        assert!(!config.bucket.is_empty(), "bucket name must not be empty");

        let mut aws_config_loader = aws_config::defaults(BehaviorVersion::latest());

        if let Some(region) = &config.region {
            aws_config_loader = aws_config_loader.region(Region::new(region.clone()));
        }

        let aws_config = aws_config_loader.load().await;

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);

        if let Some(endpoint) = &config.endpoint_url {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint);
        }

        if config.force_path_style {
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

        Ok(Self { client, config })
    }

    /// Creates storage from an existing AWS SDK client.
    ///
    /// Useful for testing with a pre-configured client.
    ///
    /// # Panics
    ///
    /// Panics if the bucket name is empty.
    #[must_use]
    pub fn from_client(client: aws_sdk_s3::Client, config: S3Config) -> Self {
        assert!(!config.bucket.is_empty(), "bucket name must not be empty");
        Self { client, config }
    }

    /// Returns the full S3 key for an object key.
    fn full_key(&self, key: &ObjectKey) -> String {
        format!("{}{}", self.config.key_prefix, key.as_str())
    }

    /// Returns a reference to the bucket name.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.config.bucket
    }

    /// Returns a reference to the key prefix.
    #[must_use]
    pub fn key_prefix(&self) -> &str {
        &self.config.key_prefix
    }
}

// -----------------------------------------------------------------------------
// ObjectStorage Implementation
// -----------------------------------------------------------------------------

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
            .storage_class(self.config.storage_class.to_sdk_class())
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

        let response = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| {
                if is_not_found_error(&e) {
                    TierError::NotFound {
                        key: key.to_string(),
                    }
                } else {
                    TierError::DownloadFailed {
                        key: key.to_string(),
                        message: format!("S3 GetObject failed: {e}"),
                    }
                }
            })?;

        let bytes = response
            .body
            .collect()
            .await
            .map_err(|e| TierError::DownloadFailed {
                key: key.to_string(),
                message: format!("failed to read S3 response body: {e}"),
            })?
            .into_bytes();

        Ok(bytes)
    }

    async fn delete(&self, key: &ObjectKey) -> TierResult<()> {
        assert!(!key.as_str().is_empty(), "key must not be empty");

        let full_key = self.full_key(key);

        // S3 delete is idempotent - doesn't error if key doesn't exist.
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
            let mut request = self
                .client
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
                        // Strip our prefix to get the ObjectKey.
                        if key.len() > prefix_len {
                            keys.push(ObjectKey::new(&key[prefix_len..]));
                        }
                    }
                }
            }

            if response.is_truncated.unwrap_or(false) {
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

        match self
            .client
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

// -----------------------------------------------------------------------------
// Helper Functions
// -----------------------------------------------------------------------------

/// Checks if an AWS SDK error indicates a "not found" condition.
fn is_not_found_error<E>(err: &SdkError<E, HttpResponse>) -> bool {
    match err {
        SdkError::ServiceError(service_err) => {
            // AWS SDK provides raw HTTP response which has the status code.
            service_err.raw().status().as_u16() == 404
        }
        _ => false,
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = S3Config::default();
        assert!(config.bucket.is_empty());
        assert_eq!(config.key_prefix, "helix/segments/");
        assert!(!config.force_path_style);
        assert_eq!(config.storage_class, S3StorageClass::Standard);
    }

    #[test]
    fn test_config_new() {
        let config = S3Config::new("my-bucket");
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.key_prefix, "helix/segments/");
    }

    #[test]
    fn test_config_builder() {
        let config = S3Config::new("my-bucket")
            .with_prefix("custom/prefix/")
            .with_endpoint("http://localhost:4566")
            .with_path_style()
            .with_region("us-west-2")
            .with_storage_class(S3StorageClass::StandardIa);

        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.key_prefix, "custom/prefix/");
        assert_eq!(config.endpoint_url, Some("http://localhost:4566".to_string()));
        assert!(config.force_path_style);
        assert_eq!(config.region, Some("us-west-2".to_string()));
        assert_eq!(config.storage_class, S3StorageClass::StandardIa);
    }

    #[test]
    fn test_storage_class_to_sdk() {
        assert_eq!(
            S3StorageClass::Standard.to_sdk_class(),
            aws_sdk_s3::types::StorageClass::Standard
        );
        assert_eq!(
            S3StorageClass::StandardIa.to_sdk_class(),
            aws_sdk_s3::types::StorageClass::StandardIa
        );
        assert_eq!(
            S3StorageClass::IntelligentTiering.to_sdk_class(),
            aws_sdk_s3::types::StorageClass::IntelligentTiering
        );
        assert_eq!(
            S3StorageClass::Glacier.to_sdk_class(),
            aws_sdk_s3::types::StorageClass::Glacier
        );
    }
}
