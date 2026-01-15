//! `LocalStack` integration tests for `S3ObjectStorage`.
//!
//! These tests require `LocalStack` to be running. Start it with:
//!
//! ```bash
//! docker run --rm -p 4566:4566 localstack/localstack
//! ```
//!
//! Create the test bucket:
//!
//! ```bash
//! aws --endpoint-url=http://localhost:4566 s3 mb s3://helix-test
//! ```
//!
//! Run tests with:
//!
//! ```bash
//! cargo test -p helix-tier --features s3 --test s3_integration -- --ignored
//! ```

#![cfg(feature = "s3")]

use bytes::Bytes;
use helix_tier::{ObjectKey, ObjectStorage, S3Config, S3ObjectStorage};

const TEST_BUCKET: &str = "helix-test";
const LOCALSTACK_ENDPOINT: &str = "http://localhost:4566";

fn localstack_config() -> S3Config {
    S3Config::new(TEST_BUCKET)
        .with_endpoint(LOCALSTACK_ENDPOINT)
        .with_path_style()
        .with_region("us-east-1")
        .with_prefix("test/")
}

async fn create_storage() -> S3ObjectStorage {
    let config = localstack_config();
    S3ObjectStorage::new(config)
        .await
        .expect("Failed to create S3 storage")
}

// -----------------------------------------------------------------------------
// Basic Operations
// -----------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_s3_put_get_roundtrip() {
    let storage = create_storage().await;
    let key = ObjectKey::new("integration/test-roundtrip.bin");
    let data = Bytes::from_static(b"Hello, S3!");

    // Put.
    storage.put(&key, data.clone()).await.expect("put failed");

    // Get.
    let retrieved = storage.get(&key).await.expect("get failed");
    assert_eq!(retrieved, data);

    // Cleanup.
    storage.delete(&key).await.expect("delete failed");
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_s3_exists() {
    let storage = create_storage().await;
    let key = ObjectKey::new("integration/test-exists.bin");
    let data = Bytes::from_static(b"exists test");

    // Should not exist initially.
    assert!(!storage.exists(&key).await.expect("exists check failed"));

    // Put the object.
    storage.put(&key, data).await.expect("put failed");

    // Should exist now.
    assert!(storage.exists(&key).await.expect("exists check failed"));

    // Cleanup.
    storage.delete(&key).await.expect("delete failed");

    // Should not exist after delete.
    assert!(!storage.exists(&key).await.expect("exists check failed"));
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_s3_get_not_found() {
    let storage = create_storage().await;
    let key = ObjectKey::new("integration/nonexistent-key.bin");

    let result = storage.get(&key).await;
    assert!(result.is_err());

    let err = result.unwrap_err();
    match err {
        helix_tier::TierError::NotFound { key: k } => {
            assert!(k.contains("nonexistent-key.bin"));
        }
        _ => panic!("Expected NotFound error, got: {err:?}"),
    }
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_s3_delete_idempotent() {
    let storage = create_storage().await;
    let key = ObjectKey::new("integration/delete-idempotent.bin");

    // Deleting non-existent key should succeed (S3 is idempotent).
    storage.delete(&key).await.expect("first delete failed");
    storage.delete(&key).await.expect("second delete failed");
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_s3_overwrite() {
    let storage = create_storage().await;
    let key = ObjectKey::new("integration/overwrite.bin");
    let data1 = Bytes::from_static(b"version 1");
    let data2 = Bytes::from_static(b"version 2 - longer");

    // Put first version.
    storage.put(&key, data1).await.expect("put v1 failed");

    // Overwrite with second version.
    storage.put(&key, data2.clone()).await.expect("put v2 failed");

    // Should get second version.
    let retrieved = storage.get(&key).await.expect("get failed");
    assert_eq!(retrieved, data2);

    // Cleanup.
    storage.delete(&key).await.expect("delete failed");
}

// -----------------------------------------------------------------------------
// List Operations
// -----------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_s3_list_with_prefix() {
    let storage = create_storage().await;

    // Create some test objects with a unique prefix for this test.
    let prefix = "integration/list-test/";
    let keys = [
        ObjectKey::new(format!("{prefix}a.bin")),
        ObjectKey::new(format!("{prefix}b.bin")),
        ObjectKey::new(format!("{prefix}subdir/c.bin")),
    ];

    for key in &keys {
        storage
            .put(key, Bytes::from_static(b"test"))
            .await
            .expect("put failed");
    }

    // List with prefix.
    let listed = storage.list(prefix).await.expect("list failed");

    assert_eq!(listed.len(), 3);
    for key in &keys {
        let key_str = key.as_str();
        assert!(
            listed.iter().any(|k: &ObjectKey| k.as_str() == key_str),
            "Expected to find {key:?} in list"
        );
    }

    // Cleanup.
    for key in &keys {
        storage.delete(key).await.expect("delete failed");
    }
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_s3_list_empty() {
    let storage = create_storage().await;

    // List with a prefix that doesn't match anything.
    let listed = storage
        .list("integration/nonexistent-prefix/")
        .await
        .expect("list failed");

    assert!(listed.is_empty());
}

// -----------------------------------------------------------------------------
// Large Data
// -----------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_s3_large_object() {
    let storage = create_storage().await;
    let key = ObjectKey::new("integration/large-object.bin");

    // Create a 1 MiB object.
    let size: usize = 1024 * 1024;
    #[allow(clippy::cast_possible_truncation)]
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    let data = Bytes::from(data);

    // Put.
    storage.put(&key, data.clone()).await.expect("put failed");

    // Get.
    let retrieved = storage.get(&key).await.expect("get failed");
    assert_eq!(retrieved.len(), size);
    assert_eq!(retrieved, data);

    // Cleanup.
    storage.delete(&key).await.expect("delete failed");
}

// -----------------------------------------------------------------------------
// Concurrent Operations
// -----------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_s3_concurrent_puts() {
    let storage = create_storage().await;

    // Concurrently put multiple objects.
    let tasks: Vec<_> = (0..10)
        .map(|i| {
            let storage = storage.clone();
            let key = ObjectKey::new(format!("integration/concurrent/{i}.bin"));
            let data = Bytes::from(format!("data-{i}"));
            tokio::spawn(async move {
                storage.put(&key, data).await.expect("put failed");
                key
            })
        })
        .collect();

    // Wait for all puts to complete.
    let results = futures::future::join_all(tasks).await;
    let keys: Vec<ObjectKey> = results
        .into_iter()
        .map(|r: Result<ObjectKey, _>| r.expect("task failed"))
        .collect();

    // Verify all objects exist.
    for key in &keys {
        assert!(storage.exists(key).await.expect("exists check failed"));
    }

    // Cleanup.
    for key in &keys {
        storage.delete(key).await.expect("delete failed");
    }
}

// -----------------------------------------------------------------------------
// Configuration Tests
// -----------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_s3_custom_prefix() {
    let config = S3Config::new(TEST_BUCKET)
        .with_endpoint(LOCALSTACK_ENDPOINT)
        .with_path_style()
        .with_region("us-east-1")
        .with_prefix("custom/prefix/");

    let storage = S3ObjectStorage::new(config)
        .await
        .expect("Failed to create S3 storage");

    let key = ObjectKey::new("test-key.bin");
    let data = Bytes::from_static(b"prefix test");

    // Put with custom prefix.
    storage.put(&key, data.clone()).await.expect("put failed");

    // Verify it exists.
    assert!(storage.exists(&key).await.expect("exists check failed"));

    // Get and verify.
    let retrieved = storage.get(&key).await.expect("get failed");
    assert_eq!(retrieved, data);

    // Cleanup.
    storage.delete(&key).await.expect("delete failed");
}
