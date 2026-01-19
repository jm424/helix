# E2E Tiering Tests Design

**Status**: Design
**Author**: Claude + Jai
**Created**: 2026-01-15

## Overview

This document designs end-to-end tests for Helix tiered storage that exercise the full stack:
Kafka client → helix-server → WAL → TieringManager → S3/Filesystem backend.

These tests complement existing coverage:
- **Unit tests**: `S3ObjectStorage`, `FilesystemObjectStorage` in isolation
- **LocalStack integration**: S3 API correctness
- **DST tests**: `TieringManager` with fault injection

What's missing: **Full-stack e2e tests** that verify tiering works correctly through the Kafka protocol with real (or LocalStack) S3.

## Goals

1. **Verify data durability**: Data written via Kafka survives tiering to S3
2. **Verify backfill reads**: Data can be fetched from S3 after local eviction
3. **Verify fault tolerance**: Tiering handles S3 failures, node restarts gracefully
4. **Verify progress tracking**: Eviction respects consumer progress
5. **Deterministic verification**: Same workload seed produces reproducible results

## Non-Goals

1. **Performance benchmarking**: Focus on correctness, not throughput
2. **S3 cost optimization**: Use LocalStack, no real AWS costs
3. **Multi-region replication**: Single-region S3 only

---

## Test Infrastructure

### 1. RealClusterConfig Extensions

Add tiering configuration to `helix-workload/src/executor.rs`:

```rust
/// Configuration for a real Helix cluster.
#[derive(Debug, Clone)]
pub struct RealClusterConfig {
    // ... existing fields ...

    // === Tiering Configuration ===

    /// Filesystem object storage directory.
    /// Mutually exclusive with `s3_config`.
    pub object_storage_dir: Option<PathBuf>,

    /// S3 configuration for tiered storage.
    /// Mutually exclusive with `object_storage_dir`.
    pub s3_config: Option<S3TieringConfig>,

    /// Tiering thresholds for testing.
    pub tiering_config: TieringTestConfig,
}

/// S3 tiering configuration for tests.
#[derive(Debug, Clone)]
pub struct S3TieringConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// S3 key prefix.
    pub prefix: String,
    /// S3 region.
    pub region: String,
    /// Custom endpoint (for LocalStack/MinIO).
    pub endpoint: Option<String>,
    /// Force path-style addressing (required for LocalStack).
    pub force_path_style: bool,
}

impl S3TieringConfig {
    /// Create config for LocalStack testing.
    pub fn localstack(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: "helix/segments/".to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some("http://localhost:4566".to_string()),
            force_path_style: true,
        }
    }
}

/// Tiering behavior configuration for tests.
#[derive(Debug, Clone)]
pub struct TieringTestConfig {
    /// Minimum segment age before tiering (0 for immediate).
    pub min_age_secs: u64,
    /// Segment size to trigger rotation (bytes).
    pub segment_size_bytes: u64,
    /// Whether to auto-evict after tiering.
    pub auto_evict: bool,
    /// Eviction delay after tiering completes (secs).
    pub eviction_delay_secs: u64,
}

impl Default for TieringTestConfig {
    fn default() -> Self {
        Self {
            min_age_secs: 0,           // Immediate tiering for tests
            segment_size_bytes: 1024 * 1024, // 1 MiB segments
            auto_evict: false,         // Manual eviction control
            eviction_delay_secs: 0,
        }
    }
}

impl TieringTestConfig {
    /// Config for fast tiering tests (small segments, immediate tier).
    pub fn fast() -> Self {
        Self {
            min_age_secs: 0,
            segment_size_bytes: 64 * 1024, // 64 KiB for fast rotation
            auto_evict: false,
            eviction_delay_secs: 0,
        }
    }
}
```

### 2. RealClusterBuilder Extensions

```rust
impl RealClusterBuilder {
    // ... existing methods ...

    /// Enable filesystem-based tiering.
    pub fn with_filesystem_tiering(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config.object_storage_dir = Some(dir.into());
        self.config.s3_config = None;
        self
    }

    /// Enable S3-based tiering (LocalStack).
    pub fn with_s3_tiering(mut self, config: S3TieringConfig) -> Self {
        self.config.s3_config = Some(config);
        self.config.object_storage_dir = None;
        self
    }

    /// Enable LocalStack S3 tiering with default config.
    pub fn with_localstack_tiering(self, bucket: &str) -> Self {
        self.with_s3_tiering(S3TieringConfig::localstack(bucket))
    }

    /// Set tiering behavior config.
    pub fn with_tiering_config(mut self, config: TieringTestConfig) -> Self {
        self.config.tiering_config = config;
        self
    }
}
```

### 3. CLI Flag Propagation

Update `RealCluster::start()` to pass tiering flags:

```rust
fn start(config: RealClusterConfig) -> Result<Self, ExecutorError> {
    // ... existing setup ...

    for node_id in 1..=config.node_count {
        let mut cmd = Command::new(&config.binary_path);

        // ... existing args ...

        // Tiering configuration
        if let Some(ref dir) = config.object_storage_dir {
            let node_tier_dir = dir.join(format!("node-{node_id}"));
            std::fs::create_dir_all(&node_tier_dir)
                .map_err(ExecutorError::SpawnFailed)?;
            cmd.arg("--object-storage-dir").arg(&node_tier_dir);
        }

        if let Some(ref s3) = config.s3_config {
            cmd.arg("--s3-bucket").arg(&s3.bucket);
            cmd.arg("--s3-prefix").arg(format!("{}{}/", s3.prefix, node_id));
            cmd.arg("--s3-region").arg(&s3.region);
            if let Some(ref endpoint) = s3.endpoint {
                cmd.arg("--s3-endpoint").arg(endpoint);
            }
            if s3.force_path_style {
                cmd.arg("--s3-force-path-style");
            }
        }

        // Tiering thresholds (requires helix-server CLI additions)
        cmd.arg("--tier-min-age-secs")
            .arg(config.tiering_config.min_age_secs.to_string());
        cmd.arg("--segment-size-bytes")
            .arg(config.tiering_config.segment_size_bytes.to_string());

        // ... spawn process ...
    }
}
```

### 4. Tiering Observability Hooks

Add methods to `RealCluster` for tiering verification:

```rust
impl RealCluster {
    /// Wait for segments to be tiered to S3.
    /// Polls until at least `min_segments` have location=Both or Remote.
    pub async fn wait_for_tiering(
        &self,
        topic: &str,
        partition: i32,
        min_segments: u32,
        timeout: Duration,
    ) -> Result<TieringStatus, ExecutorError>;

    /// Force eviction of tiered segments (admin API call).
    pub async fn force_eviction(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<u32, ExecutorError>;

    /// Get tiering status for a partition.
    pub async fn get_tiering_status(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<TieringStatus, ExecutorError>;

    /// Verify data exists in S3 (direct S3 check, bypasses helix-server).
    pub async fn verify_s3_data(
        &self,
        segment_ids: &[u64],
    ) -> Result<S3VerificationResult, ExecutorError>;
}

#[derive(Debug, Clone)]
pub struct TieringStatus {
    /// Segments with location=Local only.
    pub local_only: u32,
    /// Segments with location=Both (local + S3).
    pub tiered: u32,
    /// Segments with location=Remote only (evicted).
    pub evicted: u32,
    /// Total bytes tiered.
    pub tiered_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct S3VerificationResult {
    /// Segments found in S3.
    pub found: Vec<u64>,
    /// Segments missing from S3.
    pub missing: Vec<u64>,
    /// Total bytes in S3.
    pub total_bytes: u64,
}
```

---

## Required helix-server Changes

### 1. New CLI Flags

Add to `helix-server/src/main.rs`:

```rust
/// Minimum segment age before tiering (seconds, default: 3600).
#[arg(long, default_value = "3600")]
tier_min_age_secs: u64,

/// Segment size to trigger rotation (bytes, default: 64MiB).
#[arg(long, default_value = "67108864")]
segment_size_bytes: u64,
```

### 2. Admin API for Tiering Status

Add gRPC/Kafka admin API endpoints:

```protobuf
// TieringStatus RPC
message GetTieringStatusRequest {
    string topic = 1;
    int32 partition = 2;
}

message GetTieringStatusResponse {
    uint32 local_only_segments = 1;
    uint32 tiered_segments = 2;
    uint32 evicted_segments = 3;
    uint64 tiered_bytes = 4;
}

// ForceEviction RPC (for testing)
message ForceEvictionRequest {
    string topic = 1;
    int32 partition = 2;
    uint64 safe_offset = 3;  // Evict segments below this offset
}

message ForceEvictionResponse {
    uint32 evicted_count = 1;
}
```

---

## Test Scenarios

### Scenario 1: Basic Tiering Roundtrip

**Goal**: Verify data written via Kafka is tiered to S3 and readable after tiering.

```rust
#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_basic_tiering_roundtrip() {
    // Setup: LocalStack with small segments for fast rotation.
    ensure_localstack_running().await;
    create_s3_bucket("helix-e2e-test").await;

    let cluster = RealCluster::builder()
        .nodes(3)
        .base_port(19092)
        .with_localstack_tiering("helix-e2e-test")
        .with_tiering_config(TieringTestConfig::fast())
        .topic("tier-test", 1)
        .build()
        .expect("cluster start failed");

    let executor = RealExecutor::new(&cluster).unwrap();
    executor.wait_ready(Duration::from_secs(30)).await.unwrap();

    // Phase 1: Write enough data to trigger segment rotation + tiering.
    // With 64 KiB segments and 1 KiB messages, ~64 messages per segment.
    let topic = "tier-test";
    let partition = 0;
    let message_count = 200; // ~3 segments
    let mut acknowledged: Vec<(u64, Bytes)> = Vec::new();

    eprintln!("Phase 1: Writing {message_count} messages...");
    for i in 0..message_count {
        let payload = Bytes::from(format!("message-{i:06}"));
        let offset = executor.send(topic, partition, payload.clone()).await.unwrap();
        acknowledged.push((offset, payload));
    }
    eprintln!("  Acknowledged {} messages", acknowledged.len());

    // Phase 2: Wait for tiering to complete.
    eprintln!("Phase 2: Waiting for tiering...");
    let status = cluster
        .wait_for_tiering(topic, partition, 2, Duration::from_secs(60))
        .await
        .expect("tiering timeout");
    eprintln!("  Tiered {} segments ({} bytes)", status.tiered, status.tiered_bytes);
    assert!(status.tiered >= 2, "Expected at least 2 tiered segments");

    // Phase 3: Verify data readable (may come from local or S3).
    eprintln!("Phase 3: Verifying data...");
    let messages = executor
        .poll(topic, partition, 0, message_count as u32)
        .await
        .unwrap();

    assert_eq!(messages.len(), acknowledged.len());
    for (expected, actual) in acknowledged.iter().zip(messages.iter()) {
        assert_eq!(expected.0, actual.0, "offset mismatch");
        assert_eq!(expected.1, actual.1, "payload mismatch");
    }
    eprintln!("  Verified {} messages", messages.len());

    // Cleanup.
    delete_s3_bucket("helix-e2e-test").await;
}
```

### Scenario 2: Backfill Read from S3 After Eviction

**Goal**: Verify data is readable from S3 after local eviction.

```rust
#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_backfill_read_after_eviction() {
    ensure_localstack_running().await;
    create_s3_bucket("helix-e2e-backfill").await;

    let cluster = RealCluster::builder()
        .nodes(3)
        .base_port(19192)
        .with_localstack_tiering("helix-e2e-backfill")
        .with_tiering_config(TieringTestConfig::fast())
        .topic("backfill-test", 1)
        .build()
        .expect("cluster start failed");

    let executor = RealExecutor::new(&cluster).unwrap();
    executor.wait_ready(Duration::from_secs(30)).await.unwrap();

    let topic = "backfill-test";
    let partition = 0;
    let message_count = 200;
    let mut acknowledged: Vec<(u64, Bytes)> = Vec::new();

    // Phase 1: Write data.
    eprintln!("Phase 1: Writing {message_count} messages...");
    for i in 0..message_count {
        let payload = Bytes::from(format!("backfill-{i:06}"));
        let offset = executor.send(topic, partition, payload.clone()).await.unwrap();
        acknowledged.push((offset, payload));
    }

    // Phase 2: Wait for tiering.
    eprintln!("Phase 2: Waiting for tiering...");
    cluster
        .wait_for_tiering(topic, partition, 2, Duration::from_secs(60))
        .await
        .expect("tiering timeout");

    // Phase 3: Force eviction of tiered segments.
    eprintln!("Phase 3: Forcing eviction...");
    let evicted = cluster.force_eviction(topic, partition).await.unwrap();
    eprintln!("  Evicted {} segments", evicted);
    assert!(evicted >= 2, "Expected at least 2 evicted segments");

    // Phase 4: Verify tiering status shows evicted segments.
    let status = cluster.get_tiering_status(topic, partition).await.unwrap();
    assert!(status.evicted >= 2, "Expected evicted segments");
    eprintln!("  Status: {} evicted, {} local", status.evicted, status.local_only);

    // Phase 5: Read data - should fetch from S3.
    eprintln!("Phase 5: Reading data (should backfill from S3)...");
    let messages = executor
        .poll(topic, partition, 0, message_count as u32)
        .await
        .unwrap();

    // Verify all data.
    assert_eq!(messages.len(), acknowledged.len());
    for (expected, actual) in acknowledged.iter().zip(messages.iter()) {
        assert_eq!(expected.0, actual.0, "offset mismatch at {}", expected.0);
        assert_eq!(expected.1, actual.1, "payload mismatch at {}", expected.0);
    }
    eprintln!("  Verified {} messages from S3 backfill", messages.len());

    delete_s3_bucket("helix-e2e-backfill").await;
}
```

### Scenario 3: Node Restart with Tiered Data

**Goal**: Verify data survives node restart and is fetchable from S3.

```rust
#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_node_restart_with_tiered_data() {
    ensure_localstack_running().await;
    create_s3_bucket("helix-e2e-restart").await;

    let mut cluster = RealCluster::builder()
        .nodes(3)
        .base_port(19292)
        .with_localstack_tiering("helix-e2e-restart")
        .with_tiering_config(TieringTestConfig::fast())
        .topic("restart-test", 1)
        .build()
        .expect("cluster start failed");

    let executor = RealExecutor::new(&cluster).unwrap();
    executor.wait_ready(Duration::from_secs(30)).await.unwrap();

    let topic = "restart-test";
    let partition = 0;
    let message_count = 200;
    let mut acknowledged: Vec<(u64, Bytes)> = Vec::new();

    // Phase 1: Write data and wait for tiering.
    eprintln!("Phase 1: Writing and tiering...");
    for i in 0..message_count {
        let payload = Bytes::from(format!("restart-{i:06}"));
        let offset = executor.send(topic, partition, payload.clone()).await.unwrap();
        acknowledged.push((offset, payload));
    }
    cluster
        .wait_for_tiering(topic, partition, 2, Duration::from_secs(60))
        .await
        .unwrap();

    // Phase 2: Kill leader and wait for new leader.
    eprintln!("Phase 2: Killing leader...");
    let leader = cluster.wait_for_leader(topic, partition, Duration::from_secs(10)).await.unwrap();
    cluster.kill_node(leader).unwrap();

    // Wait for new leader election.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let new_leader = cluster
        .wait_for_leader(topic, partition, Duration::from_secs(30))
        .await
        .unwrap();
    eprintln!("  New leader: node {new_leader}");

    // Phase 3: Verify data still readable.
    eprintln!("Phase 3: Verifying data after failover...");
    let executor = RealExecutor::new(&cluster).unwrap();
    let messages = executor
        .poll(topic, partition, 0, message_count as u32)
        .await
        .unwrap();

    assert_eq!(messages.len(), acknowledged.len());
    eprintln!("  Verified {} messages after leader failover", messages.len());

    // Phase 4: Full cluster restart.
    eprintln!("Phase 4: Full cluster restart...");
    cluster.stop();
    tokio::time::sleep(Duration::from_secs(2)).await;
    cluster.restart_all().unwrap();

    let executor = RealExecutor::new(&cluster).unwrap();
    executor.wait_ready(Duration::from_secs(60)).await.unwrap();
    cluster
        .wait_for_leader(topic, partition, Duration::from_secs(60))
        .await
        .unwrap();

    // Phase 5: Verify data survives full restart.
    eprintln!("Phase 5: Verifying data after full restart...");
    let messages = executor
        .poll(topic, partition, 0, message_count as u32)
        .await
        .unwrap();

    assert_eq!(messages.len(), acknowledged.len());
    for (expected, actual) in acknowledged.iter().zip(messages.iter()) {
        assert_eq!(expected.0, actual.0, "offset mismatch");
        assert_eq!(expected.1, actual.1, "payload mismatch");
    }
    eprintln!("  Verified {} messages after full cluster restart", messages.len());

    delete_s3_bucket("helix-e2e-restart").await;
}
```

### Scenario 4: S3 Failure During Tiering

**Goal**: Verify tiering retries on S3 failures and eventually succeeds.

```rust
#[tokio::test]
#[ignore = "requires LocalStack with fault injection"]
async fn test_s3_failure_during_tiering() {
    // This test requires LocalStack Pro or a mock S3 with fault injection.
    // For now, we can test with filesystem backend and disk faults.

    let tier_dir = tempfile::tempdir().unwrap();
    let mut cluster = RealCluster::builder()
        .nodes(3)
        .base_port(19392)
        .with_filesystem_tiering(tier_dir.path())
        .with_tiering_config(TieringTestConfig::fast())
        .topic("fault-test", 1)
        .build()
        .expect("cluster start failed");

    let executor = RealExecutor::new(&cluster).unwrap();
    executor.wait_ready(Duration::from_secs(30)).await.unwrap();

    let topic = "fault-test";
    let partition = 0;
    let message_count = 100;
    let mut acknowledged: Vec<(u64, Bytes)> = Vec::new();

    // Phase 1: Write data.
    for i in 0..message_count {
        let payload = Bytes::from(format!("fault-{i:06}"));
        let offset = executor.send(topic, partition, payload.clone()).await.unwrap();
        acknowledged.push((offset, payload));
    }

    // Phase 2: Simulate storage fault by making tier dir read-only.
    eprintln!("Phase 2: Simulating storage fault...");
    std::fs::set_permissions(tier_dir.path(), std::fs::Permissions::from_mode(0o444)).unwrap();

    // Try to trigger tiering - should fail but not crash.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Phase 3: Restore permissions.
    eprintln!("Phase 3: Restoring storage access...");
    std::fs::set_permissions(tier_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

    // Phase 4: Wait for tiering to succeed after recovery.
    eprintln!("Phase 4: Waiting for tiering after recovery...");
    cluster
        .wait_for_tiering(topic, partition, 1, Duration::from_secs(60))
        .await
        .expect("tiering should succeed after recovery");

    // Phase 5: Verify data.
    let messages = executor
        .poll(topic, partition, 0, message_count as u32)
        .await
        .unwrap();
    assert_eq!(messages.len(), acknowledged.len());
}
```

### Scenario 5: Progress-Aware Eviction

**Goal**: Verify eviction respects consumer progress (doesn't evict unread data).

```rust
#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_progress_aware_eviction() {
    ensure_localstack_running().await;
    create_s3_bucket("helix-e2e-progress").await;

    let cluster = RealCluster::builder()
        .nodes(3)
        .base_port(19492)
        .with_localstack_tiering("helix-e2e-progress")
        .with_tiering_config(TieringTestConfig::fast())
        .topic("progress-test", 1)
        .build()
        .expect("cluster start failed");

    let executor = RealExecutor::new(&cluster).unwrap();
    executor.wait_ready(Duration::from_secs(30)).await.unwrap();

    let topic = "progress-test";
    let partition = 0;
    let message_count = 300; // ~4-5 segments

    // Phase 1: Write data.
    eprintln!("Phase 1: Writing {message_count} messages...");
    let mut acknowledged: Vec<(u64, Bytes)> = Vec::new();
    for i in 0..message_count {
        let payload = Bytes::from(format!("progress-{i:06}"));
        let offset = executor.send(topic, partition, payload.clone()).await.unwrap();
        acknowledged.push((offset, payload));
    }

    // Phase 2: Wait for tiering.
    cluster
        .wait_for_tiering(topic, partition, 3, Duration::from_secs(60))
        .await
        .unwrap();

    // Phase 3: Consumer reads only first 100 messages and commits.
    eprintln!("Phase 3: Consumer reads first 100 messages...");
    let messages = executor
        .poll(topic, partition, 0, 100)
        .await
        .unwrap();
    assert_eq!(messages.len(), 100);

    // Commit offset 100 (consumer has processed 0-99).
    executor
        .commit("test-group", topic, partition, 100)
        .await
        .unwrap();

    // Phase 4: Request eviction with progress tracking.
    // Only segments with end_offset < 100 should be evicted.
    eprintln!("Phase 4: Requesting progress-aware eviction...");
    let evicted = cluster
        .evict_with_progress(topic, partition, "test-group")
        .await
        .unwrap();
    eprintln!("  Evicted {} segments", evicted);

    // Phase 5: Verify segments with offsets >= 100 are still local.
    let status = cluster.get_tiering_status(topic, partition).await.unwrap();
    eprintln!("  Status: {} local, {} evicted", status.local_only + status.tiered, status.evicted);

    // Phase 6: Consumer can still read remaining messages (100-299).
    eprintln!("Phase 6: Reading remaining messages...");
    let remaining = executor
        .poll(topic, partition, 100, 200)
        .await
        .unwrap();
    assert_eq!(remaining.len(), 200);
    eprintln!("  Read {} remaining messages", remaining.len());

    delete_s3_bucket("helix-e2e-progress").await;
}
```

### Scenario 6: Large-Scale Tiering Stress Test

**Goal**: Verify tiering handles many segments and large data volumes.

```rust
#[tokio::test]
#[ignore = "requires LocalStack, long-running"]
async fn test_large_scale_tiering() {
    ensure_localstack_running().await;
    create_s3_bucket("helix-e2e-scale").await;

    let cluster = RealCluster::builder()
        .nodes(3)
        .base_port(19592)
        .with_localstack_tiering("helix-e2e-scale")
        .with_tiering_config(TieringTestConfig {
            min_age_secs: 0,
            segment_size_bytes: 256 * 1024, // 256 KiB segments
            auto_evict: false,
            eviction_delay_secs: 0,
        })
        .topic("scale-test", 4) // 4 partitions
        .build()
        .expect("cluster start failed");

    let executor = RealExecutor::new(&cluster).unwrap();
    executor.wait_ready(Duration::from_secs(30)).await.unwrap();

    // Write 10 MiB per partition = 40 MiB total = ~160 segments.
    let bytes_per_partition = 10 * 1024 * 1024;
    let message_size = 4 * 1024; // 4 KiB messages
    let messages_per_partition = bytes_per_partition / message_size;

    eprintln!("Writing {} messages per partition ({} MiB each)...",
        messages_per_partition, bytes_per_partition / 1024 / 1024);

    let mut handles = Vec::new();
    for partition in 0..4 {
        let executor = executor.clone();
        let handle = tokio::spawn(async move {
            let mut acked = Vec::new();
            for i in 0..messages_per_partition {
                let payload = Bytes::from(vec![partition as u8; message_size]);
                match executor.send("scale-test", partition, payload.clone()).await {
                    Ok(offset) => acked.push((offset, payload)),
                    Err(e) => eprintln!("  Error on p{partition} msg {i}: {e}"),
                }
            }
            (partition, acked)
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Wait for tiering across all partitions.
    eprintln!("Waiting for tiering...");
    for partition in 0..4 {
        cluster
            .wait_for_tiering("scale-test", partition, 10, Duration::from_secs(120))
            .await
            .expect(&format!("tiering timeout for partition {partition}"));
    }

    // Verify total tiered status.
    let mut total_tiered = 0;
    let mut total_bytes = 0;
    for partition in 0..4 {
        let status = cluster.get_tiering_status("scale-test", partition).await.unwrap();
        total_tiered += status.tiered;
        total_bytes += status.tiered_bytes;
    }
    eprintln!("Total: {} segments tiered, {} MiB", total_tiered, total_bytes / 1024 / 1024);
    assert!(total_tiered >= 100, "Expected at least 100 tiered segments");

    // Verify data integrity.
    for (partition, acked) in &results {
        let messages = executor
            .poll("scale-test", *partition, 0, acked.len() as u32)
            .await
            .unwrap();
        assert_eq!(messages.len(), acked.len(), "partition {partition} message count mismatch");
    }

    delete_s3_bucket("helix-e2e-scale").await;
}
```

---

## Test Utilities

### LocalStack Helpers

```rust
/// Ensures LocalStack is running, or skips the test.
async fn ensure_localstack_running() {
    let client = reqwest::Client::new();
    match client.get("http://localhost:4566/_localstack/health").send().await {
        Ok(resp) if resp.status().is_success() => {}
        _ => {
            eprintln!("LocalStack not running. Start with:");
            eprintln!("  docker run --rm -p 4566:4566 localstack/localstack");
            panic!("LocalStack required for this test");
        }
    }
}

/// Create an S3 bucket in LocalStack.
async fn create_s3_bucket(bucket: &str) {
    let output = Command::new("aws")
        .args(["--endpoint-url", "http://localhost:4566", "s3", "mb", &format!("s3://{bucket}")])
        .output()
        .expect("aws cli failed");
    if !output.status.success() {
        // Bucket may already exist, ignore error.
    }
}

/// Delete an S3 bucket in LocalStack (including all objects).
async fn delete_s3_bucket(bucket: &str) {
    let _ = Command::new("aws")
        .args(["--endpoint-url", "http://localhost:4566", "s3", "rb", "--force", &format!("s3://{bucket}")])
        .output();
}
```

---

## Verification Additions

Extend `helix-workload/src/verification.rs` with tiering-specific violations:

```rust
/// A violation of an expected property.
#[derive(Debug, Clone)]
pub enum Violation {
    // ... existing variants ...

    /// Data lost during tiering (acknowledged but not readable after tier).
    TieringDataLoss {
        topic: String,
        partition: i32,
        offset: u64,
        payload: Bytes,
        tier_status: String,
    },

    /// Data corruption after tiering (checksum mismatch).
    TieringCorruption {
        topic: String,
        partition: i32,
        offset: u64,
        expected_crc: u64,
        actual_crc: u64,
    },

    /// Segment evicted before consumer processed it.
    PrematureEviction {
        topic: String,
        partition: i32,
        segment_id: u64,
        evicted_offset: u64,
        consumer_offset: u64,
    },

    /// Tiering stuck (segment eligible but not tiered after timeout).
    TieringStuck {
        topic: String,
        partition: i32,
        segment_id: u64,
        eligible_since_secs: u64,
    },
}
```

---

## Implementation Plan

### Phase 1: Infrastructure (helix-workload)
1. Add `S3TieringConfig` and `TieringTestConfig` to `RealClusterConfig`
2. Add builder methods to `RealClusterBuilder`
3. Update `RealCluster::start()` to pass tiering CLI flags
4. Add `wait_for_tiering()`, `force_eviction()`, `get_tiering_status()`

### Phase 2: helix-server Changes
1. Add `--tier-min-age-secs` and `--segment-size-bytes` CLI flags
2. Add admin API endpoints for tiering status and forced eviction
3. Expose tiering metrics (segments tiered, bytes tiered, eviction count)

### Phase 3: Test Utilities
1. Add LocalStack helper functions
2. Add S3 verification utilities (direct bucket inspection)
3. Add tiering-specific `Violation` variants

### Phase 4: Test Scenarios
1. Implement basic tiering roundtrip test
2. Implement backfill read after eviction test
3. Implement node restart with tiered data test
4. Implement progress-aware eviction test
5. Implement large-scale stress test

### Phase 5: Documentation & Runbook
1. Document how to run e2e tiering tests locally
2. Add troubleshooting guide for LocalStack issues
3. Document test artifacts (logs, metrics) locations

**Note**: CI pipeline is deferred per project guidelines. Tests run locally for now.

---

## Open Questions

1. **Tiering trigger mechanism**: Should tiering be time-based (background task) or explicit (admin API)?
   - Recommendation: Support both. Time-based for production, explicit for tests.

2. **Eviction safety**: How to handle eviction when consumer offset is unknown?
   - Recommendation: Never evict if no consumer offset exists (conservative).

3. **S3 multipart uploads**: For large segments, should we use multipart?
   - Recommendation: Defer. Start with simple PutObject, add multipart later if needed.

4. **Cross-region replication**: Should tests verify S3 replication?
   - Recommendation: Out of scope. Single-region only for now.

---

## References

- [Object Storage Design](object-storage.md)
- [Helix Workload Design](helix-workload.md)
- [LocalStack Documentation](https://docs.localstack.cloud/)
- [Jepsen Kafka Tests](https://github.com/jepsen-io/redpanda)
