# Helix Workload: E2E Verifiable Load Testing

**Status**: Design
**Author**: Claude + Jai
**Created**: 2026-01-13

## Overview

A unified framework for end-to-end testing of Helix that serves three purposes:

1. **Correctness verification** - Detect lost writes, duplicates, ordering violations
2. **Performance measurement** - Throughput, latency percentiles, bottleneck identification
3. **Fault tolerance validation** - Verify behavior under crashes, partitions, disk failures

The same workload and verification logic runs in two modes:
- **Real mode**: Against actual Helix processes with real I/O (load testing, benchmarking)
- **Simulated mode**: In Bloodhound DST with fault injection (correctness under chaos)

## Goals

1. **Validate the entire stack**: Kafka protocol, Raft consensus, storage, tiering, progress tracking
2. **Deterministic verification**: Same seed produces same operation sequence for reproducibility
3. **Decouple from Bloodhound**: Core workload/verification has no Bloodhound dependency
4. **Production-realistic patterns**: Workloads that mirror real usage (not just micro-benchmarks)
5. **Actionable results**: Clear violation reports with enough context to debug

## Non-Goals

1. **Kafka compatibility testing**: We're testing Helix correctness, not Kafka client compatibility
2. **Exhaustive protocol coverage**: Focus on produce/fetch/offsets, not admin APIs
3. **Performance optimization**: This framework identifies bottlenecks; fixes happen elsewhere

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           helix-workload                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      WorkloadGenerator                          │   │
│  │  - Deterministic RNG (seeded)                                   │   │
│  │  - Produces Operation stream                                    │   │
│  │  - Configurable patterns (sequential, concurrent, stress)       │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                  │                                      │
│                                  ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                         History                                  │   │
│  │  - Records all operations with timestamps                       │   │
│  │  - Tracks: invocation, completion, result                       │   │
│  │  - Indexed by (topic, partition, offset)                        │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                  │                                      │
│                                  ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                        Verifier                                  │   │
│  │  - Checks invariants against History                            │   │
│  │  - Detects: lost writes, duplicates, gaps, ordering violations  │   │
│  │  - Returns detailed Violation reports                           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                        Executor Abstraction                             │
├────────────────────────────┬────────────────────────────────────────────┤
│      RealExecutor          │           SimulatedExecutor                │
│  - rdkafka client          │        - In-process HelixService           │
│  - Real TCP connections    │        - Simulated network                 │
│  - Wall clock timing       │        - Virtual time                      │
│  - Process spawning        │        - Bloodhound integration            │
└────────────────────────────┴────────────────────────────────────────────┘
```

## Core Components

### 1. Operation Model

```rust
/// A single operation in the workload.
#[derive(Debug, Clone)]
pub struct Operation {
    /// Unique operation ID for correlation.
    pub id: u64,
    /// Operation type and parameters.
    pub kind: OperationKind,
    /// When the operation was invoked.
    pub invoked_at: Timestamp,
    /// When the operation completed (filled after execution).
    pub completed_at: Option<Timestamp>,
    /// Result of the operation (filled after execution).
    pub result: Option<OperationResult>,
}

#[derive(Debug, Clone)]
pub enum OperationKind {
    /// Produce a message to a topic/partition.
    Send {
        topic: String,
        partition: i32,
        /// Deterministic payload: includes op_id for verification.
        payload: Bytes,
    },
    /// Fetch messages from a topic/partition starting at offset.
    Poll {
        topic: String,
        partition: i32,
        start_offset: u64,
        max_messages: u32,
    },
    /// Commit consumer offset.
    Commit {
        group: String,
        topic: String,
        partition: i32,
        offset: u64,
    },
    /// Fetch committed offset for a consumer group.
    FetchOffset {
        group: String,
        topic: String,
        partition: i32,
    },
}

#[derive(Debug, Clone)]
pub enum OperationResult {
    SendOk { offset: u64 },
    SendError { code: i16, message: String },
    PollOk { messages: Vec<(u64, Bytes)> },  // (offset, payload)
    PollError { code: i16, message: String },
    CommitOk,
    CommitError { code: i16, message: String },
    FetchOffsetOk { offset: Option<u64> },
    FetchOffsetError { code: i16, message: String },
}
```

### 2. History Recording

```rust
/// Complete history of a workload execution.
pub struct History {
    /// All operations in invocation order.
    operations: Vec<Operation>,

    /// Index: (topic, partition, offset) -> operation IDs that touched this offset.
    /// Used for fast lookup during verification.
    offset_index: BTreeMap<(String, i32, u64), Vec<u64>>,

    /// Successful sends: (topic, partition, offset) -> payload.
    committed_values: BTreeMap<(String, i32, u64), Bytes>,

    /// Observed reads: (topic, partition, offset) -> payload.
    observed_values: BTreeMap<(String, i32, u64), Bytes>,
}

impl History {
    /// Record operation invocation.
    pub fn record_invoke(&mut self, op: Operation) -> u64;

    /// Record operation completion.
    pub fn record_complete(&mut self, op_id: u64, result: OperationResult);

    /// Get all operations for a topic/partition.
    pub fn ops_for_partition(&self, topic: &str, partition: i32) -> Vec<&Operation>;
}
```

### 3. Verification Logic

Inspired by Jepsen's Kafka tests, we verify these properties:

```rust
/// A violation of an expected property.
#[derive(Debug, Clone)]
pub enum Violation {
    /// A successfully acknowledged send was never observed in any poll.
    LostWrite {
        op_id: u64,
        topic: String,
        partition: i32,
        offset: u64,
        payload: Bytes,
    },

    /// Same (topic, partition, offset) returned different values.
    InconsistentRead {
        topic: String,
        partition: i32,
        offset: u64,
        expected: Bytes,
        observed: Bytes,
        send_op_id: u64,
        poll_op_id: u64,
    },

    /// Gap in offset sequence (missing offsets).
    OffsetGap {
        topic: String,
        partition: i32,
        expected_offset: u64,
        actual_offset: u64,
    },

    /// Offsets returned out of order within a single poll.
    OrderingViolation {
        topic: String,
        partition: i32,
        poll_op_id: u64,
        offsets: Vec<u64>,  // The out-of-order sequence
    },

    /// Duplicate offset assigned to different sends.
    DuplicateOffset {
        topic: String,
        partition: i32,
        offset: u64,
        op_ids: Vec<u64>,
    },

    /// Consumer resumed at wrong offset after commit.
    CommitViolation {
        group: String,
        topic: String,
        partition: i32,
        committed_offset: u64,
        resumed_offset: u64,
    },
}

pub struct Verifier;

impl Verifier {
    /// Run all verification checks against a history.
    pub fn verify(history: &History) -> Vec<Violation> {
        let mut violations = Vec::new();

        violations.extend(Self::check_lost_writes(history));
        violations.extend(Self::check_inconsistent_reads(history));
        violations.extend(Self::check_offset_gaps(history));
        violations.extend(Self::check_ordering(history));
        violations.extend(Self::check_duplicates(history));
        violations.extend(Self::check_commits(history));

        violations
    }

    /// Check that all acknowledged sends are eventually observed.
    fn check_lost_writes(history: &History) -> Vec<Violation>;

    /// Check that same offset always returns same value.
    fn check_inconsistent_reads(history: &History) -> Vec<Violation>;

    /// Check for gaps in offset sequences.
    fn check_offset_gaps(history: &History) -> Vec<Violation>;

    /// Check ordering within partitions.
    fn check_ordering(history: &History) -> Vec<Violation>;

    /// Check no duplicate offsets assigned.
    fn check_duplicates(history: &History) -> Vec<Violation>;

    /// Check consumer offset commits are honored.
    fn check_commits(history: &History) -> Vec<Violation>;
}
```

### 4. Payload Design for Verification

Each message payload is self-describing to enable verification without external state:

```rust
/// Payload format: deterministic and self-verifying.
///
/// Layout (40 bytes + variable):
///   [0..8]   operation_id: u64     - Unique ID for correlation
///   [8..16]  sequence: u64         - Per-producer sequence number
///   [16..24] timestamp: u64        - Virtual/wall timestamp
///   [24..32] checksum: u64         - CRC64 of the rest
///   [32..40] payload_len: u64      - Length of variable payload
///   [40..]   payload: [u8]         - Deterministic content based on seed
///
pub struct VerifiablePayload {
    pub operation_id: u64,
    pub sequence: u64,
    pub timestamp: u64,
    pub checksum: u64,
    pub data: Bytes,
}

impl VerifiablePayload {
    /// Create payload with deterministic content.
    pub fn new(op_id: u64, seq: u64, ts: u64, size: usize, rng: &mut StdRng) -> Self;

    /// Parse and validate payload from bytes.
    pub fn parse(bytes: &Bytes) -> Result<Self, PayloadError>;

    /// Verify checksum matches content.
    pub fn verify_checksum(&self) -> bool;
}
```

### 5. Workload Patterns

```rust
/// Configurable workload pattern.
pub struct WorkloadConfig {
    /// Random seed for deterministic generation.
    pub seed: u64,
    /// Topics and partitions to use.
    pub topics: Vec<TopicConfig>,
    /// Number of producer clients.
    pub producer_count: u32,
    /// Number of consumer clients.
    pub consumer_count: u32,
    /// Total operations to generate.
    pub operation_count: u64,
    /// Operation mix.
    pub pattern: WorkloadPattern,
    /// Message size distribution.
    pub message_size: SizeDistribution,
}

pub struct TopicConfig {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
}

pub enum WorkloadPattern {
    /// Sequential writes to single partition, then read back.
    /// Good for: basic correctness, offset verification.
    Sequential,

    /// Multiple producers writing concurrently.
    /// Good for: duplicate detection, concurrent correctness.
    ConcurrentProducers {
        producers: u32,
        keys_per_producer: u32,
    },

    /// Producers and consumers running simultaneously.
    /// Good for: end-to-end latency, consumer progress.
    ProducerConsumer {
        producer_rate: u32,  // ops/sec target
        consumer_count: u32,
    },

    /// Consumer crash and resume pattern.
    /// Good for: offset commit verification.
    ConsumerResume {
        commit_interval: u32,  // commit every N messages
        crash_after: u32,      // crash after N messages
    },

    /// High throughput stress test.
    /// Good for: performance measurement, saturation point.
    HighThroughput {
        target_rate: u32,      // ops/sec target (0 = unlimited)
        duration_secs: u32,
    },

    /// Many partitions with leader distribution.
    /// Good for: MultiRaft scalability.
    ManyPartitions {
        partition_count: i32,
        operations_per_partition: u32,
    },
}

pub enum SizeDistribution {
    Fixed(usize),
    Uniform { min: usize, max: usize },
    Bimodal { small: usize, large: usize, large_ratio: f64 },
}
```

### 6. Executor Trait

```rust
/// Abstraction over execution environment.
#[async_trait]
pub trait WorkloadExecutor: Send + Sync {
    /// Send a message, return assigned offset.
    async fn send(
        &self,
        topic: &str,
        partition: i32,
        payload: Bytes,
    ) -> Result<u64, ExecutorError>;

    /// Poll messages starting at offset.
    async fn poll(
        &self,
        topic: &str,
        partition: i32,
        start_offset: u64,
        max_messages: u32,
    ) -> Result<Vec<(u64, Bytes)>, ExecutorError>;

    /// Commit consumer offset.
    async fn commit(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
        offset: u64,
    ) -> Result<(), ExecutorError>;

    /// Fetch committed offset.
    async fn fetch_offset(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<u64>, ExecutorError>;

    /// Get current timestamp (wall or virtual).
    fn now(&self) -> Timestamp;
}

#[derive(Debug)]
pub enum ExecutorError {
    NotLeader { leader_hint: Option<u32> },
    TopicNotFound,
    PartitionNotFound,
    Timeout,
    ConnectionFailed,
    Internal(String),
}
```

### 7. Cluster Management

```rust
/// Abstraction over cluster lifecycle (real processes).
/// Fault injection is handled separately in SimulatedCluster via Bloodhound.
#[async_trait]
pub trait ClusterManager: Send + Sync {
    /// Start the cluster.
    async fn start(&mut self) -> Result<(), ClusterError>;

    /// Stop the cluster gracefully.
    async fn stop(&mut self) -> Result<(), ClusterError>;

    /// Get bootstrap addresses for clients.
    fn bootstrap_addrs(&self) -> Vec<String>;

    /// Get node IDs in the cluster.
    fn node_ids(&self) -> Vec<u64>;

    /// Get the current leader for a topic/partition.
    async fn get_leader(&self, topic: &str, partition: i32) -> Result<Option<u64>, ClusterError>;

    /// Wait for cluster to be healthy (all nodes up, leaders elected).
    async fn wait_healthy(&self, timeout: Duration) -> Result<(), ClusterError>;
}
```

## Execution Modes

### Real Mode (RealExecutor + RealCluster)

```rust
/// Executor using real Kafka client (rdkafka).
pub struct RealExecutor {
    producer: FutureProducer,
    consumer: StreamConsumer,
    bootstrap: String,
}

/// Cluster manager that spawns actual helix-server processes.
pub struct RealCluster {
    nodes: Vec<NodeProcess>,
    data_dir: PathBuf,
    base_port: u16,
}

struct NodeProcess {
    node_id: u64,
    process: Child,
    kafka_port: u16,
    raft_port: u16,
    data_dir: PathBuf,
}

impl RealCluster {
    pub fn builder() -> RealClusterBuilder;
}

impl RealClusterBuilder {
    pub fn nodes(self, count: u32) -> Self;
    pub fn data_dir(self, path: impl Into<PathBuf>) -> Self;
    pub fn base_kafka_port(self, port: u16) -> Self;
    pub fn base_raft_port(self, port: u16) -> Self;
    pub fn topics(self, topics: Vec<TopicConfig>) -> Self;
    pub async fn build(self) -> Result<RealCluster, ClusterError>;
}
```

**Fault injection in Real Mode:**
- `kill_node`: `kill -9` on the process
- `partition`: iptables rules (Linux) or pf rules (macOS)
- `restart_node`: spawn new process with same data dir

### Simulated Mode (SimulatedExecutor + SimulatedCluster)

```rust
/// Executor using in-process HelixService instances.
pub struct SimulatedExecutor {
    cluster: Arc<SimulatedCluster>,
    /// Virtual clock for deterministic timing.
    clock: VirtualClock,
}

/// In-process cluster with simulated network.
pub struct SimulatedCluster {
    nodes: BTreeMap<u64, SimulatedNode>,
    network: SimulatedNetwork,
    fault_injector: FaultInjector,
}

struct SimulatedNode {
    node_id: u64,
    service: Arc<HelixService>,
    /// Inbox for simulated network messages.
    inbox: mpsc::Receiver<NetworkMessage>,
}

/// Deterministic network simulation.
struct SimulatedNetwork {
    /// Pending messages with delivery time.
    pending: BTreeMap<VirtualTime, NetworkMessage>,
    /// Active partitions.
    partitions: Vec<(BTreeSet<u64>, BTreeSet<u64>)>,
    /// Message delay distribution.
    delay: DelayDistribution,
    /// Packet loss probability.
    loss_rate: f64,
}
```

**Bloodhound Integration:**

The `SimulatedCluster` can optionally integrate with Bloodhound for:
- Virtual time control
- Deterministic fault scheduling
- State snapshots for exploration
- Property checking hooks

```rust
impl SimulatedCluster {
    /// Create standalone simulated cluster (no Bloodhound).
    pub fn new(config: ClusterConfig) -> Self;

    /// Create with Bloodhound integration.
    pub fn with_bloodhound(config: ClusterConfig, ctx: BloodhoundContext) -> Self;
}
```

## Fault Scenarios (Simulated Mode Only)

Pre-built scenarios for SimulatedExecutor with Bloodhound integration.
These are not available in RealExecutor - use RealExecutor for correctness
verification and performance measurement without faults.

```rust
pub enum FaultScenario {
    /// No faults - baseline correctness.
    None,

    /// Kill leader, verify failover and no data loss.
    LeaderKill {
        topic: String,
        partition: i32,
        kill_after_ops: u64,
    },

    /// Network partition isolating minority.
    MinorityPartition {
        duration_ms: u64,
        trigger_after_ops: u64,
    },

    /// Network partition isolating leader.
    LeaderIsolation {
        topic: String,
        partition: i32,
        duration_ms: u64,
    },

    /// Rolling restart of all nodes.
    RollingRestart {
        interval_ms: u64,
    },

    /// Random node kills throughout workload.
    RandomKills {
        probability: f64,  // per-operation probability
        min_interval_ms: u64,
    },

    /// Disk write failures.
    DiskFaults {
        probability: f64,
    },

    /// Clock skew between nodes.
    ClockSkew {
        max_drift_ms: i64,
    },

    /// Combined chaos (multiple fault types).
    Chaos {
        kill_probability: f64,
        partition_probability: f64,
        disk_fault_probability: f64,
    },
}
```

## Statistics and Reporting

```rust
/// Statistics collected during workload execution.
pub struct WorkloadStats {
    /// Total operations attempted.
    pub operations_total: u64,
    /// Successful operations.
    pub operations_ok: u64,
    /// Failed operations.
    pub operations_failed: u64,

    /// Send latency percentiles (ms).
    pub send_latency_p50: f64,
    pub send_latency_p95: f64,
    pub send_latency_p99: f64,
    pub send_latency_max: f64,

    /// Poll latency percentiles (ms).
    pub poll_latency_p50: f64,
    pub poll_latency_p95: f64,
    pub poll_latency_p99: f64,
    pub poll_latency_max: f64,

    /// Throughput.
    pub send_throughput_ops: f64,      // ops/sec
    pub send_throughput_bytes: f64,    // bytes/sec

    /// Duration.
    pub duration_ms: u64,

    /// Faults injected.
    pub faults_injected: u64,

    /// Verification results.
    pub violations: Vec<Violation>,
}

impl WorkloadStats {
    /// Print human-readable summary.
    pub fn print_summary(&self);

    /// Export as JSON for analysis.
    pub fn to_json(&self) -> String;
}
```

## Usage Examples

### Basic Correctness Test

```rust
#[tokio::test]
async fn test_basic_correctness() {
    let cluster = RealCluster::builder()
        .nodes(3)
        .topics(vec![TopicConfig {
            name: "test".into(),
            partitions: 4,
            replication_factor: 3,
        }])
        .build()
        .await
        .unwrap();

    let workload = Workload::builder()
        .seed(42)
        .pattern(WorkloadPattern::Sequential)
        .operations(1000)
        .build();

    let executor = RealExecutor::new(&cluster);
    let stats = workload.run(&executor).await;

    assert!(stats.violations.is_empty());
    cluster.stop().await.unwrap();
}
```

### Leader Failover Test

```rust
#[tokio::test]
async fn test_leader_failover() {
    let cluster = RealCluster::builder()
        .nodes(3)
        .topics(vec![TopicConfig::new("test", 1, 3)])
        .build()
        .await
        .unwrap();

    let workload = Workload::builder()
        .seed(42)
        .pattern(WorkloadPattern::ConcurrentProducers {
            producers: 4,
            keys_per_producer: 1000,
        })
        .fault_scenario(FaultScenario::LeaderKill {
            topic: "test".into(),
            partition: 0,
            kill_after_ops: 500,
        })
        .build();

    let executor = RealExecutor::new(&cluster);
    let stats = workload.run(&executor).await;

    // Verify no data loss despite leader kill
    assert!(stats.violations.is_empty());
    assert!(stats.operations_ok > 900);  // Most ops should succeed
}
```

### Performance Benchmark

```rust
#[tokio::test]
async fn bench_throughput() {
    let cluster = RealCluster::builder()
        .nodes(3)
        .data_dir("/mnt/fast-ssd/helix-bench")
        .topics(vec![TopicConfig::new("bench", 16, 3)])
        .build()
        .await
        .unwrap();

    let workload = Workload::builder()
        .seed(42)
        .pattern(WorkloadPattern::HighThroughput {
            target_rate: 0,  // unlimited
            duration_secs: 60,
        })
        .message_size(SizeDistribution::Fixed(1024))
        .build();

    let executor = RealExecutor::new(&cluster);
    let stats = workload.run(&executor).await;

    stats.print_summary();
    // Throughput: 150,000 ops/sec
    // Send p99: 2.3ms
    // Violations: 0
}
```

### DST with Bloodhound

```rust
#[test]
fn dst_chaos_test() {
    for seed in 0..1000 {
        let cluster = SimulatedCluster::new(ClusterConfig {
            nodes: 3,
            topics: vec![TopicConfig::new("test", 4, 3)],
        });

        let workload = Workload::builder()
            .seed(seed)
            .pattern(WorkloadPattern::ProducerConsumer {
                producer_rate: 100,
                consumer_count: 2,
            })
            .fault_scenario(FaultScenario::Chaos {
                kill_probability: 0.001,
                partition_probability: 0.001,
                disk_fault_probability: 0.01,
            })
            .operations(10_000)
            .build();

        let executor = SimulatedExecutor::new(&cluster);
        let stats = workload.run_simulated(&executor);

        assert!(
            stats.violations.is_empty(),
            "Seed {seed} found violations: {:?}",
            stats.violations
        );
    }
}
```

## Implementation Plan

### Phase 1: Core Framework
1. Operation model and History
2. Verification logic (all checks)
3. Payload encoding/decoding
4. WorkloadGenerator with patterns

### Phase 2: Real Executor
1. RealCluster (process spawning, lifecycle management)
2. RealExecutor (rdkafka integration)
3. Statistics collection (latency histograms, throughput)
4. Consumer group support (JoinGroup, SyncGroup, Heartbeat)

### Phase 3: Integration Tests
1. Basic correctness tests
2. Leader failover tests
3. Performance benchmarks
4. CI integration

### Phase 4: Simulated Executor
1. SimulatedCluster (in-process)
2. SimulatedNetwork
3. VirtualClock
4. Deterministic fault injection

### Phase 5: Bloodhound Integration
1. BloodhoundContext integration
2. Property checking hooks
3. State exploration support
4. DST scenarios

## Decisions

1. **Kafka client**: Use **rdkafka** for RealExecutor.
   - Full-featured, battle-tested
   - Faster to get working
   - Can revisit if dependency becomes problematic

2. **Fault injection**: **Simulated mode only** (via Bloodhound).
   - RealExecutor focuses on correctness verification and performance measurement
   - Fault injection happens in SimulatedExecutor with Bloodhound integration
   - Keeps RealCluster simple (just process lifecycle)

3. **Consumer groups**: **Full protocol** (JoinGroup, SyncGroup, Heartbeat, rebalancing).
   - More realistic testing
   - Validates our consumer group implementation
   - Required for proper offset commit testing

## Open Questions

1. **Tiering verification**: How to verify tiered storage correctness?
   - Need to force tier transitions during workload
   - Verify data accessible after tiering
   - May need workload pattern that writes enough to trigger tiering

## References

- [Jepsen Redpanda Tests](https://github.com/jepsen-io/redpanda)
- [Jepsen Kafka Workload](https://jepsen-io.github.io/jepsen/jepsen.tests.kafka.html)
- [TigerBeetle VOPR](https://github.com/tigerbeetle/tigerbeetle/tree/main/src/vopr)
- [FoundationDB Simulation](https://apple.github.io/foundationdb/testing.html)
- [Bloodhound](https://github.com/nerdsane/bloodhound)
