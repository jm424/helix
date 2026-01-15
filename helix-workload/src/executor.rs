//! Workload execution infrastructure.
//!
//! Provides the abstraction layer between workload operations and their execution.
//! The same workload can run against real Helix processes or in simulation.

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::Message;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::operation::Timestamp;

/// Errors that can occur during workload execution.
#[derive(Debug, Error)]
pub enum ExecutorError {
    /// Kafka client error.
    #[error("kafka error: {0}")]
    Kafka(#[from] KafkaError),

    /// Failed to spawn process.
    #[error("failed to spawn process: {0}")]
    SpawnFailed(std::io::Error),

    /// Process exited unexpectedly.
    #[error("process exited unexpectedly: node {node_id}")]
    ProcessExited {
        /// Node that exited.
        node_id: u64,
    },

    /// Cluster not ready.
    #[error("cluster not ready: {message}")]
    ClusterNotReady {
        /// Description of why cluster is not ready.
        message: String,
    },

    /// Timeout waiting for operation.
    #[error("timeout: {0}")]
    Timeout(String),

    /// Invalid configuration.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Generic error with error code.
    #[error("{message}")]
    Generic {
        /// Error code for Kafka-compatible error reporting.
        code: i16,
        /// Error message.
        message: String,
    },
}

impl ExecutorError {
    /// Returns the Kafka-compatible error code.
    #[must_use]
    pub const fn error_code(&self) -> i16 {
        match self {
            Self::Kafka(_) => -1,
            Self::SpawnFailed(_) => -2,
            Self::ProcessExited { .. } => -3,
            Self::ClusterNotReady { .. } => -4,
            Self::Timeout(_) => -5,
            Self::InvalidConfig(_) => -6,
            Self::Generic { code, .. } => *code,
        }
    }
}

/// Trait for executing workload operations.
///
/// This abstraction allows the same workload to run against:
/// - Real Helix processes with actual disk I/O
/// - Simulated environment with fault injection
#[allow(async_fn_in_trait)]
pub trait WorkloadExecutor {
    /// Returns the current timestamp in microseconds.
    fn now(&self) -> Timestamp;

    /// Sends a message to the specified topic/partition.
    ///
    /// Returns the assigned offset on success.
    async fn send(
        &self,
        topic: &str,
        partition: i32,
        payload: Bytes,
    ) -> Result<u64, ExecutorError>;

    /// Polls messages from the specified topic/partition.
    ///
    /// Returns a vector of (offset, payload) tuples.
    async fn poll(
        &self,
        topic: &str,
        partition: i32,
        start_offset: u64,
        max_messages: u32,
    ) -> Result<Vec<(u64, Bytes)>, ExecutorError>;

    /// Commits an offset for a consumer group.
    async fn commit(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
        offset: u64,
    ) -> Result<(), ExecutorError>;

    /// Fetches the committed offset for a consumer group.
    async fn fetch_offset(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<u64>, ExecutorError>;

    /// Returns the bootstrap servers for Kafka connections.
    fn bootstrap_servers(&self) -> &str;

    /// Waits for the cluster to be ready for operations.
    async fn wait_ready(&self, timeout: Duration) -> Result<(), ExecutorError>;
}

/// Configuration for a real Helix cluster.
#[derive(Debug, Clone)]
pub struct RealClusterConfig {
    /// Number of nodes in the cluster.
    pub node_count: u32,
    /// Base port for Kafka protocol (node N uses `base_port` + N).
    pub base_port: u16,
    /// Base port for Raft transport (node N uses `raft_base_port` + N).
    pub raft_base_port: u16,
    /// Path to helix-server binary.
    pub binary_path: PathBuf,
    /// Data directory base path.
    pub data_dir: PathBuf,
    /// Whether to auto-create topics.
    pub auto_create_topics: bool,
    /// Default replication factor for auto-created topics.
    pub default_replication_factor: u32,
    /// Topics to pre-create at startup (name, `partition_count`).
    pub topics: Vec<(String, u32)>,
}

impl Default for RealClusterConfig {
    fn default() -> Self {
        Self {
            node_count: 3,
            base_port: 9092,
            raft_base_port: 50100,
            binary_path: PathBuf::from("./target/release/helix-server"),
            data_dir: PathBuf::from("/tmp/helix-workload"),
            auto_create_topics: true,
            default_replication_factor: 3,
            topics: Vec::new(),
        }
    }
}

/// Builder for `RealCluster`.
#[derive(Debug, Default)]
pub struct RealClusterBuilder {
    config: RealClusterConfig,
}

impl RealClusterBuilder {
    /// Creates a new builder with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the number of nodes in the cluster.
    #[must_use]
    pub const fn nodes(mut self, count: u32) -> Self {
        self.config.node_count = count;
        self
    }

    /// Sets the base port for Kafka protocol.
    #[must_use]
    pub const fn base_port(mut self, port: u16) -> Self {
        self.config.base_port = port;
        self
    }

    /// Sets the base port for Raft transport.
    #[must_use]
    pub const fn raft_base_port(mut self, port: u16) -> Self {
        self.config.raft_base_port = port;
        self
    }

    /// Sets the path to the helix-server binary.
    #[must_use]
    pub fn binary_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.binary_path = path.into();
        self
    }

    /// Sets the data directory base path.
    #[must_use]
    pub fn data_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.data_dir = path.into();
        self
    }

    /// Sets whether to auto-create topics.
    #[must_use]
    pub const fn auto_create_topics(mut self, auto_create: bool) -> Self {
        self.config.auto_create_topics = auto_create;
        self
    }

    /// Sets the default replication factor for auto-created topics.
    #[must_use]
    pub const fn default_replication_factor(mut self, factor: u32) -> Self {
        self.config.default_replication_factor = factor;
        self
    }

    /// Adds a topic to pre-create at cluster startup.
    ///
    /// Topics are created on all nodes via the `--topic` flag.
    /// This ensures topics exist before any produce requests arrive.
    #[must_use]
    pub fn topic(mut self, name: impl Into<String>, partitions: u32) -> Self {
        self.config.topics.push((name.into(), partitions));
        self
    }

    /// Builds and starts the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if processes cannot be spawned.
    pub fn build(self) -> Result<RealCluster, ExecutorError> {
        RealCluster::start(self.config)
    }
}

/// A real Helix cluster with spawned processes.
pub struct RealCluster {
    config: RealClusterConfig,
    processes: Vec<Child>,
    bootstrap_servers: String,
}

impl RealCluster {
    /// Creates a new builder.
    #[must_use]
    pub fn builder() -> RealClusterBuilder {
        RealClusterBuilder::new()
    }

    /// Starts a cluster with the given configuration.
    fn start(config: RealClusterConfig) -> Result<Self, ExecutorError> {
        // Wait for all ports to be available before starting.
        // This prevents failures from leftover processes or TIME_WAIT sockets.
        for node_id in 1..=config.node_count {
            let kafka_port = config.base_port + u16::try_from(node_id).unwrap_or(0);
            let raft_port = config.raft_base_port + u16::try_from(node_id).unwrap_or(0);
            wait_for_port_available(kafka_port, std::time::Duration::from_secs(5))?;
            wait_for_port_available(raft_port, std::time::Duration::from_secs(5))?;
        }

        let mut processes = Vec::with_capacity(config.node_count as usize);

        // Build peer list for each node.
        // Format: node_id:host:kafka_port:raft_port
        let mut peer_args: Vec<Vec<String>> = Vec::new();
        for node_id in 1..=config.node_count {
            let mut peers = Vec::new();
            for other_id in 1..=config.node_count {
                if other_id != node_id {
                    let kafka_port = config.base_port + u16::try_from(other_id).unwrap_or(0);
                    let raft_port = config.raft_base_port + u16::try_from(other_id).unwrap_or(0);
                    peers.push("--peer".to_string());
                    peers.push(format!("{other_id}:127.0.0.1:{kafka_port}:{raft_port}"));
                }
            }
            peer_args.push(peers);
        }

        // Create data directories.
        for node_id in 1..=config.node_count {
            let node_dir = config.data_dir.join(format!("node-{node_id}"));
            std::fs::create_dir_all(&node_dir).map_err(ExecutorError::SpawnFailed)?;
        }

        // Spawn each node.
        for node_id in 1..=config.node_count {
            let kafka_port = config.base_port + u16::try_from(node_id).unwrap_or(0);
            let raft_port = config.raft_base_port + u16::try_from(node_id).unwrap_or(0);
            let node_dir = config.data_dir.join(format!("node-{node_id}"));

            let mut cmd = Command::new(&config.binary_path);
            cmd.arg("--protocol")
                .arg("kafka")
                .arg("--node-id")
                .arg(node_id.to_string())
                .arg("--listen-addr")
                .arg(format!("127.0.0.1:{kafka_port}"))
                .arg("--raft-addr")
                .arg(format!("127.0.0.1:{raft_port}"))
                .arg("--data-dir")
                .arg(&node_dir)
                .arg("--log-level")
                .arg("debug");

            if config.auto_create_topics {
                cmd.arg("--auto-create-topics");
            }

            // Add pre-created topics.
            for (topic_name, partitions) in &config.topics {
                cmd.arg("--topic").arg(format!("{topic_name}:{partitions}"));
            }

            // Add peers.
            let peers = &peer_args[node_id as usize - 1];
            for peer_arg in peers {
                cmd.arg(peer_arg);
            }

            // Inherit stderr for debugging, suppress stdout.
            // Explicitly inherit env so RUST_LOG propagates to child processes.
            cmd.stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .envs(std::env::vars());

            let child = cmd.spawn().map_err(ExecutorError::SpawnFailed)?;
            eprintln!(
                "[SPAWN] node_id={} pid={} kafka_port={} raft_port={}",
                node_id,
                child.id(),
                kafka_port,
                raft_port
            );
            processes.push(child);
        }

        // Diagnostic: give processes a moment to start, then check if alive.
        std::thread::sleep(std::time::Duration::from_millis(100));
        for (i, proc) in processes.iter_mut().enumerate() {
            match proc.try_wait() {
                Ok(Some(status)) => {
                    eprintln!(
                        "[SPAWN_DIED] node_id={} pid={} exit_status={:?}",
                        i + 1,
                        proc.id(),
                        status
                    );
                }
                Ok(None) => {
                    eprintln!("[SPAWN_ALIVE] node_id={} pid={}", i + 1, proc.id());
                }
                Err(e) => {
                    eprintln!(
                        "[SPAWN_CHECK_ERROR] node_id={} pid={} error={}",
                        i + 1,
                        proc.id(),
                        e
                    );
                }
            }
        }

        // Build bootstrap servers string.
        // Use explicit IPv4 address (127.0.0.1) instead of localhost to avoid IPv6 resolution
        // issues when the server only binds to IPv4.
        let bootstrap_servers: Vec<String> = (1..=config.node_count)
            .map(|id| {
                let port = config.base_port + u16::try_from(id).unwrap_or(0);
                format!("127.0.0.1:{port}")
            })
            .collect();
        let bootstrap_servers = bootstrap_servers.join(",");

        Ok(Self {
            config,
            processes,
            bootstrap_servers,
        })
    }

    /// Returns the bootstrap servers string.
    #[must_use]
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Returns the number of nodes in the cluster.
    #[must_use]
    pub const fn node_count(&self) -> u32 {
        self.config.node_count
    }

    /// Checks if all processes are still running.
    ///
    /// # Errors
    ///
    /// Returns an error if any process has exited or cannot be checked.
    pub fn check_health(&mut self) -> Result<(), ExecutorError> {
        for (i, process) in self.processes.iter_mut().enumerate() {
            match process.try_wait() {
                Ok(Some(_status)) => {
                    return Err(ExecutorError::ProcessExited {
                        node_id: (i + 1) as u64,
                    });
                }
                Ok(None) => {
                    // Still running.
                }
                Err(e) => {
                    return Err(ExecutorError::SpawnFailed(e));
                }
            }
        }
        Ok(())
    }

    /// Stops all processes in the cluster.
    pub fn stop(&mut self) {
        for process in &mut self.processes {
            let _ = process.kill();
            let _ = process.wait();
        }
    }

    /// Kills a specific node (for fault injection).
    ///
    /// # Errors
    ///
    /// Returns an error if the node cannot be killed.
    #[allow(clippy::cast_possible_truncation)] // node_id bounded by node_count.
    pub fn kill_node(&mut self, node_id: u64) -> Result<(), ExecutorError> {
        let idx = node_id as usize - 1;
        if idx >= self.processes.len() {
            return Err(ExecutorError::InvalidConfig(format!(
                "node {node_id} does not exist"
            )));
        }

        self.processes[idx]
            .kill()
            .map_err(ExecutorError::SpawnFailed)?;
        let _ = self.processes[idx].wait();
        Ok(())
    }

    /// Restarts a previously killed node.
    ///
    /// # Errors
    ///
    /// Returns an error if the node cannot be restarted.
    #[allow(clippy::cast_possible_truncation)] // node_id bounded by node_count.
    pub fn restart_node(&mut self, node_id: u64) -> Result<(), ExecutorError> {
        let idx = node_id as usize - 1;
        if idx >= self.processes.len() {
            return Err(ExecutorError::InvalidConfig(format!(
                "node {node_id} does not exist"
            )));
        }

        let kafka_port = self.config.base_port + u16::try_from(node_id).unwrap_or(0);
        let raft_port = self.config.raft_base_port + u16::try_from(node_id).unwrap_or(0);
        let node_dir = self.config.data_dir.join(format!("node-{node_id}"));

        let mut cmd = Command::new(&self.config.binary_path);
        cmd.arg("--protocol")
            .arg("kafka")
            .arg("--node-id")
            .arg(node_id.to_string())
            .arg("--listen-addr")
            .arg(format!("127.0.0.1:{kafka_port}"))
            .arg("--raft-addr")
            .arg(format!("127.0.0.1:{raft_port}"))
            .arg("--data-dir")
            .arg(&node_dir);

        if self.config.auto_create_topics {
            cmd.arg("--auto-create-topics");
        }

        // Add peers (format: node_id:host:kafka_port:raft_port).
        for other_id in 1..=self.config.node_count {
            if u64::from(other_id) != node_id {
                let other_kafka_port = self.config.base_port + u16::try_from(other_id).unwrap_or(0);
                let other_raft_port =
                    self.config.raft_base_port + u16::try_from(other_id).unwrap_or(0);
                cmd.arg("--peer");
                cmd.arg(format!(
                    "{other_id}:127.0.0.1:{other_kafka_port}:{other_raft_port}"
                ));
            }
        }

        // Suppress stdout/stderr to avoid I/O overload.
        cmd.stdout(Stdio::null()).stderr(Stdio::null());

        let child = cmd.spawn().map_err(ExecutorError::SpawnFailed)?;
        self.processes[idx] = child;
        Ok(())
    }

    /// Creates a topic on all nodes in the cluster.
    ///
    /// This simulates what a control plane would do: tell each node about the topic.
    /// Each node creates a Raft group with the same ID (deterministic based on order).
    ///
    /// # Errors
    ///
    /// Returns an error if topic creation fails on any node.
    pub fn create_topic_on_all_nodes(&self, topic: &str) -> Result<(), ExecutorError> {
        use rdkafka::consumer::{BaseConsumer, Consumer};

        for node_id in 1..=self.config.node_count {
            let port = self.config.base_port + u16::try_from(node_id).unwrap_or(0);
            let bootstrap = format!("127.0.0.1:{port}");

            // Create a consumer that connects only to this specific node.
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .set("group.id", "topic-creator")
                .create()
                .map_err(|e| ExecutorError::InvalidConfig(e.to_string()))?;

            // Request metadata for the topic - this triggers auto-create if enabled.
            let timeout = std::time::Duration::from_secs(10);
            match consumer.fetch_metadata(Some(topic), timeout) {
                Ok(metadata) => {
                    // Check if topic was created.
                    let topic_exists = metadata.topics().iter().any(|t| {
                        t.name() == topic && t.partitions().iter().any(|p| p.error().is_none())
                    });
                    if topic_exists {
                        eprintln!("  Created topic '{topic}' on node {node_id}");
                    } else {
                        eprintln!("  Topic '{topic}' not found on node {node_id} (may need retry)");
                    }
                }
                Err(e) => {
                    return Err(ExecutorError::InvalidConfig(format!(
                        "failed to create topic on node {node_id}: {e}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Waits for a topic partition to have a leader.
    ///
    /// Polls metadata until the partition reports a valid leader (not -1).
    /// This should be called after `create_topic_on_all_nodes` to ensure
    /// Raft has elected a leader before running workloads.
    ///
    /// # Errors
    ///
    /// Returns `ExecutorError::InvalidConfig` if the Kafka client cannot be created.
    /// Returns `ExecutorError::Timeout` if no leader is elected within the timeout.
    pub async fn wait_for_leader(
        &self,
        topic: &str,
        partition: i32,
        timeout: std::time::Duration,
    ) -> Result<u64, ExecutorError> {
        use rdkafka::consumer::{BaseConsumer, Consumer};

        let deadline = std::time::Instant::now() + timeout;
        let bootstrap = self.bootstrap_servers();

        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("group.id", "leader-waiter")
            .create()
            .map_err(|e| ExecutorError::InvalidConfig(e.to_string()))?;

        while std::time::Instant::now() < deadline {
            match consumer.fetch_metadata(Some(topic), std::time::Duration::from_secs(5)) {
                Ok(metadata) => {
                    for t in metadata.topics() {
                        if t.name() != topic {
                            continue;
                        }
                        for p in t.partitions() {
                            if p.id() == partition {
                                let leader = p.leader();
                                if leader >= 0 {
                                    #[allow(clippy::cast_sign_loss)]
                                    return Ok(leader as u64);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("  Metadata fetch error: {e}");
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        Err(ExecutorError::Timeout(format!(
            "no leader for {topic}:{partition} after {timeout:?}"
        )))
    }
}

impl Drop for RealCluster {
    fn drop(&mut self) {
        self.stop();
        // Clean up data directory.
        let _ = std::fs::remove_dir_all(&self.config.data_dir);
    }
}

/// Executor that runs operations against a real Helix cluster.
pub struct RealExecutor {
    bootstrap_servers: String,
    producer: FutureProducer,
    consumer: BaseConsumer,
    /// Tracks consumer position per partition (for future use in consumer group tracking).
    #[allow(dead_code)]
    consumer_positions: Arc<RwLock<HashMap<(String, i32), u64>>>,
    /// Start time for timestamp generation.
    start_time: std::time::Instant,
}

impl RealExecutor {
    /// Creates a new executor connected to the given cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if Kafka clients cannot be created.
    pub fn new(cluster: &RealCluster) -> Result<Self, ExecutorError> {
        Self::with_bootstrap_servers(cluster.bootstrap_servers())
    }

    /// Creates an executor with custom bootstrap servers.
    ///
    /// # Errors
    ///
    /// Returns an error if Kafka clients cannot be created.
    pub fn with_bootstrap_servers(bootstrap_servers: &str) -> Result<Self, ExecutorError> {
        // Configure rdkafka with connection retry settings so it handles
        // broker availability internally. No explicit wait loops needed.
        // NOTE: Retries enabled for retriable errors (e.g., controller not available).
        // This may cause duplicates until idempotent producers are implemented.
        //
        // Timeout settings aligned with DD's libstreaming for faster failover:
        // - request.timeout.ms: 12.5s (vs 30s default) - fail faster on dead brokers
        // - message.timeout.ms: 15s (vs 300s default) - retry faster with different partition
        // - connections.max.idle.ms: 0 - avoid stale connection issues
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "15000")
            .set("request.timeout.ms", "12500")
            .set("connections.max.idle.ms", "0")
            .set("retries", "5") // Retry on retriable errors
            .set("retry.backoff.ms", "500") // Wait between retries
            .set("acks", "all")
            .set("reconnect.backoff.ms", "100")
            .set("reconnect.backoff.max.ms", "1000") // 1s max backoff for faster recovery
            .create()?;

        // Consumer config aligned with DD's libstreaming for faster failover.
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", "helix-workload-consumer")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("reconnect.backoff.ms", "100")
            .set("reconnect.backoff.max.ms", "1000") // 1s max backoff for faster recovery
            // Refresh metadata quickly when topic info is missing or stale.
            .set("topic.metadata.refresh.fast.interval.ms", "100")
            // Enable partition EOF notifications so we know when we've consumed all data.
            .set("enable.partition.eof", "true")
            // Connection timeout settings - aligned with DD's libstreaming.
            .set("socket.timeout.ms", "12500")
            .set("connections.max.idle.ms", "0")
            .set("fetch.wait.max.ms", "500")
            // Debug logging for librdkafka internals.
            .set("debug", "fetch,broker,topic,msg,protocol")
            .create()?;

        Ok(Self {
            bootstrap_servers: bootstrap_servers.to_string(),
            producer,
            consumer,
            consumer_positions: Arc::new(RwLock::new(HashMap::new())),
            start_time: std::time::Instant::now(),
        })
    }
}

impl WorkloadExecutor for RealExecutor {
    #[allow(clippy::cast_possible_truncation)] // Won't overflow for reasonable test durations.
    fn now(&self) -> Timestamp {
        self.start_time.elapsed().as_micros() as u64
    }

    async fn send(
        &self,
        topic: &str,
        partition: i32,
        payload: Bytes,
    ) -> Result<u64, ExecutorError> {
        eprintln!("[SEND_START] topic={topic} partition={partition} payload_len={}", payload.len());
        let send_start = std::time::Instant::now();

        let record: FutureRecord<'_, (), [u8]> = FutureRecord::to(topic)
            .partition(partition)
            .payload(payload.as_ref());

        // FutureProducer returns the delivery result with offset.
        let delivery_result = self
            .producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _record)| {
                eprintln!("[SEND_ERROR] topic={topic} partition={partition} elapsed={:?} error={err}", send_start.elapsed());
                ExecutorError::Generic {
                    code: -1,
                    message: err.to_string(),
                }
            })?;

        // Flush to ensure delivery (important before polling).
        self.producer.flush(Duration::from_secs(5)).map_err(|e| ExecutorError::Generic {
            code: -1,
            message: format!("producer flush failed: {e}"),
        })?;

        // Extract the offset from the delivery result.
        #[allow(clippy::cast_sign_loss)]
        let offset = delivery_result.1 as u64;

        eprintln!("[SEND] topic={topic} partition={partition} offset={offset}");

        Ok(offset)
    }

    async fn poll(
        &self,
        topic: &str,
        partition: i32,
        start_offset: u64,
        max_messages: u32,
    ) -> Result<Vec<(u64, Bytes)>, ExecutorError> {
        use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

        // One-time metadata discovery: ensure consumer knows about the topic.
        // rdkafka handles connection/retry internally with configured backoff.
        self.consumer
            .fetch_metadata(Some(topic), None)
            .map_err(|e| ExecutorError::Generic {
                code: -1,
                message: format!("metadata fetch failed: {e}"),
            })?;

        // Assign partition with starting offset.
        let mut tpl = TopicPartitionList::new();
        #[allow(clippy::cast_possible_wrap)]
        tpl.add_partition_offset(topic, partition, Offset::Offset(start_offset as i64))
            .map_err(|e| ExecutorError::Generic {
                code: -1,
                message: e.to_string(),
            })?;

        self.consumer.assign(&tpl).map_err(|e| ExecutorError::Generic {
            code: -1,
            message: e.to_string(),
        })?;

        eprintln!("[POLL_START] topic={topic} partition={partition} start_offset={start_offset} max_messages={max_messages}");

        let mut messages = Vec::new();

        // Event-driven polling: keep polling until we have all expected messages
        // or receive PartitionEOF (end of committed data).
        //
        // Transient errors (BrokerTransportFailure, AllBrokersDown) are handled by
        // continuing to poll. These errors come from rdkafka's background reconnection
        // threads and DO NOT mean data isn't available - messages may already be buffered
        // internally even while errors are being returned. The error queue and message
        // queue are separate in librdkafka.
        let poll_timeout = std::time::Duration::from_secs(10);
        let deadline = std::time::Instant::now() + poll_timeout;

        while messages.len() < max_messages as usize {
            match self.consumer.poll(std::time::Duration::from_millis(500)) {
                Some(Ok(msg)) => {
                    #[allow(clippy::cast_sign_loss)]
                    let offset = msg.offset() as u64;
                    let payload = msg
                        .payload()
                        .map(Bytes::copy_from_slice)
                        .unwrap_or_default();
                    eprintln!("[POLL] topic={topic} partition={partition} offset={offset} payload_len={}", payload.len());
                    messages.push((offset, payload));
                }
                Some(Err(KafkaError::PartitionEOF(p))) => {
                    // Reached end of committed data - no more messages to read.
                    eprintln!("[POLL] topic={topic} partition={partition} EOF at partition {p}");
                    break;
                }
                Some(Err(KafkaError::MessageConsumption(code)))
                    if matches!(
                        code,
                        rdkafka::error::RDKafkaErrorCode::BrokerTransportFailure
                            | rdkafka::error::RDKafkaErrorCode::AllBrokersDown
                    ) =>
                {
                    // Transient connection error - check deadline first.
                    if std::time::Instant::now() >= deadline {
                        eprintln!("[POLL] topic={topic} partition={partition} transient error {code:?} after deadline, returning partial");
                        break;
                    }
                    // These errors come from rdkafka's background connection threads trying
                    // to reconnect to dead brokers. Crucially, messages may ALREADY be buffered
                    // internally even while errors are being returned. DO NOT sleep - just keep
                    // polling to drain both the error queue and message queue.
                    //
                    // The error queue and message queue are separate in librdkafka.
                    // Sleeping here just delays getting messages that are already available.
                    eprintln!("[POLL] topic={topic} partition={partition} transient error {code:?}, continuing poll");
                }
                Some(Err(e)) => {
                    eprintln!("[POLL] topic={topic} partition={partition} error: {e:?}");
                    return Err(ExecutorError::Generic {
                        code: -1,
                        message: e.to_string(),
                    });
                }
                None => {
                    // Timeout with no message - check if we've hit the deadline.
                    if std::time::Instant::now() >= deadline {
                        eprintln!("[POLL] topic={topic} partition={partition} deadline reached");
                        break;
                    }
                    // Otherwise keep polling - librdkafka may still be recovering.
                }
            }
        }

        eprintln!("[POLL_END] topic={topic} partition={partition} received={} messages", messages.len());

        Ok(messages)
    }

    async fn commit(
        &self,
        _group: &str,
        topic: &str,
        partition: i32,
        offset: u64,
    ) -> Result<(), ExecutorError> {
        use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

        let mut tpl = TopicPartitionList::new();
        #[allow(clippy::cast_possible_wrap)]
        tpl.add_partition_offset(topic, partition, Offset::Offset(offset as i64))
            .ok();

        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| ExecutorError::Generic {
                code: -1,
                message: e.to_string(),
            })
    }

    async fn fetch_offset(
        &self,
        _group: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<u64>, ExecutorError> {
        // Non-blocking check - if data isn't immediately available, return None.
        // Caller can retry if needed. No timing assumptions.
        // With Duration::ZERO, timeout/error is expected - return None in that case.
        self.consumer.committed(Duration::ZERO).map_or_else(
            |_| Ok(None),
            |committed| {
                let offset = committed
                    .elements()
                    .iter()
                    .find(|e| e.topic() == topic && e.partition() == partition)
                    .and_then(|e| {
                        #[allow(clippy::cast_sign_loss)]
                        match e.offset() {
                            rdkafka::topic_partition_list::Offset::Offset(o) => Some(o as u64),
                            _ => None,
                        }
                    });
                Ok(offset)
            },
        )
    }

    fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    async fn wait_ready(&self, timeout: Duration) -> Result<(), ExecutorError> {
        // Wait until we can successfully connect to the cluster and fetch metadata.
        // This ensures the server is actually running and accepting connections before
        // tests proceed. Without this check, debug builds may see timing-related failures.
        let deadline = std::time::Instant::now() + timeout;
        let poll_interval = std::time::Duration::from_millis(100);
        let start = std::time::Instant::now();
        let mut attempt = 0u32;

        eprintln!(
            "[WAIT_READY_START] bootstrap_servers={} timeout={:?}",
            self.bootstrap_servers, timeout
        );

        while std::time::Instant::now() < deadline {
            attempt += 1;
            // Try to fetch cluster metadata. This will fail if the server isn't ready.
            match self.consumer.fetch_metadata(None, std::time::Duration::from_secs(1)) {
                Ok(_metadata) => {
                    eprintln!(
                        "[WAIT_READY_OK] attempt={} elapsed={:?}",
                        attempt,
                        start.elapsed()
                    );
                    return Ok(());
                }
                Err(e) => {
                    // Log every 50 attempts (~5 seconds) to avoid spam.
                    if attempt % 50 == 0 {
                        eprintln!(
                            "[WAIT_READY_POLL] attempt={} elapsed={:?} error={:?}",
                            attempt,
                            start.elapsed(),
                            e
                        );
                    }
                    tokio::time::sleep(poll_interval).await;
                }
            }
        }

        eprintln!(
            "[WAIT_READY_TIMEOUT] attempts={} elapsed={:?}",
            attempt,
            start.elapsed()
        );
        Err(ExecutorError::Generic {
            code: -1,
            message: format!("cluster not ready after {timeout:?}"),
        })
    }
}

/// Waits for a port to become available for binding.
///
/// This is used before starting servers to ensure no leftover processes
/// or `TIME_WAIT` sockets are holding the port.
fn wait_for_port_available(port: u16, timeout: Duration) -> Result<(), ExecutorError> {
    let addr = format!("127.0.0.1:{port}");
    let deadline = std::time::Instant::now() + timeout;
    let poll_interval = Duration::from_millis(100);

    while std::time::Instant::now() < deadline {
        match TcpListener::bind(&addr) {
            Ok(_listener) => {
                // Port is available - listener is dropped immediately, releasing the port.
                return Ok(());
            }
            Err(_) => {
                // Port is in use - wait and retry.
                std::thread::sleep(poll_interval);
            }
        }
    }

    Err(ExecutorError::Timeout(format!(
        "port {port} not available after {timeout:?}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_builder() {
        let builder = RealCluster::builder()
            .nodes(3)
            .base_port(19092)
            .auto_create_topics(true);

        assert_eq!(builder.config.node_count, 3);
        assert_eq!(builder.config.base_port, 19092);
        assert!(builder.config.auto_create_topics);
    }

    #[test]
    fn test_cluster_config_defaults() {
        let config = RealClusterConfig::default();
        assert_eq!(config.node_count, 3);
        assert_eq!(config.base_port, 9092);
        assert!(config.auto_create_topics);
    }

    #[test]
    fn test_executor_error_codes() {
        let err = ExecutorError::Timeout("test".to_string());
        assert_eq!(err.error_code(), -5);

        let err = ExecutorError::Generic {
            code: 42,
            message: "test".to_string(),
        };
        assert_eq!(err.error_code(), 42);
    }
}
