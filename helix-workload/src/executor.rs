//! Workload execution infrastructure.
//!
//! Provides the abstraction layer between workload operations and their execution.
//! The same workload can run against real Helix processes or in simulation.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
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
        let mut processes = Vec::with_capacity(config.node_count as usize);

        // Build peer list for each node.
        let mut peer_args: Vec<Vec<String>> = Vec::new();
        for node_id in 1..=config.node_count {
            let mut peers = Vec::new();
            for other_id in 1..=config.node_count {
                if other_id != node_id {
                    let port = config.base_port + u16::try_from(other_id).unwrap_or(0);
                    peers.push("--peer".to_string());
                    peers.push(format!("{other_id}:localhost:{port}"));
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
            let port = config.base_port + u16::try_from(node_id).unwrap_or(0);
            let raft_port = config.raft_base_port + u16::try_from(node_id).unwrap_or(0);
            let node_dir = config.data_dir.join(format!("node-{node_id}"));

            let mut cmd = Command::new(&config.binary_path);
            cmd.arg("--protocol")
                .arg("kafka")
                .arg("--node-id")
                .arg(node_id.to_string())
                .arg("--port")
                .arg(port.to_string())
                .arg("--raft-port")
                .arg(raft_port.to_string())
                .arg("--data-dir")
                .arg(&node_dir);

            if config.auto_create_topics {
                cmd.arg("--auto-create-topics");
            }

            // Add peers.
            let peers = &peer_args[node_id as usize - 1];
            for peer_arg in peers {
                cmd.arg(peer_arg);
            }

            cmd.stdout(Stdio::null()).stderr(Stdio::null());

            let child = cmd.spawn().map_err(ExecutorError::SpawnFailed)?;
            processes.push(child);
        }

        // Build bootstrap servers string.
        let bootstrap_servers: Vec<String> = (1..=config.node_count)
            .map(|id| {
                let port = config.base_port + u16::try_from(id).unwrap_or(0);
                format!("localhost:{port}")
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

        let port = self.config.base_port + u16::try_from(node_id).unwrap_or(0);
        let raft_port = self.config.raft_base_port + u16::try_from(node_id).unwrap_or(0);
        let node_dir = self.config.data_dir.join(format!("node-{node_id}"));

        let mut cmd = Command::new(&self.config.binary_path);
        cmd.arg("--protocol")
            .arg("kafka")
            .arg("--node-id")
            .arg(node_id.to_string())
            .arg("--port")
            .arg(port.to_string())
            .arg("--raft-port")
            .arg(raft_port.to_string())
            .arg("--data-dir")
            .arg(&node_dir);

        if self.config.auto_create_topics {
            cmd.arg("--auto-create-topics");
        }

        // Add peers.
        for other_id in 1..=self.config.node_count {
            if u64::from(other_id) != node_id {
                let other_port = self.config.base_port + u16::try_from(other_id).unwrap_or(0);
                cmd.arg("--peer");
                cmd.arg(format!("{other_id}:localhost:{other_port}"));
            }
        }

        cmd.stdout(Stdio::null()).stderr(Stdio::null());

        let child = cmd.spawn().map_err(ExecutorError::SpawnFailed)?;
        self.processes[idx] = child;
        Ok(())
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
    producer: BaseProducer,
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
        let bootstrap_servers = cluster.bootstrap_servers().to_string();

        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("acks", "all")
            .create()?;

        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", "helix-workload-default")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;

        Ok(Self {
            bootstrap_servers,
            producer,
            consumer,
            consumer_positions: Arc::new(RwLock::new(HashMap::new())),
            start_time: std::time::Instant::now(),
        })
    }

    /// Creates an executor with custom bootstrap servers.
    ///
    /// # Errors
    ///
    /// Returns an error if Kafka clients cannot be created.
    pub fn with_bootstrap_servers(bootstrap_servers: &str) -> Result<Self, ExecutorError> {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("acks", "all")
            .create()?;

        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", "helix-workload-default")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
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
        let record: BaseRecord<'_, (), [u8]> = BaseRecord::to(topic)
            .partition(partition)
            .payload(payload.as_ref());

        self.producer.send(record).map_err(|(err, _record)| {
            ExecutorError::Generic {
                code: -1,
                message: err.to_string(),
            }
        })?;

        // Flush to ensure delivery.
        self.producer.flush(Duration::from_secs(5)).ok();

        // For real Kafka, we don't get the offset back from send directly.
        // We'd need to use delivery callbacks for that. For now, return 0.
        // In a real implementation, we'd use the delivery future.
        Ok(0)
    }

    async fn poll(
        &self,
        topic: &str,
        partition: i32,
        start_offset: u64,
        max_messages: u32,
    ) -> Result<Vec<(u64, Bytes)>, ExecutorError> {
        use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

        // Assign partition with specific offset.
        let mut tpl = TopicPartitionList::new();
        #[allow(clippy::cast_possible_wrap)]
        tpl.add_partition_offset(topic, partition, Offset::Offset(start_offset as i64))
            .ok();

        self.consumer.assign(&tpl).map_err(|e| ExecutorError::Generic {
            code: -1,
            message: e.to_string(),
        })?;

        let mut messages = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);

        while messages.len() < max_messages as usize {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match self.consumer.poll(remaining) {
                Some(Ok(msg)) => {
                    #[allow(clippy::cast_sign_loss)]
                    let offset = msg.offset() as u64;
                    let payload = msg
                        .payload()
                        .map(Bytes::copy_from_slice)
                        .unwrap_or_default();
                    messages.push((offset, payload));
                }
                Some(Err(e)) => {
                    if messages.is_empty() {
                        return Err(ExecutorError::Generic {
                            code: -1,
                            message: e.to_string(),
                        });
                    }
                    break;
                }
                None => {
                    // No more messages available.
                    break;
                }
            }
        }

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
        match self.consumer.committed(Duration::from_secs(5)) {
            Ok(committed) => {
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
            }
            Err(e) => Err(ExecutorError::Generic {
                code: -1,
                message: e.to_string(),
            }),
        }
    }

    fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    async fn wait_ready(&self, timeout: Duration) -> Result<(), ExecutorError> {
        let deadline = std::time::Instant::now() + timeout;

        while std::time::Instant::now() < deadline {
            // Try to fetch metadata to check if cluster is ready.
            if let Ok(metadata) = self.producer.client().fetch_metadata(None, Duration::from_secs(2)) {
                if !metadata.brokers().is_empty() {
                    return Ok(());
                }
            } else {
                // Not ready yet.
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        Err(ExecutorError::Timeout(format!(
            "cluster not ready after {timeout:?}"
        )))
    }
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
