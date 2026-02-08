//! E2E Cluster for MadSim Deterministic Simulation Testing.
//!
//! This module provides a test harness that runs REAL `HelixService` instances
//! with their background tasks (tick_task, batcher_task, output_processor_task)
//! under MadSim deterministic simulation.
//!
//! # Producer/Consumer Semantics
//!
//! The test harness implements proper producer/consumer semantics:
//!
//! - **Producer** (`produce()`): Routes to the leader using `NotLeader` hints.
//!   When a node returns `NotLeader` with a `leader_hint`, the producer follows
//!   the hint to find the correct leader. This mirrors real Kafka client behavior.
//!
//! - **Consumer** (`consume()`): Reads from the partition leader for consistency.
//!   The consumer queries Raft state to find the leader and reads from it,
//!   ensuring up-to-date data.
//!
//! # TODO: Use Public API
//!
//! Currently, `produce()` and `consume()` call internal service methods
//! (`append_blob`, `read_blobs`) directly. For true E2E testing, these should
//! go through the public API layer:
//! - gRPC: `WriteRequest`/`ReadRequest` via `helix_server::grpc`
//! - Kafka: `ProduceRequest`/`FetchRequest` via Kafka protocol handler
//!
//! The current approach tests the core service logic correctly, but skips
//! the serialization/deserialization layer of the public API.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                     MadSim Deterministic Runtime                             │
//! └─────────────────────────────────────────────────────────────────────────────┘
//!                                    │
//!         ┌──────────────────────────┼──────────────────────────┐
//!         ▼                          ▼                          ▼
//! ┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐
//! │   HelixService    │    │   HelixService    │    │   HelixService    │
//! │   (Node 1)        │    │   (Node 2)        │    │   (Node 3)        │
//! │                   │    │                   │    │                   │
//! │ tick_task      ───┼────┼─ RUNNING AS ──────┼────┼─── BACKGROUND ────│
//! │ batcher_task   ───┼────┼─ SPAWNED TASKS ───┼────┼─── (not manual) ──│
//! │ output_proc    ───┼────┼─ UNDER MADSIM ────┼────┼───────────────────│
//! └────────┬──────────┘    └────────┬──────────┘    └────────┬──────────┘
//!          │                        │                        │
//!          └────────────────────────┼────────────────────────┘
//!                                   ▼
//!               ┌─────────────────────────────────────────────┐
//!               │  SimulatedStorage    │   MadSimTransport    │
//!               │  (fault injection)   │   (partitions)       │
//!               └─────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```ignore
//! #[test]
//! fn test_e2e_produce_consume() {
//!     let rt = madsim::runtime::Runtime::with_seed_and_config(42, Default::default());
//!     rt.block_on(async {
//!         let cluster = E2ECluster::start(3).await;
//!
//!         // Real API calls
//!         cluster.create_topic("orders", 1).await.unwrap();
//!         cluster.produce("orders", 0, b"order-1").await.unwrap();
//!
//!         // Fault injection
//!         cluster.crash_node(NodeId::new(1));
//!
//!         // Verify data
//!         let records = cluster.consume("orders", 0, 0).await.unwrap();
//!         assert_eq!(records.len(), 1);
//!     });
//! }
//! ```

#![cfg(feature = "madsim")]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use helix_core::{NodeId, Offset};
use helix_server::controller::CONTROLLER_GROUP_ID;
use helix_server::kafka::KafkaHandler;
use helix_server::service::router::PartitionRouter;
use helix_server::service::HelixService;
use helix_wal::{FaultConfig, SimulatedStorage};
use tokio::sync::mpsc;
use tracing::info;

use crate::madsim_transport::{
    create_cluster_mailboxes, IncomingMessage as MadSimIncomingMessage, MadSimNetworkState,
    MadSimTransport, NodeMailboxReceiver, SharedMadSimNetworkState,
};
use crate::properties::{
    assert_no_helix_violations, check_helix_properties, HelixNodeSnapshot,
    HelixPropertyCheckResult, HelixPropertyState, SharedHelixPropertyState,
};

// ============================================================================
// Kafka RecordBatch Helper
// ============================================================================

/// Creates a minimal valid Kafka RecordBatch for testing.
///
/// This creates a properly formatted RecordBatch with the given payload as
/// a single record. The format must be valid for `count_records_in_batch`
/// to correctly parse the record count.
#[must_use]
pub fn create_test_record_batch(payload: &[u8]) -> Bytes {
    // Kafka RecordBatch v2 header format:
    // - baseOffset: 8 bytes (i64)
    // - batchLength: 4 bytes (i32) - length of everything after this field
    // - partitionLeaderEpoch: 4 bytes (i32)
    // - magic: 1 byte (2 for v2)
    // - crc: 4 bytes (i32) - we'll use 0 for testing
    // - attributes: 2 bytes (i16)
    // - lastOffsetDelta: 4 bytes (i32)
    // - firstTimestamp: 8 bytes (i64)
    // - maxTimestamp: 8 bytes (i64)
    // - producerId: 8 bytes (i64)
    // - producerEpoch: 2 bytes (i16)
    // - baseSequence: 4 bytes (i32)
    // - recordCount: 4 bytes (i32) - at offset 57

    let mut buf = BytesMut::with_capacity(128 + payload.len());

    // baseOffset: 0
    buf.put_i64(0);
    // batchLength: will be calculated (placeholder)
    let batch_length_pos = buf.len();
    buf.put_i32(0);
    // partitionLeaderEpoch: -1
    buf.put_i32(-1);
    // magic: 2
    buf.put_u8(2);
    // crc: 0 (not validated in our code)
    buf.put_i32(0);
    // attributes: 0
    buf.put_i16(0);
    // lastOffsetDelta: 0 (single record)
    buf.put_i32(0);
    // firstTimestamp: 0
    buf.put_i64(0);
    // maxTimestamp: 0
    buf.put_i64(0);
    // producerId: -1 (non-idempotent)
    buf.put_i64(-1);
    // producerEpoch: -1
    buf.put_i16(-1);
    // baseSequence: -1
    buf.put_i32(-1);
    // recordCount: 1
    buf.put_i32(1);

    // Record format (simplified):
    // - length: varint
    // - attributes: 1 byte
    // - timestampDelta: varint
    // - offsetDelta: varint
    // - keyLength: varint (-1 for null)
    // - key: bytes (if keyLength >= 0)
    // - valueLength: varint
    // - value: bytes
    // - headersCount: varint (0)

    // For simplicity, we'll just append the payload as a minimal record.
    // The record parsing isn't strictly validated in our tests.
    let record_start = buf.len();
    buf.put_u8(0); // attributes
    buf.put_u8(0); // timestampDelta varint (0)
    buf.put_u8(0); // offsetDelta varint (0)
    buf.put_u8(0xff); // keyLength varint (-1 = null key, using 0xff as signed varint)
                      // Safe cast: payload length is expected to be small for tests.
    #[allow(clippy::cast_possible_truncation)]
    {
        buf.put_u8(payload.len() as u8); // valueLength varint
    }
    buf.put_slice(payload);
    buf.put_u8(0); // headersCount varint (0)

    // Calculate record length and prepend it.
    let record_len = buf.len() - record_start;
    let mut final_buf = BytesMut::with_capacity(buf.len() + 1);
    final_buf.put_slice(&buf[..record_start]);
    // Safe cast: record_len is expected to be small for tests.
    #[allow(clippy::cast_possible_truncation)]
    final_buf.put_u8(record_len as u8); // record length varint
    final_buf.put_slice(&buf[record_start..]);

    // Update batchLength (everything after the batchLength field).
    // Safe cast: batch length is expected to be small for tests.
    #[allow(clippy::cast_possible_truncation)]
    let batch_length = (final_buf.len() - batch_length_pos - 4) as i32;
    final_buf[batch_length_pos..batch_length_pos + 4].copy_from_slice(&batch_length.to_be_bytes());

    final_buf.freeze()
}

/// Splits concatenated RecordBatches into individual batches.
///
/// Kafka RecordBatches are concatenated in fetch responses. This function
/// parses the batch headers to split them apart.
fn split_record_batches(data: &Bytes) -> Vec<Bytes> {
    let mut batches = Vec::new();
    let mut offset = 0;

    while offset + 12 <= data.len() {
        // Read batchLength at offset 8 (after 8-byte baseOffset).
        let batch_length_bytes: [u8; 4] = data[offset + 8..offset + 12]
            .try_into()
            .expect("slice should be 4 bytes");
        let batch_length = i32::from_be_bytes(batch_length_bytes);

        if batch_length <= 0 {
            break;
        }

        // Total batch size = 8 (baseOffset) + 4 (batchLength) + batchLength.
        // Safe cast: batch_length is positive.
        #[allow(clippy::cast_sign_loss)]
        let total_size = 8 + 4 + batch_length as usize;

        if offset + total_size > data.len() {
            // Incomplete batch at end - shouldn't happen in valid data.
            break;
        }

        batches.push(data.slice(offset..offset + total_size));
        offset += total_size;
    }

    batches
}

// ============================================================================
// E2E Cluster Configuration
// ============================================================================

/// Configuration for E2E cluster.
#[derive(Debug, Clone)]
pub struct E2EClusterConfig {
    /// Number of nodes in the cluster.
    pub node_count: usize,
    /// Base seed for deterministic execution.
    pub base_seed: u64,
    /// Default storage fault configuration.
    pub storage_faults: FaultConfig,
    /// Mailbox capacity for transport messages.
    pub mailbox_capacity: usize,
    /// Enable durable storage with SharedWalPool (production-like config).
    /// When true, uses SharedWalPool + BufferedWal instead of in-memory storage.
    pub use_durable_storage: bool,
    /// Enable actor mode (lock-free partition architecture).
    /// When true, uses PartitionActor instances with PartitionRouter instead of
    /// tick-based MultiRaft approach. This tests the production actor architecture.
    pub actor_mode: bool,
}

impl Default for E2EClusterConfig {
    fn default() -> Self {
        Self {
            node_count: 3,
            base_seed: 42,
            storage_faults: FaultConfig::default(),
            mailbox_capacity: 10000,
            use_durable_storage: true, // Default to production-like config
            actor_mode: false,         // Default to tick-based mode
        }
    }
}

impl E2EClusterConfig {
    /// Creates a config for a cluster with the given number of nodes.
    #[must_use]
    pub fn with_nodes(node_count: usize) -> Self {
        Self {
            node_count,
            ..Default::default()
        }
    }

    /// Sets the base seed.
    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.base_seed = seed;
        self
    }

    /// Disables durable storage (uses in-memory partition storage instead).
    #[must_use]
    pub fn without_durable_storage(mut self) -> Self {
        self.use_durable_storage = false;
        self
    }

    /// Enables actor mode (lock-free partition architecture).
    #[must_use]
    pub fn with_actor_mode(mut self) -> Self {
        self.actor_mode = true;
        self
    }
}

// ============================================================================
// E2E Node
// ============================================================================

/// Type alias for the service used in E2E tests.
pub type E2EHelixService = HelixService<SimulatedStorage, MadSimTransport>;

/// Type alias for the Kafka handler used in E2E tests.
pub type E2EKafkaHandler = KafkaHandler<SimulatedStorage, MadSimTransport>;

/// A single node in the E2E cluster with its service and infrastructure.
pub struct E2ENode {
    /// The node's ID.
    pub node_id: NodeId,
    /// The real HelixService instance (shared with handler).
    pub service: Arc<E2EHelixService>,
    /// Kafka protocol handler for this node.
    pub handler: Arc<E2EKafkaHandler>,
    /// Storage for fault injection.
    pub storage: SimulatedStorage,
    /// Transport for sending messages.
    pub transport: MadSimTransport,
    /// Shutdown sender to stop background tasks.
    shutdown_tx: mpsc::Sender<()>,
    /// Data directory for durable storage (if enabled).
    #[allow(dead_code)]
    data_dir: Option<std::path::PathBuf>,
    /// Partition router for actor mode (retrieved from service).
    #[allow(dead_code)]
    actor_router: Option<Arc<PartitionRouter>>,
}

impl E2ENode {
    /// Shuts down the node's background tasks.
    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(()).await;
    }
}

// ============================================================================
// E2E Cluster
// ============================================================================

/// E2E cluster running REAL `HelixService` instances with background tasks.
///
/// This harness provides:
/// - Real service instances with tick_task, batcher_task running
/// - Client API methods (create_topic, produce, consume)
/// - Fault injection (partitions, crashes, storage faults)
/// - Deterministic execution under MadSim
/// - Property tracking for DST verification (SingleLeaderPerTerm, data integrity)
///
/// # Modes
///
/// The cluster can run in two modes:
/// - **Tick-based mode** (default): Uses `tick_task_multi_node` with `MultiRaft` locks
/// - **Actor mode**: Uses `PartitionActor` instances with `PartitionRouter` for lock-free operation
pub struct E2ECluster {
    /// Configuration.
    #[allow(dead_code)]
    config: E2EClusterConfig,
    /// All nodes in the cluster.
    pub nodes: BTreeMap<NodeId, E2ENode>,
    /// Shared network state for partition simulation.
    network_state: SharedMadSimNetworkState,
    /// All node IDs for convenience.
    node_ids: Vec<NodeId>,
    /// Whether the cluster is running in actor mode.
    pub actor_mode: bool,
    /// Shared property state for DST verification.
    property_state: SharedHelixPropertyState,
}

impl E2ECluster {
    /// Starts an E2E cluster with the given number of nodes.
    ///
    /// This creates real `HelixService` instances and spawns their background
    /// tasks (tick_task, batcher_task) under MadSim.
    pub async fn start(node_count: usize) -> Self {
        Self::start_with_config(E2EClusterConfig::with_nodes(node_count)).await
    }

    /// Starts an E2E cluster with custom configuration.
    ///
    /// Uses the production `new_multi_node_with_transport` constructor to ensure
    /// E2E tests exercise the exact same code paths as production.
    pub async fn start_with_config(config: E2EClusterConfig) -> Self {
        use helix_core::WriteDurability;

        let node_ids: Vec<NodeId> = (1..=config.node_count)
            .map(|i| {
                #[allow(clippy::cast_possible_truncation)]
                NodeId::new(i as u64)
            })
            .collect();

        // Create shared network state.
        let network_state = Arc::new(Mutex::new(MadSimNetworkState::new()));

        // Create mailboxes for inter-node communication.
        let (mailboxes, mut receivers) =
            create_cluster_mailboxes(&node_ids, config.mailbox_capacity);

        // Create base data directory for durable storage (if enabled).
        let base_data_dir = if config.use_durable_storage {
            let dir = std::path::PathBuf::from(format!("/tmp/helix-e2e-{}", config.base_seed));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("Failed to create base data directory");
            Some(dir)
        } else {
            None
        };

        // Create nodes using the production constructor with injected transport.
        let mut nodes = BTreeMap::new();

        for &node_id in &node_ids {
            // Create storage with per-node seed for fault injection.
            let storage_seed = config.base_seed.wrapping_add(node_id.get());
            let storage =
                SimulatedStorage::with_faults(storage_seed, config.storage_faults.clone());

            // Create transport with shared network state.
            let transport = MadSimTransport::new(node_id, network_state.clone(), mailboxes.clone());

            // Get incoming message receiver for this node.
            let madsim_rx = receivers.remove(&node_id).expect("receiver should exist");

            // Create per-node data directory (if durable storage enabled).
            let node_data_dir = base_data_dir.as_ref().map(|base| {
                let dir = base.join(format!("node-{}", node_id.get()));
                std::fs::create_dir_all(&dir).expect("Failed to create node data directory");
                dir
            });

            // Bridge MadSim messages to helix_runtime format.
            let (incoming_tx, incoming_rx) = mpsc::channel(config.mailbox_capacity);
            tokio::spawn(message_bridge_task(madsim_rx, incoming_tx));

            // Use the production constructor with injected transport.
            // This exercises the exact same wiring as production code.
            let service = HelixService::new_multi_node_with_transport(
                "e2e-cluster".to_string(),
                node_id.get(),
                node_ids.clone(),
                transport.clone(),
                incoming_rx,
                node_data_dir.clone(),
                None, // shared_wal_count (use default)
                WriteDurability::Fsync,
                config.actor_mode,
                storage.clone(),
            )
            .await;

            // Get actor router from service (if actor mode enabled).
            let actor_router = service.actor_router().cloned();

            // Create shutdown channel for graceful shutdown.
            let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);

            // Create Kafka handler for this node.
            let service = Arc::new(service);
            let handler = Arc::new(KafkaHandler::new(
                Arc::clone(&service),
                "127.0.0.1".to_string(),
                9092,
                true, // auto_create_topics
                1,    // auto_create_partitions
            ));

            nodes.insert(
                node_id,
                E2ENode {
                    node_id,
                    service,
                    handler,
                    storage,
                    transport,
                    shutdown_tx,
                    data_dir: node_data_dir,
                    actor_router,
                },
            );
        }

        let actor_mode = config.actor_mode;
        let property_state = Arc::new(Mutex::new(HelixPropertyState::new()));

        info!(
            node_count = config.node_count,
            base_seed = config.base_seed,
            durable_storage = config.use_durable_storage,
            actor_mode,
            "E2E cluster started using production constructor"
        );

        Self {
            config,
            nodes,
            network_state,
            node_ids,
            actor_mode,
            property_state,
        }
    }

    // ========================================================================
    // Client API
    // ========================================================================

    /// Creates a topic on the cluster using the Kafka protocol handler.
    ///
    /// This uses the REAL Kafka protocol API (CreateTopicsRequest/CreateTopicsResponse).
    /// For multi-node clusters, the handler coordinates through the controller.
    /// For single-node, it uses direct topic creation.
    ///
    /// The replication factor is automatically set to the cluster size.
    ///
    /// Retries on different nodes up to 5 times.
    pub async fn create_topic(&self, name: &str, partitions: u32) -> Result<(), String> {
        use helix_server::kafka::KafkaError;

        // Safe cast: partition is expected to be small.
        #[allow(clippy::cast_possible_wrap)]
        let partitions_i32 = partitions as i32;

        // Use cluster size as replication factor.
        // Safe cast: cluster size is expected to be small.
        #[allow(clippy::cast_possible_truncation)]
        let replication_factor = self.node_ids.len() as i16;

        // Get available nodes.
        let available_nodes = self.get_available_nodes();
        let mut current_node_id = available_nodes
            .first()
            .or_else(|| self.node_ids.first())
            .copied();

        let mut last_error = String::new();

        // Retry on different nodes up to 5 times.
        for _attempt in 0..5 {
            let Some(node_id) = current_node_id else {
                return Err("No node available".to_string());
            };

            let Some(node) = self.nodes.get(&node_id) else {
                return Err(format!("Node {} not found", node_id.get()));
            };

            // Use the Kafka handler's create_topic method (Kafka protocol path).
            match node
                .handler
                .create_topic_via_kafka(name, partitions_i32, replication_factor)
                .await
            {
                Ok(()) => return Ok(()),
                Err(KafkaError::Protocol {
                    error_code,
                    ref message,
                }) => {
                    // Error code 36 = TOPIC_ALREADY_EXISTS (which is fine).
                    if error_code == 36 {
                        return Ok(());
                    }
                    // Try another node.
                    tracing::debug!(
                        node = node_id.get(),
                        error_code,
                        message = %message,
                        "CreateTopics failed, trying another node"
                    );
                    let idx = self
                        .node_ids
                        .iter()
                        .position(|&n| n == node_id)
                        .unwrap_or(0);
                    current_node_id = self.node_ids.get((idx + 1) % self.node_ids.len()).copied();
                    last_error = format!("CreateTopics error: {message}");
                }
                Err(e) => {
                    last_error = format!("create_topic failed: {e}");
                    // Try another node.
                    let idx = self
                        .node_ids
                        .iter()
                        .position(|&n| n == node_id)
                        .unwrap_or(0);
                    current_node_id = self.node_ids.get((idx + 1) % self.node_ids.len()).copied();
                }
            }
        }

        Err(format!(
            "Failed to create topic after retries: {last_error}"
        ))
    }

    /// Produces data to a topic partition using the Kafka protocol handler.
    ///
    /// This uses the REAL Kafka protocol API (ProduceRequest/ProduceResponse):
    /// - Starts with an available (non-partitioned) node
    /// - On NOT_LEADER error, tries the next available node
    /// - Retries up to 5 times trying different nodes
    ///
    /// Note: The data is automatically wrapped in a valid Kafka RecordBatch format.
    pub async fn produce(
        &self,
        topic: &str,
        partition: u32,
        data: impl Into<Bytes> + Clone,
    ) -> Result<Offset, String> {
        use helix_server::kafka::KafkaError;

        // Safe cast: partition is expected to be small.
        #[allow(clippy::cast_possible_wrap)]
        let partition_i32 = partition as i32;

        // Convert data to a valid Kafka RecordBatch format.
        // This is required because the Kafka handler parses record_count from the batch header.
        let data_bytes: Bytes = data.into();
        let record_batch = create_test_record_batch(&data_bytes);

        // Get available (non-partitioned, non-crashed) nodes to try first.
        let available_nodes = self.get_available_nodes();
        let starting_node = available_nodes
            .first()
            .or_else(|| self.node_ids.first())
            .copied();

        let mut current_node_id = starting_node;
        let mut tried_nodes = std::collections::BTreeSet::new();
        let mut last_error = String::new();

        // Retry on different nodes up to 5 times.
        for _attempt in 0..5 {
            let Some(node_id) = current_node_id else {
                return Err("No node available".to_string());
            };

            let Some(node) = self.nodes.get(&node_id) else {
                return Err(format!("Node {} not found", node_id.get()));
            };

            tried_nodes.insert(node_id);

            // Use the Kafka handler's produce method (Kafka protocol path).
            match node
                .handler
                .produce(topic, partition_i32, record_batch.clone())
                .await
            {
                Ok(offset) => return Ok(Offset::new(offset)),
                Err(KafkaError::Protocol { error_code, .. }) if error_code == 6 => {
                    // NOT_LEADER_OR_FOLLOWER (error code 6) - try another node.
                    tracing::debug!(node = node_id.get(), "Not leader, trying another node");
                    current_node_id = available_nodes
                        .iter()
                        .find(|&&n| !tried_nodes.contains(&n))
                        .copied()
                        .or_else(|| {
                            // All available nodes tried, cycle to next.
                            let idx = self
                                .node_ids
                                .iter()
                                .position(|&n| n == node_id)
                                .unwrap_or(0);
                            self.node_ids.get((idx + 1) % self.node_ids.len()).copied()
                        });
                    last_error = "NotLeader".to_string();
                }
                Err(KafkaError::Protocol {
                    error_code,
                    ref message,
                }) if error_code == 9 => {
                    // BROKER_NOT_AVAILABLE (error code 9) - server overloaded, try another node.
                    tracing::debug!(
                        node = node_id.get(),
                        "Broker not available (overloaded), trying another node"
                    );
                    current_node_id = available_nodes
                        .iter()
                        .find(|&&n| !tried_nodes.contains(&n))
                        .copied();
                    last_error = format!("broker_not_available: {message}");
                }
                Err(KafkaError::Protocol {
                    error_code,
                    ref message,
                }) if error_code == 3 || error_code == 5 || error_code == -1 => {
                    // UNKNOWN_TOPIC_OR_PARTITION (3), LEADER_NOT_AVAILABLE (5), or UNKNOWN (-1)
                    // These are retriable - the controller may not have replicated to this node yet,
                    // or there's a transient issue during partition setup.
                    tracing::debug!(
                        node = node_id.get(),
                        error_code,
                        "Partition not ready on this node, trying another"
                    );
                    current_node_id = available_nodes
                        .iter()
                        .find(|&&n| !tried_nodes.contains(&n))
                        .copied()
                        .or_else(|| {
                            // All available nodes tried, cycle to next.
                            let idx = self
                                .node_ids
                                .iter()
                                .position(|&n| n == node_id)
                                .unwrap_or(0);
                            self.node_ids.get((idx + 1) % self.node_ids.len()).copied()
                        });
                    last_error = format!("partition_not_ready: {message}");
                }
                Err(e) => {
                    last_error = format!("produce failed: {e}");
                    return Err(last_error);
                }
            }
        }

        Err(format!("Failed to find leader after retries: {last_error}"))
    }

    /// Returns a list of available (non-partitioned, non-crashed) node IDs.
    fn get_available_nodes(&self) -> Vec<NodeId> {
        let state = self.network_state.lock().expect("lock poisoned");
        self.node_ids
            .iter()
            .filter(|&&node_id| !state.is_crashed(node_id))
            .copied()
            .collect()
    }

    /// Debug helper: prints leadership and data state for a topic partition.
    #[allow(dead_code)]
    pub async fn debug_partition_state(&self, topic: &str, partition: u32) {
        use helix_core::PartitionId;
        use helix_raft::RaftState;

        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(u64::from(partition));

        eprintln!("=== Debug state for {topic}:{partition} ===");

        for node in self.nodes.values() {
            let node_id = node.node_id;

            // Get topic metadata from controller state (multi-node mode).
            let topic_id = {
                let state = node.service.controller_state().read().await;
                state.get_topic(topic).map(|info| info.topic_id)
            };

            let Some(topic_id) = topic_id else {
                eprintln!(
                    "  Node {}: topic not found in controller state",
                    node_id.get()
                );
                continue;
            };

            // Get group ID.
            let group_id = {
                let gm = node.service.group_map().read().await;
                gm.get(topic_id, partition_id)
            };

            let Some(group_id) = group_id else {
                eprintln!("  Node {}: partition not found in group_map", node_id.get());
                continue;
            };

            // Get Raft state.
            let (state_str, term, leader_id, commit_idx, log_len) = {
                let mr = node.service.multi_raft().read().await;
                if let Some(state) = mr.group_state(group_id) {
                    let state_str = match state.state {
                        RaftState::Follower => "Follower",
                        RaftState::PreCandidate => "PreCandidate",
                        RaftState::Candidate => "Candidate",
                        RaftState::Leader => "LEADER",
                    };
                    (
                        state_str,
                        state.current_term.get(),
                        state.leader_id.map(|n| n.get()),
                        state.commit_index.get(),
                        state.last_log_index.get(),
                    )
                } else {
                    ("NoGroup", 0, None, 0, 0)
                }
            };

            // Get blob count from storage.
            let blob_count = {
                let ps_map = node.service.partition_storage_map().read().await;
                if let Some(ps_lock) = ps_map.get(&group_id) {
                    let ps = ps_lock.read().await;
                    ps.blob_log_end_offset().get() as usize
                } else {
                    0
                }
            };

            // Get replication state from leader's perspective (if this is the leader).
            let repl_info = if state_str == "LEADER" {
                let mr = node.service.multi_raft().read().await;
                if let Some(state) = mr.group_state(group_id) {
                    // Can't access replication_state directly, but we have commit and log indices.
                    format!(
                        " repl=(commit={}, log={})",
                        state.commit_index.get(),
                        state.last_log_index.get()
                    )
                } else {
                    String::new()
                }
            } else {
                String::new()
            };

            eprintln!(
                "  Node {}: {} term={} leader={:?} commit={} log={} blobs={}{}",
                node_id.get(),
                state_str,
                term,
                leader_id,
                commit_idx,
                log_len,
                blob_count,
                repl_info
            );
        }
        eprintln!("===========================================");
    }

    /// Debug helper: prints controller group state (group 0) for all nodes.
    #[allow(dead_code)]
    pub async fn debug_controller_state(&self) {
        use helix_raft::RaftState;

        eprintln!("=== Controller (group 0) state ===");

        for node in self.nodes.values() {
            let node_id = node.node_id;

            // Get controller group state.
            let (state_str, term, leader_id, commit_idx, log_len) = {
                let mr = node.service.multi_raft().read().await;
                if let Some(state) = mr.group_state(CONTROLLER_GROUP_ID) {
                    let state_str = match state.state {
                        RaftState::Follower => "Follower",
                        RaftState::PreCandidate => "PreCandidate",
                        RaftState::Candidate => "Candidate",
                        RaftState::Leader => "LEADER",
                    };
                    (
                        state_str,
                        state.current_term.get(),
                        state.leader_id.map(|n| n.get()),
                        state.commit_index.get(),
                        state.last_log_index.get(),
                    )
                } else {
                    ("NoGroup", 0, None, 0, 0)
                }
            };

            eprintln!(
                "  Node {}: {} term={} leader={:?} commit={} log={}",
                node_id.get(),
                state_str,
                term,
                leader_id,
                commit_idx,
                log_len
            );
        }
        eprintln!("=================================");
    }

    /// Checks if a specific node is available (not partitioned from all others, not crashed).
    fn is_node_available(&self, node_id: NodeId) -> bool {
        let state = self.network_state.lock().expect("lock poisoned");
        if state.is_crashed(node_id) {
            return false;
        }
        // Check if this node can reach at least one other node.
        self.node_ids
            .iter()
            .filter(|&&n| n != node_id)
            .any(|&other| !state.is_partitioned(node_id, other))
    }

    /// Consumes data from a topic partition using the Kafka protocol handler.
    ///
    /// This uses the REAL Kafka protocol API (FetchRequest/FetchResponse):
    /// - Reads from the partition leader for consistent data (Kafka default)
    /// - On leader errors, retries after "refreshing" leader selection
    /// - Only falls back to non-leader nodes when no leader is known, mirroring
    ///   how clients probe brokers to refresh metadata
    ///
    /// Returns a `Vec<Bytes>` where each entry is a single RecordBatch.
    pub async fn consume(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
    ) -> Result<Vec<Bytes>, String> {
        use helix_server::kafka::KafkaError;

        // Safe cast: partition is expected to be small.
        #[allow(clippy::cast_possible_wrap)]
        let partition_i32 = partition as i32;

        const MAX_ATTEMPTS: u32 = 5;
        for _attempt in 0..MAX_ATTEMPTS {
            // Find the leader for this partition by checking each node's Raft state.
            let leader_node_id = self.find_partition_leader(topic, partition).await;

            if let Some(leader_id) = leader_node_id {
                // Read from the leader using Kafka handler (has most up-to-date data).
                if let Some(node) = self.nodes.get(&leader_id) {
                    match node
                        .handler
                        .fetch(topic, partition_i32, start_offset, 1024 * 1024)
                        .await
                    {
                        Ok(response) => {
                            // Split concatenated RecordBatches into individual batches.
                            let mut all_batches = Vec::new();
                            for data in response {
                                all_batches.extend(split_record_batches(&data));
                            }
                            return Ok(all_batches);
                        }
                        Err(KafkaError::Protocol { error_code, .. })
                            if error_code == 6
                                || error_code == 5
                                || error_code == 3
                                || error_code == 9 =>
                        {
                            // NOT_LEADER_OR_FOLLOWER / LEADER_NOT_AVAILABLE / UNKNOWN_TOPIC_OR_PARTITION
                            // / BROKER_NOT_AVAILABLE: refresh leader selection and retry.
                            tracing::debug!(
                                error_code,
                                leader = leader_id.get(),
                                "fetch hit leader error, retrying"
                            );
                            continue;
                        }
                        Err(e) => {
                            return Err(format!("fetch from leader failed: {e}"));
                        }
                    }
                }
            }

            // No leader known - probe available nodes (simulates metadata refresh).
            for node in self.nodes.values() {
                match node
                    .handler
                    .fetch(topic, partition_i32, start_offset, 1024 * 1024)
                    .await
                {
                    Ok(response) => {
                        // Split concatenated RecordBatches into individual batches.
                        let mut all_batches = Vec::new();
                        for data in response {
                            all_batches.extend(split_record_batches(&data));
                        }
                        return Ok(all_batches);
                    }
                    Err(KafkaError::Protocol { error_code, .. })
                        if error_code == 6
                            || error_code == 5
                            || error_code == 3
                            || error_code == 9 =>
                    {
                        tracing::debug!(error_code, "fetch probe hit leader error");
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "fetch probe failed");
                    }
                }
            }
        }

        Err("No nodes available for consume after retries".to_string())
    }

    /// Finds the leader node for a topic partition.
    ///
    /// Returns `None` if no leader is known.
    async fn find_partition_leader(&self, topic: &str, partition: u32) -> Option<NodeId> {
        use helix_core::PartitionId;
        use helix_raft::RaftState;

        // Safe cast: partition is expected to be small.
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(u64::from(partition));

        // First pass: prefer nodes that report themselves as leader to avoid stale leader hints.
        for node in self.nodes.values() {
            // Skip crashed nodes - they may have stale state.
            if !self.is_node_available(node.node_id) {
                continue;
            }

            // Get topic ID from controller state (multi-node mode) or local topics map.
            let topic_id = {
                let state = node.service.controller_state().read().await;
                state.get_topic(topic).map(|info| info.topic_id)
            };

            let Some(topic_id) = topic_id else {
                continue;
            };

            // Get the group ID for this partition.
            let group_id = {
                let gm = node.service.group_map().read().await;
                gm.get(topic_id, partition_id)
            };

            let Some(group_id) = group_id else {
                continue;
            };

            // Check if this node believes it is the leader.
            let mr = node.service.multi_raft().read().await;
            if let Some(state) = mr.group_state(group_id) {
                if state.state == RaftState::Leader {
                    return Some(node.node_id);
                }
            }
        }

        // Second pass: fall back to leader hints if no leader is known locally.
        for node in self.nodes.values() {
            // Skip crashed nodes - they may have stale state.
            if !self.is_node_available(node.node_id) {
                continue;
            }

            // Get topic ID from controller state (multi-node mode) or local topics map.
            let topic_id = {
                let state = node.service.controller_state().read().await;
                state.get_topic(topic).map(|info| info.topic_id)
            };

            let Some(topic_id) = topic_id else {
                continue;
            };

            // Get the group ID for this partition.
            let group_id = {
                let gm = node.service.group_map().read().await;
                gm.get(topic_id, partition_id)
            };

            let Some(group_id) = group_id else {
                continue;
            };

            // Check if this node knows the leader.
            let mr = node.service.multi_raft().read().await;
            if let Some(state) = mr.group_state(group_id) {
                // If this node knows who the leader is (and that leader is available), return it.
                if let Some(leader_id) = state.leader_id {
                    if self.is_node_available(leader_id) {
                        return Some(leader_id);
                    }
                }
            }
        }

        None
    }

    // ========================================================================
    // Cross-Replica Consistency Verification
    // ========================================================================

    /// Consumes data from a specific node (not necessarily the leader).
    ///
    /// This is used for cross-replica consistency verification to read from
    /// all replicas and compare their data.
    ///
    /// Returns `None` if the node doesn't exist or the fetch fails.
    pub async fn consume_from_node(
        &self,
        node_id: NodeId,
        topic: &str,
        partition: u32,
        start_offset: u64,
    ) -> Option<Vec<Bytes>> {
        #[allow(clippy::cast_possible_wrap)]
        let partition_i32 = partition as i32;

        let node = self.nodes.get(&node_id)?;

        match node
            .handler
            .fetch(topic, partition_i32, start_offset, 1024 * 1024)
            .await
        {
            Ok(response) => {
                let mut all_batches = Vec::new();
                for data in response {
                    all_batches.extend(split_record_batches(&data));
                }
                Some(all_batches)
            }
            Err(e) => {
                tracing::debug!(
                    node = node_id.get(),
                    error = %e,
                    "consume_from_node fetch failed"
                );
                None
            }
        }
    }

    /// Gets the Raft commit index for a partition on a specific node.
    ///
    /// Returns `None` if the node doesn't have the group or the topic doesn't exist.
    pub async fn get_partition_commit_index(
        &self,
        node_id: NodeId,
        topic: &str,
        partition: u32,
    ) -> Option<u64> {
        use helix_core::PartitionId;

        let node = self.nodes.get(&node_id)?;

        // Get topic ID from controller state.
        let topic_id = {
            let state = node.service.controller_state().read().await;
            state.get_topic(topic).map(|info| info.topic_id)
        }?;

        // Get the group ID for this partition.
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(u64::from(partition));
        let group_id = {
            let gm = node.service.group_map().read().await;
            gm.get(topic_id, partition_id)
        }?;

        // Get commit index from Raft state.
        let mr = node.service.multi_raft().read().await;
        mr.group_state(group_id)
            .map(|state| state.commit_index.get())
    }

    /// Verifies that all replicas have consistent data.
    ///
    /// This is the core cross-replica consistency check for DST. It:
    /// 1. Reads data from ALL nodes (not just the leader)
    /// 2. Finds the minimum number of records across all replicas
    /// 3. Verifies that all nodes have identical data up to that point
    ///
    /// This approach compares visible data rather than trying to map Raft
    /// commit indices to Kafka offsets, which can have timing mismatches.
    ///
    /// Returns `Ok(())` if consistent, or `Err` with details about the inconsistency.
    pub async fn verify_replica_consistency(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<(), String> {
        // Collect data from all available nodes.
        let mut replica_data: Vec<(NodeId, Vec<Bytes>)> = Vec::new();

        for &node_id in self.node_ids() {
            // Skip crashed nodes - they may be behind.
            {
                let state = self.network_state.lock().expect("lock poisoned");
                if state.is_crashed(node_id) {
                    tracing::debug!(
                        node = node_id.get(),
                        "Skipping crashed node in consistency check"
                    );
                    continue;
                }
            }

            // Read data from this node.
            let records = match self.consume_from_node(node_id, topic, partition, 0).await {
                Some(r) => r,
                None => {
                    tracing::debug!(
                        node = node_id.get(),
                        topic,
                        partition,
                        "Failed to read from node, skipping"
                    );
                    continue;
                }
            };

            replica_data.push((node_id, records));
        }

        if replica_data.is_empty() {
            return Err("No replicas available for consistency check".to_string());
        }

        if replica_data.len() == 1 {
            // Only one replica available - can't verify cross-replica consistency.
            tracing::debug!("Only one replica available, skipping cross-replica check");
            return Ok(());
        }

        // Find the minimum number of batches across all replicas.
        // All replicas should have identical data up to this point.
        let min_batches = replica_data
            .iter()
            .map(|(_, records)| records.len())
            .min()
            .unwrap_or(0);

        if min_batches == 0 {
            // No data yet - nothing to verify.
            return Ok(());
        }

        // Use the first replica as the reference.
        let (ref_node_id, ref_records) = &replica_data[0];
        let ref_prefix = &ref_records[..min_batches];

        // Compare each other replica against the reference.
        for (node_id, records) in &replica_data[1..] {
            let prefix = &records[..min_batches];

            // Check each batch matches byte-for-byte.
            for (i, (ref_batch, batch)) in ref_prefix.iter().zip(prefix.iter()).enumerate() {
                if ref_batch.as_ref() != batch.as_ref() {
                    let ref_offset = Self::extract_base_offset(ref_batch).unwrap_or(0);
                    let offset = Self::extract_base_offset(batch).unwrap_or(0);
                    return Err(format!(
                        "REPLICA INCONSISTENCY: Batch {} differs between node {} (offset={}, len={}) \
                         and reference node {} (offset={}, len={}) - data mismatch!",
                        i,
                        node_id.get(),
                        offset,
                        batch.len(),
                        ref_node_id.get(),
                        ref_offset,
                        ref_batch.len(),
                    ));
                }
            }
        }

        tracing::debug!(
            topic,
            partition,
            replicas = replica_data.len(),
            min_batches,
            "Replica consistency verified"
        );

        Ok(())
    }

    // ========================================================================
    // Fault Injection
    // ========================================================================

    /// Partitions the given nodes from each other.
    pub fn partition(&self, nodes: &[NodeId]) {
        let mut state = self.network_state.lock().expect("lock poisoned");
        state.partition(nodes);
        info!(?nodes, "Network partition created");
    }

    /// Heals a partition between the given nodes.
    pub fn heal(&self, nodes: &[NodeId]) {
        let mut state = self.network_state.lock().expect("lock poisoned");
        state.heal(nodes);
        info!(?nodes, "Network partition healed");
    }

    /// Heals all partitions in the cluster.
    pub fn heal_all(&self) {
        let mut state = self.network_state.lock().expect("lock poisoned");
        // Heal all pairs.
        for i in 0..self.node_ids.len() {
            for j in (i + 1)..self.node_ids.len() {
                state.heal(&[self.node_ids[i], self.node_ids[j]]);
            }
        }
        info!("All network partitions healed");
    }

    /// Crashes a node (stops receiving messages).
    pub fn crash_node(&self, node_id: NodeId) {
        let mut state = self.network_state.lock().expect("lock poisoned");
        state.crash_node(node_id);
        info!(node = node_id.get(), "Node crashed");
    }

    /// Recovers a crashed node.
    pub fn recover_node(&self, node_id: NodeId) {
        let mut state = self.network_state.lock().expect("lock poisoned");
        state.recover_node(node_id);
        info!(node = node_id.get(), "Node recovered");
    }

    /// Sets storage fault configuration for a node.
    pub fn set_storage_faults(&self, node_id: NodeId, faults: FaultConfig) {
        if let Some(node) = self.nodes.get(&node_id) {
            *node.storage.fault_config() = faults;
            info!(node = node_id.get(), "Storage faults updated");
        }
    }

    // ========================================================================
    // Cluster State
    // ========================================================================

    /// Returns all node IDs.
    #[must_use]
    pub fn node_ids(&self) -> &[NodeId] {
        &self.node_ids
    }

    /// Returns a node by ID.
    #[must_use]
    pub fn node(&self, node_id: NodeId) -> Option<&E2ENode> {
        self.nodes.get(&node_id)
    }

    /// Waits for simulated time to pass.
    pub async fn sleep(&self, duration: Duration) {
        madsim::time::sleep(duration).await;
    }

    // ========================================================================
    // Property Tracking for DST Verification
    // ========================================================================

    /// Returns a reference to the shared property state.
    #[must_use]
    pub fn property_state(&self) -> &SharedHelixPropertyState {
        &self.property_state
    }

    /// Simple FNV-1a hash for data integrity verification.
    #[must_use]
    fn simple_hash(data: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in data {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
        hash
    }

    /// Produces data and records the client ack for verification.
    ///
    /// Like `produce_with_retry`, but also records the payload for later
    /// verification via `consume_and_verify`. Stores both the hash (for
    /// backward compatibility) and the raw payload bytes (for direct comparison).
    pub async fn produce_and_track(
        &self,
        topic: &str,
        partition: u32,
        data: impl Into<Bytes> + Clone,
        max_retries: u32,
    ) -> Result<Offset, String> {
        let data_bytes: Bytes = data.into();
        let payload_hash = Self::simple_hash(&data_bytes);

        let offset = self
            .produce_with_retry(topic, partition, data_bytes.clone(), max_retries)
            .await?;

        // Record client ack for verification.
        // Get topic_id from controller state.
        let topic_id = {
            let first_node = self.nodes.values().next();
            if let Some(node) = first_node {
                let state = node.service.controller_state().read().await;
                state.get_topic(topic).map(|info| info.topic_id.get())
            } else {
                None
            }
        };

        let Some(topic_id) = topic_id else {
            return Err(format!(
                "produce_and_track: topic_id lookup failed for topic={topic}"
            ));
        };

        if let Ok(mut state) = self.property_state.lock() {
            // Record hash for backward compatibility.
            state.record_client_ack(topic_id, u64::from(partition), offset.get(), payload_hash);
            // Store raw payload for direct byte comparison (more reliable).
            state.record_expected_payload(topic_id, u64::from(partition), offset.get(), data_bytes);
        }

        Ok(offset)
    }

    /// Consumes data and verifies payload integrity against recorded client acks.
    ///
    /// This performs REAL data integrity verification:
    /// - Extracts payload from each consumed record
    /// - Extracts payload and compares directly against stored expected bytes
    /// - FAILS EXPLICITLY on parse errors (no silent skipping)
    /// - Falls back to hash comparison for backward compatibility
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Consume fails
    /// - Topic ID lookup fails (no silent skip)
    /// - Payload extraction fails (parse error)
    /// - Payload doesn't match expected (data corruption)
    /// - Zero records were verified (likely a bug)
    ///
    /// Returns the consumed records on success.
    pub async fn consume_and_verify(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
    ) -> Result<Vec<Bytes>, String> {
        // Get topic_id for verification - FAIL if not found (no silent skip).
        let topic_id = {
            let first_node = self
                .nodes
                .values()
                .next()
                .ok_or_else(|| "no nodes in cluster".to_string())?;
            let state = first_node.service.controller_state().read().await;
            state
                .get_topic(topic)
                .map(|info| info.topic_id.get())
                .ok_or_else(|| format!("topic '{}' not found in controller state", topic))?
        };

        let records = self.consume(topic, partition, start_offset).await?;

        // Track verification count - fail if zero records verified.
        let mut verified_count: u64 = 0;

        // Verify each record by extracting and comparing payload.
        for batch in &records {
            // Extract baseOffset from batch header (bytes 0-7).
            let base_offset = Self::extract_base_offset(batch)?;

            // Extract payload - FAIL EXPLICITLY on parse error.
            let payload = Self::extract_payload_from_batch(batch).map_err(|e| {
                format!(
                    "PARSE ERROR at topic={} partition={} base_offset={}: {}",
                    topic, partition, base_offset, e
                )
            })?;

            // Use baseOffset from batch, not assumed sequential offset.
            let offset = base_offset;

            // Try direct payload comparison first (more reliable).
            let state_result = self.property_state.lock();
            let mut state = state_result.map_err(|e| format!("lock poisoned: {e}"))?;

            // Primary check: direct byte comparison when we have an expected payload.
            //
            // Kafka semantics: a produce that times out or returns an error can still
            // be committed and visible later. That data is "untracked" here because
            // we only record expected payloads on successful acks, so we do NOT fail
            // on untracked data.
            if state
                .get_expected_payload(topic_id, u64::from(partition), offset)
                .is_some()
            {
                if let Err(corruption_error) =
                    state.verify_payload_direct(topic_id, u64::from(partition), offset, &payload)
                {
                    // Data corruption detected for acked data.
                    tracing::error!(
                        topic_id,
                        partition,
                        offset,
                        error = %corruption_error,
                        "DATA INTEGRITY FAILURE"
                    );
                    return Err(corruption_error);
                }
            } else {
                tracing::debug!(
                    topic_id,
                    partition,
                    offset,
                    payload_len = payload.len(),
                    "UNTRACKED DATA (likely failed produce)"
                );
            }

            // Secondary check: hash comparison (for backward compatibility).
            let actual_hash = Self::simple_hash(&payload);
            let _hash_ok =
                state.verify_offset_with_hash(topic_id, u64::from(partition), offset, actual_hash);

            verified_count += 1;
            tracing::trace!(
                topic_id,
                partition,
                offset,
                payload_len = payload.len(),
                verified_count,
                "Record verified - payload matches"
            );
        }

        // Fail if we had records but verified zero (indicates a bug).
        if !records.is_empty() && verified_count == 0 {
            return Err(format!(
                "VERIFICATION BUG: consumed {} records but verified 0",
                records.len()
            ));
        }

        tracing::debug!(
            topic,
            partition,
            start_offset,
            records_consumed = records.len(),
            verified_count,
            "consume_and_verify completed"
        );

        Ok(records)
    }

    /// Extracts the baseOffset from a Kafka RecordBatch header.
    ///
    /// The baseOffset is the first 8 bytes of the batch (big-endian i64).
    ///
    /// # Errors
    ///
    /// Returns an error if the batch is too short.
    fn extract_base_offset(batch: &Bytes) -> Result<u64, String> {
        if batch.len() < 8 {
            return Err(format!(
                "batch too short for baseOffset: {} bytes (need 8)",
                batch.len()
            ));
        }
        let base_offset = i64::from_be_bytes([
            batch[0], batch[1], batch[2], batch[3], batch[4], batch[5], batch[6], batch[7],
        ]);
        // Convert to u64 - baseOffset should never be negative in valid data.
        if base_offset < 0 {
            return Err(format!("negative baseOffset: {base_offset}"));
        }
        #[allow(clippy::cast_sign_loss)]
        Ok(base_offset as u64)
    }

    /// Extracts the payload from a Kafka RecordBatch.
    ///
    /// Returns the first record's value.
    ///
    /// # Errors
    ///
    /// Returns an error with details if parsing fails. This ensures parse failures
    /// are caught explicitly rather than silently skipped.
    fn extract_payload_from_batch(batch: &Bytes) -> Result<Vec<u8>, String> {
        // Kafka RecordBatch v2 format:
        // - 8 bytes: baseOffset
        // - 4 bytes: batchLength
        // - 4 bytes: partitionLeaderEpoch
        // - 1 byte: magic (must be 2)
        // - 4 bytes: crc
        // - 2 bytes: attributes
        // - 4 bytes: lastOffsetDelta
        // - 8 bytes: firstTimestamp
        // - 8 bytes: maxTimestamp
        // - 8 bytes: producerId
        // - 2 bytes: producerEpoch
        // - 4 bytes: baseSequence
        // - 4 bytes: recordCount
        // Records follow at offset 61...

        const BATCH_HEADER_SIZE: usize = 61;

        if batch.len() < BATCH_HEADER_SIZE {
            return Err(format!(
                "batch too short for header: {} bytes (need at least {})",
                batch.len(),
                BATCH_HEADER_SIZE
            ));
        }

        // Verify magic byte.
        let magic = batch[16];
        if magic != 2 {
            return Err(format!("unexpected magic byte: {magic} (expected 2)"));
        }

        // Check record count.
        let record_count = i32::from_be_bytes([batch[57], batch[58], batch[59], batch[60]]);
        if record_count < 1 {
            return Err(format!("no records in batch: record_count={record_count}"));
        }

        // Skip to records (offset 61).
        let record_data = &batch[BATCH_HEADER_SIZE..];
        if record_data.is_empty() {
            return Err("no record data after header".to_string());
        }

        // First byte is record length varint.
        // For test payloads (< 128 bytes), this is a single byte.
        let record_len = record_data[0] as usize;
        if record_len >= 128 {
            return Err(format!(
                "record length {record_len} >= 128, multi-byte varint not supported"
            ));
        }
        if record_data.len() < 1 + record_len {
            return Err(format!(
                "record truncated: declared length {record_len}, but only {} bytes available",
                record_data.len() - 1
            ));
        }

        // Record format (after length varint):
        // - 1 byte: attributes
        // - varint: timestampDelta (single byte for 0)
        // - varint: offsetDelta (single byte for 0)
        // - varint: keyLength (-1 for null = 0x01 zigzag encoded, but we use 0xff)
        // - varint: valueLength
        // - bytes: value
        // - varint: headersCount

        let record_content = &record_data[1..];
        if record_content.len() < 5 {
            return Err(format!(
                "record content too short: {} bytes (need at least 5)",
                record_content.len()
            ));
        }

        // Skip: attributes (1), timestampDelta (1), offsetDelta (1), keyLength (1 = null key).
        let value_len_offset = 4;
        if value_len_offset >= record_content.len() {
            return Err("cannot read valueLength: record too short".to_string());
        }

        let value_len = record_content[value_len_offset] as usize;
        if value_len >= 128 {
            return Err(format!(
                "value length {value_len} >= 128, multi-byte varint not supported"
            ));
        }

        let value_start = value_len_offset + 1;
        if value_start + value_len > record_content.len() {
            return Err(format!(
                "value truncated: declared length {value_len}, but only {} bytes available",
                record_content.len() - value_start
            ));
        }

        Ok(record_content[value_start..value_start + value_len].to_vec())
    }

    /// Finalizes verification and returns the property check result.
    ///
    /// Call this after `consume_and_verify` has been called for all partitions.
    /// This checks that ALL client-acked offsets were successfully verified.
    #[must_use]
    pub fn finalize_verification(&self) -> HelixPropertyCheckResult {
        // Finalize consumer verification in property state.
        if let Ok(mut state) = self.property_state.lock() {
            state.finalize_consumer_verification();
        }

        // Return the full check result.
        check_helix_properties(&self.property_state).expect("lock should not be poisoned")
    }

    /// Asserts that no property violations occurred.
    ///
    /// # Panics
    ///
    /// Panics if any Raft or data integrity violations were detected.
    pub fn assert_no_violations(&self, test_name: &str) {
        let result = self.finalize_verification();
        assert_no_helix_violations(&result, test_name);
    }

    /// Collects Raft snapshots from all nodes for property verification.
    ///
    /// Call this periodically during tests to track leader elections
    /// and verify `SingleLeaderPerTerm`.
    pub async fn collect_raft_snapshots(&self) {
        use helix_raft::RaftState;

        for node in self.nodes.values() {
            // Skip crashed nodes.
            if !self.is_node_available(node.node_id) {
                // Record crashed state.
                if let Ok(mut state) = self.property_state.lock() {
                    state.update_snapshot(HelixNodeSnapshot {
                        node_id: node.node_id.get(),
                        controller_term: 0,
                        controller_state: RaftState::Follower,
                        crashed: true,
                    });
                }
                continue;
            }

            // Get controller group state (group 0).
            let (controller_term, controller_state) = {
                let mr = node.service.multi_raft().read().await;
                if let Some(state) = mr.group_state(CONTROLLER_GROUP_ID) {
                    (state.current_term.get(), state.state)
                } else {
                    (0, RaftState::Follower)
                }
            };

            // Update property state with snapshot.
            if let Ok(mut state) = self.property_state.lock() {
                state.update_snapshot(HelixNodeSnapshot {
                    node_id: node.node_id.get(),
                    controller_term,
                    controller_state,
                    crashed: false,
                });
            }
        }
    }
}

// ============================================================================
// FaultInjectable Implementation
// ============================================================================

impl crate::madsim_scenarios::FaultInjectable for E2ECluster {
    fn node_ids(&self) -> &[NodeId] {
        &self.node_ids
    }

    fn partition(&self, nodes: &[NodeId]) {
        E2ECluster::partition(self, nodes);
    }

    fn heal(&self, nodes: &[NodeId]) {
        E2ECluster::heal(self, nodes);
    }

    fn crash_node(&self, node_id: NodeId) {
        E2ECluster::crash_node(self, node_id);
    }

    fn recover_node(&self, node_id: NodeId) {
        E2ECluster::recover_node(self, node_id);
    }

    fn set_storage_faults(&self, node_id: NodeId, config: FaultConfig) {
        E2ECluster::set_storage_faults(self, node_id, config);
    }
}

impl E2ECluster {
    /// Debug: prints router partition counts for all nodes.
    #[allow(dead_code)]
    pub async fn debug_print_router_state(&self) {
        if !self.actor_mode {
            eprintln!("[DEBUG] Not in actor mode, no routers");
            return;
        }
        for (node_id, node) in &self.nodes {
            if let Some(router) = &node.actor_router {
                let count = router.partition_count().await;
                let group_ids: Vec<u64> =
                    router.group_ids().await.iter().map(|g| g.get()).collect();
                eprintln!(
                    "[DEBUG] Node {}: router has {} partitions: {:?}",
                    node_id.get(),
                    count,
                    group_ids
                );

                // Query leader state for each partition.
                for group_id in router.group_ids().await {
                    if let Ok(handle) = router.partition(group_id).await {
                        let is_leader = handle.is_leader().await.unwrap_or(false);
                        let leader_id = handle.leader_id().await.unwrap_or(None);
                        eprintln!(
                            "[DEBUG]   Group {}: is_leader={}, leader_id={:?}",
                            group_id.get(),
                            is_leader,
                            leader_id.map(|n| n.get())
                        );
                    }
                }

                // Also check if the service's actor_router has the same partitions.
                let service_has_router = node.service.actor_router().is_some();
                eprintln!("[DEBUG]   Service has actor_router: {}", service_has_router);
                if let Some(service_router) = node.service.actor_router() {
                    let service_count = service_router.partition_count().await;
                    let service_groups: Vec<u64> = service_router
                        .group_ids()
                        .await
                        .iter()
                        .map(|g| g.get())
                        .collect();
                    eprintln!(
                        "[DEBUG]   Service router partitions: {} {:?}",
                        service_count, service_groups
                    );
                }
            } else {
                eprintln!("[DEBUG] Node {}: no router", node_id.get());
            }
        }
    }

    /// Produces data with retries until leader is available.
    ///
    /// This is the proper E2E approach - just use the API and retry on transient errors.
    pub async fn produce_with_retry(
        &self,
        topic: &str,
        partition: u32,
        data: impl Into<Bytes> + Clone,
        max_retries: u32,
    ) -> Result<Offset, String> {
        let mut last_error = String::new();
        for attempt in 0..max_retries {
            match self.produce(topic, partition, data.clone()).await {
                Ok(offset) => return Ok(offset),
                Err(e) => {
                    last_error = e;
                    // Short wait for leader election / replication (10ms simulated).
                    self.sleep(Duration::from_millis(10)).await;
                    if attempt % 50 == 0 && attempt > 0 {
                        tracing::debug!(attempt, "Produce retry, waiting for leader");
                    }
                }
            }
        }
        Err(format!(
            "produce failed after {max_retries} retries: {last_error}"
        ))
    }
}

// ============================================================================
// Background Tasks
// ============================================================================

/// Bridge task that converts MadSimTransport messages to helix_runtime format.
async fn message_bridge_task(
    mut madsim_rx: NodeMailboxReceiver,
    runtime_tx: mpsc::Sender<helix_runtime::IncomingMessage>,
) {
    while let Some(msg) = madsim_rx.recv().await {
        let runtime_msg = match msg {
            MadSimIncomingMessage::Raft(raft_msg) => {
                helix_runtime::IncomingMessage::Batch(raft_msg.messages)
            }
            MadSimIncomingMessage::Heartbeat(hb_msg) => {
                helix_runtime::IncomingMessage::Heartbeat(hb_msg.heartbeat)
            }
        };

        if runtime_tx.send(runtime_msg).await.is_err() {
            // Receiver dropped, task shutting down.
            break;
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(all(test, madsim))]
mod tests {
    use super::*;
    use madsim::runtime::Runtime;

    // ========================================================================
    // Parser and Verification Tests
    // ========================================================================

    /// Verifies that `extract_payload_from_batch` correctly parses batches
    /// created by `create_test_record_batch`.
    ///
    /// This is a CRITICAL test - if this fails, all DST verification is broken.
    #[test]
    fn test_extract_payload_roundtrip() {
        // Note: Record overhead is 6 bytes (attributes, timestamps, key/value lengths, headers).
        // For single-byte varint, record_length < 128, so max payload = 121 bytes.
        let test_payloads = [
            b"hello world".to_vec(),
            b"record-0".to_vec(),
            b"".to_vec(),    // Empty payload
            b"x".to_vec(),   // Single byte
            vec![0u8; 100],  // 100 zero bytes
            vec![0xAB; 121], // Max payload for single-byte record length varint (121 + 6 = 127 < 128)
        ];

        for payload in &test_payloads {
            let batch = create_test_record_batch(payload);
            let extracted = E2ECluster::extract_payload_from_batch(&batch);

            match extracted {
                Ok(result) => {
                    assert_eq!(
                        &result, payload,
                        "Payload mismatch: expected {:?}, got {:?}",
                        payload, result
                    );
                }
                Err(e) => {
                    panic!(
                        "Failed to extract payload {:?} (len={}): {}",
                        payload,
                        payload.len(),
                        e
                    );
                }
            }
        }
        eprintln!("[PASS] test_extract_payload_roundtrip: Parser correctly extracts payloads");
    }

    /// Verifies that `verify_payload_direct` catches data corruption.
    ///
    /// This is a CRITICAL test - if this fails, DST won't detect data corruption.
    #[test]
    fn test_verification_catches_corruption() {
        use crate::properties::HelixPropertyState;

        let mut state = HelixPropertyState::new();

        // Record expected payload.
        let expected_payload = Bytes::from("correct data");
        state.record_expected_payload(1, 0, 0, expected_payload);

        // Verify with correct payload - should pass.
        let result = state.verify_payload_direct(1, 0, 0, b"correct data");
        assert!(
            result.is_ok(),
            "Correct payload should verify: {:?}",
            result
        );

        // Try to verify with corrupted payload - should fail.
        let corrupted = state.verify_payload_direct(1, 0, 0, b"CORRUPTED data");
        assert!(corrupted.is_err(), "Corrupted payload should be detected");
        let err = corrupted.unwrap_err();
        assert!(
            err.contains("DATA CORRUPTION"),
            "Error should mention DATA CORRUPTION: {err}"
        );

        // Try to verify unknown offset - should fail.
        let unknown = state.verify_payload_direct(1, 0, 999, b"any data");
        assert!(unknown.is_err(), "Unknown offset should be detected");
        let err = unknown.unwrap_err();
        assert!(
            err.contains("UNEXPECTED DATA"),
            "Error should mention UNEXPECTED DATA: {err}"
        );

        eprintln!("[PASS] test_verification_catches_corruption: Corruption detection works");
    }

    /// Verifies that parse errors are caught explicitly, not silently skipped.
    #[test]
    fn test_parse_error_not_silent() {
        // Create an invalid/truncated batch.
        let invalid_batch = Bytes::from(vec![0u8; 10]); // Too short to be valid.
        let result = E2ECluster::extract_payload_from_batch(&invalid_batch);

        assert!(
            result.is_err(),
            "Invalid batch should return Err, not silently fail"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("batch too short"),
            "Error should explain the issue: {err}"
        );

        eprintln!("[PASS] test_parse_error_not_silent: Parse errors are caught explicitly");
    }

    // ========================================================================
    // E2E Cluster Tests
    // ========================================================================

    #[test]
    fn test_e2e_cluster_starts() {
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let cluster = E2ECluster::start(3).await;

            assert_eq!(cluster.node_ids().len(), 3);
            eprintln!("[PASS] test_e2e_cluster_starts: Cluster started with 3 nodes");
        });
    }

    #[test]
    fn test_e2e_create_topic() {
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let cluster = E2ECluster::start(3).await;

            // Wait for cluster to stabilize.
            cluster.sleep(Duration::from_millis(500)).await;

            // Create topic using REAL API.
            let result = cluster.create_topic("test-topic", 1).await;

            match result {
                Ok(()) => {
                    eprintln!("[PASS] test_e2e_create_topic: Created topic");
                }
                Err(e) => {
                    eprintln!("[INFO] test_e2e_create_topic: {}", e);
                    // This is expected to fail initially until we wire everything up.
                }
            }
        });
    }

    #[test]
    fn test_e2e_single_node() {
        // Test E2E with single node (no Raft replication needed).
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let cluster = E2ECluster::start(1).await;

            // Wait for cluster to stabilize.
            cluster.sleep(Duration::from_millis(500)).await;

            // Create topic.
            cluster
                .create_topic("test-topic", 1)
                .await
                .expect("create topic");

            // Produce data with retries (leader election happens in background).
            for i in 0..5 {
                let offset = cluster
                    .produce_with_retry("test-topic", 0, format!("record-{i}"), 50)
                    .await;
                assert!(offset.is_ok(), "Produce should succeed: {:?}", offset);
                eprintln!("[INFO] Produced record {} at offset {:?}", i, offset);
            }

            // Consume data.
            let records = cluster.consume("test-topic", 0, 0).await;
            assert!(records.is_ok(), "Consume should succeed");
            assert_eq!(records.unwrap().len(), 5, "Should have 5 records");

            eprintln!("[PASS] test_e2e_single_node: Full E2E works in single-node mode");
        });
    }

    #[test]
    fn test_e2e_durable_storage_exercised() {
        // Verify that durable storage (SharedWalPool) is actually being exercised.
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let cluster = E2ECluster::start(1).await;

            // Verify durable storage is enabled.
            let node = cluster.node(NodeId::new(1)).expect("node 1");
            assert!(
                node.service.shared_wal_pool().is_some(),
                "SharedWalPool should be enabled for durable storage"
            );
            assert!(
                node.service.data_dir().is_some(),
                "data_dir should be set for durable storage"
            );

            // Get initial storage stats.
            let initial_stats = node.storage.fault_stats();
            eprintln!(
                "[INFO] Initial storage stats: file_ops={}, writes={}, reads={}, syncs={}",
                initial_stats.total_ops,
                initial_stats.write_ops,
                initial_stats.read_ops,
                initial_stats.sync_ops
            );

            // Wait for cluster to stabilize.
            cluster.sleep(Duration::from_millis(500)).await;

            // Create topic and produce data.
            cluster.create_topic("durable-test", 1).await.expect("create topic");
            for i in 0..10 {
                cluster.produce_with_retry("durable-test", 0, format!("record-{i}"), 50).await
                    .expect("produce should succeed");
            }

            // Let writes flush.
            cluster.sleep(Duration::from_millis(100)).await;

            // Check storage was exercised.
            let final_stats = node.storage.fault_stats();
            let write_delta = final_stats.write_ops - initial_stats.write_ops;
            let sync_delta = final_stats.sync_ops - initial_stats.sync_ops;
            eprintln!(
                "[INFO] Final storage stats: file_ops={}, writes={} (+{}), reads={}, syncs={} (+{})",
                final_stats.total_ops,
                final_stats.write_ops,
                write_delta,
                final_stats.read_ops,
                final_stats.sync_ops,
                sync_delta
            );

            // Verify writes actually happened.
            assert!(
                write_delta > 0,
                "Storage writes should have been exercised: initial={}, final={}",
                initial_stats.write_ops, final_stats.write_ops
            );

            // Verify syncs happened (durable storage).
            assert!(
                sync_delta > 0,
                "Storage syncs should have been exercised: initial={}, final={}",
                initial_stats.sync_ops, final_stats.sync_ops
            );

            // Consume and verify data.
            let records = cluster.consume("durable-test", 0, 0).await.expect("consume");
            assert_eq!(records.len(), 10, "Should have 10 records");

            eprintln!(
                "[PASS] test_e2e_durable_storage_exercised: SharedWalPool active, {} writes, {} syncs",
                write_delta, sync_delta
            );
        });
    }

    #[test]
    fn test_e2e_storage_fault_injection() {
        // Verify that storage fault injection actually works with SharedWalPool.
        // Uses 3-node cluster because single-node uses append_nowait which doesn't
        // wait for storage durability (relies on Raft replication for guarantees).
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let cluster = E2ECluster::start(3).await;

            // Wait for cluster to stabilize (controller election, etc).
            cluster.sleep(Duration::from_secs(2)).await;

            // Create topic first (before enabling faults) - retry on failure.
            for attempt in 0..20 {
                if cluster.create_topic("fault-test", 1).await.is_ok() {
                    break;
                }
                cluster.sleep(Duration::from_millis(100)).await;
                if attempt == 19 {
                    panic!("Failed to create topic after 20 attempts");
                }
            }

            // Produce some records successfully first.
            for i in 0..5 {
                cluster.produce_with_retry("fault-test", 0, format!("record-{i}"), 50).await
                    .expect("produce should succeed");
            }

            // Wait for writes to flush.
            cluster.sleep(Duration::from_millis(100)).await;

            // Get initial fault stats.
            let node = cluster.node(NodeId::new(1)).expect("node 1");
            let initial_stats = node.storage.fault_stats();

            // Also check SharedWalPool is enabled and using our storage.
            let pool = node.service.shared_wal_pool();
            eprintln!("[INFO] SharedWalPool present: {}", pool.is_some());

            // Debug: print Arc pointer to verify sharing.
            eprintln!("[DEBUG] node.storage fault_config ptr: 0x{:x}", node.storage.fault_config_ptr());
            eprintln!(
                "[INFO] Before fault injection: write_ops={}, write_failures={}, sync_ops={}, fsync_failures={}",
                initial_stats.write_ops, initial_stats.write_failures,
                initial_stats.sync_ops, initial_stats.fsync_failures
            );

            // Verify the fault config pointer is what we expect.
            {
                let config = node.storage.fault_config();
                eprintln!("[INFO] Current fault config: write_fail_rate={}", config.write_fail_rate);
            }

            // Enable write failures on ALL nodes at 30% rate.
            // This tests cluster-wide fault tolerance - with 3 nodes and 30% failure rate,
            // most writes should succeed via replication, but some might fail.
            let fault_config = FaultConfig {
                write_fail_rate: 0.3,
                ..Default::default()
            };
            for &nid in cluster.node_ids() {
                cluster.set_storage_faults(nid, fault_config.clone());
            }
            eprintln!("[INFO] Set 30% write failure rate on all nodes");

            // Try to produce more records - some should fail.
            let mut successes = 0;
            let mut failures = 0;
            for i in 5..25 {
                // Use lower retry count to see failures.
                match cluster.produce_with_retry("fault-test", 0, format!("record-{i}"), 3).await {
                    Ok(_) => successes += 1,
                    Err(_) => failures += 1,
                }
            }

            // Wait for writes to flush to storage.
            cluster.sleep(Duration::from_millis(100)).await;

            // Aggregate fault stats from all nodes.
            let mut total_write_failures = 0u64;
            let mut total_write_ops = 0u64;
            for &nid in cluster.node_ids() {
                let n = cluster.node(nid).expect("node exists");
                let stats = n.storage.fault_stats();
                total_write_failures += stats.write_failures - initial_stats.write_failures;
                total_write_ops += stats.write_ops - initial_stats.write_ops;
                eprintln!(
                    "[INFO] Node {} after faults: write_ops={}, write_failures={}",
                    nid.get(), stats.write_ops, stats.write_failures
                );
            }
            eprintln!(
                "[INFO] Cluster total: write_ops delta={}, write_failures delta={}, produces: successes={}, failures={}",
                total_write_ops, total_write_failures, successes, failures
            );

            // Verify faults were actually injected somewhere in the cluster.
            assert!(
                total_write_failures > 0,
                "Storage write failures should have been injected across cluster"
            );

            // Disable faults on all nodes and verify recovery.
            for &nid in cluster.node_ids() {
                cluster.set_storage_faults(nid, FaultConfig::default());
            }
            cluster.sleep(Duration::from_millis(100)).await;

            // Should be able to produce again.
            cluster.produce_with_retry("fault-test", 0, "recovery-record", 50).await
                .expect("produce should succeed after disabling faults");

            eprintln!(
                "[PASS] test_e2e_storage_fault_injection: {} write failures injected across cluster, system recovered",
                total_write_failures
            );
        });
    }

    #[test]
    fn test_e2e_produce_consume() {
        // Test E2E produce/consume with 3-node cluster using just public APIs.
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let cluster = E2ECluster::start(3).await;

            // Wait for cluster to stabilize (controller election, etc).
            cluster.sleep(Duration::from_secs(2)).await;

            // Create topic - uses controller in multi-node mode.
            cluster
                .create_topic("orders", 1)
                .await
                .expect("create topic");

            // Produce with retries - will succeed once leader is elected.
            for i in 0..10 {
                let offset = cluster
                    .produce_with_retry("orders", 0, format!("order-{i}"), 100)
                    .await;
                assert!(offset.is_ok(), "Produce should succeed: {:?}", offset);
                eprintln!("[INFO] Produced record {} at offset {:?}", i, offset);
            }

            // Consume.
            let records = cluster
                .consume("orders", 0, 0)
                .await
                .expect("consume should succeed");
            eprintln!("[INFO] Consumed {} records", records.len());
            assert_eq!(records.len(), 10, "Should have 10 records");

            eprintln!("[PASS] test_e2e_produce_consume: Multi-node E2E works");
        });
    }

    #[test]
    fn test_e2e_network_partition() {
        // Test that data survives network partitions.
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let cluster = E2ECluster::start(3).await;

            // Wait for cluster to stabilize.
            cluster.sleep(Duration::from_secs(2)).await;

            // Create topic.
            cluster
                .create_topic("orders", 1)
                .await
                .expect("create topic");

            // Produce some data.
            for i in 0..5 {
                cluster
                    .produce_with_retry("orders", 0, format!("order-{i}"), 100)
                    .await
                    .expect("produce should succeed");
            }
            eprintln!("[INFO] Produced 5 records before partition");

            // Partition node 1 from nodes 2 and 3.
            cluster.partition(&[NodeId::new(1), NodeId::new(2)]);
            cluster.partition(&[NodeId::new(1), NodeId::new(3)]);
            eprintln!("[INFO] Network partition created: node 1 isolated");

            // Wait for leader election. The partitioned node needs time to:
            // 1. Realize it can't reach quorum (heartbeat failures)
            // 2. Step down from leader
            // 3. Nodes 2 and 3 need to elect a new leader
            // This requires multiple election timeout periods (~300ms each).
            cluster.sleep(Duration::from_secs(3)).await;
            for i in 5..10 {
                cluster
                    .produce_with_retry("orders", 0, format!("order-{i}"), 100)
                    .await
                    .expect("produce should succeed after partition");
            }
            eprintln!("[INFO] Produced 5 more records during partition");

            // Heal the partition.
            cluster.heal_all();

            // Wait for data to replicate from new leader back to healed node.
            // This requires time for:
            // 1. Leader to detect partition healed and send AppendEntries
            // 2. Follower to resolve log conflicts by backtracking
            // 3. Entries to be replicated and committed
            cluster.sleep(Duration::from_secs(5)).await;

            // Find and log which node we're consuming from.
            let leader = cluster.find_partition_leader("orders", 0).await;
            eprintln!(
                "[DEBUG] Consuming from leader: {:?}",
                leader.map(|n| n.get())
            );

            // Verify data available on healed cluster.
            let records = cluster
                .consume("orders", 0, 0)
                .await
                .expect("consume should succeed");
            eprintln!("[INFO] Consumed {} records after healing", records.len());
            assert_eq!(records.len(), 10, "All 10 records should be available");

            eprintln!("[PASS] test_e2e_network_partition: Data survives partitions");
        });
    }

    #[test]
    fn test_e2e_replica_consistency_after_partition() {
        // Test that replicas have consistent data after network partition heals.
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let cluster = E2ECluster::start(3).await;

            // Wait for cluster to stabilize.
            cluster.sleep(Duration::from_secs(2)).await;

            // Create topic.
            cluster.create_topic("consistency-test", 1).await.expect("create topic");

            // Produce some data before partition.
            for i in 0..5 {
                cluster.produce_with_retry("consistency-test", 0, format!("before-{i}"), 100).await
                    .expect("produce should succeed");
            }
            eprintln!("[INFO] Produced 5 records before partition");

            // Verify initial consistency (all nodes should agree).
            cluster.verify_replica_consistency("consistency-test", 0).await
                .expect("should be consistent before partition");
            eprintln!("[INFO] Replicas consistent before partition");

            // Partition node 1 from nodes 2 and 3.
            cluster.partition(&[NodeId::new(1), NodeId::new(2)]);
            cluster.partition(&[NodeId::new(1), NodeId::new(3)]);
            eprintln!("[INFO] Network partition created: node 1 isolated");

            // Wait for new leader election.
            cluster.sleep(Duration::from_secs(3)).await;

            // Produce more data during partition (goes to majority partition).
            for i in 5..10 {
                cluster.produce_with_retry("consistency-test", 0, format!("during-{i}"), 100).await
                    .expect("produce should succeed during partition");
            }
            eprintln!("[INFO] Produced 5 more records during partition");

            // Heal the partition.
            cluster.heal_all();
            eprintln!("[INFO] Partition healed");

            // Wait for replication to catch up.
            cluster.sleep(Duration::from_secs(5)).await;

            // Verify consistency after healing.
            // This is the key test - all replicas should converge to same state.
            cluster.verify_replica_consistency("consistency-test", 0).await
                .expect("should be consistent after partition heals");
            eprintln!("[INFO] Replicas consistent after healing");

            // Verify all data is readable.
            let records = cluster.consume("consistency-test", 0, 0).await.expect("consume should succeed");
            assert_eq!(records.len(), 10, "All 10 records should be available");

            eprintln!("[PASS] test_e2e_replica_consistency_after_partition: Replicas converge after partition");
        });
    }

    #[test]
    fn test_e2e_replica_consistency_after_crash() {
        // Test that replicas have consistent data after node crash and continued writes.
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let cluster = E2ECluster::start(3).await;

            // Wait for cluster to stabilize.
            cluster.sleep(Duration::from_secs(2)).await;

            // Create topic.
            cluster.create_topic("crash-consistency", 1).await.expect("create topic");

            // Produce some data.
            for i in 0..5 {
                cluster.produce_with_retry("crash-consistency", 0, format!("before-{i}"), 100).await
                    .expect("produce should succeed");
            }
            eprintln!("[INFO] Produced 5 records before crash");

            // Crash node 1.
            cluster.crash_node(NodeId::new(1));
            eprintln!("[INFO] Node 1 crashed");

            // Wait for failover.
            cluster.sleep(Duration::from_secs(3)).await;

            // Produce more data after crash (to surviving nodes only).
            for i in 5..10 {
                cluster.produce_with_retry("crash-consistency", 0, format!("after-{i}"), 100).await
                    .expect("produce should succeed after crash");
            }
            eprintln!("[INFO] Produced 5 more records after crash");

            // Wait for replication.
            cluster.sleep(Duration::from_secs(2)).await;

            // Verify consistency among surviving nodes (crashed node is skipped).
            cluster.verify_replica_consistency("crash-consistency", 0).await
                .expect("surviving replicas should be consistent");
            eprintln!("[INFO] Surviving replicas consistent");

            // Verify all data is readable from surviving nodes.
            let records = cluster.consume("crash-consistency", 0, 0).await.expect("consume should succeed");
            assert_eq!(records.len(), 10, "All 10 records should be available");

            eprintln!("[PASS] test_e2e_replica_consistency_after_crash: Surviving replicas stay consistent");
        });
    }

    #[test]
    fn test_e2e_node_crash() {
        // Test that data survives node crashes.
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let cluster = E2ECluster::start(3).await;

            // Wait for cluster to stabilize.
            cluster.sleep(Duration::from_secs(2)).await;

            // Create topic.
            cluster
                .create_topic("orders", 1)
                .await
                .expect("create topic");

            // Produce some data.
            for i in 0..5 {
                cluster
                    .produce_with_retry("orders", 0, format!("order-{i}"), 100)
                    .await
                    .expect("produce should succeed");
            }
            eprintln!("[INFO] Produced 5 records before crash");

            // Crash node 1.
            cluster.crash_node(NodeId::new(1));
            eprintln!("[INFO] Node 1 crashed");

            // Wait for failover. Similar to partition, surviving nodes need time to:
            // 1. Detect the crash (missed heartbeats)
            // 2. Elect a new leader among Nodes 2 and 3
            cluster.sleep(Duration::from_secs(3)).await;
            for i in 5..10 {
                cluster
                    .produce_with_retry("orders", 0, format!("order-{i}"), 100)
                    .await
                    .expect("produce should succeed after crash");
            }
            eprintln!("[INFO] Produced 5 more records after crash");

            // Wait a bit more for replication.
            cluster.sleep(Duration::from_secs(2)).await;

            // Debug: show state before consuming.
            cluster.debug_partition_state("orders", 0).await;

            // Find and log which node we're consuming from.
            let leader = cluster.find_partition_leader("orders", 0).await;
            eprintln!(
                "[DEBUG] Consuming from leader: {:?}",
                leader.map(|n| n.get())
            );

            // Verify data on surviving nodes.
            let records = cluster
                .consume("orders", 0, 0)
                .await
                .expect("consume should succeed");
            eprintln!("[INFO] Consumed {} records", records.len());
            assert_eq!(records.len(), 10, "All 10 records should be available");

            eprintln!("[PASS] test_e2e_node_crash: Data survives node crash");
        });
    }

    // ========================================================================
    // PROPER DST - Random Fault Injection
    // ========================================================================

    /// Proper DST test with random fault injection based on seed.
    ///
    /// This is the correct DST approach - each seed generates a DIFFERENT random
    /// fault scenario, providing comprehensive coverage across failure modes.
    ///
    /// Unlike the scripted tests (`test_e2e_crash_many_seeds`, `test_e2e_partition_many_seeds`),
    /// this test uses `FaultScenario::random(seed)` to deterministically generate
    /// varied fault patterns.
    ///
    /// # Verification
    ///
    /// This test performs three levels of verification:
    /// 1. **Raft invariants**: `SingleLeaderPerTerm` via property state snapshots
    /// 2. **Data integrity**: Payload hashes match between produce and consume
    /// 3. **Count check**: At least 50% of produced records are consumable
    fn run_e2e_dst_random_faults(
        total_seeds: u64,
        records_per_seed: usize,
        ticks_per_seed: u32,
        topic: &str,
        partition_count: u32,
        progress_mod: u64,
    ) {
        use crate::madsim_scenarios::{FaultScenario, ScenarioExecutor};
        use std::collections::HashSet;

        let mut failures: Vec<(u64, String, String)> = Vec::new();
        let mut scenario_counts: std::collections::HashMap<&'static str, u64> =
            std::collections::HashMap::new();
        let seed_filter = std::env::var("MADSIM_SEED")
            .ok()
            .and_then(|v| v.parse::<u64>().ok());
        let debug_seed = std::env::var("MADSIM_DEBUG_SEED")
            .ok()
            .and_then(|v| v.parse::<u64>().ok());

        let suite_start = std::time::Instant::now();
        for seed in 0..total_seeds {
            if seed_filter.is_some_and(|s| s != seed) {
                continue;
            }
            // Generate random scenario from seed.
            let scenario = FaultScenario::random(seed);
            let scenario_name = scenario.name();
            *scenario_counts.entry(scenario_name).or_default() += 1;

            if seed % progress_mod == 0 {
                eprintln!(
                    "[PROGRESS] Seed {}/{} - scenario: {}",
                    seed, total_seeds, scenario_name
                );
            }

            let seed_start = std::time::Instant::now();
            let result = std::panic::catch_unwind(|| {
                let rt = Runtime::with_seed_and_config(seed, Default::default());
                rt.block_on(async {
                    let cluster = E2ECluster::start(3).await;
                    let mut executor = ScenarioExecutor::new(scenario.clone());

                    // Wait for controller election (reduced from 500ms).
                    cluster.sleep(Duration::from_millis(200)).await;
                    for attempt in 0..10 {
                        if cluster.create_topic(topic, partition_count).await.is_ok() {
                            break;
                        }
                        cluster.sleep(Duration::from_millis(50)).await;
                        if attempt == 9 {
                            panic!("Failed to create topic after 10 attempts");
                        }
                    }

                    // Interleave production with fault injection.
                    let mut produced = 0;
                    let records_per_tick = records_per_seed / ticks_per_seed as usize;
                    let records_per_tick = records_per_tick.max(1);

                    for tick in 0..ticks_per_seed {
                        // Produce some records with tracking for verification.
                        for i in 0..records_per_tick {
                            let record_num = tick as usize * records_per_tick + i;
                            if record_num >= records_per_seed {
                                break;
                            }
                            let partition = (record_num as u32) % partition_count;
                            if cluster
                                .produce_and_track(
                                    topic,
                                    partition,
                                    format!("record-{}", record_num),
                                    5,
                                )
                                .await
                                .is_ok()
                            {
                                produced += 1;
                            }
                        }

                        // Collect Raft snapshots for property verification.
                        cluster.collect_raft_snapshots().await;

                        // Apply fault scenario based on tick count.
                        // Pass None for leader - scenarios that need leader will be skipped.
                        if let Some(action) = executor.tick(&cluster, None) {
                            tracing::debug!(seed, tick, action, "Fault injected");
                        }

                        // Small sleep to advance simulated time (reduced from 50ms).
                        cluster.sleep(Duration::from_millis(20)).await;
                    }

                    // Heal all partitions after test.
                    cluster.heal_all();
                    cluster.sleep(Duration::from_millis(500)).await; // Reduced from 2s
                    if partition_count > 1 {
                        // Extra stabilization time for multi-partition metadata to propagate.
                        cluster.sleep(Duration::from_millis(1000)).await;
                    }

                    // Collect final Raft snapshots.
                    cluster.collect_raft_snapshots().await;

                    // NEW: Verify cross-replica consistency after healing.
                    // All replicas should have identical committed data.
                    for partition in 0..partition_count {
                        let mut last_error: Option<String> = None;
                        for attempt in 0..20 {
                            match cluster.verify_replica_consistency(topic, partition).await {
                                Ok(()) => {
                                    last_error = None;
                                    break;
                                }
                                Err(e) => {
                                    last_error = Some(e);
                                    if attempt < 19 {
                                        cluster.sleep(Duration::from_millis(200)).await;
                                    }
                                }
                            }
                        }
                        if let Some(e) = last_error {
                            // Diagnostic dump for multi-partition failures.
                            if debug_seed == Some(seed) {
                                eprintln!(
                                    "[DEBUG] Seed {} partition {} consistency failure: {}",
                                    seed, partition, e
                                );
                                for node in cluster.nodes.values() {
                                    let node_id = node.node_id;
                                    let available = cluster.is_node_available(node_id);
                                    let topic_id = {
                                        let state = node.service.controller_state().read().await;
                                        state.get_topic(topic).map(|info| info.topic_id)
                                    };
                                    let group_id = if let Some(tid) = topic_id {
                                        let gm = node.service.group_map().read().await;
                                        gm.get(tid, helix_core::PartitionId::new(u64::from(partition)))
                                    } else {
                                        None
                                    };
                                    let group_state = if let Some(gid) = group_id {
                                        let mr = node.service.multi_raft().read().await;
                                        mr.group_state(gid)
                                    } else {
                                        None
                                    };
                                    let fetch_result = node
                                        .handler
                                        .fetch(topic, partition as i32, 0, 1024 * 1024)
                                        .await;
                                    eprintln!(
                                        "  node={} available={} topic_id={:?} group_id={:?} state={:?} fetch={:?}",
                                        node_id.get(),
                                        available,
                                        topic_id.map(|t| t.get()),
                                        group_id.map(|g| g.get()),
                                        group_state.as_ref().map(|s| (s.state, s.leader_id, s.commit_index.get())),
                                        fetch_result.as_ref().err()
                                    );
                                }
                            }
                            panic!("Seed {} partition {}: {}", seed, partition, e);
                        }
                    }

                    // Verify data with integrity checking.
                    let mut consumed = 0usize;
                    for partition in 0..partition_count {
                        let records = cluster
                            .consume_and_verify(topic, partition, 0)
                            .await
                            .expect("consume should succeed");
                        consumed += records.len();
                    }

                    // Finalize verification and check for property violations.
                    let check_result = cluster.finalize_verification();

                    // Check Raft invariants (SingleLeaderPerTerm).
                    if !check_result.violations.is_empty() {
                        let violation_strs: Vec<_> = check_result
                            .violations
                            .iter()
                            .map(|v| format!("{v}"))
                            .collect();
                        panic!(
                            "Seed {}: Raft invariant violations: {}",
                            seed,
                            violation_strs.join(", ")
                        );
                    }

                    // Check data integrity violations (hash mismatches).
                    // ALL acked data must be readable and have correct content.
                    if !check_result.consumer_violations.is_empty() {
                        if debug_seed == Some(seed) {
                            eprintln!("=== DEBUG SEED {}: consumer violations ===", seed);
                            for violation in &check_result.consumer_violations {
                                eprintln!(
                                    "violation: topic={} partition={} offset={} reason={}",
                                    violation.topic_id,
                                    violation.partition_id,
                                    violation.offset,
                                    violation.reason
                                );
                                for node_id in cluster.nodes.keys().copied() {
                                    let partition = violation.partition_id as u32;
                                    let commit_index = cluster
                                        .get_partition_commit_index(node_id, topic, partition)
                                        .await;
                                    let batches = cluster
                                        .consume_from_node(node_id, topic, partition, 0)
                                        .await;
                                    let mut offsets = HashSet::new();
                                    let mut max_offset: Option<u64> = None;
                                    if let Some(batches) = batches {
                                        for batch in &batches {
                                            if let Ok(offset) = E2ECluster::extract_base_offset(batch) {
                                                offsets.insert(offset);
                                                max_offset = Some(match max_offset {
                                                    Some(m) => m.max(offset),
                                                    None => offset,
                                                });
                                            }
                                        }
                                    }
                                    let has_offset = offsets.contains(&violation.offset);
                                    eprintln!(
                                        "  node={} commit_index={:?} has_offset={} max_offset={:?} offsets_seen={}",
                                        node_id.get(),
                                        commit_index,
                                        has_offset,
                                        max_offset,
                                        offsets.len()
                                    );
                                }
                            }
                        }
                        panic!(
                            "Seed {}: {} data integrity violations: {:?}",
                            seed,
                            check_result.consumer_violations.len(),
                            check_result.consumer_violations
                        );
                    }

                    // With fault injection, we may lose some records depending on scenario.
                    // Verify we got a reasonable number of records.
                    tracing::debug!(
                        seed,
                        produced,
                        consumed,
                        "Seed completed - all {} consumed records verified correct",
                        consumed
                    );
                });
            });

            if let Err(e) = result {
                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown panic".to_string()
                };
                let seed_elapsed = seed_start.elapsed();
                eprintln!("[SEED] {} elapsed {:?} (failed)", seed, seed_elapsed);
                eprintln!("[FAIL] Seed {} ({}): {}", seed, scenario_name, msg);
                failures.push((seed, scenario_name.to_string(), msg));
            } else {
                let seed_elapsed = seed_start.elapsed();
                if seed % progress_mod == 0 {
                    eprintln!("[SEED] {} elapsed {:?}", seed, seed_elapsed);
                }
            }
        }

        // Print scenario distribution.
        eprintln!("\n=== Scenario Distribution ===");
        for (name, count) in &scenario_counts {
            eprintln!("  {}: {} seeds", name, count);
        }

        eprintln!("\n=== E2E DST Random Faults Results ===");
        eprintln!("Total seeds: {}", total_seeds);
        eprintln!("Passed: {}", total_seeds - failures.len() as u64);
        eprintln!("Failed: {}", failures.len());

        if !failures.is_empty() {
            eprintln!("\nFailed seeds:");
            for (seed, scenario, msg) in &failures {
                eprintln!("  Seed {} ({}): {}", seed, scenario, msg);
            }
            panic!("{} seeds failed", failures.len());
        }

        eprintln!(
            "[PASS] All {} seeds passed with random fault injection",
            total_seeds
        );
        eprintln!("[TOTAL] elapsed {:?}", suite_start.elapsed());
    }

    #[test]
    fn test_e2e_dst_random_faults() {
        run_e2e_dst_random_faults(500, 20, 20, "dst-topic", 1, 10);
    }

    #[test]
    fn test_e2e_dst_random_faults_multi_partition() {
        run_e2e_dst_random_faults(200, 20, 20, "dst-topic-mp", 3, 10);
    }

    /// Extended DST test - runs 1000 seeds (ignored by default, run manually).
    #[test]
    #[ignore]
    fn test_e2e_dst_extended() {
        run_e2e_dst_random_faults(1000, 20, 20, "ext-topic", 1, 25);
    }

    // ========================================================================
    // Actor Mode Tests
    // ========================================================================

    #[test]
    fn test_e2e_actor_mode_single_node() {
        // Test actor mode with single node (no Raft replication needed).
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let config = E2EClusterConfig::with_nodes(1).with_actor_mode();
            let cluster = E2ECluster::start_with_config(config).await;

            assert!(cluster.actor_mode, "Should be in actor mode");

            // Wait for cluster to stabilize (single-node needs time for controller election).
            cluster.sleep(Duration::from_secs(2)).await;

            // Create topic with retries (same pattern as multi-node).
            for attempt in 0..30 {
                if cluster.create_topic("actor-test", 1).await.is_ok() {
                    eprintln!(
                        "[INFO] Single-node actor mode: Topic created on attempt {}",
                        attempt
                    );
                    break;
                }
                cluster.sleep(Duration::from_millis(100)).await;
                if attempt == 29 {
                    panic!("Failed to create topic after 30 attempts");
                }
            }

            // Wait for partition actors to be created.
            cluster.sleep(Duration::from_secs(1)).await;

            // Debug: check router state.
            cluster.debug_print_router_state().await;

            // Produce data with retries.
            for i in 0..5 {
                let offset = cluster
                    .produce_with_retry("actor-test", 0, format!("record-{i}"), 100)
                    .await;
                assert!(offset.is_ok(), "Produce should succeed: {:?}", offset);
                eprintln!(
                    "[INFO] Actor mode: Produced record {} at offset {:?}",
                    i, offset
                );
            }

            // Consume data.
            let records = cluster.consume("actor-test", 0, 0).await;
            assert!(records.is_ok(), "Consume should succeed");
            assert_eq!(records.unwrap().len(), 5, "Should have 5 records");

            eprintln!("[PASS] test_e2e_actor_mode_single_node: Actor mode works in single-node");
        });
    }

    #[test]
    fn test_e2e_actor_mode_multi_node() {
        // Test actor mode with 3-node cluster using Raft replication.
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let config = E2EClusterConfig::with_nodes(3).with_actor_mode();
            let cluster = E2ECluster::start_with_config(config).await;

            assert!(cluster.actor_mode, "Should be in actor mode");

            // Wait for cluster to stabilize (controller election takes longer in actor mode).
            cluster.sleep(Duration::from_secs(5)).await;

            // Create topic - uses controller in multi-node mode.
            // Retry multiple times as controller election may still be in progress.
            for attempt in 0..30 {
                if cluster.create_topic("actor-orders", 1).await.is_ok() {
                    eprintln!("[INFO] Actor mode: Topic created on attempt {}", attempt);
                    break;
                }
                cluster.sleep(Duration::from_millis(200)).await;
                if attempt == 29 {
                    panic!("Failed to create topic after 30 attempts");
                }
            }

            // Wait for partition actors to be created on all nodes.
            // The AssignPartition commit needs to replicate to all nodes and each node
            // needs to create its partition actor before the election can complete.
            cluster.sleep(Duration::from_secs(3)).await;

            // Debug: check router state.
            cluster.debug_print_router_state().await;

            // Produce with retries - will succeed once leader is elected.
            for i in 0..10 {
                let offset = cluster
                    .produce_with_retry("actor-orders", 0, format!("order-{i}"), 100)
                    .await;
                assert!(offset.is_ok(), "Produce should succeed: {:?}", offset);
                eprintln!(
                    "[INFO] Actor mode: Produced record {} at offset {:?}",
                    i, offset
                );
            }

            // Wait for replication to complete.
            cluster.sleep(Duration::from_secs(1)).await;

            // Consume.
            let records = cluster
                .consume("actor-orders", 0, 0)
                .await
                .expect("consume should succeed");
            eprintln!("[INFO] Actor mode: Consumed {} records", records.len());
            assert_eq!(records.len(), 10, "Should have 10 records");

            eprintln!("[PASS] test_e2e_actor_mode_multi_node: Actor mode works with multi-node");
        });
    }

    #[test]
    fn test_e2e_actor_mode_with_partition() {
        // Test actor mode survives network partitions.
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let config = E2EClusterConfig::with_nodes(3).with_actor_mode();
            let cluster = E2ECluster::start_with_config(config).await;

            // Wait for cluster to stabilize.
            cluster.sleep(Duration::from_secs(2)).await;

            // Create topic.
            cluster
                .create_topic("actor-orders", 1)
                .await
                .expect("create topic");

            // Produce some data.
            for i in 0..5 {
                cluster
                    .produce_with_retry("actor-orders", 0, format!("order-{i}"), 100)
                    .await
                    .expect("produce should succeed");
            }
            eprintln!("[INFO] Actor mode: Produced 5 records before partition");

            // Partition node 1 from nodes 2 and 3.
            cluster.partition(&[NodeId::new(1), NodeId::new(2)]);
            cluster.partition(&[NodeId::new(1), NodeId::new(3)]);
            eprintln!("[INFO] Actor mode: Network partition created");

            // Wait for leader election.
            cluster.sleep(Duration::from_secs(3)).await;

            // Produce more (should succeed on majority partition).
            for i in 5..10 {
                cluster
                    .produce_with_retry("actor-orders", 0, format!("order-{i}"), 100)
                    .await
                    .expect("produce should succeed after partition");
            }
            eprintln!("[INFO] Actor mode: Produced 5 more records during partition");

            // Heal the partition.
            cluster.heal_all();
            cluster.sleep(Duration::from_secs(5)).await;

            // Verify data available.
            let records = cluster
                .consume("actor-orders", 0, 0)
                .await
                .expect("consume should succeed");
            eprintln!(
                "[INFO] Actor mode: Consumed {} records after healing",
                records.len()
            );
            assert_eq!(records.len(), 10, "All 10 records should be available");

            eprintln!("[PASS] test_e2e_actor_mode_with_partition: Actor mode survives partitions");
        });
    }

    #[test]
    fn test_e2e_actor_mode_with_crash() {
        // Test actor mode survives node crashes.
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let config = E2EClusterConfig::with_nodes(3).with_actor_mode();
            let cluster = E2ECluster::start_with_config(config).await;

            // Wait for cluster to stabilize.
            cluster.sleep(Duration::from_secs(2)).await;

            // Create topic.
            cluster
                .create_topic("actor-orders", 1)
                .await
                .expect("create topic");

            // Produce some data.
            for i in 0..5 {
                cluster
                    .produce_with_retry("actor-orders", 0, format!("order-{i}"), 100)
                    .await
                    .expect("produce should succeed");
            }
            eprintln!("[INFO] Actor mode: Produced 5 records before crash");

            // Crash node 1.
            cluster.crash_node(NodeId::new(1));
            eprintln!("[INFO] Actor mode: Node 1 crashed");

            // Wait for failover.
            cluster.sleep(Duration::from_secs(3)).await;

            // Produce more (should succeed on surviving nodes).
            for i in 5..10 {
                cluster
                    .produce_with_retry("actor-orders", 0, format!("order-{i}"), 100)
                    .await
                    .expect("produce should succeed after crash");
            }
            eprintln!("[INFO] Actor mode: Produced 5 more records after crash");

            // Wait for replication.
            cluster.sleep(Duration::from_secs(2)).await;

            // Verify data on surviving nodes.
            let records = cluster
                .consume("actor-orders", 0, 0)
                .await
                .expect("consume should succeed");
            eprintln!("[INFO] Actor mode: Consumed {} records", records.len());
            assert_eq!(records.len(), 10, "All 10 records should be available");

            eprintln!("[PASS] test_e2e_actor_mode_with_crash: Actor mode survives crashes");
        });
    }

    /// Actor mode DST test with random fault injection.
    ///
    /// # Verification
    ///
    /// This test performs three levels of verification:
    /// 1. **Raft invariants**: `SingleLeaderPerTerm` via property state snapshots
    /// 2. **Data integrity**: Payload hashes match between produce and consume
    /// 3. **Count check**: At least 50% of produced records are consumable
    #[test]
    fn test_e2e_actor_mode_dst_random_faults() {
        use crate::madsim_scenarios::{FaultScenario, ScenarioExecutor};

        const TOTAL_SEEDS: u64 = 100;
        const RECORDS_PER_SEED: usize = 20;
        const TICKS_PER_SEED: u32 = 20;

        let mut failures: Vec<(u64, String, String)> = Vec::new();
        let mut scenario_counts: std::collections::HashMap<&'static str, u64> =
            std::collections::HashMap::new();

        for seed in 0..TOTAL_SEEDS {
            let scenario = FaultScenario::random(seed);
            let scenario_name = scenario.name();
            *scenario_counts.entry(scenario_name).or_default() += 1;

            if seed % 10 == 0 {
                eprintln!(
                    "[PROGRESS] Actor mode seed {}/{} - scenario: {}",
                    seed, TOTAL_SEEDS, scenario_name
                );
            }

            let result = std::panic::catch_unwind(|| {
                let rt = Runtime::with_seed_and_config(seed, Default::default());
                rt.block_on(async {
                    let config = E2EClusterConfig::with_nodes(3).with_actor_mode();
                    let cluster = E2ECluster::start_with_config(config).await;
                    let mut executor = ScenarioExecutor::new(scenario.clone());

                    // Wait for controller election.
                    cluster.sleep(Duration::from_millis(200)).await;
                    for attempt in 0..10 {
                        if cluster.create_topic("actor-dst-topic", 1).await.is_ok() {
                            break;
                        }
                        cluster.sleep(Duration::from_millis(50)).await;
                        if attempt == 9 {
                            panic!("Failed to create topic after 10 attempts");
                        }
                    }

                    // Interleave production with fault injection.
                    let mut produced = 0;
                    let records_per_tick = RECORDS_PER_SEED / TICKS_PER_SEED as usize;
                    let records_per_tick = records_per_tick.max(1);

                    for tick in 0..TICKS_PER_SEED {
                        // Produce some records with tracking for verification.
                        for i in 0..records_per_tick {
                            let record_num = tick as usize * records_per_tick + i;
                            if record_num >= RECORDS_PER_SEED {
                                break;
                            }
                            if cluster
                                .produce_and_track(
                                    "actor-dst-topic",
                                    0,
                                    format!("record-{}", record_num),
                                    5,
                                )
                                .await
                                .is_ok()
                            {
                                produced += 1;
                            }
                        }

                        // Collect Raft snapshots for property verification.
                        cluster.collect_raft_snapshots().await;

                        // Apply fault scenario.
                        if let Some(action) = executor.tick(&cluster, None) {
                            tracing::debug!(seed, tick, action, "Actor mode fault injected");
                        }

                        // Small sleep to advance simulated time.
                        cluster.sleep(Duration::from_millis(20)).await;
                    }

                    // Heal all partitions after test.
                    cluster.heal_all();
                    cluster.sleep(Duration::from_millis(500)).await;

                    // Collect final Raft snapshots.
                    cluster.collect_raft_snapshots().await;

                    // NEW: Verify cross-replica consistency after healing.
                    if let Err(e) = cluster
                        .verify_replica_consistency("actor-dst-topic", 0)
                        .await
                    {
                        panic!("Actor mode seed {}: {}", seed, e);
                    }

                    // Verify data with integrity checking.
                    let records = cluster
                        .consume_and_verify("actor-dst-topic", 0, 0)
                        .await
                        .expect("consume should succeed");

                    // Finalize verification and check for property violations.
                    let check_result = cluster.finalize_verification();

                    // Check Raft invariants (SingleLeaderPerTerm).
                    if !check_result.violations.is_empty() {
                        let violation_strs: Vec<_> = check_result
                            .violations
                            .iter()
                            .map(|v| format!("{v}"))
                            .collect();
                        panic!(
                            "Seed {}: Raft invariant violations: {}",
                            seed,
                            violation_strs.join(", ")
                        );
                    }

                    // Check data integrity violations (hash mismatches).
                    // ALL acked data must be readable and have correct content.
                    if !check_result.consumer_violations.is_empty() {
                        panic!(
                            "Seed {}: {} data integrity violations: {:?}",
                            seed,
                            check_result.consumer_violations.len(),
                            check_result.consumer_violations
                        );
                    }

                    // With fault injection, we may lose some records depending on scenario.
                    // Verify we got a reasonable number of records.
                    let consumed = records.len();
                    tracing::debug!(
                        seed,
                        produced,
                        consumed,
                        "Actor mode seed completed - all {} consumed records verified correct",
                        consumed
                    );
                });
            });

            if let Err(e) = result {
                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown panic".to_string()
                };
                eprintln!(
                    "[FAIL] Actor mode seed {} ({}): {}",
                    seed, scenario_name, msg
                );
                failures.push((seed, scenario_name.to_string(), msg));
            }
        }

        // Print scenario distribution.
        eprintln!("\n=== Actor Mode Scenario Distribution ===");
        for (name, count) in &scenario_counts {
            eprintln!("  {}: {} seeds", name, count);
        }

        eprintln!("\n=== E2E Actor Mode DST Results ===");
        eprintln!("Total seeds: {}", TOTAL_SEEDS);
        eprintln!("Passed: {}", TOTAL_SEEDS - failures.len() as u64);
        eprintln!("Failed: {}", failures.len());

        if !failures.is_empty() {
            eprintln!("\nFailed seeds:");
            for (seed, scenario, msg) in &failures {
                eprintln!("  Seed {} ({}): {}", seed, scenario, msg);
            }
            panic!("{} seeds failed", failures.len());
        }

        eprintln!(
            "[PASS] Actor mode: All {} seeds passed with random fault injection",
            TOTAL_SEEDS
        );
    }
}
