//! Helix distributed log server binary.
//!
//! This is the main entry point for the Helix server, providing either a gRPC API
//! or Kafka wire protocol compatibility for distributed log operations.
//!
//! # Single-Node Mode (gRPC)
//!
//! By default, the server runs in single-node mode with gRPC protocol:
//!
//! ```bash
//! helix-server --node-id 1
//! ```
//!
//! # Single-Node Mode (Kafka)
//!
//! Run with Kafka protocol for compatibility with Kafka clients:
//!
//! ```bash
//! helix-server --protocol kafka --listen-addr 0.0.0.0:9092 --auto-create-topics
//! ```
//!
//! # Multi-Node Mode
//!
//! For a multi-node cluster, specify `--raft-addr` and `--peer` arguments:
//!
//! ```bash
//! # Node 1 (Kafka protocol)
//! helix-server --protocol kafka --node-id 1 --raft-addr 0.0.0.0:9001 \
//!     --peer 2:node2:9002 --peer 3:node3:9003
//!
//! # Node 2
//! helix-server --protocol kafka --node-id 2 --raft-addr 0.0.0.0:9002 \
//!     --peer 1:node1:9001 --peer 3:node3:9003
//!
//! # Node 3
//! helix-server --protocol kafka --node-id 3 --raft-addr 0.0.0.0:9003 \
//!     --peer 1:node1:9001 --peer 2:node2:9002
//! ```

#![deny(unsafe_code)]
#![warn(missing_docs)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use helix_core::NodeId;
use helix_runtime::PeerInfo;
use tonic::transport::Server;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use helix_server::generated::helix_server::HelixServer;
use helix_server::kafka::{KafkaServer, KafkaServerConfig};
use helix_server::HelixService;

/// Protocol to use for client connections.
#[derive(Debug, Clone, Copy, ValueEnum, Default)]
enum Protocol {
    /// Native Helix gRPC API.
    #[default]
    Grpc,
    /// Kafka wire protocol compatibility.
    Kafka,
}

/// Helix distributed log server.
#[derive(Parser, Debug)]
#[command(name = "helix-server")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Protocol to use for client connections.
    #[arg(long, value_enum, default_value = "grpc")]
    protocol: Protocol,

    /// Node ID for this server.
    #[arg(long, default_value = "1")]
    node_id: u64,

    /// Cluster ID.
    #[arg(long, default_value = "helix-cluster")]
    cluster_id: String,

    /// Address to listen on for client connections (gRPC or Kafka).
    #[arg(long, default_value = "127.0.0.1:9092")]
    listen_addr: SocketAddr,

    /// Address to listen on for Raft peer connections.
    /// If not specified, runs in single-node mode.
    #[arg(long)]
    raft_addr: Option<SocketAddr>,

    /// Peer node in format `node_id:host:kafka_port:raft_port`.
    /// Example: `2:node2:9093:8002` means node 2 is at host "node2",
    /// with Kafka server on port 9093 and Raft server on port 8002.
    /// Can be specified multiple times for multiple peers.
    #[arg(long = "peer", value_parser = parse_peer)]
    peers: Vec<ExtendedPeerInfo>,

    /// Data directory for persistent storage.
    /// If not specified, uses in-memory storage.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: Level,

    /// Auto-create topics on first produce/metadata request (Kafka mode only).
    #[arg(long)]
    auto_create_topics: bool,

    /// Pre-create a topic at startup in format `name:partitions` (e.g., `test-topic:1`).
    /// Can be specified multiple times for multiple topics.
    /// All nodes should use the same topics for consistent Raft group allocation.
    #[arg(long = "topic", value_parser = parse_topic)]
    topics: Vec<TopicSpec>,
}

/// Topic specification for pre-creation.
#[derive(Debug, Clone)]
struct TopicSpec {
    /// Topic name.
    name: String,
    /// Number of partitions.
    partitions: i32,
}

/// Parses a topic specification in format `name:partitions`.
fn parse_topic(s: &str) -> Result<TopicSpec, String> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(format!(
            "invalid topic format '{s}', expected 'name:partitions'"
        ));
    }

    let name = parts[0].to_string();
    if name.is_empty() {
        return Err("topic name cannot be empty".to_string());
    }

    let partitions: i32 = parts[1]
        .parse()
        .map_err(|_| format!("invalid partition count '{}' in topic '{s}'", parts[1]))?;

    if partitions <= 0 {
        return Err(format!("partition count must be positive, got {partitions}"));
    }

    Ok(TopicSpec { name, partitions })
}

/// Extended peer info with host and both Kafka and Raft ports.
#[derive(Debug, Clone)]
struct ExtendedPeerInfo {
    /// The peer's node ID.
    node_id: NodeId,
    /// The peer's hostname or IP address.
    host: String,
    /// The peer's Kafka port (where clients connect).
    kafka_port: u16,
    /// The peer's Raft port (where peers connect for consensus).
    raft_port: u16,
}

impl ExtendedPeerInfo {
    /// Returns the Kafka address (`host:kafka_port`).
    fn kafka_addr(&self) -> String {
        format!("{}:{}", self.host, self.kafka_port)
    }

    /// Returns the Raft address (`host:raft_port`).
    fn raft_addr(&self) -> String {
        format!("{}:{}", self.host, self.raft_port)
    }

    /// Converts to `PeerInfo` for the transport (uses Raft address).
    fn to_peer_info(&self) -> PeerInfo {
        PeerInfo {
            node_id: self.node_id,
            addr: self.raft_addr(),
        }
    }
}

/// Parses a peer specification in format `node_id:host:kafka_port:raft_port`.
///
/// The host is used for both Kafka and Raft connections, just on different ports.
///
/// Example: `2:node2:9093:8002` means node 2 is at host "node2",
/// Kafka server on port 9093, Raft server on port 8002.
fn parse_peer(s: &str) -> Result<ExtendedPeerInfo, String> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 4 {
        return Err(format!(
            "invalid peer format '{s}', expected 'node_id:host:kafka_port:raft_port'"
        ));
    }

    let node_id: u64 = parts[0]
        .parse()
        .map_err(|_| format!("invalid node_id '{}' in peer '{s}'", parts[0]))?;

    let host = parts[1].to_string();
    if host.is_empty() {
        return Err(format!("empty host in peer '{s}'"));
    }

    let kafka_port: u16 = parts[2]
        .parse()
        .map_err(|_| format!("invalid kafka_port '{}' in peer '{s}'", parts[2]))?;

    let raft_port: u16 = parts[3]
        .parse()
        .map_err(|_| format!("invalid raft_port '{}' in peer '{s}'", parts[3]))?;

    Ok(ExtendedPeerInfo {
        node_id: NodeId::new(node_id),
        host,
        kafka_port,
        raft_port,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Diagnostic: confirm process is actually running (before any initialization).
    eprintln!("[SERVER_START] pid={}", std::process::id());

    let args = Args::parse();

    // Diagnostic: confirm args parsed successfully.
    eprintln!(
        "[SERVER_ARGS] pid={} node_id={} listen_addr={} raft_addr={:?}",
        std::process::id(),
        args.node_id,
        args.listen_addr,
        args.raft_addr
    );

    // Initialize logging to stderr (stdout may be suppressed by test harness).
    let subscriber = FmtSubscriber::builder()
        .with_max_level(args.log_level)
        .with_target(true)
        .with_thread_ids(true)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!(
        node_id = args.node_id,
        cluster_id = %args.cluster_id,
        protocol = ?args.protocol,
        listen_addr = %args.listen_addr,
        raft_addr = ?args.raft_addr,
        peer_count = args.peers.len(),
        "Starting Helix server"
    );

    // Create the service based on mode.
    let service = if let Some(raft_addr) = args.raft_addr {
        // Multi-node mode with Raft networking.
        info!(
            raft_addr = %raft_addr,
            peers = ?args.peers.iter().map(|p| format!("{}:kafka={},raft={}", p.node_id.get(), p.kafka_addr(), p.raft_addr())).collect::<Vec<_>>(),
            "Starting in multi-node mode"
        );

        // Convert ExtendedPeerInfo to PeerInfo for the transport (uses Raft addresses).
        let raft_peers: Vec<PeerInfo> = args.peers.iter().map(ExtendedPeerInfo::to_peer_info).collect();

        // Build Kafka peer addresses map for metadata responses.
        let kafka_peer_addrs: HashMap<NodeId, String> = args
            .peers
            .iter()
            .map(|p| (p.node_id, p.kafka_addr()))
            .collect();

        // This node's Kafka address (from listen_addr).
        let kafka_addr = args.listen_addr.to_string();

        HelixService::new_multi_node(
            args.cluster_id,
            args.node_id,
            raft_addr,
            raft_peers,
            args.data_dir,
            kafka_addr,
            kafka_peer_addrs,
        )
        .await?
    } else {
        // Single-node mode (for development/testing).
        info!("Starting in single-node mode");
        if let Some(data_dir) = args.data_dir {
            HelixService::with_data_dir(args.cluster_id, args.node_id, data_dir)
        } else {
            HelixService::new(args.cluster_id, args.node_id)
        }
    };

    // Wait for controller to be ready before creating topics (multi-node only).
    if service.is_multi_node() && !args.topics.is_empty() {
        info!("Waiting for controller leader election before creating topics...");
        service
            .wait_for_controller_ready(std::time::Duration::from_secs(30))
            .await?;
    }

    // Pre-create topics specified via --topic flags.
    // In multi-node mode, use controller partition for coordination.
    for topic in &args.topics {
        #[allow(clippy::cast_sign_loss)]
        let result = if service.is_multi_node() {
            // Multi-node: use controller to coordinate topic creation across cluster.
            // Default replication factor to cluster size (max 3).
            let replication_factor = service.cluster_nodes().len().min(3) as u32;
            service
                .create_topic_via_controller(
                    topic.name.clone(),
                    topic.partitions as u32,
                    replication_factor,
                )
                .await
        } else {
            // Single-node: create directly.
            service.create_topic(topic.name.clone(), topic.partitions).await
        };

        match result {
            Ok(()) => {
                info!(
                    topic = %topic.name,
                    partitions = topic.partitions,
                    "Pre-created topic"
                );
            }
            Err(e) => {
                // Topic may already exist if restarting with same data-dir.
                info!(
                    topic = %topic.name,
                    error = %e,
                    "Topic creation skipped (may already exist)"
                );
            }
        }
    }

    // Start the appropriate server based on protocol.
    match args.protocol {
        Protocol::Grpc => {
            // Create a default topic for testing if no topics were pre-created.
            if args.topics.is_empty() {
                service.create_topic("default".to_string(), 4).await?;
                info!("Created default topic with 4 partitions");
            }

            // Start the gRPC server.
            info!(addr = %args.listen_addr, "gRPC server listening");
            Server::builder()
                .add_service(HelixServer::new(service))
                .serve(args.listen_addr)
                .await?;
        }
        Protocol::Kafka => {
            // For Kafka mode, wrap service in Arc for sharing.
            let service = Arc::new(service);

            // Configure Kafka server.
            let kafka_config = KafkaServerConfig::new(args.listen_addr)
                .with_auto_create_topics(args.auto_create_topics);

            let kafka_server = KafkaServer::new(service, kafka_config);

            info!(
                addr = %args.listen_addr,
                auto_create_topics = args.auto_create_topics,
                "Kafka server listening"
            );

            kafka_server.run().await?;
        }
    }

    Ok(())
}
