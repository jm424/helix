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
//!     --peer 2:node2:9092:9002 --peer 3:node3:9092:9003
//!
//! # Node 2
//! helix-server --protocol kafka --node-id 2 --raft-addr 0.0.0.0:9002 \
//!     --peer 1:node1:9092:9001 --peer 3:node3:9092:9003
//!
//! # Node 3
//! helix-server --protocol kafka --node-id 3 --raft-addr 0.0.0.0:9003 \
//!     --peer 1:node1:9092:9001 --peer 2:node2:9092:9002
//! ```
//!
//! In Docker/K8s, use `--kafka-advertise-addr` so Metadata responses
//! return the container hostname instead of `0.0.0.0`:
//!
//! ```bash
//! helix-server --protocol kafka --listen-addr 0.0.0.0:9092 \
//!     --kafka-advertise-addr helix-node1:9092 ...
//! ```

#![deny(unsafe_code)]
#![warn(missing_docs)]

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use helix_core::{NodeId, WriteDurability};
use helix_runtime::PeerInfo;
use tonic::transport::Server;
use tracing::{info, Level};
use tracing_subscriber::{fmt::format::FmtSpan, FmtSubscriber};

use helix_server::admin_grpc::AdminService;
use helix_server::generated::helix_server::HelixServer;
use helix_server::generated_admin::resources_server::ResourcesServer;
use helix_server::kafka::{KafkaServer, KafkaServerConfig};
use helix_server::HelixService;
#[cfg(feature = "s3")]
use helix_tier::S3Config;
use helix_tier::TieringConfig;

/// Protocol to use for client connections.
#[derive(Debug, Clone, Copy, ValueEnum, Default)]
enum Protocol {
    /// Native Helix gRPC API.
    #[default]
    Grpc,
    /// Kafka wire protocol compatibility.
    Kafka,
}

/// Write durability mode for the WAL.
#[derive(Debug, Clone, Copy, ValueEnum, Default)]
enum Durability {
    /// Skip fsync, rely on Raft replication for durability (fast, default).
    /// Safe for multi-node deployments with replication.
    #[default]
    ReplicationOnly,
    /// Wait for fsync on each flush (safe for single-node, slower).
    /// Use this for single-node deployments or maximum durability.
    Fsync,
}

impl From<Durability> for WriteDurability {
    fn from(d: Durability) -> Self {
        match d {
            Durability::Fsync => Self::Fsync,
            Durability::ReplicationOnly => Self::ReplicationOnly,
        }
    }
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

    /// Directory for object storage (tiered storage backend).
    /// When specified, enables filesystem-based object storage for tiering.
    /// If not specified, uses simulated in-memory storage.
    /// Mutually exclusive with --s3-bucket.
    #[arg(long)]
    object_storage_dir: Option<PathBuf>,

    /// S3 bucket for tiered storage (requires --features s3).
    /// When specified, enables S3-based object storage for tiering.
    /// Mutually exclusive with --object-storage-dir.
    #[arg(long)]
    s3_bucket: Option<String>,

    /// S3 key prefix for tiered segments (default: "helix/segments/").
    #[arg(long, default_value = "helix/segments/")]
    s3_prefix: String,

    /// S3 region (default: from AWS config/environment).
    #[arg(long)]
    s3_region: Option<String>,

    /// Custom S3 endpoint URL (for LocalStack/MinIO).
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// Force path-style S3 addressing (required for LocalStack/MinIO).
    #[arg(long)]
    s3_force_path_style: bool,

    /// Minimum age (in seconds) before a segment is eligible for tiering.
    /// Default is 300 seconds (5 minutes). Set to 0 for immediate tiering.
    #[arg(long, default_value = "300")]
    tier_min_age_secs: u64,

    /// Number of shared WALs to use for fsync amortization (1-16).
    /// Only applies when --data-dir is specified.
    /// If not specified, defaults to 4.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=16))]
    shared_wal_count: Option<u32>,

    /// Write durability mode for the WAL.
    ///
    /// - replication-only (default): Skip fsync, rely on Raft replication.
    ///   Fast, safe for multi-node with acks=all.
    /// - fsync: Wait for fsync on each flush. Safe for single-node, slower.
    #[arg(long, value_enum, default_value = "replication-only")]
    write_durability: Durability,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: Level,

    /// Auto-create topics on first produce/metadata request (Kafka mode only).
    #[arg(long)]
    auto_create_topics: bool,

    /// Default number of partitions for auto-created topics.
    #[arg(long, default_value = "1")]
    auto_create_partitions: u32,

    /// Address to listen on for the admin gRPC API (kafkaadmin.Resources).
    /// Used for topic lifecycle management.
    /// If not specified, the admin server is not started.
    #[arg(long)]
    admin_addr: Option<SocketAddr>,

    /// Path to TLS certificate for the admin gRPC server (PEM format).
    #[arg(long)]
    admin_tls_cert: Option<String>,

    /// Path to TLS private key for the admin gRPC server (PEM format).
    #[arg(long)]
    admin_tls_key: Option<String>,

    /// Advertised Kafka address for this node (`host:port`).
    /// Used in Metadata API responses so clients can connect.
    /// Defaults to `--listen-addr` if not specified.
    /// Required for Docker/K8s where the bind address differs from
    /// the externally reachable address.
    #[arg(long)]
    kafka_advertise_addr: Option<String>,

    /// Advertised Kafka address for a peer node in format `node_id:host:port`.
    /// Overrides the peer's Kafka address in Metadata API responses.
    /// Use when peers need different internal (Raft) and external (Kafka) addresses,
    /// e.g., internal headless DNS for Raft but Fabric DNS for Kafka clients.
    /// Can be specified multiple times for multiple peers.
    #[arg(long = "peer-kafka-advertise", value_parser = parse_peer_advertise)]
    peer_kafka_advertises: Vec<PeerAdvertise>,

    /// Local disk retention in milliseconds.
    ///
    /// Sealed WAL segments older than this are deleted from local disk,
    /// provided all entries have been replicated via Raft. Always active.
    /// Default: 4 hours (14400000 ms). Set to 0 to disable retention.
    #[arg(long, default_value = "14400000")]
    local_retention_ms: u64,

    /// Total retention in milliseconds (S3 lifetime).
    ///
    /// Only meaningful when tiering is configured — extends data lifetime
    /// beyond local disk. Must be >= local_retention_ms.
    /// Default: 4 days (345600000 ms).
    #[arg(long, default_value = "345600000")]
    retention_ms: u64,

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
        return Err(format!(
            "partition count must be positive, got {partitions}"
        ));
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

/// Peer advertised Kafka address override.
#[derive(Debug, Clone)]
struct PeerAdvertise {
    /// Node ID of the peer.
    node_id: NodeId,
    /// Advertised Kafka address (`host:port`) for Metadata responses.
    addr: String,
}

/// Parses a peer advertise spec in format `node_id:host:port`.
fn parse_peer_advertise(s: &str) -> Result<PeerAdvertise, String> {
    let first_colon = s
        .find(':')
        .ok_or_else(|| format!("invalid peer-kafka-advertise '{s}', expected 'node_id:host:port'"))?;
    let node_id: u64 = s[..first_colon]
        .parse()
        .map_err(|_| format!("invalid node_id in peer-kafka-advertise '{s}'"))?;
    let addr = s[first_colon + 1..].to_string();
    if !addr.contains(':') {
        return Err(format!("invalid addr in peer-kafka-advertise '{s}', expected 'host:port'"));
    }
    Ok(PeerAdvertise {
        node_id: NodeId::new(node_id),
        addr,
    })
}

/// Parses an advertise address in format `host:port` into `(String, i32)`.
///
/// Splits on the last `:` to handle IPv6 addresses.
fn parse_advertise_addr(s: &str) -> (String, i32) {
    let last_colon = s
        .rfind(':')
        .unwrap_or_else(|| panic!("invalid advertise address '{s}', expected 'host:port'"));
    let host = s[..last_colon].to_string();
    let port: i32 = s[last_colon + 1..]
        .parse()
        .unwrap_or_else(|_| panic!("invalid port in advertise address '{s}'"));
    (host, port)
}

#[tokio::main]
#[allow(clippy::too_many_lines)] // Main entry point with CLI handling.
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let args = Args::parse();

    // Initialize logging to stderr (stdout may be suppressed by test harness).
    // FmtSpan::CLOSE logs when spans close with their duration - useful for profiling.
    let subscriber = FmtSubscriber::builder()
        .with_max_level(args.log_level)
        .with_target(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::CLOSE)
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
        let raft_peers: Vec<PeerInfo> = args
            .peers
            .iter()
            .map(ExtendedPeerInfo::to_peer_info)
            .collect();

        // Build Kafka peer addresses map for metadata responses.
        // Start with peer addresses from --peer, then override with --peer-kafka-advertise.
        let mut kafka_peer_addrs: HashMap<NodeId, String> = args
            .peers
            .iter()
            .map(|p| (p.node_id, p.kafka_addr()))
            .collect();
        for adv in &args.peer_kafka_advertises {
            kafka_peer_addrs.insert(adv.node_id, adv.addr.clone());
        }

        // This node's Kafka address for metadata responses.
        // Use --kafka-advertise-addr if set, otherwise fall back to --listen-addr.
        let kafka_addr = args
            .kafka_advertise_addr
            .clone()
            .unwrap_or_else(|| args.listen_addr.to_string());

        // Build S3 config if bucket is specified.
        #[cfg(feature = "s3")]
        let s3_config = args.s3_bucket.as_ref().map(|bucket| {
            let mut config = S3Config::new(bucket.clone());
            config.key_prefix = args.s3_prefix.clone();
            if let Some(region) = &args.s3_region {
                config.region = Some(region.clone());
            }
            if let Some(endpoint) = &args.s3_endpoint {
                config.endpoint_url = Some(endpoint.clone());
            }
            config.force_path_style = args.s3_force_path_style;
            config
        });

        // Build tiering config if S3 or object_storage_dir is set.
        #[cfg(feature = "s3")]
        let tiering_config = if args.s3_bucket.is_some() || args.object_storage_dir.is_some() {
            Some(TieringConfig {
                min_age_secs: args.tier_min_age_secs,
                max_concurrent_uploads: 4,
                verify_on_download: true,
            })
        } else {
            None
        };
        #[cfg(not(feature = "s3"))]
        let tiering_config = if args.object_storage_dir.is_some() {
            Some(TieringConfig {
                min_age_secs: args.tier_min_age_secs,
                max_concurrent_uploads: 4,
                verify_on_download: true,
            })
        } else {
            None
        };

        let write_durability = args.write_durability.into();

        let local_retention_ms = if args.local_retention_ms == 0 {
            None
        } else {
            Some(args.local_retention_ms)
        };

        #[cfg(feature = "s3")]
        let service = HelixService::new_multi_node(
            args.cluster_id,
            args.node_id,
            raft_addr,
            raft_peers,
            args.data_dir,
            args.object_storage_dir,
            s3_config,
            tiering_config,
            kafka_addr,
            kafka_peer_addrs,
            args.shared_wal_count,
            write_durability,
            local_retention_ms,
        )
        .await?;
        #[cfg(not(feature = "s3"))]
        let service = HelixService::new_multi_node(
            args.cluster_id,
            args.node_id,
            raft_addr,
            raft_peers,
            args.data_dir,
            args.object_storage_dir,
            tiering_config,
            kafka_addr,
            kafka_peer_addrs,
            args.shared_wal_count,
            write_durability,
            local_retention_ms,
        )
        .await?;

        if let Ok(report_path) = std::env::var("HELIX_BENCH_REPORT_PATH") {
            let interval_ms = std::env::var("HELIX_BENCH_REPORT_INTERVAL_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(1000);
            let report_path = report_path.replace("{node_id}", &args.node_id.to_string());
            service.start_bench_reporter(PathBuf::from(report_path), interval_ms);
        }

        service
    } else {
        // Single-node mode (for development/testing).
        info!("Starting in single-node mode");
        let write_durability = args.write_durability.into();

        match (args.data_dir, args.object_storage_dir) {
            (Some(data_dir), Some(object_storage_dir)) => {
                HelixService::with_data_and_object_storage(
                    args.cluster_id,
                    args.node_id,
                    data_dir,
                    object_storage_dir,
                    args.shared_wal_count,
                    write_durability,
                )
                .await
            }
            (Some(data_dir), None) => {
                HelixService::with_data_dir(
                    args.cluster_id,
                    args.node_id,
                    data_dir,
                    args.shared_wal_count,
                    write_durability,
                )
                .await
            }
            (None, _) => HelixService::new(args.cluster_id, args.node_id).await,
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
        // Safe truncation: cluster size is always small (max 3 for replication factor).
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
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
            service
                .create_topic(topic.name.clone(), topic.partitions)
                .await
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

            // Spawn admin gRPC server if --admin-addr is specified.
            if let Some(admin_addr) = args.admin_addr {
                let admin_svc = Arc::clone(&service);
                let admin_port = admin_addr.port();
                let admin_tls_cert = args.admin_tls_cert.clone();
                let admin_tls_key = args.admin_tls_key.clone();
                tokio::spawn(async move {
                    let mut builder = Server::builder();

                    // Configure TLS if cert and key paths are provided.
                    if let (Some(cert_path), Some(key_path)) =
                        (admin_tls_cert, admin_tls_key)
                    {
                        let cert = tokio::fs::read(&cert_path)
                            .await
                            .unwrap_or_else(|e| panic!("failed to read TLS cert {cert_path}: {e}"));
                        let key = tokio::fs::read(&key_path)
                            .await
                            .unwrap_or_else(|e| panic!("failed to read TLS key {key_path}: {e}"));
                        let tls_config = tonic::transport::ServerTlsConfig::new()
                            .identity(tonic::transport::Identity::from_pem(cert, key));
                        builder = builder
                            .tls_config(tls_config)
                            .expect("failed to configure TLS for admin server");
                        info!(addr = %admin_addr, "Admin gRPC server listening (TLS)");
                    } else {
                        info!(addr = %admin_addr, "Admin gRPC server listening (plaintext)");
                    }

                    if let Err(e) = builder
                        .add_service(ResourcesServer::new(
                            AdminService::new(admin_svc, admin_port),
                        ))
                        .serve(admin_addr)
                        .await
                    {
                        tracing::error!(error = %e, "Admin gRPC server failed");
                    }
                });
            }

            // Configure Kafka server.
            let mut kafka_config = KafkaServerConfig::new(args.listen_addr)
                .with_auto_create_topics(args.auto_create_topics)
                .with_auto_create_partitions(args.auto_create_partitions);

            // Apply advertised listener override for Docker/K8s environments.
            if let Some(ref advertise) = args.kafka_advertise_addr {
                let (host, port) = parse_advertise_addr(advertise);
                kafka_config = kafka_config
                    .with_advertised_listener(host, port);
            }

            let kafka_server = KafkaServer::new(service, kafka_config);

            info!(
                addr = %args.listen_addr,
                auto_create_topics = args.auto_create_topics,
                auto_create_partitions = args.auto_create_partitions,
                "Kafka server listening"
            );

            kafka_server.run().await?;
        }
    }

    Ok(())
}
