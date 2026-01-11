//! Helix distributed log server binary.
//!
//! This is the main entry point for the Helix server, providing a gRPC API
//! for distributed log operations.
//!
//! # Single-Node Mode
//!
//! By default, the server runs in single-node mode (no `--raft-addr`):
//!
//! ```bash
//! helix-server --node-id 1
//! ```
//!
//! # Multi-Node Mode
//!
//! For a multi-node cluster, specify `--raft-addr` and `--peer` arguments:
//!
//! ```bash
//! # Node 1
//! helix-server --node-id 1 --raft-addr 0.0.0.0:9001 \
//!     --peer 2:node2:9002 --peer 3:node3:9003
//!
//! # Node 2
//! helix-server --node-id 2 --raft-addr 0.0.0.0:9002 \
//!     --peer 1:node1:9001 --peer 3:node3:9003
//!
//! # Node 3
//! helix-server --node-id 3 --raft-addr 0.0.0.0:9003 \
//!     --peer 1:node1:9001 --peer 2:node2:9002
//! ```

#![deny(unsafe_code)]
#![warn(missing_docs)]

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use helix_core::NodeId;
use helix_runtime::PeerInfo;
use tonic::transport::Server;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use helix_server::generated::helix_server::HelixServer;
use helix_server::HelixService;

/// Helix distributed log server.
#[derive(Parser, Debug)]
#[command(name = "helix-server")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Node ID for this server.
    #[arg(long, default_value = "1")]
    node_id: u64,

    /// Cluster ID.
    #[arg(long, default_value = "helix-cluster")]
    cluster_id: String,

    /// Address to listen on for gRPC client connections.
    #[arg(long, default_value = "127.0.0.1:9092")]
    listen_addr: SocketAddr,

    /// Address to listen on for Raft peer connections.
    /// If not specified, runs in single-node mode.
    #[arg(long)]
    raft_addr: Option<SocketAddr>,

    /// Peer node in format `node_id:host:port` (e.g., `2:node2:9002`).
    /// Can be specified multiple times for multiple peers.
    #[arg(long = "peer", value_parser = parse_peer)]
    peers: Vec<PeerInfo>,

    /// Data directory for persistent storage.
    /// If not specified, uses in-memory storage.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: Level,
}

/// Parses a peer specification in format `node_id:host:port`.
///
/// The address is stored as a string and resolved at connection time,
/// which supports Docker DNS and other dynamic resolution scenarios.
fn parse_peer(s: &str) -> Result<PeerInfo, String> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 3 {
        return Err(format!(
            "invalid peer format '{s}', expected 'node_id:host:port'"
        ));
    }

    let node_id: u64 = parts[0]
        .parse()
        .map_err(|_| format!("invalid node_id '{}' in peer '{s}'", parts[0]))?;

    let host = parts[1];
    let port: u16 = parts[2]
        .parse()
        .map_err(|_| format!("invalid port '{}' in peer '{s}'", parts[2]))?;

    Ok(PeerInfo {
        node_id: NodeId::new(node_id),
        addr: format!("{host}:{port}"),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize logging.
    let subscriber = FmtSubscriber::builder()
        .with_max_level(args.log_level)
        .with_target(true)
        .with_thread_ids(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!(
        node_id = args.node_id,
        cluster_id = %args.cluster_id,
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
            peers = ?args.peers.iter().map(|p| format!("{}:{}", p.node_id.get(), p.addr)).collect::<Vec<_>>(),
            "Starting in multi-node mode"
        );
        HelixService::new_multi_node(
            args.cluster_id,
            args.node_id,
            raft_addr,
            args.peers,
            args.data_dir,
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

    // Create a default topic for testing.
    service.create_topic("default".to_string(), 4).await?;
    info!("Created default topic with 4 partitions");

    // Start the gRPC server.
    info!(addr = %args.listen_addr, "gRPC server listening");
    Server::builder()
        .add_service(HelixServer::new(service))
        .serve(args.listen_addr)
        .await?;

    Ok(())
}
