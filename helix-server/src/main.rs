//! Helix distributed log server binary.
//!
//! This is the main entry point for the Helix server, providing a gRPC API
//! for distributed log operations.

#![deny(unsafe_code)]
#![warn(missing_docs)]

use std::net::SocketAddr;

use clap::Parser;
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

    /// Address to listen on for gRPC.
    #[arg(long, default_value = "127.0.0.1:9092")]
    listen_addr: SocketAddr,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: Level,
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
        "Starting Helix server"
    );

    // Create the service.
    let service = HelixService::new(args.cluster_id, args.node_id);

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
