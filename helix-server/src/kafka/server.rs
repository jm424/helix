//! Kafka-compatible TCP server.
//!
//! Accepts connections speaking Kafka wire protocol and dispatches to handlers.
//! Uses `HelixService` for storage through `KafkaHandler`.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use socket2::{Domain, Socket, Type};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tracing::{error, info};

use super::codec;
use super::error::{KafkaError, KafkaResult};
use super::handler::KafkaHandler;
use crate::service::HelixService;
use helix_runtime::TransportService;
use helix_wal::Storage;

/// Configuration for the Kafka server.
#[derive(Debug, Clone)]
pub struct KafkaServerConfig {
    /// Address to bind to.
    pub bind_addr: SocketAddr,
    /// Hostname to advertise to clients.
    pub advertised_host: String,
    /// Port to advertise to clients.
    pub advertised_port: i32,
    /// Maximum connections.
    pub max_connections: usize,
    /// Auto-create topics on first metadata request.
    pub auto_create_topics: bool,
    /// Default number of partitions for auto-created topics.
    pub auto_create_partitions: u32,
}

impl KafkaServerConfig {
    /// Create a new server config with defaults.
    #[must_use]
    pub fn new(bind_addr: SocketAddr) -> Self {
        let host = bind_addr.ip().to_string();
        let port = i32::from(bind_addr.port());
        Self {
            bind_addr,
            advertised_host: host,
            advertised_port: port,
            max_connections: 1000,
            auto_create_topics: false,
            auto_create_partitions: 1,
        }
    }

    /// Enable auto-creation of topics on first metadata request.
    #[must_use]
    pub const fn with_auto_create_topics(mut self, enabled: bool) -> Self {
        self.auto_create_topics = enabled;
        self
    }

    /// Set the default number of partitions for auto-created topics.
    #[must_use]
    pub const fn with_auto_create_partitions(mut self, partitions: u32) -> Self {
        self.auto_create_partitions = partitions;
        self
    }

    /// Set the advertised host and port.
    #[must_use]
    pub fn with_advertised_listener(mut self, host: String, port: i32) -> Self {
        self.advertised_host = host;
        self.advertised_port = port;
        self
    }
}

/// Kafka-compatible TCP server backed by `HelixService`.
pub struct KafkaServer<S: Storage + Clone + Send + Sync + 'static, T: TransportService> {
    handler: Arc<KafkaHandler<S, T>>,
    config: KafkaServerConfig,
    shutdown: Arc<Notify>,
}

impl<S: Storage + Clone + Send + Sync + 'static, T: TransportService> KafkaServer<S, T> {
    /// Create a new Kafka server.
    #[must_use]
    pub fn new(service: Arc<HelixService<S, T>>, config: KafkaServerConfig) -> Self {
        let handler = Arc::new(KafkaHandler::new(
            service,
            config.advertised_host.clone(),
            config.advertised_port,
            config.auto_create_topics,
            config.auto_create_partitions,
        ));

        Self {
            handler,
            config,
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Get a handle to signal shutdown.
    #[must_use]
    pub fn shutdown_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.shutdown)
    }

    /// Run the server until shutdown is signaled.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to bind or accept connections.
    pub async fn run(&self) -> KafkaResult<()> {
        let listener = create_reusable_listener(self.config.bind_addr)?;
        info!(addr = %self.config.bind_addr, "Kafka server listening");

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            info!(peer = %peer_addr, "New Kafka connection");
                            let handler = Arc::clone(&self.handler);
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream, peer_addr, handler).await {
                                    match e {
                                        KafkaError::Io(ref io_err)
                                            if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
                                        {
                                            info!(peer = %peer_addr, "Connection closed by client (EOF)");
                                        }
                                        _ => {
                                            error!(peer = %peer_addr, error = %e, "Connection error");
                                        }
                                    }
                                } else {
                                    info!(peer = %peer_addr, "Connection closed normally");
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to accept connection");
                        }
                    }
                }
                () = self.shutdown.notified() => {
                    info!("Kafka server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

/// Create a TCP listener with `SO_REUSEADDR` enabled.
///
/// This allows the server to bind to a port that is in `TIME_WAIT` state,
/// which is essential for fast restarts during testing.
fn create_reusable_listener(addr: SocketAddr) -> KafkaResult<TcpListener> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };

    let socket = Socket::new(domain, Type::STREAM, None)?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    // Backlog of 128 pending connections.
    socket.listen(128)?;

    let std_listener: std::net::TcpListener = socket.into();
    let listener = TcpListener::from_std(std_listener)?;

    Ok(listener)
}

/// Handle a single client connection with pipelined request processing.
///
/// Requests are processed concurrently but responses are sent in request
/// order (required by the Kafka wire protocol). This uses an ordered
/// response slot pattern:
/// - Each request gets a sequence number and a oneshot channel
/// - Handler tasks run concurrently and send results via their slot
/// - The writer task drains slots in order, blocking until the next one is ready
///
/// This allows multiple in-flight requests on a single connection,
/// significantly improving throughput when requests have latency (e.g., Raft consensus).
#[allow(clippy::similar_names)] // slot_tx_oneshot/slot_rx_oneshot are canonical tx/rx names.
async fn handle_connection<S: Storage + Clone + Send + Sync + 'static, T: TransportService>(
    stream: TcpStream,
    peer_addr: SocketAddr,
    handler: Arc<KafkaHandler<S, T>>,
) -> KafkaResult<()> {
    use tokio::sync::{mpsc, oneshot};

    // Split stream into read and write halves for concurrent access.
    let (mut read_half, mut write_half) = stream.into_split();

    // Channel for sending ordered response slots to the writer task.
    // Each slot is a oneshot receiver that will deliver the response
    // in the order requests were received.
    let (slot_tx, mut slot_rx) = mpsc::channel::<oneshot::Receiver<BytesMut>>(100);

    // Writer task: drain response slots in order.
    // Blocks on each slot until the handler task completes, ensuring
    // responses are written to the socket in request order.
    let writer_peer_addr = peer_addr;
    let writer_task = tokio::spawn(async move {
        while let Some(slot_rx) = slot_rx.recv().await {
            match slot_rx.await {
                Ok(response_frame) => {
                    if let Err(e) = write_half.write_all(&response_frame).await {
                        error!(
                            peer = %writer_peer_addr,
                            error = %e,
                            "Failed to write response"
                        );
                        return Err(KafkaError::Io(e));
                    }
                }
                Err(_) => {
                    // Handler task dropped without sending — connection is
                    // likely closing or handler panicked. Skip this slot.
                    error!(
                        peer = %writer_peer_addr,
                        "Response slot dropped without value"
                    );
                }
            }
        }
        Ok(())
    });

    // Reader loop: read frames and spawn handler tasks with ordered slots.
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let reader_result: KafkaResult<()> = async {
        loop {
            // Read data from the socket.
            let bytes_read = read_half.read_buf(&mut read_buf).await?;
            if bytes_read == 0 {
                // Connection closed by client.
                return Ok(());
            }

            // Process all complete frames in the buffer.
            while let Some(payload) = codec::read_frame(&mut read_buf)? {
                // Decode request header.
                let request = codec::decode_request_header(payload)?;

                info!(
                    peer = %peer_addr,
                    api_key = request.api_key,
                    api_name = api_key_name(request.api_key),
                    api_version = request.api_version,
                    correlation_id = request.correlation_id,
                    "Received Kafka request"
                );

                // Create an ordered response slot. The writer task holds
                // the receiver and waits on it in FIFO order.
                let (slot_tx_oneshot, slot_rx_oneshot) = oneshot::channel();
                if slot_tx.send(slot_rx_oneshot).await.is_err() {
                    // Writer task has exited — stop reading.
                    return Ok(());
                }

                // Spawn handler task for concurrent processing.
                let handler_clone = Arc::clone(&handler);
                let handler_peer_addr = peer_addr;

                tokio::spawn(async move {
                    let api_key = request.api_key;
                    let api_name = api_key_name(api_key);

                    match handler_clone.handle_request(&request).await {
                        Ok(response_body) => {
                            // Encode response with length prefix.
                            let mut response_frame = BytesMut::new();
                            codec::write_frame(&mut response_frame, &response_body);

                            info!(
                                peer = %handler_peer_addr,
                                api_key,
                                api_name,
                                response_len = response_frame.len(),
                                "Sending response"
                            );

                            // Send to writer via this request's ordered slot.
                            let _ = slot_tx_oneshot.send(response_frame);
                        }
                        Err(e) => {
                            error!(
                                peer = %handler_peer_addr,
                                api_key,
                                api_name,
                                error = %e,
                                "Handler error"
                            );
                            // Drop the slot sender — writer will skip this slot.
                            // The client will handle timeout/retry.
                        }
                    }
                });
            }
        }
    }
    .await;

    // Drop the sender to signal writer task to exit.
    drop(slot_tx);

    // Wait for writer task to complete.
    let writer_result = writer_task.await.unwrap_or_else(|e| {
        error!(peer = %peer_addr, error = %e, "Writer task panicked");
        Err(KafkaError::Io(std::io::Error::other(
            "writer task panicked",
        )))
    });

    // Return first error encountered.
    reader_result?;
    writer_result
}

/// Convert API key to human-readable name for logging.
const fn api_key_name(key: i16) -> &'static str {
    match key {
        0 => "Produce",
        1 => "Fetch",
        2 => "ListOffsets",
        3 => "Metadata",
        8 => "OffsetCommit",
        9 => "OffsetFetch",
        10 => "FindCoordinator",
        11 => "JoinGroup",
        12 => "Heartbeat",
        13 => "LeaveGroup",
        14 => "SyncGroup",
        18 => "ApiVersions",
        19 => "CreateTopics",
        20 => "DeleteTopics",
        22 => "InitProducerId",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_config_defaults() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092);
        let config = KafkaServerConfig::new(addr);

        assert_eq!(config.bind_addr, addr);
        assert_eq!(config.advertised_host, "127.0.0.1");
        assert_eq!(config.advertised_port, 9092);
        assert!(!config.auto_create_topics);
    }

    #[test]
    fn test_config_advertised() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092);
        let config = KafkaServerConfig::new(addr)
            .with_advertised_listener("kafka.example.com".to_string(), 19092);

        assert_eq!(config.advertised_host, "kafka.example.com");
        assert_eq!(config.advertised_port, 19092);
    }

    #[test]
    fn test_config_auto_create() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092);
        let config = KafkaServerConfig::new(addr).with_auto_create_topics(true);

        assert!(config.auto_create_topics);
    }
}
