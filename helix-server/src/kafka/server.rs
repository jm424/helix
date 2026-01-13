//! Kafka-compatible TCP server.
//!
//! Accepts connections speaking Kafka wire protocol and dispatches to handlers.
//! Uses `HelixService` for storage through `KafkaHandler`.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tracing::{debug, error, info, warn};

use super::codec;
use super::error::{KafkaError, KafkaResult};
use super::handler::KafkaHandler;
use crate::HelixService;

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
        }
    }

    /// Enable auto-creation of topics on first metadata request.
    #[must_use]
    pub const fn with_auto_create_topics(mut self, enabled: bool) -> Self {
        self.auto_create_topics = enabled;
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
pub struct KafkaServer {
    handler: Arc<KafkaHandler>,
    config: KafkaServerConfig,
    shutdown: Arc<Notify>,
}

impl KafkaServer {
    /// Create a new Kafka server.
    #[must_use]
    pub fn new(service: Arc<HelixService>, config: KafkaServerConfig) -> Self {
        let handler = Arc::new(KafkaHandler::new(
            service,
            config.advertised_host.clone(),
            config.advertised_port,
            config.auto_create_topics,
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
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        info!(addr = %self.config.bind_addr, "Kafka server listening");

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            debug!(peer = %peer_addr, "New Kafka connection");
                            let handler = Arc::clone(&self.handler);
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream, peer_addr, handler).await {
                                    match e {
                                        KafkaError::Io(ref io_err)
                                            if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
                                        {
                                            debug!(peer = %peer_addr, "Connection closed");
                                        }
                                        _ => {
                                            warn!(peer = %peer_addr, error = %e, "Connection error");
                                        }
                                    }
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

/// Handle a single client connection.
async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    handler: Arc<KafkaHandler>,
) -> KafkaResult<()> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);

    loop {
        // Read data from the socket.
        let bytes_read = stream.read_buf(&mut read_buf).await?;
        if bytes_read == 0 {
            // Connection closed.
            return Ok(());
        }

        // Process all complete frames in the buffer.
        while let Some(payload) = codec::read_frame(&mut read_buf)? {
            // Decode request header.
            let request = codec::decode_request_header(payload)?;

            debug!(
                peer = %peer_addr,
                api_key = request.api_key,
                api_version = request.api_version,
                correlation_id = request.correlation_id,
                "Processing Kafka request"
            );

            // Handle the request.
            let response_body = handler.handle_request(&request).await?;

            // Write response with length prefix.
            let mut response_frame = BytesMut::new();
            codec::write_frame(&mut response_frame, &response_body);
            stream.write_all(&response_frame).await?;
        }
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
