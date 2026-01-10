//! System limits and configuration bounds.
//!
//! Following TigerStyle: put limits on everything.
//! Every queue, buffer, and resource has an explicit maximum size.
//! This prevents unbounded growth and makes the system predictable.

/// System-wide limits for Helix.
///
/// All limits are explicit and configurable. Default values are chosen
/// to be safe for most deployments while allowing customization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    // Message and batch limits.
    /// Maximum size of a single message in bytes.
    pub max_message_bytes: u32,
    /// Maximum size of a batch in bytes.
    pub max_batch_bytes: u32,
    /// Maximum number of messages in a batch.
    pub max_messages_per_batch: u32,

    // Network limits.
    /// Maximum number of in-flight requests per connection.
    pub max_in_flight_requests: u32,
    /// Maximum size of the network send buffer in bytes.
    pub max_send_buffer_bytes: u32,
    /// Maximum size of the network receive buffer in bytes.
    pub max_recv_buffer_bytes: u32,

    // Storage limits.
    /// Maximum size of a WAL segment in bytes.
    pub max_segment_bytes: u64,
    /// Maximum number of open file descriptors.
    pub max_open_files: u32,
    /// Maximum size of the write buffer in bytes.
    pub max_write_buffer_bytes: u32,

    // Raft limits.
    /// Maximum number of entries in a single append request.
    pub max_raft_entries_per_request: u32,
    /// Maximum size of the Raft log before compaction.
    pub max_raft_log_entries: u64,

    // Consumer limits.
    /// Maximum number of consumer groups per partition.
    pub max_consumer_groups_per_partition: u32,
    /// Maximum number of consumers per group.
    pub max_consumers_per_group: u32,
    /// Maximum lease duration in microseconds.
    pub max_lease_duration_us: u64,

    // Timeout limits (in microseconds).
    /// Default request timeout.
    pub default_timeout_us: u64,
    /// Maximum allowed request timeout.
    pub max_timeout_us: u64,
    /// Heartbeat interval for Raft.
    pub heartbeat_interval_us: u64,
    /// Election timeout minimum.
    pub election_timeout_min_us: u64,
    /// Election timeout maximum.
    pub election_timeout_max_us: u64,
}

impl Limits {
    /// Creates limits with safe defaults.
    ///
    /// These defaults are chosen to be conservative and safe for most
    /// deployments. Production systems should tune these based on their
    /// hardware and workload characteristics.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            // Message limits: 1MB message, 16MB batch, 10k messages/batch.
            max_message_bytes: 1024 * 1024,
            max_batch_bytes: 16 * 1024 * 1024,
            max_messages_per_batch: 10_000,

            // Network: 256 in-flight, 64MB buffers.
            max_in_flight_requests: 256,
            max_send_buffer_bytes: 64 * 1024 * 1024,
            max_recv_buffer_bytes: 64 * 1024 * 1024,

            // Storage: 1GB segments, 1024 open files, 128MB write buffer.
            max_segment_bytes: 1024 * 1024 * 1024,
            max_open_files: 1024,
            max_write_buffer_bytes: 128 * 1024 * 1024,

            // Raft: 1000 entries/request, 1M log entries.
            max_raft_entries_per_request: 1000,
            max_raft_log_entries: 1_000_000,

            // Consumers: 1000 groups, 100 consumers/group, 5min lease.
            max_consumer_groups_per_partition: 1000,
            max_consumers_per_group: 100,
            max_lease_duration_us: 5 * 60 * 1_000_000,

            // Timeouts: 30s default, 5min max, 100ms heartbeat.
            default_timeout_us: 30 * 1_000_000,
            max_timeout_us: 5 * 60 * 1_000_000,
            heartbeat_interval_us: 100 * 1000,
            election_timeout_min_us: 300 * 1000,
            election_timeout_max_us: 500 * 1000,
        }
    }

    /// Validates that all limits are internally consistent.
    ///
    /// # Errors
    /// Returns an error if any limits are invalid or inconsistent.
    pub fn validate(&self) -> crate::Result<()> {
        // Message limits must be positive.
        if self.max_message_bytes == 0 {
            return Err(crate::Error::InvalidArgument {
                name: "max_message_bytes",
                reason: "must be positive",
            });
        }

        // Batch must be at least as large as a single message.
        if self.max_batch_bytes < self.max_message_bytes {
            return Err(crate::Error::InvalidArgument {
                name: "max_batch_bytes",
                reason: "must be >= max_message_bytes",
            });
        }

        // Election timeout must be greater than heartbeat.
        if self.election_timeout_min_us <= self.heartbeat_interval_us {
            return Err(crate::Error::InvalidArgument {
                name: "election_timeout_min_us",
                reason: "must be > heartbeat_interval_us",
            });
        }

        // Election timeout range must be valid.
        if self.election_timeout_max_us < self.election_timeout_min_us {
            return Err(crate::Error::InvalidArgument {
                name: "election_timeout_max_us",
                reason: "must be >= election_timeout_min_us",
            });
        }

        // Default timeout must be within max.
        if self.default_timeout_us > self.max_timeout_us {
            return Err(crate::Error::InvalidArgument {
                name: "default_timeout_us",
                reason: "must be <= max_timeout_us",
            });
        }

        Ok(())
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limits_are_valid() {
        let limits = Limits::new();
        assert!(limits.validate().is_ok());
    }

    #[test]
    fn test_invalid_message_size() {
        let mut limits = Limits::new();
        limits.max_message_bytes = 0;
        assert!(limits.validate().is_err());
    }

    #[test]
    fn test_batch_smaller_than_message() {
        let mut limits = Limits::new();
        limits.max_batch_bytes = 512;
        limits.max_message_bytes = 1024;
        assert!(limits.validate().is_err());
    }

    #[test]
    fn test_election_timeout_less_than_heartbeat() {
        let mut limits = Limits::new();
        limits.election_timeout_min_us = 50 * 1000;
        limits.heartbeat_interval_us = 100 * 1000;
        assert!(limits.validate().is_err());
    }
}
