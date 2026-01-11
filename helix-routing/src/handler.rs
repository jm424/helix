//! Transfer handler trait for storage layer integration.
//!
//! This module defines the trait that must be implemented by the storage layer
//! to support shard transfers. The `TransferHandler` provides the operations
//! needed during each phase of a transfer:
//!
//! - **Snapshotting**: Create a snapshot of the source group
//! - **Transferring**: Read chunks of snapshot data
//! - **Installing**: Apply snapshot at the target group
//! - **Switching**: Atomically update shard ownership
//!
//! # Implementation Notes
//!
//! The handler is invoked by the transfer coordinator. Implementations should
//! delegate to the underlying Raft group and storage systems.

use bytes::Bytes;
use helix_core::{GroupId, LogIndex, TermId};

use crate::shard_map::ShardRange;

/// Unique identifier for a snapshot within a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotId(u64);

impl SnapshotId {
    /// Creates a new snapshot ID.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the inner ID value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "snapshot-{}", self.0)
    }
}

/// Metadata about a created snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// Unique identifier for this snapshot.
    pub snapshot_id: SnapshotId,
    /// Last log index included in the snapshot.
    pub last_index: LogIndex,
    /// Term of the last included entry.
    pub last_term: TermId,
    /// Total size of snapshot data in bytes.
    pub size_bytes: u64,
}

/// A chunk of snapshot data.
#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    /// Byte offset within the snapshot.
    pub offset: u64,
    /// Chunk data.
    pub data: Bytes,
    /// Whether this is the final chunk.
    pub done: bool,
}

impl SnapshotChunk {
    /// Creates a new snapshot chunk.
    #[must_use]
    pub const fn new(offset: u64, data: Bytes, done: bool) -> Self {
        Self { offset, data, done }
    }

    /// Returns the size of this chunk in bytes.
    #[must_use]
    pub const fn size(&self) -> usize {
        self.data.len()
    }
}

/// Errors that can occur during transfer handler operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerError {
    /// The specified group was not found.
    GroupNotFound(GroupId),
    /// The specified snapshot was not found.
    SnapshotNotFound(SnapshotId),
    /// The node is not the leader of the group.
    NotLeader {
        /// The group ID.
        group_id: GroupId,
        /// Known leader, if any.
        leader_hint: Option<u64>,
    },
    /// Snapshot creation failed.
    SnapshotCreationFailed {
        /// The group ID.
        group_id: GroupId,
        /// Error details.
        reason: String,
    },
    /// Snapshot installation failed.
    SnapshotInstallFailed {
        /// The group ID.
        group_id: GroupId,
        /// Error details.
        reason: String,
    },
    /// Invalid chunk offset.
    InvalidChunkOffset {
        /// Requested offset.
        offset: u64,
        /// Snapshot size.
        snapshot_size: u64,
    },
    /// Shard range not found in group.
    ShardRangeNotFound {
        /// The group ID.
        group_id: GroupId,
        /// The shard range.
        range: ShardRange,
    },
    /// General I/O error.
    IoError(String),
}

impl std::fmt::Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GroupNotFound(id) => write!(f, "group {} not found", id.get()),
            Self::SnapshotNotFound(id) => write!(f, "snapshot {id} not found"),
            Self::NotLeader { group_id, leader_hint } => {
                write!(f, "not leader of group {}", group_id.get())?;
                if let Some(leader) = leader_hint {
                    write!(f, " (leader hint: {leader})")?;
                }
                Ok(())
            }
            Self::SnapshotCreationFailed { group_id, reason } => {
                write!(f, "snapshot creation failed for group {}: {}", group_id.get(), reason)
            }
            Self::SnapshotInstallFailed { group_id, reason } => {
                write!(f, "snapshot install failed for group {}: {}", group_id.get(), reason)
            }
            Self::InvalidChunkOffset { offset, snapshot_size } => {
                write!(f, "invalid chunk offset {offset} (snapshot size: {snapshot_size})")
            }
            Self::ShardRangeNotFound { group_id, range } => {
                write!(f, "shard range {}-{} not found in group {}",
                       range.start, range.end, group_id.get())
            }
            Self::IoError(msg) => write!(f, "I/O error: {msg}"),
        }
    }
}

impl std::error::Error for HandlerError {}

/// Trait for handling transfer protocol operations.
///
/// This trait is implemented by the storage/server layer to provide the
/// actual operations needed during shard transfers. The transfer coordinator
/// uses this interface to interact with Raft groups.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` as the coordinator may be used
/// from multiple threads.
///
/// # Example
///
/// ```ignore
/// struct MyHandler {
///     multi_raft: Arc<Mutex<MultiRaft>>,
///     snapshots: Arc<Mutex<HashMap<SnapshotId, SnapshotData>>>,
/// }
///
/// impl TransferHandler for MyHandler {
///     fn create_snapshot(&self, group_id: GroupId, _range: ShardRange)
///         -> Result<SnapshotInfo, HandlerError> {
///         let mut raft = self.multi_raft.lock().unwrap();
///         // Create snapshot from group...
///     }
///     // ...
/// }
/// ```
pub trait TransferHandler: Send + Sync {
    /// Creates a snapshot for the specified group and shard range.
    ///
    /// This is called during the `Snapshotting` phase. The implementation should:
    /// 1. Ensure the group exists and this node is the leader
    /// 2. Create a snapshot of the state machine data for the shard range
    /// 3. Store the snapshot for subsequent chunk reads
    /// 4. Return metadata about the created snapshot
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The group doesn't exist
    /// - This node is not the leader
    /// - Snapshot creation fails
    fn create_snapshot(
        &self,
        group_id: GroupId,
        shard_range: ShardRange,
    ) -> Result<SnapshotInfo, HandlerError>;

    /// Reads a chunk from an existing snapshot.
    ///
    /// This is called during the `Transferring` phase to stream snapshot data.
    /// The implementation should read `max_size` bytes starting at `offset`.
    ///
    /// # Arguments
    ///
    /// * `group_id` - The Raft group that owns the snapshot
    /// * `snapshot_id` - ID of the snapshot to read from
    /// * `offset` - Byte offset to start reading from
    /// * `max_size` - Maximum number of bytes to return
    ///
    /// # Returns
    ///
    /// A chunk containing the data and whether this is the final chunk.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The snapshot doesn't exist (expired or invalid ID)
    /// - The offset is beyond the snapshot size
    fn read_snapshot_chunk(
        &self,
        group_id: GroupId,
        snapshot_id: SnapshotId,
        offset: u64,
        max_size: u32,
    ) -> Result<SnapshotChunk, HandlerError>;

    /// Installs a snapshot on the target group.
    ///
    /// This is called at the target group after all snapshot data has been
    /// transferred. The implementation should:
    /// 1. Validate the snapshot data (checksum, etc.)
    /// 2. Apply the snapshot to the Raft group's state machine
    /// 3. Update the Raft log to reflect the snapshot
    ///
    /// # Arguments
    ///
    /// * `group_id` - The target Raft group
    /// * `last_index` - The last log index covered by the snapshot
    /// * `last_term` - The term of the last entry
    /// * `data` - The complete snapshot data
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The group doesn't exist
    /// - The snapshot is corrupt or invalid
    /// - Installation fails for any reason
    fn install_snapshot(
        &self,
        group_id: GroupId,
        last_index: LogIndex,
        last_term: TermId,
        data: Bytes,
    ) -> Result<(), HandlerError>;

    /// Completes the ownership switch for a shard range.
    ///
    /// This is called during the `Switching` phase after the target group
    /// has successfully installed the snapshot and caught up. The implementation
    /// should atomically update the shard ownership.
    ///
    /// # Arguments
    ///
    /// * `group_id` - The target group (new owner)
    /// * `shard_range` - The shard range being transferred
    ///
    /// # Errors
    ///
    /// Returns an error if the switch cannot be completed.
    fn complete_switch(
        &self,
        group_id: GroupId,
        shard_range: ShardRange,
    ) -> Result<(), HandlerError>;

    /// Cleans up a snapshot after transfer completes or fails.
    ///
    /// This is called to release resources associated with a snapshot.
    /// Implementations should handle the case where the snapshot doesn't
    /// exist (already cleaned up or never created).
    ///
    /// # Arguments
    ///
    /// * `group_id` - The group that owns the snapshot
    /// * `snapshot_id` - The snapshot to clean up
    fn cleanup_snapshot(&self, group_id: GroupId, snapshot_id: SnapshotId);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_id_display() {
        let id = SnapshotId::new(42);
        assert_eq!(id.to_string(), "snapshot-42");
        assert_eq!(id.get(), 42);
    }

    #[test]
    fn test_snapshot_chunk() {
        let chunk = SnapshotChunk::new(100, Bytes::from("test data"), false);
        assert_eq!(chunk.offset, 100);
        assert_eq!(chunk.size(), 9);
        assert!(!chunk.done);
    }

    #[test]
    fn test_handler_error_display() {
        let err = HandlerError::GroupNotFound(GroupId::new(5));
        assert_eq!(err.to_string(), "group 5 not found");

        let err = HandlerError::NotLeader {
            group_id: GroupId::new(3),
            leader_hint: Some(7),
        };
        assert_eq!(err.to_string(), "not leader of group 3 (leader hint: 7)");

        let err = HandlerError::InvalidChunkOffset {
            offset: 5000,
            snapshot_size: 1000,
        };
        assert_eq!(err.to_string(), "invalid chunk offset 5000 (snapshot size: 1000)");
    }

    #[test]
    fn test_snapshot_info() {
        let info = SnapshotInfo {
            snapshot_id: SnapshotId::new(1),
            last_index: LogIndex::new(100),
            last_term: TermId::new(5),
            size_bytes: 10000,
        };

        assert_eq!(info.snapshot_id.get(), 1);
        assert_eq!(info.last_index.get(), 100);
        assert_eq!(info.last_term.get(), 5);
        assert_eq!(info.size_bytes, 10000);
    }
}
