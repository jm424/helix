//! Shard transfer types for moving shard ranges between Raft groups.
//!
//! This module provides the state machine and types for shard transfers,
//! which are used when:
//! - Rebalancing shards across nodes
//! - Adding/removing nodes from the cluster
//! - Moving shards for maintenance
//!
//! # Transfer Protocol
//!
//! A shard transfer goes through these states:
//! 1. **Snapshotting**: Source group creates a snapshot
//! 2. **Transferring**: Snapshot data is streamed to target
//! 3. **`CatchingUp`**: Target applies log entries since snapshot
//! 4. **Switching**: Shard map is atomically updated
//! 5. **Done**: Transfer complete
//!
//! # Design
//!
//! - Transfers are coordinator-driven (not peer-to-peer)
//! - Large snapshots are chunked to avoid memory pressure
//! - Concurrent writes continue during transfer (caught up later)
//! - Atomic shard map updates ensure no request routing gaps

use bytes::Bytes;
use helix_core::{GroupId, LogIndex, TermId, TransferId};

use crate::shard_map::ShardRange;

/// Maximum concurrent transfers per coordinator.
pub const MAX_CONCURRENT_TRANSFERS: usize = 10;

/// Maximum time for a transfer before timeout (in ticks).
pub const TRANSFER_TIMEOUT_TICKS: u64 = 1000;

/// State of a shard transfer operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferState {
    /// Waiting to start (pending resources).
    Pending,
    /// Creating snapshot at source group.
    Snapshotting,
    /// Sending snapshot data to target group.
    Transferring {
        /// Bytes transferred so far.
        progress: u64,
        /// Total bytes to transfer.
        total: u64,
    },
    /// Applying log entries since snapshot to catch up.
    CatchingUp {
        /// Number of entries behind.
        lag: u64,
    },
    /// Atomically switching shard ownership.
    Switching,
    /// Transfer completed successfully.
    Done,
    /// Transfer failed.
    Failed {
        /// Reason for failure.
        reason: String,
    },
}

impl TransferState {
    /// Returns true if the transfer is complete (done or failed).
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Done | Self::Failed { .. })
    }

    /// Returns true if the transfer is actively in progress.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        !matches!(self, Self::Pending | Self::Done | Self::Failed { .. })
    }

    /// Returns a human-readable description of the state.
    #[must_use]
    pub const fn description(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Snapshotting => "creating snapshot",
            Self::Transferring { .. } => "transferring snapshot",
            Self::CatchingUp { .. } => "catching up",
            Self::Switching => "switching ownership",
            Self::Done => "completed",
            Self::Failed { .. } => "failed",
        }
    }
}

impl std::fmt::Display for TransferState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Snapshotting => write!(f, "snapshotting"),
            Self::Transferring { progress, total } => {
                let pct = if *total > 0 {
                    (*progress * 100) / *total
                } else {
                    0
                };
                write!(f, "transferring ({pct}%)")
            }
            Self::CatchingUp { lag } => write!(f, "catching up ({lag} entries behind)"),
            Self::Switching => write!(f, "switching"),
            Self::Done => write!(f, "done"),
            Self::Failed { reason } => write!(f, "failed: {reason}"),
        }
    }
}

/// A shard transfer operation.
///
/// Tracks the state of moving a shard range from one Raft group to another.
#[derive(Debug, Clone)]
pub struct ShardTransfer {
    /// Unique transfer ID.
    pub transfer_id: TransferId,
    /// Source Raft group (currently owns the shard).
    pub source_group: GroupId,
    /// Target Raft group (will own the shard after transfer).
    pub target_group: GroupId,
    /// Shard range being transferred.
    pub shard_range: ShardRange,
    /// Current state of the transfer.
    pub state: TransferState,
    /// Snapshot index (set after snapshotting completes).
    pub snapshot_index: Option<LogIndex>,
    /// Snapshot term (set after snapshotting completes).
    pub snapshot_term: Option<TermId>,
    /// Tick when transfer started (for timeout tracking).
    pub started_at_tick: u64,
    /// Number of retry attempts.
    pub retry_count: u32,
}

impl ShardTransfer {
    /// Creates a new transfer in pending state.
    #[must_use]
    pub const fn new(
        transfer_id: TransferId,
        source_group: GroupId,
        target_group: GroupId,
        shard_range: ShardRange,
        current_tick: u64,
    ) -> Self {
        Self {
            transfer_id,
            source_group,
            target_group,
            shard_range,
            state: TransferState::Pending,
            snapshot_index: None,
            snapshot_term: None,
            started_at_tick: current_tick,
            retry_count: 0,
        }
    }

    /// Starts the transfer (transitions from Pending to Snapshotting).
    ///
    /// # Panics
    ///
    /// Panics if the transfer is not in the `Pending` state.
    pub fn start(&mut self) {
        assert!(
            matches!(self.state, TransferState::Pending),
            "can only start a pending transfer"
        );
        self.state = TransferState::Snapshotting;
    }

    /// Records that snapshot creation is complete.
    ///
    /// # Panics
    ///
    /// Panics if the transfer is not in the `Snapshotting` state.
    pub fn snapshot_complete(&mut self, index: LogIndex, term: TermId, total_size: u64) {
        assert!(
            matches!(self.state, TransferState::Snapshotting),
            "can only complete snapshot while snapshotting"
        );
        self.snapshot_index = Some(index);
        self.snapshot_term = Some(term);
        self.state = TransferState::Transferring {
            progress: 0,
            total: total_size,
        };
    }

    /// Updates transfer progress.
    pub fn update_progress(&mut self, bytes_transferred: u64) {
        if let TransferState::Transferring { progress, total } = &mut self.state {
            *progress = bytes_transferred.min(*total);
        }
    }

    /// Transitions to catching up after snapshot transfer completes.
    ///
    /// # Panics
    ///
    /// Panics if the transfer is not in the `Transferring` state.
    pub fn start_catchup(&mut self, lag: u64) {
        assert!(
            matches!(self.state, TransferState::Transferring { .. }),
            "can only start catchup after transferring"
        );
        self.state = TransferState::CatchingUp { lag };
    }

    /// Updates the catchup lag.
    pub fn update_lag(&mut self, new_lag: u64) {
        if let TransferState::CatchingUp { lag } = &mut self.state {
            *lag = new_lag;
        }
    }

    /// Transitions to switching after catchup completes.
    ///
    /// # Panics
    ///
    /// Panics if the transfer is not in the `CatchingUp` state with lag of 0.
    pub fn start_switching(&mut self) {
        assert!(
            matches!(self.state, TransferState::CatchingUp { lag } if lag == 0),
            "can only switch when fully caught up"
        );
        self.state = TransferState::Switching;
    }

    /// Marks the transfer as complete.
    ///
    /// # Panics
    ///
    /// Panics if the transfer is not in the `Switching` state.
    pub fn complete(&mut self) {
        assert!(
            matches!(self.state, TransferState::Switching),
            "can only complete while switching"
        );
        self.state = TransferState::Done;
    }

    /// Marks the transfer as failed.
    pub fn fail(&mut self, reason: impl Into<String>) {
        self.state = TransferState::Failed {
            reason: reason.into(),
        };
    }

    /// Checks if the transfer has timed out.
    #[must_use]
    pub const fn is_timed_out(&self, current_tick: u64) -> bool {
        current_tick.saturating_sub(self.started_at_tick) > TRANSFER_TIMEOUT_TICKS
    }

    /// Returns the duration of the transfer in ticks.
    #[must_use]
    pub const fn duration_ticks(&self, current_tick: u64) -> u64 {
        current_tick.saturating_sub(self.started_at_tick)
    }
}

/// Messages for the shard transfer protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferMessage {
    /// Request source to create a snapshot for transfer.
    PrepareRequest {
        /// Transfer ID.
        transfer_id: TransferId,
        /// Shard range being transferred.
        shard_range: ShardRange,
    },
    /// Response indicating snapshot is ready.
    PrepareResponse {
        /// Transfer ID.
        transfer_id: TransferId,
        /// Last index included in snapshot.
        snapshot_index: LogIndex,
        /// Term of last index.
        snapshot_term: TermId,
        /// Total snapshot size in bytes.
        snapshot_size: u64,
    },
    /// A chunk of snapshot data.
    SnapshotChunk {
        /// Transfer ID.
        transfer_id: TransferId,
        /// Byte offset within the snapshot.
        offset: u64,
        /// Chunk data.
        data: Bytes,
        /// Whether this is the final chunk.
        done: bool,
    },
    /// Log entries for catchup after snapshot.
    CatchupEntries {
        /// Transfer ID.
        transfer_id: TransferId,
        /// Start index of entries.
        start_index: LogIndex,
        /// Number of entries included.
        entry_count: u32,
        /// Serialized entries.
        entries_data: Bytes,
        /// Whether catchup is complete.
        done: bool,
    },
    /// Signal to complete the transfer and switch ownership.
    SwitchRequest {
        /// Transfer ID.
        transfer_id: TransferId,
    },
    /// Acknowledgment that switch is complete.
    SwitchResponse {
        /// Transfer ID.
        transfer_id: TransferId,
        /// Whether the switch succeeded.
        success: bool,
    },
    /// Cancel a transfer.
    CancelRequest {
        /// Transfer ID.
        transfer_id: TransferId,
        /// Reason for cancellation.
        reason: String,
    },
}

impl TransferMessage {
    /// Returns the transfer ID for this message.
    #[must_use]
    pub const fn transfer_id(&self) -> TransferId {
        match self {
            Self::PrepareRequest { transfer_id, .. }
            | Self::PrepareResponse { transfer_id, .. }
            | Self::SnapshotChunk { transfer_id, .. }
            | Self::CatchupEntries { transfer_id, .. }
            | Self::SwitchRequest { transfer_id }
            | Self::SwitchResponse { transfer_id, .. }
            | Self::CancelRequest { transfer_id, .. } => *transfer_id,
        }
    }
}

/// Output actions from transfer operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferOutput {
    /// Send a transfer message to a group.
    SendMessage {
        /// Target group.
        to_group: GroupId,
        /// The message to send.
        message: TransferMessage,
    },
    /// Update the shard map (transfer complete).
    UpdateShardMap {
        /// Transfer ID for tracking.
        transfer_id: TransferId,
        /// Shard range that was transferred.
        shard_range: ShardRange,
        /// Old owner.
        from_group: GroupId,
        /// New owner.
        to_group: GroupId,
    },
    /// Notify that a transfer completed.
    TransferComplete {
        /// Transfer ID.
        transfer_id: TransferId,
    },
    /// Notify that a transfer failed.
    TransferFailed {
        /// Transfer ID.
        transfer_id: TransferId,
        /// Failure reason.
        reason: String,
    },
}

/// Errors from transfer operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferError {
    /// Transfer already exists.
    TransferExists(TransferId),
    /// Transfer not found.
    TransferNotFound(TransferId),
    /// Too many concurrent transfers.
    TooManyTransfers {
        /// Current count.
        count: usize,
        /// Maximum allowed.
        max: usize,
    },
    /// Invalid state transition.
    InvalidStateTransition {
        /// Current state description.
        from: String,
        /// Attempted transition.
        to: String,
    },
    /// Source and target groups are the same.
    SameSourceAndTarget(GroupId),
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TransferExists(id) => write!(f, "transfer {} already exists", id.get()),
            Self::TransferNotFound(id) => write!(f, "transfer {} not found", id.get()),
            Self::TooManyTransfers { count, max } => {
                write!(f, "too many transfers: {count} (max {max})")
            }
            Self::InvalidStateTransition { from, to } => {
                write!(f, "invalid state transition from {from} to {to}")
            }
            Self::SameSourceAndTarget(group) => {
                write!(f, "source and target are the same group: {}", group.get())
            }
        }
    }
}

impl std::error::Error for TransferError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_transfer() -> ShardTransfer {
        ShardTransfer::new(
            TransferId::new(1),
            GroupId::new(1),
            GroupId::new(2),
            ShardRange::new(0, 1000),
            100,
        )
    }

    #[test]
    fn test_transfer_state_transitions() {
        let mut transfer = make_transfer();

        assert!(matches!(transfer.state, TransferState::Pending));
        assert!(!transfer.state.is_active());

        transfer.start();
        assert!(matches!(transfer.state, TransferState::Snapshotting));
        assert!(transfer.state.is_active());

        transfer.snapshot_complete(LogIndex::new(100), TermId::new(5), 10000);
        assert!(matches!(transfer.state, TransferState::Transferring { .. }));

        transfer.update_progress(5000);
        if let TransferState::Transferring { progress, total } = &transfer.state {
            assert_eq!(*progress, 5000);
            assert_eq!(*total, 10000);
        }

        transfer.start_catchup(50);
        assert!(matches!(transfer.state, TransferState::CatchingUp { lag: 50 }));

        transfer.update_lag(0);
        transfer.start_switching();
        assert!(matches!(transfer.state, TransferState::Switching));

        transfer.complete();
        assert!(matches!(transfer.state, TransferState::Done));
        assert!(transfer.state.is_terminal());
    }

    #[test]
    fn test_transfer_failure() {
        let mut transfer = make_transfer();
        transfer.start();
        transfer.fail("network error");

        assert!(matches!(transfer.state, TransferState::Failed { .. }));
        assert!(transfer.state.is_terminal());
    }

    #[test]
    fn test_transfer_timeout() {
        let transfer = make_transfer();

        // Not timed out at start + small delta.
        assert!(!transfer.is_timed_out(200));

        // Timed out after TRANSFER_TIMEOUT_TICKS.
        assert!(transfer.is_timed_out(100 + TRANSFER_TIMEOUT_TICKS + 1));
    }

    #[test]
    fn test_transfer_message_id() {
        let msg = TransferMessage::PrepareRequest {
            transfer_id: TransferId::new(42),
            shard_range: ShardRange::new(0, 100),
        };

        assert_eq!(msg.transfer_id().get(), 42);
    }

    #[test]
    fn test_transfer_state_display() {
        assert_eq!(TransferState::Pending.to_string(), "pending");
        assert_eq!(TransferState::Snapshotting.to_string(), "snapshotting");
        assert_eq!(
            TransferState::Transferring {
                progress: 50,
                total: 100
            }
            .to_string(),
            "transferring (50%)"
        );
        assert_eq!(
            TransferState::CatchingUp { lag: 10 }.to_string(),
            "catching up (10 entries behind)"
        );
        assert_eq!(TransferState::Done.to_string(), "done");
    }
}
