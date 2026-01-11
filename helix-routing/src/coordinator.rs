//! Transfer coordinator for orchestrating shard movements.
//!
//! The `TransferCoordinator` manages the lifecycle of shard transfers,
//! ensuring orderly progression through the transfer state machine.
//!
//! # Responsibilities
//!
//! - Track active and pending transfers
//! - Enforce concurrency limits
//! - Handle timeouts and retries
//! - Generate output messages for the transport layer

use std::collections::{HashMap, VecDeque};

use helix_core::{GroupId, TransferId};

use crate::shard_map::ShardRange;
use crate::transfer::{
    ShardTransfer, TransferError, TransferMessage, TransferOutput, TransferState,
    MAX_CONCURRENT_TRANSFERS, TRANSFER_TIMEOUT_TICKS,
};

/// Maximum pending transfers waiting to start.
const MAX_PENDING_TRANSFERS: usize = 100;

/// Maximum retry attempts per transfer.
const MAX_RETRY_ATTEMPTS: u32 = 3;

/// Coordinates shard transfer operations.
///
/// The coordinator maintains the state of all transfers and generates
/// output messages for the transport layer to send.
#[derive(Debug)]
pub struct TransferCoordinator {
    /// Active transfers (currently in progress).
    active: HashMap<TransferId, ShardTransfer>,
    /// Pending transfers (waiting to start).
    pending: VecDeque<ShardTransfer>,
    /// Completed transfer IDs (for idempotency checks).
    completed: VecDeque<TransferId>,
    /// Next transfer ID.
    next_id: u64,
    /// Maximum completed IDs to track.
    max_completed_history: usize,
}

impl TransferCoordinator {
    /// Creates a new transfer coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            active: HashMap::new(),
            pending: VecDeque::new(),
            completed: VecDeque::new(),
            next_id: 1,
            max_completed_history: 1000,
        }
    }

    /// Returns the number of active transfers.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    /// Returns the number of pending transfers.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Returns the total number of transfers (active + pending).
    #[must_use]
    pub fn total_count(&self) -> usize {
        self.active.len() + self.pending.len()
    }

    /// Returns true if a transfer with the given ID exists.
    #[must_use]
    pub fn has_transfer(&self, transfer_id: TransferId) -> bool {
        self.active.contains_key(&transfer_id)
            || self.pending.iter().any(|t| t.transfer_id == transfer_id)
    }

    /// Returns a reference to an active transfer.
    #[must_use]
    pub fn get_transfer(&self, transfer_id: TransferId) -> Option<&ShardTransfer> {
        self.active.get(&transfer_id)
    }

    /// Initiates a new shard transfer.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Source and target groups are the same
    /// - Too many pending transfers
    pub fn start_transfer(
        &mut self,
        source: GroupId,
        target: GroupId,
        shard_range: ShardRange,
        current_tick: u64,
    ) -> Result<TransferId, TransferError> {
        // Validate source != target.
        if source == target {
            return Err(TransferError::SameSourceAndTarget(source));
        }

        // Check pending queue isn't full.
        if self.pending.len() >= MAX_PENDING_TRANSFERS {
            return Err(TransferError::TooManyTransfers {
                count: self.pending.len(),
                max: MAX_PENDING_TRANSFERS,
            });
        }

        // Create transfer.
        let transfer_id = TransferId::new(self.next_id);
        self.next_id += 1;

        let transfer = ShardTransfer::new(transfer_id, source, target, shard_range, current_tick);
        self.pending.push_back(transfer);

        Ok(transfer_id)
    }

    /// Cancels a transfer.
    ///
    /// # Errors
    ///
    /// Returns an error if the transfer doesn't exist.
    pub fn cancel_transfer(&mut self, transfer_id: TransferId) -> Result<(), TransferError> {
        // Check active transfers.
        if let Some(mut transfer) = self.active.remove(&transfer_id) {
            transfer.fail("cancelled by coordinator");
            return Ok(());
        }

        // Check pending transfers.
        if let Some(pos) = self.pending.iter().position(|t| t.transfer_id == transfer_id) {
            self.pending.remove(pos);
            return Ok(());
        }

        Err(TransferError::TransferNotFound(transfer_id))
    }

    /// Advances the coordinator state, returning outputs to process.
    ///
    /// This should be called periodically (e.g., every tick) to:
    /// - Start pending transfers
    /// - Check for timeouts
    /// - Clean up completed transfers
    pub fn tick(&mut self, current_tick: u64) -> Vec<TransferOutput> {
        let mut outputs = Vec::new();

        // Start pending transfers if we have capacity.
        self.start_pending_transfers(current_tick, &mut outputs);

        // Check for timeouts.
        self.check_timeouts(current_tick, &mut outputs);

        outputs
    }

    /// Processes a transfer message and returns outputs.
    pub fn handle_message(
        &mut self,
        message: TransferMessage,
        _current_tick: u64,
    ) -> Vec<TransferOutput> {
        let transfer_id = message.transfer_id();
        let mut outputs = Vec::new();

        // Find the transfer.
        let Some(transfer) = self.active.get_mut(&transfer_id) else {
            // Transfer not found - might be cancelled or completed.
            return outputs;
        };

        match message {
            TransferMessage::PrepareResponse {
                snapshot_index,
                snapshot_term,
                snapshot_size,
                ..
            } => {
                if matches!(transfer.state, TransferState::Snapshotting) {
                    transfer.snapshot_complete(snapshot_index, snapshot_term, snapshot_size);

                    // Request first chunk.
                    outputs.push(TransferOutput::SendMessage {
                        to_group: transfer.source_group,
                        message: TransferMessage::SnapshotChunk {
                            transfer_id,
                            offset: 0,
                            data: bytes::Bytes::new(), // Request chunk at offset 0
                            done: false,
                        },
                    });
                }
            }
            TransferMessage::SnapshotChunk {
                offset,
                data,
                done,
                ..
            } => {
                if let TransferState::Transferring { progress: _, total: _ } = transfer.state {
                    let new_progress = offset + data.len() as u64;
                    transfer.update_progress(new_progress);

                    if done {
                        // Snapshot transfer complete, start catchup.
                        // In a real implementation, we'd query the lag from source.
                        transfer.start_catchup(0);
                        transfer.start_switching();

                        outputs.push(TransferOutput::SendMessage {
                            to_group: transfer.target_group,
                            message: TransferMessage::SwitchRequest { transfer_id },
                        });
                    } else {
                        // Request next chunk.
                        outputs.push(TransferOutput::SendMessage {
                            to_group: transfer.source_group,
                            message: TransferMessage::SnapshotChunk {
                                transfer_id,
                                offset: new_progress,
                                data: bytes::Bytes::new(),
                                done: false,
                            },
                        });
                    }
                }
            }
            TransferMessage::CatchupEntries { done, .. } => {
                if let TransferState::CatchingUp { .. } = transfer.state {
                    if done {
                        transfer.update_lag(0);
                        transfer.start_switching();

                        outputs.push(TransferOutput::SendMessage {
                            to_group: transfer.target_group,
                            message: TransferMessage::SwitchRequest { transfer_id },
                        });
                    }
                }
            }
            TransferMessage::SwitchResponse { success, .. } => {
                if matches!(transfer.state, TransferState::Switching) {
                    if success {
                        let shard_range = transfer.shard_range;
                        let from_group = transfer.source_group;
                        let to_group = transfer.target_group;

                        transfer.complete();

                        outputs.push(TransferOutput::UpdateShardMap {
                            transfer_id,
                            shard_range,
                            from_group,
                            to_group,
                        });
                        outputs.push(TransferOutput::TransferComplete { transfer_id });

                        // Move to completed.
                        self.complete_transfer(transfer_id);
                    } else {
                        transfer.fail("switch rejected by target");
                        outputs.push(TransferOutput::TransferFailed {
                            transfer_id,
                            reason: "switch rejected by target".to_string(),
                        });
                    }
                }
            }
            TransferMessage::CancelRequest { reason, .. } => {
                transfer.fail(&reason);
                outputs.push(TransferOutput::TransferFailed {
                    transfer_id,
                    reason,
                });
            }
            // Request messages are handled externally (sent to source/target groups).
            TransferMessage::PrepareRequest { .. } | TransferMessage::SwitchRequest { .. } => {}
        }

        outputs
    }

    /// Returns all active transfers.
    pub fn active_transfers(&self) -> impl Iterator<Item = &ShardTransfer> {
        self.active.values()
    }

    /// Returns all pending transfers.
    pub fn pending_transfers(&self) -> impl Iterator<Item = &ShardTransfer> {
        self.pending.iter()
    }

    /// Starts pending transfers up to the concurrency limit.
    fn start_pending_transfers(&mut self, current_tick: u64, outputs: &mut Vec<TransferOutput>) {
        while self.active.len() < MAX_CONCURRENT_TRANSFERS && !self.pending.is_empty() {
            let Some(mut transfer) = self.pending.pop_front() else {
                break;
            };

            // Start the transfer.
            transfer.start();
            transfer.started_at_tick = current_tick;

            let transfer_id = transfer.transfer_id;
            let source_group = transfer.source_group;
            let shard_range = transfer.shard_range;

            self.active.insert(transfer_id, transfer);

            // Send prepare request to source.
            outputs.push(TransferOutput::SendMessage {
                to_group: source_group,
                message: TransferMessage::PrepareRequest {
                    transfer_id,
                    shard_range,
                },
            });
        }
    }

    /// Checks for timed out transfers.
    fn check_timeouts(&mut self, current_tick: u64, outputs: &mut Vec<TransferOutput>) {
        let timed_out: Vec<TransferId> = self
            .active
            .iter()
            .filter(|(_, t)| t.is_timed_out(current_tick))
            .map(|(id, _)| *id)
            .collect();

        for transfer_id in timed_out {
            if let Some(transfer) = self.active.get_mut(&transfer_id) {
                if transfer.retry_count < MAX_RETRY_ATTEMPTS {
                    // Retry: reset to pending.
                    let mut retry_transfer = transfer.clone();
                    retry_transfer.state = TransferState::Pending;
                    retry_transfer.retry_count += 1;
                    retry_transfer.started_at_tick = current_tick;

                    self.active.remove(&transfer_id);
                    self.pending.push_back(retry_transfer);
                } else {
                    // Max retries exceeded, fail the transfer.
                    transfer.fail("timeout after max retries");
                    outputs.push(TransferOutput::TransferFailed {
                        transfer_id,
                        reason: format!(
                            "timeout after {TRANSFER_TIMEOUT_TICKS} ticks and {MAX_RETRY_ATTEMPTS} retries"
                        ),
                    });
                    self.active.remove(&transfer_id);
                }
            }
        }
    }

    /// Moves a transfer to the completed list.
    fn complete_transfer(&mut self, transfer_id: TransferId) {
        self.active.remove(&transfer_id);
        self.completed.push_back(transfer_id);

        // Trim completed history.
        while self.completed.len() > self.max_completed_history {
            self.completed.pop_front();
        }
    }
}

impl Default for TransferCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_core::LogIndex;
    use helix_core::TermId;

    #[test]
    fn test_coordinator_new() {
        let coord = TransferCoordinator::new();
        assert_eq!(coord.active_count(), 0);
        assert_eq!(coord.pending_count(), 0);
    }

    #[test]
    fn test_start_transfer() {
        let mut coord = TransferCoordinator::new();

        let result = coord.start_transfer(
            GroupId::new(1),
            GroupId::new(2),
            ShardRange::new(0, 1000),
            100,
        );

        assert!(result.is_ok());
        assert_eq!(coord.pending_count(), 1);
    }

    #[test]
    fn test_start_transfer_same_source_target() {
        let mut coord = TransferCoordinator::new();

        let result = coord.start_transfer(
            GroupId::new(1),
            GroupId::new(1),
            ShardRange::new(0, 1000),
            100,
        );

        assert!(matches!(result, Err(TransferError::SameSourceAndTarget(_))));
    }

    #[test]
    fn test_tick_starts_pending() {
        let mut coord = TransferCoordinator::new();

        coord
            .start_transfer(GroupId::new(1), GroupId::new(2), ShardRange::new(0, 1000), 0)
            .unwrap();

        assert_eq!(coord.pending_count(), 1);
        assert_eq!(coord.active_count(), 0);

        let outputs = coord.tick(0);

        assert_eq!(coord.pending_count(), 0);
        assert_eq!(coord.active_count(), 1);
        assert!(!outputs.is_empty());

        // Should have sent PrepareRequest.
        assert!(outputs.iter().any(|o| matches!(
            o,
            TransferOutput::SendMessage {
                message: TransferMessage::PrepareRequest { .. },
                ..
            }
        )));
    }

    #[test]
    fn test_cancel_active_transfer() {
        let mut coord = TransferCoordinator::new();

        let transfer_id = coord
            .start_transfer(GroupId::new(1), GroupId::new(2), ShardRange::new(0, 1000), 0)
            .unwrap();

        coord.tick(0); // Start the transfer

        assert_eq!(coord.active_count(), 1);

        let result = coord.cancel_transfer(transfer_id);
        assert!(result.is_ok());
        assert_eq!(coord.active_count(), 0);
    }

    #[test]
    fn test_cancel_pending_transfer() {
        let mut coord = TransferCoordinator::new();

        let transfer_id = coord
            .start_transfer(GroupId::new(1), GroupId::new(2), ShardRange::new(0, 1000), 0)
            .unwrap();

        assert_eq!(coord.pending_count(), 1);

        let result = coord.cancel_transfer(transfer_id);
        assert!(result.is_ok());
        assert_eq!(coord.pending_count(), 0);
    }

    #[test]
    fn test_handle_prepare_response() {
        let mut coord = TransferCoordinator::new();

        let transfer_id = coord
            .start_transfer(GroupId::new(1), GroupId::new(2), ShardRange::new(0, 1000), 0)
            .unwrap();

        coord.tick(0); // Start the transfer

        let response = TransferMessage::PrepareResponse {
            transfer_id,
            snapshot_index: LogIndex::new(100),
            snapshot_term: TermId::new(5),
            snapshot_size: 10000,
        };

        let outputs = coord.handle_message(response, 10);

        // Should request first chunk.
        assert!(!outputs.is_empty());

        // Transfer should be in Transferring state.
        let transfer = coord.get_transfer(transfer_id).unwrap();
        assert!(matches!(transfer.state, TransferState::Transferring { .. }));
    }

    #[test]
    fn test_concurrency_limit() {
        let mut coord = TransferCoordinator::new();

        // Queue more transfers than the concurrency limit.
        for i in 0..MAX_CONCURRENT_TRANSFERS + 5 {
            coord
                .start_transfer(
                    GroupId::new(1),
                    GroupId::new((i + 2) as u64),
                    ShardRange::new(0, 1000),
                    0,
                )
                .unwrap();
        }

        coord.tick(0);

        // Only MAX_CONCURRENT_TRANSFERS should be active.
        assert_eq!(coord.active_count(), MAX_CONCURRENT_TRANSFERS);
        assert_eq!(coord.pending_count(), 5);
    }
}
