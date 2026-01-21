//! Output Processor for actor-based multi-partition coordination.
//!
//! `OutputProcessor` processes outputs from all partition actors via a shared
//! channel. This enables a single task to handle:
//!
//! - Sending Raft messages via transport
//! - Applying committed entries to partition storage
//! - Notifying batch pending proposal waiters
//! - Persisting vote state changes
//!
//! # Design
//!
//! ```text
//! PartitionActor(0) ──┐
//!                     │     ┌──────────────────────┐
//! PartitionActor(1) ──┼────►│ Shared Output Channel │
//!                     │     └──────────────────────┘
//! PartitionActor(2) ──┘               │
//!                                     ▼
//!                          ┌──────────────────────┐
//!                          │   Output Processor   │
//!                          │  ┌────────────────┐  │
//!                          │  │ SendMessages   │──┼──► Transport
//!                          │  │ EntryCommitted │──┼──► PartitionStorage
//!                          │  │ BecameLeader   │──┼──► (logging)
//!                          │  │ SteppedDown    │──┼──► (logging)
//!                          │  │ VoteStateChanged│─┼──► VoteStore (future)
//!                          │  └────────────────┘  │
//!                          └──────────────────────┘
//! ```
//!
//! # Benefits
//!
//! - Single point for transport message batching (future optimization)
//! - Centralized commit processing with storage access
//! - Decoupled from partition actor lifecycle

use std::collections::HashMap;
use std::sync::Arc;

use helix_core::{GroupId, LogIndex, Offset};
use helix_runtime::TransportHandle;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

use crate::error::ServerError;
use crate::group_map::GroupMap;
use crate::partition_storage::ServerPartitionStorage;

use super::batcher::BackpressureState;
use super::partition_actor::{BatchNotifyInfo, GroupedOutput, PartitionOutput};
use super::{BatchPendingProposal, BatcherStats};

/// Configuration for the output processor.
#[derive(Debug, Clone, Copy)]
pub struct OutputProcessorConfig {
    /// Output channel buffer size.
    pub channel_buffer_size: usize,
}

impl Default for OutputProcessorConfig {
    fn default() -> Self {
        Self {
            channel_buffer_size: 10000,
        }
    }
}

/// Creates a shared output channel for partition actors.
///
/// Returns a (sender, receiver) pair where:
/// - `sender` is cloned and passed to each partition actor
/// - `receiver` is passed to `output_processor_task`
#[must_use]
pub fn create_output_channel(
    config: OutputProcessorConfig,
) -> (mpsc::Sender<GroupedOutput>, mpsc::Receiver<GroupedOutput>) {
    mpsc::channel(config.channel_buffer_size)
}

/// Background task that processes outputs from all partition actors.
///
/// This task receives `GroupedOutput` from the shared channel and:
/// 1. Sends Raft messages via transport
/// 2. Applies committed entries to partition storage
/// 3. Notifies batch pending proposal waiters with calculated offsets
/// 4. Logs leadership changes
/// 5. (Future) Persists vote state changes
///
/// # Arguments
///
/// * `output_rx` - Shared channel receiver for partition outputs
/// * `partition_storage` - Map of group ID to partition storage
/// * `group_map` - Mapping from group ID to (topic, partition)
/// * `batch_pending_proposals` - Pending batch proposals waiting for commit
/// * `transport_handle` - Transport for sending Raft messages (optional)
/// * `batcher_stats` - Stats for batch commit tracking (optional)
/// * `batcher_backpressure` - Backpressure state for decrementing counters (optional)
#[allow(clippy::too_many_arguments, clippy::implicit_hasher)]
pub async fn output_processor_task(
    mut output_rx: mpsc::Receiver<GroupedOutput>,
    partition_storage: Arc<RwLock<HashMap<GroupId, ServerPartitionStorage>>>,
    group_map: Arc<RwLock<GroupMap>>,
    batch_pending_proposals: Arc<RwLock<HashMap<GroupId, HashMap<LogIndex, BatchPendingProposal>>>>,
    transport_handle: Option<TransportHandle>,
    batcher_stats: Option<Arc<BatcherStats>>,
    batcher_backpressure: Option<Arc<BackpressureState>>,
) {
    info!("Output processor started");

    while let Some(grouped) = output_rx.recv().await {
        let GroupedOutput { group_id, output } = grouped;

        match output {
            PartitionOutput::SendMessages { to, messages } => {
                handle_send_messages(to, &messages, transport_handle.as_ref()).await;
            }
            PartitionOutput::EntryCommitted {
                index,
                data,
                batch_notify,
            } => {
                info!(
                    group = group_id.get(),
                    index = index.get(),
                    batch_notify_present = batch_notify.is_some(),
                    "Output processor received EntryCommitted"
                );
                handle_entry_committed(
                    group_id,
                    index,
                    &data,
                    batch_notify,
                    &partition_storage,
                    &group_map,
                    &batch_pending_proposals,
                    batcher_stats.as_ref(),
                    batcher_backpressure.as_ref(),
                )
                .await;
            }
            PartitionOutput::BecameLeader => {
                handle_became_leader(group_id, &group_map).await;
            }
            PartitionOutput::SteppedDown => {
                handle_stepped_down(group_id, &group_map).await;
            }
            PartitionOutput::VoteStateChanged { term, voted_for } => {
                handle_vote_state_changed(group_id, term, voted_for);
            }
        }
    }

    info!("Output processor stopped");
}

/// Handles `SendMessages` output by sending via transport.
async fn handle_send_messages(
    to: helix_core::NodeId,
    messages: &[helix_raft::multi::GroupMessage],
    transport_handle: Option<&TransportHandle>,
) {
    let Some(transport) = transport_handle else {
        debug!(
            to = to.get(),
            count = messages.len(),
            "Would send messages (no transport configured)"
        );
        return;
    };

    if let Err(e) = transport.send_batch(to, messages.to_vec()).await {
        debug!(
            to = to.get(),
            count = messages.len(),
            error = %e,
            "Failed to send messages to peer (will retry)"
        );
    } else {
        debug!(
            to = to.get(),
            count = messages.len(),
            "Sent messages to peer"
        );
    }
}

/// Handles `EntryCommitted` output by applying to storage and notifying waiters.
///
/// The `batch_notify` parameter is provided by the partition actor when it owns
/// the pending proposal (new actor model path). When `None`, falls back to the
/// shared `batch_pending_proposals` map (backwards compatibility path).
#[allow(clippy::too_many_arguments, clippy::too_many_lines, clippy::significant_drop_tightening)]
async fn handle_entry_committed(
    group_id: GroupId,
    index: LogIndex,
    data: &bytes::Bytes,
    batch_notify: Option<BatchNotifyInfo>,
    partition_storage: &Arc<RwLock<HashMap<GroupId, ServerPartitionStorage>>>,
    group_map: &Arc<RwLock<GroupMap>>,
    batch_pending_proposals: &Arc<RwLock<HashMap<GroupId, HashMap<LogIndex, BatchPendingProposal>>>>,
    batcher_stats: Option<&Arc<BatcherStats>>,
    batcher_backpressure: Option<&Arc<BackpressureState>>,
) {
    // Get topic/partition info for logging.
    let key = {
        let gm = group_map.read().await;
        gm.get_key(group_id)
    };

    let Some((topic_id, partition_id)) = key else {
        warn!(
            group = group_id.get(),
            index = index.get(),
            "EntryCommitted for unknown group"
        );
        return;
    };

    // Use batch_notify from partition actor if present (new actor model path).
    // Otherwise fall back to shared batch_pending_proposals map (backwards compat).
    let batch_proposal = if let Some(notify_info) = batch_notify {
        // Convert BatchNotifyInfo to BatchPendingProposal for unified handling.
        Some(BatchPendingProposal {
            log_index: index,
            first_request_at: notify_info.first_request_at,
            proposed_at: notify_info.proposed_at,
            batch_size: notify_info.batch_size,
            batch_bytes: notify_info.batch_bytes,
            total_records: notify_info.total_records,
            record_counts: notify_info.record_counts,
            result_txs: notify_info.result_txs,
        })
    } else {
        // Fall back to shared map lookup.
        let mut batch_proposals = batch_pending_proposals.write().await;
        batch_proposals
            .get_mut(&group_id)
            .and_then(|m| m.remove(&index))
    };

    if let Some(batch_proposal) = batch_proposal {
        // Batch proposal path: get base offset BEFORE apply.
        let base_offset = {
            let storage = partition_storage.read().await;
            storage
                .get(&group_id)
                .map_or(Offset::new(0), ServerPartitionStorage::blob_log_end_offset)
        };

        // Apply the batch entry.
        let apply_result = {
            let mut storage = partition_storage.write().await;
            if let Some(ps) = storage.get_mut(&group_id) {
                ps.apply_entry_async(index, data).await
            } else {
                Err(ServerError::PartitionNotFound {
                    topic: topic_id.get().to_string(),
                    partition: i32::try_from(partition_id.get()).unwrap_or(0),
                })
            }
        };

        // Notify each waiter with calculated offset.
        let batch_size = batch_proposal.record_counts.len();
        match apply_result {
            Ok(_) => {
                let commit_latency = batch_proposal.proposed_at.elapsed();
                let total_age = batch_proposal.first_request_at.elapsed();
                let batch_wait = batch_proposal
                    .proposed_at
                    .duration_since(batch_proposal.first_request_at);

                // Record stats.
                if let Some(stats) = batcher_stats {
                    // Safety: durations will never exceed u64::MAX microseconds.
                    #[allow(clippy::cast_possible_truncation)]
                    stats.record_commit(
                        u64::from(batch_proposal.batch_size),
                        u64::from(batch_proposal.batch_bytes),
                        batch_proposal.total_records,
                        batch_wait.as_micros() as u64,
                        commit_latency.as_micros() as u64,
                        total_age.as_micros() as u64,
                    );
                }

                // Notify each waiter with their calculated offset.
                let mut cumulative = 0u64;
                for (record_count, result_tx) in batch_proposal
                    .record_counts
                    .iter()
                    .zip(batch_proposal.result_txs)
                {
                    let offset = Offset::new(base_offset.get() + cumulative);
                    let _ = result_tx.send(Ok(offset));
                    cumulative += u64::from(*record_count);
                }

                // Decrement backpressure counters now that requests are complete.
                if let Some(bp) = batcher_backpressure {
                    bp.pending_requests.fetch_sub(
                        u64::from(batch_proposal.batch_size),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    bp.pending_bytes.fetch_sub(
                        u64::from(batch_proposal.batch_bytes),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }

                // Safety: durations will never exceed u64::MAX microseconds.
                #[allow(clippy::cast_possible_truncation)]
                {
                    info!(
                        group = group_id.get(),
                        index = index.get(),
                        batch_size = batch_proposal.batch_size,
                        batch_bytes = batch_proposal.batch_bytes,
                        total_records = batch_proposal.total_records,
                        batch_wait_us = batch_wait.as_micros() as u64,
                        commit_latency_us = commit_latency.as_micros() as u64,
                        total_age_us = total_age.as_micros() as u64,
                        "Batch committed (actor mode)"
                    );
                }
                debug!(
                    group = group_id.get(),
                    index = index.get(),
                    batch_size,
                    base_offset = base_offset.get(),
                    "Notified batch pending proposal (actor mode)"
                );
            }
            Err(ref e) => {
                // Decrement backpressure counters now that requests are complete (failed).
                if let Some(bp) = batcher_backpressure {
                    bp.pending_requests.fetch_sub(
                        u64::from(batch_proposal.batch_size),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    bp.pending_bytes.fetch_sub(
                        u64::from(batch_proposal.batch_bytes),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                if let Some(stats) = batcher_stats {
                    stats.record_apply_error();
                }
                let err = ServerError::Internal {
                    message: format!("batch apply failed: {e}"),
                };
                for result_tx in batch_proposal.result_txs {
                    let _ = result_tx.send(Err(err.clone()));
                }
                warn!(
                    topic = topic_id.get(),
                    partition = partition_id.get(),
                    error = %e,
                    "Failed to apply batch committed entry (actor mode)"
                );
            }
        }
    } else {
        // No batch proposal found - just apply the entry.
        // This can happen for:
        // 1. Entries replicated from leader (follower commits)
        // 2. Entries proposed via non-batcher path
        let apply_result = {
            let mut storage = partition_storage.write().await;
            if let Some(ps) = storage.get_mut(&group_id) {
                ps.apply_entry_async(index, data).await
            } else {
                Err(ServerError::PartitionNotFound {
                    topic: topic_id.get().to_string(),
                    partition: i32::try_from(partition_id.get()).unwrap_or(0),
                })
            }
        };

        if let Err(e) = apply_result {
            warn!(
                topic = topic_id.get(),
                partition = partition_id.get(),
                index = index.get(),
                error = %e,
                "Failed to apply committed entry (actor mode)"
            );
        } else {
            debug!(
                topic = topic_id.get(),
                partition = partition_id.get(),
                index = index.get(),
                "Applied committed entry (actor mode)"
            );
        }
    }
}

/// Handles `BecameLeader` output (logging only).
async fn handle_became_leader(group_id: GroupId, group_map: &Arc<RwLock<GroupMap>>) {
    let key = {
        let gm = group_map.read().await;
        gm.get_key(group_id)
    };

    if let Some((topic_id, partition_id)) = key {
        info!(
            topic = topic_id.get(),
            partition = partition_id.get(),
            group = group_id.get(),
            "Became leader (actor mode)"
        );
    } else {
        info!(
            group = group_id.get(),
            "Became leader for unknown group (actor mode)"
        );
    }
}

/// Handles `SteppedDown` output (logging only).
async fn handle_stepped_down(group_id: GroupId, group_map: &Arc<RwLock<GroupMap>>) {
    let key = {
        let gm = group_map.read().await;
        gm.get_key(group_id)
    };

    if let Some((topic_id, partition_id)) = key {
        info!(
            topic = topic_id.get(),
            partition = partition_id.get(),
            group = group_id.get(),
            "Stepped down from leader (actor mode)"
        );
    } else {
        info!(
            group = group_id.get(),
            "Stepped down from leader for unknown group (actor mode)"
        );
    }
}

/// Handles `VoteStateChanged` output.
///
/// Currently just logs the change. Full `VoteStore` integration is future work.
fn handle_vote_state_changed(
    group_id: GroupId,
    term: u64,
    voted_for: Option<helix_core::NodeId>,
) {
    debug!(
        group = group_id.get(),
        term = term,
        voted_for = ?voted_for.map(helix_core::NodeId::get),
        "Vote state changed (actor mode, persistence not yet implemented)"
    );
    // TODO: Integrate with VoteStore for persistence.
    // The VoteStore will persist to local file + async S3 backup.
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use helix_core::{PartitionId, TopicId};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn test_output_channel_creation() {
        let config = OutputProcessorConfig::default();
        let (tx, _rx) = create_output_channel(config);

        // Should be able to send.
        let output = GroupedOutput {
            group_id: GroupId::new(1),
            output: PartitionOutput::BecameLeader,
        };
        assert!(tx.send(output).await.is_ok());
    }

    #[tokio::test]
    async fn test_output_processor_became_leader() {
        let config = OutputProcessorConfig::default();
        let (tx, rx) = create_output_channel(config);

        let partition_storage = Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let batch_pending_proposals = Arc::new(RwLock::new(HashMap::new()));

        // Add a mapping.
        {
            let mut gm = group_map.write().await;
            gm.insert(TopicId::new(1), PartitionId::new(0), GroupId::new(1));
        }

        // Spawn processor.
        let processor = tokio::spawn(output_processor_task(
            rx,
            partition_storage,
            group_map,
            batch_pending_proposals,
            None,
            None,
            None,
        ));

        // Send BecameLeader output.
        let output = GroupedOutput {
            group_id: GroupId::new(1),
            output: PartitionOutput::BecameLeader,
        };
        tx.send(output).await.unwrap();

        // Drop sender to stop processor.
        drop(tx);
        let _ = processor.await;
    }

    #[tokio::test]
    async fn test_output_processor_entry_committed_no_proposal() {
        let config = OutputProcessorConfig::default();
        let (tx, rx) = create_output_channel(config);

        let partition_storage = Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let batch_pending_proposals = Arc::new(RwLock::new(HashMap::new()));

        // Add a mapping.
        {
            let mut gm = group_map.write().await;
            gm.insert(TopicId::new(1), PartitionId::new(0), GroupId::new(1));
        }

        // Add partition storage.
        {
            let mut storage = partition_storage.write().await;
            let ps =
                ServerPartitionStorage::new_in_memory(TopicId::new(1), PartitionId::new(0));
            storage.insert(GroupId::new(1), ps);
        }

        // Spawn processor.
        let processor = tokio::spawn(output_processor_task(
            rx,
            partition_storage,
            group_map,
            batch_pending_proposals,
            None,
            None,
            None,
        ));

        // Send EntryCommitted output (no batch proposal, so just applies).
        let output = GroupedOutput {
            group_id: GroupId::new(1),
            output: PartitionOutput::EntryCommitted {
                index: LogIndex::new(1),
                data: Bytes::from("test data"),
                batch_notify: None,
            },
        };
        tx.send(output).await.unwrap();

        // Give time for processing.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Drop sender to stop processor.
        drop(tx);
        let _ = processor.await;
    }

    #[tokio::test]
    async fn test_output_processor_batch_proposal_notification() {
        let config = OutputProcessorConfig::default();
        let (tx, rx) = create_output_channel(config);

        let partition_storage = Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let batch_pending_proposals: Arc<RwLock<HashMap<GroupId, HashMap<LogIndex, BatchPendingProposal>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let batcher_stats = Arc::new(BatcherStats::default());
        let backpressure = Arc::new(BackpressureState::default());

        let group_id = GroupId::new(1);
        let index = LogIndex::new(1);

        // Add a mapping.
        {
            let mut gm = group_map.write().await;
            gm.insert(TopicId::new(1), PartitionId::new(0), group_id);
        }

        // Add partition storage.
        {
            let mut storage = partition_storage.write().await;
            let ps =
                ServerPartitionStorage::new_in_memory(TopicId::new(1), PartitionId::new(0));
            storage.insert(group_id, ps);
        }

        // Create a batch pending proposal.
        let (result_tx, result_rx) = oneshot::channel();
        let batch_proposal = BatchPendingProposal {
            log_index: index,
            first_request_at: std::time::Instant::now(),
            proposed_at: std::time::Instant::now(),
            batch_size: 1,
            batch_bytes: 10,
            total_records: 1,
            record_counts: vec![1],
            result_txs: vec![result_tx],
        };

        // Set backpressure counters.
        backpressure
            .pending_requests
            .store(1, std::sync::atomic::Ordering::Relaxed);
        backpressure
            .pending_bytes
            .store(10, std::sync::atomic::Ordering::Relaxed);

        // Register the batch proposal.
        {
            let mut proposals = batch_pending_proposals.write().await;
            proposals.entry(group_id).or_default().insert(index, batch_proposal);
        }

        // Spawn processor.
        let processor = tokio::spawn(output_processor_task(
            rx,
            Arc::clone(&partition_storage),
            Arc::clone(&group_map),
            Arc::clone(&batch_pending_proposals),
            None,
            Some(Arc::clone(&batcher_stats)),
            Some(Arc::clone(&backpressure)),
        ));

        // Create valid batch entry data.
        use crate::storage::{BatchedBlob, BlobFormat, PartitionCommand};
        let command = PartitionCommand::AppendBlobBatch {
            blobs: vec![BatchedBlob {
                blob: Bytes::from("test"),
                record_count: 1,
                format: BlobFormat::Raw,
            }],
        };
        let data = command.encode();

        // Send EntryCommitted output (using shared batch_pending_proposals path).
        let output = GroupedOutput {
            group_id,
            output: PartitionOutput::EntryCommitted {
                index,
                data,
                batch_notify: None,
            },
        };
        tx.send(output).await.unwrap();

        // Wait for notification.
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            result_rx,
        )
        .await;

        assert!(result.is_ok(), "Should receive result");
        let inner_result = result.unwrap();
        assert!(inner_result.is_ok(), "Channel should not be dropped");
        let offset_result = inner_result.unwrap();
        assert!(offset_result.is_ok(), "Should succeed");
        assert_eq!(offset_result.unwrap().get(), 0, "First offset should be 0");

        // Backpressure counters should be decremented.
        assert_eq!(
            backpressure
                .pending_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            backpressure
                .pending_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        // Drop sender to stop processor.
        drop(tx);
        let _ = processor.await;
    }
}
