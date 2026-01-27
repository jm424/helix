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

use bytes::Bytes;
use helix_core::{GroupId, LogIndex, Offset, ProducerEpoch, ProducerId, SequenceNum};
use helix_runtime::TransportHandle;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

use crate::error::ServerError;
use crate::group_map::GroupMap;
use crate::kafka::extract_producer_info;
use crate::partition_storage::ServerPartitionStorage;
use crate::storage::{BlobFormat, PartitionCommand};

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
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>>,
    group_map: Arc<RwLock<GroupMap>>,
    batch_pending_proposals: Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>>,
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
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>>,
    group_map: &Arc<RwLock<GroupMap>>,
    batch_pending_proposals: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>>,
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
        let inner_lock = {
            let batch_proposals = batch_pending_proposals.read().await;
            batch_proposals.get(&group_id).cloned()
        };
        if let Some(inner_lock) = inner_lock {
            let mut inner = inner_lock.write().await;
            inner.remove(&index)
        } else {
            None
        }
    };

    if let Some(batch_proposal) = batch_proposal {
        // Extract base_offset from the encoded command - this was captured by the leader
        // at propose time and must be used by ALL replicas for consistency.
        // CRITICAL: Do NOT read base_offset from storage - that would be wrong on
        // followers where storage state differs from the original leader.
        let base_offset = match PartitionCommand::decode(data) {
            Some(PartitionCommand::AppendBlobBatch { base_offset, .. }) => base_offset,
            Some(PartitionCommand::AppendBlob { base_offset, .. }) => base_offset,
            _ => {
                warn!(
                    group = group_id.get(),
                    index = index.get(),
                    "Failed to extract base_offset from command, falling back to storage"
                );
                // Fallback to storage for backwards compatibility with old commands.
                let ps_lock = {
                    let storage = partition_storage.read().await;
                    storage.get(&group_id).cloned()
                };
                if let Some(ps_lock) = ps_lock {
                    let ps = ps_lock.read().await;
                    ps.blob_log_end_offset()
                } else {
                    Offset::new(0)
                }
            }
        };

        info!(
            group = group_id.get(),
            index = index.get(),
            base_offset = base_offset.get(),
            "COMMIT_OFFSET: Using base_offset from encoded command for client notification"
        );

        // Apply the batch entry.
        let apply_result = {
            let ps_lock = {
                let storage = partition_storage.read().await;
                storage.get(&group_id).cloned()
            };
            if let Some(ps_lock) = ps_lock {
                let mut ps = ps_lock.write().await;
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
        // 3. PREVIOUS_TERM entries committed by new leader via no-op
        //
        // CRITICAL: We must extract and record producer state from the entry data.
        // Without this, a new leader won't know about producer sequences from
        // PREVIOUS_TERM entries, causing duplicate detection to fail.

        // Get per-partition lock.
        let ps_lock = {
            let storage = partition_storage.read().await;
            storage.get(&group_id).cloned()
        };
        let Some(ps_lock) = ps_lock else {
            warn!(
                group = group_id.get(),
                index = index.get(),
                "Partition storage not found for committed entry"
            );
            return;
        };

        // Get base_offset BEFORE apply (needed for producer state recording).
        let base_offset = {
            let ps = ps_lock.read().await;
            ps.blob_log_end_offset()
        };

        let apply_result = {
            let mut ps = ps_lock.write().await;
            ps.apply_entry_async(index, data).await
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
            // Extract and record producer state from the entry data.
            // This ensures idempotent deduplication works correctly even for
            // entries committed by a new leader (PREVIOUS_TERM entries).
            extract_and_record_producer_state(
                data,
                base_offset,
                group_id,
                partition_storage,
            )
            .await;

            debug!(
                topic = topic_id.get(),
                partition = partition_id.get(),
                index = index.get(),
                base_offset = base_offset.get(),
                "Applied committed entry with producer state (actor mode)"
            );
        }
    }
}

/// Extracts producer info from a committed entry and records it in the partition state.
///
/// This is critical for handling PREVIOUS_TERM entries on a new leader. When a new
/// leader takes over, it commits uncommitted entries from the previous term via its
/// no-op entry. Without recording producer state from these entries, the new leader
/// would not know about the sequences accepted by the old leader, causing:
/// - Duplicate entries when clients retry
/// - Offset gaps in the data
///
/// # Entry Formats Handled
///
/// - `AppendBlob` with `KafkaRecordBatch`: Single blob with producer info in header
/// - `AppendBlobBatch`: Multiple blobs, each with producer info in header
pub async fn extract_and_record_producer_state(
    data: &Bytes,
    base_offset: Offset,
    group_id: GroupId,
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>>,
) {
    // Decode the partition command to access blob data.
    let Some(command) = PartitionCommand::decode(data) else {
        return;
    };

    // Get the per-partition lock once.
    let ps_lock = {
        let storage = partition_storage.read().await;
        storage.get(&group_id).cloned()
    };
    let Some(ps_lock) = ps_lock else {
        return;
    };

    match command {
        PartitionCommand::AppendBlob {
            blob,
            record_count,
            format,
            ..
        } => {
            if format == BlobFormat::KafkaRecordBatch {
                if let Some(info) = extract_producer_info(&blob) {
                    if info.is_idempotent() {
                        let mut ps = ps_lock.write().await;
                        ps.record_producer_sequence(
                            ProducerId::new(info.producer_id as u64),
                            ProducerEpoch::new(info.epoch as u16),
                            SequenceNum::new(info.base_sequence),
                            base_offset,
                        );
                        debug!(
                            group = group_id.get(),
                            producer_id = info.producer_id,
                            epoch = info.epoch,
                            sequence = info.base_sequence,
                            offset = base_offset.get(),
                            record_count,
                            "Recorded producer state from committed entry"
                        );
                    }
                }
            }
        }
        PartitionCommand::AppendBlobBatch { blobs, .. } => {
            // Process each blob in the batch, accumulating offsets.
            let mut cumulative_offset = base_offset;
            for batched in blobs {
                if batched.format == BlobFormat::KafkaRecordBatch {
                    if let Some(info) = extract_producer_info(&batched.blob) {
                        if info.is_idempotent() {
                            let mut ps = ps_lock.write().await;
                            ps.record_producer_sequence(
                                ProducerId::new(info.producer_id as u64),
                                ProducerEpoch::new(info.epoch as u16),
                                SequenceNum::new(info.base_sequence),
                                cumulative_offset,
                            );
                            debug!(
                                group = group_id.get(),
                                producer_id = info.producer_id,
                                epoch = info.epoch,
                                sequence = info.base_sequence,
                                offset = cumulative_offset.get(),
                                record_count = batched.record_count,
                                "Recorded producer state from batched entry"
                            );
                        }
                    }
                }
                // Advance offset for next blob in batch.
                cumulative_offset = Offset::new(cumulative_offset.get() + u64::from(batched.record_count));
            }
        }
        // Other command types don't have producer state to record.
        _ => {}
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

        let partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let batch_pending_proposals: Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>> =
            Arc::new(RwLock::new(HashMap::new()));

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
            storage.insert(GroupId::new(1), Arc::new(RwLock::new(ps)));
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

        let partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let batch_pending_proposals: Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>> =
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
            storage.insert(group_id, Arc::new(RwLock::new(ps)));
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
            let inner_lock = {
                let mut proposals = batch_pending_proposals.write().await;
                proposals
                    .entry(group_id)
                    .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
                    .clone()
            };
            let mut inner = inner_lock.write().await;
            inner.insert(index, batch_proposal);
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
        use helix_core::Offset;
        let command = PartitionCommand::AppendBlobBatch {
            blobs: vec![BatchedBlob {
                blob: Bytes::from("test"),
                record_count: 1,
                format: BlobFormat::Raw,
            }],
            base_offset: Offset::new(0),
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
