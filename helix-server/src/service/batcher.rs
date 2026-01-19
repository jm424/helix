//! Request batching for improved throughput.
//!
//! This module batches multiple producer requests into a single Raft entry,
//! reducing consensus overhead and improving throughput.
//!
//! # Design
//!
//! ```text
//! Request 1 ──┐
//! Request 2 ──┼──> RequestBatcher ──> AppendBlobBatch ──> 1 Raft Entry ──> Notify All
//! Request 3 ──┘       (linger_ms)
//! ```
//!
//! Each producer request is submitted to the batcher. The batcher accumulates
//! requests until either:
//! - The linger timeout expires (default: 5ms)
//! - The batch reaches max size (default: 64KB)
//! - The batch reaches max requests (default: 1000)
//!
//! The batch is then proposed to Raft as a single `AppendBlobBatch` entry.
//! When committed, the tick task notifies each waiter with their assigned offset.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use helix_core::{GroupId, LogIndex, Offset};
use helix_raft::multi::MultiRaft;
use helix_raft::RaftState;
use helix_runtime::TransportHandle;
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{debug, info, warn, Instrument};

use crate::error::{ServerError, ServerResult};
use crate::storage::{BatchedBlob, BlobFormat, PartitionCommand};

use super::{BatchPendingProposal, BatcherStats};

// =============================================================================
// Backpressure Limits
// =============================================================================

/// Maximum number of pending requests before rejecting new ones.
/// This prevents unbounded queuing that leads to commit latency explosion.
/// NOTE: Counters are decremented at commit time, so this limits total in-flight.
const MAX_PENDING_REQUESTS: u64 = 2000;

/// Maximum total bytes pending before rejecting new requests.
/// 100MB allows higher throughput while still preventing Raft saturation.
/// NOTE: Counters are decremented at commit time, so this limits total in-flight.
const MAX_PENDING_BYTES: u64 = 100 * 1024 * 1024;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for request batching.
#[derive(Debug, Clone)]
pub struct BatcherConfig {
    /// Maximum time to wait for additional requests before flushing (milliseconds).
    /// Default: 1ms - minimal wait since clients already batch with linger.ms.
    pub linger_ms: u64,
    /// Maximum total bytes in a batch before forcing flush.
    /// Default: 64KB - balanced for typical workloads.
    pub max_batch_bytes: u32,
    /// Maximum number of requests in a batch (`TigerStyle` bounded limit).
    /// Default: 1000 - prevents unbounded memory growth.
    pub max_batch_requests: u32,
}

impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            // 1ms minimal linger: clients already batch, so server-side linger
            // adds latency without much benefit. Higher values cause backpressure.
            linger_ms: 1,
            // Must be larger than typical client batches (rdkafka batch.size=512KB)
            // to allow server-side batching of multiple producer requests.
            // Too small = each producer batch becomes separate Raft proposal.
            max_batch_bytes: 4 * 1024 * 1024, // 4MB
            max_batch_requests: 1000,
        }
    }
}

// =============================================================================
// Types
// =============================================================================

#[derive(Debug, Clone, Copy)]
enum FlushReason {
    Linger,
    Size,
    Shutdown,
}

impl FlushReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Linger => "linger",
            Self::Size => "size",
            Self::Shutdown => "shutdown",
        }
    }
}

/// A pending request waiting to be batched.
pub struct PendingBatchRequest {
    /// Group ID for this request.
    pub group_id: GroupId,
    /// Raw blob data.
    pub blob: Bytes,
    /// Number of records in the blob.
    pub record_count: u32,
    /// Format of the blob data.
    pub format: BlobFormat,
    /// Channel to send the result (offset) when committed.
    pub result_tx: oneshot::Sender<ServerResult<Offset>>,
}

/// Message sent to the batcher task.
pub enum BatcherMessage {
    /// Submit a new request to be batched.
    Submit(PendingBatchRequest),
    /// Shutdown the batcher.
    Shutdown,
}

/// Shared backpressure state between handle and batcher task.
#[derive(Debug, Default)]
pub struct BackpressureState {
    /// Current number of pending requests.
    pub pending_requests: AtomicU64,
    /// Current total pending bytes.
    pub pending_bytes: AtomicU64,
    /// Total requests rejected due to backpressure.
    pub rejected_requests: AtomicU64,
    /// Total bytes rejected due to backpressure.
    pub rejected_bytes: AtomicU64,
}

/// Handle for submitting requests to the batcher.
#[derive(Clone)]
pub struct BatcherHandle {
    tx: mpsc::Sender<BatcherMessage>,
    backpressure: Arc<BackpressureState>,
}

impl BatcherHandle {
    /// Submits a request to the batcher.
    ///
    /// Returns a receiver that will be notified with the offset when committed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The batcher channel is full or closed
    /// - The server is overloaded (backpressure)
    pub async fn submit(
        &self,
        group_id: GroupId,
        blob: Bytes,
        record_count: u32,
        format: BlobFormat,
    ) -> ServerResult<oneshot::Receiver<ServerResult<Offset>>> {
        let blob_size = blob.len() as u64;

        // Check backpressure limits before accepting the request.
        let pending_requests = self.backpressure.pending_requests.load(Ordering::Relaxed);
        let pending_bytes = self.backpressure.pending_bytes.load(Ordering::Relaxed);

        if pending_requests >= MAX_PENDING_REQUESTS || pending_bytes >= MAX_PENDING_BYTES {
            // Track rejected requests for monitoring.
            self.backpressure
                .rejected_requests
                .fetch_add(1, Ordering::Relaxed);
            self.backpressure
                .rejected_bytes
                .fetch_add(blob_size, Ordering::Relaxed);

            warn!(
                pending_requests,
                pending_bytes,
                max_requests = MAX_PENDING_REQUESTS,
                max_bytes = MAX_PENDING_BYTES,
                "Batcher rejecting request due to backpressure"
            );

            return Err(ServerError::Overloaded {
                pending_requests,
                pending_bytes,
            });
        }

        // Increment counters before sending (will be decremented on flush).
        self.backpressure
            .pending_requests
            .fetch_add(1, Ordering::Relaxed);
        self.backpressure
            .pending_bytes
            .fetch_add(blob_size, Ordering::Relaxed);

        let (result_tx, result_rx) = oneshot::channel();

        let request = PendingBatchRequest {
            group_id,
            blob,
            record_count,
            format,
            result_tx,
        };

        if let Err(e) = self.tx.send(BatcherMessage::Submit(request)).await {
            // Failed to send - decrement counters.
            self.backpressure
                .pending_requests
                .fetch_sub(1, Ordering::Relaxed);
            self.backpressure
                .pending_bytes
                .fetch_sub(blob_size, Ordering::Relaxed);

            return Err(ServerError::Internal {
                message: format!("batcher channel closed: {e}"),
            });
        }

        Ok(result_rx)
    }

    /// Returns the current backpressure state for monitoring.
    #[must_use]
    pub const fn backpressure_state(&self) -> &Arc<BackpressureState> {
        &self.backpressure
    }
}

/// Creates a batcher channel and handle.
///
/// Returns (handle, receiver, backpressure) where:
/// - `handle` is used to submit requests
/// - `receiver` is passed to `batcher_task`
/// - `backpressure` is shared state for tracking pending requests/bytes
#[must_use]
pub fn create_batcher() -> (
    BatcherHandle,
    mpsc::Receiver<BatcherMessage>,
    Arc<BackpressureState>,
) {
    // Channel capacity: 10000 allows for burst of requests without blocking.
    // TigerStyle: bounded channel prevents unbounded memory growth.
    let (tx, rx) = mpsc::channel(10000);
    let backpressure = Arc::new(BackpressureState::default());
    let handle = BatcherHandle {
        tx,
        backpressure: Arc::clone(&backpressure),
    };
    (handle, rx, backpressure)
}

// =============================================================================
// Batcher Task
// =============================================================================

/// Accumulated batch for a single group.
struct AccumulatedBatch {
    /// Blobs in this batch.
    blobs: Vec<BatchedBlob>,
    /// Record counts for each request (for offset calculation at commit time).
    record_counts: Vec<u32>,
    /// Result channels for each request.
    result_txs: Vec<oneshot::Sender<ServerResult<Offset>>>,
    /// Total bytes accumulated.
    total_bytes: u32,
    /// Time when first request was added.
    first_request_time: std::time::Instant,
}

impl AccumulatedBatch {
    fn new() -> Self {
        Self {
            blobs: Vec::with_capacity(100), // Pre-allocate for typical batch size.
            record_counts: Vec::with_capacity(100),
            result_txs: Vec::with_capacity(100),
            total_bytes: 0,
            first_request_time: std::time::Instant::now(),
        }
    }

    fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }

    fn request_count(&self) -> usize {
        self.blobs.len()
    }
}

/// Background task that batches requests and proposes to Raft.
///
/// This task:
/// 1. Receives requests via the channel
/// 2. Accumulates them into per-group batches
/// 3. Flushes batches on linger timeout or max size
/// 4. Proposes `AppendBlobBatch` to Raft
/// 5. Registers `BatchPendingProposal` for commit notification
#[allow(clippy::too_many_arguments)]
#[allow(clippy::implicit_hasher)]
pub async fn batcher_task(
    mut rx: mpsc::Receiver<BatcherMessage>,
    multi_raft: Arc<RwLock<MultiRaft>>,
    batch_pending_proposals: Arc<RwLock<HashMap<GroupId, HashMap<LogIndex, BatchPendingProposal>>>>,
    transport_handle: Option<TransportHandle>,
    batcher_stats: Arc<BatcherStats>,
    config: BatcherConfig,
    backpressure: Arc<BackpressureState>,
) {
    let mut batches: HashMap<GroupId, AccumulatedBatch> = HashMap::new();
    let linger_duration = Duration::from_millis(config.linger_ms);

    info!(
        linger_ms = config.linger_ms,
        max_batch_bytes = config.max_batch_bytes,
        max_batch_requests = config.max_batch_requests,
        "Batcher task started"
    );

    loop {
        // Calculate next flush deadline based on oldest batch.
        let next_flush_deadline = batches
            .values()
            .filter(|b| !b.is_empty())
            .map(|b| b.first_request_time + linger_duration)
            .min();

        let timeout = next_flush_deadline.map(|deadline| {
            deadline
                .checked_duration_since(std::time::Instant::now())
                .unwrap_or(Duration::ZERO)
        });

        tokio::select! {
            // Receive new request or shutdown.
            msg = rx.recv() => {
                match msg {
                    Some(BatcherMessage::Submit(request)) => {
                        handle_submit(
                            request,
                            &mut batches,
                            &multi_raft,
                            &batch_pending_proposals,
                            &transport_handle,
                            &batcher_stats,
                            &config,
                            &backpressure,
                        ).await;
                    }
                    Some(BatcherMessage::Shutdown) | None => {
                        // Flush any remaining batches before shutdown.
                        for (group_id, batch) in batches.drain() {
                            if !batch.is_empty() {
                                flush_batch(
                                    group_id,
                                    batch,
                                    &multi_raft,
                                    &batch_pending_proposals,
                                    &transport_handle,
                                    &batcher_stats,
                                    FlushReason::Shutdown,
                                    &backpressure,
                                ).await;
                            }
                        }
                        info!("Batcher task shutting down");
                        break;
                    }
                }
            }
            // Linger timeout - flush oldest batches.
            () = async {
                if let Some(duration) = timeout {
                    tokio::time::sleep(duration).await;
                } else {
                    // No batches, wait forever for next message.
                    std::future::pending::<()>().await;
                }
            } => {
                let now = std::time::Instant::now();
                let mut groups_to_flush = Vec::new();

                for (group_id, batch) in &batches {
                    if !batch.is_empty()
                        && now.duration_since(batch.first_request_time) >= linger_duration
                    {
                        groups_to_flush.push(*group_id);
                    }
                }

                for group_id in groups_to_flush {
                    if let Some(batch) = batches.remove(&group_id) {
                        flush_batch(
                            group_id,
                            batch,
                            &multi_raft,
                            &batch_pending_proposals,
                            &transport_handle,
                            &batcher_stats,
                            FlushReason::Linger,
                            &backpressure,
                        ).await;
                    }
                }
            }
        }
    }
}

/// Handles a submit request.
#[tracing::instrument(skip_all, name = "batcher_handle_submit", fields(group_id = request.group_id.get()))]
#[allow(clippy::too_many_arguments)]
#[allow(clippy::ref_option)]
async fn handle_submit(
    request: PendingBatchRequest,
    batches: &mut HashMap<GroupId, AccumulatedBatch>,
    multi_raft: &Arc<RwLock<MultiRaft>>,
    batch_pending_proposals: &Arc<RwLock<HashMap<GroupId, HashMap<LogIndex, BatchPendingProposal>>>>,
    transport_handle: &Option<TransportHandle>,
    batcher_stats: &Arc<BatcherStats>,
    config: &BatcherConfig,
    backpressure: &Arc<BackpressureState>,
) {
    let group_id = request.group_id;
    #[allow(clippy::cast_possible_truncation)]
    let blob_size = request.blob.len() as u32;

    let batch = batches.entry(group_id).or_insert_with(AccumulatedBatch::new);

    // Check if adding this request would exceed limits.
    let would_exceed_bytes = batch.total_bytes + blob_size > config.max_batch_bytes;
    #[allow(clippy::cast_possible_truncation)]
    let would_exceed_requests = batch.request_count() as u32 >= config.max_batch_requests;

    // If batch would exceed limits, flush first.
    if !batch.is_empty() && (would_exceed_bytes || would_exceed_requests) {
        let old_batch = std::mem::replace(batch, AccumulatedBatch::new());
        flush_batch(
            group_id,
            old_batch,
            multi_raft,
            batch_pending_proposals,
            transport_handle,
            batcher_stats,
            FlushReason::Size,
            backpressure,
        )
        .await;
    }

    // Update first_request_time if this is the first request in the batch.
    if batch.is_empty() {
        batch.first_request_time = std::time::Instant::now();
    }

    // Add request to batch.
    batch.blobs.push(BatchedBlob {
        blob: request.blob,
        record_count: request.record_count,
        format: request.format,
    });
    batch.record_counts.push(request.record_count);
    batch.result_txs.push(request.result_tx);
    batch.total_bytes += blob_size;
}

/// Flushes a batch by proposing to Raft.
#[tracing::instrument(skip_all, name = "batcher_flush", fields(group_id = group_id.get(), batch_size = batch.request_count()))]
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
#[allow(clippy::ref_option)]
#[allow(clippy::significant_drop_tightening)]
async fn flush_batch(
    group_id: GroupId,
    batch: AccumulatedBatch,
    multi_raft: &Arc<RwLock<MultiRaft>>,
    batch_pending_proposals: &Arc<RwLock<HashMap<GroupId, HashMap<LogIndex, BatchPendingProposal>>>>,
    transport_handle: &Option<TransportHandle>,
    batcher_stats: &Arc<BatcherStats>,
    reason: FlushReason,
    backpressure: &Arc<BackpressureState>,
) {
    let batch_size = batch.request_count();
    let batch_bytes = batch.total_bytes;
    let batch_age = batch.first_request_time.elapsed();
    let batch_records: u64 = batch
        .record_counts
        .iter()
        .map(|count| u64::from(*count))
        .sum();

    // NOTE: Backpressure counters are NOT decremented here at flush time.
    // They are decremented when the client is notified (success or error),
    // which limits the total in-flight requests through Raft consensus.
    // Error paths below decrement immediately; success path is decremented
    // in tick.rs when the batch commit is notified to waiters.

    // Safety: durations will never exceed u64::MAX microseconds
    // (would require running for millions of years).
    #[allow(clippy::cast_possible_truncation)]
    batcher_stats.record_flush(
        reason.as_str(),
        batch_size as u64,
        u64::from(batch_bytes),
        batch_records,
        batch_age.as_micros() as u64,
    );

    // Safety: duration will never exceed u64::MAX microseconds.
    #[allow(clippy::cast_possible_truncation)]
    {
        info!(
            group_id = group_id.get(),
            reason = reason.as_str(),
            batch_size,
            batch_bytes,
            batch_records,
            batch_age_us = batch_age.as_micros() as u64,
            "Batch flush"
        );
    }

    // Check if we're the leader.
    let (is_leader, _leader_hint) = async {
        let mr = multi_raft.read().await;
        let state = mr.group_state(group_id);
        let is_leader = state.as_ref().is_some_and(|s| s.state == RaftState::Leader);
        let leader = state.and_then(|s| s.leader_id);
        (is_leader, leader)
    }
    .instrument(tracing::info_span!("check_leader"))
    .await;

    if !is_leader {
        batcher_stats.record_not_leader();
        // Not leader - notify all waiters with error.
        // Decrement backpressure counters since requests are complete (failed).
        backpressure
            .pending_requests
            .fetch_sub(batch_size as u64, Ordering::Relaxed);
        backpressure
            .pending_bytes
            .fetch_sub(u64::from(batch_bytes), Ordering::Relaxed);

        let err = ServerError::NotLeader {
            topic: "unknown".to_string(),
            partition: 0,
            leader_hint: None,
        };
        for result_tx in batch.result_txs {
            let _ = result_tx.send(Err(err.clone()));
        }
        return;
    }

    // Encode the batch command (sync, no span needed - fast).
    let command = PartitionCommand::AppendBlobBatch { blobs: batch.blobs };
    let command_data = command.encode();

    // Propose to Raft and get log index.
    let propose_result = async {
        let mut mr = multi_raft.write().await;
        let result = mr.propose_with_index(group_id, command_data);
        result.as_ref()?;
        let (outputs, idx) = result.unwrap();
        let flush_outputs = mr.flush();
        let all_outputs: Vec<_> = outputs.into_iter().chain(flush_outputs).collect();
        Some((idx, all_outputs))
    }
    .instrument(tracing::info_span!("propose_to_raft"))
    .await;

    let Some((proposed_index, messages_to_send)) = propose_result else {
        // Propose failed - not leader or other error.
        // Decrement backpressure counters since requests are complete (failed).
        backpressure
            .pending_requests
            .fetch_sub(batch_size as u64, Ordering::Relaxed);
        backpressure
            .pending_bytes
            .fetch_sub(u64::from(batch_bytes), Ordering::Relaxed);

        let err = ServerError::NotLeader {
            topic: "unknown".to_string(),
            partition: 0,
            leader_hint: None,
        };
        for result_tx in batch.result_txs {
            let _ = result_tx.send(Err(err.clone()));
        }
        return;
    };

    // Send Raft messages immediately.
    let msg_count = messages_to_send.len();
    async {
        if let Some(ref transport) = transport_handle {
            for output in &messages_to_send {
                if let helix_raft::multi::MultiRaftOutput::SendMessages { to, messages } = output {
                    if let Err(e) = transport.send_batch(*to, messages.clone()).await {
                        debug!(
                            to = to.get(),
                            error = %e,
                            "Failed to send batch Raft messages"
                        );
                    }
                }
            }
        }
    }
    .instrument(tracing::info_span!("send_raft_messages", msg_count))
    .await;

    // Register the batch pending proposal.
    let proposed_at = std::time::Instant::now();
    // Safe cast: batch_size bounded by MAX_BATCH_REQUESTS (1000), fits in u32.
    #[allow(clippy::cast_possible_truncation)]
    let batch_proposal = BatchPendingProposal {
        log_index: proposed_index,
        first_request_at: batch.first_request_time,
        proposed_at,
        batch_size: batch_size as u32,
        batch_bytes,
        total_records: batch_records,
        record_counts: batch.record_counts,
        result_txs: batch.result_txs,
    };

    async {
        let mut proposals = batch_pending_proposals.write().await;
        proposals
            .entry(group_id)
            .or_default()
            .insert(proposed_index, batch_proposal);
    }
    .instrument(tracing::info_span!("register_pending"))
    .await;

    debug!(
        group_id = group_id.get(),
        log_index = proposed_index.get(),
        batch_size,
        "Batch proposed to Raft"
    );
}
