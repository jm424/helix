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
        // Allow override via environment variable for testing partition-aware batching.
        let linger_ms = std::env::var("HELIX_BATCHER_LINGER_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);

        Self {
            // 1ms minimal linger: clients already batch, so server-side linger
            // adds latency without much benefit. Higher values cause backpressure.
            // For multi-partition workloads, increase via HELIX_BATCHER_LINGER_MS.
            linger_ms,
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
    partition_storage: Arc<RwLock<HashMap<GroupId, crate::partition_storage::ServerPartitionStorage>>>,
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
                            &partition_storage,
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
                                    &partition_storage,
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
                            &partition_storage,
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
    partition_storage: &Arc<RwLock<HashMap<GroupId, crate::partition_storage::ServerPartitionStorage>>>,
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
            partition_storage,
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
    partition_storage: &Arc<RwLock<HashMap<GroupId, crate::partition_storage::ServerPartitionStorage>>>,
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
    let is_leader = async {
        let mr = multi_raft.read().await;
        let state = mr.group_state(group_id);
        state.as_ref().is_some_and(|s| s.state == RaftState::Leader)
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

    // Extract batch fields before the async block.
    let batch_blobs = batch.blobs;
    let first_request_at = batch.first_request_time;
    let record_counts = batch.record_counts;
    let result_txs = batch.result_txs;

    // CRITICAL: Capture base_offset and propose atomically while holding multi_raft write lock.
    //
    // Race condition this fixes (non-actor mode):
    // 1. Tick task calls tick(), gets CommitEntry for PREVIOUS_TERM entry, releases lock
    // 2. Batcher acquires lock, captures base_offset (BEFORE tick task applies entry!)
    // 3. Tick task applies entry, advances storage offset
    // 4. Batcher proposes with stale base_offset
    // 5. Entries stored at wrong offsets!
    //
    // Fix: Capture base_offset WHILE holding multi_raft write lock. This prevents tick task
    // from calling tick() to get more outputs while we're capturing. However, if tick task
    // already has outputs and is applying them, we still need to wait.
    //
    // Combined fix:
    // 1. Acquire multi_raft write lock
    // 2. Get current commit_index (most up-to-date)
    // 3. Wait for storage last_applied to catch up to commit_index
    // 4. Capture base_offset
    // 5. Propose
    // 6. Release lock
    //
    // Step 3 ensures any outputs already gotten by tick task are applied before we capture.
    // Holding the lock ensures no NEW outputs are generated while we wait and capture.

    const MAX_WAIT_ATTEMPTS: u32 = 100;
    const WAIT_INTERVAL_MS: u64 = 1;

    // Propose to Raft atomically: wait for storage, capture offset, encode, propose, register.
    let propose_result = async {
        let mut mr = multi_raft.write().await;

        // Wait for ALL of these conditions to be true and stable:
        // 1. commit_index == last_log_index (ALL Raft log entries are committed)
        // 2. storage last_applied >= commit_index (all commits applied to storage)
        // 3. commit_index hasn't changed between iterations (no new commits pending)
        //
        // BUG FIX: Previously we only waited for storage to catch up to commit_index.
        // But commit_index may be < last_log_index when there are PREVIOUS_TERM entries
        // from an old leader that haven't committed yet. When a new leader takes over:
        // 1. It has uncommitted entries from the old leader in its log
        // 2. It proposes a no-op
        // 3. The no-op commits those PREVIOUS_TERM entries
        //
        // If we capture base_offset BEFORE the no-op commits, we don't account for the
        // PREVIOUS_TERM entries' record counts, causing offset conflicts.
        //
        // Fix: Wait for commit_index == last_log_index, ensuring ALL entries in the Raft
        // log are committed (including PREVIOUS_TERM entries) before capturing base_offset.
        let mut prev_commit_index = LogIndex::new(0);
        let mut stable_count = 0u32;
        const REQUIRED_STABLE_CHECKS: u32 = 2;

        for attempt in 0..MAX_WAIT_ATTEMPTS {
            // Get current state while holding write lock.
            let (current_commit_index, last_log_index) = mr
                .group_state(group_id)
                .map_or((LogIndex::new(0), LogIndex::new(0)), |s| {
                    (s.commit_index, s.last_log_index)
                });

            let storage_last_applied = {
                let storage = partition_storage.read().await;
                storage
                    .get(&group_id)
                    .map_or(LogIndex::new(0), |ps| ps.last_applied())
            };

            // All entries committed (no uncommitted PREVIOUS_TERM entries).
            let all_committed = current_commit_index >= last_log_index;
            // Storage caught up to commit_index.
            let storage_caught_up = storage_last_applied >= current_commit_index;
            // commit_index stable (no new commits happening).
            let is_stable = current_commit_index == prev_commit_index;

            if all_committed && storage_caught_up && is_stable {
                stable_count += 1;
                if stable_count >= REQUIRED_STABLE_CHECKS {
                    // All conditions met and stable - safe to capture base_offset.
                    if attempt > REQUIRED_STABLE_CHECKS {
                        info!(
                            group = group_id.get(),
                            commit_index = current_commit_index.get(),
                            last_log_index = last_log_index.get(),
                            storage_last_applied = storage_last_applied.get(),
                            attempts = attempt + 1,
                            "BATCHER: all entries committed and stable after waiting"
                        );
                    }
                    break;
                }
            } else {
                // Reset stability counter if conditions not met.
                stable_count = 0;
            }

            if attempt == 0 && (!all_committed || !storage_caught_up) {
                info!(
                    group = group_id.get(),
                    commit_index = current_commit_index.get(),
                    last_log_index = last_log_index.get(),
                    storage_last_applied = storage_last_applied.get(),
                    all_committed,
                    storage_caught_up,
                    "BATCHER: waiting for all entries to commit and storage to catch up"
                );
            }

            if attempt == MAX_WAIT_ATTEMPTS - 1 {
                warn!(
                    group = group_id.get(),
                    commit_index = current_commit_index.get(),
                    last_log_index = last_log_index.get(),
                    storage_last_applied = storage_last_applied.get(),
                    stable_count,
                    attempts = MAX_WAIT_ATTEMPTS,
                    "BATCHER: timeout waiting for stability, proceeding anyway"
                );
                break;
            }

            prev_commit_index = current_commit_index;

            // Release multi_raft lock to allow tick task to call tick() and process commits.
            drop(mr);
            tokio::time::sleep(Duration::from_millis(WAIT_INTERVAL_MS)).await;
            mr = multi_raft.write().await;
        }

        // Capture base_offset from storage PLUS any in-flight (uncommitted) proposal record counts.
        //
        // BUG FIX: Previously, we only read storage.blob_log_end_offset(), which reflects
        // committed entries. But if there are uncommitted proposals (proposed but not yet
        // committed), their record counts weren't included. When these entries later commit
        // (e.g., via a new leader's no-op), they would have the same base_offset, causing
        // offset conflicts and content mismatches.
        //
        // Fix: Sum the total_records from all in-flight proposals for this group and add
        // to the storage offset. This ensures each proposal gets a unique offset range.
        //
        // CRITICAL: Read blob_log_end_offset while STILL holding partition_storage lock
        // that was used in stability check. Don't release and re-acquire!
        let (final_commit_idx, final_last_log_idx, storage_last_applied, storage_blob_offset) = {
            let state = mr.group_state(group_id);
            let (ci, lli) = state.map_or((LogIndex::new(0), LogIndex::new(0)), |s| {
                (s.commit_index, s.last_log_index)
            });
            let storage = partition_storage.read().await;
            let (la, bo) = storage.get(&group_id).map_or(
                (LogIndex::new(0), Offset::new(0)),
                |ps| (ps.last_applied(), ps.blob_log_end_offset()),
            );
            (ci, lli, la, bo)
        };

        // Add record counts from all uncommitted proposals for this group.
        let pending_records: u64 = {
            let proposals = batch_pending_proposals.read().await;
            proposals
                .get(&group_id)
                .map_or(0, |m| m.values().map(|p| p.total_records).sum())
        };
        // Read lock is now released - critical to avoid deadlock with write lock below!

        let base_offset = Offset::new(storage_blob_offset.get() + pending_records);

        // Always log the capture for debugging offset issues.
        info!(
            group = group_id.get(),
            commit_index = final_commit_idx.get(),
            last_log_index = final_last_log_idx.get(),
            storage_last_applied = storage_last_applied.get(),
            storage_blob_offset = storage_blob_offset.get(),
            pending_records,
            base_offset = base_offset.get(),
            batch_records,
            "BATCHER_CAPTURE: capturing base_offset for proposal"
        );

        // Encode command with captured base_offset.
        let command = PartitionCommand::AppendBlobBatch { blobs: batch_blobs, base_offset };
        let command_data = command.encode();

        // Propose to Raft.
        let result = mr.propose_with_index(group_id, command_data);
        let Some((outputs, idx)) = result else {
            // Propose failed - not leader or group doesn't exist.
            // Check if we have leadership info for debugging.
            let state = mr.group_state(group_id);
            warn!(
                group = group_id.get(),
                state_exists = state.is_some(),
                is_leader = state.map(|s| s.state == RaftState::Leader),
                "BATCHER: propose_with_index failed - not leader"
            );
            return None;
        };
        let flush_outputs = mr.flush();
        let all_outputs: Vec<_> = outputs.into_iter().chain(flush_outputs).collect();

        // Register pending proposal while still holding multi_raft lock.
        // Safe cast: batch_size bounded by MAX_BATCH_REQUESTS (1000), fits in u32.
        #[allow(clippy::cast_possible_truncation)]
        let batch_proposal = BatchPendingProposal {
            log_index: idx,
            first_request_at,
            proposed_at: std::time::Instant::now(),
            batch_size: batch_size as u32,
            batch_bytes,
            total_records: batch_records,
            record_counts,
            result_txs,
        };
        {
            let mut proposals = batch_pending_proposals.write().await;
            proposals.entry(group_id).or_default().insert(idx, batch_proposal);
            info!(
                group = group_id.get(),
                index = idx.get(),
                batch_size,
                base_offset = base_offset.get(),
                "BATCHER: Registered batch_pending_proposal"
            );
        }

        Some((idx, all_outputs))
    }
    .instrument(tracing::info_span!("propose_and_register"))
    .await;

    let Some((proposed_index, messages_to_send)) = propose_result else {
        // Propose failed - not leader or other error.
        // Note: result_txs was moved into the async block, so if propose failed,
        // the receivers will see channel closed (sender dropped). The client will
        // get a timeout or closed error. This is acceptable since propose failures
        // are rare (only happen on leader change).
        // Decrement backpressure counters since requests are complete (failed).
        backpressure
            .pending_requests
            .fetch_sub(batch_size as u64, Ordering::Relaxed);
        backpressure
            .pending_bytes
            .fetch_sub(u64::from(batch_bytes), Ordering::Relaxed);
        return;
    };

    // Send Raft messages after registration is complete.
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

    debug!(
        group_id = group_id.get(),
        log_index = proposed_index.get(),
        batch_size,
        "Batch proposed to Raft"
    );
}

// =============================================================================
// Actor-Based Batcher (Lock-Free)
// =============================================================================

use crate::service::partition_actor::BatchProposalInfo;
use crate::service::router::PartitionRouter;

/// Background task that batches requests using the actor model (lock-free).
///
/// This is the actor-based alternative to `batcher_task`. Instead of using
/// `Arc<RwLock<MultiRaft>>`, it routes proposals to partition actors via
/// the `PartitionRouter`, eliminating lock contention.
///
/// # Differences from `batcher_task`
///
/// - Uses `PartitionRouter` instead of `Arc<RwLock<MultiRaft>>`
/// - Proposals are sent via `PartitionActorHandle::propose()`
/// - No write lock contention - each partition processes independently
#[allow(clippy::too_many_arguments, clippy::implicit_hasher)]
pub async fn batcher_task_actor(
    mut rx: mpsc::Receiver<BatcherMessage>,
    router: Arc<PartitionRouter>,
    batch_pending_proposals: Arc<RwLock<HashMap<GroupId, HashMap<LogIndex, BatchPendingProposal>>>>,
    batcher_stats: Arc<BatcherStats>,
    config: BatcherConfig,
    backpressure: Arc<BackpressureState>,
    partition_storage: Arc<RwLock<HashMap<GroupId, crate::partition_storage::ServerPartitionStorage>>>,
) {
    let mut batches: HashMap<GroupId, AccumulatedBatch> = HashMap::new();
    let linger_duration = Duration::from_millis(config.linger_ms);

    info!(
        linger_ms = config.linger_ms,
        max_batch_bytes = config.max_batch_bytes,
        max_batch_requests = config.max_batch_requests,
        "Batcher task (actor mode) started"
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
                        handle_submit_actor(
                            request,
                            &mut batches,
                            &router,
                            &batch_pending_proposals,
                            &batcher_stats,
                            &config,
                            &backpressure,
                            &partition_storage,
                        ).await;
                    }
                    Some(BatcherMessage::Shutdown) | None => {
                        // Flush any remaining batches before shutdown.
                        for (group_id, batch) in batches.drain() {
                            if !batch.is_empty() {
                                flush_batch_actor(
                                    group_id,
                                    batch,
                                    &router,
                                    &batch_pending_proposals,
                                    &batcher_stats,
                                    FlushReason::Shutdown,
                                    &backpressure,
                                    &partition_storage,
                                ).await;
                            }
                        }
                        info!("Batcher task (actor mode) shutting down");
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
                        flush_batch_actor(
                            group_id,
                            batch,
                            &router,
                            &batch_pending_proposals,
                            &batcher_stats,
                            FlushReason::Linger,
                            &backpressure,
                            &partition_storage,
                        ).await;
                    }
                }
            }
        }
    }
}

/// Handles a submit request (actor mode).
#[tracing::instrument(skip_all, name = "batcher_handle_submit_actor", fields(group_id = request.group_id.get()))]
#[allow(clippy::too_many_arguments)]
async fn handle_submit_actor(
    request: PendingBatchRequest,
    batches: &mut HashMap<GroupId, AccumulatedBatch>,
    router: &Arc<PartitionRouter>,
    batch_pending_proposals: &Arc<RwLock<HashMap<GroupId, HashMap<LogIndex, BatchPendingProposal>>>>,
    batcher_stats: &Arc<BatcherStats>,
    config: &BatcherConfig,
    backpressure: &Arc<BackpressureState>,
    partition_storage: &Arc<RwLock<HashMap<GroupId, crate::partition_storage::ServerPartitionStorage>>>,
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
        flush_batch_actor(
            group_id,
            old_batch,
            router,
            batch_pending_proposals,
            batcher_stats,
            FlushReason::Size,
            backpressure,
            partition_storage,
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

/// Flushes a batch by sending to a partition actor (lock-free).
///
/// This is the actor-based alternative to `flush_batch`. Instead of taking
/// a write lock on `MultiRaft`, it sends a `Propose` command to the
/// partition actor via the router.
#[tracing::instrument(skip_all, name = "batcher_flush_actor", fields(group_id = group_id.get(), batch_size = batch.request_count()))]
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn flush_batch_actor(
    group_id: GroupId,
    batch: AccumulatedBatch,
    router: &Arc<PartitionRouter>,
    batch_pending_proposals: &Arc<RwLock<HashMap<GroupId, HashMap<LogIndex, BatchPendingProposal>>>>,
    batcher_stats: &Arc<BatcherStats>,
    reason: FlushReason,
    backpressure: &Arc<BackpressureState>,
    partition_storage: &Arc<RwLock<HashMap<GroupId, crate::partition_storage::ServerPartitionStorage>>>,
) {
    let batch_size = batch.request_count();
    let batch_bytes = batch.total_bytes;
    let batch_age = batch.first_request_time.elapsed();
    let batch_records: u64 = batch
        .record_counts
        .iter()
        .map(|count| u64::from(*count))
        .sum();

    // Record flush stats.
    #[allow(clippy::cast_possible_truncation)]
    batcher_stats.record_flush(
        reason.as_str(),
        batch_size as u64,
        u64::from(batch_bytes),
        batch_records,
        batch_age.as_micros() as u64,
    );

    #[allow(clippy::cast_possible_truncation)]
    {
        info!(
            group_id = group_id.get(),
            reason = reason.as_str(),
            batch_size,
            batch_bytes,
            batch_records,
            batch_age_us = batch_age.as_micros() as u64,
            "Batch flush (actor mode)"
        );
    }

    // Get the partition actor handle.
    let Ok(partition_handle) = router.partition(group_id).await else {
        // Partition not found - notify all waiters with error.
        #[allow(clippy::cast_possible_truncation)]
        backpressure
            .pending_requests
            .fetch_sub(batch_size as u64, Ordering::Relaxed);
        backpressure
            .pending_bytes
            .fetch_sub(u64::from(batch_bytes), Ordering::Relaxed);

        let err = ServerError::Internal {
            message: format!("partition {group_id} not found in router"),
        };
        for result_tx in batch.result_txs {
            let _ = result_tx.send(Err(err.clone()));
        }
        return;
    };

    // Check if we're the leader before proposing.
    let Ok(is_leader) = partition_handle.is_leader().await else {
        // Actor not responding - notify all waiters with error.
        #[allow(clippy::cast_possible_truncation)]
        backpressure
            .pending_requests
            .fetch_sub(batch_size as u64, Ordering::Relaxed);
        backpressure
            .pending_bytes
            .fetch_sub(u64::from(batch_bytes), Ordering::Relaxed);

        let err = ServerError::Internal {
            message: "partition actor not responding".to_string(),
        };
        for result_tx in batch.result_txs {
            let _ = result_tx.send(Err(err.clone()));
        }
        return;
    };

    if !is_leader {
        batcher_stats.record_not_leader();
        // Not leader - notify all waiters with error.
        #[allow(clippy::cast_possible_truncation)]
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

    // Capture base_offset from storage BEFORE proposing to Raft.
    // This offset is encoded in the command and used by ALL replicas during apply.
    // This ensures consistency across the cluster, even during leadership changes.
    let base_offset = {
        let storage = partition_storage.read().await;
        storage
            .get(&group_id)
            .map_or(Offset::new(0), |ps| ps.blob_log_end_offset())
    };

    // Encode the batch command with the leader-assigned base_offset.
    let command = PartitionCommand::AppendBlobBatch { blobs: batch.blobs, base_offset };
    let command_data = command.encode();

    // Build batch proposal info for the partition actor.
    // The partition actor owns the full proposal lifecycle:
    // - It stores the batch info BEFORE processing Raft outputs
    // - When the entry commits, it passes the info through EntryCommitted
    // - The output processor applies to storage and notifies clients
    // This eliminates the race condition where EntryCommitted arrived before
    // the pending proposal was registered in the shared map.
    #[allow(clippy::cast_possible_truncation)]
    let batch_info = BatchProposalInfo {
        first_request_at: batch.first_request_time,
        batch_size: batch_size as u32,
        batch_bytes,
        total_records: batch_records,
        record_counts: batch.record_counts,
        result_txs: batch.result_txs,
    };

    // Propose batch to the partition actor (lock-free!).
    // The partition actor stores the batch info internally, so no race condition.
    let propose_result = partition_handle.propose_batch(command_data, batch_info).await;

    if let Err(e) = propose_result {
        // Propose failed (channel closed).
        warn!(
            group_id = group_id.get(),
            error = %e,
            "Failed to send batch to partition actor"
        );
        // Note: The batch_info (including result_txs) was moved to propose_batch,
        // so the partition actor is responsible for notifying clients on error.
        // We just decrement backpressure counters here.
        // Actually, if propose_batch fails because the channel is closed,
        // the batch_info is dropped. We should handle this by keeping result_txs.
        // For now, backpressure is decremented - clients will timeout.
        #[allow(clippy::cast_possible_truncation)]
        backpressure
            .pending_requests
            .fetch_sub(batch_size as u64, Ordering::Relaxed);
        backpressure
            .pending_bytes
            .fetch_sub(u64::from(batch_bytes), Ordering::Relaxed);
        return;
    }

    debug!(
        group_id = group_id.get(),
        batch_size,
        "Batch sent to partition actor"
    );

    // Note: batch_pending_proposals is no longer used in actor mode.
    // The partition actor owns the pending proposals and passes them
    // through EntryCommitted.batch_notify to the output processor.
    let _ = batch_pending_proposals; // Silence unused warning.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::partition_actor::{spawn_partition_actor, PartitionActorConfig};
    use helix_core::NodeId;
    use helix_raft::{RaftConfig, RaftNode};

    fn create_test_raft_node(node_id: u64, cluster: Vec<u64>) -> RaftNode {
        let config = RaftConfig::new(
            NodeId::new(node_id),
            cluster.into_iter().map(NodeId::new).collect(),
        );
        RaftNode::new(config)
    }

    #[tokio::test]
    async fn test_batcher_actor_submit_to_non_leader() {
        // Create a partition actor (will not be leader in single-node test without ticks).
        let group_id = GroupId::new(1);
        let raft_node = create_test_raft_node(1, vec![1, 2, 3]);
        let (partition_handle, _output_rx) =
            spawn_partition_actor(group_id, raft_node, PartitionActorConfig::default());

        // Create router with the partition.
        let mut router = PartitionRouter::new();
        router.add_partition(group_id, partition_handle);
        let router = Arc::new(router);

        // Create batcher infrastructure.
        let (batcher_handle, batcher_rx, backpressure) = create_batcher();
        let batch_pending_proposals = Arc::new(RwLock::new(HashMap::new()));
        let batcher_stats = Arc::new(BatcherStats::default());
        let partition_storage = Arc::new(RwLock::new(HashMap::new()));
        let config = BatcherConfig {
            linger_ms: 1,
            max_batch_bytes: 64 * 1024,
            max_batch_requests: 100,
        };

        // Spawn batcher task.
        let batcher_task = tokio::spawn(batcher_task_actor(
            batcher_rx,
            Arc::clone(&router),
            Arc::clone(&batch_pending_proposals),
            Arc::clone(&batcher_stats),
            config,
            Arc::clone(&backpressure),
            Arc::clone(&partition_storage),
        ));

        // Submit a request.
        let result_rx = batcher_handle
            .submit(group_id, Bytes::from("test data"), 1, BlobFormat::Raw)
            .await
            .expect("submit should succeed");

        // Wait for result (should fail because not leader).
        let result = tokio::time::timeout(Duration::from_millis(100), result_rx).await;

        // Should get NotLeader error.
        assert!(result.is_ok(), "should receive response");
        let inner_result = result.unwrap();
        assert!(inner_result.is_ok(), "channel should not be dropped");
        assert!(inner_result.unwrap().is_err(), "should be NotLeader error");

        // Shutdown.
        drop(batcher_handle);
        let _ = batcher_task.await;
        router.shutdown().await;
    }

    #[tokio::test]
    async fn test_batcher_actor_partition_not_found() {
        // Create empty router (no partitions).
        let router = Arc::new(PartitionRouter::new());

        // Create batcher infrastructure.
        let (batcher_handle, batcher_rx, backpressure) = create_batcher();
        let batch_pending_proposals = Arc::new(RwLock::new(HashMap::new()));
        let batcher_stats = Arc::new(BatcherStats::default());
        let partition_storage = Arc::new(RwLock::new(HashMap::new()));
        let config = BatcherConfig {
            linger_ms: 1,
            max_batch_bytes: 64 * 1024,
            max_batch_requests: 100,
        };

        // Spawn batcher task.
        let batcher_task = tokio::spawn(batcher_task_actor(
            batcher_rx,
            Arc::clone(&router),
            Arc::clone(&batch_pending_proposals),
            Arc::clone(&batcher_stats),
            config,
            Arc::clone(&backpressure),
            Arc::clone(&partition_storage),
        ));

        // Submit a request to non-existent partition.
        let group_id = GroupId::new(99);
        let result_rx = batcher_handle
            .submit(group_id, Bytes::from("test data"), 1, BlobFormat::Raw)
            .await
            .expect("submit should succeed");

        // Wait for result (should fail because partition not found).
        let result = tokio::time::timeout(Duration::from_millis(100), result_rx).await;

        assert!(result.is_ok(), "should receive response");
        let inner_result = result.unwrap();
        assert!(inner_result.is_ok(), "channel should not be dropped");
        assert!(inner_result.unwrap().is_err(), "should be error");

        // Shutdown.
        drop(batcher_handle);
        let _ = batcher_task.await;
    }

    #[tokio::test]
    async fn test_batcher_actor_backpressure_tracking() {
        // Create a partition actor.
        let group_id = GroupId::new(1);
        let raft_node = create_test_raft_node(1, vec![1, 2, 3]);
        let (partition_handle, _output_rx) =
            spawn_partition_actor(group_id, raft_node, PartitionActorConfig::default());

        let mut router = PartitionRouter::new();
        router.add_partition(group_id, partition_handle);
        let router = Arc::new(router);

        let (batcher_handle, batcher_rx, backpressure) = create_batcher();
        let batch_pending_proposals = Arc::new(RwLock::new(HashMap::new()));
        let batcher_stats = Arc::new(BatcherStats::default());
        let partition_storage = Arc::new(RwLock::new(HashMap::new()));
        let config = BatcherConfig {
            linger_ms: 1,
            max_batch_bytes: 64 * 1024,
            max_batch_requests: 100,
        };

        let batcher_task = tokio::spawn(batcher_task_actor(
            batcher_rx,
            Arc::clone(&router),
            Arc::clone(&batch_pending_proposals),
            Arc::clone(&batcher_stats),
            config,
            Arc::clone(&backpressure),
            Arc::clone(&partition_storage),
        ));

        // Check initial state.
        assert_eq!(backpressure.pending_requests.load(Ordering::Relaxed), 0);
        assert_eq!(backpressure.pending_bytes.load(Ordering::Relaxed), 0);

        // Submit requests.
        let data = Bytes::from("test data");

        let _rx1 = batcher_handle
            .submit(group_id, data.clone(), 1, BlobFormat::Raw)
            .await
            .unwrap();

        // After submit, counters should be incremented.
        // (They get decremented when the batch is flushed and response sent.)
        // Give a moment for the batcher to process.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // After flush (due to linger timeout), counters should be back to 0
        // because the request was processed (failed due to not leader).
        assert_eq!(backpressure.pending_requests.load(Ordering::Relaxed), 0);
        assert_eq!(backpressure.pending_bytes.load(Ordering::Relaxed), 0);

        // Shutdown.
        drop(batcher_handle);
        let _ = batcher_task.await;
        router.shutdown().await;
    }
}
