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

// Allow complex nested types for proposal maps - refactoring would require significant API changes.
#![allow(clippy::type_complexity)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use helix_core::{GroupId, LogIndex, Offset};
use helix_raft::multi::MultiRaft;
use helix_raft::RaftState;
use helix_runtime::TransportService;
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{debug, info, warn, Instrument};

use crate::error::{ServerError, ServerResult};
use crate::partition_storage::PartitionStorage;
use crate::storage::{BatchedBlob, BlobFormat, PartitionCommand};
use helix_wal::Storage;

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
    /// Peak pending requests (high-water mark).
    pub peak_pending_requests: AtomicU64,
    /// Peak pending bytes (high-water mark).
    pub peak_pending_bytes: AtomicU64,
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
        let new_requests = self
            .backpressure
            .pending_requests
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let new_bytes = self
            .backpressure
            .pending_bytes
            .fetch_add(blob_size, Ordering::Relaxed)
            + blob_size;
        // Track high-water marks for bottleneck analysis.
        self.backpressure
            .peak_pending_requests
            .fetch_max(new_requests, Ordering::Relaxed);
        self.backpressure
            .peak_pending_bytes
            .fetch_max(new_bytes, Ordering::Relaxed);

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
pub async fn batcher_task<S: Storage + Clone + Send + Sync + 'static, T: TransportService>(
    mut rx: mpsc::Receiver<BatcherMessage>,
    multi_raft: Arc<RwLock<MultiRaft>>,
    batch_pending_proposals: Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>,
    >,
    transport_handle: Option<T>,
    batcher_stats: Arc<BatcherStats>,
    config: BatcherConfig,
    backpressure: Arc<BackpressureState>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
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
async fn handle_submit<S: Storage + Clone + Send + Sync + 'static, T: TransportService>(
    request: PendingBatchRequest,
    batches: &mut HashMap<GroupId, AccumulatedBatch>,
    multi_raft: &Arc<RwLock<MultiRaft>>,
    batch_pending_proposals: &Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>,
    >,
    transport_handle: &Option<T>,
    batcher_stats: &Arc<BatcherStats>,
    config: &BatcherConfig,
    backpressure: &Arc<BackpressureState>,
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
) {
    let group_id = request.group_id;
    #[allow(clippy::cast_possible_truncation)]
    let blob_size = request.blob.len() as u32;

    let batch = batches
        .entry(group_id)
        .or_insert_with(AccumulatedBatch::new);

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
async fn flush_batch<S: Storage + Clone + Send + Sync + 'static, T: TransportService>(
    group_id: GroupId,
    batch: AccumulatedBatch,
    multi_raft: &Arc<RwLock<MultiRaft>>,
    batch_pending_proposals: &Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>,
    >,
    transport_handle: &Option<T>,
    batcher_stats: &Arc<BatcherStats>,
    reason: FlushReason,
    backpressure: &Arc<BackpressureState>,
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
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
    // Race condition this fixes:
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
    const REQUIRED_STABLE_CHECKS: u32 = 2;

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

        for attempt in 0..MAX_WAIT_ATTEMPTS {
            // Get current state while holding write lock.
            let (current_commit_index, last_log_index) = mr
                .group_state(group_id)
                .map_or((LogIndex::new(0), LogIndex::new(0)), |s| {
                    (s.commit_index, s.last_log_index)
                });

            let storage_last_applied = {
                let ps_lock = {
                    let storage = partition_storage.read().await;
                    storage.get(&group_id).cloned()
                };
                if let Some(ps_lock) = ps_lock {
                    let ps = ps_lock.read().await;
                    ps.last_applied()
                } else {
                    LogIndex::new(0)
                }
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
            let ps_lock = {
                let storage = partition_storage.read().await;
                storage.get(&group_id).cloned()
            };
            let (la, bo) = if let Some(ps_lock) = ps_lock {
                let ps = ps_lock.read().await;
                (ps.last_applied(), ps.blob_log_end_offset())
            } else {
                (LogIndex::new(0), Offset::new(0))
            };
            (ci, lli, la, bo)
        };

        // Add record counts from all uncommitted proposals for this group.
        let pending_records: u64 = {
            let inner_lock = {
                let proposals = batch_pending_proposals.read().await;
                proposals.get(&group_id).cloned()
            };
            if let Some(inner_lock) = inner_lock {
                let inner = inner_lock.read().await;
                inner.values().map(|p| p.total_records).sum()
            } else {
                0
            }
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
        let command = PartitionCommand::AppendBlobBatch {
            blobs: batch_blobs,
            base_offset,
        };
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
            let inner_lock = {
                let mut proposals = batch_pending_proposals.write().await;
                proposals
                    .entry(group_id)
                    .or_insert_with(|| {
                        std::sync::Arc::new(tokio::sync::RwLock::new(
                            std::collections::HashMap::new(),
                        ))
                    })
                    .clone()
            };
            let mut inner = inner_lock.write().await;
            inner.insert(idx, batch_proposal);
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
#[allow(clippy::too_many_arguments, clippy::implicit_hasher, clippy::too_many_lines)]
pub async fn batcher_task_actor<S: Storage + Clone + Send + Sync + 'static>(
    mut rx: mpsc::Receiver<BatcherMessage>,
    router: Arc<PartitionRouter>,
    batch_pending_proposals: Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>,
    >,
    batcher_stats: Arc<BatcherStats>,
    config: BatcherConfig,
    backpressure: Arc<BackpressureState>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
) {
    let mut batches: HashMap<GroupId, AccumulatedBatch> = HashMap::new();
    let linger_duration = Duration::from_millis(config.linger_ms);

    info!(
        linger_ms = config.linger_ms,
        max_batch_bytes = config.max_batch_bytes,
        max_batch_requests = config.max_batch_requests,
        "Batcher task (actor mode) started"
    );

    let report_interval = Duration::from_secs(10);
    let mut last_report = std::time::Instant::now();
    let mut iter_start = std::time::Instant::now();

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

        let select_start = std::time::Instant::now();

        tokio::select! {
            // Receive new request or shutdown.
            msg = rx.recv() => {
                #[allow(clippy::cast_possible_truncation)]
                let idle_us =
                    select_start.elapsed().as_micros() as u64;
                batcher_stats.loop_idle_us.fetch_add(
                    idle_us,
                    std::sync::atomic::Ordering::Relaxed,
                );

                match msg {
                    Some(BatcherMessage::Submit(request)) => {
                        let submit_start =
                            std::time::Instant::now();
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
                        #[allow(clippy::cast_possible_truncation)]
                        let submit_us =
                            submit_start.elapsed().as_micros() as u64;
                        batcher_stats.loop_submit_us.fetch_add(
                            submit_us,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        batcher_stats.loop_submit_count.fetch_add(
                            1,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                    Some(BatcherMessage::Shutdown) | None => {
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
                    std::future::pending::<()>().await;
                }
            } => {
                #[allow(clippy::cast_possible_truncation)]
                let idle_us =
                    select_start.elapsed().as_micros() as u64;
                batcher_stats.loop_idle_us.fetch_add(
                    idle_us,
                    std::sync::atomic::Ordering::Relaxed,
                );

                let flush_cycle_start =
                    std::time::Instant::now();
                let now = std::time::Instant::now();
                let mut groups_to_flush = Vec::new();

                for (group_id, batch) in &batches {
                    if !batch.is_empty()
                        && now.duration_since(
                            batch.first_request_time,
                        ) >= linger_duration
                    {
                        groups_to_flush.push(*group_id);
                    }
                }

                let flush_group_count =
                    groups_to_flush.len() as u64;
                for group_id in groups_to_flush {
                    if let Some(batch) =
                        batches.remove(&group_id)
                    {
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

                #[allow(clippy::cast_possible_truncation)]
                let flush_cycle_us = flush_cycle_start
                    .elapsed()
                    .as_micros() as u64;
                batcher_stats.loop_linger_flush_us.fetch_add(
                    flush_cycle_us,
                    std::sync::atomic::Ordering::Relaxed,
                );
                batcher_stats
                    .loop_linger_flush_cycles
                    .fetch_add(
                        1,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                batcher_stats
                    .loop_linger_flush_groups
                    .fetch_add(
                        flush_group_count,
                        std::sync::atomic::Ordering::Relaxed,
                    );
            }
        }

        batcher_stats.loop_iterations.fetch_add(
            1,
            std::sync::atomic::Ordering::Relaxed,
        );

        // Periodic batcher utilization report.
        if last_report.elapsed() >= report_interval {
            report_batcher_loop_stats(&batcher_stats, &iter_start);
            last_report = std::time::Instant::now();
            iter_start = std::time::Instant::now();
        }
    }
}

/// Reports batcher loop utilization stats.
fn report_batcher_loop_stats(
    stats: &super::BatcherStats,
    window_start: &std::time::Instant,
) {
    use std::sync::atomic::Ordering::Relaxed;
    #[allow(clippy::cast_possible_truncation)]
    let wall_us = window_start.elapsed().as_micros() as u64;
    if wall_us == 0 {
        return;
    }

    let idle = stats.loop_idle_us.load(Relaxed);
    let submit = stats.loop_submit_us.load(Relaxed);
    let submit_n = stats.loop_submit_count.load(Relaxed);
    let linger_flush = stats.loop_linger_flush_us.load(Relaxed);
    let linger_cycles = stats.loop_linger_flush_cycles.load(Relaxed);
    let linger_groups = stats.loop_linger_flush_groups.load(Relaxed);
    let iters = stats.loop_iterations.load(Relaxed);
    let router = stats.flush_router_us.load(Relaxed);
    let storage = stats.flush_storage_lock_us.load(Relaxed);
    let encode = stats.flush_encode_us.load(Relaxed);
    let propose = stats.flush_propose_us.load(Relaxed);
    let inline_n = stats.inline_flush_count.load(Relaxed);
    let inline = stats.inline_flush_us.load(Relaxed);

    let busy = submit + linger_flush + inline;
    let utilization = if wall_us > 0 {
        (busy * 100) / wall_us
    } else {
        0
    };

    let avg_submit = if submit_n > 0 {
        submit / submit_n
    } else {
        0
    };
    let avg_linger = if linger_cycles > 0 {
        linger_flush / linger_cycles
    } else {
        0
    };

    // Use warn level so stats are visible with --log-level warn
    // (the default for benchmarks). Fires once per 10s — not noisy.
    warn!(
        wall_ms = wall_us / 1000,
        utilization_pct = utilization,
        idle_ms = idle / 1000,
        submit_ms = submit / 1000,
        submit_count = submit_n,
        avg_submit_us = avg_submit,
        linger_flush_ms = linger_flush / 1000,
        linger_cycles,
        linger_groups,
        avg_linger_us = avg_linger,
        inline_flush_count = inline_n,
        inline_flush_ms = inline / 1000,
        loop_iterations = iters,
        flush_router_ms = router / 1000,
        flush_storage_ms = storage / 1000,
        flush_encode_ms = encode / 1000,
        flush_propose_ms = propose / 1000,
        "BATCHER_LOOP_STATS"
    );
}

/// Handles a submit request (actor mode).
#[tracing::instrument(skip_all, name = "batcher_handle_submit_actor", fields(group_id = request.group_id.get()))]
#[allow(clippy::too_many_arguments)]
async fn handle_submit_actor<S: Storage + Clone + Send + Sync + 'static>(
    request: PendingBatchRequest,
    batches: &mut HashMap<GroupId, AccumulatedBatch>,
    router: &Arc<PartitionRouter>,
    batch_pending_proposals: &Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>,
    >,
    batcher_stats: &Arc<BatcherStats>,
    config: &BatcherConfig,
    backpressure: &Arc<BackpressureState>,
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
) {
    let group_id = request.group_id;
    #[allow(clippy::cast_possible_truncation)]
    let blob_size = request.blob.len() as u32;

    let batch = batches
        .entry(group_id)
        .or_insert_with(AccumulatedBatch::new);

    // Check if adding this request would exceed limits.
    let would_exceed_bytes = batch.total_bytes + blob_size > config.max_batch_bytes;
    #[allow(clippy::cast_possible_truncation)]
    let would_exceed_requests = batch.request_count() as u32 >= config.max_batch_requests;

    // If batch would exceed limits, flush first (inline flush).
    if !batch.is_empty() && (would_exceed_bytes || would_exceed_requests) {
        let inline_start = std::time::Instant::now();
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
        #[allow(clippy::cast_possible_truncation)]
        batcher_stats.inline_flush_us.fetch_add(
            inline_start.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        batcher_stats.inline_flush_count.fetch_add(
            1,
            std::sync::atomic::Ordering::Relaxed,
        );
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
async fn flush_batch_actor<S: Storage + Clone + Send + Sync + 'static>(
    group_id: GroupId,
    batch: AccumulatedBatch,
    router: &Arc<PartitionRouter>,
    batch_pending_proposals: &Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>,
    >,
    batcher_stats: &Arc<BatcherStats>,
    reason: FlushReason,
    backpressure: &Arc<BackpressureState>,
    _partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
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
        debug!(
            group_id = group_id.get(),
            reason = reason.as_str(),
            batch_size,
            batch_bytes,
            batch_records,
            batch_age_us = batch_age.as_micros() as u64,
            "Batch flush (actor mode)"
        );
    }

    // --- Instrumented flush path ---
    let t0 = std::time::Instant::now();

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
    // Use the cached atomic flag (~1ns) instead of async round-trip (~300μs)
    // through the partition actor command channel. Staleness is bounded by
    // tick interval and is safe — propose_batch handles not-leader errors.
    #[allow(clippy::cast_possible_truncation)]
    batcher_stats.flush_router_us.fetch_add(
        t0.elapsed().as_micros() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );

    if !partition_handle.is_leader_cached() {
        batcher_stats.record_not_leader();
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

    // Read base_offset from the partition actor's cached atomic (~1 ns)
    // instead of acquiring partition_storage RwLock reads (was ~813 us
    // avg due to contention with output processor write locks).
    // The partition actor's patch_batch_base_offset() will override
    // this with its own next_base_offset tracker if stale.
    let t1 = std::time::Instant::now();
    let base_offset = partition_handle.blob_end_offset_cached();
    #[allow(clippy::cast_possible_truncation)]
    batcher_stats.flush_storage_lock_us.fetch_add(
        t1.elapsed().as_micros() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );

    // Encode the batch command with split metadata/payload.
    let t2 = std::time::Instant::now();
    let command = PartitionCommand::AppendBlobBatch {
        blobs: batch.blobs,
        base_offset,
    };
    let (command_metadata, command_payload) = command.encode_split();
    #[allow(clippy::cast_possible_truncation)]
    batcher_stats.flush_encode_us.fetch_add(
        t2.elapsed().as_micros() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );

    #[allow(clippy::cast_possible_truncation)]
    let batch_info = BatchProposalInfo {
        first_request_at: batch.first_request_time,
        batch_size: batch_size as u32,
        batch_bytes,
        total_records: batch_records,
        record_counts: batch.record_counts,
        result_txs: batch.result_txs,
    };

    // Propose batch to the partition actor.
    let t3 = std::time::Instant::now();
    let propose_result = partition_handle
        .propose_batch(command_metadata, command_payload, batch_info)
        .await;
    #[allow(clippy::cast_possible_truncation)]
    batcher_stats.flush_propose_us.fetch_add(
        t3.elapsed().as_micros() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );

    if let Err(e) = propose_result {
        warn!(
            group_id = group_id.get(),
            error = %e,
            "Failed to send batch to partition actor"
        );
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
        batch_size, "Batch sent to partition actor"
    );

    // Note: batch_pending_proposals is no longer used in actor mode.
    // The partition actor owns the pending proposals and passes them
    // through EntryCommitted.batch_notify to the output processor.
    let _ = batch_pending_proposals; // Silence unused warning.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_storage::ServerPartitionStorage;
    use crate::service::partition_actor::{spawn_partition_actor, PartitionActorConfig};
    use helix_core::NodeId;
    use helix_raft::{RaftConfig, RaftNode};
    use helix_wal::TokioStorage;

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
        let partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let config = BatcherConfig {
            linger_ms: 1,
            max_batch_bytes: 64 * 1024,
            max_batch_requests: 100,
        };

        // Spawn batcher task.
        let batcher_task = tokio::spawn(batcher_task_actor::<TokioStorage>(
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
        let partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let config = BatcherConfig {
            linger_ms: 1,
            max_batch_bytes: 64 * 1024,
            max_batch_requests: 100,
        };

        // Spawn batcher task.
        let batcher_task = tokio::spawn(batcher_task_actor::<TokioStorage>(
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
        let partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let config = BatcherConfig {
            linger_ms: 1,
            max_batch_bytes: 64 * 1024,
            max_batch_requests: 100,
        };

        let batcher_task = tokio::spawn(batcher_task_actor::<TokioStorage>(
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
