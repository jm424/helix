//! Snapshot infrastructure for Raft group state machines.
//!
//! This module provides the generic snapshot machinery used by all Raft group
//! types: the controller, offset groups, and data partitions.
//!
//! # Design
//!
//! Every Raft group's state machine implements [`Snapshottable`]. A single
//! [`SnapshotStore`] persists snapshots to local disk and uploads them to S3
//! via a background worker. A [`SnapshotCoordinator`] tracks when the next
//! snapshot is due for a given group.
//!
//! See `docs/design/fresh-node-recovery-architecture.md` for the full design.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use helix_core::GroupId;
use helix_tier::{ObjectKey, ObjectStorage};
use tokio::sync::{mpsc, Notify};
use tracing::{debug, info, warn};

// ============================================================================
// SnapshotError
// ============================================================================

/// Errors that can occur during snapshot operations.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// CRC32 checksum mismatch — the snapshot data is corrupted.
    #[error("snapshot CRC mismatch: expected {expected:#010x}, got {got:#010x}")]
    ChecksumMismatch {
        /// Expected CRC stored in the snapshot envelope.
        expected: u32,
        /// CRC computed over the received bytes.
        got: u32,
    },

    /// The snapshot magic bytes do not match the expected value for this group type.
    #[error("snapshot bad magic bytes")]
    BadMagic,

    /// The snapshot was written by an unsupported format version.
    #[error("snapshot unsupported version: {version}")]
    UnknownVersion {
        /// The version number found in the snapshot envelope.
        version: u32,
    },

    /// The snapshot data ended before all expected fields were read.
    #[error("snapshot truncated: {context}")]
    Truncated {
        /// Description of which field or section was missing.
        context: String,
    },

    /// An I/O error occurred while reading or writing the snapshot file.
    #[error("snapshot I/O error: {source}")]
    Io {
        /// The underlying I/O error.
        #[from]
        source: std::io::Error,
    },
}

// ============================================================================
// SnapshotMeta
// ============================================================================

/// Metadata written in the common snapshot envelope.
///
/// The envelope is parsed by [`SnapshotStore`]. The body bytes are opaque to
/// the store and are parsed by the [`Snapshottable`] implementation. `group_id`
/// is included so the store can validate that the file belongs to the expected
/// group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMeta {
    /// Raft log index of the last entry included in this snapshot.
    pub last_included_index: u64,
    /// Raft term of the last entry included in this snapshot.
    pub last_included_term: u64,
    /// `SharedWAL` index of the last entry included in this snapshot.
    ///
    /// Used by `SharedWal::set_snapshot_floor` to advance the WAL eviction
    /// floor after a successful snapshot. For dedicated-WAL groups this
    /// equals `last_included_index`.
    pub last_included_wal_index: u64,
    /// The Raft group this snapshot belongs to.
    pub group_id: GroupId,
}

// ============================================================================
// Snapshottable
// ============================================================================

/// Implemented by every Raft group's application state machine.
///
/// There is one implementation per group type: `ControllerState`,
/// `OffsetGroupState`, and `DurablePartition`. The calling convention,
/// trigger logic, persistence, and transfer are identical across all three.
///
/// # Invariants
///
/// - `take_snapshot` is called only after entries are applied, so the snapshot
///   always captures committed state.
/// - `apply_snapshot` fully replaces current state. Callers must not apply
///   entries with index ≤ `meta.last_included_index` after this returns.
/// - After a successful snapshot at WAL index W, `min_required_wal_index`
///   returns `Some(W + 1)`.
pub trait Snapshottable {
    /// Serializes the committed state at the current applied index.
    ///
    /// Returns `(meta, bytes)` where `meta` contains the Raft index/term and
    /// WAL index for floor computation. The `bytes` are the opaque body
    /// written into the snapshot envelope by [`SnapshotStore`].
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] if serialization fails.
    fn take_snapshot(&self) -> Result<(SnapshotMeta, Bytes), SnapshotError>;

    /// Replaces current state entirely from a snapshot.
    ///
    /// After this call the state machine is in the same state as the node that
    /// created the snapshot at `meta.last_included_index`. All previous state
    /// is discarded.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] if the data is corrupt or cannot be decoded.
    fn apply_snapshot(&mut self, meta: &SnapshotMeta, data: &[u8]) -> Result<(), SnapshotError>;

    /// The minimum WAL index this group still needs for correct recovery.
    ///
    /// After a successful snapshot at WAL index W, returns `Some(W + 1)`.
    /// Before any snapshot has been taken, returns `None`, meaning full WAL
    /// replay is required.
    ///
    /// The WAL layer uses `min(g.min_required_wal_index() for all groups g)`
    /// to decide which segments can be evicted.
    fn min_required_wal_index(&self) -> Option<u64>;

    /// Updates the snapshot WAL floor on the leader after a successful save.
    ///
    /// Called by the tick loop's async save callback so that
    /// `min_required_wal_index` returns the correct value on leaders (not
    /// only on followers that receive `apply_snapshot`).
    fn set_last_snapshot_wal_index(&mut self, wal_index: u64);
}

// ============================================================================
// SnapshotConfig
// ============================================================================

/// Controls when snapshots are taken for a Raft group.
///
/// A snapshot fires when *both* thresholds are met: the entry count has
/// crossed `entry_threshold` *and* `interval` has elapsed since the last
/// snapshot. This prevents thrashing on bursts while ensuring snapshots happen
/// regularly under low write rates.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Take a snapshot after this many entries have been applied since the
    /// last snapshot.
    pub entry_threshold: u64,
    /// Minimum time between snapshots. The entry threshold must also be met.
    pub interval: Duration,
}

impl SnapshotConfig {
    /// Configuration for the controller group (small state, fast to serialize).
    #[must_use]
    pub const fn for_controller() -> Self {
        Self {
            entry_threshold: 1_000,
            interval: Duration::from_secs(60),
        }
    }

    /// Configuration for offset groups (medium-sized offsets map).
    #[must_use]
    pub const fn for_offset_group() -> Self {
        Self {
            entry_threshold: 10_000,
            interval: Duration::from_secs(60),
        }
    }

    /// Configuration for data partitions (`BlobIndex` only — small).
    #[must_use]
    pub const fn for_data_partition() -> Self {
        Self {
            entry_threshold: 5_000,
            interval: Duration::from_secs(60),
        }
    }

    /// Configuration for eagerly snapshotting a newly created group.
    ///
    /// Fires after the very first applied entry so the WAL eviction floor is
    /// not pinned at 0 by a fresh partition. The caller switches back to the
    /// normal config after the first snapshot is confirmed.
    #[must_use]
    pub const fn eager_first_snapshot() -> Self {
        Self {
            entry_threshold: 1,
            interval: Duration::ZERO,
        }
    }
}

// ============================================================================
// SnapshotCoordinator
// ============================================================================

/// Tracks when a snapshot is due for a single Raft group.
///
/// Lives alongside the state machine it guards. The tick loop calls
/// `record_entry` on every commit and checks `should_snapshot` after each
/// batch. A snapshot fires when both the entry count *and* time interval
/// thresholds are met.
///
/// After a successful snapshot the caller must call `mark_completed` to reset
/// the counters.
#[derive(Debug)]
pub struct SnapshotCoordinator {
    /// Entries applied since the last completed snapshot.
    entries_since_snapshot: u64,
    /// Timestamp of the last completed snapshot, or coordinator creation time.
    last_snapshot_time: Instant,
    /// Thresholds controlling snapshot frequency.
    config: SnapshotConfig,
}

impl SnapshotCoordinator {
    /// Creates a new coordinator with the given config.
    ///
    /// The interval timer begins from construction time, so the first snapshot
    /// cannot fire until at least `config.interval` has elapsed (unless
    /// `config.interval` is `Duration::ZERO`).
    #[must_use]
    pub fn new(config: SnapshotConfig) -> Self {
        Self {
            entries_since_snapshot: 0,
            last_snapshot_time: Instant::now(),
            config,
        }
    }

    /// Records that one entry has been applied to the state machine.
    ///
    /// Call this after every committed entry, before calling
    /// `should_snapshot`.
    pub const fn record_entry(&mut self) {
        self.entries_since_snapshot = self.entries_since_snapshot.saturating_add(1);
    }

    /// Returns `true` if both the entry count and time interval thresholds
    /// are met and a snapshot should be taken now.
    #[must_use]
    pub fn should_snapshot(&self) -> bool {
        self.entries_since_snapshot >= self.config.entry_threshold
            && self.last_snapshot_time.elapsed() >= self.config.interval
    }

    /// Resets the coordinator after a successful snapshot.
    ///
    /// Zeroes the entry counter and restarts the interval timer. Must be
    /// called exactly once per successful snapshot.
    pub fn mark_completed(&mut self) {
        self.entries_since_snapshot = 0;
        self.last_snapshot_time = Instant::now();
    }

    /// Number of entries applied since the last snapshot.
    ///
    /// Used by the graceful shutdown path to decide whether a force-snapshot
    /// is needed before draining the upload worker.
    #[must_use]
    pub const fn entries_since_snapshot(&self) -> u64 {
        self.entries_since_snapshot
    }
}

// ============================================================================
// Constants
// ============================================================================

/// Format version written in every snapshot envelope.
const SNAPSHOT_ENVELOPE_VERSION: u32 = 1;

/// Byte count of the fixed-size envelope header (excluding body and trailing CRC).
///
/// Layout: `version(4) + group_id(8) + last_index(8) + last_term(8) + last_wal_index(8) + body_len(4)`.
const SNAPSHOT_ENVELOPE_HEADER_SIZE: usize = 40;

/// Capacity of the bounded S3 upload channel.
///
/// Bounded to apply backpressure: if the worker falls behind, `save()` will
/// skip S3 upload rather than blocking the tick loop.
const UPLOAD_CHANNEL_CAPACITY: usize = 16;

/// Number of local snapshot files to retain per group.
///
/// The two most-recent are kept so a failed write of the latest does not
/// leave the group with no local copy.
const LOCAL_SNAPSHOT_RETENTION: usize = 2;

/// Initial retry backoff for failed S3 uploads (milliseconds).
const UPLOAD_BACKOFF_INITIAL_MS: u64 = 100;

/// Maximum retry backoff for failed S3 uploads (milliseconds).
const UPLOAD_BACKOFF_MAX_MS: u64 = 5_000;

// ============================================================================
// Envelope helpers
// ============================================================================

/// Wraps `body` in the common snapshot envelope for a given `meta`.
///
/// Format (little-endian):
/// ```text
/// [version: u32][group_id: u64][last_included_index: u64]
/// [last_included_term: u64][last_included_wal_index: u64]
/// [body_len: u32][body bytes][crc32: u32]
/// ```
///
/// The CRC32 covers all bytes preceding it (header + body).
pub(crate) fn encode_envelope(meta: &SnapshotMeta, body: &Bytes) -> Bytes {
    assert!(
        u32::try_from(body.len()).is_ok(),
        "snapshot body too large: {} bytes",
        body.len(),
    );
    #[allow(clippy::cast_possible_truncation)] // asserted above
    let body_len = body.len() as u32;
    let capacity = SNAPSHOT_ENVELOPE_HEADER_SIZE + body.len() + 4;
    let mut buf = BytesMut::with_capacity(capacity);
    buf.put_u32_le(SNAPSHOT_ENVELOPE_VERSION);
    buf.put_u64_le(meta.group_id.get());
    buf.put_u64_le(meta.last_included_index);
    buf.put_u64_le(meta.last_included_term);
    buf.put_u64_le(meta.last_included_wal_index);
    buf.put_u32_le(body_len);
    buf.put_slice(body);
    let checksum = crc32fast::hash(&buf);
    buf.put_u32_le(checksum);
    buf.freeze()
}

/// Decodes a snapshot envelope produced by [`encode_envelope`].
///
/// Verifies the CRC32, then returns `(meta, body)`.
///
/// # Errors
///
/// Returns [`SnapshotError`] if the data is truncated, corrupted, or uses an
/// unknown version.
pub(crate) fn decode_envelope(data: &[u8]) -> Result<(SnapshotMeta, Bytes), SnapshotError> {
    // Minimum: 40 header bytes + 0 body + 4 CRC = 44 bytes.
    if data.len() < SNAPSHOT_ENVELOPE_HEADER_SIZE + 4 {
        return Err(SnapshotError::Truncated {
            context: format!(
                "envelope: need {} bytes, got {}",
                SNAPSHOT_ENVELOPE_HEADER_SIZE + 4,
                data.len(),
            ),
        });
    }
    let (payload, crc_slice) = data.split_at(data.len() - 4);
    let expected_crc =
        u32::from_le_bytes(crc_slice.try_into().expect("4 bytes from split_at"));
    let got_crc = crc32fast::hash(payload);
    if got_crc != expected_crc {
        return Err(SnapshotError::ChecksumMismatch {
            expected: expected_crc,
            got: got_crc,
        });
    }

    let version = u32::from_le_bytes(payload[0..4].try_into().expect("4 bytes"));
    if version != SNAPSHOT_ENVELOPE_VERSION {
        return Err(SnapshotError::UnknownVersion { version });
    }
    let group_id_raw = u64::from_le_bytes(payload[4..12].try_into().expect("8 bytes"));
    let last_included_index = u64::from_le_bytes(payload[12..20].try_into().expect("8 bytes"));
    let last_included_term = u64::from_le_bytes(payload[20..28].try_into().expect("8 bytes"));
    let last_included_wal_index =
        u64::from_le_bytes(payload[28..36].try_into().expect("8 bytes"));
    let body_len = u32::from_le_bytes(payload[36..40].try_into().expect("4 bytes")) as usize;

    if payload.len() < SNAPSHOT_ENVELOPE_HEADER_SIZE + body_len {
        return Err(SnapshotError::Truncated {
            context: format!(
                "body: need {body_len} bytes, got {}",
                payload.len() - SNAPSHOT_ENVELOPE_HEADER_SIZE,
            ),
        });
    }
    let body = Bytes::copy_from_slice(
        &payload[SNAPSHOT_ENVELOPE_HEADER_SIZE..SNAPSHOT_ENVELOPE_HEADER_SIZE + body_len],
    );
    Ok((
        SnapshotMeta {
            group_id: GroupId::new(group_id_raw),
            last_included_index,
            last_included_term,
            last_included_wal_index,
        },
        body,
    ))
}

/// Returns the filename for a snapshot with the given WAL index.
fn snap_filename(wal_index: u64) -> String {
    format!("snap_{wal_index:016x}.bin")
}

/// Extracts the WAL index from a snapshot filename (`snap_{index:016x}.bin`).
///
/// Returns `None` for any name that does not match the pattern.
fn wal_index_from_filename(name: &str) -> Option<u64> {
    // Accept both bare filenames and full paths.
    let base = name.rsplit('/').next().unwrap_or(name);
    let hex = base.strip_prefix("snap_")?.strip_suffix(".bin")?;
    u64::from_str_radix(hex, 16).ok()
}

// ============================================================================
// SnapshotUpload
// ============================================================================

/// A pending S3 upload enqueued by [`SnapshotStore::save`].
///
/// The background upload worker processes these in order, keeping only the
/// latest snapshot per group key when multiple uploads are queued.
pub(crate) struct SnapshotUpload {
    /// S3 object key for the snapshot.
    pub(crate) key: ObjectKey,
    /// Raw snapshot bytes (common envelope + body).
    pub(crate) data: Bytes,
}

// ============================================================================
// SnapshotStore
// ============================================================================

/// Unified snapshot store for all Raft group types.
///
/// Persists snapshots to local disk, then asynchronously uploads them to S3
/// via a bounded background channel. S3 is the durable store; local disk is
/// an ephemeral cache that accelerates warm starts.
///
/// One `SnapshotStore` is shared across all group types. It is parameterized
/// by group ID, not by group type — there is no per-type store.
///
/// # Local path
///
/// `{data_dir}/snapshots/{group_id:016x}/snap_{wal_index:016x}.bin`
///
/// # S3 key
///
/// `{s3_prefix}snapshots/{group_id:016x}/snap_{wal_index:016x}.bin`
///
/// # S3 on the critical path
///
/// None. `save()` writes to local disk and enqueues to a bounded channel. The
/// S3 PUT runs in the background and never blocks the tick loop. S3 is only
/// accessed at startup (recovery) and at graceful shutdown (`drain()`).
pub struct SnapshotStore {
    /// Root directory under which per-group snapshot directories are created.
    data_dir: PathBuf,
    /// S3 key prefix, e.g. `"node-0/"`.
    s3_prefix: String,
    /// S3 backend (or simulated equivalent for DST).
    remote: Arc<dyn ObjectStorage>,
    /// Bounded channel to the background S3 upload worker.
    ///
    /// Wrapped in `Mutex<Option<…>>` so `drain()` can take the sender,
    /// signalling EOF to the worker without requiring ownership of `self`.
    upload_tx: std::sync::Mutex<Option<mpsc::Sender<SnapshotUpload>>>,
    /// Notified by the upload worker when its queue is fully drained.
    ///
    /// `drain()` drops `upload_tx` to signal EOF, then awaits this.
    drain_notify: Arc<Notify>,
}

impl SnapshotStore {
    /// Creates a new store and spawns the background S3 upload worker.
    ///
    /// Must be called from within a Tokio runtime context.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime (due to `tokio::spawn`).
    pub fn new(data_dir: PathBuf, s3_prefix: String, remote: Arc<dyn ObjectStorage>) -> Self {
        let (upload_tx, upload_rx) = mpsc::channel(UPLOAD_CHANNEL_CAPACITY);
        let drain_notify = Arc::new(Notify::new());
        tokio::spawn(snapshot_upload_worker(
            upload_rx,
            Arc::clone(&remote),
            Arc::clone(&drain_notify),
        ));
        Self {
            data_dir,
            s3_prefix,
            remote,
            upload_tx: std::sync::Mutex::new(Some(upload_tx)),
            drain_notify,
        }
    }

    /// Persists a snapshot to local disk and enqueues it for S3 upload.
    ///
    /// Writes atomically via a `.tmp` rename. S3 upload is best-effort:
    /// if the upload channel is full the request is silently dropped
    /// (the next snapshot will supersede it).
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Io`] if the local disk write fails.
    ///
    /// # Panics
    ///
    /// Panics if the internal `upload_tx` mutex is poisoned (unreachable in
    /// practice; only occurs if a thread panicked while holding the lock).
    pub async fn save(&self, meta: &SnapshotMeta, body: &Bytes) -> Result<(), SnapshotError> {
        let envelope = encode_envelope(meta, body);
        let group_dir = self.group_dir(meta.group_id);
        std::fs::create_dir_all(&group_dir)?;

        let filename = snap_filename(meta.last_included_wal_index);
        let final_path = group_dir.join(&filename);
        let tmp_path = group_dir.join(format!("{filename}.tmp"));

        // Atomic write: write to .tmp, then rename.
        std::fs::write(&tmp_path, &envelope)?;
        std::fs::rename(&tmp_path, &final_path)?;

        // Enqueue S3 upload (non-blocking; skip if channel is full).
        // Block scope ensures the MutexGuard is dropped before the next await.
        let key = self.s3_key(meta.group_id, meta.last_included_wal_index);
        {
            let guard = self.upload_tx.lock().expect("upload_tx lock poisoned");
            if let Some(tx) = guard.as_ref() {
                if tx.try_send(SnapshotUpload { key, data: envelope }).is_err() {
                    warn!(
                        group_id = meta.group_id.get(),
                        "S3 upload channel full; snapshot upload skipped"
                    );
                }
            }
        } // guard dropped here, before prune_local

        self.prune_local(meta.group_id).await;
        Ok(())
    }

    /// Returns the latest valid snapshot for `group_id` from local disk.
    ///
    /// Tries files from newest to oldest, skipping corrupt ones. This function
    /// performs blocking I/O and should only be called at startup.
    #[must_use]
    pub fn load_local(&self, group_id: GroupId) -> Option<(SnapshotMeta, Bytes)> {
        let group_dir = self.group_dir(group_id);
        let entries = std::fs::read_dir(&group_dir).ok()?;
        let mut candidates: Vec<u64> = entries
            .filter_map(std::result::Result::ok)
            .filter_map(|e| wal_index_from_filename(&e.file_name().to_string_lossy()))
            .collect();
        // Sort descending: try newest first.
        candidates.sort_unstable_by(|a, b| b.cmp(a));
        for wal_index in candidates {
            let path = group_dir.join(snap_filename(wal_index));
            match std::fs::read(&path) {
                Ok(data) => match decode_envelope(&data) {
                    Ok(pair) => return Some(pair),
                    Err(e) => warn!(?path, error = %e, "Corrupt local snapshot; skipping"),
                },
                Err(e) => warn!(?path, error = %e, "Cannot read local snapshot; skipping"),
            }
        }
        None
    }

    /// Downloads the latest valid snapshot for `group_id` from S3.
    ///
    /// Lists the S3 prefix for the group, then fetches the most recent file.
    /// Returns `None` if no remote snapshot exists or all are corrupt.
    pub async fn load_remote(&self, group_id: GroupId) -> Option<(SnapshotMeta, Bytes)> {
        let prefix = self.s3_prefix_for_group(group_id);
        let keys = match self.remote.list(&prefix).await {
            Ok(k) => k,
            Err(e) => {
                warn!(group_id = group_id.get(), error = %e, "Failed to list remote snapshots");
                return None;
            }
        };
        let mut candidates: Vec<(u64, ObjectKey)> = keys
            .into_iter()
            .filter_map(|k| Some((wal_index_from_filename(k.as_str())?, k)))
            .collect();
        // Sort descending: try newest first.
        candidates.sort_unstable_by(|a, b| b.0.cmp(&a.0));
        for (_, key) in candidates {
            match self.remote.get(&key).await {
                Ok(data) => match decode_envelope(&data) {
                    Ok(pair) => return Some(pair),
                    Err(e) => warn!(%key, error = %e, "Corrupt remote snapshot; skipping"),
                },
                Err(e) => warn!(%key, error = %e, "Failed to fetch remote snapshot; skipping"),
            }
        }
        None
    }

    /// Returns the latest snapshot for `group_id`, preferring local disk.
    ///
    /// Falls back to S3 if no valid local snapshot exists.
    /// Returns `(meta, body, from_remote)` where `from_remote` is `true`
    /// when the snapshot was fetched from S3.
    pub async fn load(&self, group_id: GroupId) -> Option<(SnapshotMeta, Bytes, bool)> {
        if let Some((meta, body)) = self.load_local(group_id) {
            debug!(
                group_id = group_id.get(),
                last_index = meta.last_included_index,
                "Loaded snapshot from local disk"
            );
            return Some((meta, body, false));
        }
        if let Some((meta, body)) = self.load_remote(group_id).await {
            info!(
                group_id = group_id.get(),
                last_index = meta.last_included_index,
                "Loaded snapshot from S3 (no local copy)"
            );
            return Some((meta, body, true));
        }
        None
    }

    /// Signals the background upload worker to stop and waits until all queued
    /// uploads have completed.
    ///
    /// Should be called once at graceful shutdown. After this returns, all
    /// enqueued S3 uploads are confirmed durable.
    ///
    /// # Panics
    ///
    /// Panics if the internal `upload_tx` mutex is poisoned (unreachable in
    /// practice; only occurs if a thread panicked while holding the lock).
    pub async fn drain(&self) {
        // Drop the sender to signal EOF to the worker.
        drop(self.upload_tx.lock().expect("upload_tx lock poisoned").take());
        // Wait for the worker to finish all pending uploads and notify.
        let notify = Arc::clone(&self.drain_notify);
        notify.notified().await;
    }

    /// Returns the local directory for a group's snapshot files.
    fn group_dir(&self, group_id: GroupId) -> PathBuf {
        self.data_dir
            .join("snapshots")
            .join(format!("{:016x}", group_id.get()))
    }

    /// Returns the S3 key for a snapshot at `wal_index`.
    fn s3_key(&self, group_id: GroupId, wal_index: u64) -> ObjectKey {
        ObjectKey::new(format!(
            "{}snapshots/{:016x}/{}",
            self.s3_prefix,
            group_id.get(),
            snap_filename(wal_index),
        ))
    }

    /// Returns the S3 key prefix for listing a group's snapshots.
    fn s3_prefix_for_group(&self, group_id: GroupId) -> String {
        format!("{}snapshots/{:016x}/", self.s3_prefix, group_id.get())
    }

    /// Returns the group IDs for which at least one local snapshot file exists.
    ///
    /// Scans the `{data_dir}/snapshots/` directory for per-group subdirectories
    /// and returns all group IDs that have valid hex-named directories. Used at
    /// startup to enumerate which groups have snapshots without knowing group
    /// assignments in advance (e.g., single-node and DST paths).
    #[must_use]
    pub fn list_local_group_ids(&self) -> Vec<GroupId> {
        let snapshots_dir = self.data_dir.join("snapshots");
        let Ok(entries) = std::fs::read_dir(&snapshots_dir) else {
            return Vec::new();
        };
        entries
            .filter_map(std::result::Result::ok)
            .filter_map(|e| {
                let name = e.file_name();
                let s = name.to_string_lossy();
                u64::from_str_radix(s.as_ref(), 16).ok().map(GroupId::new)
            })
            .collect()
    }

    /// Returns the group IDs for which at least one remote snapshot exists.
    ///
    /// Lists S3 keys under `{s3_prefix}snapshots/` and parses group IDs from
    /// the hex directory component. Used at startup to discover which groups
    /// have snapshots without relying on controller state.
    pub async fn list_remote_group_ids(&self) -> Vec<GroupId> {
        let prefix = format!("{}snapshots/", self.s3_prefix);
        let keys = match self.remote.list(&prefix).await {
            Ok(k) => k,
            Err(e) => {
                warn!(error = %e, "Failed to list remote snapshots");
                return Vec::new();
            }
        };
        // S3 key format: {s3_prefix}snapshots/{group_id_hex}/snap_{wal_index}.bin
        // Extract the hex directory component after "snapshots/".
        let mut group_ids: Vec<GroupId> = keys
            .iter()
            .filter_map(|k| {
                let after_prefix = k.as_str().strip_prefix(prefix.as_str())?;
                let hex_part = after_prefix.split('/').next()?;
                u64::from_str_radix(hex_part, 16).ok().map(GroupId::new)
            })
            .collect();
        group_ids.sort_unstable();
        group_ids.dedup();
        group_ids
    }

    /// Deletes the oldest local snapshot files, keeping only the most recent
    /// `LOCAL_SNAPSHOT_RETENTION` files per group.
    #[allow(clippy::unused_async)]
    async fn prune_local(&self, group_id: GroupId) {
        let group_dir = self.group_dir(group_id);
        let Ok(entries) = std::fs::read_dir(&group_dir) else { return };
        let mut candidates: Vec<(u64, std::path::PathBuf)> = entries
            .filter_map(std::result::Result::ok)
            .filter_map(|e| {
                let idx = wal_index_from_filename(&e.file_name().to_string_lossy())?;
                Some((idx, e.path()))
            })
            .collect();
        // Sort ascending: oldest first for removal.
        candidates.sort_unstable_by_key(|(idx, _)| *idx);
        while candidates.len() > LOCAL_SNAPSHOT_RETENTION {
            let (_, old_path) = candidates.remove(0);
            if let Err(e) = std::fs::remove_file(&old_path) {
                warn!(?old_path, error = %e, "Failed to prune old snapshot");
            }
        }
    }
}

// ============================================================================
// Background upload worker
// ============================================================================

/// Background task that processes the S3 upload queue.
///
/// Deduplicates uploads: if multiple snapshots for the same S3 key are queued
/// before the worker drains them, only the latest is uploaded. This prevents
/// wasted bandwidth when snapshots are taken faster than S3 can accept them.
///
/// Retries failed uploads with exponential backoff capped at
/// `UPLOAD_BACKOFF_MAX_MS`. On channel close (`drain()`), flushes remaining
/// uploads before signalling `drain_notify`.
async fn snapshot_upload_worker(
    mut rx: mpsc::Receiver<SnapshotUpload>,
    remote: Arc<dyn ObjectStorage>,
    drain_notify: Arc<Notify>,
) {
    // Keyed by S3 key string; holds the latest upload for each key.
    let mut pending: HashMap<String, SnapshotUpload> = HashMap::new();
    let mut closed = false;

    loop {
        // Phase 1: drain all available messages without blocking (dedup by key).
        loop {
            match rx.try_recv() {
                Ok(u) => {
                    pending.insert(u.key.as_str().to_string(), u);
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    closed = true;
                    break;
                }
            }
        }

        // Phase 2: upload all pending entries (retry with backoff on failure).
        for (_, upload) in pending.drain() {
            upload_with_backoff(&*remote, &upload.key, upload.data).await;
        }

        if closed {
            drain_notify.notify_one();
            return;
        }

        // Phase 3: wait for the next message (or channel close).
        if let Some(u) = rx.recv().await {
            pending.insert(u.key.as_str().to_string(), u);
        } else {
            drain_notify.notify_one();
            return;
        }
    }
}

/// Uploads `data` to `key`, retrying with exponential backoff on failure.
async fn upload_with_backoff(remote: &dyn ObjectStorage, key: &ObjectKey, data: Bytes) {
    let mut backoff_ms = UPLOAD_BACKOFF_INITIAL_MS;
    loop {
        match remote.put(key, data.clone()).await {
            Ok(()) => {
                debug!(%key, "Snapshot uploaded to S3");
                return;
            }
            Err(e) => {
                warn!(%key, error = %e, backoff_ms, "S3 snapshot upload failed, retrying");
                tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(UPLOAD_BACKOFF_MAX_MS);
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // SnapshotCoordinator
    // -------------------------------------------------------------------------

    #[test]
    fn test_coordinator_does_not_fire_before_threshold() {
        let config = SnapshotConfig {
            entry_threshold: 3,
            interval: Duration::ZERO,
        };
        let mut coord = SnapshotCoordinator::new(config);

        coord.record_entry();
        assert!(!coord.should_snapshot(), "should not fire at 1 of 3 entries");

        coord.record_entry();
        assert!(!coord.should_snapshot(), "should not fire at 2 of 3 entries");
    }

    #[test]
    fn test_coordinator_fires_when_both_conditions_met() {
        let config = SnapshotConfig {
            entry_threshold: 3,
            interval: Duration::ZERO, // interval already satisfied
        };
        let mut coord = SnapshotCoordinator::new(config);

        coord.record_entry();
        coord.record_entry();
        coord.record_entry();

        assert!(
            coord.should_snapshot(),
            "should fire once entry threshold and zero interval are met"
        );
    }

    #[test]
    fn test_coordinator_does_not_fire_before_interval() {
        let config = SnapshotConfig {
            entry_threshold: 1,
            interval: Duration::from_secs(3600), // will not elapse in the test
        };
        let mut coord = SnapshotCoordinator::new(config);

        coord.record_entry();

        assert!(
            !coord.should_snapshot(),
            "should not fire when interval has not elapsed"
        );
    }

    #[test]
    fn test_coordinator_reset_after_mark_completed() {
        let config = SnapshotConfig {
            entry_threshold: 2,
            interval: Duration::ZERO,
        };
        let mut coord = SnapshotCoordinator::new(config);

        coord.record_entry();
        coord.record_entry();
        assert!(coord.should_snapshot());

        coord.mark_completed();

        assert_eq!(coord.entries_since_snapshot(), 0);
        assert!(
            !coord.should_snapshot(),
            "should not fire immediately after mark_completed"
        );
    }

    #[test]
    fn test_coordinator_eager_first_snapshot() {
        let mut coord = SnapshotCoordinator::new(SnapshotConfig::eager_first_snapshot());

        assert!(!coord.should_snapshot(), "fires only after record_entry");
        coord.record_entry();
        assert!(
            coord.should_snapshot(),
            "eager config fires after exactly one entry"
        );
    }

    #[test]
    fn test_coordinator_saturating_add_does_not_overflow() {
        let config = SnapshotConfig {
            entry_threshold: 1,
            interval: Duration::ZERO,
        };
        let mut coord = SnapshotCoordinator::new(config);
        coord.entries_since_snapshot = u64::MAX;

        coord.record_entry(); // saturating_add must not panic

        assert_eq!(coord.entries_since_snapshot(), u64::MAX);
    }

    // -------------------------------------------------------------------------
    // Envelope encode / decode
    // -------------------------------------------------------------------------

    fn make_meta(group_id: u64, index: u64, term: u64, wal_index: u64) -> SnapshotMeta {
        SnapshotMeta {
            group_id: GroupId::new(group_id),
            last_included_index: index,
            last_included_term: term,
            last_included_wal_index: wal_index,
        }
    }

    #[test]
    fn test_encode_decode_roundtrip_empty_body() {
        let meta = make_meta(1, 100, 3, 200);
        let body = Bytes::new();
        let encoded = encode_envelope(&meta, &body);
        let (decoded_meta, decoded_body) = decode_envelope(&encoded).expect("decode failed");
        assert_eq!(decoded_meta, meta);
        assert_eq!(decoded_body, body);
    }

    #[test]
    fn test_encode_decode_roundtrip_with_body() {
        let meta = make_meta(42, 9999, 7, 10000);
        let body = Bytes::from_static(b"hello snapshot world");
        let encoded = encode_envelope(&meta, &body);
        let (decoded_meta, decoded_body) = decode_envelope(&encoded).expect("decode failed");
        assert_eq!(decoded_meta, meta);
        assert_eq!(decoded_body, body);
    }

    #[test]
    fn test_decode_crc_mismatch() {
        let meta = make_meta(1, 1, 1, 1);
        let body = Bytes::from_static(b"data");
        let mut encoded = encode_envelope(&meta, &body).to_vec();
        // Flip the last byte of the CRC.
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;
        let err = decode_envelope(&encoded).unwrap_err();
        assert!(
            matches!(err, SnapshotError::ChecksumMismatch { .. }),
            "expected ChecksumMismatch, got {err}"
        );
    }

    #[test]
    fn test_decode_bad_version() {
        let meta = make_meta(1, 1, 1, 1);
        let body = Bytes::from_static(b"data");
        let mut encoded = encode_envelope(&meta, &body).to_vec();
        // Overwrite version (first 4 bytes) with an unknown value.
        encoded[0..4].copy_from_slice(&99u32.to_le_bytes());
        // Recompute CRC so we don't hit ChecksumMismatch first.
        let (payload, _) = encoded.split_at(encoded.len() - 4);
        let new_crc = crc32fast::hash(payload);
        let len = encoded.len();
        encoded[len - 4..].copy_from_slice(&new_crc.to_le_bytes());
        let err = decode_envelope(&encoded).unwrap_err();
        assert!(
            matches!(err, SnapshotError::UnknownVersion { version: 99 }),
            "expected UnknownVersion(99), got {err}"
        );
    }

    #[test]
    fn test_decode_truncated() {
        let err = decode_envelope(&[0u8; 10]).unwrap_err();
        assert!(
            matches!(err, SnapshotError::Truncated { .. }),
            "expected Truncated, got {err}"
        );
    }

    // -------------------------------------------------------------------------
    // Filename helpers
    // -------------------------------------------------------------------------

    #[test]
    fn test_snap_filename_roundtrip() {
        for wal_index in [0, 1, 0xDEADBEEF, u64::MAX] {
            let name = snap_filename(wal_index);
            let parsed = wal_index_from_filename(&name);
            assert_eq!(parsed, Some(wal_index), "roundtrip failed for {wal_index}");
        }
    }

    #[test]
    fn test_wal_index_from_filename_rejects_bad_names() {
        assert_eq!(wal_index_from_filename("controller-snapshot.bin"), None);
        assert_eq!(wal_index_from_filename("snap_.bin"), None); // empty hex
        assert_eq!(wal_index_from_filename("snap_xyz.bin"), None); // non-hex
        assert_eq!(wal_index_from_filename("snapshot.bin"), None);
    }

    // -------------------------------------------------------------------------
    // SnapshotStore save / load_local
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_store_save_and_load_local() {
        use helix_tier::SimulatedObjectStorage;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("tempdir");
        let remote: Arc<dyn ObjectStorage> = Arc::new(SimulatedObjectStorage::new(0));
        let store = SnapshotStore::new(dir.path().to_path_buf(), "node-0/".to_string(), remote);

        let group_id = GroupId::new(7);
        let meta = make_meta(group_id.get(), 500, 4, 600);
        let body = Bytes::from_static(b"partition state bytes");

        store.save(&meta, &body).await.expect("save failed");

        let (loaded_meta, loaded_body) = store
            .load_local(group_id)
            .expect("load_local returned None");
        assert_eq!(loaded_meta, meta);
        assert_eq!(loaded_body, body);
    }

    #[tokio::test]
    async fn test_store_load_returns_none_when_empty() {
        use helix_tier::SimulatedObjectStorage;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("tempdir");
        let remote: Arc<dyn ObjectStorage> = Arc::new(SimulatedObjectStorage::new(0));
        let store = SnapshotStore::new(dir.path().to_path_buf(), "node-0/".to_string(), remote);

        let group_id = GroupId::new(99);
        assert!(store.load_local(group_id).is_none());
        assert!(store.load(group_id).await.is_none());
    }

    #[tokio::test]
    async fn test_store_prunes_old_local_snapshots() {
        use helix_tier::SimulatedObjectStorage;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("tempdir");
        let remote: Arc<dyn ObjectStorage> = Arc::new(SimulatedObjectStorage::new(0));
        let store = SnapshotStore::new(dir.path().to_path_buf(), "node-0/".to_string(), remote);

        let group_id = GroupId::new(5);
        // Save LOCAL_SNAPSHOT_RETENTION + 1 snapshots.
        for i in 0..=(LOCAL_SNAPSHOT_RETENTION as u64) {
            let meta = make_meta(group_id.get(), i, 1, i);
            let body = Bytes::from(vec![u8::try_from(i).unwrap_or(0); 4]);
            store.save(&meta, &body).await.expect("save failed");
        }

        // Directory should contain at most LOCAL_SNAPSHOT_RETENTION files.
        let group_dir = store.group_dir(group_id);
        let count = std::fs::read_dir(&group_dir)
            .expect("read_dir")
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("snap_")
            })
            .count();
        assert!(
            count <= LOCAL_SNAPSHOT_RETENTION,
            "expected at most {LOCAL_SNAPSHOT_RETENTION} files, got {count}"
        );
    }

    #[tokio::test]
    async fn test_store_load_returns_latest_snapshot() {
        use helix_tier::SimulatedObjectStorage;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("tempdir");
        let remote: Arc<dyn ObjectStorage> = Arc::new(SimulatedObjectStorage::new(0));
        let store = SnapshotStore::new(dir.path().to_path_buf(), "node-0/".to_string(), remote);

        let group_id = GroupId::new(3);

        // Save two snapshots; pruning keeps only the latest 2.
        let meta1 = make_meta(group_id.get(), 10, 1, 10);
        let meta2 = make_meta(group_id.get(), 20, 1, 20);
        store
            .save(&meta1, &Bytes::from_static(b"v1"))
            .await
            .expect("save1");
        store
            .save(&meta2, &Bytes::from_static(b"v2"))
            .await
            .expect("save2");

        let (loaded_meta, loaded_body) =
            store.load_local(group_id).expect("load_local returned None");
        // load_local must return the newest snapshot.
        assert_eq!(loaded_meta.last_included_index, 20);
        assert_eq!(&loaded_body[..], b"v2");
    }

    // -------------------------------------------------------------------------
    // SnapshotStore drain
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_store_drain_waits_for_worker() {
        // Verify that drain() returns only after the upload worker has processed
        // all queued uploads.  We use a SimulatedObjectStorage so the PUT is
        // non-blocking, then drain and verify the key is present.
        use helix_tier::SimulatedObjectStorage;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("tempdir");
        let sim: Arc<SimulatedObjectStorage> = Arc::new(SimulatedObjectStorage::new(0));
        let remote: Arc<dyn ObjectStorage> = Arc::clone(&sim) as Arc<dyn ObjectStorage>;
        let store = SnapshotStore::new(dir.path().to_path_buf(), "node-0/".to_string(), remote);

        let group_id = GroupId::new(11);
        let meta = make_meta(group_id.get(), 1, 1, 1);
        let body = Bytes::from_static(b"drain-test");
        store.save(&meta, &body).await.expect("save");

        store.drain().await;

        // After drain the worker must have completed the S3 PUT.
        let key = ObjectKey::new(format!(
            "node-0/snapshots/{:016x}/{}",
            group_id.get(),
            snap_filename(1),
        ));
        assert!(
            sim.get_raw_content(&key).is_some(),
            "snapshot must be in remote store after drain"
        );
    }

    // -------------------------------------------------------------------------
    // SnapshotStore remote fallback
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_store_load_falls_back_to_remote() {
        use helix_tier::SimulatedObjectStorage;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("tempdir");
        let sim = Arc::new(SimulatedObjectStorage::new(0));
        let store = SnapshotStore::new(
            dir.path().to_path_buf(),
            "node-0/".to_string(),
            Arc::clone(&sim) as Arc<dyn ObjectStorage>,
        );

        // Pre-populate S3 directly (simulating a snapshot uploaded by another node).
        let group_id = GroupId::new(42);
        let meta = make_meta(group_id.get(), 5, 2, 7);
        let body = Bytes::from_static(b"remote-snap");
        let envelope = encode_envelope(&meta, &body);
        let key = ObjectKey::new(format!(
            "node-0/snapshots/{:016x}/{}",
            group_id.get(),
            snap_filename(meta.last_included_wal_index),
        ));
        sim.set_raw_content(&key, envelope.to_vec());

        // No local copy exists — load() must fall back to S3.
        assert!(store.load_local(group_id).is_none(), "no local copy");
        let result = store.load(group_id).await;
        assert!(result.is_some(), "load() must find remote snapshot");
        let (loaded_meta, loaded_body, from_remote) = result.unwrap();
        assert!(from_remote, "must be flagged as from_remote");
        assert_eq!(loaded_meta.last_included_index, 5);
        assert_eq!(&loaded_body[..], b"remote-snap");
    }

    // -------------------------------------------------------------------------
    // list_local_group_ids
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_store_list_local_group_ids() {
        use helix_tier::SimulatedObjectStorage;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("tempdir");
        let remote: Arc<dyn ObjectStorage> = Arc::new(SimulatedObjectStorage::new(0));
        let store = SnapshotStore::new(dir.path().to_path_buf(), "node-0/".to_string(), remote);

        // No snapshots yet.
        assert!(store.list_local_group_ids().is_empty());

        // Save snapshots for two groups.
        let gid1 = GroupId::new(1);
        let gid2 = GroupId::new(2);
        store
            .save(&make_meta(gid1.get(), 1, 1, 1), &Bytes::from_static(b"a"))
            .await
            .expect("save g1");
        store
            .save(&make_meta(gid2.get(), 1, 1, 1), &Bytes::from_static(b"b"))
            .await
            .expect("save g2");

        let mut ids = store.list_local_group_ids();
        ids.sort_by_key(|id| id.get());
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], gid1);
        assert_eq!(ids[1], gid2);
    }
}
