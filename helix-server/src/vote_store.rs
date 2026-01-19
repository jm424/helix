//! Vote state persistence for Raft consensus.
//!
//! This module provides durable storage for Raft vote state (`term` and `voted_for`)
//! to prevent double-voting after node restarts. It implements a two-tier approach:
//!
//! - **Local file**: Synchronous writes with fsync for fast recovery from process crashes
//! - **S3 backup**: Async uploads for recovery from disk loss (pod rescheduling)
//!
//! # Design
//!
//! ```text
//! Vote/term change:
//!   ├─→ Local file + fsync (synchronous, ~1-5ms) ← critical path
//!   └─→ S3 upload (async, background)            ← off critical path
//!
//! Startup:
//!   ├─→ Local file exists? Use it (fast path)
//!   └─→ Fetch from S3 → observation mode (safe recovery)
//! ```
//!
//! # Observation Mode
//!
//! When recovering from S3, the state may be stale (async upload was in progress
//! when crash occurred). To prevent double-voting, the node enters "observation mode"
//! for one election timeout, during which it rejects vote requests but learns the
//! current term from heartbeats.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use helix_core::{GroupId, NodeId, TermId};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use helix_tier::{ObjectKey, ObjectStorage, TierError};

// =============================================================================
// Constants
// =============================================================================

/// Magic bytes for vote state file format.
const VOTE_STATE_MAGIC: u32 = 0x564F5445; // "VOTE"

/// Current version of the vote state format.
const VOTE_STATE_VERSION: u32 = 1;

/// Maximum number of groups in a vote state file.
/// Matches GROUPS_PER_NODE_MAX in helix-raft.
const MAX_GROUPS: usize = 10_000;

// =============================================================================
// Errors
// =============================================================================

/// Errors from vote store operations.
#[derive(Debug, Error)]
pub enum VoteStoreError {
    /// I/O error during local file operations.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Remote storage error.
    #[error("remote storage error: {0}")]
    Remote(#[from] TierError),

    /// Invalid vote state format.
    #[error("invalid vote state format: {0}")]
    InvalidFormat(String),

    /// Checksum mismatch.
    #[error("checksum mismatch: expected {expected:#x}, got {actual:#x}")]
    ChecksumMismatch {
        /// Expected checksum value.
        expected: u32,
        /// Actual checksum value.
        actual: u32,
    },

    /// Too many groups.
    #[error("too many groups: {count} exceeds maximum {max}")]
    TooManyGroups {
        /// Actual group count.
        count: usize,
        /// Maximum allowed.
        max: usize,
    },
}

/// Result type for vote store operations.
pub type VoteStoreResult<T> = Result<T, VoteStoreError>;

// =============================================================================
// VoteState
// =============================================================================

/// Vote state for a single Raft group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupVoteState {
    /// The group ID.
    pub group_id: GroupId,
    /// Current term.
    pub term: TermId,
    /// Node voted for in current term (None if haven't voted).
    pub voted_for: Option<NodeId>,
}

impl GroupVoteState {
    /// Creates a new group vote state.
    #[must_use]
    pub const fn new(group_id: GroupId, term: TermId, voted_for: Option<NodeId>) -> Self {
        Self {
            group_id,
            term,
            voted_for,
        }
    }

    /// Serialized size in bytes.
    const fn serialized_size() -> usize {
        8 + 8 + 8 // group_id + term + voted_for
    }
}

/// Persistent vote state for all groups on a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteState {
    /// Monotonically increasing sequence number.
    /// Incremented on every state change.
    pub sequence: u64,
    /// Per-group vote state.
    groups: HashMap<GroupId, GroupVoteState>,
}

impl VoteState {
    /// Creates a new empty vote state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sequence: 0,
            groups: HashMap::new(),
        }
    }

    /// Returns the number of groups.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Gets vote state for a group.
    #[must_use]
    pub fn get_group(&self, group_id: GroupId) -> Option<&GroupVoteState> {
        self.groups.get(&group_id)
    }

    /// Updates vote state for a group.
    pub fn update_group(&mut self, group_id: GroupId, term: TermId, voted_for: Option<NodeId>) {
        self.groups.insert(
            group_id,
            GroupVoteState::new(group_id, term, voted_for),
        );
    }

    /// Removes a group from the vote state.
    pub fn remove_group(&mut self, group_id: GroupId) {
        self.groups.remove(&group_id);
    }

    /// Returns an iterator over all group states.
    pub fn iter_groups(&self) -> impl Iterator<Item = &GroupVoteState> {
        self.groups.values()
    }

    /// Serializes the vote state to bytes.
    ///
    /// Format:
    /// ```text
    /// [magic: 4][version: 4][sequence: 8][group_count: 4]
    /// [group_id: 8][term: 8][voted_for: 8] × group_count
    /// [crc32: 4]
    /// ```
    #[must_use]
    pub fn serialize(&self) -> Bytes {
        let header_size = 4 + 4 + 8 + 4; // magic + version + sequence + count
        let groups_size = self.groups.len() * GroupVoteState::serialized_size();
        let checksum_size = 4;
        let total_size = header_size + groups_size + checksum_size;

        let mut buf = BytesMut::with_capacity(total_size);

        // Header
        buf.put_u32(VOTE_STATE_MAGIC);
        buf.put_u32(VOTE_STATE_VERSION);
        buf.put_u64(self.sequence);

        // Safe: group count is bounded by MAX_GROUPS which fits in u32.
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32(self.groups.len() as u32);

        // Groups (sorted by group_id for determinism)
        let mut groups: Vec<_> = self.groups.values().collect();
        groups.sort_by_key(|g| g.group_id);

        for group in groups {
            buf.put_u64(group.group_id.get());
            buf.put_u64(group.term.get());
            buf.put_u64(group.voted_for.map_or(0, |n| n.get()));
        }

        // Checksum (over everything except the checksum itself)
        let checksum = crc32fast::hash(&buf);
        buf.put_u32(checksum);

        buf.freeze()
    }

    /// Deserializes vote state from bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Magic bytes don't match
    /// - Version is unsupported
    /// - Checksum doesn't match
    /// - Data is truncated
    pub fn deserialize(data: &[u8]) -> VoteStoreResult<Self> {
        let min_size = 4 + 4 + 8 + 4 + 4; // header + checksum, no groups
        if data.len() < min_size {
            return Err(VoteStoreError::InvalidFormat(format!(
                "data too short: {} bytes, minimum {}",
                data.len(),
                min_size
            )));
        }

        let mut buf = data;

        // Verify checksum first
        let payload = &data[..data.len() - 4];
        let expected_checksum = crc32fast::hash(payload);
        let stored_checksum = u32::from_be_bytes([
            data[data.len() - 4],
            data[data.len() - 3],
            data[data.len() - 2],
            data[data.len() - 1],
        ]);

        if expected_checksum != stored_checksum {
            return Err(VoteStoreError::ChecksumMismatch {
                expected: expected_checksum,
                actual: stored_checksum,
            });
        }

        // Parse header
        let magic = buf.get_u32();
        if magic != VOTE_STATE_MAGIC {
            return Err(VoteStoreError::InvalidFormat(format!(
                "invalid magic: {magic:#x}, expected {VOTE_STATE_MAGIC:#x}"
            )));
        }

        let version = buf.get_u32();
        if version != VOTE_STATE_VERSION {
            return Err(VoteStoreError::InvalidFormat(format!(
                "unsupported version: {version}, expected {VOTE_STATE_VERSION}"
            )));
        }

        let sequence = buf.get_u64();

        // Safe: u32 always fits in usize.
        #[allow(clippy::cast_possible_truncation)]
        let group_count = buf.get_u32() as usize;

        if group_count > MAX_GROUPS {
            return Err(VoteStoreError::TooManyGroups {
                count: group_count,
                max: MAX_GROUPS,
            });
        }

        // Verify we have enough data for all groups
        let expected_groups_size = group_count * GroupVoteState::serialized_size();
        if buf.remaining() < expected_groups_size + 4 {
            // +4 for checksum
            return Err(VoteStoreError::InvalidFormat(format!(
                "data truncated: expected {} bytes for {} groups",
                expected_groups_size, group_count
            )));
        }

        // Parse groups
        let mut groups = HashMap::with_capacity(group_count);
        for _ in 0..group_count {
            let group_id = GroupId::new(buf.get_u64());
            let term = TermId::new(buf.get_u64());
            let voted_for_raw = buf.get_u64();
            let voted_for = if voted_for_raw == 0 {
                None
            } else {
                Some(NodeId::new(voted_for_raw))
            };

            groups.insert(group_id, GroupVoteState::new(group_id, term, voted_for));
        }

        Ok(Self { sequence, groups })
    }
}

impl Default for VoteState {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// VoteStorage Trait (for DST)
// =============================================================================

/// Local vote state storage abstraction.
///
/// This trait enables deterministic simulation testing by allowing
/// the local storage to be replaced with an in-memory implementation.
pub trait VoteStorage: Send + Sync {
    /// Reads vote state from storage.
    ///
    /// Returns `Ok(None)` if no state exists.
    fn read(&self) -> VoteStoreResult<Option<Bytes>>;

    /// Writes vote state to storage with fsync.
    fn write(&self, data: &[u8]) -> VoteStoreResult<()>;

    /// Deletes vote state from storage.
    fn delete(&self) -> VoteStoreResult<()>;
}

/// Production implementation using local filesystem.
pub struct LocalFileVoteStorage {
    path: PathBuf,
}

impl LocalFileVoteStorage {
    /// Creates a new local file storage.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl VoteStorage for LocalFileVoteStorage {
    fn read(&self) -> VoteStoreResult<Option<Bytes>> {
        match std::fs::read(&self.path) {
            Ok(data) => Ok(Some(Bytes::from(data))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(VoteStoreError::Io(e)),
        }
    }

    fn write(&self, data: &[u8]) -> VoteStoreResult<()> {
        // Write to temp file first, then rename for atomicity
        let temp_path = self.path.with_extension("tmp");

        let mut file = std::fs::File::create(&temp_path)?;
        file.write_all(data)?;
        file.sync_all()?; // fsync

        std::fs::rename(&temp_path, &self.path)?;
        Ok(())
    }

    fn delete(&self) -> VoteStoreResult<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(VoteStoreError::Io(e)),
        }
    }
}

/// Simulated vote storage for DST.
pub struct SimulatedVoteStorage {
    /// In-memory storage.
    data: Arc<Mutex<Option<Vec<u8>>>>,
    /// Fault injection configuration.
    fault_config: Arc<Mutex<VoteStorageFaultConfig>>,
    /// RNG seed for deterministic faults.
    seed: u64,
    /// Operation counter for deterministic RNG.
    counter: Arc<AtomicU64>,
}

/// Fault injection configuration for simulated storage.
#[derive(Debug, Clone, Default)]
pub struct VoteStorageFaultConfig {
    /// Probability of read failing.
    pub read_fail_rate: f64,
    /// Probability of write failing.
    pub write_fail_rate: f64,
    /// Force next read to fail (one-shot).
    pub force_read_fail: bool,
    /// Force next write to fail (one-shot).
    pub force_write_fail: bool,
}

impl SimulatedVoteStorage {
    /// Creates a new simulated storage with the given seed.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            data: Arc::new(Mutex::new(None)),
            fault_config: Arc::new(Mutex::new(VoteStorageFaultConfig::default())),
            seed,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns a reference to the fault configuration.
    pub fn fault_config(&self) -> std::sync::MutexGuard<'_, VoteStorageFaultConfig> {
        self.fault_config.lock().expect("fault config lock poisoned")
    }

    /// Deterministic check for fault injection.
    fn should_inject_fault(&self, rate: f64) -> bool {
        if rate <= 0.0 {
            return false;
        }
        if rate >= 1.0 {
            return true;
        }
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        let hash = self.seed.wrapping_add(counter).wrapping_mul(0x5851_f42d_4c95_7f2d);
        #[allow(clippy::cast_precision_loss)]
        let normalized = (hash as f64) / (u64::MAX as f64);
        normalized < rate
    }
}

impl Clone for SimulatedVoteStorage {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            fault_config: self.fault_config.clone(),
            seed: self.seed,
            counter: self.counter.clone(),
        }
    }
}

impl VoteStorage for SimulatedVoteStorage {
    fn read(&self) -> VoteStoreResult<Option<Bytes>> {
        let mut config = self.fault_config.lock().expect("fault config lock poisoned");
        if config.force_read_fail {
            config.force_read_fail = false;
            return Err(VoteStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "simulated read failure (forced)",
            )));
        }
        let read_fail_rate = config.read_fail_rate;
        drop(config);

        if self.should_inject_fault(read_fail_rate) {
            return Err(VoteStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "simulated read failure (random)",
            )));
        }

        let data = self.data.lock().expect("data lock poisoned");
        Ok(data.as_ref().map(|d| Bytes::from(d.clone())))
    }

    fn write(&self, data: &[u8]) -> VoteStoreResult<()> {
        let mut config = self.fault_config.lock().expect("fault config lock poisoned");
        if config.force_write_fail {
            config.force_write_fail = false;
            return Err(VoteStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "simulated write failure (forced)",
            )));
        }
        let write_fail_rate = config.write_fail_rate;
        drop(config);

        if self.should_inject_fault(write_fail_rate) {
            return Err(VoteStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "simulated write failure (random)",
            )));
        }

        let mut storage = self.data.lock().expect("data lock poisoned");
        *storage = Some(data.to_vec());
        Ok(())
    }

    fn delete(&self) -> VoteStoreResult<()> {
        let mut storage = self.data.lock().expect("data lock poisoned");
        *storage = None;
        Ok(())
    }
}

// =============================================================================
// VoteStore
// =============================================================================

/// Vote store managing local + S3 persistence.
pub struct VoteStore<L: VoteStorage> {
    /// Node ID.
    node_id: NodeId,
    /// Current vote state.
    state: VoteState,
    /// Local storage.
    local: Arc<L>,
    /// Channel for async S3 uploads.
    upload_tx: mpsc::UnboundedSender<VoteState>,
}

/// Result of loading vote state.
pub struct LoadResult {
    /// The loaded vote state.
    pub state: VoteState,
    /// Whether state was recovered from remote (S3).
    /// If true, observation mode should be enabled.
    pub recovered_from_remote: bool,
}

impl<L: VoteStorage + 'static> VoteStore<L> {
    /// Creates a new vote store and starts the background upload worker.
    pub fn new<R: ObjectStorage + 'static>(
        node_id: NodeId,
        local: Arc<L>,
        remote: Arc<R>,
        state: VoteState,
    ) -> (Self, VoteStoreHandle) {
        let (upload_tx, upload_rx) = mpsc::unbounded_channel();

        let store = Self {
            node_id,
            state,
            local,
            upload_tx,
        };

        let handle = VoteStoreHandle {
            upload_rx,
            remote,
            node_id,
        };

        (store, handle)
    }

    /// Loads vote state from local or remote storage.
    ///
    /// Tries local first (fast path), falls back to remote (S3).
    pub async fn load<R: ObjectStorage>(
        node_id: NodeId,
        local: &L,
        remote: &R,
    ) -> VoteStoreResult<LoadResult> {
        // Try local first
        if let Some(data) = local.read()? {
            match VoteState::deserialize(&data) {
                Ok(state) => {
                    info!(
                        node_id = node_id.get(),
                        sequence = state.sequence,
                        groups = state.group_count(),
                        "Loaded vote state from local file"
                    );
                    return Ok(LoadResult {
                        state,
                        recovered_from_remote: false,
                    });
                }
                Err(e) => {
                    warn!(
                        node_id = node_id.get(),
                        error = %e,
                        "Local vote state corrupted, trying remote"
                    );
                    // Fall through to remote
                }
            }
        }

        // Try remote (S3)
        let key = Self::s3_key(node_id);
        match remote.get(&key).await {
            Ok(data) => {
                let state = VoteState::deserialize(&data)?;
                info!(
                    node_id = node_id.get(),
                    sequence = state.sequence,
                    groups = state.group_count(),
                    "Recovered vote state from S3"
                );

                // Write to local for next restart
                if let Err(e) = local.write(&data) {
                    warn!(
                        node_id = node_id.get(),
                        error = %e,
                        "Failed to write recovered state to local"
                    );
                }

                Ok(LoadResult {
                    state,
                    recovered_from_remote: true,
                })
            }
            Err(TierError::NotFound { .. }) => {
                info!(
                    node_id = node_id.get(),
                    "No vote state found, starting fresh"
                );
                Ok(LoadResult {
                    state: VoteState::new(),
                    recovered_from_remote: false,
                })
            }
            Err(e) => Err(VoteStoreError::Remote(e)),
        }
    }

    /// Saves vote state for a group.
    ///
    /// Writes to local storage synchronously (blocks until fsync),
    /// then queues async upload to S3.
    pub fn save(
        &mut self,
        group_id: GroupId,
        term: TermId,
        voted_for: Option<NodeId>,
    ) -> VoteStoreResult<()> {
        // Update in-memory state
        self.state.sequence += 1;
        self.state.update_group(group_id, term, voted_for);

        debug!(
            node_id = self.node_id.get(),
            group_id = group_id.get(),
            term = term.get(),
            voted_for = voted_for.map(|n| n.get()),
            sequence = self.state.sequence,
            "Saving vote state"
        );

        // Write to local (synchronous with fsync)
        let data = self.state.serialize();
        self.local.write(&data)?;

        // Queue async S3 upload
        let _ = self.upload_tx.send(self.state.clone());

        Ok(())
    }

    /// Removes a group from the vote state.
    pub fn remove_group(&mut self, group_id: GroupId) -> VoteStoreResult<()> {
        self.state.sequence += 1;
        self.state.remove_group(group_id);

        let data = self.state.serialize();
        self.local.write(&data)?;

        let _ = self.upload_tx.send(self.state.clone());

        Ok(())
    }

    /// Returns the current vote state.
    #[must_use]
    pub fn state(&self) -> &VoteState {
        &self.state
    }

    /// Returns the S3 key for a node's vote state.
    fn s3_key(node_id: NodeId) -> ObjectKey {
        ObjectKey::new(format!("helix/vote-state/node-{}.bin", node_id.get()))
    }
}

/// Handle for the background S3 upload worker.
pub struct VoteStoreHandle {
    upload_rx: mpsc::UnboundedReceiver<VoteState>,
    remote: Arc<dyn ObjectStorage>,
    node_id: NodeId,
}

impl VoteStoreHandle {
    /// Runs the background S3 upload worker.
    ///
    /// This should be spawned as a background task.
    pub async fn run(mut self) {
        info!(node_id = self.node_id.get(), "Vote store S3 worker started");

        let mut pending: Option<VoteState> = None;

        loop {
            // Wait for work or use pending state
            let state = match pending.take() {
                Some(s) => s,
                None => match self.upload_rx.recv().await {
                    Some(s) => s,
                    None => {
                        info!(node_id = self.node_id.get(), "Vote store S3 worker shutting down");
                        break;
                    }
                },
            };

            // Drain channel - only upload latest state
            while let Ok(newer) = self.upload_rx.try_recv() {
                pending = Some(newer);
            }
            let state = pending.take().unwrap_or(state);

            // Upload to S3
            let key = ObjectKey::new(format!(
                "helix/vote-state/node-{}.bin",
                self.node_id.get()
            ));
            let data = state.serialize();

            match self.remote.put(&key, data).await {
                Ok(()) => {
                    debug!(
                        node_id = self.node_id.get(),
                        sequence = state.sequence,
                        "Uploaded vote state to S3"
                    );
                }
                Err(e) => {
                    warn!(
                        node_id = self.node_id.get(),
                        error = %e,
                        "S3 upload failed, will retry"
                    );
                    // Retry with this state
                    pending = Some(state);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vote_state_serialize_deserialize() {
        let mut state = VoteState::new();
        state.sequence = 42;
        state.update_group(GroupId::new(1), TermId::new(5), Some(NodeId::new(2)));
        state.update_group(GroupId::new(2), TermId::new(3), None);

        let bytes = state.serialize();
        let recovered = VoteState::deserialize(&bytes).unwrap();

        assert_eq!(state, recovered);
        assert_eq!(recovered.sequence, 42);
        assert_eq!(recovered.group_count(), 2);

        let g1 = recovered.get_group(GroupId::new(1)).unwrap();
        assert_eq!(g1.term, TermId::new(5));
        assert_eq!(g1.voted_for, Some(NodeId::new(2)));

        let g2 = recovered.get_group(GroupId::new(2)).unwrap();
        assert_eq!(g2.term, TermId::new(3));
        assert_eq!(g2.voted_for, None);
    }

    #[test]
    fn test_vote_state_empty() {
        let state = VoteState::new();
        let bytes = state.serialize();
        let recovered = VoteState::deserialize(&bytes).unwrap();

        assert_eq!(recovered.sequence, 0);
        assert_eq!(recovered.group_count(), 0);
    }

    #[test]
    fn test_vote_state_checksum_detects_corruption() {
        let mut state = VoteState::new();
        state.update_group(GroupId::new(1), TermId::new(5), Some(NodeId::new(2)));

        let mut bytes = state.serialize().to_vec();

        // Corrupt one byte
        bytes[10] ^= 0xFF;

        let result = VoteState::deserialize(&bytes);
        assert!(matches!(result, Err(VoteStoreError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_vote_state_invalid_magic() {
        let bytes = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let result = VoteState::deserialize(&bytes);
        assert!(matches!(result, Err(VoteStoreError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_vote_state_truncated() {
        let bytes = vec![0; 10]; // Too short
        let result = VoteState::deserialize(&bytes);
        assert!(matches!(result, Err(VoteStoreError::InvalidFormat(_))));
    }

    #[test]
    fn test_simulated_storage_basic() {
        let storage = SimulatedVoteStorage::new(42);

        // Initially empty
        assert!(storage.read().unwrap().is_none());

        // Write
        storage.write(b"test data").unwrap();

        // Read back
        let data = storage.read().unwrap().unwrap();
        assert_eq!(&data[..], b"test data");

        // Delete
        storage.delete().unwrap();
        assert!(storage.read().unwrap().is_none());
    }

    #[test]
    fn test_simulated_storage_fault_injection() {
        let storage = SimulatedVoteStorage::new(42);

        // Force write failure
        storage.fault_config().force_write_fail = true;
        assert!(storage.write(b"data").is_err());

        // Next write succeeds (one-shot)
        assert!(storage.write(b"data").is_ok());

        // Force read failure
        storage.fault_config().force_read_fail = true;
        assert!(storage.read().is_err());

        // Next read succeeds
        let data = storage.read().unwrap().unwrap();
        assert_eq!(&data[..], b"data");
    }

    #[test]
    fn test_simulated_storage_clone_shares_state() {
        let storage1 = SimulatedVoteStorage::new(42);
        let storage2 = storage1.clone();

        storage1.write(b"shared data").unwrap();

        let data = storage2.read().unwrap().unwrap();
        assert_eq!(&data[..], b"shared data");
    }
}
