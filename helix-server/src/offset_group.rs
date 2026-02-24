//! Dedicated Raft groups for Kafka consumer group offset storage.
//!
//! Replaces the old `CommitConsumerGroupOffset` controller command with N=3
//! dedicated offset Raft groups. Each node leads one group.
//! `hash(group_id) % 3` routes commits to the correct group leader, matching
//! the Kafka `__consumer_offsets` pattern with N partitions, RF=3.
//!
//! # Group ID reservation
//!
//! Offset groups use a high fixed range starting at `OFFSET_GROUP_ID_BASE`
//! (1,000,000), far above the maximum expected data partition count. This
//! ensures no collision with data partition group IDs regardless of cluster age.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use helix_core::{GroupId, PartitionId, TopicId};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use helix_tier::{ObjectKey, ObjectStorage};

// =============================================================================
// Constants
// =============================================================================

/// Number of dedicated offset Raft groups.
pub const OFFSET_GROUP_COUNT: u64 = 3;

/// `OFFSET_GROUP_COUNT` as `usize` for use in slice indexing and `Vec` operations.
///
/// Defined separately to avoid repeated `as usize` casts that trigger
/// `clippy::cast_possible_truncation` (safe: the value is 3, well within `usize`).
// Safe: OFFSET_GROUP_COUNT = 3, always fits in usize on any platform.
#[allow(clippy::cast_possible_truncation)]
pub const OFFSET_GROUP_COUNT_USIZE: usize = OFFSET_GROUP_COUNT as usize;

/// First offset group ID. Uses a high fixed base (1,000,000) so that offset
/// groups never collide with data partition group IDs, even on clusters that
/// were created before offset groups were introduced.
pub const OFFSET_GROUP_ID_BASE: GroupId = GroupId::new(1_000_000);

/// Snapshot after this many entries have been committed since the last snapshot.
pub const OFFSET_SNAPSHOT_ENTRY_THRESHOLD: u64 = 10_000;

/// Minimum wall-clock interval between snapshots (prevents thrashing).
pub const OFFSET_SNAPSHOT_INTERVAL_MS: u64 = 60_000;

// Command type byte.
const CMD_COMMIT_OFFSET: u8 = 1;

// Snapshot format constants.
const OFFSET_SNAPSHOT_MAGIC: &[u8; 4] = b"OSNP";
/// Version 2 adds per-entry `committed_metadata` after the offset field.
const OFFSET_SNAPSHOT_VERSION: u32 = 2;

// =============================================================================
// Routing
// =============================================================================

/// Returns the index in `[0, OFFSET_GROUP_COUNT)` for the given consumer group.
///
/// Uses `xxh3_64` (seed-free, deterministic) for stable routing that survives
/// rolling restarts. The same group ID always maps to the same offset group.
#[must_use]
pub fn offset_group_index(consumer_group_id: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(consumer_group_id.as_bytes()) % OFFSET_GROUP_COUNT
}

/// Returns the `GroupId` of the offset Raft group that owns this consumer group.
#[must_use]
pub fn offset_group_id(consumer_group_id: &str) -> GroupId {
    GroupId::new(OFFSET_GROUP_ID_BASE.get() + offset_group_index(consumer_group_id))
}

/// Returns `true` if `group_id` is one of the reserved offset groups
/// (in the range `[OFFSET_GROUP_ID_BASE, OFFSET_GROUP_ID_BASE + OFFSET_GROUP_COUNT)`).
#[must_use]
pub const fn is_offset_group(group_id: GroupId) -> bool {
    let base = OFFSET_GROUP_ID_BASE.get();
    let id = group_id.get();
    id >= base && id < base + OFFSET_GROUP_COUNT
}

/// Returns the slot index `[0, OFFSET_GROUP_COUNT)` for an offset group ID.
///
/// Returns `None` if the group is not an offset group.
#[must_use]
pub const fn offset_group_slot(group_id: GroupId) -> Option<usize> {
    if is_offset_group(group_id) {
        // Safe: slot is in [0, 2], always fits in usize.
        #[allow(clippy::cast_possible_truncation)]
        Some((group_id.get() - OFFSET_GROUP_ID_BASE.get()) as usize)
    } else {
        None
    }
}

// =============================================================================
// OffsetGroupCommand
// =============================================================================

/// Commands that can be proposed to an offset Raft group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetGroupCommand {
    /// Commit a Kafka consumer group offset.
    CommitOffset {
        /// Raw Kafka consumer group ID string.
        group: String,
        /// Topic ID.
        topic_id: TopicId,
        /// Partition ID.
        partition_id: PartitionId,
        /// Committed offset (next offset to read).
        offset: u64,
        /// Commit timestamp in milliseconds.
        timestamp_ms: u64,
        /// Opaque client metadata (e.g. libstreaming inflight state).
        metadata: String,
    },
}

impl OffsetGroupCommand {
    /// Encodes the command to bytes for Raft replication.
    ///
    /// Encoding:
    /// ```text
    /// [type: u8][group_len: u32 LE][group bytes][topic_id: u64 LE]
    /// [partition_id: u64 LE][offset: u64 LE][timestamp_ms: u64 LE]
    /// [metadata_len: u32 LE][metadata bytes]
    /// ```
    ///
    /// Old entries written without the metadata suffix decode with `metadata = ""`.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        match self {
            Self::CommitOffset {
                group,
                topic_id,
                partition_id,
                offset,
                timestamp_ms,
                metadata,
            } => {
                let group_bytes = group.as_bytes();
                let meta_bytes = metadata.as_bytes();
                let mut buf =
                    BytesMut::with_capacity(1 + 4 + group_bytes.len() + 32 + 4 + meta_bytes.len());
                buf.put_u8(CMD_COMMIT_OFFSET);
                #[allow(clippy::cast_possible_truncation)]
                buf.put_u32_le(group_bytes.len() as u32);
                buf.put_slice(group_bytes);
                buf.put_u64_le(topic_id.get());
                buf.put_u64_le(partition_id.get());
                buf.put_u64_le(*offset);
                buf.put_u64_le(*timestamp_ms);
                #[allow(clippy::cast_possible_truncation)]
                buf.put_u32_le(meta_bytes.len() as u32);
                buf.put_slice(meta_bytes);
                buf.freeze()
            }
        }
    }

    /// Decodes a command from bytes.
    ///
    /// Returns `None` if the data is invalid or incomplete.
    #[must_use]
    pub fn decode(data: &Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let mut buf = data.clone();
        let cmd_type = buf.get_u8();
        match cmd_type {
            CMD_COMMIT_OFFSET => {
                if buf.remaining() < 4 {
                    return None;
                }
                let group_len = buf.get_u32_le() as usize;
                if buf.remaining() < group_len + 32 {
                    return None;
                }
                let group =
                    String::from_utf8(buf.copy_to_bytes(group_len).to_vec()).ok()?;
                let topic_id = TopicId::new(buf.get_u64_le());
                let partition_id = PartitionId::new(buf.get_u64_le());
                let offset = buf.get_u64_le();
                let timestamp_ms = buf.get_u64_le();
                // Metadata suffix is optional for backward compatibility with old entries.
                let metadata = if buf.remaining() >= 4 {
                    let meta_len = buf.get_u32_le() as usize;
                    if buf.remaining() >= meta_len {
                        String::from_utf8(buf.copy_to_bytes(meta_len).to_vec())
                            .unwrap_or_default()
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                };
                Some(Self::CommitOffset {
                    group,
                    topic_id,
                    partition_id,
                    offset,
                    timestamp_ms,
                    metadata,
                })
            }
            _ => None,
        }
    }
}

// =============================================================================
// OffsetGroupState
// =============================================================================

/// In-memory state for one offset Raft group.
///
/// Rebuilt by replaying committed `OffsetGroupCommand` entries from the
/// `SharedWAL` on startup (or from a snapshot when available).
#[derive(Debug, Default)]
pub struct OffsetGroupState {
    /// Committed consumer group offsets.
    ///
    /// Key: (`consumer_group_id`, `topic_id`, `partition_id`)
    /// Value: `(committed_offset, committed_metadata)`.
    pub offsets: HashMap<(String, TopicId, PartitionId), (u64, String)>,

    /// Raft index of the last applied entry.
    ///
    /// Used as `last_included_index` in snapshots. Entries at or before this
    /// index can be safely trimmed from the WAL once a snapshot is persisted.
    pub last_applied_index: u64,

    /// Raft term of the last applied entry.
    pub last_applied_term: u64,

    /// Number of entries applied since the last snapshot.
    ///
    /// When this crosses `OFFSET_SNAPSHOT_ENTRY_THRESHOLD`, the leader triggers
    /// a new snapshot to bound WAL growth.
    pub entries_since_snapshot: u64,
}

impl OffsetGroupState {
    /// Creates a new empty state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a committed command and advances the index.
    ///
    /// # Panics
    ///
    /// Panics if `raft_index` is zero or less than the last applied index.
    pub fn apply(&mut self, cmd: &OffsetGroupCommand, raft_index: u64, raft_term: u64) {
        assert!(raft_index > 0, "raft_index must be positive");

        match cmd {
            OffsetGroupCommand::CommitOffset {
                group,
                topic_id,
                partition_id,
                offset,
                metadata,
                ..
            } => {
                self.offsets.insert(
                    (group.clone(), *topic_id, *partition_id),
                    (*offset, metadata.clone()),
                );
            }
        }

        assert!(
            raft_index >= self.last_applied_index,
            "raft_index must be monotonically increasing"
        );
        self.last_applied_index = raft_index;
        self.last_applied_term = raft_term;
        self.entries_since_snapshot += 1;
    }

    /// Returns the committed offset and metadata for a consumer group / topic / partition.
    ///
    /// Returns `None` if no offset has been committed yet.
    #[must_use]
    pub fn get(
        &self,
        group: &str,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<(u64, &str)> {
        self.offsets
            .get(&(group.to_owned(), topic_id, partition_id))
            .map(|(offset, meta)| (*offset, meta.as_str()))
    }

    /// Iterates all committed offsets and metadata for a consumer group.
    pub fn get_all_for_group<'a>(
        &'a self,
        group: &'a str,
    ) -> impl Iterator<Item = (&'a (String, TopicId, PartitionId), &'a (u64, String))> + 'a {
        self.offsets.iter().filter(move |((g, ..), _)| g == group)
    }
}

// =============================================================================
// OffsetSnapshotStore
// =============================================================================

/// Persists offset group snapshots to local disk (and optionally S3).
///
/// # Design
///
/// ```text
/// Snapshot trigger (leader only, threshold + interval based):
///   ├─→ Local file + fsync  (synchronous, critical path)
///   └─→ S3 upload (async, best-effort)          ← off critical path
///
/// Startup:
///   ├─→ Local file exists? Use it (fast path)
///   └─→ Fetch from S3 → return remote flag (safe recovery)
/// ```
///
/// # Binary Format
///
/// ```text
/// [magic: 4][version: u32 LE][last_included_index: u64 LE][last_included_term: u64 LE]
/// [entry_count: u32 LE]
/// per entry: [group_id_len: u32 LE][group_id bytes][topic_id: u64 LE][partition_id: u64 LE][offset: u64 LE]
/// [crc32: u32 LE]
/// ```
pub struct OffsetSnapshotStore {
    /// Local file path for this group's snapshot.
    local_path: PathBuf,
    /// S3 key for this group's snapshot.
    s3_key: ObjectKey,
    /// Channel for async S3 uploads.
    upload_tx: mpsc::UnboundedSender<Bytes>,
}

impl OffsetSnapshotStore {
    /// Creates a new snapshot store for the given offset group slot.
    ///
    /// Spawns a background S3 upload worker.
    pub fn new(
        data_dir: &Path,
        group_slot: usize,
        remote: Arc<dyn ObjectStorage>,
    ) -> Self {
        let local_path = data_dir.join(format!("offset-group-{group_slot}-snapshot.bin"));
        let s3_key = ObjectKey::new(format!("helix/offset-snapshot/group-{group_slot}.bin"));

        let (upload_tx, upload_rx) = mpsc::unbounded_channel::<Bytes>();
        let key_clone = s3_key.clone();

        // Spawn async S3 upload worker.
        tokio::spawn(offset_snapshot_s3_worker(remote, upload_rx, key_clone));

        Self {
            local_path,
            s3_key,
            upload_tx,
        }
    }

    /// Serializes `state` to the snapshot format.
    fn serialize(state: &OffsetGroupState) -> Bytes {
        let entry_count = state.offsets.len();
        // Estimate capacity: header (28) + per-entry overhead + average group key length.
        let mut buf = BytesMut::with_capacity(64 + entry_count * 64);

        buf.put_slice(OFFSET_SNAPSHOT_MAGIC);
        buf.put_u32_le(OFFSET_SNAPSHOT_VERSION);
        buf.put_u64_le(state.last_applied_index);
        buf.put_u64_le(state.last_applied_term);

        // Sort entries for deterministic output.
        let mut entries: Vec<_> = state.offsets.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));

        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(entries.len() as u32);

        for ((group, topic_id, partition_id), (offset, metadata)) in &entries {
            let group_bytes = group.as_bytes();
            let meta_bytes = metadata.as_bytes();
            #[allow(clippy::cast_possible_truncation)]
            buf.put_u32_le(group_bytes.len() as u32);
            buf.put_slice(group_bytes);
            buf.put_u64_le(topic_id.get());
            buf.put_u64_le(partition_id.get());
            buf.put_u64_le(*offset);
            #[allow(clippy::cast_possible_truncation)]
            buf.put_u32_le(meta_bytes.len() as u32);
            buf.put_slice(meta_bytes);
        }

        // Append CRC32 over everything above.
        let checksum = crc32fast::hash(&buf);
        buf.put_u32_le(checksum);

        buf.freeze()
    }

    /// Deserializes a snapshot from raw bytes.
    fn deserialize(data: &[u8]) -> Option<(OffsetGroupState, u64, u64)> {
        if data.len() < 32 {
            warn!("Offset snapshot too short: {} bytes", data.len());
            return None;
        }

        // Verify CRC32.
        let payload = &data[..data.len() - 4];
        let expected = crc32fast::hash(payload);
        let stored = u32::from_le_bytes(
            data[data.len() - 4..]
                .try_into()
                .expect("4-byte slice"),
        );
        if expected != stored {
            warn!(
                "Offset snapshot CRC mismatch: expected {expected:#x}, got {stored:#x}"
            );
            return None;
        }

        let mut buf = data;

        // Parse magic.
        if &buf[..4] != OFFSET_SNAPSHOT_MAGIC {
            warn!("Offset snapshot bad magic");
            return None;
        }
        buf.advance(4);

        let version = buf.get_u32_le();
        // Version 1 had no per-entry metadata; version 2 adds it.
        // Any other version is unknown.
        if version != OFFSET_SNAPSHOT_VERSION && version != 1 {
            warn!("Offset snapshot unsupported version {version}");
            return None;
        }

        let last_index = buf.get_u64_le();
        let last_term = buf.get_u64_le();
        let entry_count = buf.get_u32_le() as usize;

        let mut state = OffsetGroupState::new();
        state.last_applied_index = last_index;
        state.last_applied_term = last_term;

        for _ in 0..entry_count {
            if buf.remaining() < 4 {
                warn!("Offset snapshot truncated at entry");
                return None;
            }
            let group_len = buf.get_u32_le() as usize;
            if buf.remaining() < group_len + 24 {
                warn!("Offset snapshot truncated in group string or fixed fields");
                return None;
            }
            let group = String::from_utf8(buf.copy_to_bytes(group_len).to_vec()).ok()?;
            let topic_id = TopicId::new(buf.get_u64_le());
            let partition_id = PartitionId::new(buf.get_u64_le());
            let offset = buf.get_u64_le();
            // Version 2 includes per-entry metadata; version 1 entries have none.
            let metadata = if version == OFFSET_SNAPSHOT_VERSION && buf.remaining() >= 4 {
                let meta_len = buf.get_u32_le() as usize;
                if buf.remaining() >= meta_len {
                    String::from_utf8(buf.copy_to_bytes(meta_len).to_vec()).unwrap_or_default()
                } else {
                    String::new()
                }
            } else {
                String::new()
            };
            state.offsets.insert((group, topic_id, partition_id), (offset, metadata));
        }

        // Remaining 4 bytes are the CRC (already verified).
        Some((state, last_index, last_term))
    }

    /// Saves the offset state to local disk (fsync) and queues an async S3 upload.
    ///
    /// # Errors
    ///
    /// Returns an error if the local file write or fsync fails.
    ///
    /// # Panics
    ///
    /// Panics if `state` has a non-zero `last_applied_index` but an empty offsets map.
    pub fn save(&self, state: &OffsetGroupState) -> std::io::Result<()> {
        assert!(
            state.last_applied_index > 0 || state.offsets.is_empty(),
            "cannot snapshot empty state with non-zero index"
        );

        let data = Self::serialize(state);

        // Write to temp file then rename for atomicity.
        let temp = self.local_path.with_extension("tmp");
        let mut file = std::fs::File::create(&temp)?;
        file.write_all(&data)?;
        file.sync_all()?;
        std::fs::rename(&temp, &self.local_path)?;

        debug!(
            path = %self.local_path.display(),
            last_index = state.last_applied_index,
            entries = state.offsets.len(),
            "Offset snapshot saved to local disk"
        );

        // Queue async S3 upload (best-effort).
        let _ = self.upload_tx.send(data);

        Ok(())
    }

    /// Loads the snapshot from local disk, falling back to S3.
    ///
    /// Returns `(state, last_index, last_term, recovered_from_remote)`.
    /// Returns `None` if no snapshot exists anywhere.
    pub async fn load(
        &self,
        remote: &dyn ObjectStorage,
    ) -> Option<(OffsetGroupState, u64, u64, bool)> {
        // Try local file first.
        match std::fs::read(&self.local_path) {
            Ok(data) => {
                if let Some((state, idx, term)) = Self::deserialize(&data) {
                    info!(
                        path = %self.local_path.display(),
                        last_index = idx,
                        entries = state.offsets.len(),
                        "Loaded offset snapshot from local disk"
                    );
                    return Some((state, idx, term, false));
                }
                warn!(
                    path = %self.local_path.display(),
                    "Local offset snapshot corrupted, trying S3"
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!(
                    path = %self.local_path.display(),
                    "No local offset snapshot found, trying S3"
                );
            }
            Err(e) => {
                warn!(
                    path = %self.local_path.display(),
                    error = %e,
                    "Failed to read local offset snapshot, trying S3"
                );
            }
        }

        // Try S3 (remote).
        match remote.get(&self.s3_key).await {
            Ok(data) => {
                if let Some((state, idx, term)) = Self::deserialize(&data) {
                    info!(
                        key = %self.s3_key,
                        last_index = idx,
                        entries = state.offsets.len(),
                        "Loaded offset snapshot from S3"
                    );
                    // Write to local for next restart.
                    if let Err(e) = std::fs::write(&self.local_path, &data) {
                        warn!(
                            path = %self.local_path.display(),
                            error = %e,
                            "Failed to cache S3 offset snapshot locally"
                        );
                    }
                    return Some((state, idx, term, true));
                }
                warn!(key = %self.s3_key, "S3 offset snapshot corrupted or invalid");
                None
            }
            Err(helix_tier::TierError::NotFound { .. }) => {
                debug!(key = %self.s3_key, "No S3 offset snapshot found");
                None
            }
            Err(e) => {
                warn!(key = %self.s3_key, error = %e, "Failed to fetch S3 offset snapshot");
                None
            }
        }
    }
}

/// Background worker that uploads offset snapshots to S3.
async fn offset_snapshot_s3_worker(
    remote: Arc<dyn ObjectStorage>,
    mut rx: mpsc::UnboundedReceiver<Bytes>,
    key: ObjectKey,
) {
    let mut pending: Option<Bytes> = None;

    loop {
        let data = if let Some(d) = pending.take() {
            d
        } else {
            let Some(d) = rx.recv().await else {
                break; // Channel closed, store dropped.
            };
            d
        };

        // Drain channel — only upload latest snapshot.
        while let Ok(newer) = rx.try_recv() {
            pending = Some(newer);
        }
        let data = pending.take().unwrap_or(data);

        match remote.put(&key, data.clone()).await {
            Ok(()) => {
                debug!(key = %key, "Uploaded offset snapshot to S3");
            }
            Err(e) => {
                warn!(key = %key, error = %e, "S3 offset snapshot upload failed, will retry");
                pending = Some(data);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
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
    fn test_offset_group_command_encode_decode() {
        let cmd = OffsetGroupCommand::CommitOffset {
            group: "my-consumer-group".to_string(),
            topic_id: TopicId::new(42),
            partition_id: PartitionId::new(3),
            offset: 1234,
            timestamp_ms: 9_999_999,
            metadata: "some-client-metadata".to_string(),
        };

        let encoded = cmd.encode();
        let decoded = OffsetGroupCommand::decode(&encoded).unwrap();
        assert_eq!(cmd, decoded);
    }

    #[test]
    fn test_offset_group_command_decode_no_metadata_backward_compat() {
        // Old entries encoded without the metadata suffix should decode with empty metadata.
        let cmd_no_meta = OffsetGroupCommand::CommitOffset {
            group: "g".to_string(),
            topic_id: TopicId::new(1),
            partition_id: PartitionId::new(0),
            offset: 99,
            timestamp_ms: 0,
            metadata: String::new(),
        };
        // Manually build the old wire format (no metadata suffix).
        let group_bytes = b"g";
        let mut buf = bytes::BytesMut::with_capacity(1 + 4 + 1 + 32);
        buf.put_u8(1_u8); // CMD_COMMIT_OFFSET
        buf.put_u32_le(1_u32); // group_len
        buf.put_slice(group_bytes);
        buf.put_u64_le(1_u64); // topic_id
        buf.put_u64_le(0_u64); // partition_id
        buf.put_u64_le(99_u64); // offset
        buf.put_u64_le(0_u64); // timestamp_ms
        let old_encoded = buf.freeze();
        let decoded = OffsetGroupCommand::decode(&old_encoded).unwrap();
        assert_eq!(cmd_no_meta, decoded);
    }

    #[test]
    fn test_offset_group_command_decode_empty() {
        let encoded = Bytes::new();
        assert!(OffsetGroupCommand::decode(&encoded).is_none());
    }

    #[test]
    fn test_offset_group_state_apply() {
        let mut state = OffsetGroupState::new();
        let cmd = OffsetGroupCommand::CommitOffset {
            group: "group-1".to_string(),
            topic_id: TopicId::new(1),
            partition_id: PartitionId::new(0),
            offset: 100,
            timestamp_ms: 0,
            metadata: "meta-a".to_string(),
        };

        state.apply(&cmd, 1, 1);
        assert_eq!(
            state.get("group-1", TopicId::new(1), PartitionId::new(0)),
            Some((100, "meta-a"))
        );
        assert_eq!(state.last_applied_index, 1);
        assert_eq!(state.entries_since_snapshot, 1);

        // Overwrite with higher offset and different metadata.
        let cmd2 = OffsetGroupCommand::CommitOffset {
            group: "group-1".to_string(),
            topic_id: TopicId::new(1),
            partition_id: PartitionId::new(0),
            offset: 200,
            timestamp_ms: 1,
            metadata: "meta-b".to_string(),
        };
        state.apply(&cmd2, 2, 1);
        assert_eq!(
            state.get("group-1", TopicId::new(1), PartitionId::new(0)),
            Some((200, "meta-b"))
        );
        assert_eq!(state.entries_since_snapshot, 2);
    }

    #[test]
    fn test_offset_group_routing_determinism() {
        // Same input always produces the same index.
        for _ in 0..100 {
            let idx = offset_group_index("test-group");
            assert!(idx < OFFSET_GROUP_COUNT);
            let gid = offset_group_id("test-group");
            assert!(is_offset_group(gid));
        }
    }

    #[test]
    fn test_offset_group_routing_distribution() {
        // Verify that different group IDs can route to different groups.
        let groups = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];
        let indices: Vec<u64> = groups
            .iter()
            .map(|g| offset_group_index(g))
            .collect();
        // Not all should be the same (extremely unlikely with xxh3).
        let all_same = indices.iter().all(|&i| i == indices[0]);
        assert!(!all_same, "All groups routed to same index: {indices:?}");
    }

    #[test]
    fn test_is_offset_group() {
        use helix_core::GroupId;
        assert!(!is_offset_group(GroupId::new(0))); // controller
        assert!(!is_offset_group(GroupId::new(1))); // data partition (low range)
        assert!(!is_offset_group(GroupId::new(64))); // data partition
        assert!(is_offset_group(GroupId::new(1_000_000)));
        assert!(is_offset_group(GroupId::new(1_000_001)));
        assert!(is_offset_group(GroupId::new(1_000_002)));
        assert!(!is_offset_group(GroupId::new(1_000_003))); // out of range
    }

    #[test]
    fn test_offset_snapshot_serialize_deserialize() {
        let mut state = OffsetGroupState::new();
        state.last_applied_index = 42;
        state.last_applied_term = 3;
        state.offsets.insert(
            ("group-1".to_string(), TopicId::new(1), PartitionId::new(0)),
            (100, "client-state-1".to_string()),
        );
        state.offsets.insert(
            ("group-2".to_string(), TopicId::new(2), PartitionId::new(1)),
            (200, String::new()),
        );

        let bytes = OffsetSnapshotStore::serialize(&state);
        let (recovered, last_index, last_term) =
            OffsetSnapshotStore::deserialize(&bytes).unwrap();

        assert_eq!(last_index, 42);
        assert_eq!(last_term, 3);
        assert_eq!(recovered.offsets.len(), 2);
        assert_eq!(
            recovered.get("group-1", TopicId::new(1), PartitionId::new(0)),
            Some((100, "client-state-1"))
        );
        assert_eq!(
            recovered.get("group-2", TopicId::new(2), PartitionId::new(1)),
            Some((200, ""))
        );
    }

    #[test]
    fn test_offset_snapshot_detects_corruption() {
        let mut state = OffsetGroupState::new();
        state.last_applied_index = 1;
        state.last_applied_term = 1;
        state.offsets.insert(
            ("g".to_string(), TopicId::new(1), PartitionId::new(0)),
            (0, String::new()),
        );

        let mut bytes = OffsetSnapshotStore::serialize(&state).to_vec();
        // Corrupt one byte in the middle.
        bytes[10] ^= 0xFF;
        assert!(OffsetSnapshotStore::deserialize(&bytes).is_none());
    }
}
