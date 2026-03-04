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
use bytes::{Buf, BufMut, Bytes, BytesMut};
use helix_core::{GroupId, PartitionId, TopicId};

use crate::snapshot::{SnapshotError, SnapshotMeta, Snapshottable};

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

    /// The `GroupId` of this offset group (`OFFSET_GROUP_ID_BASE + slot`).
    ///
    /// Stored so that `take_snapshot` can populate `SnapshotMeta::group_id`
    /// without requiring the caller to pass it in. Set at construction time
    /// via `new_for_slot`; the default value `GroupId::new(0)` is a sentinel
    /// that must not appear in a real snapshot.
    pub group_id: GroupId,

    /// `SharedWAL` index of the last entry applied to this state machine.
    ///
    /// Set by the tick task on every committed entry. Placed into
    /// `SnapshotMeta::last_included_wal_index` when taking a snapshot.
    pub last_applied_wal_index: u64,

    /// `SharedWAL` index at which this group's most recent snapshot was taken.
    ///
    /// `None` until the first snapshot is durably saved, or until state is
    /// restored from a snapshot via `apply_snapshot`. Used by
    /// `min_required_wal_index` to report the WAL retention floor.
    pub last_snapshot_wal_index: Option<u64>,
}

impl OffsetGroupState {
    /// Creates a new empty state.
    ///
    /// The `group_id` is left at its default (`GroupId::new(0)`). Prefer
    /// `new_for_slot` when the correct group ID is known.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new empty state with the correct group ID for the given slot.
    ///
    /// Slot is the index in `[0, OFFSET_GROUP_COUNT)`.
    #[must_use]
    pub fn new_for_slot(slot: usize) -> Self {
        // Safe: slot < OFFSET_GROUP_COUNT = 3, always fits in u64.
        #[allow(clippy::cast_possible_truncation)]
        let group_id = GroupId::new(OFFSET_GROUP_ID_BASE.get() + slot as u64);
        Self {
            group_id,
            ..Self::default()
        }
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

    // -------------------------------------------------------------------------
    // Snapshot serialization (OSNP v2 format)
    // -------------------------------------------------------------------------

    /// Serializes this state to the OSNP v2 binary format.
    ///
    /// The format is unchanged since the initial release so that snapshots
    /// written by older nodes remain readable.
    fn serialize_snapshot(&self) -> Bytes {
        let entry_count = self.offsets.len();
        let mut buf = BytesMut::with_capacity(64 + entry_count * 64);

        buf.put_slice(OFFSET_SNAPSHOT_MAGIC);
        buf.put_u32_le(OFFSET_SNAPSHOT_VERSION);
        buf.put_u64_le(self.last_applied_index);
        buf.put_u64_le(self.last_applied_term);

        // Sort entries for deterministic output.
        let mut entries: Vec<_> = self.offsets.iter().collect();
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

        let checksum = crc32fast::hash(&buf);
        buf.put_u32_le(checksum);

        buf.freeze()
    }

    /// Deserializes OSNP bytes (v1 or v2) into a fresh `OffsetGroupState`.
    ///
    /// Handles both v1 (no per-entry metadata) and v2 (with metadata) for
    /// backward compatibility with snapshots written by older nodes.
    ///
    /// # Errors
    ///
    /// Returns a `SnapshotError` if the data is truncated, has bad magic,
    /// an unsupported version, or a CRC mismatch.
    fn deserialize_snapshot(data: &[u8]) -> Result<Self, SnapshotError> {
        if data.len() < 32 {
            return Err(SnapshotError::Truncated {
                context: "snapshot shorter than minimum header (32 bytes)".to_string(),
            });
        }

        let payload = &data[..data.len() - 4];
        let expected = crc32fast::hash(payload);
        let stored = u32::from_le_bytes(
            data[data.len() - 4..].try_into().expect("4-byte slice"),
        );
        if expected != stored {
            return Err(SnapshotError::ChecksumMismatch { expected, got: stored });
        }

        let mut buf = data;

        if &buf[..4] != OFFSET_SNAPSHOT_MAGIC {
            return Err(SnapshotError::BadMagic);
        }
        buf.advance(4);

        let version = buf.get_u32_le();
        // Version 1 had no per-entry metadata; version 2 adds it.
        if version != OFFSET_SNAPSHOT_VERSION && version != 1 {
            return Err(SnapshotError::UnknownVersion { version });
        }

        let last_index = buf.get_u64_le();
        let last_term = buf.get_u64_le();
        let entry_count = buf.get_u32_le() as usize;

        let mut state = Self::new();
        state.last_applied_index = last_index;
        state.last_applied_term = last_term;

        for _ in 0..entry_count {
            if buf.remaining() < 4 {
                return Err(SnapshotError::Truncated {
                    context: "offset entry".to_string(),
                });
            }
            let group_len = buf.get_u32_le() as usize;
            if buf.remaining() < group_len + 24 {
                return Err(SnapshotError::Truncated {
                    context: "group string or fixed offset fields".to_string(),
                });
            }
            let group = String::from_utf8(buf.copy_to_bytes(group_len).to_vec())
                .map_err(|_| SnapshotError::Truncated {
                    context: "group string is not valid UTF-8".to_string(),
                })?;
            let topic_id = TopicId::new(buf.get_u64_le());
            let partition_id = PartitionId::new(buf.get_u64_le());
            let offset = buf.get_u64_le();
            // Version 2 includes per-entry metadata; v1 entries have none.
            let metadata = if version == OFFSET_SNAPSHOT_VERSION && buf.remaining() >= 4 {
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
            state.offsets.insert((group, topic_id, partition_id), (offset, metadata));
        }

        Ok(state)
    }
}

// =============================================================================
// Snapshottable impl
// =============================================================================

impl Snapshottable for OffsetGroupState {
    fn take_snapshot(&self) -> Result<(SnapshotMeta, Bytes), SnapshotError> {
        assert_ne!(
            self.group_id,
            GroupId::new(0),
            "take_snapshot called on OffsetGroupState with unset group_id; use new_for_slot"
        );
        let bytes = self.serialize_snapshot();
        let meta = SnapshotMeta {
            last_included_index: self.last_applied_index,
            last_included_term: self.last_applied_term,
            last_included_wal_index: self.last_applied_wal_index,
            group_id: self.group_id,
        };
        Ok((meta, bytes))
    }

    fn apply_snapshot(&mut self, meta: &SnapshotMeta, data: &[u8]) -> Result<(), SnapshotError> {
        assert!(
            is_offset_group(meta.group_id),
            "apply_snapshot called with non-offset group_id {:?}",
            meta.group_id
        );
        let mut state = Self::deserialize_snapshot(data)?;
        state.group_id = meta.group_id;
        state.last_applied_wal_index = meta.last_included_wal_index;
        state.last_snapshot_wal_index = Some(meta.last_included_wal_index);
        *self = state;
        Ok(())
    }

    fn min_required_wal_index(&self) -> Option<u64> {
        self.last_snapshot_wal_index.map(|w| w + 1)
    }

    fn set_last_snapshot_wal_index(&mut self, wal_index: u64) {
        self.last_snapshot_wal_index = Some(wal_index);
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

    // -------------------------------------------------------------------------
    // Snapshottable tests
    // -------------------------------------------------------------------------

    fn make_populated_offset_state(slot: usize) -> OffsetGroupState {
        let mut state = OffsetGroupState::new_for_slot(slot);
        state.last_applied_index = 77;
        state.last_applied_term = 2;
        state.last_applied_wal_index = 200;
        state.offsets.insert(
            ("cg-a".to_string(), TopicId::new(10), PartitionId::new(0)),
            (500, "meta-a".to_string()),
        );
        state.offsets.insert(
            ("cg-b".to_string(), TopicId::new(10), PartitionId::new(1)),
            (999, String::new()),
        );
        state
    }

    #[test]
    fn test_offset_snapshottable_roundtrip() {
        let original = make_populated_offset_state(1);
        let expected_gid = GroupId::new(OFFSET_GROUP_ID_BASE.get() + 1);

        let (meta, bytes) = original.take_snapshot().expect("take_snapshot failed");

        assert_eq!(meta.last_included_index, 77);
        assert_eq!(meta.last_included_term, 2);
        assert_eq!(meta.last_included_wal_index, 200);
        assert_eq!(meta.group_id, expected_gid);

        let mut restored = OffsetGroupState::new_for_slot(1);
        restored.apply_snapshot(&meta, &bytes).expect("apply_snapshot failed");

        assert_eq!(restored.last_applied_index, 77);
        assert_eq!(restored.last_applied_wal_index, 200);
        assert_eq!(restored.last_snapshot_wal_index, Some(200));
        assert_eq!(restored.group_id, expected_gid);
        assert_eq!(restored.offsets.len(), 2);
        assert_eq!(
            restored.get("cg-a", TopicId::new(10), PartitionId::new(0)),
            Some((500, "meta-a"))
        );
    }

    #[test]
    fn test_offset_snapshottable_crc_mismatch() {
        let state = make_populated_offset_state(0);
        let (_meta, bytes) = state.take_snapshot().unwrap();

        let mut corrupted = bytes.to_vec();
        let mid = corrupted.len() / 2;
        corrupted[mid] ^= 0xFF;

        let result = OffsetGroupState::deserialize_snapshot(&corrupted);
        assert!(
            matches!(result, Err(SnapshotError::ChecksumMismatch { .. })),
            "expected ChecksumMismatch, got {result:?}"
        );
    }

    #[test]
    fn test_offset_snapshottable_bad_magic() {
        let state = make_populated_offset_state(0);
        let (_meta, bytes) = state.take_snapshot().unwrap();

        let mut corrupted = bytes.to_vec();
        corrupted[0] = b'X';
        let payload_len = corrupted.len() - 4;
        let new_crc = crc32fast::hash(&corrupted[..payload_len]);
        corrupted[payload_len..].copy_from_slice(&new_crc.to_le_bytes());

        let result = OffsetGroupState::deserialize_snapshot(&corrupted);
        assert!(
            matches!(result, Err(SnapshotError::BadMagic)),
            "expected BadMagic, got {result:?}"
        );
    }

    #[test]
    fn test_offset_snapshottable_empty_state() {
        let mut state = OffsetGroupState::new_for_slot(2);
        state.last_applied_index = 1;
        state.last_applied_term = 1;
        state.last_applied_wal_index = 5;

        let (meta, bytes) = state.take_snapshot().unwrap();
        let mut restored = OffsetGroupState::new_for_slot(2);
        restored.apply_snapshot(&meta, &bytes).unwrap();

        assert_eq!(restored.offsets.len(), 0);
        assert_eq!(restored.last_snapshot_wal_index, Some(5));
        assert_eq!(restored.group_id, GroupId::new(OFFSET_GROUP_ID_BASE.get() + 2));
    }

    #[test]
    fn test_offset_min_required_wal_index() {
        let mut state = OffsetGroupState::new_for_slot(0);

        // Before any snapshot: must return None.
        assert_eq!(state.min_required_wal_index(), None);

        state.last_applied_wal_index = 50;
        let (meta, bytes) = state.take_snapshot().unwrap();
        state.apply_snapshot(&meta, &bytes).unwrap();

        assert_eq!(state.min_required_wal_index(), Some(51));
    }

    #[test]
    fn test_offset_snapshottable_backward_compat_with_old_serialize() {
        // Bytes produced by serialize_snapshot (the stable on-disk format) must be
        // readable by the new deserialize_snapshot path without the envelope wrapper.
        let original = make_populated_offset_state(0);
        let old_bytes = original.serialize_snapshot();

        let result = OffsetGroupState::deserialize_snapshot(&old_bytes);
        assert!(result.is_ok(), "old-format bytes must be readable: {result:?}");
        let restored = result.unwrap();
        assert_eq!(restored.offsets.len(), original.offsets.len());
    }

    #[test]
    fn test_new_for_slot_sets_correct_group_id() {
        for slot in 0..OFFSET_GROUP_COUNT_USIZE {
            let state = OffsetGroupState::new_for_slot(slot);
            let expected = GroupId::new(OFFSET_GROUP_ID_BASE.get() + slot as u64);
            assert_eq!(state.group_id, expected);
        }
    }

    #[test]
    fn test_snapshot_equivalence() {
        // Property: snapshot at K + replay of K+1..N == full replay from scratch.
        use helix_core::{PartitionId, TopicId};

        // Build a deterministic sequence of CommitOffset commands.
        let cmds: Vec<OffsetGroupCommand> = (1_u64..=20)
            .map(|i| OffsetGroupCommand::CommitOffset {
                group: format!("cg-{}", i % 3),
                topic_id: TopicId::new(1),
                partition_id: PartitionId::new(i % 4),
                offset: i * 100,
                timestamp_ms: i * 1000,
                metadata: format!("meta-{i}"),
            })
            .collect();

        let n = cmds.len();
        let k = n / 2;

        // Full replay: apply all N commands from scratch.
        let mut state_full = OffsetGroupState::new_for_slot(0);
        for (i, cmd) in cmds.iter().enumerate() {
            let raft_index = (i as u64) + 1;
            state_full.last_applied_wal_index = raft_index;
            state_full.apply(cmd, raft_index, 1);
        }

        // Hybrid: apply K, snapshot, restore, apply K+1..N.
        let mut state_at_k = OffsetGroupState::new_for_slot(0);
        for (i, cmd) in cmds[..k].iter().enumerate() {
            let raft_index = (i as u64) + 1;
            state_at_k.last_applied_wal_index = raft_index;
            state_at_k.apply(cmd, raft_index, 1);
        }
        let (meta, body) = state_at_k.take_snapshot().expect("take_snapshot at K");

        let mut state_hybrid = OffsetGroupState::new_for_slot(0);
        state_hybrid.apply_snapshot(&meta, &body).expect("apply_snapshot");

        for (i, cmd) in cmds[k..].iter().enumerate() {
            let raft_index = (k as u64) + (i as u64) + 1;
            state_hybrid.last_applied_wal_index = raft_index;
            state_hybrid.apply(cmd, raft_index, 1);
        }

        // Compare observable state: for every (group, topic, partition) combo
        // the committed offset must match between full replay and hybrid.
        let tid = TopicId::new(1);
        for i in 1_u64..=20 {
            let cg = format!("cg-{}", i % 3);
            let pid = PartitionId::new(i % 4);
            let full_val = state_full.get(&cg, tid, pid);
            let hybrid_val = state_hybrid.get(&cg, tid, pid);
            assert_eq!(
                full_val, hybrid_val,
                "offset mismatch for cg={cg} pid={pid:?}"
            );
        }

        assert_eq!(
            state_full.last_applied_index,
            state_hybrid.last_applied_index,
            "last_applied_index differs"
        );
    }

    #[test]
    fn test_offset_group_snapshot_raft_pipeline() {
        // Phase 3 pipeline: take_snapshot → encode_envelope → decode_envelope → apply_snapshot.
        //
        // The Raft Snapshot struct only carries (index, term, data bytes).
        // last_included_wal_index and group_id must therefore survive the Raft
        // wire as part of the data payload via encode_envelope/decode_envelope.
        use crate::snapshot::{decode_envelope, encode_envelope};

        let original = make_populated_offset_state(2); // slot 2 → group_id = base + 2

        let (meta, body) = original.take_snapshot().expect("take_snapshot");
        assert_eq!(meta.last_included_wal_index, original.last_applied_wal_index);
        assert_eq!(meta.group_id, original.group_id);

        // Leader side (provide_controller_group_snapshot): pack body + meta into wire bytes.
        let wire_bytes = encode_envelope(&meta, &body);

        // Follower side (apply_controller_group_snapshot): decode wire bytes.
        let (decoded_meta, decoded_body) =
            decode_envelope(&wire_bytes).expect("decode_envelope");

        // The envelope must preserve fields absent from the Raft Snapshot struct.
        assert_eq!(
            decoded_meta.last_included_wal_index,
            meta.last_included_wal_index,
            "wal_index must survive Raft wire transmission"
        );
        assert_eq!(
            decoded_meta.group_id,
            meta.group_id,
            "group_id must survive Raft wire transmission"
        );

        // Apply decoded snapshot to a fresh state machine — must restore full state.
        let mut restored = OffsetGroupState::new_for_slot(2);
        restored
            .apply_snapshot(&decoded_meta, &decoded_body)
            .expect("apply_snapshot from decoded envelope");

        assert_eq!(
            restored.last_applied_wal_index,
            original.last_applied_wal_index,
            "wal index must be restored after envelope roundtrip"
        );
        assert_eq!(restored.group_id, original.group_id, "group_id restored");
        assert_eq!(
            restored.offsets.len(),
            original.offsets.len(),
            "offset entries must be preserved"
        );
        assert_eq!(
            restored.last_snapshot_wal_index,
            Some(original.last_applied_wal_index),
            "last_snapshot_wal_index must be set after apply_snapshot"
        );
    }
}
