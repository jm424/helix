//! Core progress tracking data structures.

#![allow(clippy::items_after_statements)]

use std::collections::HashMap;

use helix_core::{ConsumerGroupId, ConsumerId, LeaseId, Limits, Offset, PartitionId, TopicId};
use roaring::RoaringBitmap;

// -----------------------------------------------------------------------------
// Partition Key
// -----------------------------------------------------------------------------

/// Key identifying a partition within a consumer group's progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    /// Topic identifier.
    pub topic_id: TopicId,
    /// Partition identifier.
    pub partition_id: PartitionId,
}

impl PartitionKey {
    /// Creates a new partition key.
    #[must_use]
    pub const fn new(topic_id: TopicId, partition_id: PartitionId) -> Self {
        Self {
            topic_id,
            partition_id,
        }
    }
}

// -----------------------------------------------------------------------------
// Acknowledgment Mode
// -----------------------------------------------------------------------------

/// Acknowledgment mode for a consumer group's partition.
///
/// Determines how commits are processed and how the low watermark advances.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AckMode {
    /// Kafka-style cumulative acknowledgment.
    ///
    /// Commit offset N means "processed everything up to and including N".
    /// Watermark advances directly to N+1. Bitmap unused.
    #[default]
    Cumulative,

    /// Pulsar-style individual acknowledgment.
    ///
    /// Ack individual offsets, can have gaps for parallel processing.
    /// Watermark advances only when contiguous offsets are committed.
    Individual,
}

// -----------------------------------------------------------------------------
// Lease State
// -----------------------------------------------------------------------------

/// State of a lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LeaseState {
    /// Lease is active and valid.
    #[default]
    Active,
    /// Lease has expired (offsets available for re-lease).
    Expired,
    /// Lease was released by consumer (offsets committed or abandoned).
    Released,
}

// -----------------------------------------------------------------------------
// Lease
// -----------------------------------------------------------------------------

/// An active lease for a range of offsets.
///
/// Leases prevent redelivery during processing. A consumer must hold a lease
/// on an offset before committing it.
#[derive(Debug, Clone)]
pub struct Lease {
    /// Unique lease identifier.
    pub lease_id: LeaseId,
    /// Consumer holding this lease.
    pub consumer_id: ConsumerId,
    /// Consumer group this lease belongs to.
    pub group_id: ConsumerGroupId,
    /// Partition key (topic + partition).
    pub partition_key: PartitionKey,
    /// First offset in the lease (inclusive).
    pub from_offset: Offset,
    /// Last offset in the lease (inclusive).
    pub to_offset: Offset,
    /// Lease creation timestamp (microseconds since epoch).
    pub created_at_us: u64,
    /// Lease expiration timestamp (microseconds since epoch).
    pub expires_at_us: u64,
    /// Current state of the lease.
    pub state: LeaseState,
}

impl Lease {
    /// Creates a new active lease.
    ///
    /// # Panics
    ///
    /// Panics if `from_offset > to_offset` or `duration_us == 0`.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        lease_id: LeaseId,
        consumer_id: ConsumerId,
        group_id: ConsumerGroupId,
        partition_key: PartitionKey,
        from_offset: Offset,
        to_offset: Offset,
        created_at_us: u64,
        duration_us: u64,
    ) -> Self {
        // TigerStyle: Assert preconditions.
        assert!(
            from_offset.get() <= to_offset.get(),
            "from_offset ({}) must be <= to_offset ({})",
            from_offset.get(),
            to_offset.get()
        );
        assert!(duration_us > 0, "duration must be positive");

        let lease = Self {
            lease_id,
            consumer_id,
            group_id,
            partition_key,
            from_offset,
            to_offset,
            created_at_us,
            expires_at_us: created_at_us.saturating_add(duration_us),
            state: LeaseState::Active,
        };

        // TigerStyle: Assert postconditions.
        assert!(lease.expires_at_us >= lease.created_at_us);
        assert!(lease.state == LeaseState::Active);

        lease
    }

    /// Returns true if the lease has expired at the given time.
    #[must_use]
    pub const fn is_expired_at(&self, current_time_us: u64) -> bool {
        current_time_us >= self.expires_at_us
    }

    /// Returns the number of offsets covered by this lease.
    #[must_use]
    pub const fn offset_count(&self) -> u64 {
        self.to_offset.get() - self.from_offset.get() + 1
    }

    /// Returns true if the given offset is covered by this lease.
    #[must_use]
    pub const fn contains_offset(&self, offset: Offset) -> bool {
        offset.get() >= self.from_offset.get() && offset.get() <= self.to_offset.get()
    }
}

// -----------------------------------------------------------------------------
// Partition Progress
// -----------------------------------------------------------------------------

/// Per-partition progress within a consumer group.
///
/// Tracks committed offsets using a bitmap for efficient sparse tracking,
/// active leases, and the low watermark for eviction coordination.
#[derive(Debug, Clone)]
pub struct PartitionProgress {
    /// Partition key (topic + partition).
    pub partition_key: PartitionKey,
    /// Acknowledgment mode (Cumulative or Individual).
    pub ack_mode: AckMode,
    /// Low watermark - all offsets below this are committed and safe to evict.
    /// Advances as contiguous offsets are committed.
    pub low_watermark: Offset,
    /// Bitmap of committed offsets above `low_watermark` (Individual mode only).
    /// Offset N is committed if bit (N - `low_watermark`) is set.
    pub committed_bitmap: RoaringBitmap,
    /// Active leases on this partition (keyed by `lease_id`).
    pub active_leases: HashMap<LeaseId, Lease>,
    /// Next offset available for leasing.
    pub next_lease_offset: Offset,
}

impl PartitionProgress {
    /// Creates new partition progress starting at the given offset.
    #[must_use]
    pub fn new(partition_key: PartitionKey, start_offset: Offset, ack_mode: AckMode) -> Self {
        Self {
            partition_key,
            ack_mode,
            low_watermark: start_offset,
            committed_bitmap: RoaringBitmap::new(),
            active_leases: HashMap::new(),
            next_lease_offset: start_offset,
        }
    }

    /// Commits an offset, updating the bitmap and potentially advancing `low_watermark`.
    ///
    /// In Cumulative mode, advances watermark directly to offset+1.
    /// In Individual mode, adds to bitmap and advances watermark only if contiguous.
    ///
    /// # Panics
    ///
    /// Panics if offset is below `low_watermark`.
    pub fn commit_offset(&mut self, offset: Offset) {
        // TigerStyle: Assert precondition.
        assert!(
            offset.get() >= self.low_watermark.get(),
            "cannot commit offset {} below low_watermark {}",
            offset.get(),
            self.low_watermark.get()
        );

        match self.ack_mode {
            AckMode::Cumulative => {
                // Cumulative: advance watermark directly, bitmap stays empty.
                self.low_watermark = offset.next();
                // Clean up leases that are now fully below the watermark.
                self.cleanup_committed_leases();
            }
            AckMode::Individual => {
                // Individual: add to bitmap, advance watermark if contiguous.
                let bitmap_index = offset.get() - self.low_watermark.get();
                #[allow(clippy::cast_possible_truncation)]
                self.committed_bitmap.insert(bitmap_index as u32);
                self.advance_watermark();
            }
        }

        // TigerStyle: Assert postcondition.
        assert!(
            self.ack_mode == AckMode::Cumulative || !self.committed_bitmap.contains(0),
            "bit 0 should never be set after commit in Individual mode"
        );
    }

    /// Advances `low_watermark` as far as contiguous committed offsets allow.
    fn advance_watermark(&mut self) {
        let mut advance_count = 0u64;

        // TigerStyle: Bound the loop.
        const MAX_ADVANCE: u64 = 1_000_000;

        while advance_count < MAX_ADVANCE {
            #[allow(clippy::cast_possible_truncation)]
            if !self.committed_bitmap.contains(advance_count as u32) {
                break;
            }
            #[allow(clippy::cast_possible_truncation)]
            self.committed_bitmap.remove(advance_count as u32);
            advance_count += 1;
        }

        if advance_count > 0 {
            // Shift all remaining bits down.
            let mut new_bitmap = RoaringBitmap::new();
            for bit in &self.committed_bitmap {
                let new_bit = u64::from(bit).saturating_sub(advance_count);
                #[allow(clippy::cast_possible_truncation)]
                new_bitmap.insert(new_bit as u32);
            }
            self.committed_bitmap = new_bitmap;
            self.low_watermark = Offset::new(self.low_watermark.get() + advance_count);

            // Clean up leases that are now fully below the watermark.
            self.cleanup_committed_leases();
        }

        // TigerStyle: Assert postcondition.
        assert!(
            !self.committed_bitmap.contains(0),
            "bit 0 should never be set after advance"
        );
    }

    /// Removes leases that are fully below the watermark.
    ///
    /// These leases have been fully committed and are no longer needed.
    /// Returns the IDs of removed leases for consumer cleanup.
    fn cleanup_committed_leases(&mut self) -> Vec<LeaseId> {
        let fully_committed: Vec<LeaseId> = self
            .active_leases
            .iter()
            .filter(|(_, lease)| lease.to_offset.get() < self.low_watermark.get())
            .map(|(id, _)| *id)
            .collect();

        for lease_id in &fully_committed {
            self.active_leases.remove(lease_id);
        }

        fully_committed
    }

    /// Returns the committed offset (highest contiguous committed offset).
    #[must_use]
    pub const fn committed_offset(&self) -> Offset {
        if self.low_watermark.get() == 0 {
            Offset::new(0)
        } else {
            Offset::new(self.low_watermark.get().saturating_sub(1))
        }
    }

    /// Returns true if the given offset is committed.
    #[must_use]
    pub fn is_committed(&self, offset: Offset) -> bool {
        if offset.get() < self.low_watermark.get() {
            true
        } else {
            let bitmap_index = offset.get() - self.low_watermark.get();
            #[allow(clippy::cast_possible_truncation)]
            self.committed_bitmap.contains(bitmap_index as u32)
        }
    }

    /// Returns true if the given offset is currently leased (and not expired).
    #[must_use]
    pub fn is_leased(&self, offset: Offset, current_time_us: u64) -> bool {
        for lease in self.active_leases.values() {
            if lease.contains_offset(offset) && !lease.is_expired_at(current_time_us) {
                return true;
            }
        }
        false
    }

    /// Removes expired leases at the given time.
    ///
    /// Returns the number of leases removed.
    pub fn expire_leases(&mut self, current_time_us: u64) -> u32 {
        let expired: Vec<LeaseId> = self
            .active_leases
            .iter()
            .filter(|(_, lease)| lease.is_expired_at(current_time_us))
            .map(|(id, _)| *id)
            .collect();

        #[allow(clippy::cast_possible_truncation)]
        let count = expired.len() as u32;
        for lease_id in expired {
            self.active_leases.remove(&lease_id);
        }
        count
    }
}

// -----------------------------------------------------------------------------
// Consumer State
// -----------------------------------------------------------------------------

/// Per-consumer state within a group.
#[derive(Debug, Clone)]
pub struct ConsumerState {
    /// Consumer ID.
    pub consumer_id: ConsumerId,
    /// Active leases held by this consumer.
    pub lease_ids: Vec<LeaseId>,
    /// Last heartbeat timestamp (microseconds since epoch).
    pub last_heartbeat_us: u64,
}

impl ConsumerState {
    /// Creates a new consumer state.
    #[must_use]
    pub const fn new(consumer_id: ConsumerId, current_time_us: u64) -> Self {
        Self {
            consumer_id,
            lease_ids: Vec::new(),
            last_heartbeat_us: current_time_us,
        }
    }
}

// -----------------------------------------------------------------------------
// Consumer Group State
// -----------------------------------------------------------------------------

/// Per-consumer group state.
///
/// Contains progress for all partitions this group is consuming.
#[derive(Debug, Clone)]
pub struct ConsumerGroupState {
    /// Consumer group ID.
    pub group_id: ConsumerGroupId,
    /// Active consumers in this group.
    pub consumers: HashMap<ConsumerId, ConsumerState>,
    /// Progress per partition.
    pub partitions: HashMap<PartitionKey, PartitionProgress>,
    /// Default acknowledgment mode for new partitions.
    pub default_ack_mode: AckMode,
    /// Creation timestamp (microseconds since epoch).
    pub created_at_us: u64,
}

impl ConsumerGroupState {
    /// Creates a new consumer group state.
    #[must_use]
    pub fn new(group_id: ConsumerGroupId, created_at_us: u64, default_ack_mode: AckMode) -> Self {
        Self {
            group_id,
            consumers: HashMap::new(),
            partitions: HashMap::new(),
            default_ack_mode,
            created_at_us,
        }
    }

    /// Gets or creates partition progress for the given partition.
    pub fn get_or_create_partition(
        &mut self,
        partition_key: PartitionKey,
        start_offset: Offset,
    ) -> &mut PartitionProgress {
        let ack_mode = self.default_ack_mode;
        self.partitions
            .entry(partition_key)
            .or_insert_with(|| PartitionProgress::new(partition_key, start_offset, ack_mode))
    }

    /// Removes stale lease references from consumers.
    ///
    /// When leases are cleaned up from partitions (due to expiration or commit),
    /// the consumer's `lease_ids` list may still reference them. This method
    /// syncs consumer state by removing references to non-existent leases.
    pub fn cleanup_stale_consumer_leases(&mut self) {
        // Collect all valid lease IDs from all partitions.
        let valid_lease_ids: std::collections::HashSet<LeaseId> = self
            .partitions
            .values()
            .flat_map(|p| p.active_leases.keys().copied())
            .collect();

        // Remove invalid lease_ids from each consumer.
        for consumer in self.consumers.values_mut() {
            consumer.lease_ids.retain(|id| valid_lease_ids.contains(id));
        }
    }
}

// -----------------------------------------------------------------------------
// Progress Config
// -----------------------------------------------------------------------------

/// Configuration for progress tracking.
#[derive(Debug, Clone)]
pub struct ProgressConfig {
    /// Maximum consumer groups per partition.
    pub max_groups_per_partition: u32,
    /// Maximum consumers per group.
    pub max_consumers_per_group: u32,
    /// Maximum lease duration in microseconds.
    pub max_lease_duration_us: u64,
    /// Default lease duration in microseconds.
    pub default_lease_duration_us: u64,
    /// Maximum offsets per lease request.
    pub max_offsets_per_lease: u32,
    /// Default acknowledgment mode.
    pub default_ack_mode: AckMode,
}

impl ProgressConfig {
    /// Creates config from system limits.
    #[must_use]
    pub const fn from_limits(limits: &Limits) -> Self {
        Self {
            max_groups_per_partition: limits.max_consumer_groups_per_partition,
            max_consumers_per_group: limits.max_consumers_per_group,
            max_lease_duration_us: limits.max_lease_duration_us,
            default_lease_duration_us: 60_000_000, // 1 minute default.
            max_offsets_per_lease: 10_000,
            default_ack_mode: AckMode::Cumulative,
        }
    }

    /// Creates config for testing with relaxed limits.
    #[must_use]
    pub const fn for_testing() -> Self {
        Self {
            max_groups_per_partition: 100,
            max_consumers_per_group: 10,
            max_lease_duration_us: 60_000_000, // 1 minute.
            default_lease_duration_us: 10_000_000, // 10 seconds.
            max_offsets_per_lease: 1000,
            default_ack_mode: AckMode::Cumulative,
        }
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_key() {
        let key1 = PartitionKey::new(TopicId::new(1), PartitionId::new(2));
        let key2 = PartitionKey::new(TopicId::new(1), PartitionId::new(2));
        let key3 = PartitionKey::new(TopicId::new(1), PartitionId::new(3));

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_lease_creation() {
        let lease = Lease::new(
            LeaseId::new(1),
            ConsumerId::new(10),
            ConsumerGroupId::new(100),
            PartitionKey::new(TopicId::new(1), PartitionId::new(0)),
            Offset::new(0),
            Offset::new(9),
            1000,
            5000,
        );

        assert_eq!(lease.offset_count(), 10);
        assert!(lease.contains_offset(Offset::new(0)));
        assert!(lease.contains_offset(Offset::new(9)));
        assert!(!lease.contains_offset(Offset::new(10)));
        assert!(!lease.is_expired_at(5999));
        assert!(lease.is_expired_at(6000));
    }

    #[test]
    fn test_cumulative_mode_commit() {
        let key = PartitionKey::new(TopicId::new(1), PartitionId::new(0));
        let mut progress = PartitionProgress::new(key, Offset::new(0), AckMode::Cumulative);

        // Cumulative: commit offset 5 = "processed up to 5".
        progress.commit_offset(Offset::new(5));

        assert_eq!(progress.low_watermark.get(), 6);
        assert!(progress.committed_bitmap.is_empty());
        assert!(progress.is_committed(Offset::new(0)));
        assert!(progress.is_committed(Offset::new(5)));
        assert!(!progress.is_committed(Offset::new(6)));
    }

    #[test]
    fn test_individual_mode_commit_contiguous() {
        let key = PartitionKey::new(TopicId::new(1), PartitionId::new(0));
        let mut progress = PartitionProgress::new(key, Offset::new(0), AckMode::Individual);

        // Individual: commit 0, 1, 2 contiguously.
        progress.commit_offset(Offset::new(0));
        assert_eq!(progress.low_watermark.get(), 1);

        progress.commit_offset(Offset::new(1));
        assert_eq!(progress.low_watermark.get(), 2);

        progress.commit_offset(Offset::new(2));
        assert_eq!(progress.low_watermark.get(), 3);

        assert!(progress.committed_bitmap.is_empty());
    }

    #[test]
    fn test_individual_mode_commit_sparse() {
        let key = PartitionKey::new(TopicId::new(1), PartitionId::new(0));
        let mut progress = PartitionProgress::new(key, Offset::new(0), AckMode::Individual);

        // Individual: commit 0, 2, 4 (sparse).
        progress.commit_offset(Offset::new(0));
        assert_eq!(progress.low_watermark.get(), 1);

        progress.commit_offset(Offset::new(2));
        assert_eq!(progress.low_watermark.get(), 1); // Can't advance past 1.
        assert!(progress.committed_bitmap.contains(1)); // 2 - 1 = 1.

        progress.commit_offset(Offset::new(4));
        assert_eq!(progress.low_watermark.get(), 1);
        assert!(progress.committed_bitmap.contains(3)); // 4 - 1 = 3.

        // Now fill the gap at offset 1.
        progress.commit_offset(Offset::new(1));
        assert_eq!(progress.low_watermark.get(), 3); // Advances past 0, 1, 2.

        // Now fill the gap at offset 3.
        progress.commit_offset(Offset::new(3));
        assert_eq!(progress.low_watermark.get(), 5); // Advances past 3, 4.

        assert!(progress.committed_bitmap.is_empty());
    }

    #[test]
    fn test_lease_expiration() {
        let key = PartitionKey::new(TopicId::new(1), PartitionId::new(0));
        let mut progress = PartitionProgress::new(key, Offset::new(0), AckMode::Cumulative);

        // Add two leases.
        let lease1 = Lease::new(
            LeaseId::new(1),
            ConsumerId::new(10),
            ConsumerGroupId::new(100),
            key,
            Offset::new(0),
            Offset::new(9),
            1000,
            1000, // Expires at 2000.
        );
        let lease2 = Lease::new(
            LeaseId::new(2),
            ConsumerId::new(10),
            ConsumerGroupId::new(100),
            key,
            Offset::new(10),
            Offset::new(19),
            1000,
            5000, // Expires at 6000.
        );

        progress.active_leases.insert(lease1.lease_id, lease1);
        progress.active_leases.insert(lease2.lease_id, lease2);

        assert_eq!(progress.active_leases.len(), 2);

        // Expire at time 3000 - only lease1 should expire.
        let expired = progress.expire_leases(3000);
        assert_eq!(expired, 1);
        assert_eq!(progress.active_leases.len(), 1);
        assert!(progress.active_leases.contains_key(&LeaseId::new(2)));

        // Expire at time 7000 - lease2 should expire.
        let expired = progress.expire_leases(7000);
        assert_eq!(expired, 1);
        assert!(progress.active_leases.is_empty());
    }

    #[test]
    fn test_consumer_group_state() {
        let mut group =
            ConsumerGroupState::new(ConsumerGroupId::new(1), 1000, AckMode::Cumulative);

        let key = PartitionKey::new(TopicId::new(1), PartitionId::new(0));
        let progress = group.get_or_create_partition(key, Offset::new(0));

        assert_eq!(progress.ack_mode, AckMode::Cumulative);
        assert_eq!(progress.low_watermark.get(), 0);
    }

    #[test]
    fn test_progress_config_from_limits() {
        let limits = Limits::default();
        let config = ProgressConfig::from_limits(&limits);

        assert_eq!(
            config.max_groups_per_partition,
            limits.max_consumer_groups_per_partition
        );
        assert_eq!(
            config.max_consumers_per_group,
            limits.max_consumers_per_group
        );
        assert_eq!(config.max_lease_duration_us, limits.max_lease_duration_us);
    }
}
