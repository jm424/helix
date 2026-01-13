//! Progress manager for coordinating consumer progress operations.
//!
//! The `ProgressManager` orchestrates consumer progress tracking including:
//! - Consumer group and consumer registration
//! - Lease management (create, extend, expire)
//! - Offset commits with watermark advancement
//! - Progress queries for eviction coordination

#![allow(clippy::items_after_statements)]

use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use helix_core::{ConsumerGroupId, ConsumerId, LeaseId, Offset, PartitionId, TopicId};
use tracing::{debug, info};

use crate::error::{ProgressError, ProgressResult};
use crate::storage::ProgressStore;
use crate::types::{
    AckMode, ConsumerGroupState, ConsumerState, Lease, PartitionKey, PartitionProgress,
    ProgressConfig,
};

// -----------------------------------------------------------------------------
// Progress Manager
// -----------------------------------------------------------------------------

/// Progress manager orchestrates consumer progress tracking.
///
/// # Design
///
/// The manager maintains consumer group state, partition progress, and leases.
/// It does NOT perform background work - the caller must invoke methods explicitly.
/// All mutations are persisted through the `ProgressStore`.
///
/// # Thread Safety
///
/// The manager is `Send + Sync` and can be shared across tasks.
pub struct ProgressManager<S: ProgressStore> {
    /// Storage backend.
    store: Arc<S>,
    /// Configuration.
    config: ProgressConfig,
    /// Lease ID generator (monotonic).
    next_lease_id: AtomicU64,
}

impl<S: ProgressStore> ProgressManager<S> {
    /// Creates a new progress manager.
    #[must_use]
    pub fn new(store: S, config: ProgressConfig) -> Self {
        Self {
            store: Arc::new(store),
            config,
            next_lease_id: AtomicU64::new(1),
        }
    }

    /// Returns a reference to the underlying store.
    #[must_use]
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Returns the configuration.
    #[must_use]
    pub const fn config(&self) -> &ProgressConfig {
        &self.config
    }

    // -------------------------------------------------------------------------
    // Consumer Group Management
    // -------------------------------------------------------------------------

    /// Creates or gets a consumer group.
    ///
    /// If the group doesn't exist, creates it with the default ack mode.
    ///
    /// # Errors
    ///
    /// Returns error if storage operation fails.
    pub async fn get_or_create_group(
        &self,
        group_id: ConsumerGroupId,
        current_time_us: u64,
    ) -> ProgressResult<ConsumerGroupState> {
        if let Some(group) = self.store.get_group(group_id).await? {
            return Ok(group);
        }

        let group =
            ConsumerGroupState::new(group_id, current_time_us, self.config.default_ack_mode);
        self.store.save_group(&group).await?;

        info!(group_id = %group_id, "Created consumer group");
        Ok(group)
    }

    /// Creates or gets a consumer group with a specific ack mode.
    ///
    /// # Errors
    ///
    /// Returns error if storage operation fails.
    pub async fn get_or_create_group_with_mode(
        &self,
        group_id: ConsumerGroupId,
        current_time_us: u64,
        ack_mode: AckMode,
    ) -> ProgressResult<ConsumerGroupState> {
        if let Some(group) = self.store.get_group(group_id).await? {
            return Ok(group);
        }

        let group = ConsumerGroupState::new(group_id, current_time_us, ack_mode);
        self.store.save_group(&group).await?;

        info!(group_id = %group_id, ?ack_mode, "Created consumer group");
        Ok(group)
    }

    /// Registers a consumer in a group.
    ///
    /// # Errors
    ///
    /// Returns error if too many consumers or storage fails.
    pub async fn register_consumer(
        &self,
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        current_time_us: u64,
    ) -> ProgressResult<()> {
        let mut group = self.get_or_create_group(group_id, current_time_us).await?;

        // TigerStyle: Check limit.
        #[allow(clippy::cast_possible_truncation)]
        if group.consumers.len() >= self.config.max_consumers_per_group as usize {
            return Err(ProgressError::TooManyConsumers {
                group_id,
                count: group.consumers.len() as u32,
                max: self.config.max_consumers_per_group,
            });
        }

        if let Entry::Vacant(e) = group.consumers.entry(consumer_id) {
            e.insert(ConsumerState::new(consumer_id, current_time_us));
            self.store.save_group(&group).await?;
            debug!(group_id = %group_id, consumer_id = %consumer_id, "Registered consumer");
        }

        Ok(())
    }

    /// Removes a consumer from a group.
    ///
    /// Also removes all leases held by this consumer.
    ///
    /// # Errors
    ///
    /// Returns error if group not found or storage fails.
    pub async fn unregister_consumer(
        &self,
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
    ) -> ProgressResult<()> {
        let mut group = self
            .store
            .get_group(group_id)
            .await?
            .ok_or(ProgressError::GroupNotFound { group_id })?;

        // Remove consumer.
        group.consumers.remove(&consumer_id);

        // Remove leases held by this consumer.
        for progress in group.partitions.values_mut() {
            progress
                .active_leases
                .retain(|_, lease| lease.consumer_id != consumer_id);
        }

        self.store.save_group(&group).await?;
        debug!(group_id = %group_id, consumer_id = %consumer_id, "Unregistered consumer");
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Lease Management
    // -------------------------------------------------------------------------

    /// Leases offsets for consumption.
    ///
    /// Returns a lease covering offsets from `from_offset` up to `max_count` offsets.
    /// Only unleased, uncommitted offsets are included.
    ///
    /// # Arguments
    ///
    /// * `group_id` - Consumer group
    /// * `topic_id` - Topic to consume from
    /// * `partition_id` - Partition to consume from
    /// * `consumer_id` - Consumer requesting the lease
    /// * `from_offset` - Starting offset (inclusive)
    /// * `max_count` - Maximum offsets to include in lease
    /// * `duration_us` - Lease duration in microseconds
    /// * `current_time_us` - Current time for expiration calculation
    ///
    /// # Errors
    ///
    /// Returns error if consumer not registered, duration exceeds max, or storage fails.
    ///
    /// # Panics
    ///
    /// Panics if `max_count` or `duration_us` is zero.
    #[allow(clippy::too_many_arguments)]
    pub async fn lease_offsets(
        &self,
        group_id: ConsumerGroupId,
        topic_id: TopicId,
        partition_id: PartitionId,
        consumer_id: ConsumerId,
        from_offset: Offset,
        max_count: u32,
        duration_us: u64,
        current_time_us: u64,
    ) -> ProgressResult<Option<Lease>> {
        // TigerStyle: Assert preconditions.
        assert!(max_count > 0, "max_count must be positive");
        assert!(duration_us > 0, "duration_us must be positive");

        // Validate lease duration.
        if duration_us > self.config.max_lease_duration_us {
            return Err(ProgressError::LeaseDurationExceeded {
                requested_us: duration_us,
                max_us: self.config.max_lease_duration_us,
            });
        }

        let max_count = max_count.min(self.config.max_offsets_per_lease);

        let mut group = self
            .store
            .get_group(group_id)
            .await?
            .ok_or(ProgressError::GroupNotFound { group_id })?;

        // Verify consumer is registered.
        if !group.consumers.contains_key(&consumer_id) {
            return Err(ProgressError::ConsumerNotFound {
                group_id,
                consumer_id,
            });
        }

        let partition_key = PartitionKey::new(topic_id, partition_id);

        // Create lease ID upfront.
        let lease_id = LeaseId::new(self.next_lease_id.fetch_add(1, Ordering::Relaxed));

        // Find available offsets and create lease.
        // Use a block to limit the mutable borrow of partition progress.
        let lease = {
            let progress = group.get_or_create_partition(partition_key, from_offset);

            // Expire old leases first.
            progress.expire_leases(current_time_us);

            // Find available offsets starting from from_offset.
            let mut available_start: Option<Offset> = None;
            let mut count = 0u32;

            // TigerStyle: Bound the search.
            const MAX_SEARCH: u32 = 100_000;
            let mut offset = from_offset;

            for _ in 0..MAX_SEARCH {
                if count >= max_count {
                    break;
                }

                // Skip committed and leased offsets.
                if progress.is_committed(offset) || progress.is_leased(offset, current_time_us) {
                    if count > 0 {
                        break; // Return contiguous range found so far.
                    }
                    offset = offset.next();
                    continue;
                }

                if available_start.is_none() {
                    available_start = Some(offset);
                }
                count += 1;
                offset = offset.next();
            }

            let Some(start) = available_start else {
                return Ok(None); // No available offsets.
            };

            if count == 0 {
                return Ok(None);
            }

            let end = Offset::new(start.get() + u64::from(count) - 1);

            // Create the lease.
            let lease = Lease::new(
                lease_id,
                consumer_id,
                group_id,
                partition_key,
                start,
                end,
                current_time_us,
                duration_us,
            );

            // Add lease to progress.
            progress.active_leases.insert(lease_id, lease.clone());

            // Update next lease offset.
            if end.get() >= progress.next_lease_offset.get() {
                progress.next_lease_offset = end.next();
            }

            lease
        };

        // Add lease to consumer (separate borrow).
        if let Some(consumer) = group.consumers.get_mut(&consumer_id) {
            consumer.lease_ids.push(lease_id);
        }

        self.store.save_group(&group).await?;

        debug!(
            group_id = %group_id,
            consumer_id = %consumer_id,
            lease_id = %lease_id,
            from = lease.from_offset.get(),
            to = lease.to_offset.get(),
            "Created lease"
        );

        Ok(Some(lease))
    }

    /// Extends an existing lease.
    ///
    /// # Errors
    ///
    /// Returns error if lease not found, expired, not owned by consumer, or
    /// extended duration would exceed maximum.
    pub async fn extend_lease(
        &self,
        group_id: ConsumerGroupId,
        partition_key: PartitionKey,
        consumer_id: ConsumerId,
        lease_id: LeaseId,
        additional_duration_us: u64,
        current_time_us: u64,
    ) -> ProgressResult<Lease> {
        let mut group = self
            .store
            .get_group(group_id)
            .await?
            .ok_or(ProgressError::GroupNotFound { group_id })?;

        let progress = group.partitions.get_mut(&partition_key).ok_or(
            ProgressError::PartitionNotFound {
                group_id,
                topic_id: partition_key.topic_id,
                partition_id: partition_key.partition_id,
            },
        )?;

        let lease = progress
            .active_leases
            .get_mut(&lease_id)
            .ok_or(ProgressError::LeaseNotFound { lease_id })?;

        // Verify ownership.
        if lease.consumer_id != consumer_id {
            return Err(ProgressError::OffsetNotLeased {
                consumer_id,
                offset: lease.from_offset,
            });
        }

        // Check if already expired.
        if lease.is_expired_at(current_time_us) {
            return Err(ProgressError::LeaseExpired {
                lease_id,
                expired_at_us: lease.expires_at_us,
            });
        }

        // Validate new duration.
        let new_expires = current_time_us.saturating_add(additional_duration_us);
        let total_duration = new_expires.saturating_sub(lease.created_at_us);
        if total_duration > self.config.max_lease_duration_us {
            return Err(ProgressError::LeaseDurationExceeded {
                requested_us: total_duration,
                max_us: self.config.max_lease_duration_us,
            });
        }

        lease.expires_at_us = new_expires;
        let updated_lease = lease.clone();

        self.store.save_group(&group).await?;

        debug!(
            lease_id = %lease_id,
            new_expires = new_expires,
            "Extended lease"
        );

        Ok(updated_lease)
    }

    /// Releases a lease explicitly.
    ///
    /// The offsets become available for re-lease immediately.
    ///
    /// # Errors
    ///
    /// Returns error if lease not found or not owned by consumer.
    pub async fn release_lease(
        &self,
        group_id: ConsumerGroupId,
        partition_key: PartitionKey,
        consumer_id: ConsumerId,
        lease_id: LeaseId,
    ) -> ProgressResult<()> {
        let mut group = self
            .store
            .get_group(group_id)
            .await?
            .ok_or(ProgressError::GroupNotFound { group_id })?;

        let progress = group.partitions.get_mut(&partition_key).ok_or(
            ProgressError::PartitionNotFound {
                group_id,
                topic_id: partition_key.topic_id,
                partition_id: partition_key.partition_id,
            },
        )?;

        let lease = progress
            .active_leases
            .get(&lease_id)
            .ok_or(ProgressError::LeaseNotFound { lease_id })?;

        // Verify ownership.
        if lease.consumer_id != consumer_id {
            return Err(ProgressError::OffsetNotLeased {
                consumer_id,
                offset: lease.from_offset,
            });
        }

        progress.active_leases.remove(&lease_id);

        // Remove from consumer's lease list.
        if let Some(consumer) = group.consumers.get_mut(&consumer_id) {
            consumer.lease_ids.retain(|&id| id != lease_id);
        }

        self.store.save_group(&group).await?;

        debug!(lease_id = %lease_id, "Released lease");
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Commit Operations
    // -------------------------------------------------------------------------

    /// Commits a processed offset.
    ///
    /// In Cumulative mode: advances watermark directly to offset+1.
    /// In Individual mode: marks offset as committed, advances watermark if contiguous.
    ///
    /// # Errors
    ///
    /// Returns error if offset not leased by consumer, already committed, or storage fails.
    pub async fn commit_offset(
        &self,
        group_id: ConsumerGroupId,
        topic_id: TopicId,
        partition_id: PartitionId,
        consumer_id: ConsumerId,
        offset: Offset,
        current_time_us: u64,
    ) -> ProgressResult<()> {
        let mut group = self
            .store
            .get_group(group_id)
            .await?
            .ok_or(ProgressError::GroupNotFound { group_id })?;

        let partition_key = PartitionKey::new(topic_id, partition_id);
        let progress = group.partitions.get_mut(&partition_key).ok_or(
            ProgressError::PartitionNotFound {
                group_id,
                topic_id,
                partition_id,
            },
        )?;

        // Check if already committed.
        if progress.is_committed(offset) {
            return Err(ProgressError::OffsetAlreadyCommitted {
                topic_id,
                partition_id,
                offset,
            });
        }

        // Verify offset is leased by this consumer.
        let mut found_lease: Option<LeaseId> = None;
        for (lease_id, lease) in &progress.active_leases {
            if lease.consumer_id == consumer_id
                && lease.contains_offset(offset)
                && !lease.is_expired_at(current_time_us)
            {
                found_lease = Some(*lease_id);
                break;
            }
        }

        if found_lease.is_none() {
            return Err(ProgressError::OffsetNotLeased {
                consumer_id,
                offset,
            });
        }

        // Commit the offset and capture watermarks before releasing borrow.
        let old_watermark = progress.low_watermark;
        progress.commit_offset(offset);
        let new_watermark = progress.low_watermark;

        // Clean up stale lease references from consumers.
        group.cleanup_stale_consumer_leases();

        self.store.save_group(&group).await?;

        debug!(
            group_id = %group_id,
            offset = offset.get(),
            old_watermark = old_watermark.get(),
            new_watermark = new_watermark.get(),
            "Committed offset"
        );

        Ok(())
    }

    /// Commits multiple offsets in a batch.
    ///
    /// More efficient than calling `commit_offset` multiple times.
    ///
    /// # Errors
    ///
    /// Returns error on first failure. Previously committed offsets in the batch
    /// are persisted.
    pub async fn commit_offsets(
        &self,
        group_id: ConsumerGroupId,
        topic_id: TopicId,
        partition_id: PartitionId,
        consumer_id: ConsumerId,
        offsets: &[Offset],
        current_time_us: u64,
    ) -> ProgressResult<()> {
        if offsets.is_empty() {
            return Ok(());
        }

        let mut group = self
            .store
            .get_group(group_id)
            .await?
            .ok_or(ProgressError::GroupNotFound { group_id })?;

        let partition_key = PartitionKey::new(topic_id, partition_id);
        let progress = group.partitions.get_mut(&partition_key).ok_or(
            ProgressError::PartitionNotFound {
                group_id,
                topic_id,
                partition_id,
            },
        )?;

        for &offset in offsets {
            // Check if already committed.
            if progress.is_committed(offset) {
                continue; // Skip, don't error on batch.
            }

            // Verify offset is leased by this consumer.
            let mut found = false;
            for lease in progress.active_leases.values() {
                if lease.consumer_id == consumer_id
                    && lease.contains_offset(offset)
                    && !lease.is_expired_at(current_time_us)
                {
                    found = true;
                    break;
                }
            }

            if !found {
                return Err(ProgressError::OffsetNotLeased {
                    consumer_id,
                    offset,
                });
            }

            progress.commit_offset(offset);
        }

        // Capture watermark before releasing borrow.
        let new_watermark = progress.low_watermark;

        // Clean up stale lease references from consumers.
        group.cleanup_stale_consumer_leases();

        self.store.save_group(&group).await?;

        debug!(
            group_id = %group_id,
            count = offsets.len(),
            new_watermark = new_watermark.get(),
            "Committed offset batch"
        );

        Ok(())
    }

    /// Commits all offsets in a lease and releases the lease.
    ///
    /// In Cumulative mode: advances watermark directly to `to_offset+1`.
    /// In Individual mode: marks all offsets in the lease range as committed.
    ///
    /// # Errors
    ///
    /// Returns error if lease not found, not owned by consumer, expired, or storage fails.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_lease(
        &self,
        group_id: ConsumerGroupId,
        topic_id: TopicId,
        partition_id: PartitionId,
        consumer_id: ConsumerId,
        lease_id: LeaseId,
        current_time_us: u64,
    ) -> ProgressResult<()> {
        let mut group = self
            .store
            .get_group(group_id)
            .await?
            .ok_or(ProgressError::GroupNotFound { group_id })?;

        let partition_key = PartitionKey::new(topic_id, partition_id);
        let progress = group.partitions.get_mut(&partition_key).ok_or(
            ProgressError::PartitionNotFound {
                group_id,
                topic_id,
                partition_id,
            },
        )?;

        // Find and validate the lease.
        let lease = progress
            .active_leases
            .get(&lease_id)
            .ok_or(ProgressError::LeaseNotFound { lease_id })?;

        // Verify ownership.
        if lease.consumer_id != consumer_id {
            return Err(ProgressError::OffsetNotLeased {
                consumer_id,
                offset: lease.from_offset,
            });
        }

        // Check expiration.
        if lease.is_expired_at(current_time_us) {
            return Err(ProgressError::LeaseExpired {
                lease_id,
                expired_at_us: lease.expires_at_us,
            });
        }

        // Get the offset range from the lease.
        let from_offset = lease.from_offset;
        let to_offset = lease.to_offset;

        // Commit all offsets in the lease range.
        for offset_val in from_offset.get()..=to_offset.get() {
            let offset = Offset::new(offset_val);
            if !progress.is_committed(offset) {
                progress.commit_offset(offset);
            }
        }

        // Remove the lease.
        progress.active_leases.remove(&lease_id);

        // Remove from consumer's lease list.
        if let Some(consumer) = group.consumers.get_mut(&consumer_id) {
            consumer.lease_ids.retain(|&id| id != lease_id);
        }

        // Capture watermark before releasing borrow.
        let new_watermark = progress.low_watermark;

        // Clean up stale lease references from consumers.
        group.cleanup_stale_consumer_leases();

        self.store.save_group(&group).await?;

        debug!(
            lease_id = %lease_id,
            from = from_offset.get(),
            to = to_offset.get(),
            new_watermark = new_watermark.get(),
            "Committed lease"
        );

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Query Operations
    // -------------------------------------------------------------------------

    /// Gets the committed offset for a partition (highest contiguous committed).
    ///
    /// # Errors
    ///
    /// Returns error if group not found or storage operation fails.
    pub async fn fetch_committed(
        &self,
        group_id: ConsumerGroupId,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> ProgressResult<Option<Offset>> {
        let group = self
            .store
            .get_group(group_id)
            .await?
            .ok_or(ProgressError::GroupNotFound { group_id })?;

        let partition_key = PartitionKey::new(topic_id, partition_id);
        Ok(group
            .partitions
            .get(&partition_key)
            .map(PartitionProgress::committed_offset))
    }

    /// Gets the low watermark for a partition within a group.
    ///
    /// All offsets below this are committed and safe to evict (for this group).
    ///
    /// # Errors
    ///
    /// Returns error if group not found or storage operation fails.
    pub async fn get_low_watermark(
        &self,
        group_id: ConsumerGroupId,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> ProgressResult<Option<Offset>> {
        let group = self
            .store
            .get_group(group_id)
            .await?
            .ok_or(ProgressError::GroupNotFound { group_id })?;

        let partition_key = PartitionKey::new(topic_id, partition_id);
        Ok(group
            .partitions
            .get(&partition_key)
            .map(|p| p.low_watermark))
    }

    /// Gets the minimum low watermark across ALL consumer groups for a partition.
    ///
    /// This is the safe eviction boundary - data below this can be evicted from
    /// local storage as all consumers have processed it.
    ///
    /// # Errors
    ///
    /// Returns error if storage operation fails.
    pub async fn get_safe_eviction_offset(
        &self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> ProgressResult<Option<Offset>> {
        let partition_key = PartitionKey::new(topic_id, partition_id);
        self.store.get_min_low_watermark(partition_key).await
    }

    // -------------------------------------------------------------------------
    // Maintenance Operations
    // -------------------------------------------------------------------------

    /// Expires leases for all partitions in a group.
    ///
    /// Returns the number of leases expired.
    ///
    /// # Errors
    ///
    /// Returns error if group not found or storage operation fails.
    pub async fn expire_leases(
        &self,
        group_id: ConsumerGroupId,
        current_time_us: u64,
    ) -> ProgressResult<u32> {
        let mut group = self
            .store
            .get_group(group_id)
            .await?
            .ok_or(ProgressError::GroupNotFound { group_id })?;

        let mut total_expired = 0u32;
        for progress in group.partitions.values_mut() {
            total_expired += progress.expire_leases(current_time_us);
        }

        if total_expired > 0 {
            // Update consumer lease lists.
            for consumer in group.consumers.values_mut() {
                consumer.lease_ids.retain(|lease_id| {
                    group
                        .partitions
                        .values()
                        .any(|p| p.active_leases.contains_key(lease_id))
                });
            }

            self.store.save_group(&group).await?;
            info!(group_id = %group_id, expired = total_expired, "Expired leases");
        }

        Ok(total_expired)
    }

    /// Expires leases for all groups.
    ///
    /// Returns the total number of leases expired.
    ///
    /// # Errors
    ///
    /// Returns error if storage operation fails.
    pub async fn expire_all_leases(&self, current_time_us: u64) -> ProgressResult<u32> {
        let groups = self.store.list_groups().await?;
        let mut total = 0u32;
        for group_id in groups {
            total += self.expire_leases(group_id, current_time_us).await?;
        }
        Ok(total)
    }

    // -------------------------------------------------------------------------
    // Kafka-Compatible Operations
    // -------------------------------------------------------------------------
    //
    // These methods support Kafka protocol semantics where:
    // - Groups are auto-created on first commit
    // - Commits don't require leases (cumulative semantics)
    // - Fetch returns None for non-existent partitions (not error)

    /// Commits an offset using Kafka-style cumulative semantics.
    ///
    /// Unlike the standard `commit_offset`, this method:
    /// - Auto-creates the consumer group if it doesn't exist
    /// - Auto-creates the partition progress if it doesn't exist
    /// - Does NOT require a lease (direct commit)
    /// - Uses cumulative mode (watermark advances directly)
    ///
    /// This is designed for Kafka protocol compatibility where clients
    /// don't use the lease mechanism.
    ///
    /// # Errors
    ///
    /// Returns error if storage operation fails.
    pub async fn commit_offset_kafka(
        &self,
        group_id: ConsumerGroupId,
        topic_id: TopicId,
        partition_id: PartitionId,
        offset: Offset,
        current_time_us: u64,
    ) -> ProgressResult<()> {
        // Get or create group with cumulative mode.
        let mut group = if let Some(g) = self.store.get_group(group_id).await? {
            g
        } else {
            let g = ConsumerGroupState::new(group_id, current_time_us, AckMode::Cumulative);
            self.store.save_group(&g).await?;
            debug!(group_id = %group_id, "Created consumer group for Kafka commit");
            g
        };

        let partition_key = PartitionKey::new(topic_id, partition_id);

        // Get or create partition progress.
        let progress = group
            .partitions
            .entry(partition_key)
            .or_insert_with(|| PartitionProgress::new(partition_key, Offset::new(0), AckMode::Cumulative));

        // Cumulative commit: advance watermark directly to offset + 1.
        // This means "I've processed everything up to and including this offset".
        let new_watermark = Offset::new(offset.get().saturating_add(1));
        if new_watermark > progress.low_watermark {
            progress.low_watermark = new_watermark;
            progress.next_lease_offset = progress.next_lease_offset.max(new_watermark);
        }

        // Save updated group.
        self.store.save_group(&group).await?;

        debug!(
            group_id = %group_id,
            topic_id = %topic_id,
            partition_id = %partition_id,
            offset = %offset,
            new_watermark = %new_watermark,
            "Kafka-style offset commit"
        );

        Ok(())
    }

    /// Fetches the committed offset using Kafka-style semantics.
    ///
    /// Unlike the standard `fetch_committed`, this method:
    /// - Returns `None` if the group doesn't exist (not an error)
    /// - Returns `None` if the partition doesn't exist (not an error)
    ///
    /// The returned offset is the last committed offset (watermark - 1).
    /// Returns `None` if no offset has been committed.
    ///
    /// # Errors
    ///
    /// Returns error only if storage operation fails.
    pub async fn fetch_committed_kafka(
        &self,
        group_id: ConsumerGroupId,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> ProgressResult<Option<Offset>> {
        let Some(group) = self.store.get_group(group_id).await? else {
            return Ok(None);
        };

        let partition_key = PartitionKey::new(topic_id, partition_id);
        let Some(progress) = group.partitions.get(&partition_key) else {
            return Ok(None);
        };

        // Watermark is "next offset to be committed", so committed = watermark - 1.
        // If watermark is 0, nothing has been committed.
        if progress.low_watermark.get() == 0 {
            Ok(None)
        } else {
            Ok(Some(Offset::new(progress.low_watermark.get() - 1)))
        }
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SimulatedProgressStore;

    fn create_manager(seed: u64) -> ProgressManager<SimulatedProgressStore> {
        let store = SimulatedProgressStore::new(seed);
        ProgressManager::new(store, ProgressConfig::for_testing())
    }

    #[tokio::test]
    async fn test_group_lifecycle() {
        let manager = create_manager(42);

        // Create group.
        let group = manager
            .get_or_create_group(ConsumerGroupId::new(1), 1000)
            .await
            .unwrap();
        assert_eq!(group.group_id, ConsumerGroupId::new(1));

        // Get existing group.
        let group = manager
            .get_or_create_group(ConsumerGroupId::new(1), 2000)
            .await
            .unwrap();
        assert_eq!(group.created_at_us, 1000); // Original creation time.
    }

    #[tokio::test]
    async fn test_consumer_registration() {
        let manager = create_manager(42);

        // Register consumer.
        manager
            .register_consumer(ConsumerGroupId::new(1), ConsumerId::new(10), 1000)
            .await
            .unwrap();

        // Verify consumer exists.
        let group = manager
            .get_or_create_group(ConsumerGroupId::new(1), 1000)
            .await
            .unwrap();
        assert!(group.consumers.contains_key(&ConsumerId::new(10)));

        // Unregister consumer.
        manager
            .unregister_consumer(ConsumerGroupId::new(1), ConsumerId::new(10))
            .await
            .unwrap();

        let group = manager
            .store()
            .get_group(ConsumerGroupId::new(1))
            .await
            .unwrap()
            .unwrap();
        assert!(!group.consumers.contains_key(&ConsumerId::new(10)));
    }

    #[tokio::test]
    async fn test_lease_creation() {
        let manager = create_manager(42);

        // Register consumer.
        manager
            .register_consumer(ConsumerGroupId::new(1), ConsumerId::new(10), 1000)
            .await
            .unwrap();

        // Create lease.
        let lease = manager
            .lease_offsets(
                ConsumerGroupId::new(1),
                TopicId::new(1),
                PartitionId::new(0),
                ConsumerId::new(10),
                Offset::new(0),
                10,
                10_000_000, // 10 seconds.
                1000,
            )
            .await
            .unwrap();

        assert!(lease.is_some());
        let lease = lease.unwrap();
        assert_eq!(lease.from_offset.get(), 0);
        assert_eq!(lease.to_offset.get(), 9);
        assert_eq!(lease.offset_count(), 10);
    }

    #[tokio::test]
    async fn test_lease_extend() {
        let manager = create_manager(42);

        // Setup.
        manager
            .register_consumer(ConsumerGroupId::new(1), ConsumerId::new(10), 1000)
            .await
            .unwrap();

        let lease = manager
            .lease_offsets(
                ConsumerGroupId::new(1),
                TopicId::new(1),
                PartitionId::new(0),
                ConsumerId::new(10),
                Offset::new(0),
                10,
                5_000_000, // 5 seconds.
                1000,
            )
            .await
            .unwrap()
            .unwrap();

        let original_expires = lease.expires_at_us;

        // Extend lease.
        let key = PartitionKey::new(TopicId::new(1), PartitionId::new(0));
        let extended = manager
            .extend_lease(
                ConsumerGroupId::new(1),
                key,
                ConsumerId::new(10),
                lease.lease_id,
                5_000_000, // Additional 5 seconds.
                3000,
            )
            .await
            .unwrap();

        assert!(extended.expires_at_us > original_expires);
    }

    #[tokio::test]
    async fn test_commit_offset_cumulative() {
        let manager = create_manager(42);

        // Setup with cumulative mode (default).
        manager
            .register_consumer(ConsumerGroupId::new(1), ConsumerId::new(10), 1000)
            .await
            .unwrap();

        let lease = manager
            .lease_offsets(
                ConsumerGroupId::new(1),
                TopicId::new(1),
                PartitionId::new(0),
                ConsumerId::new(10),
                Offset::new(0),
                10,
                10_000_000,
                1000,
            )
            .await
            .unwrap()
            .unwrap();

        assert!(lease.contains_offset(Offset::new(5)));

        // Commit offset 5 (cumulative = "processed up to 5").
        manager
            .commit_offset(
                ConsumerGroupId::new(1),
                TopicId::new(1),
                PartitionId::new(0),
                ConsumerId::new(10),
                Offset::new(5),
                2000,
            )
            .await
            .unwrap();

        // Watermark should be at 6.
        let watermark = manager
            .get_low_watermark(
                ConsumerGroupId::new(1),
                TopicId::new(1),
                PartitionId::new(0),
            )
            .await
            .unwrap();
        assert_eq!(watermark, Some(Offset::new(6)));
    }

    #[tokio::test]
    async fn test_commit_requires_lease() {
        let manager = create_manager(42);

        // Setup.
        manager
            .register_consumer(ConsumerGroupId::new(1), ConsumerId::new(10), 1000)
            .await
            .unwrap();

        // Lease offsets 0-9.
        manager
            .lease_offsets(
                ConsumerGroupId::new(1),
                TopicId::new(1),
                PartitionId::new(0),
                ConsumerId::new(10),
                Offset::new(0),
                10,
                10_000_000,
                1000,
            )
            .await
            .unwrap();

        // Try to commit offset 20 (not leased) - should fail.
        let result = manager
            .commit_offset(
                ConsumerGroupId::new(1),
                TopicId::new(1),
                PartitionId::new(0),
                ConsumerId::new(10),
                Offset::new(20),
                2000,
            )
            .await;

        assert!(matches!(result, Err(ProgressError::OffsetNotLeased { .. })));
    }

    #[tokio::test]
    async fn test_lease_expiration() {
        let manager = create_manager(42);

        // Setup.
        manager
            .register_consumer(ConsumerGroupId::new(1), ConsumerId::new(10), 1000)
            .await
            .unwrap();

        // Create lease that expires at 11000.
        manager
            .lease_offsets(
                ConsumerGroupId::new(1),
                TopicId::new(1),
                PartitionId::new(0),
                ConsumerId::new(10),
                Offset::new(0),
                10,
                10_000, // 10ms.
                1000,
            )
            .await
            .unwrap();

        // Expire at time 20000.
        let expired = manager
            .expire_leases(ConsumerGroupId::new(1), 20_000)
            .await
            .unwrap();
        assert_eq!(expired, 1);

        // Lease should be gone.
        let group = manager
            .store()
            .get_group(ConsumerGroupId::new(1))
            .await
            .unwrap()
            .unwrap();
        let key = PartitionKey::new(TopicId::new(1), PartitionId::new(0));
        assert!(group.partitions[&key].active_leases.is_empty());
    }

    #[tokio::test]
    async fn test_safe_eviction_offset() {
        let manager = create_manager(42);
        let topic_id = TopicId::new(1);
        let partition_id = PartitionId::new(0);

        // No groups - no safe offset.
        let safe = manager
            .get_safe_eviction_offset(topic_id, partition_id)
            .await
            .unwrap();
        assert!(safe.is_none());

        // Create group 1 with watermark at 10.
        manager
            .register_consumer(ConsumerGroupId::new(1), ConsumerId::new(10), 1000)
            .await
            .unwrap();
        manager
            .lease_offsets(
                ConsumerGroupId::new(1),
                topic_id,
                partition_id,
                ConsumerId::new(10),
                Offset::new(0),
                20,
                10_000_000,
                1000,
            )
            .await
            .unwrap();
        manager
            .commit_offset(
                ConsumerGroupId::new(1),
                topic_id,
                partition_id,
                ConsumerId::new(10),
                Offset::new(9), // Cumulative: watermark = 10.
                2000,
            )
            .await
            .unwrap();

        let safe = manager
            .get_safe_eviction_offset(topic_id, partition_id)
            .await
            .unwrap();
        assert_eq!(safe, Some(Offset::new(10)));

        // Create group 2 with watermark at 5 (lower).
        manager
            .register_consumer(ConsumerGroupId::new(2), ConsumerId::new(20), 1000)
            .await
            .unwrap();
        manager
            .lease_offsets(
                ConsumerGroupId::new(2),
                topic_id,
                partition_id,
                ConsumerId::new(20),
                Offset::new(0),
                20,
                10_000_000,
                1000,
            )
            .await
            .unwrap();
        manager
            .commit_offset(
                ConsumerGroupId::new(2),
                topic_id,
                partition_id,
                ConsumerId::new(20),
                Offset::new(4), // Cumulative: watermark = 5.
                2000,
            )
            .await
            .unwrap();

        // Safe offset is the minimum = 5.
        let safe = manager
            .get_safe_eviction_offset(topic_id, partition_id)
            .await
            .unwrap();
        assert_eq!(safe, Some(Offset::new(5)));
    }

    #[tokio::test]
    async fn test_individual_mode_sparse_commits() {
        let store = SimulatedProgressStore::new(42);
        let config = ProgressConfig {
            default_ack_mode: AckMode::Individual,
            ..ProgressConfig::for_testing()
        };
        let manager = ProgressManager::new(store, config);

        let topic_id = TopicId::new(1);
        let partition_id = PartitionId::new(0);

        // Setup.
        manager
            .register_consumer(ConsumerGroupId::new(1), ConsumerId::new(10), 1000)
            .await
            .unwrap();
        manager
            .lease_offsets(
                ConsumerGroupId::new(1),
                topic_id,
                partition_id,
                ConsumerId::new(10),
                Offset::new(0),
                10,
                10_000_000,
                1000,
            )
            .await
            .unwrap();

        // Commit 0, 2, 4 (sparse).
        manager
            .commit_offset(
                ConsumerGroupId::new(1),
                topic_id,
                partition_id,
                ConsumerId::new(10),
                Offset::new(0),
                2000,
            )
            .await
            .unwrap();

        let wm = manager
            .get_low_watermark(ConsumerGroupId::new(1), topic_id, partition_id)
            .await
            .unwrap();
        assert_eq!(wm, Some(Offset::new(1))); // Advanced past 0.

        manager
            .commit_offset(
                ConsumerGroupId::new(1),
                topic_id,
                partition_id,
                ConsumerId::new(10),
                Offset::new(2),
                2000,
            )
            .await
            .unwrap();

        let wm = manager
            .get_low_watermark(ConsumerGroupId::new(1), topic_id, partition_id)
            .await
            .unwrap();
        assert_eq!(wm, Some(Offset::new(1))); // Can't advance past gap at 1.

        // Fill the gap.
        manager
            .commit_offset(
                ConsumerGroupId::new(1),
                topic_id,
                partition_id,
                ConsumerId::new(10),
                Offset::new(1),
                2000,
            )
            .await
            .unwrap();

        let wm = manager
            .get_low_watermark(ConsumerGroupId::new(1), topic_id, partition_id)
            .await
            .unwrap();
        assert_eq!(wm, Some(Offset::new(3))); // Now advances past 0, 1, 2.
    }
}
