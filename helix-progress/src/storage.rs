//! Progress storage abstraction.
//!
//! Provides the `ProgressStore` trait for persisting consumer progress,
//! and `SimulatedProgressStore` for deterministic simulation testing.

#![allow(clippy::significant_drop_tightening)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use helix_core::{ConsumerGroupId, Offset};

use crate::error::{ProgressError, ProgressResult};
use crate::types::{ConsumerGroupState, Lease, PartitionKey, PartitionProgress};

// -----------------------------------------------------------------------------
// Progress Store Trait
// -----------------------------------------------------------------------------

/// Progress store trait for persistence.
///
/// Implementations provide durable storage for consumer progress.
/// Progress is stored in the same Raft group as partition data.
#[async_trait]
pub trait ProgressStore: Send + Sync {
    /// Gets consumer group state.
    ///
    /// Returns `None` if the group does not exist.
    async fn get_group(
        &self,
        group_id: ConsumerGroupId,
    ) -> ProgressResult<Option<ConsumerGroupState>>;

    /// Saves consumer group state.
    ///
    /// Creates the group if it doesn't exist, or updates if it does.
    async fn save_group(&self, state: &ConsumerGroupState) -> ProgressResult<()>;

    /// Removes consumer group.
    ///
    /// No-op if the group doesn't exist.
    async fn remove_group(&self, group_id: ConsumerGroupId) -> ProgressResult<()>;

    /// Gets partition progress for a group.
    ///
    /// Returns `None` if the group or partition doesn't exist.
    async fn get_partition_progress(
        &self,
        group_id: ConsumerGroupId,
        partition_key: PartitionKey,
    ) -> ProgressResult<Option<PartitionProgress>>;

    /// Saves partition progress atomically.
    ///
    /// # Errors
    ///
    /// Returns `GroupNotFound` if the group doesn't exist.
    async fn save_partition_progress(
        &self,
        group_id: ConsumerGroupId,
        progress: &PartitionProgress,
    ) -> ProgressResult<()>;

    /// Gets all active leases for a partition across all groups.
    async fn get_active_leases(&self, partition_key: PartitionKey) -> ProgressResult<Vec<Lease>>;

    /// Gets the minimum `low_watermark` across ALL consumer groups for a partition.
    ///
    /// Returns `None` if no groups are tracking this partition.
    /// This is the safe eviction boundary.
    async fn get_min_low_watermark(
        &self,
        partition_key: PartitionKey,
    ) -> ProgressResult<Option<Offset>>;

    /// Lists all consumer groups.
    async fn list_groups(&self) -> ProgressResult<Vec<ConsumerGroupId>>;

    /// Syncs to durable storage.
    async fn sync(&self) -> ProgressResult<()>;
}

// -----------------------------------------------------------------------------
// Fault Configuration
// -----------------------------------------------------------------------------

/// Fault configuration for simulated progress store.
#[derive(Debug, Clone, Default)]
pub struct ProgressStoreFaultConfig {
    /// Probability of get operations failing (0.0 - 1.0).
    pub get_fail_rate: f64,
    /// Probability of save operations failing (0.0 - 1.0).
    pub save_fail_rate: f64,
    /// Probability of remove operations failing (0.0 - 1.0).
    pub remove_fail_rate: f64,
    /// Force next get to fail (one-shot).
    pub force_get_fail: bool,
    /// Force next save to fail (one-shot).
    pub force_save_fail: bool,
}

impl ProgressStoreFaultConfig {
    /// No faults (all operations succeed).
    #[must_use]
    pub const fn none() -> Self {
        Self {
            get_fail_rate: 0.0,
            save_fail_rate: 0.0,
            remove_fail_rate: 0.0,
            force_get_fail: false,
            force_save_fail: false,
        }
    }

    /// Flaky configuration for stress testing.
    #[must_use]
    pub const fn flaky() -> Self {
        Self {
            get_fail_rate: 0.05,
            save_fail_rate: 0.05,
            remove_fail_rate: 0.02,
            force_get_fail: false,
            force_save_fail: false,
        }
    }

    /// Builder: set get fail rate.
    #[must_use]
    pub const fn with_get_fail_rate(mut self, rate: f64) -> Self {
        self.get_fail_rate = rate;
        self
    }

    /// Builder: set save fail rate.
    #[must_use]
    pub const fn with_save_fail_rate(mut self, rate: f64) -> Self {
        self.save_fail_rate = rate;
        self
    }

    /// Builder: set remove fail rate.
    #[must_use]
    pub const fn with_remove_fail_rate(mut self, rate: f64) -> Self {
        self.remove_fail_rate = rate;
        self
    }
}

// -----------------------------------------------------------------------------
// Simulated Progress Store
// -----------------------------------------------------------------------------

/// In-memory simulated progress store for DST.
///
/// Clones share state via `Arc` for multi-handle testing.
/// Supports deterministic fault injection for testing error paths.
#[derive(Debug, Clone)]
pub struct SimulatedProgressStore {
    /// Consumer group states.
    groups: Arc<Mutex<HashMap<ConsumerGroupId, ConsumerGroupState>>>,
    /// Fault configuration.
    fault_config: Arc<Mutex<ProgressStoreFaultConfig>>,
    /// RNG seed for deterministic faults.
    seed: u64,
    /// Operation counter for deterministic RNG.
    counter: Arc<AtomicU64>,
}

impl SimulatedProgressStore {
    /// Creates a new simulated store with no faults.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            groups: Arc::new(Mutex::new(HashMap::new())),
            fault_config: Arc::new(Mutex::new(ProgressStoreFaultConfig::none())),
            seed,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Creates a simulated store with fault injection.
    #[must_use]
    pub fn with_faults(seed: u64, config: ProgressStoreFaultConfig) -> Self {
        Self {
            groups: Arc::new(Mutex::new(HashMap::new())),
            fault_config: Arc::new(Mutex::new(config)),
            seed,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns fault config for modification.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    pub fn fault_config(&self) -> std::sync::MutexGuard<'_, ProgressStoreFaultConfig> {
        self.fault_config.lock().expect("fault config lock poisoned")
    }

    /// Deterministic RNG following helix-tier pattern.
    ///
    /// Uses `(seed + counter) * M` formula for reproducible fault injection.
    fn should_inject_fault(&self, rate: f64) -> bool {
        if rate <= 0.0 {
            return false;
        }
        if rate >= 1.0 {
            return true;
        }
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        let hash = self
            .seed
            .wrapping_add(counter)
            .wrapping_mul(0x9e37_79b9_7f4a_7c15);
        #[allow(clippy::cast_precision_loss)]
        let normalized = (hash as f64) / (u64::MAX as f64);
        normalized < rate
    }

    /// Returns all groups (bypasses faults, for invariant checking in tests).
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    #[must_use]
    pub fn all_groups(&self) -> Vec<(ConsumerGroupId, ConsumerGroupState)> {
        let groups = self.groups.lock().expect("groups lock poisoned");
        groups.iter().map(|(k, v)| (*k, v.clone())).collect()
    }

    /// Returns group count (bypasses faults, for assertions in tests).
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    #[must_use]
    pub fn group_count(&self) -> usize {
        let groups = self.groups.lock().expect("groups lock poisoned");
        groups.len()
    }
}

#[async_trait]
impl ProgressStore for SimulatedProgressStore {
    async fn get_group(
        &self,
        group_id: ConsumerGroupId,
    ) -> ProgressResult<Option<ConsumerGroupState>> {
        // Check for forced failure.
        {
            let mut config = self.fault_config.lock().expect("lock poisoned");
            if config.force_get_fail {
                config.force_get_fail = false;
                return Err(ProgressError::Io {
                    operation: "get_group",
                    message: "simulated failure (forced)".into(),
                });
            }
        }

        // Check for probabilistic failure.
        let get_fail_rate = self.fault_config.lock().expect("lock").get_fail_rate;
        if self.should_inject_fault(get_fail_rate) {
            return Err(ProgressError::Io {
                operation: "get_group",
                message: "simulated failure (random)".into(),
            });
        }

        let groups = self.groups.lock().expect("groups lock poisoned");
        Ok(groups.get(&group_id).cloned())
    }

    async fn save_group(&self, state: &ConsumerGroupState) -> ProgressResult<()> {
        // TigerStyle: Assert precondition.
        assert!(
            state.group_id.get() != 0,
            "group_id must be non-zero"
        );

        // Check for forced failure.
        {
            let mut config = self.fault_config.lock().expect("lock poisoned");
            if config.force_save_fail {
                config.force_save_fail = false;
                return Err(ProgressError::Io {
                    operation: "save_group",
                    message: "simulated failure (forced)".into(),
                });
            }
        }

        // Check for probabilistic failure.
        let save_fail_rate = self.fault_config.lock().expect("lock").save_fail_rate;
        if self.should_inject_fault(save_fail_rate) {
            return Err(ProgressError::Io {
                operation: "save_group",
                message: "simulated failure (random)".into(),
            });
        }

        let mut groups = self.groups.lock().expect("groups lock poisoned");
        groups.insert(state.group_id, state.clone());

        // TigerStyle: Assert postcondition.
        assert!(groups.contains_key(&state.group_id));
        Ok(())
    }

    async fn remove_group(&self, group_id: ConsumerGroupId) -> ProgressResult<()> {
        // Check for probabilistic failure.
        let remove_fail_rate = self.fault_config.lock().expect("lock").remove_fail_rate;
        if self.should_inject_fault(remove_fail_rate) {
            return Err(ProgressError::Io {
                operation: "remove_group",
                message: "simulated failure (random)".into(),
            });
        }

        let mut groups = self.groups.lock().expect("groups lock poisoned");
        groups.remove(&group_id);
        Ok(())
    }

    async fn get_partition_progress(
        &self,
        group_id: ConsumerGroupId,
        partition_key: PartitionKey,
    ) -> ProgressResult<Option<PartitionProgress>> {
        // Check for probabilistic failure.
        let get_fail_rate = self.fault_config.lock().expect("lock").get_fail_rate;
        if self.should_inject_fault(get_fail_rate) {
            return Err(ProgressError::Io {
                operation: "get_partition_progress",
                message: "simulated failure (random)".into(),
            });
        }

        let groups = self.groups.lock().expect("groups lock poisoned");
        Ok(groups
            .get(&group_id)
            .and_then(|g| g.partitions.get(&partition_key).cloned()))
    }

    async fn save_partition_progress(
        &self,
        group_id: ConsumerGroupId,
        progress: &PartitionProgress,
    ) -> ProgressResult<()> {
        // Check for probabilistic failure.
        let save_fail_rate = self.fault_config.lock().expect("lock").save_fail_rate;
        if self.should_inject_fault(save_fail_rate) {
            return Err(ProgressError::Io {
                operation: "save_partition_progress",
                message: "simulated failure (random)".into(),
            });
        }

        let mut groups = self.groups.lock().expect("groups lock poisoned");
        let group = groups
            .get_mut(&group_id)
            .ok_or(ProgressError::GroupNotFound { group_id })?;
        group
            .partitions
            .insert(progress.partition_key, progress.clone());
        Ok(())
    }

    async fn get_active_leases(&self, partition_key: PartitionKey) -> ProgressResult<Vec<Lease>> {
        let groups = self.groups.lock().expect("groups lock poisoned");
        let mut all_leases = Vec::new();
        for group in groups.values() {
            if let Some(progress) = group.partitions.get(&partition_key) {
                for lease in progress.active_leases.values() {
                    all_leases.push(lease.clone());
                }
            }
        }
        Ok(all_leases)
    }

    async fn get_min_low_watermark(
        &self,
        partition_key: PartitionKey,
    ) -> ProgressResult<Option<Offset>> {
        let groups = self.groups.lock().expect("groups lock poisoned");
        let mut min_watermark: Option<Offset> = None;
        for group in groups.values() {
            if let Some(progress) = group.partitions.get(&partition_key) {
                match min_watermark {
                    None => min_watermark = Some(progress.low_watermark),
                    Some(current) if progress.low_watermark.get() < current.get() => {
                        min_watermark = Some(progress.low_watermark);
                    }
                    _ => {}
                }
            }
        }
        Ok(min_watermark)
    }

    async fn list_groups(&self) -> ProgressResult<Vec<ConsumerGroupId>> {
        let groups = self.groups.lock().expect("groups lock poisoned");
        Ok(groups.keys().copied().collect())
    }

    async fn sync(&self) -> ProgressResult<()> {
        // No-op for in-memory store.
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AckMode;
    use helix_core::{ConsumerId, LeaseId, PartitionId, TopicId};

    #[tokio::test]
    async fn test_basic_group_operations() {
        let store = SimulatedProgressStore::new(42);

        // Group doesn't exist initially.
        let result = store.get_group(ConsumerGroupId::new(1)).await.unwrap();
        assert!(result.is_none());

        // Create and save a group.
        let group = ConsumerGroupState::new(ConsumerGroupId::new(1), 1000, AckMode::Cumulative);
        store.save_group(&group).await.unwrap();

        // Group exists now.
        let result = store.get_group(ConsumerGroupId::new(1)).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().group_id, ConsumerGroupId::new(1));

        // Remove the group.
        store.remove_group(ConsumerGroupId::new(1)).await.unwrap();
        let result = store.get_group(ConsumerGroupId::new(1)).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_partition_progress() {
        let store = SimulatedProgressStore::new(42);

        // Create a group.
        let mut group = ConsumerGroupState::new(ConsumerGroupId::new(1), 1000, AckMode::Cumulative);
        let key = PartitionKey::new(TopicId::new(1), PartitionId::new(0));
        group.get_or_create_partition(key, Offset::new(0));
        store.save_group(&group).await.unwrap();

        // Get partition progress.
        let progress = store
            .get_partition_progress(ConsumerGroupId::new(1), key)
            .await
            .unwrap();
        assert!(progress.is_some());
        assert_eq!(progress.unwrap().low_watermark.get(), 0);
    }

    #[tokio::test]
    async fn test_min_low_watermark() {
        let store = SimulatedProgressStore::new(42);
        let key = PartitionKey::new(TopicId::new(1), PartitionId::new(0));

        // No groups - no watermark.
        let min = store.get_min_low_watermark(key).await.unwrap();
        assert!(min.is_none());

        // Add group 1 with watermark at 10.
        let mut group1 =
            ConsumerGroupState::new(ConsumerGroupId::new(1), 1000, AckMode::Cumulative);
        let progress1 = group1.get_or_create_partition(key, Offset::new(10));
        progress1.low_watermark = Offset::new(10);
        store.save_group(&group1).await.unwrap();

        let min = store.get_min_low_watermark(key).await.unwrap();
        assert_eq!(min, Some(Offset::new(10)));

        // Add group 2 with watermark at 5 (lower).
        let mut group2 =
            ConsumerGroupState::new(ConsumerGroupId::new(2), 1000, AckMode::Cumulative);
        let progress2 = group2.get_or_create_partition(key, Offset::new(5));
        progress2.low_watermark = Offset::new(5);
        store.save_group(&group2).await.unwrap();

        let min = store.get_min_low_watermark(key).await.unwrap();
        assert_eq!(min, Some(Offset::new(5)));
    }

    #[tokio::test]
    async fn test_active_leases() {
        let store = SimulatedProgressStore::new(42);
        let key = PartitionKey::new(TopicId::new(1), PartitionId::new(0));

        // Add group with a lease.
        let mut group = ConsumerGroupState::new(ConsumerGroupId::new(1), 1000, AckMode::Cumulative);
        let progress = group.get_or_create_partition(key, Offset::new(0));

        let lease = crate::types::Lease::new(
            LeaseId::new(1),
            ConsumerId::new(10),
            ConsumerGroupId::new(1),
            key,
            Offset::new(0),
            Offset::new(9),
            1000,
            5000,
        );
        progress.active_leases.insert(lease.lease_id, lease);

        store.save_group(&group).await.unwrap();

        // Get active leases.
        let leases = store.get_active_leases(key).await.unwrap();
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].lease_id, LeaseId::new(1));
    }

    #[tokio::test]
    async fn test_forced_failure() {
        let store = SimulatedProgressStore::new(42);

        // Force get to fail.
        store.fault_config().force_get_fail = true;
        let result = store.get_group(ConsumerGroupId::new(1)).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ProgressError::Io { .. }));

        // Next get should succeed (one-shot).
        let result = store.get_group(ConsumerGroupId::new(1)).await;
        assert!(result.is_ok());

        // Force save to fail.
        store.fault_config().force_save_fail = true;
        let group = ConsumerGroupState::new(ConsumerGroupId::new(1), 1000, AckMode::Cumulative);
        let result = store.save_group(&group).await;
        assert!(result.is_err());

        // Next save should succeed.
        let result = store.save_group(&group).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_probabilistic_failure() {
        // 100% fail rate should always fail.
        let store = SimulatedProgressStore::with_faults(
            42,
            ProgressStoreFaultConfig::none().with_get_fail_rate(1.0),
        );

        for _ in 0..10 {
            let result = store.get_group(ConsumerGroupId::new(1)).await;
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn test_clones_share_state() {
        let store1 = SimulatedProgressStore::new(42);
        let store2 = store1.clone();

        // Save via store1.
        let group = ConsumerGroupState::new(ConsumerGroupId::new(1), 1000, AckMode::Cumulative);
        store1.save_group(&group).await.unwrap();

        // Read via store2.
        let result = store2.get_group(ConsumerGroupId::new(1)).await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_list_groups() {
        let store = SimulatedProgressStore::new(42);

        // Empty initially.
        let groups = store.list_groups().await.unwrap();
        assert!(groups.is_empty());

        // Add some groups.
        store
            .save_group(&ConsumerGroupState::new(
                ConsumerGroupId::new(1),
                1000,
                AckMode::Cumulative,
            ))
            .await
            .unwrap();
        store
            .save_group(&ConsumerGroupState::new(
                ConsumerGroupId::new(2),
                1000,
                AckMode::Individual,
            ))
            .await
            .unwrap();

        let groups = store.list_groups().await.unwrap();
        assert_eq!(groups.len(), 2);
    }
}
