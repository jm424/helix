//! Utility methods for the Helix service.

use helix_core::{NodeId, PartitionId};

use super::super::HelixService;

impl HelixService {
    /// Hashes a string to produce a deterministic u64 ID.
    pub(crate) fn hash_string(s: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    /// Returns the current time in microseconds.
    pub(crate) fn current_time_us() -> u64 {
        // Safe cast: u64 microseconds can represent ~584,942 years from UNIX epoch.
        // We won't overflow for any reasonable time value.
        #[allow(clippy::cast_possible_truncation)]
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as u64)
    }

    // =========================================================================
    // Cluster Info Methods (for Kafka protocol support)
    // =========================================================================

    /// Returns this node's ID.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Gets the leader for a topic/partition.
    ///
    /// Returns `None` if the topic or partition doesn't exist, or leader is unknown.
    pub async fn get_leader(&self, topic: &str, partition: i32) -> Option<NodeId> {
        // Get topic metadata.
        let topic_meta = self.get_topic(topic).await?;

        // Validate partition index.
        if partition < 0 || partition >= topic_meta.partition_count {
            return None;
        }

        // Safe cast: partition is validated to be non-negative.
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(partition as u64);

        // Get group ID.
        let group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id)?
        };

        // Get leader from MultiRaft.
        let mr = self.multi_raft.read().await;
        mr.group_state(group_id).and_then(|s| s.leader_id)
    }

    /// Checks if this node is the leader for a topic/partition.
    pub async fn is_leader(&self, topic: &str, partition: i32) -> bool {
        self.get_leader(topic, partition)
            .await
            .is_some_and(|leader| leader == self.node_id)
    }

    /// Gets partition info for a topic/partition.
    ///
    /// Returns (`log_start_offset`, `log_end_offset`, `high_watermark`) or `None` if not found.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn get_partition_offsets(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<(u64, u64, u64)> {
        // Get topic metadata.
        let topic_meta = self.get_topic(topic).await?;

        // Validate partition index.
        if partition < 0 || partition >= topic_meta.partition_count {
            return None;
        }

        // Safe cast: partition is validated to be non-negative.
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(partition as u64);

        // Get group ID.
        let group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id)?
        };

        // Get offsets from storage.
        let storage = self.partition_storage.read().await;
        let ps = storage.get(&group_id)?;

        Some((
            ps.log_start_offset().get(),
            ps.log_end_offset().get(),
            ps.high_watermark().get(),
        ))
    }
}
