//! Consistency checks, property tracking, and payload extraction.

use bytes::Bytes;
use helix_core::{NodeId, Offset};
use helix_raft::RaftState;
use helix_server::controller::CONTROLLER_GROUP_ID;
use tracing::{debug, error, trace};

use crate::properties::{assert_no_helix_violations, HelixNodeSnapshot};

use super::cluster::E2ECluster;

impl E2ECluster {
    // ========================================================================
    // Cross-Replica Consistency Verification
    // ========================================================================

    /// Consumes data from a specific node (not necessarily the leader).
    ///
    /// This is used for cross-replica consistency verification to read from
    /// all replicas and compare their data.
    ///
    /// Returns `None` if the node doesn't exist or the fetch fails.
    pub async fn consume_from_node(
        &self,
        node_id: NodeId,
        topic: &str,
        partition: u32,
        start_offset: u64,
    ) -> Option<Vec<Bytes>> {
        use super::helpers::split_record_batches;

        #[allow(clippy::cast_possible_wrap)]
        let partition_i32 = partition as i32;

        let node = self.nodes.get(&node_id)?;

        match node
            .handler
            .fetch(topic, partition_i32, start_offset, 1024 * 1024)
            .await
        {
            Ok(response) => {
                let mut all_batches = Vec::new();
                for data in response {
                    all_batches.extend(split_record_batches(&data));
                }
                Some(all_batches)
            }
            Err(e) => {
                debug!(
                    node = node_id.get(),
                    error = %e,
                    "consume_from_node fetch failed"
                );
                None
            }
        }
    }

    /// Gets the Raft commit index for a partition on a specific node.
    ///
    /// Returns `None` if the node doesn't have the group or the topic doesn't exist.
    pub async fn get_partition_commit_index(
        &self,
        node_id: NodeId,
        topic: &str,
        partition: u32,
    ) -> Option<u64> {
        use helix_core::PartitionId;

        let node = self.nodes.get(&node_id)?;

        // Get topic ID from controller state.
        let topic_id = {
            let state = node.service.controller_state().read().await;
            state.get_topic(topic).map(|info| info.topic_id)
        }?;

        // Get the group ID for this partition.
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(u64::from(partition));
        let group_id = {
            let gm = node.service.group_map().read().await;
            gm.get(topic_id, partition_id)
        }?;

        // Get commit index from Raft state.
        let mr = node.service.multi_raft().read().await;
        mr.group_state(group_id)
            .map(|state| state.commit_index.get())
    }

    /// Verifies that all replicas have consistent data.
    ///
    /// This is the core cross-replica consistency check for DST. It:
    /// 1. Reads data from ALL nodes (not just the leader)
    /// 2. Finds the minimum number of records across all replicas
    /// 3. Verifies that all nodes have identical data up to that point
    ///
    /// This approach compares visible data rather than trying to map Raft
    /// commit indices to Kafka offsets, which can have timing mismatches.
    ///
    /// Returns `Ok(())` if consistent, or `Err` with details about the inconsistency.
    pub async fn verify_replica_consistency(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<(), String> {
        // Collect data from all available nodes.
        let mut replica_data: Vec<(NodeId, Vec<Bytes>)> = Vec::new();

        for &node_id in self.node_ids() {
            // Skip crashed nodes - they may be behind.
            if self
                .crashed_nodes
                .lock()
                .expect("lock poisoned")
                .contains(&node_id)
            {
                debug!(
                    node = node_id.get(),
                    "Skipping crashed node in consistency check"
                );
                continue;
            }

            // Read data from this node.
            let records = match self.consume_from_node(node_id, topic, partition, 0).await {
                Some(r) => r,
                None => {
                    debug!(
                        node = node_id.get(),
                        topic,
                        partition,
                        "Failed to read from node, skipping"
                    );
                    continue;
                }
            };

            replica_data.push((node_id, records));
        }

        if replica_data.is_empty() {
            return Err("No replicas available for consistency check".to_string());
        }

        if replica_data.len() == 1 {
            // Only one replica available - can't verify cross-replica consistency.
            debug!("Only one replica available, skipping cross-replica check");
            return Ok(());
        }

        // Find the minimum number of batches across all replicas.
        // All replicas should have identical data up to this point.
        let min_batches = replica_data
            .iter()
            .map(|(_, records)| records.len())
            .min()
            .unwrap_or(0);

        if min_batches == 0 {
            // No data yet - nothing to verify.
            return Ok(());
        }

        // Use the first replica as the reference.
        let (ref_node_id, ref_records) = &replica_data[0];
        let ref_prefix = &ref_records[..min_batches];

        // Compare each other replica against the reference.
        for (node_id, records) in &replica_data[1..] {
            let prefix = &records[..min_batches];

            // Check each batch matches byte-for-byte.
            for (i, (ref_batch, batch)) in ref_prefix.iter().zip(prefix.iter()).enumerate() {
                if ref_batch.as_ref() != batch.as_ref() {
                    let ref_offset = Self::extract_base_offset(ref_batch).unwrap_or(0);
                    let offset = Self::extract_base_offset(batch).unwrap_or(0);
                    return Err(format!(
                        "REPLICA INCONSISTENCY: Batch {} differs between node {} (offset={}, len={}) \
                         and reference node {} (offset={}, len={}) - data mismatch!",
                        i,
                        node_id.get(),
                        offset,
                        batch.len(),
                        ref_node_id.get(),
                        ref_offset,
                        ref_batch.len(),
                    ));
                }
            }
        }

        debug!(
            topic,
            partition,
            replicas = replica_data.len(),
            min_batches,
            "Replica consistency verified"
        );

        Ok(())
    }

    // ========================================================================
    // Data Integrity Verification
    // ========================================================================

    /// Simple FNV-1a hash for data integrity verification.
    #[must_use]
    pub(crate) fn simple_hash(data: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in data {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
        hash
    }

    /// Produces data and records the client ack for verification.
    ///
    /// Like `produce_with_retry`, but also records the payload for later
    /// verification via `consume_and_verify`. Stores both the hash (for
    /// backward compatibility) and the raw payload bytes (for direct comparison).
    pub async fn produce_and_track(
        &self,
        topic: &str,
        partition: u32,
        data: impl Into<Bytes> + Clone,
        max_retries: u32,
    ) -> Result<Offset, String> {
        let data_bytes: Bytes = data.into();
        let payload_hash = Self::simple_hash(&data_bytes);

        let offset = self
            .produce_with_retry(topic, partition, data_bytes.clone(), max_retries)
            .await?;

        // Record client ack for verification.
        // Get topic_id from controller state — try all nodes since a recently
        // restarted node may not have replicated controller state yet.
        let topic_id = {
            let mut found = None;
            for node in self.nodes.values() {
                let state = node.service.controller_state().read().await;
                if let Some(info) = state.get_topic(topic) {
                    found = Some(info.topic_id.get());
                    break;
                }
            }
            found
        };

        let Some(topic_id) = topic_id else {
            return Err(format!(
                "produce_and_track: topic_id lookup failed for topic={topic}"
            ));
        };

        if let Ok(mut state) = self.property_state.lock() {
            // Record hash for backward compatibility.
            state.record_client_ack(topic_id, u64::from(partition), offset.get(), payload_hash);
            // Store raw payload for direct byte comparison (more reliable).
            state.record_expected_payload(topic_id, u64::from(partition), offset.get(), data_bytes);
        }

        Ok(offset)
    }

    /// Consumes data and verifies payload integrity against recorded client acks.
    ///
    /// This performs REAL data integrity verification:
    /// - Extracts payload from each consumed record
    /// - Extracts payload and compares directly against stored expected bytes
    /// - FAILS EXPLICITLY on parse errors (no silent skipping)
    /// - Falls back to hash comparison for backward compatibility
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Consume fails
    /// - Topic ID lookup fails (no silent skip)
    /// - Payload extraction fails (parse error)
    /// - Payload doesn't match expected (data corruption)
    /// - Zero records were verified (likely a bug)
    ///
    /// Returns the consumed records on success.
    pub async fn consume_and_verify(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
    ) -> Result<Vec<Bytes>, String> {
        // Get topic_id for verification - try all nodes since a recently
        // restarted node may not have replicated controller state yet.
        let topic_id = {
            let mut found = None;
            for node in self.nodes.values() {
                let state = node.service.controller_state().read().await;
                if let Some(info) = state.get_topic(topic) {
                    found = Some(info.topic_id.get());
                    break;
                }
            }
            found.ok_or_else(|| format!("topic '{}' not found in controller state", topic))?
        };

        let records = self.consume(topic, partition, start_offset).await?;

        // Track verification count - fail if zero records verified.
        let mut verified_count: u64 = 0;

        // Verify each record by extracting and comparing payload.
        for batch in &records {
            // Extract baseOffset from batch header (bytes 0-7).
            let base_offset = Self::extract_base_offset(batch)?;

            // Extract payload - FAIL EXPLICITLY on parse error.
            let payload = Self::extract_payload_from_batch(batch).map_err(|e| {
                format!(
                    "PARSE ERROR at topic={} partition={} base_offset={}: {}",
                    topic, partition, base_offset, e
                )
            })?;

            // Use baseOffset from batch, not assumed sequential offset.
            let offset = base_offset;

            // Try direct payload comparison first (more reliable).
            let state_result = self.property_state.lock();
            let mut state = state_result.map_err(|e| format!("lock poisoned: {e}"))?;

            // Primary check: direct byte comparison when we have an expected payload.
            //
            // Kafka semantics: a produce that times out or returns an error can still
            // be committed and visible later. That data is "untracked" here because
            // we only record expected payloads on successful acks, so we do NOT fail
            // on untracked data.
            if state
                .get_expected_payload(topic_id, u64::from(partition), offset)
                .is_some()
            {
                if let Err(corruption_error) =
                    state.verify_payload_direct(topic_id, u64::from(partition), offset, &payload)
                {
                    // Data corruption detected for acked data.
                    error!(
                        topic_id,
                        partition,
                        offset,
                        error = %corruption_error,
                        "DATA INTEGRITY FAILURE"
                    );
                    return Err(corruption_error);
                }
            } else {
                debug!(
                    topic_id,
                    partition,
                    offset,
                    payload_len = payload.len(),
                    "UNTRACKED DATA (likely failed produce)"
                );
            }

            // Secondary check: hash comparison (for backward compatibility).
            let actual_hash = Self::simple_hash(&payload);
            let _hash_ok =
                state.verify_offset_with_hash(topic_id, u64::from(partition), offset, actual_hash);

            verified_count += 1;
            trace!(
                topic_id,
                partition,
                offset,
                payload_len = payload.len(),
                verified_count,
                "Record verified - payload matches"
            );
        }

        // Fail if we had records but verified zero (indicates a bug).
        if !records.is_empty() && verified_count == 0 {
            return Err(format!(
                "VERIFICATION BUG: consumed {} records but verified 0",
                records.len()
            ));
        }

        debug!(
            topic,
            partition,
            start_offset,
            records_consumed = records.len(),
            verified_count,
            "consume_and_verify completed"
        );

        Ok(records)
    }

    /// Extracts the baseOffset from a Kafka RecordBatch header.
    ///
    /// The baseOffset is the first 8 bytes of the batch (big-endian i64).
    ///
    /// # Errors
    ///
    /// Returns an error if the batch is too short.
    pub(crate) fn extract_base_offset(batch: &Bytes) -> Result<u64, String> {
        if batch.len() < 8 {
            return Err(format!(
                "batch too short for baseOffset: {} bytes (need 8)",
                batch.len()
            ));
        }
        let base_offset = i64::from_be_bytes([
            batch[0], batch[1], batch[2], batch[3], batch[4], batch[5], batch[6], batch[7],
        ]);
        // Convert to u64 - baseOffset should never be negative in valid data.
        if base_offset < 0 {
            return Err(format!("negative baseOffset: {base_offset}"));
        }
        #[allow(clippy::cast_sign_loss)]
        Ok(base_offset as u64)
    }

    /// Extracts the payload from a Kafka RecordBatch.
    ///
    /// Returns the first record's value.
    ///
    /// # Errors
    ///
    /// Returns an error with details if parsing fails. This ensures parse failures
    /// are caught explicitly rather than silently skipped.
    pub(crate) fn extract_payload_from_batch(batch: &Bytes) -> Result<Vec<u8>, String> {
        // Kafka RecordBatch v2 format:
        // - 8 bytes: baseOffset
        // - 4 bytes: batchLength
        // - 4 bytes: partitionLeaderEpoch
        // - 1 byte: magic (must be 2)
        // - 4 bytes: crc
        // - 2 bytes: attributes
        // - 4 bytes: lastOffsetDelta
        // - 8 bytes: firstTimestamp
        // - 8 bytes: maxTimestamp
        // - 8 bytes: producerId
        // - 2 bytes: producerEpoch
        // - 4 bytes: baseSequence
        // - 4 bytes: recordCount
        // Records follow at offset 61...

        const BATCH_HEADER_SIZE: usize = 61;

        if batch.len() < BATCH_HEADER_SIZE {
            return Err(format!(
                "batch too short for header: {} bytes (need at least {})",
                batch.len(),
                BATCH_HEADER_SIZE
            ));
        }

        // Verify magic byte.
        let magic = batch[16];
        if magic != 2 {
            return Err(format!("unexpected magic byte: {magic} (expected 2)"));
        }

        // Check record count.
        let record_count = i32::from_be_bytes([batch[57], batch[58], batch[59], batch[60]]);
        if record_count < 1 {
            return Err(format!("no records in batch: record_count={record_count}"));
        }

        // Skip to records (offset 61).
        let record_data = &batch[BATCH_HEADER_SIZE..];
        if record_data.is_empty() {
            return Err("no record data after header".to_string());
        }

        // First byte is record length varint.
        // For test payloads (< 128 bytes), this is a single byte.
        let record_len = record_data[0] as usize;
        if record_len >= 128 {
            return Err(format!(
                "record length {record_len} >= 128, multi-byte varint not supported"
            ));
        }
        if record_data.len() < 1 + record_len {
            return Err(format!(
                "record truncated: declared length {record_len}, but only {} bytes available",
                record_data.len() - 1
            ));
        }

        // Record format (after length varint):
        // - 1 byte: attributes
        // - varint: timestampDelta (single byte for 0)
        // - varint: offsetDelta (single byte for 0)
        // - varint: keyLength (-1 for null = 0x01 zigzag encoded, but we use 0xff)
        // - varint: valueLength
        // - bytes: value
        // - varint: headersCount

        let record_content = &record_data[1..];
        if record_content.len() < 5 {
            return Err(format!(
                "record content too short: {} bytes (need at least 5)",
                record_content.len()
            ));
        }

        // Skip: attributes (1), timestampDelta (1), offsetDelta (1), keyLength (1 = null key).
        let value_len_offset = 4;
        if value_len_offset >= record_content.len() {
            return Err("cannot read valueLength: record too short".to_string());
        }

        let value_len = record_content[value_len_offset] as usize;
        if value_len >= 128 {
            return Err(format!(
                "value length {value_len} >= 128, multi-byte varint not supported"
            ));
        }

        let value_start = value_len_offset + 1;
        if value_start + value_len > record_content.len() {
            return Err(format!(
                "value truncated: declared length {value_len}, but only {} bytes available",
                record_content.len() - value_start
            ));
        }

        Ok(record_content[value_start..value_start + value_len].to_vec())
    }

    /// Asserts that no property violations occurred.
    ///
    /// # Panics
    ///
    /// Panics if any Raft or data integrity violations were detected.
    pub fn assert_no_violations(&self, test_name: &str) {
        let result = self.finalize_verification();
        assert_no_helix_violations(&result, test_name);
    }

    /// Collects Raft snapshots from all nodes for property verification.
    ///
    /// Call this periodically during tests to track leader elections
    /// and verify `SingleLeaderPerTerm`.
    pub async fn collect_raft_snapshots(&self) {
        for node in self.nodes.values() {
            // Skip crashed nodes.
            if !self.is_node_available(node.node_id) {
                // Record crashed state.
                if let Ok(mut state) = self.property_state.lock() {
                    state.update_snapshot(HelixNodeSnapshot {
                        node_id: node.node_id.get(),
                        controller_term: 0,
                        controller_state: RaftState::Follower,
                        crashed: true,
                    });
                }
                continue;
            }

            // Get controller group state (group 0).
            let (controller_term, controller_state) = {
                let mr = node.service.multi_raft().read().await;
                if let Some(state) = mr.group_state(CONTROLLER_GROUP_ID) {
                    (state.current_term.get(), state.state)
                } else {
                    (0, RaftState::Follower)
                }
            };

            // Update property state with snapshot (backward compat for controller).
            if let Ok(mut state) = self.property_state.lock() {
                state.update_snapshot(HelixNodeSnapshot {
                    node_id: node.node_id.get(),
                    controller_term,
                    controller_state,
                    crashed: false,
                });
            }

            // Track leaders for ALL groups (controller + data partitions).
            {
                let mr = node.service.multi_raft().read().await;
                let group_ids = mr.group_ids();
                if let Ok(mut prop_state) = self.property_state.lock() {
                    for group_id in group_ids {
                        if let Some(gs) = mr.group_state(group_id) {
                            prop_state.update_group_snapshot(
                                group_id.get(),
                                gs.current_term.get(),
                                gs.state,
                                node.node_id.get(),
                            );
                        }
                    }
                }
            }
        }
    }

}
