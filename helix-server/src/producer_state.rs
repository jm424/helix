//! Idempotent producer state tracking for deduplication.
//!
//! This module tracks producer state per partition to enable idempotent producing.
//! When a producer sends a batch with (producer_id, epoch, sequence), the broker
//! can detect duplicates and return the cached offset without re-committing.
//!
//! # Design
//!
//! - Deduplication check happens BEFORE Raft proposal to prevent duplicate log entries.
//! - Each partition maintains a map of producer states.
//! - Bounded memory: LRU eviction when exceeding `PRODUCERS_PER_PARTITION_MAX`.
//! - Kafka-compatible 5-batch deduplication window for out-of-order retries.

use std::collections::HashMap;

use helix_core::{Offset, ProducerEpoch, ProducerId, SequenceNum};

/// Maximum number of producers tracked per partition.
/// After this limit, oldest producers are evicted (LRU).
pub const PRODUCERS_PER_PARTITION_MAX: usize = 1000;

/// Deduplication window size (matching Kafka's 5 in-flight requests).
pub const DEDUP_WINDOW_SIZE: usize = 5;

/// Result of sequence number validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceCheckResult {
    /// Sequence is valid and new, proceed with Raft proposal.
    Valid,
    /// Sequence is a duplicate, return cached offset without proposing.
    Duplicate {
        /// The offset assigned when this batch was first committed.
        cached_offset: Offset,
    },
    /// Sequence number is out of order (gap detected).
    OutOfSequence {
        /// The sequence number we expected.
        expected: i32,
        /// The sequence number we received.
        received: i32,
    },
    /// Producer epoch is stale (fenced by newer instance).
    ProducerFenced {
        /// The current epoch for this producer ID.
        current_epoch: ProducerEpoch,
    },
}

/// Entry in the deduplication window.
#[derive(Debug, Clone)]
struct DedupEntry {
    /// The sequence number for this batch.
    sequence: SequenceNum,
    /// The offset assigned when this batch was committed.
    base_offset: Offset,
}

/// State for a single producer on a partition.
#[derive(Debug, Clone)]
pub struct ProducerPartitionState {
    /// Producer epoch (for fencing).
    pub epoch: ProducerEpoch,
    /// Last committed sequence number.
    last_sequence: SequenceNum,
    /// Recent batches for deduplication (circular buffer).
    /// Handles out-of-order retries within the window.
    dedup_window: Vec<DedupEntry>,
    /// Last activity timestamp in microseconds (for LRU eviction).
    last_activity_us: u64,
}

impl ProducerPartitionState {
    /// Creates a new producer state with the given epoch.
    #[must_use]
    pub fn new(epoch: ProducerEpoch, initial_sequence: SequenceNum, current_time_us: u64) -> Self {
        Self {
            epoch,
            last_sequence: initial_sequence,
            dedup_window: Vec::with_capacity(DEDUP_WINDOW_SIZE),
            last_activity_us: current_time_us,
        }
    }

    /// Checks if a sequence number is a duplicate.
    ///
    /// Returns `Some(offset)` if duplicate, `None` if new.
    #[must_use]
    pub fn check_duplicate(&self, sequence: SequenceNum) -> Option<Offset> {
        self.dedup_window
            .iter()
            .find(|e| e.sequence == sequence)
            .map(|e| e.base_offset)
    }

    /// Records a successfully committed batch.
    ///
    /// Updates the deduplication window and last sequence.
    pub fn record_committed(&mut self, sequence: SequenceNum, base_offset: Offset) {
        // Update last sequence if this is newer.
        if sequence > self.last_sequence {
            self.last_sequence = sequence;
        }

        // Remove existing entry for this sequence (shouldn't happen, but defensive).
        self.dedup_window.retain(|e| e.sequence != sequence);

        // Add new entry.
        self.dedup_window.push(DedupEntry {
            sequence,
            base_offset,
        });

        // Maintain window size (FIFO eviction).
        if self.dedup_window.len() > DEDUP_WINDOW_SIZE {
            self.dedup_window.remove(0);
        }
    }

    /// Updates the last activity timestamp.
    #[allow(clippy::missing_const_for_fn)] // Const mut ref not stable yet.
    pub fn touch(&mut self, current_time_us: u64) {
        self.last_activity_us = current_time_us;
    }
}

/// Per-partition producer state for idempotent deduplication.
#[derive(Debug, Default)]
pub struct PartitionProducerState {
    /// Producer states keyed by producer ID.
    producers: HashMap<ProducerId, ProducerPartitionState>,
}

impl PartitionProducerState {
    /// Creates a new empty producer state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            producers: HashMap::new(),
        }
    }

    /// Checks if a produce request should be processed or is a duplicate.
    ///
    /// # Arguments
    ///
    /// * `producer_id` - The producer's unique ID
    /// * `epoch` - The producer's current epoch
    /// * `sequence` - The batch sequence number
    ///
    /// # Returns
    ///
    /// - `Valid` if this is a new request that should be proposed to Raft
    /// - `Duplicate { cached_offset }` if this is a retry of an already-committed batch
    /// - `OutOfSequence` if there's a gap in sequence numbers
    /// - `ProducerFenced` if the epoch is stale
    #[must_use]
    pub fn check_sequence(
        &self,
        producer_id: ProducerId,
        epoch: ProducerEpoch,
        sequence: SequenceNum,
    ) -> SequenceCheckResult {
        let Some(state) = self.producers.get(&producer_id) else {
            // New producer - this is their first batch, always valid.
            return SequenceCheckResult::Valid;
        };

        // Check epoch - reject if stale.
        if epoch < state.epoch {
            return SequenceCheckResult::ProducerFenced {
                current_epoch: state.epoch,
            };
        }

        // If epoch is newer, producer was reinitialized - reset state and accept.
        if epoch > state.epoch {
            return SequenceCheckResult::Valid;
        }

        // Same epoch - check for duplicate in window.
        if let Some(cached_offset) = state.check_duplicate(sequence) {
            return SequenceCheckResult::Duplicate { cached_offset };
        }

        // Check for out-of-order (gap in sequence).
        // Note: Kafka allows some slack for in-flight requests, but we're strict for now.
        let expected_next = state.last_sequence.next();
        // Safe cast: DEDUP_WINDOW_SIZE is a small constant (5).
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        if sequence.get() > expected_next.get() + (DEDUP_WINDOW_SIZE as i32) {
            return SequenceCheckResult::OutOfSequence {
                expected: expected_next.get(),
                received: sequence.get(),
            };
        }

        SequenceCheckResult::Valid
    }

    /// Records a successful produce (call after Raft commit).
    ///
    /// # Arguments
    ///
    /// * `producer_id` - The producer's unique ID
    /// * `epoch` - The producer's current epoch
    /// * `sequence` - The batch sequence number
    /// * `base_offset` - The offset assigned to this batch
    /// * `current_time_us` - Current timestamp for LRU tracking
    pub fn record_produce(
        &mut self,
        producer_id: ProducerId,
        epoch: ProducerEpoch,
        sequence: SequenceNum,
        base_offset: Offset,
        current_time_us: u64,
    ) {
        let state = self.producers.entry(producer_id).or_insert_with(|| {
            // New producer - initialize with the sequence before this one.
            ProducerPartitionState::new(
                epoch,
                SequenceNum::new(sequence.get().saturating_sub(1)),
                current_time_us,
            )
        });

        // If epoch is newer, reset the state.
        if epoch > state.epoch {
            state.epoch = epoch;
            state.dedup_window.clear();
            state.last_sequence = SequenceNum::new(sequence.get().saturating_sub(1));
        }

        state.record_committed(sequence, base_offset);
        state.touch(current_time_us);

        // LRU eviction if over limit.
        if self.producers.len() > PRODUCERS_PER_PARTITION_MAX {
            self.evict_oldest();
        }
    }

    /// Evicts the oldest (least recently active) producer.
    fn evict_oldest(&mut self) {
        if let Some(oldest_pid) = self
            .producers
            .iter()
            .min_by_key(|(_, s)| s.last_activity_us)
            .map(|(pid, _)| *pid)
        {
            self.producers.remove(&oldest_pid);
        }
    }

    /// Returns the number of producers being tracked.
    #[allow(dead_code)]
    #[must_use]
    pub fn producer_count(&self) -> usize {
        self.producers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts() -> u64 {
        1_000_000 // Arbitrary timestamp for tests.
    }

    #[test]
    fn test_new_producer_is_valid() {
        let state = PartitionProducerState::new();
        let result = state.check_sequence(
            ProducerId::new(1),
            ProducerEpoch::new(0),
            SequenceNum::new(0),
        );
        assert_eq!(result, SequenceCheckResult::Valid);
    }

    #[test]
    fn test_duplicate_detection() {
        let mut state = PartitionProducerState::new();
        let pid = ProducerId::new(1);
        let epoch = ProducerEpoch::new(0);
        let seq = SequenceNum::new(0);
        let offset = Offset::new(42);

        // First commit succeeds.
        state.record_produce(pid, epoch, seq, offset, ts());

        // Same sequence is duplicate.
        let result = state.check_sequence(pid, epoch, seq);
        assert_eq!(
            result,
            SequenceCheckResult::Duplicate {
                cached_offset: offset
            }
        );
    }

    #[test]
    fn test_epoch_fencing() {
        let mut state = PartitionProducerState::new();
        let pid = ProducerId::new(1);
        let old_epoch = ProducerEpoch::new(0);
        let new_epoch = ProducerEpoch::new(1);

        // Record with new epoch.
        state.record_produce(pid, new_epoch, SequenceNum::new(0), Offset::new(0), ts());

        // Old epoch is fenced.
        let result = state.check_sequence(pid, old_epoch, SequenceNum::new(1));
        assert_eq!(
            result,
            SequenceCheckResult::ProducerFenced {
                current_epoch: new_epoch
            }
        );
    }

    #[test]
    fn test_new_epoch_resets_state() {
        let mut state = PartitionProducerState::new();
        let pid = ProducerId::new(1);

        // Record with epoch 0.
        state.record_produce(
            pid,
            ProducerEpoch::new(0),
            SequenceNum::new(5),
            Offset::new(100),
            ts(),
        );

        // New epoch with sequence 0 should be valid (not out of order).
        let result = state.check_sequence(pid, ProducerEpoch::new(1), SequenceNum::new(0));
        assert_eq!(result, SequenceCheckResult::Valid);
    }

    #[test]
    fn test_sequential_batches() {
        let mut state = PartitionProducerState::new();
        let pid = ProducerId::new(1);
        let epoch = ProducerEpoch::new(0);

        // First batch.
        let result = state.check_sequence(pid, epoch, SequenceNum::new(0));
        assert_eq!(result, SequenceCheckResult::Valid);
        state.record_produce(pid, epoch, SequenceNum::new(0), Offset::new(0), ts());

        // Second batch.
        let result = state.check_sequence(pid, epoch, SequenceNum::new(1));
        assert_eq!(result, SequenceCheckResult::Valid);
        state.record_produce(pid, epoch, SequenceNum::new(1), Offset::new(1), ts());

        // Third batch.
        let result = state.check_sequence(pid, epoch, SequenceNum::new(2));
        assert_eq!(result, SequenceCheckResult::Valid);
    }

    #[test]
    fn test_lru_eviction() {
        let mut state = PartitionProducerState::new();
        let epoch = ProducerEpoch::new(0);

        // Add more than MAX producers.
        for i in 0..=PRODUCERS_PER_PARTITION_MAX {
            let pid = ProducerId::new(i as u64);
            let time = i as u64 * 1000; // Different timestamps.
            state.record_produce(pid, epoch, SequenceNum::new(0), Offset::new(i as u64), time);
        }

        // Should have evicted the oldest.
        assert_eq!(state.producer_count(), PRODUCERS_PER_PARTITION_MAX);
    }
}
