//! History recording for workload verification.
//!
//! Tracks all operations performed during a workload execution, indexed for
//! efficient verification queries.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::operation::{Operation, OperationKind, OperationResult, Timestamp};
use crate::payload::VerifiablePayload;

/// Key for indexing by topic, partition, and offset.
pub type OffsetKey = (String, i32, u64);

/// Key for indexing by topic and partition.
pub type PartitionKey = (String, i32);

/// Key for indexing by group, topic, and partition.
pub type CommitKey = (String, String, i32);

/// Complete history of a workload execution.
#[derive(Debug, Default)]
pub struct History {
    /// All operations in invocation order.
    operations: Vec<Operation>,

    /// Next operation ID to assign.
    next_op_id: u64,

    /// Successful sends indexed by (topic, partition, offset).
    /// Maps to operation ID and payload.
    sends: BTreeMap<OffsetKey, (u64, Bytes)>,

    /// Observed values from polls indexed by (topic, partition, offset).
    /// Maps to list of (`op_id`, payload) that observed this offset.
    observations: BTreeMap<OffsetKey, Vec<(u64, Bytes)>>,

    /// Committed offsets indexed by (group, topic, partition).
    /// Maps to list of (`op_id`, offset) commits.
    commits: BTreeMap<CommitKey, Vec<(u64, u64)>>,

    /// High watermark per partition (highest offset seen).
    high_watermarks: BTreeMap<PartitionKey, u64>,
}

impl History {
    /// Creates a new empty history.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an operation invocation and returns the assigned operation ID.
    pub fn record_invoke(&mut self, kind: OperationKind, invoked_at: Timestamp) -> u64 {
        let id = self.next_op_id;
        self.next_op_id += 1;

        let op = Operation::new(id, kind, invoked_at);
        self.operations.push(op);

        id
    }

    /// Records the completion of an operation.
    ///
    /// # Panics
    ///
    /// Panics if the operation ID is invalid.
    #[allow(clippy::cast_possible_truncation)] // op_id is always within Vec bounds.
    pub fn record_complete(&mut self, op_id: u64, completed_at: Timestamp, result: OperationResult) {
        let op = self
            .operations
            .get_mut(op_id as usize)
            .expect("invalid operation ID");

        // Index successful results.
        match (&op.kind, &result) {
            (
                OperationKind::Send {
                    topic, partition, payload,
                },
                OperationResult::SendOk { offset },
            ) => {
                let key = (topic.clone(), *partition, *offset);
                self.sends.insert(key, (op_id, payload.clone()));

                // Update high watermark.
                let hwm = self
                    .high_watermarks
                    .entry((topic.clone(), *partition))
                    .or_insert(0);
                *hwm = (*hwm).max(*offset + 1);
            }

            (
                OperationKind::Poll { topic, partition, .. },
                OperationResult::PollOk { messages },
            ) => {
                for (offset, payload) in messages {
                    let key = (topic.clone(), *partition, *offset);
                    self.observations
                        .entry(key)
                        .or_default()
                        .push((op_id, payload.clone()));

                    // Update high watermark.
                    let hwm = self
                        .high_watermarks
                        .entry((topic.clone(), *partition))
                        .or_insert(0);
                    *hwm = (*hwm).max(*offset + 1);
                }
            }

            (
                OperationKind::Commit {
                    group,
                    topic,
                    partition,
                    offset,
                },
                OperationResult::CommitOk,
            ) => {
                let key = (group.clone(), topic.clone(), *partition);
                self.commits.entry(key).or_default().push((op_id, *offset));
            }

            _ => {}
        }

        op.complete(completed_at, result);
    }

    /// Returns all operations.
    #[must_use]
    pub const fn operations(&self) -> &Vec<Operation> {
        &self.operations
    }

    /// Returns an operation by ID.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn get_operation(&self, op_id: u64) -> Option<&Operation> {
        self.operations.get(op_id as usize)
    }

    /// Returns all successful sends.
    #[must_use]
    pub const fn sends(&self) -> &BTreeMap<OffsetKey, (u64, Bytes)> {
        &self.sends
    }

    /// Returns all observations from polls.
    #[must_use]
    pub const fn observations(&self) -> &BTreeMap<OffsetKey, Vec<(u64, Bytes)>> {
        &self.observations
    }

    /// Returns all commits.
    #[must_use]
    pub const fn commits(&self) -> &BTreeMap<CommitKey, Vec<(u64, u64)>> {
        &self.commits
    }

    /// Returns the high watermark for a partition.
    #[must_use]
    pub fn high_watermark(&self, topic: &str, partition: i32) -> u64 {
        self.high_watermarks
            .get(&(topic.to_string(), partition))
            .copied()
            .unwrap_or(0)
    }

    /// Returns the low watermark (minimum offset written) for a partition.
    ///
    /// Returns `None` if no sends have been recorded for this partition.
    #[must_use]
    pub fn low_watermark(&self, topic: &str, partition: i32) -> Option<u64> {
        // Find the minimum offset in sends for this partition.
        self.sends
            .keys()
            .filter(|(t, p, _)| t == topic && *p == partition)
            .map(|(_, _, offset)| *offset)
            .min()
    }

    /// Returns all partitions that have activity.
    #[must_use]
    pub fn active_partitions(&self) -> Vec<PartitionKey> {
        self.high_watermarks.keys().cloned().collect()
    }

    /// Returns all operations for a specific partition in invocation order.
    #[must_use]
    pub fn ops_for_partition(&self, topic: &str, partition: i32) -> Vec<&Operation> {
        self.operations
            .iter()
            .filter(|op| {
                op.kind.topic() == Some(topic) && op.kind.partition() == Some(partition)
            })
            .collect()
    }

    /// Returns successful send operations in offset order for a partition.
    ///
    /// # Panics
    ///
    /// Panics if the operation ID stored in sends is invalid.
    #[must_use]
    pub fn sends_for_partition(&self, topic: &str, partition: i32) -> Vec<(u64, &Operation, &Bytes)> {
        self.sends
            .iter()
            .filter(|((t, p, _), _)| t == topic && *p == partition)
            .map(|((_, _, offset), (op_id, payload))| {
                (*offset, self.get_operation(*op_id).expect("valid op_id"), payload)
            })
            .collect()
    }

    /// Parses a payload from bytes, returning None if parsing fails.
    #[must_use]
    pub fn parse_payload(bytes: &Bytes) -> Option<VerifiablePayload> {
        VerifiablePayload::parse(bytes).ok()
    }

    /// Returns statistics about the history.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn stats(&self) -> HistoryStats {
        let total_ops = self.operations.len() as u64;
        let successful_ops = self.operations.iter().filter(|op| op.is_ok()).count() as u64;
        let failed_ops = total_ops - successful_ops;

        let send_count = self.sends.len() as u64;
        let poll_count = self
            .operations
            .iter()
            .filter(|op| matches!(op.kind, OperationKind::Poll { .. }) && op.is_ok())
            .count() as u64;
        let message_count = self.observations.len() as u64;

        HistoryStats {
            total_ops,
            successful_ops,
            failed_ops,
            send_count,
            poll_count,
            message_count,
            partition_count: self.high_watermarks.len() as u64,
        }
    }
}

/// Statistics about a history.
#[derive(Debug, Clone)]
pub struct HistoryStats {
    /// Total operations recorded.
    pub total_ops: u64,
    /// Successful operations.
    pub successful_ops: u64,
    /// Failed operations.
    pub failed_ops: u64,
    /// Successful sends.
    pub send_count: u64,
    /// Successful polls.
    pub poll_count: u64,
    /// Unique messages observed.
    pub message_count: u64,
    /// Number of partitions with activity.
    pub partition_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_history_send_tracking() {
        let mut history = History::new();

        let op_id = history.record_invoke(
            OperationKind::Send {
                topic: "test".to_string(),
                partition: 0,
                payload: Bytes::from_static(b"hello"),
            },
            1000,
        );

        history.record_complete(op_id, 1100, OperationResult::SendOk { offset: 0 });

        assert_eq!(history.sends().len(), 1);
        assert_eq!(history.high_watermark("test", 0), 1);

        let sends = history.sends_for_partition("test", 0);
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].0, 0); // offset
    }

    #[test]
    fn test_history_poll_tracking() {
        let mut history = History::new();

        let op_id = history.record_invoke(
            OperationKind::Poll {
                topic: "test".to_string(),
                partition: 0,
                start_offset: 0,
                max_messages: 10,
            },
            1000,
        );

        history.record_complete(
            op_id,
            1100,
            OperationResult::PollOk {
                messages: vec![
                    (0, Bytes::from_static(b"msg0")),
                    (1, Bytes::from_static(b"msg1")),
                ],
            },
        );

        assert_eq!(history.observations().len(), 2);
        assert_eq!(history.high_watermark("test", 0), 2);
    }

    #[test]
    fn test_history_stats() {
        let mut history = History::new();

        // Successful send.
        let op1 = history.record_invoke(
            OperationKind::Send {
                topic: "test".to_string(),
                partition: 0,
                payload: Bytes::new(),
            },
            1000,
        );
        history.record_complete(op1, 1100, OperationResult::SendOk { offset: 0 });

        // Failed send.
        let op2 = history.record_invoke(
            OperationKind::Send {
                topic: "test".to_string(),
                partition: 0,
                payload: Bytes::new(),
            },
            2000,
        );
        history.record_complete(
            op2,
            2100,
            OperationResult::SendError {
                code: 6,
                message: "not leader".to_string(),
            },
        );

        let stats = history.stats();
        assert_eq!(stats.total_ops, 2);
        assert_eq!(stats.successful_ops, 1);
        assert_eq!(stats.failed_ops, 1);
        assert_eq!(stats.send_count, 1);
    }
}
