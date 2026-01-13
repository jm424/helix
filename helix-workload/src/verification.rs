//! Verification logic for detecting correctness violations.
//!
//! Implements Jepsen-inspired checks for Kafka semantics:
//! - Lost writes (acknowledged but never observed)
//! - Inconsistent reads (same offset returns different values)
//! - Offset gaps (missing offsets in sequence)
//! - Ordering violations (out-of-order within partition)
//! - Duplicate offsets (same offset assigned to multiple sends)
//! - Commit violations (consumer resumes at wrong offset)

use bytes::Bytes;

use crate::history::History;
use crate::payload::VerifiablePayload;

/// A violation of an expected property.
#[derive(Debug, Clone)]
pub enum Violation {
    /// A successfully acknowledged send was never observed in any poll.
    LostWrite {
        /// Operation ID of the send.
        op_id: u64,
        /// Topic name.
        topic: String,
        /// Partition.
        partition: i32,
        /// Assigned offset.
        offset: u64,
        /// Payload that was sent.
        payload: Bytes,
    },

    /// Same (topic, partition, offset) returned different values.
    InconsistentRead {
        /// Topic name.
        topic: String,
        /// Partition.
        partition: i32,
        /// Offset.
        offset: u64,
        /// Expected value (from send).
        expected: Bytes,
        /// Observed value (from poll).
        observed: Bytes,
        /// Operation ID of the send.
        send_op_id: u64,
        /// Operation ID of the poll.
        poll_op_id: u64,
    },

    /// Gap in offset sequence (missing offsets).
    OffsetGap {
        /// Topic name.
        topic: String,
        /// Partition.
        partition: i32,
        /// Expected offset.
        expected_offset: u64,
        /// Actual offset found.
        actual_offset: u64,
    },

    /// Offsets returned out of order within a single poll.
    OrderingViolation {
        /// Topic name.
        topic: String,
        /// Partition.
        partition: i32,
        /// Operation ID of the poll.
        poll_op_id: u64,
        /// The out-of-order sequence of offsets.
        offsets: Vec<u64>,
    },

    /// Duplicate offset assigned to different sends.
    DuplicateOffset {
        /// Topic name.
        topic: String,
        /// Partition.
        partition: i32,
        /// The duplicated offset.
        offset: u64,
        /// Operation IDs of sends that got this offset.
        op_ids: Vec<u64>,
    },

    /// Consumer resumed at wrong offset after commit.
    CommitViolation {
        /// Consumer group ID.
        group: String,
        /// Topic name.
        topic: String,
        /// Partition.
        partition: i32,
        /// Offset that was committed.
        committed_offset: u64,
        /// Offset that was actually resumed from.
        resumed_offset: u64,
    },

    /// Payload checksum verification failed.
    ChecksumFailure {
        /// Topic name.
        topic: String,
        /// Partition.
        partition: i32,
        /// Offset.
        offset: u64,
        /// Operation ID that observed the corrupted payload.
        poll_op_id: u64,
    },
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LostWrite {
                op_id,
                topic,
                partition,
                offset,
                ..
            } => {
                write!(
                    f,
                    "Lost write: op={op_id} {topic}:{partition}@{offset} was acknowledged but never observed"
                )
            }
            Self::InconsistentRead {
                topic,
                partition,
                offset,
                send_op_id,
                poll_op_id,
                ..
            } => {
                write!(
                    f,
                    "Inconsistent read: {topic}:{partition}@{offset} sent by op={send_op_id}, \
                     different value observed by op={poll_op_id}"
                )
            }
            Self::OffsetGap {
                topic,
                partition,
                expected_offset,
                actual_offset,
            } => {
                write!(
                    f,
                    "Offset gap: {topic}:{partition} expected offset {expected_offset}, got {actual_offset}"
                )
            }
            Self::OrderingViolation {
                topic,
                partition,
                poll_op_id,
                offsets,
            } => {
                write!(
                    f,
                    "Ordering violation: {topic}:{partition} op={poll_op_id} returned out-of-order offsets: {offsets:?}"
                )
            }
            Self::DuplicateOffset {
                topic,
                partition,
                offset,
                op_ids,
            } => {
                write!(
                    f,
                    "Duplicate offset: {topic}:{partition}@{offset} assigned to multiple sends: {op_ids:?}"
                )
            }
            Self::CommitViolation {
                group,
                topic,
                partition,
                committed_offset,
                resumed_offset,
            } => {
                write!(
                    f,
                    "Commit violation: group={group} {topic}:{partition} committed offset {committed_offset}, \
                     resumed at {resumed_offset}"
                )
            }
            Self::ChecksumFailure {
                topic,
                partition,
                offset,
                poll_op_id,
            } => {
                write!(
                    f,
                    "Checksum failure: {topic}:{partition}@{offset} observed by op={poll_op_id}"
                )
            }
        }
    }
}

/// Verification runner that checks all properties.
pub struct Verification;

impl Verification {
    /// Runs all verification checks against a history.
    #[must_use]
    pub fn verify(history: &History) -> Vec<Violation> {
        let mut violations = Vec::new();

        violations.extend(Self::check_lost_writes(history));
        violations.extend(Self::check_inconsistent_reads(history));
        violations.extend(Self::check_offset_gaps(history));
        violations.extend(Self::check_ordering(history));
        violations.extend(Self::check_checksums(history));
        // Note: check_duplicates and check_commits require tracking across
        // multiple sends to same offset and consumer group state respectively.
        // These are more complex and implemented separately.

        violations
    }

    /// Checks that all acknowledged sends are eventually observed.
    ///
    /// A "lost write" occurs when a send is acknowledged (got an offset back)
    /// but the value was never observed in any poll of that offset.
    #[must_use]
    pub fn check_lost_writes(history: &History) -> Vec<Violation> {
        let mut violations = Vec::new();

        for ((topic, partition, offset), (send_op_id, payload)) in history.sends() {
            // Check if this offset was ever observed.
            let key = (topic.clone(), *partition, *offset);
            let observed = history.observations().get(&key);

            if observed.is_none_or(Vec::is_empty) {
                violations.push(Violation::LostWrite {
                    op_id: *send_op_id,
                    topic: topic.clone(),
                    partition: *partition,
                    offset: *offset,
                    payload: payload.clone(),
                });
            }
        }

        violations
    }

    /// Checks that the same offset always returns the same value.
    ///
    /// An "inconsistent read" occurs when a poll returns a different value
    /// for an offset than what was originally sent.
    #[must_use]
    pub fn check_inconsistent_reads(history: &History) -> Vec<Violation> {
        let mut violations = Vec::new();

        for ((topic, partition, offset), (send_op_id, expected)) in history.sends() {
            let key = (topic.clone(), *partition, *offset);

            if let Some(observations) = history.observations().get(&key) {
                for (poll_op_id, observed) in observations {
                    if observed != expected {
                        violations.push(Violation::InconsistentRead {
                            topic: topic.clone(),
                            partition: *partition,
                            offset: *offset,
                            expected: expected.clone(),
                            observed: observed.clone(),
                            send_op_id: *send_op_id,
                            poll_op_id: *poll_op_id,
                        });
                    }
                }
            }
        }

        violations
    }

    /// Checks for gaps in the offset sequence.
    ///
    /// An "offset gap" occurs when offsets are not contiguous.
    #[must_use]
    pub fn check_offset_gaps(history: &History) -> Vec<Violation> {
        let mut violations = Vec::new();

        for (topic, partition) in history.active_partitions() {
            let sends = history.sends_for_partition(&topic, partition);
            if sends.is_empty() {
                continue;
            }

            // Collect all offsets and sort them.
            let mut offsets: Vec<u64> = sends.iter().map(|(offset, _, _)| *offset).collect();
            offsets.sort_unstable();

            // Check for gaps starting from 0.
            let mut expected = 0u64;
            for offset in offsets {
                if offset != expected {
                    violations.push(Violation::OffsetGap {
                        topic: topic.clone(),
                        partition,
                        expected_offset: expected,
                        actual_offset: offset,
                    });
                    // Skip to the actual offset to continue checking.
                    expected = offset;
                }
                expected += 1;
            }
        }

        violations
    }

    /// Checks that offsets within a poll are in order.
    ///
    /// An "ordering violation" occurs when a poll returns offsets that are
    /// not monotonically increasing.
    #[must_use]
    pub fn check_ordering(history: &History) -> Vec<Violation> {
        use crate::operation::{OperationKind, OperationResult};

        let mut violations = Vec::new();

        for op in history.operations() {
            if let (
                OperationKind::Poll { topic, partition, .. },
                Some(OperationResult::PollOk { messages }),
            ) = (&op.kind, &op.result)
            {
                if messages.len() < 2 {
                    continue;
                }

                let offsets: Vec<u64> = messages.iter().map(|(o, _)| *o).collect();

                // Check if offsets are monotonically increasing.
                let mut sorted = true;
                for i in 1..offsets.len() {
                    if offsets[i] <= offsets[i - 1] {
                        sorted = false;
                        break;
                    }
                }

                if !sorted {
                    violations.push(Violation::OrderingViolation {
                        topic: topic.clone(),
                        partition: *partition,
                        poll_op_id: op.id,
                        offsets,
                    });
                }
            }
        }

        violations
    }

    /// Checks that all observed payloads have valid checksums.
    #[must_use]
    pub fn check_checksums(history: &History) -> Vec<Violation> {
        let mut violations = Vec::new();

        for ((topic, partition, offset), observations) in history.observations() {
            for (poll_op_id, payload) in observations {
                // Try to parse and verify the payload.
                if let Ok(verified) = VerifiablePayload::parse(payload) {
                    if !verified.verify_checksum() {
                        violations.push(Violation::ChecksumFailure {
                            topic: topic.clone(),
                            partition: *partition,
                            offset: *offset,
                            poll_op_id: *poll_op_id,
                        });
                    }
                }
                // If parsing fails, the payload might not be a VerifiablePayload,
                // which is okay for non-workload messages.
            }
        }

        violations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{OperationKind, OperationResult};

    fn setup_send(history: &mut History, topic: &str, partition: i32, offset: u64, payload: &[u8]) {
        let op_id = history.record_invoke(
            OperationKind::Send {
                topic: topic.to_string(),
                partition,
                payload: Bytes::from(payload.to_vec()),
            },
            1000,
        );
        history.record_complete(op_id, 1100, OperationResult::SendOk { offset });
    }

    fn setup_poll(
        history: &mut History,
        topic: &str,
        partition: i32,
        messages: Vec<(u64, &[u8])>,
    ) {
        let op_id = history.record_invoke(
            OperationKind::Poll {
                topic: topic.to_string(),
                partition,
                start_offset: 0,
                max_messages: 100,
            },
            2000,
        );
        history.record_complete(
            op_id,
            2100,
            OperationResult::PollOk {
                messages: messages
                    .into_iter()
                    .map(|(o, p)| (o, Bytes::from(p.to_vec())))
                    .collect(),
            },
        );
    }

    #[test]
    fn test_no_violations() {
        let mut history = History::new();

        setup_send(&mut history, "test", 0, 0, b"msg0");
        setup_send(&mut history, "test", 0, 1, b"msg1");
        setup_poll(&mut history, "test", 0, vec![(0, b"msg0"), (1, b"msg1")]);

        let violations = Verification::verify(&history);
        assert!(violations.is_empty(), "Expected no violations: {violations:?}");
    }

    #[test]
    fn test_lost_write() {
        let mut history = History::new();

        setup_send(&mut history, "test", 0, 0, b"msg0");
        setup_send(&mut history, "test", 0, 1, b"msg1");
        // Only poll offset 0, offset 1 is "lost".
        setup_poll(&mut history, "test", 0, vec![(0, b"msg0")]);

        let violations = Verification::check_lost_writes(&history);
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            Violation::LostWrite { offset: 1, .. }
        ));
    }

    #[test]
    fn test_inconsistent_read() {
        let mut history = History::new();

        setup_send(&mut history, "test", 0, 0, b"original");
        // Poll returns different value.
        setup_poll(&mut history, "test", 0, vec![(0, b"corrupted")]);

        let violations = Verification::check_inconsistent_reads(&history);
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            Violation::InconsistentRead { offset: 0, .. }
        ));
    }

    #[test]
    fn test_offset_gap() {
        let mut history = History::new();

        setup_send(&mut history, "test", 0, 0, b"msg0");
        setup_send(&mut history, "test", 0, 2, b"msg2"); // Gap at offset 1.

        let violations = Verification::check_offset_gaps(&history);
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            Violation::OffsetGap {
                expected_offset: 1,
                actual_offset: 2,
                ..
            }
        ));
    }

    #[test]
    fn test_ordering_violation() {
        let mut history = History::new();

        // Poll returns out-of-order offsets.
        let op_id = history.record_invoke(
            OperationKind::Poll {
                topic: "test".to_string(),
                partition: 0,
                start_offset: 0,
                max_messages: 100,
            },
            1000,
        );
        history.record_complete(
            op_id,
            1100,
            OperationResult::PollOk {
                messages: vec![
                    (2, Bytes::from_static(b"msg2")),
                    (1, Bytes::from_static(b"msg1")), // Out of order!
                    (3, Bytes::from_static(b"msg3")),
                ],
            },
        );

        let violations = Verification::check_ordering(&history);
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            Violation::OrderingViolation { .. }
        ));
    }
}
