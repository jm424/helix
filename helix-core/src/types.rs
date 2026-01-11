//! Strongly-typed identifiers for Helix entities.
//!
//! Following `TigerStyle`: explicit types prevent bugs from mixing up IDs.
//! All IDs are 64-bit to handle large-scale deployments.

use std::fmt;

/// Macro to generate strongly-typed ID wrappers.
///
/// Each ID type wraps a u64 and provides:
/// - Type safety (can't mix `NodeId` with `PartitionId`)
/// - Debug/Display formatting
/// - Zero-cost abstraction (same as raw u64)
macro_rules! define_id {
    ($name:ident, $prefix:expr, $doc:expr) => {
        #[doc = $doc]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        #[repr(transparent)]
        pub struct $name(u64);

        impl $name {
            /// Creates a new ID from a raw u64 value.
            #[inline]
            #[must_use]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// Returns the raw u64 value.
            #[inline]
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }

            /// Returns the next ID in sequence.
            ///
            /// # Panics
            /// Panics if the ID would overflow.
            #[inline]
            #[must_use]
            pub const fn next(self) -> Self {
                assert!(self.0 < u64::MAX, "ID overflow");
                Self(self.0 + 1)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", $prefix, self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}-{}", $prefix, self.0)
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self::new(value)
            }
        }

        impl From<$name> for u64 {
            fn from(id: $name) -> Self {
                id.get()
            }
        }
    };
}

// Node and replica identification.
define_id!(NodeId, "node", "Unique identifier for a Helix node in the cluster.");
define_id!(ReplicaId, "replica", "Unique identifier for a replica within a Raft group.");

// Topic and partition identification.
define_id!(TopicId, "topic", "Unique identifier for a topic (stream).");
define_id!(PartitionId, "partition", "Unique identifier for a partition within a topic.");

// Consumer tracking.
define_id!(ConsumerGroupId, "cg", "Unique identifier for a consumer group.");
define_id!(ConsumerId, "consumer", "Unique identifier for a consumer within a group.");
define_id!(LeaseId, "lease", "Unique identifier for a batch lease.");

// Batch and record identification.
define_id!(BatchId, "batch", "Unique identifier for a batch of records.");

// Raft consensus.
define_id!(TermId, "term", "Raft term number for leader election.");
define_id!(LogIndex, "idx", "Index into the Raft log.");
define_id!(GroupId, "group", "Unique identifier for a Raft group.");

// Shard management.
define_id!(TransferId, "xfer", "Unique identifier for a shard transfer operation.");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_type_safety() {
        let node = NodeId::new(1);
        let partition = PartitionId::new(1);

        // These are different types even with same value.
        assert_eq!(node.get(), partition.get());
        // But they can't be compared directly (won't compile):
        // assert_ne!(node, partition);
    }

    #[test]
    fn test_id_display() {
        let node = NodeId::new(42);
        assert_eq!(format!("{node}"), "node-42");
        assert_eq!(format!("{node:?}"), "node(42)");
    }

    #[test]
    fn test_id_next() {
        let id = BatchId::new(0);
        assert_eq!(id.next().get(), 1);
        assert_eq!(id.next().next().get(), 2);
    }

    #[test]
    #[should_panic(expected = "ID overflow")]
    fn test_id_overflow_panics() {
        let id = NodeId::new(u64::MAX);
        let _ = id.next();
    }

    #[test]
    fn test_id_ordering() {
        let a = TermId::new(1);
        let b = TermId::new(2);
        let c = TermId::new(1);

        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, c);
    }
}
