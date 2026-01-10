//! Shard map - mapping of key ranges to Raft groups.
//!
//! The shard map defines which Raft group is responsible for which key range.
//! Keys are hashed and the hash value determines the shard.

use std::collections::BTreeMap;

use helix_core::GroupId;

/// Maximum number of shards supported.
pub const SHARDS_MAX: u32 = 65_536;

/// A range of shard hashes [start, end).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardRange {
    /// Start of the range (inclusive).
    pub start: u32,
    /// End of the range (exclusive).
    pub end: u32,
}

impl ShardRange {
    /// Creates a new shard range.
    ///
    /// # Panics
    ///
    /// Panics if start >= end.
    #[must_use]
    pub fn new(start: u32, end: u32) -> Self {
        assert!(start < end, "shard range start must be < end");
        Self { start, end }
    }

    /// Returns true if this range contains the given hash value.
    #[must_use]
    pub const fn contains(&self, hash: u32) -> bool {
        hash >= self.start && hash < self.end
    }

    /// Returns the number of hash values in this range.
    #[must_use]
    pub const fn size(&self) -> u32 {
        self.end - self.start
    }

    /// Splits this range at the given point.
    ///
    /// Returns (left, right) where left covers [start, mid) and right covers [mid, end).
    ///
    /// # Panics
    ///
    /// Panics if mid is not within the range.
    #[must_use]
    pub fn split_at(&self, mid: u32) -> (Self, Self) {
        assert!(mid > self.start && mid < self.end, "split point must be within range");
        (
            Self::new(self.start, mid),
            Self::new(mid, self.end),
        )
    }
}

/// Entry in the shard map.
#[derive(Debug, Clone)]
struct ShardEntry {
    /// The key range for this shard.
    range: ShardRange,
    /// The Raft group responsible for this shard.
    group_id: GroupId,
}

/// Maps key hash ranges to Raft groups.
///
/// The shard map uses a B-tree to efficiently look up which Raft group
/// is responsible for a given key hash.
#[derive(Debug, Clone)]
pub struct ShardMap {
    /// Map from range start to shard entry.
    /// Using `BTreeMap` allows efficient range lookups.
    shards: BTreeMap<u32, ShardEntry>,
    /// Total number of shards.
    shard_count: u32,
    /// Version number for tracking updates.
    version: u64,
}

impl ShardMap {
    /// Creates a new empty shard map.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // BTreeMap::new() is not const stable
    pub fn new() -> Self {
        Self {
            shards: BTreeMap::new(),
            shard_count: 0,
            version: 0,
        }
    }

    /// Creates a shard map with uniform distribution across groups.
    ///
    /// The hash space is divided evenly among the specified groups.
    ///
    /// # Panics
    ///
    /// Panics if groups is empty or exceeds `SHARDS_MAX`.
    #[must_use]
    pub fn uniform(groups: &[GroupId]) -> Self {
        assert!(!groups.is_empty(), "groups cannot be empty");
        assert!(
            groups.len() <= SHARDS_MAX as usize,
            "too many groups: {} > {}",
            groups.len(),
            SHARDS_MAX
        );

        let mut map = Self::new();
        // Safety: Length is bounded by SHARDS_MAX (65_536) which fits in u32.
        #[allow(clippy::cast_possible_truncation)]
        let group_count = groups.len() as u32;
        let shard_size = u32::MAX / group_count;

        for (i, &group_id) in groups.iter().enumerate() {
            // Safety: i < group_count which fits in u32.
            #[allow(clippy::cast_possible_truncation)]
            let i = i as u32;
            let start = i.saturating_mul(shard_size);
            let end = if i == group_count - 1 {
                u32::MAX
            } else {
                (i + 1).saturating_mul(shard_size)
            };

            let range = ShardRange::new(start, end);
            map.shards.insert(start, ShardEntry { range, group_id });
            map.shard_count += 1;
        }

        map.version = 1;
        map
    }

    /// Returns the Raft group responsible for the given key hash.
    #[must_use]
    pub fn lookup(&self, hash: u32) -> Option<GroupId> {
        // Find the entry with the largest start <= hash.
        self.shards
            .range(..=hash)
            .next_back()
            .filter(|(_, entry)| entry.range.contains(hash))
            .map(|(_, entry)| entry.group_id)
    }

    /// Returns the number of shards in the map.
    #[must_use]
    pub const fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// Returns the version number of the shard map.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Assigns a shard range to a Raft group.
    ///
    /// This overwrites any existing assignment for the range.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation would exceed limits.
    pub fn assign(&mut self, range: ShardRange, group_id: GroupId) -> Result<(), ShardMapError> {
        // Check if we're at the limit.
        if self.shard_count >= SHARDS_MAX && !self.shards.contains_key(&range.start) {
            return Err(ShardMapError::TooManyShards {
                count: self.shard_count,
                max: SHARDS_MAX,
            });
        }

        // Insert or update.
        let is_new = !self.shards.contains_key(&range.start);
        self.shards.insert(range.start, ShardEntry { range, group_id });

        if is_new {
            self.shard_count += 1;
        }
        self.version += 1;

        Ok(())
    }

    /// Removes a shard assignment.
    ///
    /// # Errors
    ///
    /// Returns an error if the shard doesn't exist.
    pub fn remove(&mut self, start: u32) -> Result<(), ShardMapError> {
        if self.shards.remove(&start).is_none() {
            return Err(ShardMapError::ShardNotFound { start });
        }

        self.shard_count -= 1;
        self.version += 1;

        Ok(())
    }

    /// Returns an iterator over all shard assignments.
    pub fn iter(&self) -> impl Iterator<Item = (ShardRange, GroupId)> + '_ {
        self.shards.values().map(|e| (e.range, e.group_id))
    }

    /// Returns all groups that have shard assignments.
    #[must_use]
    pub fn groups(&self) -> Vec<GroupId> {
        let mut groups: Vec<_> = self.shards.values().map(|e| e.group_id).collect();
        groups.sort();
        groups.dedup();
        groups
    }
}

impl Default for ShardMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors from shard map operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardMapError {
    /// Too many shards.
    TooManyShards {
        /// Current count.
        count: u32,
        /// Maximum allowed.
        max: u32,
    },
    /// Shard not found.
    ShardNotFound {
        /// The shard start that wasn't found.
        start: u32,
    },
}

impl std::fmt::Display for ShardMapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyShards { count, max } => {
                write!(f, "too many shards: {count} (max {max})")
            }
            Self::ShardNotFound { start } => {
                write!(f, "shard not found: start={start}")
            }
        }
    }
}

impl std::error::Error for ShardMapError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_range() {
        let range = ShardRange::new(100, 200);

        assert!(range.contains(100));
        assert!(range.contains(150));
        assert!(range.contains(199));
        assert!(!range.contains(99));
        assert!(!range.contains(200));
        assert_eq!(range.size(), 100);
    }

    #[test]
    fn test_shard_range_split() {
        let range = ShardRange::new(0, 100);
        let (left, right) = range.split_at(50);

        assert_eq!(left.start, 0);
        assert_eq!(left.end, 50);
        assert_eq!(right.start, 50);
        assert_eq!(right.end, 100);
    }

    #[test]
    #[should_panic(expected = "start must be < end")]
    fn test_shard_range_invalid() {
        let _ = ShardRange::new(100, 100);
    }

    #[test]
    fn test_uniform_shard_map() {
        let groups = vec![GroupId::new(1), GroupId::new(2), GroupId::new(3)];
        let map = ShardMap::uniform(&groups);

        assert_eq!(map.shard_count(), 3);
        assert_eq!(map.version(), 1);

        // Every hash value should map to exactly one group.
        for hash in [0, 100, 1000, u32::MAX / 2, u32::MAX - 1] {
            let group = map.lookup(hash);
            assert!(group.is_some(), "hash {hash} should map to a group");
        }
    }

    #[test]
    fn test_lookup() {
        let groups = vec![GroupId::new(1), GroupId::new(2)];
        let map = ShardMap::uniform(&groups);

        // First half should map to group 1.
        let group = map.lookup(0).unwrap();
        assert_eq!(group, GroupId::new(1));

        // Second half should map to group 2.
        let group = map.lookup(u32::MAX - 1).unwrap();
        assert_eq!(group, GroupId::new(2));
    }

    #[test]
    fn test_assign_and_remove() {
        let mut map = ShardMap::new();

        let range = ShardRange::new(0, 1000);
        map.assign(range, GroupId::new(1)).unwrap();

        assert_eq!(map.shard_count(), 1);
        assert_eq!(map.lookup(500), Some(GroupId::new(1)));

        map.remove(0).unwrap();
        assert_eq!(map.shard_count(), 0);
        assert_eq!(map.lookup(500), None);
    }

    #[test]
    fn test_version_increments() {
        let mut map = ShardMap::new();
        assert_eq!(map.version(), 0);

        let range = ShardRange::new(0, 1000);
        map.assign(range, GroupId::new(1)).unwrap();
        assert_eq!(map.version(), 1);

        map.assign(range, GroupId::new(2)).unwrap();
        assert_eq!(map.version(), 2);

        map.remove(0).unwrap();
        assert_eq!(map.version(), 3);
    }

    #[test]
    fn test_groups() {
        let groups = vec![GroupId::new(3), GroupId::new(1), GroupId::new(2)];
        let map = ShardMap::uniform(&groups);

        let result = map.groups();
        // Should be sorted and deduplicated.
        assert_eq!(result, vec![GroupId::new(1), GroupId::new(2), GroupId::new(3)]);
    }
}
