//! Raft log entries and in-memory log.

use bytes::Bytes;
use helix_core::{LogIndex, TermId};

/// A single entry in the Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// The term when this entry was created.
    pub term: TermId,
    /// The log index of this entry.
    pub index: LogIndex,
    /// The command/data payload.
    pub data: Bytes,
}

impl LogEntry {
    /// Creates a new log entry.
    #[must_use]
    pub const fn new(term: TermId, index: LogIndex, data: Bytes) -> Self {
        Self { term, index, data }
    }
}

/// In-memory Raft log.
///
/// This is a simple in-memory implementation for simulation testing.
/// Production would use the WAL for persistence.
#[derive(Debug, Default)]
pub struct RaftLog {
    /// Log entries (0-indexed internally, but `LogIndex` starts at 1).
    entries: Vec<LogEntry>,
    /// Index of first entry (1 if non-empty, 0 if empty).
    first_index: u64,
}

impl RaftLog {
    /// Creates a new empty log.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
            first_index: 0,
        }
    }

    /// Returns true if the log is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the number of entries in the log.
    #[must_use]
    pub fn len(&self) -> u64 {
        // Safe cast: entries.len() is bounded by system memory which is always < u64::MAX.
        #[allow(clippy::cast_possible_truncation)]
        let len = self.entries.len() as u64;
        len
    }

    /// Returns the first log index, or 0 if empty.
    #[must_use]
    pub fn first_index(&self) -> LogIndex {
        if self.entries.is_empty() {
            LogIndex::new(0)
        } else {
            LogIndex::new(self.first_index)
        }
    }

    /// Returns the last log index, or 0 if empty.
    #[must_use]
    pub fn last_index(&self) -> LogIndex {
        if self.entries.is_empty() {
            LogIndex::new(0)
        } else {
            // Safe cast: entries.len() is bounded by system memory which always fits in u64.
            #[allow(clippy::cast_possible_truncation)]
            let idx = self.first_index + self.entries.len() as u64 - 1;
            LogIndex::new(idx)
        }
    }

    /// Returns the term of the last entry, or 0 if empty.
    #[must_use]
    pub fn last_term(&self) -> TermId {
        self.entries
            .last()
            .map_or(TermId::new(0), |e| e.term)
    }

    /// Gets an entry by index.
    #[must_use]
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        if self.entries.is_empty() || index.get() < self.first_index {
            return None;
        }
        // Safe cast: index difference is bounded by entries.len() which fits in usize.
        #[allow(clippy::cast_possible_truncation)]
        let offset = (index.get() - self.first_index) as usize;
        self.entries.get(offset)
    }

    /// Returns the term at a given index, or 0 if not found.
    #[must_use]
    pub fn term_at(&self, index: LogIndex) -> TermId {
        self.get(index).map_or(TermId::new(0), |e| e.term)
    }

    /// Appends an entry to the log.
    ///
    /// # Panics
    /// Panics if the entry index is not sequential.
    pub fn append(&mut self, entry: LogEntry) {
        let expected_index = if self.entries.is_empty() {
            self.first_index = entry.index.get();
            entry.index.get()
        } else {
            self.last_index().get() + 1
        };

        assert_eq!(
            entry.index.get(),
            expected_index,
            "log entry index must be sequential: expected {}, got {}",
            expected_index,
            entry.index.get()
        );

        self.entries.push(entry);
    }

    /// Appends multiple entries to the log.
    ///
    /// This handles the case where entries might conflict with existing ones.
    /// If a conflict is found, the log is truncated and new entries are appended.
    pub fn append_entries(&mut self, entries: Vec<LogEntry>) {
        for entry in entries {
            // Check for conflict.
            if let Some(existing) = self.get(entry.index) {
                if existing.term != entry.term {
                    // Conflict! Truncate from this point.
                    self.truncate_after(LogIndex::new(entry.index.get() - 1));
                }
            }

            // Append if we don't already have this entry.
            if self.is_empty() || entry.index.get() > self.last_index().get() {
                self.append(entry);
            }
        }
    }

    /// Truncates the log after the given index.
    ///
    /// Keeps entries up to and including `last_to_keep`.
    pub fn truncate_after(&mut self, last_to_keep: LogIndex) {
        if self.entries.is_empty() {
            return;
        }

        if last_to_keep.get() < self.first_index {
            // Truncate everything.
            self.entries.clear();
            self.first_index = 0;
            return;
        }

        // Safe cast: keep_count is bounded by entries.len() which fits in usize.
        #[allow(clippy::cast_possible_truncation)]
        let keep_count = (last_to_keep.get() - self.first_index + 1) as usize;
        if keep_count < self.entries.len() {
            self.entries.truncate(keep_count);
        }
    }

    /// Returns entries from `start_index` to the end.
    #[must_use]
    pub fn entries_from(&self, start_index: LogIndex) -> Vec<LogEntry> {
        if self.entries.is_empty() || start_index.get() > self.last_index().get() {
            return Vec::new();
        }

        let start = if start_index.get() < self.first_index {
            0
        } else {
            // Safe cast: start is bounded by entries.len() which fits in usize.
            #[allow(clippy::cast_possible_truncation)]
            let s = (start_index.get() - self.first_index) as usize;
            s
        };

        self.entries[start..].to_vec()
    }

    /// Checks if our log is at least as up-to-date as (`last_term`, `last_index`).
    ///
    /// Used in leader election to determine if we should grant a vote.
    #[must_use]
    pub fn is_up_to_date(&self, other_term: TermId, other_index: LogIndex) -> bool {
        let my_term = self.last_term();
        let my_index = self.last_index();

        // Compare by term first, then by index.
        my_term > other_term || (my_term == other_term && my_index >= other_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(term: u64, index: u64) -> LogEntry {
        LogEntry::new(
            TermId::new(term),
            LogIndex::new(index),
            Bytes::from(format!("entry-{index}")),
        )
    }

    #[test]
    fn test_empty_log() {
        let log = RaftLog::new();

        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
        assert_eq!(log.first_index().get(), 0);
        assert_eq!(log.last_index().get(), 0);
        assert_eq!(log.last_term().get(), 0);
        assert!(log.get(LogIndex::new(1)).is_none());
    }

    #[test]
    fn test_append_and_get() {
        let mut log = RaftLog::new();

        log.append(make_entry(1, 1));
        log.append(make_entry(1, 2));
        log.append(make_entry(2, 3));

        assert_eq!(log.len(), 3);
        assert_eq!(log.first_index().get(), 1);
        assert_eq!(log.last_index().get(), 3);
        assert_eq!(log.last_term().get(), 2);

        assert_eq!(log.get(LogIndex::new(1)).unwrap().term.get(), 1);
        assert_eq!(log.get(LogIndex::new(2)).unwrap().term.get(), 1);
        assert_eq!(log.get(LogIndex::new(3)).unwrap().term.get(), 2);
        assert!(log.get(LogIndex::new(4)).is_none());
    }

    #[test]
    fn test_truncate() {
        let mut log = RaftLog::new();

        log.append(make_entry(1, 1));
        log.append(make_entry(1, 2));
        log.append(make_entry(2, 3));

        log.truncate_after(LogIndex::new(1));

        assert_eq!(log.len(), 1);
        assert_eq!(log.last_index().get(), 1);
    }

    #[test]
    fn test_append_entries_with_conflict() {
        let mut log = RaftLog::new();

        log.append(make_entry(1, 1));
        log.append(make_entry(1, 2));
        log.append(make_entry(1, 3));

        // New entries with conflict at index 2 (different term).
        let new_entries = vec![
            LogEntry::new(TermId::new(2), LogIndex::new(2), Bytes::from("new")),
            LogEntry::new(TermId::new(2), LogIndex::new(3), Bytes::from("new")),
        ];

        log.append_entries(new_entries);

        assert_eq!(log.len(), 3);
        assert_eq!(log.term_at(LogIndex::new(2)).get(), 2);
        assert_eq!(log.term_at(LogIndex::new(3)).get(), 2);
    }

    #[test]
    fn test_is_up_to_date() {
        let mut log = RaftLog::new();
        log.append(make_entry(1, 1));
        log.append(make_entry(2, 2));

        // Our log: term=2, index=2

        // Higher term always wins.
        assert!(!log.is_up_to_date(TermId::new(3), LogIndex::new(1)));

        // Same term, longer log wins.
        assert!(!log.is_up_to_date(TermId::new(2), LogIndex::new(3)));

        // Same term and index - we're up to date.
        assert!(log.is_up_to_date(TermId::new(2), LogIndex::new(2)));

        // Lower term - we're more up to date.
        assert!(log.is_up_to_date(TermId::new(1), LogIndex::new(5)));
    }

    #[test]
    fn test_entries_from() {
        let mut log = RaftLog::new();
        log.append(make_entry(1, 1));
        log.append(make_entry(1, 2));
        log.append(make_entry(2, 3));

        let entries = log.entries_from(LogIndex::new(2));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index.get(), 2);
        assert_eq!(entries[1].index.get(), 3);
    }
}
