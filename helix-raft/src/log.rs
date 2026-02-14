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

    /// Returns the wire size of this entry when encoded.
    ///
    /// Wire format: 8 (term) + 8 (index) + 4 (data length prefix) + `data.len()`.
    #[must_use]
    pub const fn wire_size(&self) -> u64 {
        // 8 (term) + 8 (index) + 4 (data_len prefix) + data.len()
        20 + self.data.len() as u64
    }
}

/// In-memory Raft log.
///
/// This is a simple in-memory implementation for simulation testing.
/// Production would use the WAL for persistence.
///
/// # Compacted State
///
/// After restart with an ephemeral log, `compacted_index` and `compacted_term`
/// represent the last committed entry recovered from the data WAL. This makes
/// `last_index()`, `last_term()`, `term_at()`, and `is_up_to_date()` return
/// correct values so that election safety holds without any changes to the
/// election call sites.
#[derive(Debug, Default)]
pub struct RaftLog {
    /// Log entries (0-indexed internally, but `LogIndex` starts at 1).
    entries: Vec<LogEntry>,
    /// Index of first entry (1 if non-empty, 0 if empty).
    first_index: u64,
    /// Floor for `last_index()` when entries is empty (recovered commit index).
    compacted_index: u64,
    /// Term at `compacted_index` (recovered commit term).
    compacted_term: u64,
}

impl RaftLog {
    /// Creates a new empty log.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
            first_index: 0,
            compacted_index: 0,
            compacted_term: 0,
        }
    }

    /// Creates a log with compacted state from recovery.
    ///
    /// After restart, the data WAL tells us the last committed index and term.
    /// Setting these as compacted state makes `last_index()`, `last_term()`,
    /// `term_at()`, and `is_up_to_date()` return correct values so that
    /// election safety holds automatically.
    #[must_use]
    pub const fn with_compacted(index: u64, term: u64) -> Self {
        Self {
            entries: Vec::new(),
            first_index: 0,
            compacted_index: index,
            compacted_term: term,
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
    ///
    /// When entries is empty but compacted state exists, returns `compacted_index`
    /// so that election comparisons reflect recovered WAL state.
    #[must_use]
    pub fn last_index(&self) -> LogIndex {
        if self.entries.is_empty() {
            // Return compacted_index as floor (0 if no compacted state).
            LogIndex::new(self.compacted_index)
        } else {
            // Safe cast: entries.len() is bounded by system memory which always fits in u64.
            #[allow(clippy::cast_possible_truncation)]
            let idx = self.first_index + self.entries.len() as u64 - 1;
            LogIndex::new(idx)
        }
    }

    /// Returns the term of the last entry, or 0 if empty.
    ///
    /// When entries is empty but compacted state exists, returns `compacted_term`
    /// so that election comparisons reflect recovered WAL state.
    #[must_use]
    pub fn last_term(&self) -> TermId {
        self.entries.last().map_or_else(
            || TermId::new(self.compacted_term),
            |e| e.term,
        )
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
    ///
    /// Returns `compacted_term` when `index == compacted_index` and the entry
    /// is not in the vec (i.e., after recovery before new entries are appended).
    #[must_use]
    pub fn term_at(&self, index: LogIndex) -> TermId {
        if let Some(entry) = self.get(index) {
            return entry.term;
        }
        // Check compacted state for the boundary index.
        if self.compacted_index > 0 && index.get() == self.compacted_index {
            return TermId::new(self.compacted_term);
        }
        TermId::new(0)
    }

    /// Appends an entry to the log.
    ///
    /// When entries is empty and compacted state exists, the expected index
    /// is `compacted_index + 1` (first entry after recovered state).
    ///
    /// # Panics
    /// Panics if the entry index is not sequential.
    pub fn append(&mut self, entry: LogEntry) {
        let expected_index = if self.entries.is_empty() {
            if self.compacted_index > 0 {
                // First entry after recovery: must follow compacted state.
                let idx = self.compacted_index + 1;
                self.first_index = idx;
                idx
            } else {
                self.first_index = entry.index.get();
                entry.index.get()
            }
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
    /// Compacted state is preserved as an immutable floor.
    pub fn truncate_after(&mut self, last_to_keep: LogIndex) {
        if self.entries.is_empty() {
            return;
        }

        if last_to_keep.get() < self.first_index {
            // Truncate all entries, but preserve compacted state.
            self.entries.clear();
            if self.compacted_index > 0 {
                self.first_index = self.compacted_index + 1;
            } else {
                self.first_index = 0;
            }
            return;
        }

        // Safe cast: keep_count is bounded by entries.len() which fits in usize.
        #[allow(clippy::cast_possible_truncation)]
        let keep_count = (last_to_keep.get() - self.first_index + 1) as usize;
        if keep_count < self.entries.len() {
            self.entries.truncate(keep_count);
        }
    }

    /// Truncates the log prefix up to and including the given index.
    ///
    /// Removes entries with index <= `last_to_remove`. Updates compacted
    /// state so `term_at(last_to_remove)` returns the correct term for
    /// `prev_log_term` lookups in `send_append_entries`.
    pub fn truncate_prefix(&mut self, last_to_remove: LogIndex) {
        if self.entries.is_empty() {
            return;
        }

        if last_to_remove.get() >= self.last_index().get() {
            // Save compacted state from the last entry before clearing.
            if let Some(last) = self.entries.last() {
                self.compacted_index = last.index.get();
                self.compacted_term = last.term.get();
            }
            self.entries.clear();
            self.first_index = last_to_remove.get() + 1;
            return;
        }

        if last_to_remove.get() < self.first_index {
            // Nothing to remove.
            return;
        }

        // Calculate how many entries to remove.
        // Safe cast: remove_count is bounded by entries.len() which fits in usize.
        #[allow(clippy::cast_possible_truncation)]
        let remove_count = (last_to_remove.get() - self.first_index + 1) as usize;

        // Save compacted state from the last removed entry so that
        // term_at(last_to_remove) returns correctly after truncation.
        let last_removed = &self.entries[remove_count - 1];
        self.compacted_index = last_removed.index.get();
        self.compacted_term = last_removed.term.get();

        // Remove the prefix.
        self.entries.drain(..remove_count);
        self.first_index = last_to_remove.get() + 1;
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

    /// Returns entries from `start_index` up to `max_bytes` total wire size.
    ///
    /// This is used to limit the size of `AppendEntries` messages to fit within
    /// transport message size limits. At least one entry is always returned if
    /// available, even if it exceeds `max_bytes` (to ensure progress).
    #[must_use]
    pub fn entries_from_limited(&self, start_index: LogIndex, max_bytes: u64) -> Vec<LogEntry> {
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

        let mut result = Vec::new();
        let mut total_bytes: u64 = 0;

        for entry in &self.entries[start..] {
            let entry_size = entry.wire_size();

            // Always include at least one entry to ensure progress,
            // even if it exceeds max_bytes.
            if !result.is_empty() && total_bytes + entry_size > max_bytes {
                break;
            }

            total_bytes += entry_size;
            result.push(entry.clone());
        }

        result
    }

    /// Checks if the candidate's log (`other_term`, `other_index`) is at least as
    /// up-to-date as ours.
    ///
    /// Used in leader election to determine if we should grant a vote.
    /// A vote is granted if the candidate's log is at least as up-to-date as ours.
    ///
    /// From the Raft paper: "Raft determines which of two logs is more up-to-date
    /// by comparing the index and term of the last entries in the logs. If the logs
    /// have last entries with different terms, then the log with the later term is
    /// more up-to-date. If the logs end with the same term, then whichever log is
    /// longer is more up-to-date."
    #[must_use]
    pub fn is_up_to_date(&self, other_term: TermId, other_index: LogIndex) -> bool {
        let my_term = self.last_term();
        let my_index = self.last_index();

        // Candidate's log is at least as up-to-date if:
        // 1. Candidate's last term > our last term, OR
        // 2. Terms are equal AND candidate's last index >= our last index
        other_term > my_term || (other_term == my_term && other_index >= my_index)
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
        // is_up_to_date returns true if the CANDIDATE's log is at least as up-to-date as ours.

        // Candidate has higher term - they're more up-to-date, grant vote.
        assert!(log.is_up_to_date(TermId::new(3), LogIndex::new(1)));

        // Candidate has same term but longer log - they're more up-to-date, grant vote.
        assert!(log.is_up_to_date(TermId::new(2), LogIndex::new(3)));

        // Candidate has same term and same index - equal, grant vote.
        assert!(log.is_up_to_date(TermId::new(2), LogIndex::new(2)));

        // Candidate has lower term - we're more up-to-date, deny vote.
        assert!(!log.is_up_to_date(TermId::new(1), LogIndex::new(5)));

        // Candidate has same term but shorter log - we're more up-to-date, deny vote.
        assert!(!log.is_up_to_date(TermId::new(2), LogIndex::new(1)));
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

    #[test]
    fn test_wire_size() {
        // Wire format: 8 (term) + 8 (index) + 4 (data length) + data.len()
        let entry = LogEntry::new(TermId::new(1), LogIndex::new(1), Bytes::from("hello"));
        // 20 + 5 = 25
        assert_eq!(entry.wire_size(), 25);

        let empty_entry = LogEntry::new(TermId::new(1), LogIndex::new(1), Bytes::new());
        // 20 + 0 = 20
        assert_eq!(empty_entry.wire_size(), 20);

        let large_entry = LogEntry::new(
            TermId::new(1),
            LogIndex::new(1),
            Bytes::from(vec![0u8; 1000]),
        );
        // 20 + 1000 = 1020
        assert_eq!(large_entry.wire_size(), 1020);
    }

    #[test]
    fn test_entries_from_limited_all_fit() {
        let mut log = RaftLog::new();
        log.append(make_entry(1, 1)); // ~27 bytes
        log.append(make_entry(1, 2)); // ~27 bytes
        log.append(make_entry(2, 3)); // ~27 bytes

        // Large limit - all entries fit.
        let entries = log.entries_from_limited(LogIndex::new(1), 1000);
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_entries_from_limited_partial() {
        let mut log = RaftLog::new();
        // Each entry: 20 (header) + ~7 bytes data = ~27 bytes.
        log.append(make_entry(1, 1));
        log.append(make_entry(1, 2));
        log.append(make_entry(2, 3));

        // Limit to fit only 2 entries (54 bytes).
        let entries = log.entries_from_limited(LogIndex::new(1), 55);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index.get(), 1);
        assert_eq!(entries[1].index.get(), 2);
    }

    #[test]
    fn test_entries_from_limited_at_least_one() {
        let mut log = RaftLog::new();
        // Create a large entry.
        let large_data = vec![0u8; 1000];
        log.append(LogEntry::new(
            TermId::new(1),
            LogIndex::new(1),
            Bytes::from(large_data),
        ));
        log.append(make_entry(1, 2));

        // Even with a tiny limit, at least one entry is returned.
        let entries = log.entries_from_limited(LogIndex::new(1), 10);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index.get(), 1);
    }

    #[test]
    fn test_entries_from_limited_empty_log() {
        let log = RaftLog::new();
        let entries = log.entries_from_limited(LogIndex::new(1), 1000);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_entries_from_limited_start_beyond_end() {
        let mut log = RaftLog::new();
        log.append(make_entry(1, 1));

        let entries = log.entries_from_limited(LogIndex::new(5), 1000);
        assert!(entries.is_empty());
    }

    // ========================================================================
    // Compacted State Tests
    // ========================================================================

    #[test]
    fn test_log_compacted_last_index_term() {
        // Empty log with compacted state returns correct values.
        let log = RaftLog::with_compacted(10, 3);

        assert!(log.is_empty()); // entries vec is empty
        assert_eq!(log.last_index().get(), 10);
        assert_eq!(log.last_term().get(), 3);
    }

    #[test]
    fn test_log_compacted_term_at() {
        let log = RaftLog::with_compacted(10, 3);

        // Returns compacted_term at compacted_index.
        assert_eq!(log.term_at(LogIndex::new(10)).get(), 3);
        // Returns 0 for other indices.
        assert_eq!(log.term_at(LogIndex::new(9)).get(), 0);
        assert_eq!(log.term_at(LogIndex::new(11)).get(), 0);
    }

    #[test]
    fn test_log_compacted_append() {
        let mut log = RaftLog::with_compacted(10, 3);

        // Appending at compacted_index+1 works.
        log.append(make_entry(4, 11));
        assert_eq!(log.last_index().get(), 11);
        assert_eq!(log.last_term().get(), 4);
        assert_eq!(log.len(), 1);

        // Can continue appending.
        log.append(make_entry(4, 12));
        assert_eq!(log.last_index().get(), 12);
        assert_eq!(log.len(), 2);
    }

    #[test]
    #[should_panic(expected = "log entry index must be sequential")]
    fn test_log_compacted_append_wrong_index() {
        let mut log = RaftLog::with_compacted(10, 3);
        // Appending at wrong index panics.
        log.append(make_entry(4, 15));
    }

    #[test]
    fn test_log_compacted_truncate_floor() {
        let mut log = RaftLog::with_compacted(10, 3);
        log.append(make_entry(4, 11));
        log.append(make_entry(4, 12));

        // Truncate below compacted keeps compacted state.
        log.truncate_after(LogIndex::new(5));
        assert!(log.is_empty());
        assert_eq!(log.last_index().get(), 10);
        assert_eq!(log.last_term().get(), 3);

        // Can still append at compacted_index + 1.
        log.append(make_entry(5, 11));
        assert_eq!(log.last_index().get(), 11);
    }

    #[test]
    fn test_log_compacted_is_up_to_date() {
        let log = RaftLog::with_compacted(10, 3);

        // Candidate with higher term is more up-to-date.
        assert!(log.is_up_to_date(TermId::new(4), LogIndex::new(1)));

        // Candidate with same term, higher index is more up-to-date.
        assert!(log.is_up_to_date(TermId::new(3), LogIndex::new(11)));

        // Candidate with same term, same index is equal.
        assert!(log.is_up_to_date(TermId::new(3), LogIndex::new(10)));

        // Candidate with lower term is less up-to-date.
        assert!(!log.is_up_to_date(TermId::new(2), LogIndex::new(100)));

        // Candidate with same term but shorter log is less up-to-date.
        assert!(!log.is_up_to_date(TermId::new(3), LogIndex::new(9)));
    }
}
