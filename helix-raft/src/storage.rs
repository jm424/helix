//! Raft persistent storage abstraction.
//!
//! This module defines traits and types for persisting Raft state
//! to durable storage. The key persistent state in Raft:
//!
//! - **currentTerm**: The latest term the server has seen
//! - **votedFor**: The candidate that received our vote in the current term
//! - **log[]**: The log entries (each containing term, index, and command)
//!
//! # Design
//!
//! The storage layer is kept separate from the core state machine to maintain
//! deterministic simulation testing. RaftNode remains a pure state machine
//! that produces `PersistState` and `PersistEntries` outputs which the
//! runtime layer handles.

use bytes::BytesMut;
use helix_core::{LogIndex, NodeId, TermId};

use crate::log::LogEntry;

/// Persistent Raft state that must survive crashes.
///
/// This state must be persisted to stable storage before responding to RPCs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentState {
    /// Latest term server has seen (initialized to 0, increases monotonically).
    pub current_term: TermId,
    /// CandidateId that received vote in current term (or None).
    pub voted_for: Option<NodeId>,
}

impl PersistentState {
    /// Creates a new persistent state with initial values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            current_term: TermId::new(0),
            voted_for: None,
        }
    }

    /// Creates a persistent state with the given values.
    #[must_use]
    pub fn with_values(current_term: TermId, voted_for: Option<NodeId>) -> Self {
        Self {
            current_term,
            voted_for,
        }
    }

    /// Encodes the persistent state to bytes.
    ///
    /// Format: term (8 bytes) + voted_for_present (1 byte) + voted_for (8 bytes if present)
    pub fn encode(&self, buf: &mut BytesMut) {
        use bytes::BufMut;

        buf.put_u64_le(self.current_term.get());
        match self.voted_for {
            Some(node_id) => {
                buf.put_u8(1);
                buf.put_u64_le(node_id.get());
            }
            None => {
                buf.put_u8(0);
            }
        }
    }

    /// Decodes persistent state from bytes.
    ///
    /// # Errors
    /// Returns `None` if the buffer is invalid.
    pub fn decode(buf: &mut impl bytes::Buf) -> Option<Self> {
        if buf.remaining() < 9 {
            return None;
        }

        let current_term = TermId::new(buf.get_u64_le());
        let has_voted_for = buf.get_u8();

        let voted_for = if has_voted_for == 1 {
            if buf.remaining() < 8 {
                return None;
            }
            Some(NodeId::new(buf.get_u64_le()))
        } else {
            None
        };

        Some(Self {
            current_term,
            voted_for,
        })
    }
}

impl Default for PersistentState {
    fn default() -> Self {
        Self::new()
    }
}

/// Storage result type.
pub type StorageResult<T> = Result<T, StorageError>;

/// Storage error types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// I/O error during storage operation.
    Io {
        /// The operation that failed.
        operation: &'static str,
        /// Error description.
        message: String,
    },
    /// Data corruption detected.
    Corruption {
        /// Description of the corruption.
        message: String,
    },
    /// Entry not found in storage.
    NotFound {
        /// The index that was not found.
        index: LogIndex,
    },
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { operation, message } => {
                write!(f, "storage I/O error during {}: {}", operation, message)
            }
            Self::Corruption { message } => {
                write!(f, "storage corruption: {}", message)
            }
            Self::NotFound { index } => {
                write!(f, "entry not found at index {}", index.get())
            }
        }
    }
}

impl std::error::Error for StorageError {}

/// Trait for Raft persistent storage.
///
/// Implementations must guarantee durability - data must survive crashes
/// after methods return successfully.
pub trait RaftStorage {
    /// Saves the persistent state (term and voted_for).
    ///
    /// This must be called before responding to RequestVote or AppendEntries RPCs
    /// when the term or votedFor changes.
    ///
    /// # Errors
    /// Returns an error if the state cannot be persisted.
    fn save_state(&mut self, state: &PersistentState) -> StorageResult<()>;

    /// Loads the persistent state.
    ///
    /// Returns `None` if no state has been saved (fresh start).
    ///
    /// # Errors
    /// Returns an error if the state cannot be loaded.
    fn load_state(&self) -> StorageResult<Option<PersistentState>>;

    /// Appends entries to the log.
    ///
    /// Entries must be appended atomically - either all succeed or none do.
    ///
    /// # Errors
    /// Returns an error if entries cannot be appended.
    fn append_entries(&mut self, entries: &[LogEntry]) -> StorageResult<()>;

    /// Reads a log entry by index.
    ///
    /// # Errors
    /// Returns `NotFound` if the entry doesn't exist.
    fn get_entry(&self, index: LogIndex) -> StorageResult<LogEntry>;

    /// Returns entries from start_index to end_index (inclusive).
    ///
    /// # Errors
    /// Returns an error if entries cannot be read.
    fn get_entries(&self, start: LogIndex, end: LogIndex) -> StorageResult<Vec<LogEntry>>;

    /// Truncates the log after the given index.
    ///
    /// Keeps entries up to and including `last_to_keep`.
    ///
    /// # Errors
    /// Returns an error if truncation fails.
    fn truncate_after(&mut self, last_to_keep: LogIndex) -> StorageResult<()>;

    /// Returns the first log index, or 0 if empty.
    fn first_index(&self) -> LogIndex;

    /// Returns the last log index, or 0 if empty.
    fn last_index(&self) -> LogIndex;

    /// Returns the term of the last entry, or 0 if empty.
    fn last_term(&self) -> TermId;

    /// Syncs all data to stable storage.
    ///
    /// This ensures all previously written data is durable.
    ///
    /// # Errors
    /// Returns an error if sync fails.
    fn sync(&mut self) -> StorageResult<()>;
}

/// In-memory storage implementation for testing.
///
/// This implementation does NOT provide durability - it's only for
/// unit tests and simulation testing where persistence is not needed.
#[derive(Debug, Default)]
pub struct MemoryStorage {
    /// Persistent state.
    state: Option<PersistentState>,
    /// Log entries.
    entries: Vec<LogEntry>,
    /// First index (1 if non-empty, 0 if empty).
    first_index: u64,
}

impl MemoryStorage {
    /// Creates a new in-memory storage.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: None,
            entries: Vec::new(),
            first_index: 0,
        }
    }
}

impl RaftStorage for MemoryStorage {
    fn save_state(&mut self, state: &PersistentState) -> StorageResult<()> {
        self.state = Some(state.clone());
        Ok(())
    }

    fn load_state(&self) -> StorageResult<Option<PersistentState>> {
        Ok(self.state.clone())
    }

    fn append_entries(&mut self, entries: &[LogEntry]) -> StorageResult<()> {
        for entry in entries {
            let expected_index = if self.entries.is_empty() {
                self.first_index = entry.index.get();
                entry.index.get()
            } else {
                self.last_index().get() + 1
            };

            if entry.index.get() != expected_index {
                return Err(StorageError::Corruption {
                    message: format!(
                        "non-sequential entry: expected {}, got {}",
                        expected_index,
                        entry.index.get()
                    ),
                });
            }

            self.entries.push(entry.clone());
        }
        Ok(())
    }

    fn get_entry(&self, index: LogIndex) -> StorageResult<LogEntry> {
        if self.entries.is_empty() || index.get() < self.first_index {
            return Err(StorageError::NotFound { index });
        }
        let offset = (index.get() - self.first_index) as usize;
        self.entries
            .get(offset)
            .cloned()
            .ok_or(StorageError::NotFound { index })
    }

    fn get_entries(&self, start: LogIndex, end: LogIndex) -> StorageResult<Vec<LogEntry>> {
        if self.entries.is_empty() {
            return Ok(Vec::new());
        }

        let first = self.first_index;
        let last = first + self.entries.len() as u64 - 1;

        let actual_start = start.get().max(first);
        let actual_end = end.get().min(last);

        if actual_start > actual_end {
            return Ok(Vec::new());
        }

        let start_offset = (actual_start - first) as usize;
        let end_offset = (actual_end - first) as usize + 1;

        Ok(self.entries[start_offset..end_offset].to_vec())
    }

    fn truncate_after(&mut self, last_to_keep: LogIndex) -> StorageResult<()> {
        if self.entries.is_empty() {
            return Ok(());
        }

        if last_to_keep.get() < self.first_index {
            self.entries.clear();
            self.first_index = 0;
            return Ok(());
        }

        let keep_count = (last_to_keep.get() - self.first_index + 1) as usize;
        if keep_count < self.entries.len() {
            self.entries.truncate(keep_count);
        }

        Ok(())
    }

    fn first_index(&self) -> LogIndex {
        if self.entries.is_empty() {
            LogIndex::new(0)
        } else {
            LogIndex::new(self.first_index)
        }
    }

    fn last_index(&self) -> LogIndex {
        if self.entries.is_empty() {
            LogIndex::new(0)
        } else {
            LogIndex::new(self.first_index + self.entries.len() as u64 - 1)
        }
    }

    fn last_term(&self) -> TermId {
        self.entries
            .last()
            .map_or(TermId::new(0), |e| e.term)
    }

    fn sync(&mut self) -> StorageResult<()> {
        // No-op for memory storage.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_entry(term: u64, index: u64) -> LogEntry {
        LogEntry::new(
            TermId::new(term),
            LogIndex::new(index),
            Bytes::from(format!("entry-{index}")),
        )
    }

    #[test]
    fn test_persistent_state_roundtrip() {
        let state = PersistentState::with_values(TermId::new(5), Some(NodeId::new(42)));

        let mut buf = BytesMut::new();
        state.encode(&mut buf);

        let decoded = PersistentState::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded, state);
    }

    #[test]
    fn test_persistent_state_no_vote() {
        let state = PersistentState::with_values(TermId::new(3), None);

        let mut buf = BytesMut::new();
        state.encode(&mut buf);

        let decoded = PersistentState::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded, state);
    }

    #[test]
    fn test_memory_storage_state() {
        let mut storage = MemoryStorage::new();

        // Initially no state.
        assert!(storage.load_state().unwrap().is_none());

        // Save and load state.
        let state = PersistentState::with_values(TermId::new(1), Some(NodeId::new(2)));
        storage.save_state(&state).unwrap();

        let loaded = storage.load_state().unwrap().unwrap();
        assert_eq!(loaded, state);
    }

    #[test]
    fn test_memory_storage_entries() {
        let mut storage = MemoryStorage::new();

        // Initially empty.
        assert_eq!(storage.first_index().get(), 0);
        assert_eq!(storage.last_index().get(), 0);

        // Append entries.
        let entries = vec![make_entry(1, 1), make_entry(1, 2), make_entry(2, 3)];
        storage.append_entries(&entries).unwrap();

        assert_eq!(storage.first_index().get(), 1);
        assert_eq!(storage.last_index().get(), 3);
        assert_eq!(storage.last_term().get(), 2);

        // Get single entry.
        let entry = storage.get_entry(LogIndex::new(2)).unwrap();
        assert_eq!(entry.index.get(), 2);
        assert_eq!(entry.term.get(), 1);

        // Get range.
        let range = storage.get_entries(LogIndex::new(1), LogIndex::new(2)).unwrap();
        assert_eq!(range.len(), 2);
    }

    #[test]
    fn test_memory_storage_truncate() {
        let mut storage = MemoryStorage::new();

        let entries = vec![make_entry(1, 1), make_entry(1, 2), make_entry(2, 3)];
        storage.append_entries(&entries).unwrap();

        storage.truncate_after(LogIndex::new(1)).unwrap();

        assert_eq!(storage.last_index().get(), 1);
        assert!(storage.get_entry(LogIndex::new(2)).is_err());
    }
}
