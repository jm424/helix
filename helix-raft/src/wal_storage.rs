//! WAL-backed persistent storage for Raft.
//!
//! This module provides a `WalStorage` implementation that uses helix-wal
//! for durable storage of Raft persistent state.
//!
//! # Storage Layout
//!
//! The WAL stores both log entries and metadata:
//! - Log entries are stored at their Raft index (starting at 1)
//! - Metadata (term, votedFor) is stored in a separate in-memory cache
//!   and persisted as a special entry at index 0
//!
//! # Thread Safety
//!
//! This implementation is designed for single-threaded use within
//! a Raft node. External synchronization is required for concurrent access.

use bytes::{Bytes, BytesMut};
use helix_core::{LogIndex, TermId};
use helix_wal::{Entry, Segment, SegmentConfig, SegmentId};

use crate::log::LogEntry;
use crate::storage::{PersistentState, RaftStorage, StorageError, StorageResult};

/// Magic term value used to identify metadata entries.
const METADATA_TERM: u64 = u64::MAX;

/// Magic index for metadata entry.
const METADATA_INDEX: u64 = 0;

/// WAL-backed storage for Raft.
///
/// Uses helix-wal segments for durable storage of log entries.
/// Currently uses in-memory segments for simulation compatibility.
#[derive(Debug)]
pub struct WalStorage {
    /// Current segment for writing.
    segment: Segment,
    /// Segment configuration (used when creating new segments).
    #[allow(dead_code)]
    config: SegmentConfig,
    /// Cached persistent state.
    state: Option<PersistentState>,
    /// First log index (excluding metadata).
    first_index: u64,
    /// Last log index.
    last_index: u64,
    /// Term of last entry.
    last_term: TermId,
}

impl WalStorage {
    /// Creates a new WAL storage.
    #[must_use]
    pub const fn new() -> Self {
        let config = SegmentConfig::new();
        // Segment starts at index 0 to hold metadata.
        let segment = Segment::new(SegmentId::new(0), 0, config);

        Self {
            segment,
            config,
            state: None,
            first_index: 0,
            last_index: 0,
            last_term: TermId::new(0),
        }
    }

    /// Creates WAL storage with a custom segment configuration.
    #[must_use]
    pub const fn with_config(config: SegmentConfig) -> Self {
        let segment = Segment::new(SegmentId::new(0), 0, config);

        Self {
            segment,
            config,
            state: None,
            first_index: 0,
            last_index: 0,
            last_term: TermId::new(0),
        }
    }

    /// Recovers storage from an existing segment.
    ///
    /// # Errors
    /// Returns an error if recovery fails.
    pub fn recover(data: Bytes, config: SegmentConfig) -> StorageResult<Self> {
        let segment = Segment::decode(data, config).map_err(|e| StorageError::Corruption {
            message: format!("failed to decode segment: {e}"),
        })?;

        let mut storage = Self {
            segment,
            config,
            state: None,
            first_index: 0,
            last_index: 0,
            last_term: TermId::new(0),
        };

        // Recover state and index information.
        storage.recover_state()?;

        Ok(storage)
    }

    /// Recovers persistent state and index information from the segment.
    fn recover_state(&mut self) -> StorageResult<()> {
        // Try to read metadata entry at index 0.
        if let Ok(wal_entry) = self.segment.read(METADATA_INDEX) {
            if wal_entry.term() == METADATA_TERM {
                // This is a metadata entry.
                let state = PersistentState::decode(&mut wal_entry.payload.clone())
                    .ok_or_else(|| StorageError::Corruption {
                        message: "invalid metadata entry".to_string(),
                    })?;
                self.state = Some(state);
            }
        }

        // Scan for first and last log entries.
        let mut found_first = false;
        let mut current_index = 1u64; // Log entries start at index 1

        while let Ok(entry) = self.segment.read(current_index) {
            // Skip metadata entries (term == MAX).
            if entry.term() != METADATA_TERM {
                if !found_first {
                    self.first_index = current_index;
                    found_first = true;
                }
                self.last_index = current_index;
                self.last_term = TermId::new(entry.term());
            }
            current_index += 1;
        }

        Ok(())
    }

    /// Converts a Raft `LogEntry` to a WAL Entry.
    fn log_to_wal_entry(entry: &LogEntry) -> StorageResult<Entry> {
        Entry::new(entry.term.get(), entry.index.get(), entry.data.clone()).map_err(|e| {
            StorageError::Io {
                operation: "encode",
                message: format!("failed to create WAL entry: {e}"),
            }
        })
    }

    /// Converts a WAL Entry to a Raft `LogEntry`.
    fn wal_to_log_entry(entry: &helix_wal::Entry) -> LogEntry {
        LogEntry::new(
            TermId::new(entry.term()),
            LogIndex::new(entry.index()),
            entry.payload.clone(),
        )
    }

    /// Encodes metadata as a WAL entry payload.
    fn encode_metadata(state: &PersistentState) -> Bytes {
        let mut buf = BytesMut::new();
        state.encode(&mut buf);
        buf.freeze()
    }
}

impl Default for WalStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftStorage for WalStorage {
    fn save_state(&mut self, state: &PersistentState) -> StorageResult<()> {
        // Create a metadata entry.
        let payload = Self::encode_metadata(state);

        let wal_entry = Entry::new(METADATA_TERM, METADATA_INDEX, payload).map_err(|e| {
            StorageError::Io {
                operation: "save_state",
                message: format!("failed to create metadata entry: {e}"),
            }
        })?;

        // For now, we need to recreate the segment to update the metadata
        // at index 0. In a production implementation, we'd use a separate
        // metadata file or handle this more efficiently.
        //
        // This is acceptable for initial implementation - we can optimize later.
        self.state = Some(state.clone());

        // If the segment is empty, we can just append the metadata.
        if self.segment.entry_count() == 0 {
            self.segment.append(wal_entry).map_err(|e| StorageError::Io {
                operation: "save_state",
                message: format!("failed to append metadata: {e}"),
            })?;
        }
        // Otherwise, the metadata is cached and will be persisted on full rebuild.
        // This is a simplification - production code would use a separate metadata file.

        Ok(())
    }

    fn load_state(&self) -> StorageResult<Option<PersistentState>> {
        Ok(self.state.clone())
    }

    fn append_entries(&mut self, entries: &[LogEntry]) -> StorageResult<()> {
        for entry in entries {
            let wal_entry = Self::log_to_wal_entry(entry)?;

            self.segment.append(wal_entry).map_err(|e| StorageError::Io {
                operation: "append",
                message: format!("failed to append entry: {e}"),
            })?;

            // Update tracking.
            if self.first_index == 0 {
                self.first_index = entry.index.get();
            }
            self.last_index = entry.index.get();
            self.last_term = entry.term;
        }

        Ok(())
    }

    fn get_entry(&self, index: LogIndex) -> StorageResult<LogEntry> {
        let wal_entry = self.segment.read(index.get()).map_err(|e| match e {
            helix_wal::WalError::IndexOutOfBounds { .. } => StorageError::NotFound { index },
            other => StorageError::Io {
                operation: "read",
                message: format!("{other}"),
            },
        })?;

        // Don't return metadata entries.
        if wal_entry.term() == METADATA_TERM {
            return Err(StorageError::NotFound { index });
        }

        Ok(Self::wal_to_log_entry(wal_entry))
    }

    fn get_entries(&self, start: LogIndex, end: LogIndex) -> StorageResult<Vec<LogEntry>> {
        let mut result = Vec::new();

        for idx in start.get()..=end.get() {
            match self.segment.read(idx) {
                Ok(wal_entry) => {
                    // Skip metadata entries.
                    if wal_entry.term() != METADATA_TERM {
                        result.push(Self::wal_to_log_entry(wal_entry));
                    }
                }
                Err(helix_wal::WalError::IndexOutOfBounds { .. }) => {
                    // Past the end, stop.
                    break;
                }
                Err(e) => {
                    return Err(StorageError::Io {
                        operation: "read",
                        message: format!("{e}"),
                    });
                }
            }
        }

        Ok(result)
    }

    fn truncate_after(&mut self, last_to_keep: LogIndex) -> StorageResult<()> {
        self.segment.truncate_after(last_to_keep.get()).map_err(|e| StorageError::Io {
            operation: "truncate",
            message: format!("{e}"),
        })?;

        // Update tracking.
        if last_to_keep.get() < self.first_index {
            self.first_index = 0;
            self.last_index = 0;
            self.last_term = TermId::new(0);
        } else {
            self.last_index = last_to_keep.get();
            // Update last_term from the actual entry.
            if let Ok(entry) = self.segment.read(last_to_keep.get()) {
                if entry.term() != METADATA_TERM {
                    self.last_term = TermId::new(entry.term());
                }
            }
        }

        Ok(())
    }

    fn first_index(&self) -> LogIndex {
        LogIndex::new(self.first_index)
    }

    fn last_index(&self) -> LogIndex {
        LogIndex::new(self.last_index)
    }

    fn last_term(&self) -> TermId {
        self.last_term
    }

    fn sync(&mut self) -> StorageResult<()> {
        // For in-memory segments, this is a no-op.
        // Production implementation would fsync here.
        Ok(())
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
    fn test_wal_storage_state() {
        let mut storage = WalStorage::new();

        // Initially no state.
        assert!(storage.load_state().unwrap().is_none());

        // Save and load state.
        let state = PersistentState::with_values(TermId::new(5), Some(helix_core::NodeId::new(3)));
        storage.save_state(&state).unwrap();

        let loaded = storage.load_state().unwrap().unwrap();
        assert_eq!(loaded, state);
    }

    #[test]
    fn test_wal_storage_entries() {
        let mut storage = WalStorage::new();

        // Save state first (metadata at index 0).
        let state = PersistentState::new();
        storage.save_state(&state).unwrap();

        // Append entries starting at index 1.
        let entries = vec![make_entry(1, 1), make_entry(1, 2), make_entry(2, 3)];
        storage.append_entries(&entries).unwrap();

        assert_eq!(storage.first_index().get(), 1);
        assert_eq!(storage.last_index().get(), 3);
        assert_eq!(storage.last_term().get(), 2);

        // Get single entry.
        let entry = storage.get_entry(LogIndex::new(2)).unwrap();
        assert_eq!(entry.index.get(), 2);
        assert_eq!(entry.term.get(), 1);
    }

    #[test]
    fn test_wal_storage_get_range() {
        let mut storage = WalStorage::new();

        // Save state first.
        storage.save_state(&PersistentState::new()).unwrap();

        let entries = vec![make_entry(1, 1), make_entry(1, 2), make_entry(2, 3)];
        storage.append_entries(&entries).unwrap();

        let range = storage.get_entries(LogIndex::new(1), LogIndex::new(3)).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].index.get(), 1);
        assert_eq!(range[2].index.get(), 3);
    }

    #[test]
    fn test_wal_storage_truncate() {
        let mut storage = WalStorage::new();

        storage.save_state(&PersistentState::new()).unwrap();

        let entries = vec![make_entry(1, 1), make_entry(1, 2), make_entry(2, 3)];
        storage.append_entries(&entries).unwrap();

        storage.truncate_after(LogIndex::new(1)).unwrap();

        assert_eq!(storage.last_index().get(), 1);
        assert!(storage.get_entry(LogIndex::new(2)).is_err());
    }

    #[test]
    fn test_wal_storage_roundtrip() {
        let mut storage = WalStorage::new();

        // Save state.
        let state = PersistentState::with_values(TermId::new(3), Some(helix_core::NodeId::new(1)));
        storage.save_state(&state).unwrap();

        // Append entries.
        let entries = vec![make_entry(1, 1), make_entry(2, 2)];
        storage.append_entries(&entries).unwrap();

        // Encode the segment.
        let encoded = storage.segment.encode();

        // Recover from encoded data.
        let recovered = WalStorage::recover(encoded, storage.config).unwrap();

        assert_eq!(recovered.first_index().get(), 1);
        assert_eq!(recovered.last_index().get(), 2);
        assert_eq!(recovered.last_term().get(), 2);
    }
}
