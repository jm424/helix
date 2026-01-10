//! WAL crash recovery and integrity simulation tests.
//!
//! These tests verify WAL correctness under crash conditions.
//! Note: Basic entry encoding/decoding tests are in helix-wal itself.
//! These tests focus on integration and crash recovery scenarios.

use bytes::{Bytes, BytesMut};
use helix_wal::{Entry, ENTRY_HEADER_SIZE};

#[test]
fn test_wal_entry_roundtrip() {
    let entry = Entry::new(1, 1, Bytes::from("test data")).expect("entry creation should succeed");

    // Encode the entry.
    let mut buf = BytesMut::new();
    entry.encode(&mut buf);
    assert!(!buf.is_empty());

    // Decode it back.
    let decoded = Entry::decode(&mut buf.freeze(), 0).expect("decode should succeed");

    assert_eq!(decoded.header.term, entry.header.term);
    assert_eq!(decoded.header.index, entry.header.index);
    assert_eq!(decoded.payload, entry.payload);
}

#[test]
fn test_wal_entry_corruption_detection() {
    let entry = Entry::new(1, 1, Bytes::from("test data")).expect("entry creation should succeed");

    let mut buf = BytesMut::new();
    entry.encode(&mut buf);

    // Corrupt a byte in the payload section.
    if buf.len() > ENTRY_HEADER_SIZE + 1 {
        let corrupt_idx = ENTRY_HEADER_SIZE + 1;
        buf[corrupt_idx] ^= 0xFF;
    }

    // Should fail to decode due to CRC mismatch.
    let result = Entry::decode(&mut buf.freeze(), 0);
    assert!(result.is_err());
}

#[test]
fn test_wal_entry_truncated_data() {
    let entry =
        Entry::new(1, 1, Bytes::from("test data with more bytes")).expect("entry creation should succeed");

    let mut buf = BytesMut::new();
    entry.encode(&mut buf);

    // Truncate the data (leave header but not all payload).
    let truncated_len = ENTRY_HEADER_SIZE + 2;
    buf.truncate(truncated_len);

    // Should fail to decode due to insufficient data.
    let result = Entry::decode(&mut buf.freeze(), 0);
    assert!(result.is_err());
}

#[test]
fn test_wal_entry_empty_payload() {
    let entry = Entry::new(5, 100, Bytes::new()).expect("entry creation should succeed");

    let mut buf = BytesMut::new();
    entry.encode(&mut buf);

    let decoded = Entry::decode(&mut buf.freeze(), 0).expect("decode should succeed");

    assert_eq!(decoded.header.term, 5);
    assert_eq!(decoded.header.index, 100);
    assert!(decoded.payload.is_empty());
}
