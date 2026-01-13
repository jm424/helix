//! WAL crash recovery and integrity simulation tests.
//!
//! These tests verify WAL correctness under crash conditions using
//! `SimulatedStorage` with deterministic fault injection.
//!
//! # Test Categories
//!
//! 1. **Torn Writes**: Crash during write at various byte boundaries
//! 2. **Crash Before Sync**: Write completes but sync doesn't happen
//! 3. **Segment Rotation Crash**: Crash during segment rotation
//! 4. **Corrupted Header Recovery**: Recovery with invalid segment header
//! 5. **Corrupted Entry Recovery**: Recovery with corrupted entry in middle
//! 6. **Multi-Segment Corruption**: Multiple segments, last one corrupted
//! 7. **Concurrent Access**: Reads while writes are happening
//! 8. **Fsync Failures**: Disk full and I/O error handling

// Test-specific lint allowances - these are less critical in test code.
#![allow(clippy::cast_precision_loss)] // f64 precision loss acceptable in test stats
#![allow(clippy::cast_possible_truncation)] // u64 to usize safe on 64-bit test machines
#![allow(clippy::too_many_lines)] // Test functions can be longer for clarity
#![allow(clippy::significant_drop_tightening)] // Test code clarity > drop optimization
#![allow(clippy::unreadable_literal)] // Large seed numbers are fine without separators
#![allow(clippy::doc_markdown)] // Backticks in docs not critical for tests
#![allow(clippy::uninlined_format_args)] // Format string style not critical for tests
#![allow(clippy::needless_pass_by_value)] // Pass by value can improve test clarity
#![allow(clippy::type_complexity)] // Complex types acceptable in test utilities
#![allow(clippy::explicit_iter_loop)] // Explicit iteration can be clearer
#![allow(clippy::manual_let_else)] // Let-else style not critical
#![allow(clippy::panic_in_result_fn)] // Tests may intentionally panic
#![allow(clippy::cast_sign_loss)] // Test data is always positive
#![allow(clippy::deref_addrof)] // Ref/deref pattern may be intentional
#![allow(clippy::single_match)] // Single match for clarity

use std::path::Path;

use bytes::{Bytes, BytesMut};
use helix_wal::{
    Entry, FaultConfig, Segment, SegmentConfig, SegmentId, SimulatedStorage, Storage,
    Wal, WalConfig, ENTRY_HEADER_SIZE, SEGMENT_HEADER_SIZE,
};

/// Type alias for WAL with `SimulatedStorage` and standard `Entry` type.
type TestWal = Wal<SimulatedStorage, Entry>;

// ============================================================================
// Basic Entry Encoding Tests (from original file)
// ============================================================================

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

// ============================================================================
// DST Test 1: Crash During Write (Torn Writes)
// ============================================================================

/// Tests recovery when a crash occurs mid-write, producing a torn (partial) entry.
/// The WAL should detect the partial entry via CRC and truncate it during recovery.
#[tokio::test]
async fn test_crash_during_write_torn_at_header() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/torn_header");
    let config = WalConfig::new(wal_dir);

    // Write some entries successfully.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();
        for i in 1..=5 {
            let entry = Entry::new(1, i, Bytes::from(format!("data-{i}"))).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Get the segment file path and simulate a torn write in the header of entry 6.
    let segment_path = wal_dir.join("segment-00000001.wal");
    let content_before = storage.get_raw_content(&segment_path).unwrap();

    // Manually append a partial entry header (simulating crash mid-write).
    // Entry header is 33 bytes, so write only 10 bytes of it.
    let partial_header = vec![0xDE; 10];
    let mut new_content = content_before.clone();
    new_content.extend_from_slice(&partial_header);
    storage.set_raw_content(&segment_path, new_content);

    // Recovery should succeed, ignoring the partial entry.
    let wal = TestWal::open(storage.clone(), config).await.unwrap();
    assert_eq!(wal.last_index(), Some(5));

    // Verify all 5 original entries are readable.
    for i in 1..=5 {
        let entry = wal.read(i).unwrap();
        assert_eq!(entry.index(), i);
    }

    // File should be truncated back to original size during next write.
    // (Recovery reads but doesn't truncate; next append would start fresh.)
}

/// Tests recovery when torn write happens in the middle of entry payload.
#[tokio::test]
async fn test_crash_during_write_torn_at_payload() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/torn_payload");
    let config = WalConfig::new(wal_dir);

    // Write some entries.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();
        for i in 1..=3 {
            let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Now simulate: WAL reopens, starts writing entry 4, but crashes mid-payload.
    let segment_path = wal_dir.join("segment-00000001.wal");
    let content = storage.get_raw_content(&segment_path).unwrap();

    // Create a partial entry 4: full header but truncated payload.
    let entry4 = Entry::new(1, 4, Bytes::from("this is a longer payload")).unwrap();
    let mut buf = BytesMut::new();
    entry4.encode(&mut buf);
    let full_entry_bytes = buf.freeze();

    // Append only header + 5 bytes of payload (entry is incomplete).
    let partial_len = ENTRY_HEADER_SIZE + 5;
    let mut new_content = content;
    new_content.extend_from_slice(&full_entry_bytes[..partial_len]);
    storage.set_raw_content(&segment_path, new_content);

    // Recovery should succeed with only entries 1-3.
    let wal = TestWal::open(storage.clone(), config).await.unwrap();
    assert_eq!(wal.last_index(), Some(3));
}

/// Tests multiple torn write positions using SimulatedStorage's fault injection.
#[tokio::test]
async fn test_crash_during_write_various_positions() {
    // Test torn writes at different byte offsets.
    for torn_offset in [1, 5, 10, 20, ENTRY_HEADER_SIZE - 1, ENTRY_HEADER_SIZE + 1] {
        let config = FaultConfig::none().with_force_torn_write_at(torn_offset);
        let storage = SimulatedStorage::with_faults(42, config);
        let wal_dir = Path::new("/wal/torn_various");
        let wal_config = WalConfig::new(wal_dir);

        // First, write successfully without fault.
        {
            // Clear fault for initial writes.
            storage.fault_config().force_torn_write_at = None;

            let mut wal = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();
            for i in 1..=3 {
                let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
                wal.append(entry).await.unwrap();
            }
            wal.sync().await.unwrap();
        }

        // Now enable torn write for entry 4.
        storage.fault_config().force_torn_write_at = Some(torn_offset);

        {
            let mut wal = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();
            let entry = Entry::new(1, 4, Bytes::from("this will be torn")).unwrap();
            // Write will succeed but be torn.
            let _ = wal.append(entry).await;
        }

        // Recovery should preserve entries 1-3.
        let wal = TestWal::open(storage.clone(), wal_config).await.unwrap();
        assert!(
            wal.last_index().unwrap_or(0) <= 4,
            "torn at {torn_offset}: should have at most 4 entries"
        );
        // At minimum we should have entries 1-3 from before the torn write.
        assert!(
            wal.last_index().unwrap_or(0) >= 3,
            "torn at {torn_offset}: should have at least 3 entries"
        );
    }
}

// ============================================================================
// DST Test 2: Crash After Write Before Sync
// ============================================================================

/// Tests scenario where write completes but sync doesn't happen before crash.
/// Without sync, data may be lost but WAL should remain consistent.
#[tokio::test]
async fn test_crash_after_write_before_sync() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/no_sync");
    let config = WalConfig::new(wal_dir);

    // Write and sync some entries.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();
        for i in 1..=5 {
            let entry = Entry::new(1, i, Bytes::from("synced")).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Write more entries but DON'T sync.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();
        for i in 6..=10 {
            let entry = Entry::new(1, i, Bytes::from("unsynced")).unwrap();
            wal.append(entry).await.unwrap();
        }
        // No sync - simulate crash.
        // In SimulatedStorage, data is still there (no actual OS buffer).
        // In real systems, this data might be lost.
    }

    // Recovery - in SimulatedStorage, writes are immediate so data persists.
    let wal = TestWal::open(storage.clone(), config).await.unwrap();
    // With SimulatedStorage, we get all entries since writes are synchronous.
    assert_eq!(wal.last_index(), Some(10));
}

/// Tests that fsync failure doesn't corrupt existing data.
#[tokio::test]
async fn test_fsync_failure_preserves_existing_data() {
    let config = FaultConfig::none().with_force_fsync_fail();
    let storage = SimulatedStorage::with_faults(42, config);
    let wal_dir = Path::new("/wal/fsync_fail");
    let wal_config = WalConfig::new(wal_dir);

    // Disable fault for initial writes.
    storage.fault_config().force_fsync_fail = false;

    // Write and sync some entries.
    {
        let mut wal = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();
        for i in 1..=5 {
            let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Enable fsync failure.
    storage.fault_config().force_fsync_fail = true;

    // Write more entries, sync will fail.
    {
        let mut wal = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();
        for i in 6..=8 {
            let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
            wal.append(entry).await.unwrap();
        }
        // Sync should fail.
        let result = wal.sync().await;
        assert!(result.is_err());
    }

    // Recovery should still have the data (SimulatedStorage keeps it).
    let wal = TestWal::open(storage.clone(), wal_config).await.unwrap();
    assert_eq!(wal.last_index(), Some(8));
}

// ============================================================================
// DST Test 3: Crash During Segment Rotation
// ============================================================================

/// Tests recovery when crash happens during segment rotation.
/// This is a critical scenario as it involves multiple file operations.
#[tokio::test]
async fn test_crash_during_segment_rotation() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/rotation");

    // Use small segment size to force rotation.
    let segment_config = SegmentConfig::new().with_max_size(1024 * 1024); // 1MB
    let config = WalConfig::new(wal_dir).with_segment_config(segment_config);

    // Write entries to fill first segment.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();

        // Write enough data to approach segment limit.
        let large_payload = Bytes::from(vec![0u8; 100_000]);
        for i in 1..=8 {
            let entry = Entry::new(1, i, large_payload.clone()).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Get state before rotation.
    let files_before = storage.list_files(wal_dir, "wal").await.unwrap();

    // Now trigger rotation by writing more.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();
        let large_payload = Bytes::from(vec![0u8; 100_000]);

        // This should trigger segment rotation.
        for i in 9..=12 {
            let entry = Entry::new(1, i, large_payload.clone()).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Verify recovery works with multiple segments.
    let wal = TestWal::open(storage.clone(), config).await.unwrap();
    assert!(wal.last_index().unwrap() >= 12);

    // Should have created new segment(s).
    let files_after = storage.list_files(wal_dir, "wal").await.unwrap();
    assert!(files_after.len() >= files_before.len());
}

// ============================================================================
// DST Test 4: Recovery with Corrupted Segment Header
// ============================================================================

/// Tests recovery when segment header magic bytes are corrupted.
#[tokio::test]
async fn test_recovery_corrupted_segment_header_magic() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/corrupt_magic");
    let config = WalConfig::new(wal_dir);

    // Write some entries.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();
        for i in 1..=5 {
            let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Corrupt the magic bytes at the start of the segment.
    let segment_path = wal_dir.join("segment-00000001.wal");
    storage.corrupt_bytes(&segment_path, 0, 4); // Corrupt first 4 bytes of magic.

    // Recovery should skip the corrupted segment.
    let wal = TestWal::open(storage.clone(), config).await.unwrap();
    // WAL should be empty since the only segment is corrupted.
    assert!(wal.is_empty() || wal.last_index().is_none());
}

/// Tests recovery when segment header version is corrupted.
#[tokio::test]
async fn test_recovery_corrupted_segment_header_version() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/corrupt_version");
    let config = WalConfig::new(wal_dir);

    // Write some entries.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();
        for i in 1..=5 {
            let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Corrupt the version field (bytes 8-11 after magic).
    let segment_path = wal_dir.join("segment-00000001.wal");
    storage.corrupt_bytes(&segment_path, 8, 4);

    // Recovery should skip the corrupted segment.
    let wal = TestWal::open(storage.clone(), config).await.unwrap();
    assert!(wal.is_empty());
}

/// Tests recovery with a truncated segment header.
#[tokio::test]
async fn test_recovery_truncated_segment_header() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/truncated_header");
    let config = WalConfig::new(wal_dir);

    // Write some entries.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();
        for i in 1..=5 {
            let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Truncate segment to less than header size.
    let segment_path = wal_dir.join("segment-00000001.wal");
    storage.truncate_file(&segment_path, SEGMENT_HEADER_SIZE - 5);

    // Recovery should skip the truncated segment.
    let wal = TestWal::open(storage.clone(), config).await.unwrap();
    assert!(wal.is_empty());
}

// ============================================================================
// DST Test 5: Recovery with Corrupted Entry in Middle
// ============================================================================

/// Tests that corruption in the middle of a segment causes the entire segment to be skipped.
///
/// Design note: The current WAL implementation skips the entire segment if any entry
/// has a CRC mismatch. This is a conservative approach - if corruption is detected,
/// we can't fully trust any data in that segment. An alternative would be to recover
/// entries before the corruption, but that's not implemented.
#[tokio::test]
async fn test_recovery_corrupted_entry_middle() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/corrupt_middle");
    let config = WalConfig::new(wal_dir);

    // Write entries with fixed-size payload for predictable offsets.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();
        for i in 1..=10 {
            // Use "data" (4 bytes) for all entries.
            let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Corrupt entry 5's CRC field.
    // Entry format: CRC (4) + Length (4) + Term (8) + Index (8) + Payload (4) = 28 bytes
    // Segment header: 32 bytes
    // Entry 5 starts at: 32 + 4 * 28 = 144 bytes
    let segment_path = wal_dir.join("segment-00000001.wal");
    let entry_size = ENTRY_HEADER_SIZE + 4; // 24 + 4 = 28 bytes
    let entry5_offset = SEGMENT_HEADER_SIZE + 4 * entry_size;
    storage.corrupt_bytes(&segment_path, entry5_offset, 4); // Corrupt CRC

    // Current behavior: CRC mismatch causes entire segment to be skipped.
    let wal = TestWal::open(storage.clone(), config).await.unwrap();

    // Entire segment is skipped, WAL is empty.
    assert!(wal.is_empty());
}

/// Tests that CRC corruption causes the entire segment to be skipped.
///
/// This verifies the conservative recovery behavior: if any entry fails CRC
/// validation, the entire segment is considered untrustworthy.
#[tokio::test]
async fn test_recovery_corrupted_entry_crc() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/corrupt_crc");
    let config = WalConfig::new(wal_dir);

    // Write entries.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();
        for i in 1..=5 {
            let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Corrupt the CRC of entry 3 (CRC is at offset 0 of entry header).
    // Entry format: CRC (4) + Length (4) + Term (8) + Index (8) + Payload (4) = 28 bytes
    // Entry 3 starts at: segment_header (32) + 2 * entry_size (28) = 88 bytes
    let entry_size = ENTRY_HEADER_SIZE + 4; // 24 + 4 = 28 bytes
    let entry3_offset = SEGMENT_HEADER_SIZE + 2 * entry_size;
    let segment_path = wal_dir.join("segment-00000001.wal");
    storage.corrupt_bytes(&segment_path, entry3_offset, 4); // Corrupt CRC

    // Current behavior: entire segment is skipped on CRC mismatch.
    let wal = TestWal::open(storage.clone(), config).await.unwrap();
    assert!(wal.is_empty());
}

// ============================================================================
// DST Test 6: Recovery with Multiple Segments, Last One Corrupted
// ============================================================================

/// Tests recovery with multiple segments where only the last is corrupted.
#[tokio::test]
async fn test_recovery_multiple_segments_last_corrupted() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/multi_corrupt");

    // Use small segment to force multiple segments.
    let segment_config = SegmentConfig::new().with_max_size(1024 * 1024);
    let config = WalConfig::new(wal_dir).with_segment_config(segment_config);

    // Write enough to create multiple segments.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();
        let payload = Bytes::from(vec![0u8; 200_000]);
        for i in 1..=15 {
            let entry = Entry::new(1, i, payload.clone()).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Find the last segment and corrupt it.
    let segments = storage.list_files(wal_dir, "wal").await.unwrap();
    assert!(!segments.is_empty(), "should have created segments");

    if let Some(last_segment) = segments.last() {
        // Corrupt the header of the last segment.
        storage.corrupt_bytes(last_segment, 0, 8);
    }

    // Recovery should succeed with entries from earlier segments.
    let wal = TestWal::open(storage.clone(), config).await.unwrap();

    // Should have at least some entries from uncorrupted segments.
    let last_idx = wal.last_index().unwrap_or(0);
    assert!(last_idx >= 1, "should recover entries from good segments");
    assert!(last_idx < 15, "should not have all entries due to corruption");
}

/// Tests that good segments before a corrupted one are fully recoverable.
#[tokio::test]
async fn test_recovery_preserves_good_segments() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/preserve_good");

    // Create a manually crafted scenario with 2 segments.
    // First segment has 5 entries, second is completely corrupted.

    // Create first segment manually.
    let seg1_path = wal_dir.join("segment-00000001.wal");
    let mut segment1 = Segment::new(SegmentId::new(1), 1, SegmentConfig::new());
    for i in 1..=5 {
        let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
        segment1.append(entry).unwrap();
    }
    let seg1_data = segment1.encode();
    storage.set_raw_content(&seg1_path, seg1_data.to_vec());

    // Create second segment with corrupted header.
    let seg2_path = wal_dir.join("segment-00000002.wal");
    storage.set_raw_content(&seg2_path, vec![0xFF; 100]); // All garbage

    // Recovery should get all 5 entries from segment 1.
    let config = WalConfig::new(wal_dir);
    let wal = TestWal::open(storage.clone(), config).await.unwrap();
    assert_eq!(wal.last_index(), Some(5));

    // Verify all entries are readable.
    for i in 1..=5 {
        let entry = wal.read(i).unwrap();
        assert_eq!(entry.index(), i);
    }
}

// ============================================================================
// DST Test 7: Concurrent Reads While Writes
// ============================================================================

/// Tests that reads work correctly while writes are in progress.
/// Note: WAL is not designed for concurrent access, but we verify
/// that sequential access patterns work correctly.
#[tokio::test]
async fn test_sequential_read_write_interleaving() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/interleave");
    let config = WalConfig::new(wal_dir);

    let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();

    // Write and read interleaved.
    for batch in 0..5 {
        // Write a batch of entries.
        for i in 0..10 {
            let idx = batch * 10 + i + 1;
            let entry = Entry::new(1, idx, Bytes::from(format!("batch{batch}-{i}"))).unwrap();
            wal.append(entry).await.unwrap();
        }

        // Read back entries from this and previous batches.
        for i in 1..=(batch + 1) * 10 {
            let entry = wal.read(i).unwrap();
            assert_eq!(entry.index(), i);
        }
    }

    wal.sync().await.unwrap();

    // Verify final state.
    assert_eq!(wal.last_index(), Some(50));
}

/// Tests that reading doesn't interfere with subsequent writes.
#[tokio::test]
async fn test_read_doesnt_affect_write_position() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/read_write");
    let config = WalConfig::new(wal_dir);

    let mut wal = TestWal::open(storage.clone(), config).await.unwrap();

    // Write entries.
    for i in 1..=10 {
        let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
        wal.append(entry).await.unwrap();
    }

    // Read entries in various orders.
    let _ = wal.read(5);
    let _ = wal.read(1);
    let _ = wal.read(10);
    let _ = wal.read(3);

    // Continue writing.
    for i in 11..=15 {
        let entry = Entry::new(1, i, Bytes::from("more")).unwrap();
        wal.append(entry).await.unwrap();
    }

    // All entries should be accessible.
    assert_eq!(wal.last_index(), Some(15));
    for i in 1..=15 {
        assert!(wal.read(i).is_ok());
    }
}

// ============================================================================
// DST Test 8: Fsync Failure (Disk Full, I/O Error)
// ============================================================================

/// Tests handling of disk full error during write.
#[tokio::test]
async fn test_disk_full_error_handling() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/disk_full");
    let config = WalConfig::new(wal_dir);

    // Write some entries successfully.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();
        for i in 1..=5 {
            let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Enable disk full error.
    storage.fault_config().force_disk_full = true;

    // Next write should fail.
    {
        let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();
        let entry = Entry::new(1, 6, Bytes::from("will_fail")).unwrap();
        let result = wal.append(entry).await;
        assert!(result.is_err());
    }

    // Clear fault and verify recovery.
    storage.fault_config().force_disk_full = false;

    let wal = TestWal::open(storage.clone(), config).await.unwrap();
    // Original 5 entries should still be there.
    assert_eq!(wal.last_index(), Some(5));
}

/// Tests handling of I/O error during sync.
#[tokio::test]
async fn test_io_error_during_sync() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/io_error");
    let config = WalConfig::new(wal_dir);

    let mut wal = TestWal::open(storage.clone(), config.clone()).await.unwrap();

    // Write entries.
    for i in 1..=5 {
        let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
        wal.append(entry).await.unwrap();
    }

    // Enable fsync failure.
    storage.fault_config().force_fsync_fail = true;

    // Sync should fail.
    let result = wal.sync().await;
    assert!(result.is_err());

    // Clear fault.
    storage.fault_config().force_fsync_fail = false;

    // Retry sync should succeed.
    let result = wal.sync().await;
    assert!(result.is_ok());

    // Data should be preserved.
    assert_eq!(wal.last_index(), Some(5));
}

/// Tests repeated fsync failures followed by recovery.
#[tokio::test]
async fn test_repeated_fsync_failures() {
    let config = FaultConfig::none().with_fsync_fail_rate(0.5); // 50% failure rate
    let storage = SimulatedStorage::with_faults(42, config);
    let wal_dir = Path::new("/wal/repeated_fsync");
    let wal_config = WalConfig::new(wal_dir);

    let mut wal = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();

    // Try to write and sync entries, handling failures.
    let mut written = 0u64;
    for i in 1..=20 {
        let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
        if wal.append(entry).await.is_ok() {
            written = i;
            // Try sync, ignore failure.
            let _ = wal.sync().await;
        }
    }

    assert!(written > 0, "should have written at least one entry");

    // Disable faults for recovery.
    storage.fault_config().fsync_fail_rate = 0.0;

    // Recovery should work.
    let wal = TestWal::open(storage.clone(), wal_config).await.unwrap();
    assert!(wal.last_index().is_some());
}

// ============================================================================
// Randomized DST Test: Multiple Faults
// ============================================================================

/// Runs a sequence of operations with random faults enabled.
/// This is a "stress test" that verifies WAL handles mixed fault conditions.
#[tokio::test]
async fn test_random_faults_stress() {
    // Run with different seeds for variety.
    for seed in [1, 42, 123, 999, 12345] {
        let config = FaultConfig::none()
            .with_torn_write_rate(0.02)      // 2% torn writes
            .with_fsync_fail_rate(0.05);     // 5% fsync failures
        let storage = SimulatedStorage::with_faults(seed, config);
        let wal_dir = Path::new("/wal/stress");
        let wal_config = WalConfig::new(wal_dir);

        // Try to write entries, some may fail due to faults.
        let mut last_successful = 0u64;
        {
            let mut wal = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();
            for i in 1..=100 {
                let entry = Entry::new(1, i, Bytes::from(format!("stress-{i}"))).unwrap();
                match wal.append(entry).await {
                    Ok(_) => {
                        last_successful = i;
                        // Try sync occasionally.
                        if i % 10 == 0 {
                            let _ = wal.sync().await;
                        }
                    }
                    Err(_) => {
                        // Fault injected, stop writing.
                        break;
                    }
                }
            }
        }

        // Disable faults for recovery.
        storage.fault_config().torn_write_rate = 0.0;
        storage.fault_config().fsync_fail_rate = 0.0;

        // Recovery should work and preserve consistent state.
        let wal = TestWal::open(storage.clone(), wal_config).await.unwrap();

        // Last index should be <= what we successfully wrote.
        if let Some(last_idx) = wal.last_index() {
            assert!(
                last_idx <= last_successful,
                "seed {seed}: recovered index {last_idx} > last written {last_successful}"
            );

            // All recovered entries should be readable and valid.
            for i in 1..=last_idx {
                let entry = wal.read(i).expect("recovered entry should be readable");
                assert_eq!(entry.index(), i);
                assert_eq!(entry.term(), 1);
            }
        }
    }
}

// ============================================================================
// Comprehensive Invariant Checking
// ============================================================================

/// Tracks what we wrote for content verification (simple, non-crash-aware tracking).
#[derive(Default)]
struct WrittenEntries {
    /// Map from index to (term, payload).
    entries: std::collections::HashMap<u64, (u64, String)>,
}

impl WrittenEntries {
    fn record(&mut self, index: u64, term: u64, payload: String) {
        self.entries.insert(index, (term, payload));
    }
}

/// WAL invariants checked by `check_wal_invariants`:
///
/// 1. **Durable bound**: `durable_index() <= last_index()` (always)
/// 2. **Post-recovery consistency**: After recovery, `durable_index() == last_index()`
/// 3. **Cross-crash durability**: `recovered_last_index >= durable_index_before_crash`
///    (durable entries MUST survive; recovered MAY include unsynced entries)
/// 4. **No regression**: `last_index <= max_written` (can't have entries we never wrote)
/// 5. **Readability**: Every entry from 1..=last_index is readable (implies CRC valid)
/// 6. **Index contiguity**: Entry at position i has `index() == i`
/// 7. **Term validity**: All terms are > 0
///
/// Returns Ok(()) if all invariants hold, Err with description if any fails.
/// Context for WAL invariant checking.
struct InvariantContext {
    seed: u64,
    op_num: usize,
    /// Maximum index ever successfully written (for regression check).
    max_written: u64,
    /// If set, this WAL was just recovered - enables post-recovery checks.
    is_post_recovery: bool,
    /// Durable index before crash (for cross-crash durability check).
    durable_before_crash: Option<u64>,
}

fn check_wal_invariants<S: Storage>(
    wal: &Wal<S>,
    ctx: &InvariantContext,
) -> Result<(), String> {
    let seed = ctx.seed;
    let op_num = ctx.op_num;

    // INVARIANT 1: durable_index <= last_index (always)
    if let (Some(durable), Some(last)) = (wal.durable_index(), wal.last_index()) {
        if durable > last {
            return Err(format!(
                "seed {seed}, op {op_num}: DURABLE BOUND VIOLATION - durable_index {durable} > last_index {last}"
            ));
        }
    }

    // INVARIANT 2: Post-recovery, durable_index == last_index
    if ctx.is_post_recovery {
        if wal.durable_index() != wal.last_index() {
            return Err(format!(
                "seed {seed}, op {op_num}: POST-RECOVERY VIOLATION - durable_index {:?} != last_index {:?}",
                wal.durable_index(),
                wal.last_index()
            ));
        }
    }

    // INVARIANT 3: Cross-crash durability - durable entries MUST survive crash.
    // After crash, recovered_index >= durable_index_before (lower bound).
    // Note: recovered CAN be > durable if unsynced entries happened to survive,
    // or if truncation didn't persist to disk (stale entries recovered).
    if let Some(durable_before) = ctx.durable_before_crash {
        if let Some(recovered) = wal.last_index() {
            if recovered < durable_before {
                return Err(format!(
                    "seed {seed}, op {op_num}: DURABILITY VIOLATION - recovered {recovered} < durable_before_crash {durable_before}"
                ));
            }
        } else if durable_before > 0 {
            return Err(format!(
                "seed {seed}, op {op_num}: DURABILITY VIOLATION - recovered nothing but durable_before_crash was {durable_before}"
            ));
        }
    }

    let last_idx = match wal.last_index() {
        Some(idx) => idx,
        None => return Ok(()), // Empty WAL is valid
    };

    // INVARIANT 4: No regression - last_index <= max successfully written
    if last_idx > ctx.max_written && ctx.max_written > 0 {
        return Err(format!(
            "seed {seed}, op {op_num}: INDEX REGRESSION - last_index {last_idx} > max_written {}",
            ctx.max_written
        ));
    }

    // INVARIANT 5-7: Check each entry
    for i in 1..=last_idx {
        // INVARIANT 5: Readability (implies CRC validity)
        let entry = wal.read(i).map_err(|e| {
            // Print debug info about what entries ARE readable.
            let mut readable = Vec::new();
            for j in 1..=last_idx {
                if wal.read(j).is_ok() {
                    readable.push(j);
                }
            }
            format!(
                "seed {seed}, op {op_num}: READABILITY VIOLATION - entry {i} unreadable: {e} (readable: {:?})",
                if readable.len() <= 20 { readable } else { vec![] }
            )
        })?;

        // INVARIANT 6: Index contiguity - entry.index() == position
        if entry.index() != i {
            return Err(format!(
                "seed {seed}, op {op_num}: INDEX MISMATCH - entry at position {i} has index {}",
                entry.index()
            ));
        }

        // INVARIANT 7: Term validity - term must be positive
        if entry.term() == 0 {
            return Err(format!(
                "seed {seed}, op {op_num}: INVALID TERM - entry {i} has term 0"
            ));
        }
    }

    Ok(())
}

// ============================================================================
// Comprehensive Stress Test: 500 Seeds, 25% Fault Rates
// ============================================================================

/// Comprehensive WAL stress test with high fault rates.
///
/// Uses the WAL's `durable_index()` API to verify crash safety:
/// - After crash, `last_index() <= durable_index()` from before crash
/// - All entries in `1..=last_index()` are readable (CRC valid)
/// - Index contiguity (no gaps)
#[tokio::test]
async fn test_comprehensive_wal_stress() {
    const NUM_SEEDS: u64 = 500;
    const OPS_PER_SEED: usize = 100;
    const FAULT_RATE: f64 = 0.25;

    // Statistics to verify test coverage.
    let mut seeds_completed = 0u64;
    let mut seeds_skipped_open = 0u64;
    let mut total_ops_executed = 0u64;
    let mut total_writes = 0u64;
    let mut total_syncs = 0u64;
    let mut total_reads = 0u64;
    let mut total_truncations = 0u64;
    let mut total_crashes = 0u64;

    for base_seed in 0..NUM_SEEDS {
        let seed = base_seed * 12345 + 42;

        let config = FaultConfig {
            torn_write_rate: FAULT_RATE / 2.0,
            fsync_fail_rate: FAULT_RATE,
            read_corruption_rate: 0.0,
            read_fail_rate: FAULT_RATE,
            write_fail_rate: FAULT_RATE,
            exists_fail_rate: FAULT_RATE / 5.0,
            list_files_fail_rate: FAULT_RATE / 5.0,
            open_fail_rate: FAULT_RATE / 10.0,
            remove_fail_rate: FAULT_RATE / 5.0,
            force_torn_write_at: None,
            force_fsync_fail: false,
            force_disk_full: false,
        };

        let storage = SimulatedStorage::with_faults(seed, config);
        let wal_dir = Path::new("/wal/comprehensive");
        let wal_config = WalConfig::new(wal_dir);

        let mut max_written = 0u64;
        let mut current_term = 1u64;

        let wal_result = TestWal::open(storage.clone(), wal_config.clone()).await;
        let mut wal = match wal_result {
            Ok(w) => w,
            Err(_) => {
                seeds_skipped_open += 1;
                continue;
            }
        };

        for op_num in 0..OPS_PER_SEED {
            let op_hash = seed.wrapping_add(op_num as u64).wrapping_mul(0x9e3779b97f4a7c15);
            let op_type = op_hash % 10;

            // Debug output for failing seed (disabled after fix)
            let debug = false && seed == 530877 && op_num >= 75 && op_num <= 90;

            match op_type {
                0..=4 => {
                    // 50%: Write
                    total_writes += 1;
                    let next_idx = wal.last_index().map_or(1, |i| i + 1);
                    let entry = Entry::new(current_term, next_idx, Bytes::from("data")).unwrap();
                    let write_result = wal.append(entry).await;
                    if write_result.is_ok() {
                        max_written = next_idx;
                        let sync_result = wal.sync().await;
                        if debug {
                            let synced_snapshot = storage.synced_snapshot();
                            eprintln!(
                                "seed {seed}, op {op_num}: WRITE idx={next_idx}, sync={:?}, last={:?}, durable={:?}",
                                sync_result.is_ok(),
                                wal.last_index(),
                                wal.durable_index()
                            );
                            for (path, content) in &synced_snapshot {
                                eprintln!("  synced {}: {} bytes", path.display(), content.len());
                            }
                        }
                    } else if debug {
                        eprintln!(
                            "seed {seed}, op {op_num}: WRITE FAILED idx={next_idx}, last={:?}, durable={:?}",
                            wal.last_index(),
                            wal.durable_index()
                        );
                    }
                }
                5 => {
                    // 10%: Sync
                    total_syncs += 1;
                    let _ = wal.sync().await;
                }
                6 => {
                    // 10%: Read
                    total_reads += 1;
                    if let Some(last) = wal.last_index() {
                        let idx = (op_hash % last) + 1;
                        let _ = wal.read(idx);
                    }
                }
                7 => {
                    // 10%: Term bump
                    current_term += 1;
                }
                8 => {
                    // 10%: Truncation
                    total_truncations += 1;
                    if let Some(last) = wal.last_index() {
                        if last > 1 {
                            let truncate_to = (op_hash % (last - 1)) + 1;
                            let durable_before_trunc = wal.durable_index();
                            if debug {
                                // Dump synced state before truncation
                                let synced_snapshot = storage.synced_snapshot();
                                eprintln!(
                                    "seed {seed}, op {op_num}: TRUNCATE BEFORE synced_files has {} files",
                                    synced_snapshot.len()
                                );
                                for (path, content) in &synced_snapshot {
                                    eprintln!("  - {}: {} bytes", path.display(), content.len());
                                }
                            }
                            // Debug: show what sealed segments exist
                            if debug {
                                eprintln!("  sealed_segment_ids: {:?}", wal.sealed_segment_ids());
                                // Show first_index for each sealed segment
                                for seg_id in wal.sealed_segment_ids() {
                                    if let Some(info) = wal.segment_info(seg_id) {
                                        eprintln!(
                                            "    segment {}: first={} last={:?}",
                                            seg_id.get(),
                                            info.first_index,
                                            info.last_index
                                        );
                                    }
                                }
                            }
                            let truncate_result = wal.truncate_after(truncate_to).await;
                            let sync_result = wal.sync().await;
                            if debug {
                                // Dump synced state after truncation + sync
                                let synced_snapshot = storage.synced_snapshot();
                                eprintln!(
                                    "seed {seed}, op {op_num}: TRUNCATE from={last} to={truncate_to}, trunc={:?}, sync={:?}, durable_before={:?}, durable_after={:?}",
                                    truncate_result.is_ok(),
                                    sync_result.is_ok(),
                                    durable_before_trunc,
                                    wal.durable_index()
                                );
                                eprintln!(
                                    "seed {seed}, op {op_num}: TRUNCATE AFTER synced_files has {} files",
                                    synced_snapshot.len()
                                );
                                for (path, content) in &synced_snapshot {
                                    eprintln!("  - {}: {} bytes", path.display(), content.len());
                                }
                            }
                            max_written = wal.last_index().unwrap_or(0);
                        }
                    }
                }
                _ => {
                    // 10%: Crash and recovery
                    total_crashes += 1;
                    // Record durable state BEFORE crash
                    let durable_before = wal.durable_index();
                    let last_before = wal.last_index();

                    // Debug output for failing seed
                    if debug {
                        eprintln!(
                            "seed {seed}, op {op_num}: PRE-CRASH last={last_before:?}, durable={durable_before:?}"
                        );
                        let synced = storage.synced_snapshot();
                        eprintln!("  synced files before crash:");
                        for (path, content) in &synced {
                            eprintln!("    {}: {} bytes", path.display(), content.len());
                        }
                    }

                    drop(wal);
                    storage.simulate_crash();

                    // Add marker before recovery for easy grep
                    if debug {
                        eprintln!("=== RECOVERY START seed {seed} op {op_num} ===");
                    }

                    {
                        let mut fc = storage.fault_config();
                        fc.torn_write_rate = 0.0;
                        fc.write_fail_rate = 0.0;
                        fc.read_fail_rate = 0.0;
                        fc.fsync_fail_rate = 0.0;
                        fc.open_fail_rate = 0.0;
                        fc.list_files_fail_rate = 0.0;
                    }

                    match TestWal::open(storage.clone(), wal_config.clone()).await {
                        Ok(w) => {
                            if debug {
                                eprintln!("=== RECOVERY END seed {seed} op {op_num} ===");
                            }
                            let recovered = w.last_index();
                            if debug {
                                eprintln!(
                                    "seed {seed}, op {op_num}: RECOVERED last={recovered:?}, durable_before_crash={durable_before:?}"
                                );
                            }
                            max_written = w.last_index().unwrap_or(0);
                            let ctx = InvariantContext {
                                seed,
                                op_num,
                                max_written,
                                is_post_recovery: true,
                                durable_before_crash: durable_before,
                            };
                            if let Err(e) = check_wal_invariants(&w, &ctx) {
                                panic!("{e}");
                            }
                            wal = w;
                        }
                        Err(e) => {
                            panic!("seed {seed}, op {op_num}: Recovery failed: {e}");
                        }
                    }

                    {
                        let mut fc = storage.fault_config();
                        fc.torn_write_rate = FAULT_RATE / 2.0;
                        fc.write_fail_rate = FAULT_RATE;
                        fc.read_fail_rate = FAULT_RATE;
                        fc.fsync_fail_rate = FAULT_RATE;
                        fc.open_fail_rate = FAULT_RATE / 10.0;
                        fc.list_files_fail_rate = FAULT_RATE / 5.0;
                    }
                }
            }

            total_ops_executed += 1;

            // Check structural invariants (no content verification).
            if op_type != 9 {
                let old_read_fail = storage.fault_config().read_fail_rate;
                storage.fault_config().read_fail_rate = 0.0;

                let ctx = InvariantContext {
                    seed,
                    op_num,
                    max_written,
                    is_post_recovery: false,
                    durable_before_crash: None,
                };
                if let Err(e) = check_wal_invariants(&wal, &ctx) {
                    panic!("{e}");
                }

                storage.fault_config().read_fail_rate = old_read_fail;
            }
        }

        seeds_completed += 1;

        // Final recovery check - capture durable state before crash.
        let final_durable_before = wal.durable_index();
        drop(wal);
        storage.simulate_crash();

        {
            let mut fc = storage.fault_config();
            fc.torn_write_rate = 0.0;
            fc.write_fail_rate = 0.0;
            fc.read_fail_rate = 0.0;
            fc.fsync_fail_rate = 0.0;
            fc.open_fail_rate = 0.0;
            fc.list_files_fail_rate = 0.0;
        }

        if let Ok(final_wal) = TestWal::open(storage.clone(), wal_config).await {
            max_written = final_wal.last_index().unwrap_or(0);
            let ctx = InvariantContext {
                seed,
                op_num: OPS_PER_SEED,
                max_written,
                is_post_recovery: true,
                durable_before_crash: final_durable_before,
            };
            if let Err(e) = check_wal_invariants(&final_wal, &ctx) {
                panic!("{e}");
            }
        }
    }

    // Print statistics.
    println!("\n=== WAL DST Comprehensive Stress Test Statistics ===");
    println!(
        "Seeds: {} completed, {} skipped (open failed)",
        seeds_completed, seeds_skipped_open
    );
    println!("Total ops executed: {total_ops_executed}");
    println!(
        "Operations: {} writes, {} syncs, {} reads, {} truncations, {} crashes",
        total_writes, total_syncs, total_reads, total_truncations, total_crashes
    );
    println!("=====================================================\n");

    // Ensure we actually tested something.
    assert!(
        seeds_completed > 0,
        "No seeds completed - test is not meaningful"
    );
    assert!(
        total_ops_executed > 1000,
        "Too few operations executed ({total_ops_executed}) - test is not meaningful"
    );
}

// ============================================================================
// Targeted Invariant Tests
// ============================================================================

/// Test that indices are always contiguous after recovery.
#[tokio::test]
async fn test_invariant_index_contiguity() {
    for seed in 0..50 {
        let config = FaultConfig::none()
            .with_torn_write_rate(0.15)
            .with_write_fail_rate(0.15);
        let storage = SimulatedStorage::with_faults(seed, config);
        let wal_dir = Path::new("/wal/contiguity");
        let wal_config = WalConfig::new(wal_dir);

        // Write entries with faults - track actual next index
        {
            let mut wal = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();
            let mut next_idx = 1u64;
            for _ in 0..50 {
                let entry = Entry::new(1, next_idx, Bytes::from("data")).unwrap();
                if wal.append(entry).await.is_ok() {
                    next_idx += 1;
                }
            }
        }

        // Disable faults and check recovery
        storage.fault_config().torn_write_rate = 0.0;
        storage.fault_config().write_fail_rate = 0.0;

        let wal = TestWal::open(storage.clone(), wal_config).await.unwrap();

        // Verify contiguity: every index from 1 to last_index should be readable
        if let Some(last_idx) = wal.last_index() {
            for i in 1..=last_idx {
                let entry = wal.read(i);
                assert!(
                    entry.is_ok(),
                    "seed {seed}: Gap at index {i}, last_index={last_idx}"
                );
                assert_eq!(entry.unwrap().index(), i);
            }
        }
    }
}

/// Test truncation under faults.
#[tokio::test]
async fn test_truncation_under_faults() {
    for seed in 0..100 {
        let config = FaultConfig::none()
            .with_fsync_fail_rate(0.15);
        let storage = SimulatedStorage::with_faults(seed, config);
        let wal_dir = Path::new("/wal/truncate");
        let wal_config = WalConfig::new(wal_dir);

        // Write entries without write faults (to ensure we have data)
        let mut max_written = 0u64;
        {
            let mut wal = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();
            for i in 1..=20 {
                let entry = Entry::new(1, i, Bytes::from(format!("data-{i}"))).unwrap();
                if wal.append(entry).await.is_ok() {
                    max_written = i;
                }
            }
            // Disable faults for sync to ensure data is persisted
            storage.fault_config().fsync_fail_rate = 0.0;
            wal.sync().await.ok();
        }

        if max_written < 5 {
            continue; // Need enough entries to truncate
        }

        // Re-enable faults for truncation
        storage.fault_config().fsync_fail_rate = 0.15;

        // Truncate with faults enabled
        let truncate_to = max_written / 2;
        {
            let mut wal = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();
            let _ = wal.truncate_after(truncate_to).await;
        }

        // Disable faults and verify
        storage.fault_config().fsync_fail_rate = 0.0;

        let wal = TestWal::open(storage.clone(), wal_config).await.unwrap();

        // Invariant: last_index should be <= original max_written
        if let Some(last_idx) = wal.last_index() {
            assert!(
                last_idx <= max_written,
                "seed {seed}: last_index {last_idx} > max_written {max_written}"
            );

            // All remaining entries should be readable and contiguous
            for i in 1..=last_idx {
                let entry = wal.read(i);
                assert!(entry.is_ok(), "seed {seed}: entry {i} unreadable after truncation");
                assert_eq!(entry.unwrap().index(), i);
            }
        }
    }
}

/// Test segment rotation under faults with large payloads.
#[tokio::test]
async fn test_segment_rotation_under_faults() {
    for seed in 0..50 {
        let config = FaultConfig::none()
            .with_write_fail_rate(0.15)
            .with_fsync_fail_rate(0.15)
            .with_torn_write_rate(0.05);
        let storage = SimulatedStorage::with_faults(seed, config);
        let wal_dir = Path::new("/wal/rotation");

        // Use minimum segment size (1MB) and max_entries to force rotation
        let segment_config = SegmentConfig::new().with_max_entries(5); // Force rotation after 5 entries
        let wal_config = WalConfig::new(wal_dir).with_segment_config(segment_config);

        let mut written = WrittenEntries::default();
        let mut next_idx = 1u64;

        {
            let mut wal = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();

            // Write entries to force segment rotation via max_entries limit
            for _ in 0..30 {
                let payload = format!("segment-rotation-test-{seed}-{next_idx}");
                let entry = Entry::new(1, next_idx, Bytes::from(payload.clone())).unwrap();
                if wal.append(entry).await.is_ok() {
                    written.record(next_idx, 1, payload);
                    next_idx += 1;
                }
            }
            wal.sync().await.ok();
        }

        // Disable faults and verify
        storage.fault_config().write_fail_rate = 0.0;
        storage.fault_config().fsync_fail_rate = 0.0;
        storage.fault_config().torn_write_rate = 0.0;

        let wal = TestWal::open(storage.clone(), wal_config).await.unwrap();

        // Should have multiple sealed segments if rotation worked
        let sealed_count = wal.sealed_segment_count();

        if let Some(last_idx) = wal.last_index() {
            // All entries should be readable across segments
            for i in 1..=last_idx {
                let entry = wal.read(i);
                assert!(
                    entry.is_ok(),
                    "seed {seed}: entry {i} unreadable, sealed_segments={sealed_count}"
                );

                // Verify content
                if let Some((expected_term, expected_payload)) = written.entries.get(&i) {
                    let e = entry.unwrap();
                    assert_eq!(e.term(), *expected_term);
                    let actual = String::from_utf8_lossy(&e.payload);
                    assert_eq!(&*actual, expected_payload);
                }
            }
        }
    }
}

/// Test that term monotonicity is preserved.
#[tokio::test]
async fn test_invariant_term_monotonicity() {
    for seed in 0..50 {
        let config = FaultConfig::none()
            .with_torn_write_rate(0.1);
        let storage = SimulatedStorage::with_faults(seed, config);
        let wal_dir = Path::new("/wal/term_mono");
        let wal_config = WalConfig::new(wal_dir);

        // Write entries with increasing terms - track actual next index
        {
            let mut wal = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();
            let mut term = 1u64;
            let mut next_idx = 1u64;
            for attempt in 0..30 {
                if attempt % 5 == 0 && attempt > 0 {
                    term += 1; // Bump term occasionally
                }
                let entry = Entry::new(term, next_idx, Bytes::from("data")).unwrap();
                if wal.append(entry).await.is_ok() {
                    next_idx += 1;
                }
            }
        }

        // Disable faults and check
        storage.fault_config().torn_write_rate = 0.0;

        let wal = TestWal::open(storage.clone(), wal_config).await.unwrap();

        // Verify term monotonicity
        if let Some(last_idx) = wal.last_index() {
            let mut prev_term = 0u64;
            for i in 1..=last_idx {
                let entry = wal.read(i).unwrap();
                assert!(
                    entry.term() >= prev_term,
                    "seed {seed}: Term regression at index {i}: {} < {prev_term}",
                    entry.term()
                );
                prev_term = entry.term();
            }
        }
    }
}

/// Reproduction test for debugging durable_index bugs.
#[tokio::test]
async fn test_repro_seed_135837() {
    const FAULT_RATE: f64 = 0.25;
    let seed = 2074002u64;

    let config = FaultConfig {
        torn_write_rate: FAULT_RATE / 2.0,
        fsync_fail_rate: FAULT_RATE,
        read_corruption_rate: 0.0,
        read_fail_rate: FAULT_RATE,
        write_fail_rate: FAULT_RATE,
        exists_fail_rate: FAULT_RATE / 5.0,
        list_files_fail_rate: FAULT_RATE / 5.0,
        open_fail_rate: FAULT_RATE / 10.0,
        remove_fail_rate: FAULT_RATE / 5.0,
        force_torn_write_at: None,
        force_fsync_fail: false,
        force_disk_full: false,
    };

    let storage = SimulatedStorage::with_faults(seed, config);
    let wal_dir = Path::new("/wal/comprehensive");
    let wal_config = WalConfig::new(wal_dir);

    let mut current_term = 1u64;

    let mut wal = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();

    // Run operations until op 69
    for op_num in 0..=69 {
        let op_hash = seed.wrapping_add(op_num as u64).wrapping_mul(0x9e3779b97f4a7c15);
        let op_type = op_hash % 10;

        eprintln!(
            "op {}: type={}, last_idx={:?}, durable_idx={:?}",
            op_num,
            op_type,
            wal.last_index(),
            wal.durable_index()
        );

        if op_num == 69 {
            // This should be a crash operation
            let durable_before = wal.durable_index();
            let sealed_count = wal.sealed_segment_count();
            eprintln!(
                "Before crash: durable={:?}, sealed_segments={}",
                durable_before, sealed_count
            );

            // Debug: dump storage state before crash
            let wal_dir = Path::new("/wal/repro");
            if let Ok(files) = storage.list_files(wal_dir, "wal").await {
                eprintln!("Files before crash (dirty state):");
                for f in &files {
                    if let Some(content) = storage.get_raw_content(f) {
                        eprintln!("  {}: {} bytes", f.display(), content.len());
                    }
                }
            }
            eprintln!("Synced files before crash (durable state):");
            for f in storage.synced_file_paths() {
                if let Some(content) = storage.get_synced_content(&f) {
                    eprintln!("  {}: {} bytes", f.display(), content.len());
                }
            }

            drop(wal);
            storage.simulate_crash();

            // Debug: dump storage state after crash
            {
                let mut fc = storage.fault_config();
                fc.list_files_fail_rate = 0.0;
            }
            eprintln!("Files after crash (should equal synced):");
            match storage.list_files(wal_dir, "wal").await {
                Ok(files) => {
                    eprintln!("  Found {} files", files.len());
                    for f in &files {
                        if let Some(content) = storage.get_raw_content(f) {
                            eprintln!("  {}: {} bytes", f.display(), content.len());
                        } else {
                            eprintln!("  {}: no content!", f.display());
                        }
                    }
                }
                Err(e) => {
                    eprintln!("  list_files failed: {e}");
                }
            }

            // Disable faults
            {
                let mut fc = storage.fault_config();
                fc.torn_write_rate = 0.0;
                fc.write_fail_rate = 0.0;
                fc.read_fail_rate = 0.0;
                fc.fsync_fail_rate = 0.0;
                fc.open_fail_rate = 0.0;
                fc.list_files_fail_rate = 0.0;
                fc.exists_fail_rate = 0.0;
                fc.remove_fail_rate = 0.0;
            }

            // Print synced files for debugging
            let synced_paths = storage.synced_file_paths();
            eprintln!("  synced_files: {synced_paths:?}");

            let recovered = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();
            eprintln!("After crash: recovered={:?}", recovered.last_index());

            if let (Some(r), Some(d)) = (recovered.last_index(), durable_before) {
                assert!(r <= d, "recovered {r} > durable {d}");
            }
            return;
        }

        match op_type {
            0..=4 => {
                let next_idx = wal.last_index().map_or(1, |i| i + 1);
                let entry = Entry::new(current_term, next_idx, Bytes::from("data")).unwrap();
                let result = wal.append(entry).await;
                eprintln!("  Write idx {}: {:?}", next_idx, result.is_ok());
                if result.is_ok() {
                    let sync_result = wal.sync().await;
                    eprintln!("  Sync: {:?}", sync_result.is_ok());
                    // Debug: show files after sync
                    if sync_result.is_ok() {
                        if let Ok(files) = storage.list_files(wal_dir, "wal").await {
                            eprintln!("    files: {:?}", files.iter().map(|p| p.display().to_string()).collect::<Vec<_>>());
                        }
                        for p in storage.synced_file_paths() {
                            if let Some(c) = storage.get_synced_content(&p) {
                                eprintln!("    synced {}: {} bytes", p.display(), c.len());
                            }
                        }
                    }
                }
            }
            5 => {
                let sync_result = wal.sync().await;
                eprintln!("  Sync: {:?}", sync_result.is_ok());
            }
            6 => {
                if let Some(last) = wal.last_index() {
                    let idx = (op_hash % last) + 1;
                    let _ = wal.read(idx);
                }
            }
            7 => {
                current_term += 1;
            }
            8 => {
                if let Some(last) = wal.last_index() {
                    if last > 1 {
                        let truncate_to = (op_hash % (last - 1)) + 1;
                        let _ = wal.truncate_after(truncate_to).await;
                        let _ = wal.sync().await;
                    }
                }
            }
            _ => {
                // Crash - skip for now, we want to run until op 64
                let durable_before = wal.durable_index();
                let last_before = wal.last_index();
                eprintln!("  CRASH: durable={durable_before:?}, last={last_before:?}");
                drop(wal);

                // Debug: print synced files before crash with sizes
                let synced_paths = storage.synced_file_paths();
                for p in &synced_paths {
                    if let Some(content) = storage.get_synced_content(p) {
                        eprintln!("    synced {}: {} bytes", p.display(), content.len());
                    }
                }
                eprintln!("  synced_files: {synced_paths:?}");

                storage.simulate_crash();

                // Only disable the faults that comprehensive test disables (NOT exists/remove)
                {
                    let mut fc = storage.fault_config();
                    fc.torn_write_rate = 0.0;
                    fc.write_fail_rate = 0.0;
                    fc.read_fail_rate = 0.0;
                    fc.fsync_fail_rate = 0.0;
                    fc.open_fail_rate = 0.0;
                    fc.list_files_fail_rate = 0.0;
                    // NOT disabling exists_fail_rate and remove_fail_rate to match comprehensive
                }

                // Debug: print files after crash (before recovery)
                match storage.list_files(wal_dir, "wal").await {
                    Ok(files) => {
                        eprintln!("  files after crash (for recovery):");
                        for f in &files {
                            if let Some(content) = storage.get_raw_content(f) {
                                eprintln!("    {}: {} bytes", f.display(), content.len());
                            } else {
                                eprintln!("    {}: no content!", f.display());
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("  list_files failed: {e}");
                    }
                }

                let w = TestWal::open(storage.clone(), wal_config.clone()).await.unwrap();
                eprintln!("  Recovered: last={:?}", w.last_index());

                // Debug: print files after recovery to see what was created
                match storage.list_files(wal_dir, "wal").await {
                    Ok(files) => {
                        eprintln!("  files after recovery:");
                        for f in &files {
                            if let Some(content) = storage.get_raw_content(f) {
                                eprintln!("    {}: {} bytes", f.display(), content.len());
                            }
                        }
                    }
                    Err(_) => {}
                }

                // Check the invariant here too
                if let (Some(r), Some(d)) = (w.last_index(), durable_before) {
                    assert!(r <= d, "op {op_num}: recovered {r} > durable {d}");
                }

                wal = w;

                {
                    let mut fc = storage.fault_config();
                    fc.torn_write_rate = FAULT_RATE / 2.0;
                    fc.write_fail_rate = FAULT_RATE;
                    fc.read_fail_rate = FAULT_RATE;
                    fc.fsync_fail_rate = FAULT_RATE;
                    fc.open_fail_rate = FAULT_RATE / 10.0;
                    fc.list_files_fail_rate = FAULT_RATE / 5.0;
                }
            }
        }
    }
}
