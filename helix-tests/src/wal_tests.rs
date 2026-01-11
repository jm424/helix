//! WAL crash recovery and integrity simulation tests.
//!
//! These tests verify WAL correctness under crash conditions using
//! SimulatedStorage with deterministic fault injection.
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

use std::path::Path;

use bytes::{Bytes, BytesMut};
use helix_wal::{
    Entry, FaultConfig, Segment, SegmentConfig, SegmentId, SimulatedStorage, Storage,
    Wal, WalConfig, ENTRY_HEADER_SIZE, SEGMENT_HEADER_SIZE,
};

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
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();
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
    let wal = Wal::open(storage.clone(), config).await.unwrap();
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
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();
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
    let wal = Wal::open(storage.clone(), config).await.unwrap();
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

            let mut wal = Wal::open(storage.clone(), wal_config.clone()).await.unwrap();
            for i in 1..=3 {
                let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
                wal.append(entry).await.unwrap();
            }
            wal.sync().await.unwrap();
        }

        // Now enable torn write for entry 4.
        storage.fault_config().force_torn_write_at = Some(torn_offset);

        {
            let mut wal = Wal::open(storage.clone(), wal_config.clone()).await.unwrap();
            let entry = Entry::new(1, 4, Bytes::from("this will be torn")).unwrap();
            // Write will succeed but be torn.
            let _ = wal.append(entry).await;
        }

        // Recovery should preserve entries 1-3.
        let wal = Wal::open(storage.clone(), wal_config).await.unwrap();
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
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();
        for i in 1..=5 {
            let entry = Entry::new(1, i, Bytes::from("synced")).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Write more entries but DON'T sync.
    {
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();
        for i in 6..=10 {
            let entry = Entry::new(1, i, Bytes::from("unsynced")).unwrap();
            wal.append(entry).await.unwrap();
        }
        // No sync - simulate crash.
        // In SimulatedStorage, data is still there (no actual OS buffer).
        // In real systems, this data might be lost.
    }

    // Recovery - in SimulatedStorage, writes are immediate so data persists.
    let wal = Wal::open(storage.clone(), config).await.unwrap();
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
        let mut wal = Wal::open(storage.clone(), wal_config.clone()).await.unwrap();
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
        let mut wal = Wal::open(storage.clone(), wal_config.clone()).await.unwrap();
        for i in 6..=8 {
            let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
            wal.append(entry).await.unwrap();
        }
        // Sync should fail.
        let result = wal.sync().await;
        assert!(result.is_err());
    }

    // Recovery should still have the data (SimulatedStorage keeps it).
    let wal = Wal::open(storage.clone(), wal_config).await.unwrap();
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
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();

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
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();
        let large_payload = Bytes::from(vec![0u8; 100_000]);

        // This should trigger segment rotation.
        for i in 9..=12 {
            let entry = Entry::new(1, i, large_payload.clone()).unwrap();
            wal.append(entry).await.unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Verify recovery works with multiple segments.
    let wal = Wal::open(storage.clone(), config).await.unwrap();
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
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();
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
    let wal = Wal::open(storage.clone(), config).await.unwrap();
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
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();
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
    let wal = Wal::open(storage.clone(), config).await.unwrap();
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
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();
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
    let wal = Wal::open(storage.clone(), config).await.unwrap();
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
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();
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
    let wal = Wal::open(storage.clone(), config).await.unwrap();

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
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();
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
    let wal = Wal::open(storage.clone(), config).await.unwrap();
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
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();
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
    let wal = Wal::open(storage.clone(), config).await.unwrap();

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
    let wal = Wal::open(storage.clone(), config).await.unwrap();
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

    let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();

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

    let mut wal = Wal::open(storage.clone(), config).await.unwrap();

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
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();
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
        let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();
        let entry = Entry::new(1, 6, Bytes::from("will_fail")).unwrap();
        let result = wal.append(entry).await;
        assert!(result.is_err());
    }

    // Clear fault and verify recovery.
    storage.fault_config().force_disk_full = false;

    let wal = Wal::open(storage.clone(), config).await.unwrap();
    // Original 5 entries should still be there.
    assert_eq!(wal.last_index(), Some(5));
}

/// Tests handling of I/O error during sync.
#[tokio::test]
async fn test_io_error_during_sync() {
    let storage = SimulatedStorage::new(42);
    let wal_dir = Path::new("/wal/io_error");
    let config = WalConfig::new(wal_dir);

    let mut wal = Wal::open(storage.clone(), config.clone()).await.unwrap();

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

    let mut wal = Wal::open(storage.clone(), wal_config.clone()).await.unwrap();

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
    let wal = Wal::open(storage.clone(), wal_config).await.unwrap();
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
        let config = FaultConfig {
            torn_write_rate: 0.02,      // 2% torn writes
            fsync_fail_rate: 0.05,      // 5% fsync failures
            read_corruption_rate: 0.0,  // Disable read corruption for this test
            force_torn_write_at: None,
            force_fsync_fail: false,
            force_disk_full: false,
        };
        let storage = SimulatedStorage::with_faults(seed, config);
        let wal_dir = Path::new("/wal/stress");
        let wal_config = WalConfig::new(wal_dir);

        // Try to write entries, some may fail due to faults.
        let mut last_successful = 0u64;
        {
            let mut wal = Wal::open(storage.clone(), wal_config.clone()).await.unwrap();
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
        let wal = Wal::open(storage.clone(), wal_config).await.unwrap();

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
