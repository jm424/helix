//! Payload roundtrip and corruption detection tests.

use super::super::*;
use tracing::info;

/// Verifies that `extract_payload_from_batch` correctly parses batches
/// created by `create_test_record_batch`.
///
/// This is a CRITICAL test - if this fails, all DST verification is broken.
#[test]
fn test_extract_payload_roundtrip() {
    // Note: Record overhead is 6 bytes (attributes, timestamps, key/value lengths, headers).
    // For single-byte varint, record_length < 128, so max payload = 121 bytes.
    let test_payloads = [
        b"hello world".to_vec(),
        b"record-0".to_vec(),
        b"".to_vec(),    // Empty payload
        b"x".to_vec(),   // Single byte
        vec![0u8; 100],  // 100 zero bytes
        vec![0xAB; 121], // Max payload for single-byte record length varint (121 + 6 = 127 < 128)
    ];

    for payload in &test_payloads {
        let batch = create_test_record_batch(payload);
        let extracted = E2ECluster::extract_payload_from_batch(&batch);

        match extracted {
            Ok(result) => {
                assert_eq!(
                    &result, payload,
                    "Payload mismatch: expected {:?}, got {:?}",
                    payload, result
                );
            }
            Err(e) => {
                panic!(
                    "Failed to extract payload {:?} (len={}): {}",
                    payload,
                    payload.len(),
                    e
                );
            }
        }
    }
    info!("test_extract_payload_roundtrip: parser correctly extracts payloads");
}

/// Verifies that `verify_payload_direct` catches data corruption.
///
/// This is a CRITICAL test - if this fails, DST won't detect data corruption.
#[test]
fn test_verification_catches_corruption() {
    use crate::properties::HelixPropertyState;
    use bytes::Bytes;

    let mut state = HelixPropertyState::new();

    // Record expected payload.
    let expected_payload = Bytes::from("correct data");
    state.record_expected_payload(1, 0, 0, expected_payload);

    // Verify with correct payload - should pass.
    let result = state.verify_payload_direct(1, 0, 0, b"correct data");
    assert!(
        result.is_ok(),
        "Correct payload should verify: {:?}",
        result
    );

    // Try to verify with corrupted payload - should fail.
    let corrupted = state.verify_payload_direct(1, 0, 0, b"CORRUPTED data");
    assert!(corrupted.is_err(), "Corrupted payload should be detected");
    let err = corrupted.unwrap_err();
    assert!(
        err.contains("DATA CORRUPTION"),
        "Error should mention DATA CORRUPTION: {err}"
    );

    // Try to verify unknown offset - should fail.
    let unknown = state.verify_payload_direct(1, 0, 999, b"any data");
    assert!(unknown.is_err(), "Unknown offset should be detected");
    let err = unknown.unwrap_err();
    assert!(
        err.contains("UNEXPECTED DATA"),
        "Error should mention UNEXPECTED DATA: {err}"
    );

    info!("test_verification_catches_corruption: corruption detection works");
}

/// Verifies that parse errors are caught explicitly, not silently skipped.
#[test]
fn test_parse_error_not_silent() {
    use bytes::Bytes;

    // Create an invalid/truncated batch.
    let invalid_batch = Bytes::from(vec![0u8; 10]); // Too short to be valid.
    let result = E2ECluster::extract_payload_from_batch(&invalid_batch);

    assert!(
        result.is_err(),
        "Invalid batch should return Err, not silently fail"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("batch too short"),
        "Error should explain the issue: {err}"
    );

    info!("test_parse_error_not_silent: parse errors are caught explicitly");
}
