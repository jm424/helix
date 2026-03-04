//! Thin `#[test]` wrappers that call DST runner functions.

use super::dst_runner::{run_e2e_dst_concurrent_faults, run_e2e_dst_random_faults};

/// DST test with random fault injection (100 seeds).
///
/// Uses higher record/tick counts to force WAL segment rotation and
/// retention, exercising the snapshot transfer pipeline.
#[test]
fn test_e2e_dst_random_faults() {
    run_e2e_dst_random_faults(100, 100, 50, "dst-topic", 1, 10);
}

/// DST with random fault injection (500 seeds).
#[test]
fn test_e2e_dst_random_faults_500() {
    run_e2e_dst_random_faults(500, 20, 20, "dst-topic", 1, 10);
}

/// DST with multiple partitions (100 seeds).
#[test]
fn test_e2e_dst_random_faults_multi_partition() {
    run_e2e_dst_random_faults(100, 100, 50, "dst-mp", 3, 10);
}

/// DST with multiple partitions (200 seeds).
#[test]
fn test_e2e_dst_random_faults_multi_partition_200() {
    run_e2e_dst_random_faults(200, 20, 20, "dst-mp", 3, 10);
}

/// Extended DST test - runs 1000 seeds (ignored by default, run manually).
#[test]
#[ignore]
fn test_e2e_dst_extended() {
    run_e2e_dst_random_faults(1000, 20, 20, "ext-topic", 1, 25);
}

/// Concurrent DST: single partition, 200 seeds.
///
/// Uses inflated network latency (50x) so faults injected while Raft
/// messages are in-flight (sleeping) take effect at delivery time.
/// Duration increased to 3s to give retention more simulated time.
#[test]
fn test_e2e_dst_concurrent_faults() {
    run_e2e_dst_concurrent_faults(200, 3000, 50, 2, 10, "concurrent-dst", 1, 10);
}

/// Concurrent DST: 3 partitions, 100 seeds.
#[test]
fn test_e2e_dst_concurrent_faults_multi_partition() {
    run_e2e_dst_concurrent_faults(100, 2000, 50, 3, 10, "concurrent-dst-mp", 3, 10);
}

/// Extended concurrent DST: 1000 seeds (manual run).
#[test]
#[ignore]
fn test_e2e_dst_concurrent_extended() {
    run_e2e_dst_concurrent_faults(1000, 3000, 50, 2, 10, "concurrent-ext", 1, 25);
}
