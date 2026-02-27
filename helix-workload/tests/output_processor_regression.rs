//! Regression test for output processor silent message drops.
//!
//! # What this tests
//!
//! When a follower restarts with an empty log and must catch up on multiple
//! partition groups simultaneously, the output processor merges all groups'
//! `AppendEntries` messages for that peer into a single `encode_group_batch`
//! call. If the merged encoded size exceeds `MAX_MESSAGE_SIZE` (16 MB), the
//! call returns `Err(MessageTooLarge)`. The output processor logs this at
//! DEBUG level and discards all messages to that peer for the entire drain
//! cycle. The follower never catches up.
//!
//! # Scenario
//!
//! - 3-node cluster, 64 partitions, RF=3 (matches the production incident)
//! - Write 512 KB per partition group (2 × 256 KB)
//! - Kill one follower and wipe its data directory
//! - Restart the follower (empty log — must catch up on all 64 groups)
//! - Immediately write more data with 32 concurrent senders (sustained load)
//!   - All 64 groups continuously queue `AppendEntries` for the recovering node
//!   - Output processor drain cycle sees messages from dozens of groups together
//!   - 64 groups × 512 KB each = 32 MB merged batch → exceeds 16 MB → silent drop
//!   - Follower falls further behind each tick; catch-up never completes
//! - Kill the other follower; try to write
//!   - With fix: catch-up completed during sustained-load phase → writes succeed
//!   - Without fix: follower never caught up → writes fail (no quorum)
//!
//! # Why 64 groups are necessary
//!
//! With only 4 groups the output processor drains each group's `AppendEntries`
//! individually between ticks (avg_batch_size ≈ 1), so the merged batch never
//! exceeds 16 MB and the bug is invisible. With 64 groups the drain cycle always
//! sees messages from many groups simultaneously — the same dynamic that caused
//! the staging outage (64 leaderships on helix-2 under 25 MB/s mirror load).

#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use helix_workload::{ProducerMode, RealCluster, RealExecutor, WorkloadExecutor};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Blob size per produce call.
const BLOB_SIZE: usize = 256 * 1024;

/// Writes per partition for the initial log-fill phase.
///
/// 2 × 256 KB = 512 KB per group. On catch-up, 64 groups × 512 KB = 32 MB
/// merged batch — reliably above the 16 MB limit.
const WRITES_PER_PARTITION: u32 = 2;

/// Writes per partition during the sustained-load phase (while node 2 recovers).
///
/// 8 × 256 KB × 64 groups = 128 MB total. Keeps all 64 groups generating
/// AppendEntries for node 2 simultaneously for several seconds.
const CATCHUP_LOAD_WRITES_PER_PARTITION: u32 = 8;

/// Number of partition groups.
///
/// 64 matches the production scenario (helix-2 held 64 leaderships during the
/// incident). With 64 groups all ticking at the same 50ms interval, the output
/// processor drain cycle reliably sees messages from dozens of groups at once.
/// The merged batch (64 × 512 KB = 32 MB) far exceeds the 16 MB limit.
///
/// 4 groups is not enough: the output processor drains each group's message
/// individually between ticks (avg_batch_size=1), so the merged batch never
/// exceeds 16 MB and the bug is invisible.
const PARTITION_COUNT: u32 = 64;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn binary_path() -> PathBuf {
    let release = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/release/helix-server");
    if release.exists() {
        return release;
    }
    panic!(
        "helix-server binary not found at {}.\n\
         Run `cargo build --release --bin helix-server` first.",
        release.display()
    );
}

fn test_data_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("helix-workload-tests")
        .join(name)
        .join(format!("{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn find_available_base_port(start: u16, count: u16) -> u16 {
    let step = 10u16;
    let mut base = start;
    while base <= u16::MAX.saturating_sub(count) {
        let mut ok = true;
        let mut listeners = Vec::new();
        for offset in 1..=count {
            let port = base.saturating_add(offset);
            if let Ok(l) = std::net::TcpListener::bind(format!("127.0.0.1:{port}")) {
                listeners.push(l);
            } else {
                ok = false;
                break;
            }
        }
        drop(listeners);
        if ok {
            return base;
        }
        base = base.saturating_add(step);
    }
    panic!("no available port block (count={count}) from {start}");
}

/// Write `count` blobs of `BLOB_SIZE` to each of `PARTITION_COUNT` partitions.
async fn write_to_all_partitions(
    executor: &RealExecutor,
    topic: &str,
    count: u32,
    label: &str,
) -> u32 {
    let payload = Bytes::from(vec![0xABu8; BLOB_SIZE]);
    let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
    const INFLIGHT: usize = 32;
    let total = count * PARTITION_COUNT;
    let mut dispatched = 0u32;
    let mut succeeded = 0u32;
    let mut failed = 0u32;

    loop {
        while futs.len() < INFLIGHT && dispatched < total {
            let partition = (dispatched % PARTITION_COUNT) as i32;
            futs.push(executor.send(topic, partition, payload.clone()));
            dispatched += 1;
        }
        if futs.is_empty() {
            break;
        }
        match futs.next().await {
            Some(Ok(_)) => succeeded += 1,
            Some(Err(e)) => {
                failed += 1;
                eprintln!("[{label}] write error: {e}");
            }
            None => break,
        }
    }

    println!(
        "[{label}] {}/{} writes succeeded ({} failed)",
        succeeded, total, failed
    );
    succeeded
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// Verifies that a follower that restarts with an empty log can catch up on
/// multiple partition groups simultaneously without silent message drops.
///
/// Without Fix A (`output-processor-redesign.md`): `encode_group_batch` merges
/// all groups' catch-up `AppendEntries` into one buffer, which exceeds 16 MB
/// and is silently discarded. The follower never catches up. Writes fail when
/// the other follower is killed (no quorum).
///
/// With Fix A: encoding moves to `sender_loop` with 4 MB chunks. Catch-up
/// completes. Writes succeed when the other follower is killed.
#[tokio::test]
async fn follower_catchup_multigroup_regression() {
    let node_count = 3u32;
    let topic = "catchup-test";

    let base_port = find_available_base_port(25500, node_count as u16);
    let raft_base_port = find_available_base_port(35500, node_count as u16);

    let mut cluster = RealCluster::builder()
        .nodes(node_count)
        .base_port(base_port)
        .raft_base_port(raft_base_port)
        .binary_path(binary_path())
        .data_dir(test_data_dir("output_processor_regression"))
        .topic(topic, PARTITION_COUNT)
        .default_replication_factor(3)
        .log_level("warn")
        .build()
        .expect("failed to start cluster");

    let executor = RealExecutor::with_mode(
        cluster.bootstrap_servers(),
        ProducerMode::LowLatency,
    )
    .expect("failed to create executor");

    executor
        .wait_ready(Duration::from_secs(30))
        .await
        .expect("cluster not ready within 30s");

    // -----------------------------------------------------------------
    // Phase 1: Write WRITES_PER_PARTITION × BLOB_SIZE per partition.
    //
    // 2 × 256 KB = 512 KB per group. When node 2 restarts with an empty
    // log, all 64 groups will try to send catch-up AppendEntries(512 KB)
    // simultaneously: 64 × 512 KB = 32 MB merged batch > 16 MB limit.
    // -----------------------------------------------------------------
    let t0 = std::time::Instant::now();
    println!(
        "[regression] phase 1: writing {} MB ({} × {} KB to {} partitions)",
        WRITES_PER_PARTITION as usize * (BLOB_SIZE / 1024) / 1024 * PARTITION_COUNT as usize,
        WRITES_PER_PARTITION,
        BLOB_SIZE / 1024,
        PARTITION_COUNT,
    );
    let p1_ok = write_to_all_partitions(&executor, topic, WRITES_PER_PARTITION, "phase1").await;
    println!("[regression] phase 1 done in {:.1}s", t0.elapsed().as_secs_f64());

    assert_eq!(
        p1_ok,
        WRITES_PER_PARTITION * PARTITION_COUNT,
        "phase 1: expected all {} writes to succeed",
        WRITES_PER_PARTITION * PARTITION_COUNT
    );

    // -----------------------------------------------------------------
    // Phase 2: Kill node 2, wipe its data, restart it, then immediately
    // write under sustained concurrent load while it is recovering.
    //
    // The sustained load (32 inflight senders across 4 groups) keeps the
    // output processor's drain cycles full of messages from all groups
    // simultaneously — this is the condition that triggers the merge bug.
    //
    // Without sustained load, groups drain one-at-a-time (avg_batch_size=1)
    // and each individual 8 MB send stays under the 16 MB limit, masking
    // the bug. With sustained load, all 4 groups queue AppendEntries for
    // node 2 at the same time (up to 4 × 8 MB = 32 MB merged) → bug fires.
    //
    // Quorum during this phase: nodes 1+3. All writes succeed regardless
    // of whether node 2 caught up.
    // -----------------------------------------------------------------
    println!("[regression] phase 2: killing node 2");
    cluster.kill_node(2).expect("failed to kill node 2");

    cluster
        .wipe_node_data_dir(2)
        .expect("failed to wipe node 2 data dir");

    cluster.restart_node(2).expect("failed to restart node 2");
    println!("[regression] node 2 restarted with empty log");

    // Write under sustained load while node 2 is recovering.
    // With the bug: AppendEntries are silently dropped every drain cycle;
    //   node 2 never catches up regardless of how long we write.
    // With the fix: encoding moves to sender_loop in 4 MB chunks; node 2
    //   catches up during this window and is ready for phase 3.
    println!(
        "[regression] phase 2: writing {} MB under sustained load while node 2 recovers \
         ({} × {} KB × {} partitions)",
        CATCHUP_LOAD_WRITES_PER_PARTITION as usize * BLOB_SIZE / 1024 / 1024
            * PARTITION_COUNT as usize,
        CATCHUP_LOAD_WRITES_PER_PARTITION,
        BLOB_SIZE / 1024,
        PARTITION_COUNT,
    );
    let p2_ok = write_to_all_partitions(
        &executor,
        topic,
        CATCHUP_LOAD_WRITES_PER_PARTITION,
        "catchup-load",
    )
    .await;
    assert_eq!(
        p2_ok,
        CATCHUP_LOAD_WRITES_PER_PARTITION * PARTITION_COUNT,
        "phase 2 writes should succeed (quorum: nodes 1+3)"
    );

    // Brief pause for node 2 to finish catch-up after write load ends.
    // With the fix, catch-up on ~96 MB over local TCP completes in < 5s.
    // Without the fix, node 2 is perpetually stale — 5s changes nothing.
    println!("[regression] waiting 5s for node 2 catch-up to settle...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Confirm the cluster is still healthy after the sustained load.
    cluster
        .check_health()
        .expect("cluster unhealthy after node 2 catch-up window");

    // -----------------------------------------------------------------
    // Phase 3: Kill node 3 (the always-up follower), then write.
    //
    // After killing node 3, the cluster has:
    //   node 1 = leader (all data: phase 1 + phase 2)
    //   node 2 = follower (caught up with fix, stale with bug)
    //   node 3 = dead
    //
    // Quorum requires 2/3 nodes. Writes succeed iff node 2 caught up
    // on all data written during phase 1 and phase 2.
    // -----------------------------------------------------------------
    println!("[regression] phase 3: killing node 3");
    cluster.kill_node(3).expect("failed to kill node 3");

    // Brief pause for Raft to stabilize after node 3 disappears (heartbeat
    // timeout is election_tick × 50ms = 500ms; give 2s to be safe).
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("[regression] phase 3: writing with only nodes 1 and 2 available");
    const FINAL_WRITES_PER_PARTITION: u32 = 5;
    let p3_ok =
        write_to_all_partitions(&executor, topic, FINAL_WRITES_PER_PARTITION, "phase3").await;
    let p3_total = FINAL_WRITES_PER_PARTITION * PARTITION_COUNT;

    // All writes must succeed: quorum requires node 2 to have caught up.
    // If the silent-drop bug is present, node 2 never caught up and these
    // writes fail with NotEnoughReplicas or timeout.
    assert_eq!(
        p3_ok, p3_total,
        "phase 3: {p3_ok}/{p3_total} writes succeeded — node 2 may not have caught up \
         (silent AppendEntries drops?). Without Fix A, merged AppendEntries from \
         {PARTITION_COUNT} groups exceeds the 16 MB wire limit and is silently \
         discarded each drain cycle, leaving node 2 perpetually stale.",
    );

    println!(
        "[regression] PASS: node 2 caught up on {} groups under sustained load; \
         {}/{} writes succeeded with only nodes 1+2",
        PARTITION_COUNT, p3_ok, p3_total
    );

    cluster.stop();
}
