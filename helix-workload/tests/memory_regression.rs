//! Memory growth measurement tests.
//!
//! These tests measure RSS growth rather than asserting thresholds — the goal
//! is to observe and understand what accumulates, not to gate on a magic number.
//!
//! # Tests
//!
//! - `measure_memory_growth`: Write 100 MB through a 3-node cluster with default
//!   retention (disabled in test window). Reports per-node RSS and total growth.
//!   Shows what accumulates without any retention-triggered cleanup.
//!
//! - `measure_memory_growth_with_retention`: Same write volume but with
//!   `--local-retention-ms=1000` so WAL segments are eligible for deletion
//!   after 1 second. The retention task fires every 60 seconds; this test
//!   waits long enough to observe whether BlobIndex compaction reduces RSS.

#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use helix_workload::{self, ProducerMode, RealCluster, RealExecutor, WorkloadExecutor};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TOTAL_BYTES: u64 = 100 * 1024 * 1024; // 100 MB
const MSG_SIZE: usize = 1024;                 // 1 KB per message

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

/// Read RSS (bytes) for a process via `ps`.
///
/// RSS includes pages the allocator has freed but not yet returned to the OS.
/// Use alongside `AllocStats::allocated` for a complete picture:
/// `rss - allocated` ≈ allocator headroom + page rounding.
///
/// Returns `None` if the process no longer exists.
fn rss_bytes(pid: u32) -> Option<u64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&out.stdout);
    let kb: u64 = s.trim().parse().ok()?;
    Some(kb * 1024)
}

/// Jemalloc allocator stats from a node's `--alloc-stats-port`.
#[derive(Debug, Default, Clone, Copy)]
struct AllocStats {
    /// Bytes currently held by live allocations (the authoritative
    /// "how much is the app using?" metric — no OS reclaim-timing noise).
    allocated: u64,
    /// Bytes in jemalloc active pages (allocated + alignment rounding).
    active: u64,
    /// Bytes in resident pages from jemalloc's perspective.
    resident: u64,
    /// Bytes retained by jemalloc from the OS after freeing.
    retained: u64,
}

/// Reads jemalloc stats from a node's allocator stats port.
///
/// Connects to `127.0.0.1:{port}`, reads one line, parses key=value pairs.
/// Returns `None` on any error (connection refused, parse failure, etc.).
fn read_alloc_stats(port: u16) -> Option<AllocStats> {
    use std::io::Read as _;
    let mut stream =
        std::net::TcpStream::connect(format!("127.0.0.1:{port}")).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .ok()?;
    let mut s = String::new();
    stream.read_to_string(&mut s).ok()?;

    let mut stats = AllocStats::default();
    for kv in s.split_whitespace() {
        let mut parts = kv.splitn(2, '=');
        let key = parts.next().unwrap_or("");
        let val: u64 = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
        match key {
            "allocated" => stats.allocated = val,
            "active" => stats.active = val,
            "resident" => stats.resident = val,
            "retained" => stats.retained = val,
            _ => {}
        }
    }
    Some(stats)
}

/// Prints and returns per-node and total jemalloc `allocated` bytes.
///
/// Also prints `active` and `resident` for context. `allocated` is the
/// primary metric: bytes currently held by live Rust objects.
fn print_alloc_stats(label: &str, node_count: u16, cluster: &helix_workload::RealCluster) -> u64 {
    let mut total_allocated: u64 = 0;
    println!("[alloc] {label}:");
    for node_id in 1..=node_count {
        let port = cluster.alloc_stats_port(u32::from(node_id));
        let stats = port.and_then(read_alloc_stats).unwrap_or_default();
        total_allocated += stats.allocated;
        println!(
            "[alloc]   node-{node_id}: allocated={:.1} MB  active={:.1} MB  \
             resident={:.1} MB  retained={:.1} MB",
            stats.allocated as f64 / 1024.0 / 1024.0,
            stats.active as f64 / 1024.0 / 1024.0,
            stats.resident as f64 / 1024.0 / 1024.0,
            stats.retained as f64 / 1024.0 / 1024.0,
        );
    }
    println!(
        "[alloc]   total allocated: {:.1} MB",
        total_allocated as f64 / 1024.0 / 1024.0,
    );
    total_allocated
}

fn total_rss(pids: &[u32]) -> u64 {
    pids.iter()
        .map(|&pid| {
            rss_bytes(pid).unwrap_or_else(|| {
                panic!("helix-server process {pid} disappeared during test");
            })
        })
        .sum()
}

fn print_rss(label: &str, pids: &[u32]) -> u64 {
    let total = total_rss(pids);
    println!(
        "[memory] {label}: total={:.1} MB",
        total as f64 / 1024.0 / 1024.0
    );
    for (i, &pid) in pids.iter().enumerate() {
        let node_rss = rss_bytes(pid).unwrap_or(0);
        println!(
            "[memory]   node-{} (pid {}): {:.1} MB",
            i + 1,
            pid,
            node_rss as f64 / 1024.0 / 1024.0,
        );
    }
    total
}

/// Write `total_bytes` through `executor` using `INFLIGHT` concurrent sends.
async fn write_data(executor: &RealExecutor, topic: &str, total_bytes: u64) {
    write_data_sized(executor, topic, total_bytes, MSG_SIZE).await;
}

/// Write `total_bytes` using blobs of `msg_size` bytes with 32 concurrent sends.
async fn write_data_sized(executor: &RealExecutor, topic: &str, total_bytes: u64, msg_size: usize) {
    let payload = Bytes::from(vec![0xABu8; msg_size]);
    let msg_count = total_bytes / msg_size as u64;
    let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
    const INFLIGHT: usize = 32;
    let mut dispatched: u64 = 0;

    loop {
        while futs.len() < INFLIGHT && dispatched < msg_count {
            futs.push(executor.send(topic, 0, payload.clone()));
            dispatched += 1;
        }
        if futs.is_empty() {
            break;
        }
        futs.next().await;
    }
}

// ---------------------------------------------------------------------------
// Test 1: default retention (no compaction in test window)
// ---------------------------------------------------------------------------

/// Verifies that the Raft log trailing window is bounded.
///
/// Writes data in three phases and measures `allocated` after each one.
/// The critical invariant: once the trailing window is full (after phase 1),
/// further writes must not grow `allocated`. If phase 3 shows significant
/// growth compared to phase 2, `compact_log` is not working correctly.
///
/// Phase sizes are chosen so that phase 1 definitively fills the 1,000-entry
/// window, and phases 2 and 3 each represent additional complete window-fulls
/// of data — any growth in those phases indicates unbounded accumulation.
#[tokio::test]
async fn measure_memory_growth() {
    let node_count = 3u16;
    let topic = "memory-growth";

    let base_port = find_available_base_port(25200, node_count);
    let raft_base_port = find_available_base_port(35200, node_count);
    let alloc_stats_base_port = find_available_base_port(45200, node_count);

    let cluster = RealCluster::builder()
        .nodes(u32::from(node_count))
        .base_port(base_port)
        .raft_base_port(raft_base_port)
        .binary_path(binary_path())
        .data_dir(test_data_dir("memory_growth"))
        .auto_create_topics(true)
        .default_replication_factor(3)
        .topic(topic, 1)
        .log_level("warn")
        .alloc_stats_base_port(alloc_stats_base_port)
        .build()
        .expect("failed to start cluster");

    let pids = cluster.node_pids();

    let executor = RealExecutor::with_mode(
        cluster.bootstrap_servers(),
        ProducerMode::LowLatency,
    )
    .expect("failed to create executor");

    executor
        .wait_ready(Duration::from_secs(30))
        .await
        .expect("cluster not ready within 30s");

    // Baseline: cluster running idle, no writes yet.
    let alloc_baseline = print_alloc_stats("baseline (no writes)", node_count, &cluster);
    print_rss("rss baseline", &pids);

    // Phase 1: write 100 MB. This fills the Raft trailing window (1,000 entries).
    let t0 = std::time::Instant::now();
    println!("[memory] phase 1: writing {} MB", TOTAL_BYTES / 1024 / 1024);
    write_data(&executor, topic, TOTAL_BYTES).await;
    println!("[memory] phase 1 done in {:.1}s", t0.elapsed().as_secs_f64());
    tokio::time::sleep(Duration::from_secs(2)).await;

    let alloc_p1 = print_alloc_stats("after phase 1 (100 MB)", node_count, &cluster);
    print_rss("rss after phase 1", &pids);
    let growth_p1 = alloc_p1.saturating_sub(alloc_baseline);

    // Phase 2: write another 100 MB. Window already full — should add near-zero.
    let t0 = std::time::Instant::now();
    println!("[memory] phase 2: writing {} MB more", TOTAL_BYTES / 1024 / 1024);
    write_data(&executor, topic, TOTAL_BYTES).await;
    println!("[memory] phase 2 done in {:.1}s", t0.elapsed().as_secs_f64());
    tokio::time::sleep(Duration::from_secs(2)).await;

    let alloc_p2 = print_alloc_stats("after phase 2 (200 MB total)", node_count, &cluster);
    print_rss("rss after phase 2", &pids);
    let growth_p2 = alloc_p2.saturating_sub(alloc_p1);

    // Phase 3: write a third 100 MB batch to confirm steady state.
    let t0 = std::time::Instant::now();
    println!("[memory] phase 3: writing {} MB more", TOTAL_BYTES / 1024 / 1024);
    write_data(&executor, topic, TOTAL_BYTES).await;
    println!("[memory] phase 3 done in {:.1}s", t0.elapsed().as_secs_f64());
    tokio::time::sleep(Duration::from_secs(2)).await;

    let alloc_p3 = print_alloc_stats("after phase 3 (300 MB total)", node_count, &cluster);
    print_rss("rss after phase 3", &pids);
    let growth_p3 = alloc_p3.saturating_sub(alloc_p2);

    println!(
        "[memory] SUMMARY (allocated growth per phase):\n\
         [memory]   phase 1 (fill window):     {:+.1} MB\n\
         [memory]   phase 2 (window already full): {:+.1} MB  ← should be ~0\n\
         [memory]   phase 3 (steady state):    {:+.1} MB  ← should be ~0",
        growth_p1 as f64 / 1024.0 / 1024.0,
        growth_p2 as f64 / 1024.0 / 1024.0,
        growth_p3 as f64 / 1024.0 / 1024.0,
    );
}

// ---------------------------------------------------------------------------
// Test 2: short retention (BlobIndex should get compacted)
// ---------------------------------------------------------------------------

/// Measures RSS and allocated-bytes growth with 1s local retention.
///
/// WAL segments become eligible for deletion 1 second after sealing.
/// The retention task fires every 60 seconds. Comparing `allocated` before and
/// after retention cycles reveals exactly how much BlobIndex compaction reclaims.
/// Comparing with `measure_memory_growth` shows how much BlobIndex contributes.
#[tokio::test]
async fn measure_memory_growth_with_retention() {
    let node_count = 3u16;
    let topic = "memory-growth-ret";

    let base_port = find_available_base_port(25300, node_count);
    let raft_base_port = find_available_base_port(35300, node_count);
    let alloc_stats_base_port = find_available_base_port(45300, node_count);

    let cluster = RealCluster::builder()
        .nodes(u32::from(node_count))
        .base_port(base_port)
        .raft_base_port(raft_base_port)
        .binary_path(binary_path())
        .data_dir(test_data_dir("memory_growth_retention"))
        .auto_create_topics(true)
        .default_replication_factor(3)
        .topic(topic, 1)
        .log_level("warn")
        .local_retention_ms(1_000) // 1 second — segments eligible immediately
        .alloc_stats_base_port(alloc_stats_base_port)
        .build()
        .expect("failed to start cluster");

    let pids = cluster.node_pids();

    let executor = RealExecutor::with_mode(
        cluster.bootstrap_servers(),
        ProducerMode::LowLatency,
    )
    .expect("failed to create executor");

    executor
        .wait_ready(Duration::from_secs(30))
        .await
        .expect("cluster not ready within 30s");

    // Warm-up: write 10 MB and let allocator settle.
    println!("[memory_ret] warm-up: writing 10 MB");
    write_data(&executor, topic, 10 * 1024 * 1024).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let rss_before = print_rss("rss after warm-up", &pids);
    let alloc_before = print_alloc_stats("alloc after warm-up", node_count, &cluster);

    // Write 100 MB.
    let t0 = std::time::Instant::now();
    println!("[memory_ret] writing {} MB", TOTAL_BYTES / 1024 / 1024);
    write_data(&executor, topic, TOTAL_BYTES).await;
    println!("[memory_ret] write done in {:.1}s", t0.elapsed().as_secs_f64());

    // Snapshot right after writes, before retention fires.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let alloc_post_write = print_alloc_stats("alloc after write (pre-retention)", node_count, &cluster);

    // Retention task fires every 60 seconds. Wait for 2 cycles to ensure
    // all sealed segments have been deleted and BlobIndex trimmed.
    println!("[memory_ret] waiting 130s for 2 retention cycles...");
    tokio::time::sleep(Duration::from_secs(130)).await;

    let rss_after = print_rss("rss after 100 MB write + 2 retention cycles", &pids);
    let alloc_after = print_alloc_stats("alloc after 2 retention cycles", node_count, &cluster);

    let rss_growth = rss_after.saturating_sub(rss_before);
    let alloc_growth = alloc_after.saturating_sub(alloc_before);
    let retention_freed = alloc_post_write.saturating_sub(alloc_after);
    println!(
        "[memory_ret] GROWTH (allocated): {:.1} MB | GROWTH (rss): {:.1} MB | \
         freed by retention: {:.1} MB  (writing {} MB, 1s retention)",
        alloc_growth as f64 / 1024.0 / 1024.0,
        rss_growth as f64 / 1024.0 / 1024.0,
        retention_freed as f64 / 1024.0 / 1024.0,
        TOTAL_BYTES / 1024 / 1024,
    );
}

// ---------------------------------------------------------------------------
// Test 3: encode_split savings — large-blob Raft log window
// ---------------------------------------------------------------------------

/// Measures peak allocated bytes with 128 KB blobs to quantify encode_split savings.
///
/// Uses a SINGLE-NODE cluster so writes go through the direct `AppendBlob`
/// path (no batcher). For AppendBlob, `encode_split()` passes the blob to
/// Raft as a refcount clone (zero copy); `encode()` copies the full blob
/// into a fresh `BytesMut`. The Raft log trailing window is capped at 64 MB,
/// so 512 × 128 KB = 64 MB of blob data lives in the log.
///
///   encode()       → 64 MB extra per node in Raft log entries
///   encode_split() → 0 extra (refcount clone; same Arc shared with WAL write)
///
/// Run this test twice (once with the current binary, once with `encode()`
/// patched back in `partition_actor.rs::handle_propose`) and compare
/// "after write" alloc totals.
#[tokio::test]
async fn measure_encode_split_savings() {
    // Single-node: no batcher, writes go through AppendBlob directly.
    // encode_split() makes the blob payload a refcount clone (zero copy).
    const BLOB_SIZE: usize = 128 * 1024; // 128 KB — typical Kafka RecordBatch
    const WRITE_BYTES: u64 = 128 * 1024 * 1024; // 128 MB total (1024 blobs)
    let node_count = 1u16;
    let topic = "encode-split-proof";

    let base_port = find_available_base_port(25400, node_count);
    let raft_base_port = find_available_base_port(35400, node_count);
    let alloc_stats_base_port = find_available_base_port(45400, node_count);

    let cluster = RealCluster::builder()
        .nodes(u32::from(node_count))
        .base_port(base_port)
        .raft_base_port(raft_base_port)
        .binary_path(binary_path())
        .data_dir(test_data_dir("encode_split_proof"))
        .auto_create_topics(true)
        .default_replication_factor(1)
        .topic(topic, 1)
        .log_level("warn")
        .alloc_stats_base_port(alloc_stats_base_port)
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

    // The Raft trailing window holds min(1000 entries, 64 MB).
    // At 128 KB/entry: min(1000, 64 MB / 128 KB = 512) = 512 entries = 64 MB.
    let trailing_window_mb = {
        let bytes_limit = 64u64;
        let entries_limit = 1000u64;
        let entries_per_mb = 1024 * 1024 / BLOB_SIZE as u64;
        let window_entries = entries_limit.min(bytes_limit * entries_per_mb);
        window_entries * BLOB_SIZE as u64 / 1024 / 1024
    };

    println!(
        "[encode_split] single-node | blob_size={} KB | total_write={} MB",
        BLOB_SIZE / 1024,
        WRITE_BYTES / 1024 / 1024,
    );
    println!(
        "[encode_split] Raft trailing window: ~{trailing_window_mb} MB of blob data",
    );
    println!(
        "[encode_split] Expected extra allocated with encode():    ~{trailing_window_mb} MB \
         (fresh BytesMut per entry)",
    );
    println!(
        "[encode_split] Expected extra allocated with encode_split(): ~0 MB \
         (refcount clone, no extra alloc)",
    );

    // Baseline: idle cluster.
    let alloc_baseline = print_alloc_stats("baseline (idle)", node_count, &cluster);

    // Write 128 MB with 128 KB blobs.
    println!(
        "[encode_split] writing {} MB in {} KB blobs...",
        WRITE_BYTES / 1024 / 1024,
        BLOB_SIZE / 1024,
    );
    let t0 = std::time::Instant::now();
    write_data_sized(&executor, topic, WRITE_BYTES, BLOB_SIZE).await;
    println!("[encode_split] done in {:.1}s", t0.elapsed().as_secs_f64());
    tokio::time::sleep(Duration::from_secs(2)).await;

    let alloc_post = print_alloc_stats("after write (window full)", node_count, &cluster);
    let growth = alloc_post.saturating_sub(alloc_baseline);

    println!(
        "[encode_split] allocated growth = {:.1} MB",
        growth as f64 / 1024.0 / 1024.0,
    );
    println!(
        "[encode_split] encode_split savings vs encode() = ~{trailing_window_mb} MB \
         (Raft trailing window × blob_size, no extra copies)"
    );
}
