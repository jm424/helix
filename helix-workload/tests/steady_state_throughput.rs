//! Steady-state throughput test for helix-workload.
//!
//! Spawns a real Helix cluster and uses rdkafka producer(s) to measure
//! steady-state throughput over a fixed window.
//!
//! # Usage
//!
//! Run with a built-in profile:
//! ```bash
//! HELIX_PROFILE=throughput cargo test -p helix-workload --test steady_state_throughput
//! ```
//!
//! Available profiles: baseline, throughput, latency, stress, sustained
//!
//! Or load a custom profile from a TOML file:
//! ```bash
//! HELIX_PROFILE_FILE=my_profile.toml cargo test ...
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use helix_workload::profiles::{self, BenchmarkProfile};
use helix_workload::{ProducerMode, RealCluster, RealExecutor, WorkloadExecutor};
use rand::RngCore;

fn binary_path() -> PathBuf {
    let release = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/release/helix-server");
    if release.exists() {
        return release;
    }

    let debug = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/debug/helix-server");
    if debug.exists() {
        return debug;
    }

    panic!(
        "helix-server binary not found. Run `cargo build --release -p helix-server` first.\n\
         Checked:\n  - {}\n  - {}",
        release.display(),
        debug.display()
    );
}

fn test_data_dir(test_name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("helix-workload-tests")
        .join(test_name)
        .join(format!("{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn find_available_base_port(start: u16, count: u16) -> u16 {
    let max_base = u16::MAX.saturating_sub(count);
    for base in start..=max_base {
        let mut available = true;
        for offset in 1..=count {
            let port = base.saturating_add(offset);
            let addr = format!("127.0.0.1:{port}");
            if std::net::TcpListener::bind(&addr).is_err() {
                available = false;
                break;
            }
        }
        if available {
            return base;
        }
    }
    panic!("unable to find available port block (count={count}) starting at {start}");
}

/// Load benchmark profile from environment.
///
/// Priority:
/// 1. `HELIX_PROFILE_FILE` - path to a TOML file
/// 2. `HELIX_PROFILE` - name of a built-in profile
/// 3. Default: "throughput" profile
fn load_profile_from_env() -> BenchmarkProfile {
    // Check for custom file first
    if let Ok(path) = std::env::var("HELIX_PROFILE_FILE") {
        return BenchmarkProfile::from_file(&path)
            .unwrap_or_else(|e| panic!("failed to load profile from {path}: {e}"));
    }

    // Check for named profile
    // Default to "baseline" for CI stability. Use HELIX_PROFILE=throughput for performance testing.
    let profile_name = std::env::var("HELIX_PROFILE").unwrap_or_else(|_| "baseline".to_string());
    profiles::load_profile(&profile_name).unwrap_or_else(|_| {
        let available = profiles::list_profiles().join(", ");
        panic!("unknown profile '{profile_name}'. Available: {available}");
    })
}

async fn run_phase(
    executor: Arc<RealExecutor>,
    topic: Arc<String>,
    payload: Bytes,
    inflight: usize,
    duration: Duration,
    success: Option<&Arc<AtomicU64>>,
    errors: Option<&Arc<AtomicU64>>,
    error_samples: Option<&Arc<std::sync::Mutex<HashMap<String, u64>>>>,
) {
    let end = std::time::Instant::now() + duration;
    let mut inflight_set: FuturesUnordered<
        tokio::task::JoinHandle<Result<u64, helix_workload::ExecutorError>>,
    > = FuturesUnordered::new();

    while std::time::Instant::now() < end || !inflight_set.is_empty() {
        while inflight_set.len() < inflight && std::time::Instant::now() < end {
            let exec = Arc::clone(&executor);
            let payload = payload.clone();
            let topic = Arc::clone(&topic);
            inflight_set.push(tokio::spawn(async move {
                exec.send(&topic, 0, payload).await
            }));
        }

        if let Some(result) = inflight_set.next().await {
            match result {
                Ok(Ok(_)) => {
                    if let Some(counter) = success {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(Err(e)) => {
                    if let Some(counter) = errors {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    if let Some(samples) = error_samples {
                        let mut map = samples.lock().unwrap();
                        let mut key = e.to_string();
                        if key.len() > 200 {
                            key.truncate(200);
                        }
                        *map.entry(key).or_insert(0) += 1;
                    }
                }
                Err(e) => {
                    if let Some(counter) = errors {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    if let Some(samples) = error_samples {
                        let mut map = samples.lock().unwrap();
                        let key = format!("join_error: {e}");
                        *map.entry(key).or_insert(0) += 1;
                    }
                }
            }
        }
    }
}

/// Steady-state throughput test (real Helix server + rdkafka).
#[tokio::test]
async fn test_single_partition_steady_state_throughput() {
    let profile = load_profile_from_env();

    // Extract profile settings
    let producer_count = profile.producer.threads;
    let inflight = profile.producer.inflight;
    let message_size = profile.producer.message_size;
    let warmup_secs = profile.execution.warmup_secs;
    let duration_secs = profile.execution.duration_secs;
    let node_count = profile.cluster.nodes as u16;

    // Producer mode based on linger_ms
    let producer_mode = if profile.producer.linger_ms == 0 {
        ProducerMode::LowLatency
    } else {
        ProducerMode::HighThroughput
    };

    let base_port = std::env::var("HELIX_STEADY_BASE_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or_else(|| find_available_base_port(21000, node_count));
    let raft_base_port = std::env::var("HELIX_STEADY_RAFT_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or_else(|| find_available_base_port(31000, node_count));

    let topic = "throughput-steady-state";
    let topic_name = Arc::new(topic.to_string());

    let report_dir = test_data_dir("steady_state_reports");
    std::fs::create_dir_all(&report_dir).expect("failed to create report dir");
    let report_template = report_dir.join("batcher-stats-{node_id}.json");
    std::env::set_var(
        "HELIX_BENCH_REPORT_PATH",
        report_template.to_string_lossy().to_string(),
    );
    std::env::set_var("HELIX_BENCH_REPORT_INTERVAL_MS", "1000");

    // Configure producer based on profile
    if profile.producer.compression != "none" {
        std::env::set_var("HELIX_USE_LIBSTREAMING_DEFAULTS", "1");
        std::env::set_var("HELIX_LIBSTREAMING_COMPRESSION", &profile.producer.compression);
    }

    let cluster = RealCluster::builder()
        .nodes(u32::from(node_count))
        .base_port(base_port)
        .raft_base_port(raft_base_port)
        .binary_path(binary_path())
        .data_dir(test_data_dir("steady_state_throughput"))
        .auto_create_topics(true)
        .default_replication_factor(profile.cluster.replication_factor)
        .topic(topic, profile.cluster.partitions)
        .build()
        .expect("failed to start cluster");

    let wait_executor = RealExecutor::with_mode(cluster.bootstrap_servers(), producer_mode)
        .expect("failed to create executor");
    wait_executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    let success = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let error_samples = Arc::new(std::sync::Mutex::new(HashMap::new()));

    // Use random data to avoid compression artifacts.
    let mut payload_data = vec![0u8; message_size];
    rand::thread_rng().fill_bytes(&mut payload_data);
    let payload = Bytes::from(payload_data);

    // Print profile info
    println!("=== Benchmark Profile: {} ===", profile.name);
    println!("{}", profile.description);
    println!();
    println!("Producer:");
    println!("  threads: {}", profile.producer.threads);
    println!("  inflight: {}", profile.producer.inflight);
    println!("  message_size: {} bytes", profile.producer.message_size);
    println!("  linger_ms: {}", profile.producer.linger_ms);
    println!("  batch_size: {} bytes", profile.producer.batch_size);
    println!("  compression: {}", profile.producer.compression);
    println!();
    println!("Cluster:");
    println!("  nodes: {}", profile.cluster.nodes);
    println!("  replication_factor: {}", profile.cluster.replication_factor);
    println!("  partitions: {}", profile.cluster.partitions);
    println!();
    println!("Execution:");
    println!("  warmup: {}s", profile.execution.warmup_secs);
    println!("  duration: {}s", profile.execution.duration_secs);
    println!();

    let warmup_duration = Duration::from_secs(warmup_secs);
    let measure_duration = Duration::from_secs(duration_secs);
    let ready_barrier = Arc::new(tokio::sync::Barrier::new(producer_count + 1));
    let start_barrier = Arc::new(tokio::sync::Barrier::new(producer_count + 1));
    let mut handles = Vec::new();
    let bootstrap_servers = cluster.bootstrap_servers().to_string();

    for _ in 0..producer_count {
        let bootstrap = bootstrap_servers.clone();
        let payload = payload.clone();
        let ready_barrier = Arc::clone(&ready_barrier);
        let start_barrier = Arc::clone(&start_barrier);
        let success = Arc::clone(&success);
        let errors = Arc::clone(&errors);
        let topic_name = Arc::clone(&topic_name);
        let error_samples = Arc::clone(&error_samples);

        let handle = tokio::spawn(async move {
            let executor = Arc::new(
                RealExecutor::with_mode(&bootstrap, producer_mode)
                    .expect("failed to create producer executor"),
            );

            run_phase(
                Arc::clone(&executor),
                Arc::clone(&topic_name),
                payload.clone(),
                inflight,
                warmup_duration,
                None,
                None,
                None,
            )
            .await;

            ready_barrier.wait().await;
            start_barrier.wait().await;

            run_phase(
                Arc::clone(&executor),
                Arc::clone(&topic_name),
                payload,
                inflight,
                measure_duration,
                Some(&success),
                Some(&errors),
                Some(&error_samples),
            )
            .await;
        });
        handles.push(handle);
    }

    ready_barrier.wait().await;
    let measure_start = std::time::Instant::now();
    start_barrier.wait().await;
    for handle in handles {
        handle.await.expect("producer task panicked");
    }
    let measure_elapsed = measure_start.elapsed();

    let success_count = success.load(Ordering::Relaxed);
    let error_count = errors.load(Ordering::Relaxed);
    let throughput_ops = success_count as f64 / measure_elapsed.as_secs_f64();
    let throughput_bytes =
        (success_count as f64 * message_size as f64) / measure_elapsed.as_secs_f64();

    println!("=== Results ===");
    println!("Measured duration: {:.2}s", measure_elapsed.as_secs_f64());
    println!("Successful: {success_count}");
    println!("Errors: {error_count}");
    println!("Throughput: {:.1} ops/sec", throughput_ops);
    println!("Throughput: {:.1} bytes/sec ({:.1} MB/s)", throughput_bytes, throughput_bytes / 1_000_000.0);
    println!("=== Batcher Stats ===");
    for node_id in 1..=node_count {
        let report_path = report_dir.join(format!("batcher-stats-{node_id}.json"));
        match std::fs::read_to_string(&report_path) {
            Ok(contents) => {
                println!("Node {node_id}: {contents}");
            }
            Err(e) => {
                println!("Node {node_id}: report missing ({e})");
            }
        }
    }
    println!("=== Error Summary ===");
    let samples = error_samples.lock().unwrap();
    if samples.is_empty() {
        println!("No errors");
    } else {
        let mut entries: Vec<_> = samples.iter().collect();
        entries.sort_by_key(|(_, count)| std::cmp::Reverse(**count));
        for (idx, (msg, count)) in entries.into_iter().take(5).enumerate() {
            println!("{}. count={} error={}", idx + 1, count, msg);
        }
    }

    let allow_errors = std::env::var("HELIX_ALLOW_ERRORS").ok().as_deref() == Some("1");
    if !allow_errors {
        assert!(error_count == 0, "Expected no errors, got {error_count}");
    }
    assert!(success_count > 0, "No successful operations recorded");
}
