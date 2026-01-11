//! Simple BufferedWal benchmark for comparison.
//!
//! Run with: cargo run --release --example simple_bench

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::task::JoinSet;

use helix_wal::{BufferedWal, BufferedWalConfig, Entry, TokioStorage, WalConfig};

/// Benchmark with shared atomic counter (entries arrive out of order).
async fn run_bench_shared_counter(concurrency: usize, entries_per_task: usize, data_size: usize) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let wal_config = WalConfig::new(temp_dir.path());
    let config = BufferedWalConfig::new(wal_config)
        .with_flush_interval(Duration::from_millis(10))
        .with_max_buffer_entries(100_000);

    let wal = Arc::new(
        BufferedWal::open(TokioStorage::new(), config)
            .await
            .expect("open WAL"),
    );

    let data = Bytes::from(vec![0u8; data_size]);
    let counter = Arc::new(std::sync::atomic::AtomicU64::new(1));
    let total_entries = concurrency * entries_per_task;

    let start = Instant::now();

    let mut join_set = JoinSet::new();
    for _ in 0..concurrency {
        let wal_clone = wal.clone();
        let data_clone = data.clone();
        let counter_clone = counter.clone();

        join_set.spawn(async move {
            for _ in 0..entries_per_task {
                let idx = counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let entry = Entry::new(1, idx, data_clone.clone()).expect("entry");
                wal_clone.append(entry).await.expect("append");
            }
        });
    }

    while join_set.join_next().await.is_some() {}
    wal.flush().await.expect("flush");

    let elapsed = start.elapsed();
    let throughput = total_entries as f64 / elapsed.as_secs_f64();

    println!(
        "shared_ctr: conc {:>5} | entries {:>6} | {:>8.2}ms | {:>10.0}/sec",
        concurrency,
        total_entries,
        elapsed.as_secs_f64() * 1000.0,
        throughput
    );
}

/// Benchmark with separate WAL per partition (realistic production pattern).
/// Pre-creates WALs to measure steady-state write throughput.
#[allow(dead_code)]
async fn run_bench_wal_per_partition(num_partitions: usize, entries_per_partition: usize, data_size: usize) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data = Bytes::from(vec![0u8; data_size]);
    let total_entries = num_partitions * entries_per_partition;

    // Pre-create all WALs before timing (this is setup, not the benchmark)
    let mut wals = Vec::with_capacity(num_partitions);
    for partition_id in 0..num_partitions {
        let partition_dir = temp_dir.path().join(format!("partition_{partition_id}"));
        std::fs::create_dir_all(&partition_dir).expect("create partition dir");

        let wal_config = WalConfig::new(&partition_dir);
        let config = BufferedWalConfig::new(wal_config)
            .with_flush_interval(Duration::from_millis(10))
            .with_max_buffer_entries(10_000);

        let wal = Arc::new(
            BufferedWal::open(TokioStorage::new(), config)
                .await
                .expect("open WAL"),
        );
        wals.push(wal);
    }

    // Now benchmark just the writes (steady-state)
    let start = Instant::now();

    let mut join_set = JoinSet::new();
    for (partition_id, wal) in wals.into_iter().enumerate() {
        let data_clone = data.clone();

        join_set.spawn(async move {
            // Each partition writes sequential indices 1, 2, 3...
            for i in 1..=entries_per_partition {
                let entry = Entry::new(1, i as u64, data_clone.clone()).expect("entry");
                wal.append(entry).await.expect("append");
            }
            wal.flush().await.expect("flush");
            (partition_id, wal)
        });
    }

    while join_set.join_next().await.is_some() {}

    let elapsed = start.elapsed();
    let throughput = total_entries as f64 / elapsed.as_secs_f64();

    println!(
        "wal/partit: part {:>5} | entries {:>6} | {:>8.2}ms | {:>10.0}/sec",
        num_partitions,
        total_entries,
        elapsed.as_secs_f64() * 1000.0,
        throughput
    );
}

/// Benchmark with few WALs, many entries each (simulates per-core model).
/// This is closer to how high-throughput systems with thread-per-core designs operate.
async fn run_bench_few_wals_many_entries(num_wals: usize, entries_per_wal: usize, data_size: usize) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data = Bytes::from(vec![0u8; data_size]);
    let total_entries = num_wals * entries_per_wal;

    // Pre-create WALs
    let mut wals = Vec::with_capacity(num_wals);
    for wal_id in 0..num_wals {
        let wal_dir = temp_dir.path().join(format!("wal_{wal_id}"));
        std::fs::create_dir_all(&wal_dir).expect("create wal dir");

        let wal_config = WalConfig::new(&wal_dir);
        let config = BufferedWalConfig::new(wal_config)
            .with_flush_interval(Duration::from_millis(10))
            .with_max_buffer_entries(100_000);

        let wal = Arc::new(
            BufferedWal::open(TokioStorage::new(), config)
                .await
                .expect("open WAL"),
        );
        wals.push(wal);
    }

    // Benchmark writes
    let start = Instant::now();

    let mut join_set = JoinSet::new();
    for wal in wals.into_iter() {
        let data_clone = data.clone();

        join_set.spawn(async move {
            for i in 1..=entries_per_wal {
                let entry = Entry::new(1, i as u64, data_clone.clone()).expect("entry");
                wal.append(entry).await.expect("append");
            }
            wal.flush().await.expect("flush");
        });
    }

    while join_set.join_next().await.is_some() {}

    let elapsed = start.elapsed();
    let throughput = total_entries as f64 / elapsed.as_secs_f64();

    println!(
        "few_wals:   wals {:>5} | entries {:>6} | {:>8.2}ms | {:>10.0}/sec",
        num_wals,
        total_entries,
        elapsed.as_secs_f64() * 1000.0,
        throughput
    );
}

#[tokio::main]
async fn main() {
    println!("BufferedWal Benchmark (10ms flush, 1KB records)");
    println!("================================================\n");

    // Warm up
    println!("Warming up...");
    run_bench_few_wals_many_entries(8, 1000, 1024).await;

    println!("\n--- Few WALs, Many Entries (per-core model) ---");
    println!("Fixed WAL count, scaling entries. Similar to thread-per-core designs.\n");

    // 8 WALs (like 8 CPU cores), increasing entries
    for &entries_per_wal in &[1000, 4000, 16000] {
        for _ in 0..3 {
            run_bench_few_wals_many_entries(8, entries_per_wal, 1024).await;
        }
        println!();
    }

    println!("\n--- Shared Counter Mode (single WAL, random arrival) ---");
    println!("Single WAL with entries arriving out of order.\n");

    for &concurrency in &[64, 256, 1024] {
        for _ in 0..3 {
            run_bench_shared_counter(concurrency, 100, 1024).await;
        }
        println!();
    }
}
