//! WAL write benchmarks.
//!
//! Measures WAL append throughput and latency under various configurations.

#![allow(missing_docs)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use hdrhistogram::Histogram;
use tempfile::TempDir;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::Instant;

use helix_wal::{Entry, TokioStorage, Wal, WalConfig};

/// Benchmark configuration.
#[derive(Clone, Debug)]
struct BenchConfig {
    /// Size of each entry payload in bytes.
    data_size: usize,
    /// Number of entries to write per iteration.
    batch_size: usize,
    /// Whether to sync after each batch.
    sync_after_batch: bool,
}

impl BenchConfig {
    fn name(&self) -> String {
        let sync = if self.sync_after_batch { "_sync" } else { "" };
        format!("size_{}_batch_{}{}", self.data_size, self.batch_size, sync)
    }
}

/// Creates a temporary WAL for benchmarking.
async fn setup_wal(sync_on_write: bool) -> (Wal<TokioStorage>, TempDir) {
    let tempdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = WalConfig::new(tempdir.path()).with_sync_on_write(sync_on_write);
    let wal = Wal::open(TokioStorage::new(), config)
        .await
        .expect("failed to open WAL");
    (wal, tempdir)
}

/// Benchmark sequential WAL writes (single writer).
///
/// This measures raw WAL append performance without concurrency overhead.
fn bench_wal_sequential(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .expect("failed to build runtime");

    // Test configurations: data sizes and batch sizes.
    let data_sizes = vec![64, 256, 1024, 4096];
    let batch_sizes = vec![100, 1000, 10000];

    let mut group = c.benchmark_group("wal_sequential");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for &data_size in &data_sizes {
        for &batch_size in &batch_sizes {
            let config = BenchConfig {
                data_size,
                batch_size,
                sync_after_batch: false,
            };

            group.throughput(Throughput::Elements(batch_size as u64));

            group.bench_with_input(
                BenchmarkId::new("no_sync", config.name()),
                &config,
                |b, cfg| {
                    let data = Bytes::from(vec![0u8; cfg.data_size]);

                    b.iter_custom(|iters| {
                        rt.block_on(async {
                            let total_start = Instant::now();

                            for _ in 0..iters {
                                let (mut wal, _tmp) = setup_wal(false).await;

                                for i in 0..cfg.batch_size {
                                    let entry = Entry::new(1, (i + 1) as u64, data.clone())
                                        .expect("entry creation failed");
                                    black_box(wal.append(entry).await.expect("append failed"));
                                }
                            }

                            total_start.elapsed()
                        })
                    });
                },
            );

            // With sync after batch.
            let config_sync = BenchConfig {
                data_size,
                batch_size,
                sync_after_batch: true,
            };

            group.bench_with_input(
                BenchmarkId::new("with_sync", config_sync.name()),
                &config_sync,
                |b, cfg| {
                    let data = Bytes::from(vec![0u8; cfg.data_size]);

                    b.iter_custom(|iters| {
                        rt.block_on(async {
                            let total_start = Instant::now();

                            for _ in 0..iters {
                                let (mut wal, _tmp) = setup_wal(false).await;

                                for i in 0..cfg.batch_size {
                                    let entry = Entry::new(1, (i + 1) as u64, data.clone())
                                        .expect("entry creation failed");
                                    black_box(wal.append(entry).await.expect("append failed"));
                                }
                                // Sync at end of batch.
                                wal.sync().await.expect("sync failed");
                            }

                            total_start.elapsed()
                        })
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark concurrent WAL writes using multiple independent WALs.
///
/// This simulates Kafka's partition model where each partition has its own WAL.
/// Multiple partitions can be written to concurrently.
fn bench_wal_multi_partition(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to build runtime");

    let partition_counts = vec![1, 4, 8, 16];
    let entries_per_partition = 1000;
    let data_size = 1024;

    let mut group = c.benchmark_group("wal_multi_partition");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for &partitions in &partition_counts {
        let total_entries = partitions * entries_per_partition;
        group.throughput(Throughput::Elements(total_entries as u64));

        group.bench_with_input(
            BenchmarkId::new("partitions", partitions),
            &partitions,
            |b, &partition_count| {
                let data = Bytes::from(vec![0u8; data_size]);

                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let total_start = Instant::now();

                        for _ in 0..iters {
                            let mut join_set = JoinSet::new();

                            // Create a WAL per partition and write concurrently.
                            for _ in 0..partition_count {
                                let data_clone = data.clone();
                                join_set.spawn(async move {
                                    let (mut wal, _tmp) = setup_wal(false).await;

                                    for i in 0..entries_per_partition {
                                        let entry =
                                            Entry::new(1, (i + 1) as u64, data_clone.clone())
                                                .expect("entry creation failed");
                                        wal.append(entry).await.expect("append failed");
                                    }
                                    wal.sync().await.expect("sync failed");
                                });
                            }

                            // Wait for all partitions.
                            while join_set.join_next().await.is_some() {}
                        }

                        total_start.elapsed()
                    })
                });
            },
        );
    }

    group.finish();
}

/// Benchmark with latency percentiles.
///
/// Uses a shared WAL with mutex for concurrent access and tracks latency distribution.
fn bench_wal_latency(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to build runtime");

    let concurrency_levels = vec![1, 4, 16, 64];
    let operations_per_task = 100;
    let data_size = 1024;

    let mut group = c.benchmark_group("wal_latency");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for &concurrency in &concurrency_levels {
        let total_ops = concurrency * operations_per_task;
        group.throughput(Throughput::Elements(total_ops as u64));

        let latencies = Arc::new(Mutex::new(Histogram::<u64>::new(3).expect("histogram")));

        group.bench_with_input(
            BenchmarkId::new("concurrency", concurrency),
            &concurrency,
            |b, &conc| {
                let data = Bytes::from(vec![0u8; data_size]);
                let latencies_clone = latencies.clone();

                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let total_start = Instant::now();

                        for _ in 0..iters {
                            // Create WAL with mutex for shared access.
                            let (wal, _tmp) = setup_wal(false).await;
                            let wal = Arc::new(tokio::sync::Mutex::new(wal));
                            let index = Arc::new(std::sync::atomic::AtomicU64::new(1));

                            let mut join_set = JoinSet::new();

                            for _ in 0..conc {
                                let wal_clone = wal.clone();
                                let data_clone = data.clone();
                                let index_clone = index.clone();
                                let lat_clone = latencies_clone.clone();

                                join_set.spawn(async move {
                                    for _ in 0..operations_per_task {
                                        let idx = index_clone
                                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        let entry = Entry::new(1, idx, data_clone.clone())
                                            .expect("entry creation failed");

                                        let op_start = Instant::now();
                                        {
                                            let mut wal = wal_clone.lock().await;
                                            wal.append(entry).await.expect("append failed");
                                        }
                                        let duration = op_start.elapsed();

                                        // Record latency (microseconds).
                                        let _ = lat_clone
                                            .lock()
                                            .unwrap()
                                            .record(duration.as_micros() as u64);
                                    }
                                });
                            }

                            while join_set.join_next().await.is_some() {}
                        }

                        total_start.elapsed()
                    })
                });
            },
        );

        // Print latency percentiles.
        let hist = latencies.lock().unwrap();
        if hist.len() > 0 {
            println!("\nLatency percentiles for concurrency={}:", concurrency);
            println!("  count: {}", hist.len());
            println!("  p50:   {} us", hist.value_at_quantile(0.50));
            println!("  p90:   {} us", hist.value_at_quantile(0.90));
            println!("  p95:   {} us", hist.value_at_quantile(0.95));
            println!("  p99:   {} us", hist.value_at_quantile(0.99));
            println!("  p99.9: {} us", hist.value_at_quantile(0.999));
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_wal_sequential,
    bench_wal_multi_partition,
    bench_wal_latency
);
criterion_main!(benches);
