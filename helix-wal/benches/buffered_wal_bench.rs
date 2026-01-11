//! BufferedWal benchmarks.
//!
//! Measures throughput with different flush intervals and concurrency levels.

#![allow(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use tempfile::TempDir;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::Instant;

use helix_wal::{BufferedWal, BufferedWalConfig, Entry, TokioStorage, WalConfig};

/// Benchmark configuration.
#[derive(Clone, Debug)]
struct BenchConfig {
    /// Number of concurrent tasks.
    concurrency: usize,
    /// Entries per task.
    entries_per_task: usize,
    /// Flush interval in milliseconds.
    flush_interval_ms: u64,
}

impl BenchConfig {
    fn name(&self) -> String {
        format!(
            "conc_{}_entries_{}_flush_{}ms",
            self.concurrency, self.entries_per_task, self.flush_interval_ms
        )
    }

    fn total_entries(&self) -> usize {
        self.concurrency * self.entries_per_task
    }
}

/// Creates a BufferedWal for benchmarking.
async fn setup_buffered_wal(flush_interval_ms: u64) -> (BufferedWal<TokioStorage>, TempDir) {
    let tempdir = tempfile::tempdir().expect("failed to create temp dir");
    let wal_config = WalConfig::new(tempdir.path());
    let config = BufferedWalConfig::new(wal_config)
        .with_flush_interval(Duration::from_millis(flush_interval_ms))
        .with_max_buffer_entries(100_000);

    let wal = BufferedWal::open(TokioStorage::new(), config)
        .await
        .expect("failed to open WAL");

    (wal, tempdir)
}

/// Benchmark concurrent writes with buffering.
fn bench_buffered_wal_concurrent(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to build runtime");

    // Benchmark parameters for varying concurrency levels.
    let concurrency_levels = vec![64, 256, 1024, 4096];
    let entries_per_task = 8;
    let flush_intervals = vec![0, 10]; // Sync vs buffered
    let data_size = 1024;

    let mut group = c.benchmark_group("buffered_wal");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for &flush_interval_ms in &flush_intervals {
        for &concurrency in &concurrency_levels {
            let config = BenchConfig {
                concurrency,
                entries_per_task,
                flush_interval_ms,
            };

            group.throughput(Throughput::Elements(config.total_entries() as u64));

            group.bench_with_input(
                BenchmarkId::new("config", config.name()),
                &config,
                |b, cfg| {
                    let data = Bytes::from(vec![0u8; data_size]);

                    b.iter_custom(|iters| {
                        rt.block_on(async {
                            let total_start = Instant::now();

                            for _ in 0..iters {
                                let (wal, _tmp) =
                                    setup_buffered_wal(cfg.flush_interval_ms).await;
                                let wal = Arc::new(wal);
                                let index_counter = Arc::new(
                                    std::sync::atomic::AtomicU64::new(1),
                                );

                                let mut join_set = JoinSet::new();

                                for _ in 0..cfg.concurrency {
                                    let wal_clone = wal.clone();
                                    let data_clone = data.clone();
                                    let counter = index_counter.clone();
                                    let entries_count = cfg.entries_per_task;

                                    join_set.spawn(async move {
                                        for _ in 0..entries_count {
                                            let idx = counter.fetch_add(
                                                1,
                                                std::sync::atomic::Ordering::Relaxed,
                                            );
                                            let entry = Entry::new(1, idx, data_clone.clone())
                                                .expect("entry");
                                            black_box(
                                                wal_clone.append(entry).await.expect("append"),
                                            );
                                        }
                                    });
                                }

                                // Wait for all tasks.
                                while join_set.join_next().await.is_some() {}

                                // Ensure all data is flushed.
                                wal.flush().await.expect("flush");
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

/// Benchmark batch appends (single writer).
fn bench_buffered_wal_batch(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .expect("failed to build runtime");

    let batch_sizes = vec![100, 1000, 10000];
    let data_size = 1024;

    let mut group = c.benchmark_group("buffered_wal_batch");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for &batch_size in &batch_sizes {
        group.throughput(Throughput::Elements(batch_size as u64));

        let id = format!("batch_{}", batch_size);

        // Buffered mode (10ms).
        group.bench_with_input(BenchmarkId::new("buffered_10ms", &id), &batch_size, |b, &size| {
            let data = Bytes::from(vec![0u8; data_size]);

            b.iter_custom(|iters| {
                rt.block_on(async {
                    let total_start = Instant::now();

                    for _ in 0..iters {
                        let (wal, _tmp) = setup_buffered_wal(10).await;

                        let entries: Vec<Entry> = (1..=size as u64)
                            .map(|i| Entry::new(1, i, data.clone()).unwrap())
                            .collect();

                        black_box(wal.append_batch(entries).await.expect("batch"));
                        wal.flush().await.expect("flush");
                    }

                    total_start.elapsed()
                })
            });
        });

        // Sync mode.
        group.bench_with_input(BenchmarkId::new("sync", &id), &batch_size, |b, &size| {
            let data = Bytes::from(vec![0u8; data_size]);

            b.iter_custom(|iters| {
                rt.block_on(async {
                    let total_start = Instant::now();

                    for _ in 0..iters {
                        let (wal, _tmp) = setup_buffered_wal(0).await;

                        let entries: Vec<Entry> = (1..=size as u64)
                            .map(|i| Entry::new(1, i, data.clone()).unwrap())
                            .collect();

                        black_box(wal.append_batch(entries).await.expect("batch"));
                    }

                    total_start.elapsed()
                })
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_buffered_wal_concurrent, bench_buffered_wal_batch);
criterion_main!(benches);
