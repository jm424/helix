//! `SharedWalPool` fsync amortization benchmarks.
//!
//! Compares fsync cost between:
//! - Per-partition WALs: N partitions, N WALs, N fsyncs per batch
//! - `SharedWalPool`: N partitions, K shared WALs, K fsyncs per batch (K << N)
//!
//! This demonstrates the fsync amortization benefit of sharing WALs via
//! the coordinator-backed pool which batches writes before fsync.
//!
//! Run with io_uring support:
//! ```bash
//! cargo bench --bench shared_wal_bench --features io-uring
//! ```

#![allow(missing_docs)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::cast_sign_loss)]

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use helix_core::PartitionId;
use tempfile::TempDir;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::Instant;

use helix_wal::{Entry, PoolConfig, SharedWalPool, Storage, TokioStorage, Wal, WalConfig};

#[cfg(all(target_os = "linux", feature = "io-uring"))]
use helix_wal::IoUringWorkerStorage;

/// Benchmark configuration.
#[derive(Clone, Debug)]
struct BenchConfig {
    /// Total number of partitions.
    partition_count: usize,
    /// Number of shared WALs (for SharedWalPool).
    shared_wal_count: usize,
    /// Entries per partition per batch.
    entries_per_partition: usize,
    /// Size of each entry payload in bytes.
    #[allow(dead_code)]
    data_size: usize,
}

impl BenchConfig {
    fn total_entries(&self) -> usize {
        self.partition_count * self.entries_per_partition
    }
}

/// Returns the benchmark base directory from HELIX_BENCH_DIR env var.
fn bench_base_dir() -> Option<PathBuf> {
    std::env::var("HELIX_BENCH_DIR").ok().map(PathBuf::from)
}

/// Creates a temporary directory for benchmarking.
fn create_temp_dir() -> TempDir {
    match bench_base_dir() {
        Some(base) => {
            tempfile::tempdir_in(base).expect("failed to create temp dir in HELIX_BENCH_DIR")
        }
        None => tempfile::tempdir().expect("failed to create temp dir"),
    }
}

// ============================================================================
// Per-partition WAL model (baseline - like current Kafka)
// ============================================================================

/// Benchmark N separate WALs with concurrent writers.
///
/// Each partition has its own WAL. Writers run concurrently.
/// Total fsyncs = N (one per partition).
async fn bench_separate_wals(config: &BenchConfig, data: &Bytes) -> Duration {
    let tempdir = create_temp_dir();
    let start = Instant::now();

    let mut join_set = JoinSet::new();

    for p in 0..config.partition_count {
        let wal_path = tempdir.path().join(format!("partition-{p}"));
        std::fs::create_dir_all(&wal_path).expect("failed to create partition dir");
        let entries = config.entries_per_partition;
        let data = data.clone();

        join_set.spawn(async move {
            let wal_config = WalConfig::new(&wal_path);
            let mut wal = Wal::open(TokioStorage::new(), wal_config)
                .await
                .expect("failed to open WAL");

            for i in 0..entries {
                let entry =
                    Entry::new(1, (i + 1) as u64, data.clone()).expect("entry creation failed");
                wal.append(entry).await.expect("append failed");
            }

            // Each partition syncs independently.
            wal.sync().await.expect("sync failed");
        });
    }

    // Wait for all partitions.
    while join_set.join_next().await.is_some() {}

    start.elapsed()
}

// ============================================================================
// SharedWalPool model (amortized fsync via coordinator batching)
// ============================================================================

/// Benchmark SharedWalPool with K WALs serving N partitions.
///
/// Uses the coordinator-backed pool which batches writes and fsyncs.
/// Total fsyncs = K (one per shared WAL, amortized across partitions).
///
/// Pattern: submit all entries with `append_async`, then `flush()` to sync,
/// then await durability acks. This matches the intended usage for fsync
/// amortization.
async fn bench_shared_wal_pool_with_storage<S>(
    storage: S,
    config: &BenchConfig,
    data: &Bytes,
) -> Duration
where
    S: Storage + Clone + Send + Sync + 'static,
{
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let tempdir = create_temp_dir();

    // Use long flush interval since we'll trigger explicit flush.
    let pool_config = PoolConfig::new(tempdir.path(), config.shared_wal_count as u32)
        .with_flush_interval(Duration::from_secs(60));

    let pool = Arc::new(
        SharedWalPool::open(storage, pool_config)
            .await
            .expect("failed to open pool"),
    );

    let start = Instant::now();

    // Barrier to synchronize all writers before flush.
    let barrier = Arc::new(Barrier::new(config.partition_count + 1));
    let mut join_set = JoinSet::new();

    // Spawn a writer task per partition.
    for p in 0..config.partition_count {
        let partition_id = PartitionId::new(p as u64 + 1);
        let handle = pool.handle(partition_id);
        let entries = config.entries_per_partition;
        let data = data.clone();
        let barrier = barrier.clone();

        join_set.spawn(async move {
            // Submit all entries without waiting (append_async).
            let mut receivers = Vec::with_capacity(entries);
            for i in 0..entries {
                let rx = handle
                    .append_async(1, (i + 1) as u64, data.clone())
                    .await
                    .expect("append_async failed");
                receivers.push(rx);
            }

            // Signal that all entries are submitted.
            barrier.wait().await;

            // Wait for all entries to be durable.
            for rx in receivers {
                rx.await
                    .expect("durable ack channel closed")
                    .expect("durable ack failed");
            }
        });
    }

    // Wait for all partitions to submit entries.
    barrier.wait().await;

    // Trigger immediate flush (K fsyncs for K WALs).
    pool.flush().await.expect("flush failed");

    // Wait for all writers to confirm durability.
    while join_set.join_next().await.is_some() {}

    pool.shutdown().await.expect("shutdown failed");

    start.elapsed()
}

/// Benchmark SharedWalPool with TokioStorage.
async fn bench_shared_wal_pool(config: &BenchConfig, data: &Bytes) -> Duration {
    bench_shared_wal_pool_with_storage(TokioStorage::new(), config, data).await
}

/// Benchmark SharedWalPool with IoUringWorkerStorage.
#[cfg(all(target_os = "linux", feature = "io-uring"))]
async fn bench_shared_wal_pool_io_uring(config: &BenchConfig, data: &Bytes) -> Duration {
    let storage = IoUringWorkerStorage::try_new().expect("failed to create io_uring storage");
    bench_shared_wal_pool_with_storage(storage, config, data).await
}

// ============================================================================
// Benchmarks
// ============================================================================

/// Compare per-partition WALs vs SharedWalPool with varying partition counts.
///
/// Shows fsync amortization benefit as partition count increases.
fn bench_fsync_amortization(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to build runtime");

    // Test: N partitions, comparing N separate WALs vs 1 shared WAL pool.
    let partition_counts = vec![4, 16, 64];
    let entries_per_partition = 10;
    let data_size = 1024; // 1 KB payload

    let mut group = c.benchmark_group("fsync_amortization");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    for &partition_count in &partition_counts {
        let config_separate = BenchConfig {
            partition_count,
            shared_wal_count: partition_count, // N WALs = N fsyncs
            entries_per_partition,
            data_size,
        };

        let config_shared = BenchConfig {
            partition_count,
            shared_wal_count: 1, // 1 shared WAL = 1 fsync
            entries_per_partition,
            data_size,
        };

        let data = Bytes::from(vec![0u8; data_size]);
        let total_entries = config_separate.total_entries();

        group.throughput(Throughput::Elements(total_entries as u64));

        // Baseline: N separate WALs (N fsyncs).
        group.bench_with_input(
            BenchmarkId::new("separate", format!("p{partition_count}")),
            &config_separate,
            |b, _cfg| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += bench_separate_wals(&config_separate, &data).await;
                        }
                        total
                    })
                });
            },
        );

        // SharedWalPool with Tokio storage (1 fsync).
        group.bench_with_input(
            BenchmarkId::new("pool_tokio", format!("p{partition_count}")),
            &config_shared,
            |b, _cfg| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += bench_shared_wal_pool(&config_shared, &data).await;
                        }
                        total
                    })
                });
            },
        );

        // SharedWalPool with io_uring storage (1 fsync).
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        group.bench_with_input(
            BenchmarkId::new("pool_uring", format!("p{partition_count}")),
            &config_shared,
            |b, _cfg| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += bench_shared_wal_pool_io_uring(&config_shared, &data).await;
                        }
                        total
                    })
                });
            },
        );
    }

    group.finish();
}

/// Vary the number of shared WALs (K) for a fixed partition count.
///
/// Shows the trade-off between fsync amortization (fewer WALs) and
/// parallelism (more WALs = more concurrent flushes).
fn bench_pool_scaling(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to build runtime");

    let partition_count = 64;
    let pool_sizes = vec![1, 2, 4, 8, 16];
    let entries_per_partition = 10;
    let data_size = 1024; // 1 KB payload

    let mut group = c.benchmark_group("pool_scaling");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    let data = Bytes::from(vec![0u8; data_size]);
    let total_entries = partition_count * entries_per_partition;

    group.throughput(Throughput::Elements(total_entries as u64));

    for &pool_size in &pool_sizes {
        let config = BenchConfig {
            partition_count,
            shared_wal_count: pool_size,
            entries_per_partition,
            data_size,
        };

        group.bench_with_input(
            BenchmarkId::new("k", pool_size),
            &config,
            |b, cfg| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += bench_shared_wal_pool(cfg, &data).await;
                        }
                        total
                    })
                });
            },
        );
    }

    group.finish();
}

/// Benchmark with larger data sizes to stress I/O more.
fn bench_pool_data_sizes(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to build runtime");

    let partition_count = 16;
    let shared_wal_count = 1;
    let entries_per_partition = 100;
    let data_sizes = vec![64, 256, 1024, 4096];

    let mut group = c.benchmark_group("pool_data_size");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for &data_size in &data_sizes {
        let config = BenchConfig {
            partition_count,
            shared_wal_count,
            entries_per_partition,
            data_size,
        };

        let data = Bytes::from(vec![0u8; data_size]);
        let total_bytes = config.total_entries() * data_size;

        group.throughput(Throughput::Bytes(total_bytes as u64));

        group.bench_with_input(BenchmarkId::new("size", data_size), &config, |b, cfg| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += bench_shared_wal_pool(cfg, &data).await;
                    }
                    total
                })
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_fsync_amortization,
    bench_pool_scaling,
    bench_pool_data_sizes,
);
criterion_main!(benches);
