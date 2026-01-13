//! `SharedWal` fsync amortization benchmarks.
//!
//! Compares fsync cost between:
//! - Per-partition WALs: N partitions, N WALs, N fsyncs per batch
//! - `SharedWal`: N partitions, K shared WALs, K fsyncs per batch (K << N)
//!
//! This demonstrates the fsync amortization benefit of sharing a WAL.

#![allow(missing_docs)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::cast_sign_loss)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use helix_core::PartitionId;
use tempfile::TempDir;
use tokio::runtime::Builder;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::Instant;

use helix_wal::{Entry, SharedWal, SharedWalConfig, TokioStorage, Wal, WalConfig};

/// Benchmark configuration.
#[derive(Clone, Debug)]
struct BenchConfig {
    /// Total number of partitions.
    partition_count: usize,
    /// Number of shared WALs (for SharedWal model).
    shared_wal_count: usize,
    /// Entries per partition per batch.
    entries_per_partition: usize,
    /// Size of each entry payload in bytes (optional, defaults to 0 if not used).
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
async fn bench_separate_wals_concurrent(config: &BenchConfig, data: &Bytes) -> Duration {
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
                let entry = Entry::new(1, (i + 1) as u64, data.clone())
                    .expect("entry creation failed");
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
// SharedWal model (amortized fsync)
// ============================================================================

/// Benchmark K shared WALs with N partitions distributed across them.
///
/// Partitions are distributed round-robin across shared WALs.
/// Each shared WAL is protected by a mutex for concurrent access.
/// Total fsyncs = K (one per shared WAL).
async fn bench_shared_wals_concurrent(config: &BenchConfig, data: &Bytes) -> Duration {
    let tempdir = create_temp_dir();

    // Create K shared WALs.
    let mut shared_wals: Vec<Arc<Mutex<SharedWal<TokioStorage>>>> =
        Vec::with_capacity(config.shared_wal_count);

    for k in 0..config.shared_wal_count {
        let wal_path = tempdir.path().join(format!("shared-{k}"));
        std::fs::create_dir_all(&wal_path).expect("failed to create shared wal dir");
        let wal_config = SharedWalConfig::new(&wal_path);
        let wal = SharedWal::open(TokioStorage::new(), wal_config)
            .await
            .expect("failed to open SharedWal");
        shared_wals.push(Arc::new(Mutex::new(wal)));
    }

    let start = Instant::now();

    let mut join_set = JoinSet::new();

    // Spawn a writer task per partition.
    for p in 0..config.partition_count {
        // Round-robin assignment to shared WALs.
        let shared_wal_idx = p % config.shared_wal_count;
        let shared_wal = shared_wals[shared_wal_idx].clone();
        let partition_id = PartitionId::new(p as u64 + 1);
        let entries = config.entries_per_partition;
        let data = data.clone();

        join_set.spawn(async move {
            for i in 0..entries {
                let mut wal = shared_wal.lock().await;
                wal.append(partition_id, 1, (i + 1) as u64, data.clone())
                    .await
                    .expect("append failed");
                // Note: We release the lock between appends to allow interleaving.
            }
        });
    }

    // Wait for all writers to finish appending.
    while join_set.join_next().await.is_some() {}

    // Sync each shared WAL (K fsyncs total).
    for shared_wal in &shared_wals {
        let mut wal = shared_wal.lock().await;
        wal.sync().await.expect("sync failed");
    }

    start.elapsed()
}

/// Alternative: batch all appends then sync once per shared WAL.
/// This maximizes fsync amortization.
async fn bench_shared_wals_batched(config: &BenchConfig, data: &Bytes) -> Duration {
    let tempdir = create_temp_dir();

    // Create K shared WALs.
    let mut shared_wals: Vec<SharedWal<TokioStorage>> = Vec::with_capacity(config.shared_wal_count);

    for k in 0..config.shared_wal_count {
        let wal_path = tempdir.path().join(format!("shared-{k}"));
        std::fs::create_dir_all(&wal_path).expect("failed to create shared wal dir");
        let wal_config = SharedWalConfig::new(&wal_path);
        let wal = SharedWal::open(TokioStorage::new(), wal_config)
            .await
            .expect("failed to open SharedWal");
        shared_wals.push(wal);
    }

    let start = Instant::now();

    // Append all entries sequentially (batched per shared WAL).
    for p in 0..config.partition_count {
        let shared_wal_idx = p % config.shared_wal_count;
        let partition_id = PartitionId::new(p as u64 + 1);

        for i in 0..config.entries_per_partition {
            shared_wals[shared_wal_idx]
                .append(partition_id, 1, (i + 1) as u64, data.clone())
                .await
                .expect("append failed");
        }
    }

    // Sync each shared WAL (K fsyncs total).
    for wal in &mut shared_wals {
        wal.sync().await.expect("sync failed");
    }

    start.elapsed()
}

// ============================================================================
// Benchmarks
// ============================================================================

/// Compare per-partition WALs vs SharedWal with varying partition counts.
///
/// Shows fsync amortization benefit as partition count increases.
fn bench_fsync_amortization(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to build runtime");

    // Test: N partitions, comparing N separate WALs vs 1 shared WAL.
    let partition_counts = vec![4, 16, 64];
    let entries_per_partition = 10;
    let data_size = 256;

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
                            total += bench_separate_wals_concurrent(&config_separate, &data).await;
                        }
                        total
                    })
                });
            },
        );

        // SharedWal with concurrent writers (1 fsync).
        group.bench_with_input(
            BenchmarkId::new("shared_concurrent", format!("p{partition_count}")),
            &config_shared,
            |b, _cfg| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += bench_shared_wals_concurrent(&config_shared, &data).await;
                        }
                        total
                    })
                });
            },
        );

        // SharedWal with batched writes (1 fsync, no lock contention).
        group.bench_with_input(
            BenchmarkId::new("shared_batched", format!("p{partition_count}")),
            &config_shared,
            |b, _cfg| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += bench_shared_wals_batched(&config_shared, &data).await;
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
/// lock contention (more WALs = less contention).
fn bench_shared_wal_count(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to build runtime");

    let partition_count = 64;
    let shared_wal_counts = vec![1, 2, 4, 8, 16, 64]; // 64 = same as separate
    let entries_per_partition = 10;
    let data_size = 256;

    let mut group = c.benchmark_group("shared_wal_count");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    let data = Bytes::from(vec![0u8; data_size]);
    let total_entries = partition_count * entries_per_partition;

    group.throughput(Throughput::Elements(total_entries as u64));

    for &shared_wal_count in &shared_wal_counts {
        let config = BenchConfig {
            partition_count,
            shared_wal_count,
            entries_per_partition,
            data_size,
        };

        group.bench_with_input(
            BenchmarkId::new("k", shared_wal_count),
            &config,
            |b, cfg| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += bench_shared_wals_concurrent(cfg, &data).await;
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
fn bench_shared_wal_data_sizes(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to build runtime");

    let partition_count = 16;
    let shared_wal_count = 1;
    let entries_per_partition = 100;
    let data_sizes = vec![64, 256, 1024, 4096];

    let mut group = c.benchmark_group("shared_wal_data_size");
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

        group.bench_with_input(
            BenchmarkId::new("size", data_size),
            &config,
            |b, cfg| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += bench_shared_wals_batched(cfg, &data).await;
                        }
                        total
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_fsync_amortization,
    bench_shared_wal_count,
    bench_shared_wal_data_sizes,
);
criterion_main!(benches);
