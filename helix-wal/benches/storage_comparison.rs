//! Storage backend comparison benchmarks.
//!
//! Compares TokioStorage vs IoUringWorkerStorage performance.
//!
//! Run with:
//! ```bash
//! # TokioStorage only (all platforms)
//! cargo bench --package helix-wal --bench storage_comparison
//!
//! # With io_uring (Linux only)
//! cargo bench --package helix-wal --bench storage_comparison --features io-uring
//! ```

#![allow(missing_docs)]
#![allow(dead_code)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::uninlined_format_args)]

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use tempfile::TempDir;
use tokio::runtime::Builder;
use tokio::time::Instant;

use helix_wal::{Entry, Storage, TokioStorage, Wal, WalConfig};

#[cfg(all(target_os = "linux", feature = "io-uring"))]
use helix_wal::IoUringWorkerStorage;

/// Returns the benchmark base directory from HELIX_BENCH_DIR env var, or None for system temp.
fn bench_base_dir() -> Option<PathBuf> {
    std::env::var("HELIX_BENCH_DIR").ok().map(PathBuf::from)
}

/// Creates a temp directory for benchmarking.
fn create_temp_dir() -> TempDir {
    match bench_base_dir() {
        Some(base) => {
            tempfile::tempdir_in(base).expect("failed to create temp dir in HELIX_BENCH_DIR")
        }
        None => tempfile::tempdir().expect("failed to create temp dir"),
    }
}

/// Benchmark configuration.
#[derive(Clone, Debug)]
struct BenchConfig {
    /// Size of each entry payload in bytes.
    data_size: usize,
    /// Number of entries to write.
    entry_count: usize,
    /// Whether to sync after each write.
    sync_each: bool,
}

impl BenchConfig {
    fn name(&self) -> String {
        let sync = if self.sync_each { "_sync_each" } else { "_sync_end" };
        format!("{}B_x{}{}", self.data_size, self.entry_count, sync)
    }
}

/// Generic benchmark runner for any storage backend.
async fn run_write_benchmark<S: Storage + 'static>(
    storage: S,
    config: &BenchConfig,
) -> Duration {
    let temp_dir = create_temp_dir();
    let wal_config = WalConfig::new(temp_dir.path()).with_sync_on_write(config.sync_each);
    let mut wal = Wal::open(storage, wal_config)
        .await
        .expect("failed to open WAL");

    let data = Bytes::from(vec![0u8; config.data_size]);
    let start = Instant::now();

    for i in 1..=config.entry_count {
        let entry = Entry::new(1, i as u64, data.clone()).expect("entry creation failed");
        black_box(wal.append(entry).await.expect("append failed"));
    }

    if !config.sync_each {
        wal.sync().await.expect("sync failed");
    }

    start.elapsed()
}

/// Benchmark TokioStorage writes.
fn bench_tokio_storage(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .expect("failed to build runtime");

    let configs = vec![
        // Sync at end (batched) - typical use case
        BenchConfig { data_size: 1024, entry_count: 1000, sync_each: false },
        BenchConfig { data_size: 1024, entry_count: 10000, sync_each: false },
        // Sync each write (worst case for fsync overhead)
        BenchConfig { data_size: 1024, entry_count: 100, sync_each: true },
        BenchConfig { data_size: 1024, entry_count: 1000, sync_each: true },
    ];

    let mut group = c.benchmark_group("storage_tokio");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for config in &configs {
        group.throughput(Throughput::Elements(config.entry_count as u64));

        group.bench_with_input(
            BenchmarkId::new("write", config.name()),
            config,
            |b, cfg| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += run_write_benchmark(TokioStorage::new(), cfg).await;
                        }
                        total
                    })
                });
            },
        );
    }

    group.finish();
}

/// Benchmark IoUringWorkerStorage writes.
#[cfg(all(target_os = "linux", feature = "io-uring"))]
fn bench_io_uring_storage(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .expect("failed to build runtime");

    let configs = vec![
        // Sync at end (batched) - typical use case
        BenchConfig { data_size: 1024, entry_count: 1000, sync_each: false },
        BenchConfig { data_size: 1024, entry_count: 10000, sync_each: false },
        // Sync each write (worst case for fsync overhead)
        BenchConfig { data_size: 1024, entry_count: 100, sync_each: true },
        BenchConfig { data_size: 1024, entry_count: 1000, sync_each: true },
    ];

    // Create storage once and reuse across iterations (it's cloneable).
    let storage = IoUringWorkerStorage::try_new().expect("failed to create io_uring storage");

    let mut group = c.benchmark_group("storage_io_uring");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for config in &configs {
        group.throughput(Throughput::Elements(config.entry_count as u64));

        let storage = storage.clone();
        group.bench_with_input(
            BenchmarkId::new("write", config.name()),
            config,
            |b, cfg| {
                b.iter_custom(|iters| {
                    let storage = storage.clone();
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += run_write_benchmark(storage.clone(), cfg).await;
                        }
                        total
                    })
                });
            },
        );
    }

    group.finish();
}

/// Side-by-side comparison benchmark.
#[cfg(all(target_os = "linux", feature = "io-uring"))]
fn bench_comparison(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .expect("failed to build runtime");

    // Focus on the most meaningful comparisons
    let configs = vec![
        ("1KB_x1000_sync_end", BenchConfig { data_size: 1024, entry_count: 1000, sync_each: false }),
        ("1KB_x1000_sync_each", BenchConfig { data_size: 1024, entry_count: 1000, sync_each: true }),
        ("4KB_x1000_sync_end", BenchConfig { data_size: 4096, entry_count: 1000, sync_each: false }),
    ];

    // Create io_uring storage once and reuse across iterations.
    let io_uring_storage =
        IoUringWorkerStorage::try_new().expect("failed to create io_uring storage");

    let mut group = c.benchmark_group("storage_comparison");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for (name, config) in &configs {
        group.throughput(Throughput::Elements(config.entry_count as u64));

        // TokioStorage
        group.bench_with_input(
            BenchmarkId::new("tokio", *name),
            config,
            |b, cfg| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += run_write_benchmark(TokioStorage::new(), cfg).await;
                        }
                        total
                    })
                });
            },
        );

        // IoUringWorkerStorage
        let io_uring_storage = io_uring_storage.clone();
        group.bench_with_input(
            BenchmarkId::new("io_uring", *name),
            config,
            |b, cfg| {
                b.iter_custom(|iters| {
                    let storage = io_uring_storage.clone();
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += run_write_benchmark(storage.clone(), cfg).await;
                        }
                        total
                    })
                });
            },
        );
    }

    group.finish();
}

/// Placeholder when io_uring is not available.
#[cfg(not(all(target_os = "linux", feature = "io-uring")))]
fn bench_io_uring_storage(_c: &mut Criterion) {
    println!("io_uring benchmarks skipped (not Linux or feature not enabled)");
}

#[cfg(not(all(target_os = "linux", feature = "io-uring")))]
fn bench_comparison(_c: &mut Criterion) {
    println!("comparison benchmarks skipped (not Linux or feature not enabled)");
}

/// Benchmark concurrent raw storage writes (where batching can help).
#[cfg(all(target_os = "linux", feature = "io-uring"))]
fn bench_concurrent_storage(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to build runtime");

    let concurrency_levels = vec![4, 8, 16];
    let writes_per_task = 100;
    let data_size = 1024usize;

    // Create storages once.
    let tokio_storage = TokioStorage::new();
    let io_uring_storage =
        IoUringWorkerStorage::try_new().expect("failed to create io_uring storage");

    let mut group = c.benchmark_group("concurrent_storage");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for concurrency in &concurrency_levels {
        let total_ops = concurrency * writes_per_task;
        group.throughput(Throughput::Elements(total_ops as u64));

        // TokioStorage concurrent
        group.bench_function(
            BenchmarkId::new("tokio", format!("{concurrency}_tasks")),
            |b| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += run_concurrent_write_benchmark(
                                &tokio_storage,
                                *concurrency,
                                writes_per_task,
                                data_size,
                            )
                            .await;
                        }
                        total
                    })
                });
            },
        );

        // IoUringWorkerStorage concurrent
        let io_uring_storage = io_uring_storage.clone();
        group.bench_function(
            BenchmarkId::new("io_uring", format!("{concurrency}_tasks")),
            |b| {
                b.iter_custom(|iters| {
                    let storage = io_uring_storage.clone();
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += run_concurrent_write_benchmark(
                                &storage,
                                *concurrency,
                                writes_per_task,
                                data_size,
                            )
                            .await;
                        }
                        total
                    })
                });
            },
        );
    }

    group.finish();
}

/// Run concurrent writes to multiple files.
async fn run_concurrent_write_benchmark<S: Storage + Clone + 'static>(
    storage: &S,
    concurrency: usize,
    writes_per_task: usize,
    data_size: usize,
) -> Duration {
    let temp_dir = create_temp_dir();
    let data = vec![0u8; data_size];

    let start = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for task_id in 0..concurrency {
        let storage = storage.clone();
        let path = temp_dir.path().join(format!("file-{task_id}.dat"));
        let data = data.clone();
        handles.push(tokio::spawn(async move {
            let file = storage.open(&path).await.expect("open failed");
            for i in 0..writes_per_task {
                let offset = (i * data.len()) as u64;
                file.write_at(offset, &data).await.expect("write failed");
            }
            file.sync().await.expect("sync failed");
        }));
    }

    for handle in handles {
        handle.await.expect("task failed");
    }

    start.elapsed()
}

#[cfg(not(all(target_os = "linux", feature = "io-uring")))]
fn bench_concurrent_storage(_c: &mut Criterion) {
    println!("concurrent storage benchmarks skipped (not Linux or feature not enabled)");
}

criterion_group!(
    benches,
    bench_tokio_storage,
    bench_io_uring_storage,
    bench_comparison,
    bench_concurrent_storage
);
criterion_main!(benches);
