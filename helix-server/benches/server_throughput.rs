//! Server throughput benchmarks.
//!
//! Measures end-to-end Helix server throughput via gRPC.
//! Tests both single-node and can be extended for multi-node scenarios.

#![allow(missing_docs)]
#![allow(clippy::significant_drop_tightening)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use hdrhistogram::Histogram;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tonic::transport::Server;

use helix_server::generated::helix_client::HelixClient;
use helix_server::generated::helix_server::HelixServer;
use helix_server::generated::{ReadRequest, Record, WriteRequest};
use helix_server::HelixService;

/// Port counter for unique ports per benchmark.
static PORT_COUNTER: AtomicU16 = AtomicU16::new(19000);

/// Gets a unique port for each benchmark run.
fn get_unique_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Starts a single-node Helix server and returns the address.
async fn start_server() -> SocketAddr {
    let port = get_unique_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let service = HelixService::new("bench-cluster".to_string(), 1);

    // Create the default topic.
    service
        .create_topic("bench".to_string(), 4)
        .await
        .expect("failed to create topic");

    // Spawn server in background.
    let server_addr = addr;
    tokio::spawn(async move {
        Server::builder()
            .add_service(HelixServer::new(service))
            .serve(server_addr)
            .await
            .expect("server failed");
    });

    // Wait for server to be ready.
    tokio::time::sleep(Duration::from_millis(100)).await;

    addr
}

/// Creates a gRPC client connected to the server.
async fn create_client(addr: SocketAddr) -> HelixClient<tonic::transport::Channel> {
    let uri = format!("http://{addr}");
    let mut retries = 0;
    loop {
        match HelixClient::connect(uri.clone()).await {
            Ok(client) => return client,
            Err(e) => {
                retries += 1;
                assert!(
                    retries <= 10,
                    "failed to connect to server after 10 retries: {e}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Benchmark single-client write throughput.
fn bench_write_throughput(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to build runtime");

    let record_sizes = vec![64, 256, 1024];
    let batch_sizes = vec![1, 10, 100];

    let mut group = c.benchmark_group("server_write");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for &record_size in &record_sizes {
        for &batch_size in &batch_sizes {
            #[allow(clippy::cast_sign_loss)]
            group.throughput(Throughput::Elements(batch_size as u64));

            let id = format!("size_{record_size}_batch_{batch_size}");

            group.bench_with_input(BenchmarkId::new("config", &id), &id, |b, _| {
                // Start server once per benchmark.
                let addr = rt.block_on(start_server());

                // Create records template.
                let records: Vec<Record> = (0..batch_size)
                    .map(|_| Record {
                        key: None,
                        value: vec![0u8; record_size],
                        headers: HashMap::default(),
                        timestamp_ms: None,
                    })
                    .collect();

                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut client = create_client(addr).await;
                        let total_start = Instant::now();

                        for _ in 0..iters {
                            let request = WriteRequest {
                                topic: "bench".to_string(),
                                partition: 0,
                                records: records.clone(),
                                required_acks: 1,
                            };

                            let response = client.write(request).await.expect("write failed");
                            black_box(response);
                        }

                        total_start.elapsed()
                    })
                });
            });
        }
    }

    group.finish();
}

/// Benchmark concurrent write throughput with multiple clients.
fn bench_write_concurrent(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(8)
        .build()
        .expect("failed to build runtime");

    let concurrency_levels = vec![1, 4, 16, 64];
    let writes_per_client = 100;
    let record_size = 1024;
    let batch_size = 10;

    let mut group = c.benchmark_group("server_write_concurrent");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for &concurrency in &concurrency_levels {
        let total_writes = concurrency * writes_per_client;
        #[allow(clippy::cast_sign_loss)]
        group.throughput(Throughput::Elements((total_writes * batch_size) as u64));

        let latencies = Arc::new(Mutex::new(Histogram::<u64>::new(3).expect("histogram")));

        group.bench_with_input(
            BenchmarkId::new("clients", concurrency),
            &concurrency,
            |b, &conc| {
                let addr = rt.block_on(start_server());
                let latencies_clone = latencies.clone();

                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let total_start = Instant::now();

                        for _ in 0..iters {
                            let mut join_set = JoinSet::new();

                            for client_id in 0..conc {
                                let lat_clone = latencies_clone.clone();
                                let partition = client_id % 4;

                                join_set.spawn(async move {
                                    let mut client = create_client(addr).await;

                                    let records: Vec<Record> = (0..batch_size)
                                        .map(|_| Record {
                                            key: None,
                                            value: vec![0u8; record_size],
                                            headers: HashMap::default(),
                                            timestamp_ms: None,
                                        })
                                        .collect();

                                    for _ in 0..writes_per_client {
                                        let request = WriteRequest {
                                            topic: "bench".to_string(),
                                            partition,
                                            records: records.clone(),
                                            required_acks: 1,
                                        };

                                        let op_start = Instant::now();
                                        let response =
                                            client.write(request).await.expect("write failed");
                                        let duration = op_start.elapsed();

                                        black_box(response);

                                        #[allow(clippy::cast_possible_truncation)]
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
        if !hist.is_empty() {
            println!("\nWrite latency percentiles for concurrency={concurrency}:");
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

/// Benchmark read throughput after pre-populating data.
fn bench_read_throughput(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to build runtime");

    let record_count = 10000;
    let record_size = 1024;
    let read_batch_sizes = vec![1, 10, 100];

    let mut group = c.benchmark_group("server_read");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for &read_batch in &read_batch_sizes {
        group.throughput(Throughput::Elements(u64::from(read_batch)));

        let id = format!("batch_{read_batch}");

        group.bench_with_input(BenchmarkId::new("config", &id), &id, |b, _| {
            // Start server and pre-populate.
            let addr = rt.block_on(async {
                let addr = start_server().await;
                let mut client = create_client(addr).await;

                // Pre-populate with records.
                let records: Vec<Record> = (0..100)
                    .map(|_| Record {
                        key: None,
                        value: vec![0u8; record_size],
                        headers: HashMap::default(),
                        timestamp_ms: None,
                    })
                    .collect();

                for _ in 0..(record_count / 100) {
                    let request = WriteRequest {
                        topic: "bench".to_string(),
                        partition: 0,
                        records: records.clone(),
                        required_acks: 1,
                    };
                    client.write(request).await.expect("write failed");
                }

                addr
            });

            b.iter_custom(|iters| {
                rt.block_on(async {
                    let mut client = create_client(addr).await;
                    let total_start = Instant::now();

                    for _ in 0..iters {
                        // Read from beginning.
                        let request = ReadRequest {
                            topic: "bench".to_string(),
                            partition: 0,
                            offset: 0,
                            max_records: read_batch,
                            max_bytes: 1024 * 1024,
                        };

                        let response = client.read(request).await.expect("read failed");
                        black_box(response);
                    }

                    total_start.elapsed()
                })
            });
        });
    }

    group.finish();
}

/// Benchmark read-after-write latency.
fn bench_read_after_write(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to build runtime");

    let record_sizes = vec![64, 1024];
    let operation_count = 100;

    let mut group = c.benchmark_group("server_read_after_write");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for &record_size in &record_sizes {
        #[allow(clippy::cast_sign_loss)]
        group.throughput(Throughput::Elements(operation_count as u64));

        let latencies = Arc::new(Mutex::new(Histogram::<u64>::new(3).expect("histogram")));

        group.bench_with_input(
            BenchmarkId::new("size", record_size),
            &record_size,
            |b, &size| {
                let addr = rt.block_on(start_server());
                let latencies_clone = latencies.clone();

                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut client = create_client(addr).await;
                        let total_start = Instant::now();

                        for _ in 0..iters {
                            for i in 0..operation_count {
                                // Write one record.
                                let records = vec![Record {
                                    key: None,
                                    value: vec![0u8; size],
                                    headers: HashMap::default(),
                                    timestamp_ms: None,
                                }];

                                let write_req = WriteRequest {
                                    topic: "bench".to_string(),
                                    partition: 0,
                                    records,
                                    required_acks: 1,
                                };

                                let op_start = Instant::now();

                                let write_resp =
                                    client.write(write_req).await.expect("write failed");
                                let offset = write_resp.into_inner().base_offset;

                                // Read it back immediately.
                                let read_req = ReadRequest {
                                    topic: "bench".to_string(),
                                    partition: 0,
                                    offset,
                                    max_records: 1,
                                    max_bytes: 1024 * 1024,
                                };

                                let read_resp =
                                    client.read(read_req).await.expect("read failed");
                                black_box(read_resp);

                                let duration = op_start.elapsed();
                                #[allow(clippy::cast_possible_truncation)]
                                let _ = latencies_clone
                                    .lock()
                                    .unwrap()
                                    .record(duration.as_micros() as u64);

                                black_box(i);
                            }
                        }

                        total_start.elapsed()
                    })
                });
            },
        );

        // Print latency percentiles.
        let hist = latencies.lock().unwrap();
        if !hist.is_empty() {
            println!("\nRead-after-write latency percentiles for size={record_size}:");
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
    bench_write_throughput,
    bench_write_concurrent,
    bench_read_throughput,
    bench_read_after_write
);
criterion_main!(benches);
