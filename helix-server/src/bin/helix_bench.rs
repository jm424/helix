//! Helix benchmark tool.
//!
//! A standalone benchmark tool for measuring Helix server performance.
//! Can be used against single-node or multi-node clusters.
//!
//! # Usage
//!
//! ```bash
//! # Single-node benchmark
//! helix-bench --addr 127.0.0.1:9092 --topic default --records 100000
//!
//! # Multi-node benchmark (against Docker cluster)
//! helix-bench --addr 127.0.0.1:9092 --topic default --records 100000 --clients 16
//! ```
//!
//! # Comparison with Kafka
//!
//! To compare with Kafka, use similar parameters:
//! ```bash
//! # Kafka producer benchmark
//! kafka-producer-perf-test --topic test --num-records 100000 \
//!     --record-size 1024 --throughput -1 \
//!     --producer-props bootstrap.servers=localhost:9092
//!
//! # Helix equivalent
//! helix-bench --addr 127.0.0.1:9092 --topic test --records 100000 --size 1024
//! ```

#![deny(unsafe_code)]
#![warn(missing_docs)]
// Benchmark tool lint allowances - these are less critical for CLI tools.
#![allow(clippy::cast_precision_loss)] // f64 precision loss acceptable in benchmark stats
#![allow(clippy::cast_possible_truncation)] // Safe casts in benchmark context
#![allow(clippy::uninlined_format_args)] // Format string style not critical
#![allow(clippy::too_many_lines)] // Benchmark functions can be longer
#![allow(clippy::doc_markdown)] // Backticks in docs not critical
#![allow(clippy::significant_drop_tightening)] // Clarity over drop optimization
#![allow(clippy::default_trait_access)] // HashMap::new() vs default() style not critical
#![allow(clippy::cast_possible_wrap)] // Safe wrapping in benchmark context
#![allow(clippy::cast_lossless)] // Safe casts, verbose From not needed
#![allow(clippy::too_many_arguments)] // Benchmark runner needs many params

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use clap::{Parser, Subcommand};
use hdrhistogram::Histogram;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use helix_server::generated::helix_client::HelixClient;
use helix_server::generated::{ReadRequest, Record, WriteRequest};

/// Helix benchmark tool.
#[derive(Parser, Debug)]
#[command(name = "helix-bench")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Server address(es) to connect to.
    #[arg(long, default_value = "127.0.0.1:9092")]
    addr: String,

    #[command(subcommand)]
    command: Command,
}

/// Benchmark command.
#[derive(Subcommand, Debug)]
enum Command {
    /// Run write (producer) benchmark.
    Write {
        /// Topic to write to.
        #[arg(long, default_value = "default")]
        topic: String,

        /// Partition to write to (-1 for round-robin).
        #[arg(long, default_value = "0")]
        partition: i32,

        /// Total number of records to write.
        #[arg(long, default_value = "100000")]
        records: u64,

        /// Size of each record in bytes.
        #[arg(long, default_value = "1024")]
        size: usize,

        /// Number of records per batch.
        #[arg(long, default_value = "100")]
        batch_size: u32,

        /// Number of concurrent clients.
        #[arg(long, default_value = "1")]
        clients: u32,

        /// Required acks (0=none, 1=leader, -1=all).
        #[arg(long, default_value = "1")]
        acks: i32,
    },

    /// Run read (consumer) benchmark.
    Read {
        /// Topic to read from.
        #[arg(long, default_value = "default")]
        topic: String,

        /// Partition to read from.
        #[arg(long, default_value = "0")]
        partition: i32,

        /// Starting offset.
        #[arg(long, default_value = "0")]
        offset: u64,

        /// Total number of records to read.
        #[arg(long, default_value = "100000")]
        records: u64,

        /// Number of records per fetch.
        #[arg(long, default_value = "100")]
        batch_size: u32,

        /// Number of concurrent clients.
        #[arg(long, default_value = "1")]
        clients: u32,
    },

    /// Run end-to-end latency benchmark (write + read).
    Latency {
        /// Topic to use.
        #[arg(long, default_value = "default")]
        topic: String,

        /// Partition to use.
        #[arg(long, default_value = "0")]
        partition: i32,

        /// Number of operations.
        #[arg(long, default_value = "1000")]
        ops: u64,

        /// Size of each record in bytes.
        #[arg(long, default_value = "1024")]
        size: usize,
    },
}

/// Creates a gRPC client.
async fn create_client(
    addr: &str,
) -> Result<HelixClient<tonic::transport::Channel>, tonic::transport::Error> {
    let uri = if addr.starts_with("http") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    };
    HelixClient::connect(uri).await
}

/// Runs the write benchmark.
async fn run_write_benchmark(
    addr: String,
    topic: String,
    partition: i32,
    total_records: u64,
    record_size: usize,
    batch_size: u32,
    num_clients: u32,
    acks: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Helix Write Benchmark ===");
    println!("Server:       {addr}");
    println!("Topic:        {topic}");
    println!("Partition:    {partition}");
    println!("Records:      {total_records}");
    println!("Record size:  {record_size} bytes");
    println!("Batch size:   {batch_size}");
    println!("Clients:      {num_clients}");
    println!("Acks:         {acks}");
    println!();

    let records_per_client = total_records / u64::from(num_clients);
    let batches_per_client = records_per_client / u64::from(batch_size);

    let total_sent = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));

    let start = Instant::now();
    let mut join_set = JoinSet::new();

    for client_id in 0..num_clients {
        let addr = addr.clone();
        let topic = topic.clone();
        let total_sent = total_sent.clone();
        let latencies = latencies.clone();
        let client_partition = if partition < 0 {
            (client_id % 4) as i32
        } else {
            partition
        };

        join_set.spawn(async move {
            let mut client = create_client(&addr).await?;

            // Pre-create record template.
            let records: Vec<Record> = (0..batch_size)
                .map(|_| Record {
                    key: None,
                    value: vec![0u8; record_size],
                    headers: Default::default(),
                    timestamp_ms: None,
                })
                .collect();

            for _ in 0..batches_per_client {
                let request = WriteRequest {
                    topic: topic.clone(),
                    partition: client_partition,
                    records: records.clone(),
                    required_acks: acks,
                };

                let op_start = Instant::now();
                let response = client.write(request).await?;
                let duration = op_start.elapsed();

                if response.get_ref().error_code != 0 {
                    eprintln!(
                        "Write error: {:?}",
                        response.get_ref().error_message
                    );
                }

                total_sent.fetch_add(u64::from(batch_size), Ordering::Relaxed);
                latencies
                    .lock()
                    .await
                    .record(duration.as_micros() as u64)
                    .ok();
            }

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
    }

    // Wait for all clients.
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("Client error: {e}"),
            Err(e) => eprintln!("Join error: {e}"),
        }
    }

    let elapsed = start.elapsed();
    let total = total_sent.load(Ordering::Relaxed);
    let throughput = total as f64 / elapsed.as_secs_f64();
    let mb_per_sec = (total as f64 * record_size as f64) / elapsed.as_secs_f64() / 1_000_000.0;

    println!("=== Results ===");
    println!("Total records:    {total}");
    println!("Total time:       {:.2?}", elapsed);
    println!("Throughput:       {throughput:.0} records/sec");
    println!("Throughput:       {mb_per_sec:.2} MB/sec");
    println!();

    let hist = latencies.lock().await;
    println!("=== Latency (per batch) ===");
    println!("  p50:   {} us", hist.value_at_quantile(0.50));
    println!("  p90:   {} us", hist.value_at_quantile(0.90));
    println!("  p95:   {} us", hist.value_at_quantile(0.95));
    println!("  p99:   {} us", hist.value_at_quantile(0.99));
    println!("  p99.9: {} us", hist.value_at_quantile(0.999));
    println!("  max:   {} us", hist.max());

    Ok(())
}

/// Runs the read benchmark.
async fn run_read_benchmark(
    addr: String,
    topic: String,
    partition: i32,
    start_offset: u64,
    total_records: u64,
    batch_size: u32,
    num_clients: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Helix Read Benchmark ===");
    println!("Server:       {addr}");
    println!("Topic:        {topic}");
    println!("Partition:    {partition}");
    println!("Start offset: {start_offset}");
    println!("Records:      {total_records}");
    println!("Batch size:   {batch_size}");
    println!("Clients:      {num_clients}");
    println!();

    let records_per_client = total_records / u64::from(num_clients);
    let batches_per_client = records_per_client / u64::from(batch_size);

    let total_read = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));

    let start = Instant::now();
    let mut join_set = JoinSet::new();

    for client_id in 0..num_clients {
        let addr = addr.clone();
        let topic = topic.clone();
        let total_read = total_read.clone();
        let total_bytes = total_bytes.clone();
        let latencies = latencies.clone();

        // Each client reads a different range.
        let client_start = start_offset + (client_id as u64 * records_per_client);

        join_set.spawn(async move {
            let mut client = create_client(&addr).await?;
            let mut offset = client_start;

            for _ in 0..batches_per_client {
                let request = ReadRequest {
                    topic: topic.clone(),
                    partition,
                    offset,
                    max_records: batch_size,
                    max_bytes: 10 * 1024 * 1024, // 10MB
                };

                let op_start = Instant::now();
                let response = client.read(request).await?;
                let duration = op_start.elapsed();

                let records = &response.get_ref().records;
                let count = records.len() as u64;
                let bytes: u64 = records
                    .iter()
                    .filter_map(|r| r.record.as_ref())
                    .map(|r| r.value.len() as u64)
                    .sum();

                total_read.fetch_add(count, Ordering::Relaxed);
                total_bytes.fetch_add(bytes, Ordering::Relaxed);
                offset += count;

                latencies
                    .lock()
                    .await
                    .record(duration.as_micros() as u64)
                    .ok();
            }

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
    }

    // Wait for all clients.
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("Client error: {e}"),
            Err(e) => eprintln!("Join error: {e}"),
        }
    }

    let elapsed = start.elapsed();
    let total = total_read.load(Ordering::Relaxed);
    let bytes = total_bytes.load(Ordering::Relaxed);
    let throughput = total as f64 / elapsed.as_secs_f64();
    let mb_per_sec = bytes as f64 / elapsed.as_secs_f64() / 1_000_000.0;

    println!("=== Results ===");
    println!("Total records:    {total}");
    println!("Total bytes:      {bytes}");
    println!("Total time:       {:.2?}", elapsed);
    println!("Throughput:       {throughput:.0} records/sec");
    println!("Throughput:       {mb_per_sec:.2} MB/sec");
    println!();

    let hist = latencies.lock().await;
    println!("=== Latency (per batch) ===");
    println!("  p50:   {} us", hist.value_at_quantile(0.50));
    println!("  p90:   {} us", hist.value_at_quantile(0.90));
    println!("  p95:   {} us", hist.value_at_quantile(0.95));
    println!("  p99:   {} us", hist.value_at_quantile(0.99));
    println!("  p99.9: {} us", hist.value_at_quantile(0.999));
    println!("  max:   {} us", hist.max());

    Ok(())
}

/// Runs the end-to-end latency benchmark.
async fn run_latency_benchmark(
    addr: String,
    topic: String,
    partition: i32,
    ops: u64,
    record_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Helix End-to-End Latency Benchmark ===");
    println!("Server:       {addr}");
    println!("Topic:        {topic}");
    println!("Partition:    {partition}");
    println!("Operations:   {ops}");
    println!("Record size:  {record_size} bytes");
    println!();

    let mut client = create_client(&addr).await?;
    let mut latencies = Histogram::<u64>::new(3)?;

    let start = Instant::now();

    for _ in 0..ops {
        let records = vec![Record {
            key: None,
            value: vec![0u8; record_size],
            headers: Default::default(),
            timestamp_ms: None,
        }];

        let write_req = WriteRequest {
            topic: topic.clone(),
            partition,
            records,
            required_acks: 1,
        };

        let op_start = Instant::now();

        // Write.
        let write_resp = client.write(write_req).await?;
        let offset = write_resp.get_ref().base_offset;

        // Read back.
        let read_req = ReadRequest {
            topic: topic.clone(),
            partition,
            offset,
            max_records: 1,
            max_bytes: 1024 * 1024,
        };

        let _read_resp = client.read(read_req).await?;

        let duration = op_start.elapsed();
        latencies.record(duration.as_micros() as u64).ok();
    }

    let elapsed = start.elapsed();
    let throughput = ops as f64 / elapsed.as_secs_f64();

    println!("=== Results ===");
    println!("Total operations: {ops}");
    println!("Total time:       {:.2?}", elapsed);
    println!("Throughput:       {throughput:.0} ops/sec");
    println!();

    println!("=== End-to-End Latency (write + read) ===");
    println!("  p50:   {} us", latencies.value_at_quantile(0.50));
    println!("  p90:   {} us", latencies.value_at_quantile(0.90));
    println!("  p95:   {} us", latencies.value_at_quantile(0.95));
    println!("  p99:   {} us", latencies.value_at_quantile(0.99));
    println!("  p99.9: {} us", latencies.value_at_quantile(0.999));
    println!("  max:   {} us", latencies.max());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    match args.command {
        Command::Write {
            topic,
            partition,
            records,
            size,
            batch_size,
            clients,
            acks,
        } => {
            run_write_benchmark(
                args.addr, topic, partition, records, size, batch_size, clients, acks,
            )
            .await?;
        }
        Command::Read {
            topic,
            partition,
            offset,
            records,
            batch_size,
            clients,
        } => {
            run_read_benchmark(args.addr, topic, partition, offset, records, batch_size, clients)
                .await?;
        }
        Command::Latency {
            topic,
            partition,
            ops,
            size,
        } => {
            run_latency_benchmark(args.addr, topic, partition, ops, size).await?;
        }
    }

    Ok(())
}
