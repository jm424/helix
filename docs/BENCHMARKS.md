# Helix Benchmark Results

Benchmark results from local testing. All tests run on Apple Silicon (M-series) Mac.

## Benchmark Tools

Helix includes several benchmark tools:

1. **helix-bench** - Standalone benchmark tool for measuring server performance
2. **criterion benchmarks** - WAL and server micro-benchmarks

### Running Benchmarks

```bash
# Build release binaries
cargo build --release --package helix-server

# Single-node server benchmark
./target/release/helix-server --node-id 1 &
./target/release/helix-bench write --records 100000 --size 1024 --batch-size 100 --clients 4

# WAL micro-benchmarks
cargo bench --package helix-wal

# Server micro-benchmarks
cargo bench --package helix-server
```

## Results Summary

### Single-Node Server (In-Memory)

| Metric | 1 Client | 4 Clients | 16 Clients |
|--------|----------|-----------|------------|
| Write Throughput (records/sec) | 537K | 1.43M | 2.13M |
| Write Throughput (MB/sec) | 549 | 1,466 | 2,182 |
| Write p50 latency | 175 us | 241 us | 720 us |
| Write p99 latency | 259 us | 701 us | 1,243 us |
| Write p99.9 latency | 325 us | 864 us | 1,517 us |

| Metric | 4 Clients |
|--------|-----------|
| Read Throughput (records/sec) | 1.58M |
| Read Throughput (MB/sec) | 1,619 |
| Read p50 latency | 224 us |
| Read p99 latency | 462 us |

### End-to-End Latency (Write + Read)

| Metric | Value |
|--------|-------|
| Throughput | 6,784 ops/sec |
| p50 latency | 138 us |
| p99 latency | 244 us |
| p99.9 latency | 407 us |

### Multi-Node Server (3-Node Docker Cluster)

| Metric | 4 Clients (acks=1) |
|--------|-------------------|
| Write Throughput (records/sec) | 129K |
| Write Throughput (MB/sec) | 132 |
| Write p50 latency | 2,863 us |
| Write p99 latency | 8,703 us |

**Note**: Multi-node results include network overhead from Docker and Raft replication latency.

## Test Configuration

All tests used:
- Record size: 1,024 bytes
- Batch size: 100 records
- Topic: "default" with 4 partitions

## Comparison with Kafka

For comparison, typical Kafka benchmarks show:

| System | Write Throughput | p99 Latency |
|--------|-----------------|-------------|
| Kafka (single-node) | 200K-500K records/sec | 2-10 ms |
| Kafka (3-node, acks=all) | 100K-200K records/sec | 10-50 ms |
| **Helix (single-node)** | **1.4M records/sec** | **700 us** |
| **Helix (3-node, acks=1)** | **129K records/sec** | **8.7 ms** |

**Note**: These are rough comparisons. Kafka benchmarks vary significantly based on configuration, hardware, and workload patterns.

## Benchmark Files

- `helix-wal/benches/wal_write.rs` - WAL append throughput and latency
- `helix-wal/benches/wal_read.rs` - WAL read throughput and latency
- `helix-server/benches/server_throughput.rs` - gRPC server benchmarks
- `helix-server/src/bin/helix_bench.rs` - Standalone benchmark tool

## Future Work

1. **Durable storage benchmarks** - Currently using in-memory storage
2. **io_uring benchmarks** - When io_uring storage is implemented
3. **Network benchmark isolation** - Separate network vs storage overhead
4. **Load generator improvements** - Kafka-compatible load testing
