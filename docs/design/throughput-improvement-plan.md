# Helix Throughput Optimization

**Status**: 100 MB/s stable throughput achieved
**Updated**: 2026-01-17

## Current Performance

| Configuration | Throughput | Notes |
|---------------|------------|-------|
| Single producer | 11 MB/s | Baseline |
| 6 producers, optimized | **100 MB/s** | Stable, 0 errors |
| Peak (4 producers) | 130 MB/s | Before backpressure limits |

## Architecture

### Storage Model

Helix follows Kafka's durability model:

| Component | Storage | Durable? |
|-----------|---------|----------|
| Raft log (`RaftLog`) | In-memory | No |
| Partition data (`SharedWal`) | Disk | **Yes** |

- **Raft state is ephemeral** - lost on crash, recovered from peers
- **Partition WAL is the source of truth** - survives crashes
- On full cluster restart: each node recovers from disk WAL, Raft elects new leader, writes continue from last durable offset

### Write Path

```
Producer Request
      ↓
  [Batcher]           ← Collects requests for 1ms or until batch full
      ↓
  [Raft Propose]      ← Single proposal for entire batch
      ↓
  [Replication]       ← Push to followers, wait for majority
      ↓
  [Commit]            ← Apply to partition WAL
      ↓
  Response to clients
```

---

## Completed Optimizations

### 1. Request Batching

**Impact**: ~10x improvement

Multiple client requests are batched into a single Raft proposal:

```
Without batching: 100 requests → 100 Raft rounds → 100 × 20ms = 2000ms
With batching:    100 requests → 1 Raft round   → 1 × 25ms  = 25ms
```

Configuration in `helix-server/src/service/batcher.rs`:
- `linger_ms: 1` - Wait up to 1ms to collect requests
- `max_batch_bytes: 4MB` - Flush when batch reaches 4MB
- `max_batch_requests: 1000` - Flush at 1000 requests

### 2. Backpressure

**Impact**: Stability under load

Prevents unbounded queue growth that caused commit latency explosion:

```rust
const MAX_PENDING_REQUESTS: u64 = 2000;
const MAX_PENDING_BYTES: u64 = 100 * 1024 * 1024; // 100MB
```

When limits are reached, returns `BROKER_NOT_AVAILABLE` (Kafka error 9), triggering client retry with backoff.

### 3. Skip Fsync (Replication-Only Durability)

**Impact**: ~8% (minor, but matches Kafka's model)

With `WriteDurability::ReplicationOnly` (default):
- Data written to OS page cache, not fsynced
- Durability provided by Raft replication across nodes
- Safe per Raft thesis 10.2.1

CLI: `--write-durability replication-only` (default) or `--write-durability fsync`

### 4. Raft Pipelining

**Impact**: Improved concurrent throughput

Leader sends multiple AppendEntries without waiting for each response:
- `MAX_INFLIGHT_APPEND_ENTRIES = 5`
- Implemented in `helix-raft/src/state.rs`

---

## Future Optimizations

### Per-Partition Locking

**Expected Impact**: 3-5x for multi-partition workloads

Currently all partitions serialize through a global `MultiRaft` lock. Sharding by partition would allow parallel writes:

```rust
pub struct ShardedMultiRaft {
    shards: Vec<RwLock<MultiRaft>>,
}
```

### WAL Lock Scope Reduction

**Expected Impact**: 2-3x

Currently the WAL lock is held during disk I/O. Reducing lock scope would eliminate occasional 2-3ms latency spikes.

### Metadata-Only Raft

**Expected Impact**: 2-5x for large messages

For large payloads (1MB+), replicate only metadata through Raft; followers fetch data separately.

### Out-of-Band Heartbeats

**Expected Impact**: Stability improvement

Separate heartbeats from data path to prevent elections under high load. Currently AppendEntries carries both data and heartbeats.

---

## Client Configuration

### Low-Latency Mode (Default)

For sequential/synchronous producers:

```
queue.buffering.max.ms = 0    # Send immediately
```

### High-Throughput Mode

For concurrent producers with async sends:

```
queue.buffering.max.ms = 5    # 5ms linger
batch.size = 524288           # 512KB batches
batch.num.messages = 10000
```

### Recommended Settings

| Scenario | Producers | Inflight/Producer | Linger |
|----------|-----------|-------------------|--------|
| Low latency | 1 | 100 | 0ms |
| Balanced | 4 | 500 | 5ms |
| Max throughput | 6 | 2000 | 5ms |

---

## Benchmark Profiles

Run throughput tests with built-in profiles:

```bash
HELIX_PROFILE=baseline cargo test -p helix-workload --test steady_state_throughput
HELIX_PROFILE=throughput cargo test -p helix-workload --test steady_state_throughput
```

| Profile | Producers | Inflight | Duration | Use Case |
|---------|-----------|----------|----------|----------|
| baseline | 1 | 100 | 5s | Quick sanity check |
| throughput | 6 | 2000 | 10s | Maximum throughput |
| latency | 4 | 100 | 10s | Low latency |
| stress | 12 | 4000 | 30s | Find limits |
| sustained | 6 | 1000 | 60s | Stability validation |

Custom profiles via TOML:

```toml
name = "custom"

[producer]
threads = 8
inflight = 3000
message_size = 1024
linger_ms = 5

[execution]
warmup_secs = 3
duration_secs = 10
```

---

## References

- [Why Apache Kafka Doesn't Need Fsync](https://jack-vanlightly.com/blog/2023/4/24/why-apache-kafka-doesnt-need-fsync-to-be-safe)
- [Kafka vs Redpanda Performance](https://jack-vanlightly.com/blog/2023/5/15/kafka-vs-redpanda-performance-do-the-claims-add-up)
