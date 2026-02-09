# Follower Reads (KIP-392)

## Overview

Kafka [KIP-392](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica) (shipped in Kafka 2.4) allows consumers to fetch from follower replicas instead of always going to the leader. Followers serve data up to their local high watermark — committed data they've already replicated.

This reduces cross-datacenter traffic and improves read latency when consumers are co-located with followers rather than the leader.

## Current Behavior

All reads go to the partition leader. Followers reject reads with `NOT_LEADER_OR_FOLLOWER` (error code 6), forcing the client to find the leader.

The gate is in `service/handlers/blob.rs`:

```rust
let (is_leader, leader_hint) = self.check_leadership(group_id).await;
if !is_leader {
    return Err(ServerError::NotLeader { ... });
}
```

## Why This Is Low-Cost

Followers already have the data. Raft replication via `AppendEntries` copies all committed entries to followers, and the `OutputProcessor` applies them to `PartitionStorage`. The follower's commit index (propagated by the leader in every `AppendEntries` RPC) is the high watermark.

There's no new replication protocol, no new storage layer, and no consistency model change — Kafka consumers already accept eventual consistency from follower reads.

## Implementation Plan

### Phase 1: Core Read Path (Small)

Relax the leadership gate in `read_blobs` to allow follower reads when configured:

```rust
if !is_leader && !partition_allows_follower_reads {
    return Err(ServerError::NotLeader { ... });
}

// Clamp reads to local applied index (not commit_index, since
// OutputProcessor may lag behind commit by a few entries).
let local_hwm = partition_storage.last_applied.get();
// Serve data up to local_hwm only.
```

Key detail: clamp to `last_applied`, not `commit_index`. The `OutputProcessor` applies committed entries asynchronously, so a follower's `commit_index` might be at 100 but `last_applied` at 95. Only data at or below `last_applied` is in `PartitionStorage` and readable.

**Files:** `service/handlers/blob.rs`, `kafka/handler.rs` (Fetch handler)

### Phase 2: Kafka Protocol (Small)

The `FetchRequest` already has a `ReplicaId` field that indicates whether the fetch is from a consumer (`replica_id = -1`) or a follower (`replica_id >= 0`). For follower reads, Kafka uses `replica_id = -1` with the broker routing the request locally.

The `FetchResponse` partition data includes `preferred_read_replica` (added in FetchResponse v11+). This tells the client which replica to fetch from next.

**Files:** `kafka/handler.rs` (parse/emit `preferred_read_replica`)

### Phase 3: Metadata and Replica Selection (Medium)

This is the bulk of the work. Kafka clients discover preferred replicas through:

1. **Broker rack configuration**: Each broker has a `broker.rack` config (e.g., `us-east-1a`)
2. **Consumer rack configuration**: Consumer sets `client.rack` (e.g., `us-east-1a`)
3. **Replica selector**: `ReplicaSelector` interface matches consumer rack to broker rack
4. **MetadataResponse**: Includes replica list per partition with rack info

For Helix, this requires:
- Rack ID in node/broker configuration
- Rack-aware replica selection logic
- MetadataResponse to advertise replicas with rack info
- `preferred_read_replica` in FetchResponse

### Phase 4: Testing

- E2E test: produce to leader, consume from follower, verify data up to HWM
- Lag test: verify follower doesn't serve data beyond its `last_applied`
- Failover test: when preferred replica crashes, client falls back to leader
- Rack-aware test: verify client selects closest replica

## Design Considerations

### Consistency Guarantees

Follower reads are **eventually consistent**. A consumer reading from a follower may see data that lags behind the leader by the replication delay (typically milliseconds). This is identical to Kafka's semantics.

Read-your-writes is NOT guaranteed: a producer writing to the leader and immediately reading from a follower may not see the write. This is expected and documented in KIP-392.

### Applied Index vs Commit Index

The high watermark for follower reads must be `last_applied` (the index up to which `OutputProcessor` has written data to `PartitionStorage`), not `commit_index` (the Raft commit index). The gap between these is usually small (sub-millisecond) but matters for correctness — serving an offset that hasn't been applied yet would return stale or missing data.

### Without Rack Awareness

A minimal version without rack awareness is possible: any follower serves reads, and the client explicitly targets a broker. This skips Phase 3 entirely but requires the client to know the cluster topology. Useful for testing and simple deployments.

## Estimated Effort

| Phase | Effort | Prerequisite |
|-------|--------|-------------|
| Phase 1: Core read path | ~50 lines | None |
| Phase 2: Kafka protocol fields | ~30 lines | Phase 1 |
| Phase 3: Rack-aware metadata | ~200 lines | Rack config design |
| Phase 4: Tests | ~100 lines | Phase 1 |

Phase 1+2+4 can ship without Phase 3. Phase 3 requires decisions about rack configuration format and node topology that affect the operational deployment model.
