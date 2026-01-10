# ADR-0001: Use Raft for Partition Replication

## Status

Accepted

## Context

Helix needs to replicate partition data across multiple nodes for durability and availability. Each partition is an append-only log of records that must be consistently replicated to survive node failures.

We need a consensus protocol that provides:
- Strong consistency (linearizable writes)
- Automatic failover when the leader fails
- Ability to catch up lagging replicas
- Well-understood correctness properties

## Decision

Use Raft consensus for partition replication. Each partition has its own Raft group with a configurable replication factor.

## Consequences

**Easier:**
- Linearizable writes through single leader
- Automatic leader election on failures
- Log compaction via snapshots
- Well-understood correctness (TLA+ verified)
- Straightforward implementation (matches our existing helix-raft crate)

**Harder:**
- Write throughput limited by leader (can't parallel write to replicas)
- Cross-partition transactions require additional coordination
- Leader changes cause brief unavailability

## Options Considered

### Option 1: Raft (Chosen)

**Pros:**
- Simple to understand and implement correctly
- Proven in production (etcd, CockroachDB, TiKV)
- Strong leader makes reasoning about consistency easier
- Existing TLA+ specifications available

**Cons:**
- Single leader bottleneck for writes
- Leader election causes brief unavailability

### Option 2: Multi-Paxos

**Pros:**
- More flexible leader election
- Can have different leaders for different log positions

**Cons:**
- More complex to implement correctly
- Harder to reason about
- Less tooling and community support

### Option 3: Chain Replication

**Pros:**
- Can pipeline writes for higher throughput
- Simple failure model

**Cons:**
- Requires separate reconfiguration protocol
- Tail becomes bottleneck for reads
- Less flexible than Raft

### Option 4: Primary-Backup with External Coordinator

**Pros:**
- Simple replication logic
- Can use existing coordination service (ZooKeeper)

**Cons:**
- External dependency
- Split-brain risk if coordinator unavailable
- Kafka's original design had issues with this approach

## References

- [Raft Paper](https://raft.github.io/raft.pdf)
- [TiKV Raft Implementation](https://tikv.org/deep-dive/consensus-algorithm/raft/)
- [Kafka's Move from ZooKeeper to Raft (KRaft)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500)
