# Helix Implementation Status

This document tracks progress against the [implementation plan](../helix-implementation-plan.md).

## Summary

| Phase | Status | Completion |
|-------|--------|------------|
| Phase 0: Foundations | Partial | ~60% |
| Phase 1: Core Consensus | Partial | ~70% |
| Phase 2: Multi-Raft & Sharding | Not Started | 0% |
| Phase 3: Storage Features | Not Started | 0% |
| Phase 4: API & Flow Control | Partial (out of order) | ~30% |
| Phase 5: Production Readiness | Not Started | 0% |

## Deviations from Plan

### 1. Out-of-Order Implementation

We built components out of order:
- Built `helix-partition` (not in original plan) before completing Phase 1
- Built `helix-server` gRPC API (Phase 4.1) before Phase 2 (Multi-Raft)
- Skipped extensive Bloodhound testing milestones

### 2. Missing Testing Milestones

The plan requires:
- **10,000 simulated hours with random faults, zero safety violations** - NOT ACHIEVED
- **ScenarioBuilder test patterns** - NOT USED
- **PropertyExecutor for invariant checking** - BASIC ONLY
- **TLA+ trace validation** - NOT IMPLEMENTED

### 3. Architecture Differences

- **helix-partition**: Created but not in original plan. Combines partition storage with Raft replication.
- **Multi-Raft**: Plan calls for `MultiRaft` engine managing thousands of groups per node. Current implementation has one RaftNode per partition (inefficient).

---

## Detailed Phase Status

### Phase 0: Foundations

#### 0.1 TLA+ Specifications

| Item | Status | Notes |
|------|--------|-------|
| `raft.tla` - Core Raft consensus | ✅ Done | In `specs/` |
| `raft_log.tla` - Log replication invariants | ❌ Not Started | |
| `multi_raft.tla` - Multi-group coordination | ❌ Not Started | |
| `progress.tla` - Consumer progress tracking | ❌ Not Started | |
| `tiering.tla` - Tiered storage consistency | ❌ Not Started | |
| Model checking CI job (TLC) | ❌ Not Started | |

#### 0.2 Helix Core Types

| Item | Status | Notes |
|------|--------|-------|
| `helix-core` crate | ✅ Done | |
| Strongly-typed IDs | ✅ Done | NodeId, TopicId, PartitionId, etc. |
| Configuration and limits | ✅ Done | `limits.rs` |
| Error types hierarchy | ✅ Done | `error.rs` |
| Message serialization (prost) | ⚠️ Partial | Using custom binary, not prost |

#### 0.3 Bloodhound Integration

| Item | Status | Notes |
|------|--------|-------|
| Bloodhound as workspace dependency | ✅ Done | |
| `RaftActor` implementing `SimulatedActor` | ✅ Done | `helix-tests/raft_actor.rs` |
| Property definitions for Raft invariants | ⚠️ Basic | `properties.rs` exists but minimal |
| ScenarioBuilder test examples | ❌ Not Used | Tests don't use ScenarioBuilder |
| CI with multiple seed runs | ❌ Not Set Up | |

#### 0.4 Production Runtime

| Item | Status | Notes |
|------|--------|-------|
| `helix-runtime` crate | ✅ Done | |
| Tokio-based time utilities | ✅ Done | |
| TCP Network implementation | ✅ Done | `transport.rs` |
| io_uring Storage implementation | ❌ Not Started | |
| Feature flags (sim vs prod) | ❌ Not Implemented | |

---

### Phase 1: Core Consensus

#### 1.1 WAL Implementation

| Item | Status | Notes |
|------|--------|-------|
| Segment format and serialization | ✅ Done | |
| Append-only writes with CRC32 | ✅ Done | |
| Read by index | ✅ Done | |
| Fsync batching (group commit) | ⚠️ Basic | |
| Segment rotation | ✅ Done | |
| Crash recovery | ✅ Done | |
| Truncation for compaction | ✅ Done | |

**Testing Milestones:**
| Item | Status |
|------|--------|
| Unit tests for segment format | ✅ Done |
| Bloodhound: 1000 random write/read sequences | ❌ Not Done |
| Bloodhound: crash recovery with torn writes | ❌ Not Done |
| Bloodhound: concurrent append + read | ❌ Not Done |

#### 1.2 Raft State Machine

| Item | Status | Notes |
|------|--------|-------|
| State transitions | ✅ Done | |
| Leader election (RequestVote) | ✅ Done | |
| Log replication (AppendEntries) | ✅ Done | |
| Commit index advancement | ✅ Done | |
| Leader heartbeats | ✅ Done | |
| Pre-vote extension | ❌ Not Implemented | Prevents disruption |
| Leadership transfer | ❌ Not Implemented | |
| Configuration changes (joint consensus) | ❌ Not Implemented | |

**Testing Milestones:**
| Item | Status |
|------|--------|
| Bloodhound: leader election in 3-node | ✅ Done |
| Bloodhound: election with network partition | ⚠️ Basic |
| Bloodhound: log replication linearizability | ❌ Not Done |
| Bloodhound: 10,000 hours random faults | ❌ NOT DONE - CRITICAL |
| Property: SingleLeaderPerTerm | ⚠️ Basic check only |
| Property: LogMatching | ❌ Not Done |
| TLA+ trace validation | ❌ Not Done |

#### 1.3 Raft Client Interface

| Item | Status | Notes |
|------|--------|-------|
| RaftClient struct | ✅ Done | `helix-raft/client.rs` |
| Leader tracking and retry | ⚠️ Basic | |
| Linearizable reads | ❌ Not Implemented | |

#### 1.4 Benchmarking & Optimization

| Item | Status |
|------|--------|
| Benchmark: single-node write throughput | ❌ Not Done |
| Benchmark: 3-node replication latency | ❌ Not Done |
| Optimize: batch AppendEntries | ❌ Not Done |
| Optimize: pipelining | ❌ Not Done |
| Optimize: parallel disk/network | ❌ Not Done |

---

### Phase 2: Multi-Raft & Sharding

**Status: NOT STARTED**

The plan calls for:
- `MultiRaft` engine managing thousands of Raft groups per node
- Shared WAL across groups
- Message batching to same node
- Election staggering
- `helix-routing` crate for shard routing

Current state: Each partition has its own independent RaftNode. No shared infrastructure.

---

### Phase 3: Storage Features

**Status: NOT STARTED**

Missing crates:
- `helix-tier` - Tiered storage to S3
- `helix-progress` - Consumer progress tracking with leases

---

### Phase 4: API & Flow Control

#### 4.1 gRPC API Layer

| Item | Status | Notes |
|------|--------|-------|
| HelixServer struct | ⚠️ Done but simple | `helix-server` |
| Write/Read/Metadata RPCs | ✅ Done | |
| Integration with Multi-Raft | ❌ No Multi-Raft yet | |
| Integration with ShardRouter | ❌ No routing yet | |

**Note:** Built out of order. Plan says this comes after Multi-Raft (Phase 2).

#### 4.2 Flow Control

**Status: NOT STARTED**

Missing: `helix-flow` crate with token buckets, weighted fair queues.

#### 4.3 Kafka Compatibility Proxy

**Status: NOT STARTED**

Missing: `helix-kafka-proxy` crate.

---

### Phase 5: Production Readiness

**Status: NOT STARTED**

---

## Recommended Next Steps

To get back on track with the plan:

1. **Complete Phase 1 Testing Milestones** (CRITICAL)
   - Implement comprehensive Bloodhound simulation tests
   - Run 10,000+ simulated hours with fault injection
   - Verify SingleLeaderPerTerm and LogMatching properties
   - Add TLA+ trace validation

2. **Complete Missing Raft Features**
   - Pre-vote extension
   - Leadership transfer
   - (Optional) Configuration changes

3. **Implement Multi-Raft (Phase 2)**
   - Create `MultiRaft` engine
   - Message batching
   - Election staggering
   - Create `helix-routing` for shard management

4. **Refactor helix-server**
   - Wire to Multi-Raft instead of individual RaftNodes per partition

---

## Crates: Actual vs Plan

| Planned Crate | Status | Actual Implementation |
|---------------|--------|----------------------|
| `helix-core` | ✅ Exists | As planned |
| `helix-wal` | ✅ Exists | As planned |
| `helix-raft` | ⚠️ Partial | Missing multi.rs, pre-vote, leadership transfer |
| `helix-runtime` | ⚠️ Partial | Missing io_uring |
| `helix-routing` | ❌ Missing | Need to create |
| `helix-tier` | ❌ Missing | Need to create |
| `helix-progress` | ❌ Missing | Need to create |
| `helix-flow` | ❌ Missing | Need to create |
| `helix-server` | ⚠️ Exists | Built early, needs Multi-Raft integration |
| `helix-kafka-proxy` | ❌ Missing | Need to create |
| `helix-cli` | ❌ Missing | Need to create |
| `helix-tests` | ⚠️ Partial | Needs more comprehensive Bloodhound tests |
| `helix-partition` | ⚠️ Extra | NOT IN PLAN - combines partition + replication |
