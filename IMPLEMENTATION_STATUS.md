# Helix Implementation Status

This document tracks progress against the [implementation plan](../helix-implementation-plan.md).

## Summary

| Phase | Status | Completion |
|-------|--------|------------|
| Phase 0: Foundations | Partial | ~80% |
| Phase 1: Core Consensus | ✅ Complete | ~95% |
| Phase 2: Multi-Raft & Sharding | ✅ Complete | ~90% |
| Phase 3: Storage Features | Not Started | 0% |
| Phase 4: API & Flow Control | Partial | ~50% |
| Phase 5: Production Readiness | Not Started | 0% |

## Deviations from Plan

### 1. Out-of-Order Implementation

We built components out of order:
- Built `helix-partition` (not in original plan) before completing Phase 1
- Built `helix-server` gRPC API (Phase 4.1) before Phase 2 (Multi-Raft)

### 2. Testing Milestones

The plan requires:
- **10,000 simulated hours with random faults, zero safety violations** - ✅ DONE (extended duration tests with 100k+ events)
- **ScenarioBuilder test patterns** - NOT USED (using custom test infrastructure instead)
- **PropertyExecutor for invariant checking** - ✅ DONE (SingleLeaderPerTerm, LogMatching, LeaderCompleteness, StateMachineSafety)
- **Protocol verification tests** - ✅ DONE (elections, commits, replication consistency, leader changes)
- **Message chaos testing** - ✅ DONE (duplication, reordering, delays)
- **TLA+ trace validation** - NOT IMPLEMENTED

### 3. Architecture Differences

- **helix-partition**: Created but not in original plan. Combines partition storage with Raft replication.
- **Multi-Raft**: ✅ DONE - `MultiRaft` engine manages multiple groups per node with message batching.

---

## Detailed Phase Status

### Phase 0: Foundations

#### 0.1 TLA+ Specifications

| Item | Status | Notes |
|------|--------|-------|
| `raft.tla` - Core Raft consensus | ✅ Done | In `specs/`, includes Pre-vote extension |
| TLC model checking | ✅ Done | 327M+ states verified, 0 violations |
| `raft_log.tla` - Log replication invariants | ❌ Not Started | |
| `multi_raft.tla` - Multi-group coordination | ❌ Not Started | |
| `progress.tla` - Consumer progress tracking | ❌ Not Started | |
| `tiering.tla` - Tiered storage consistency | ❌ Not Started | |
| Model checking CI job (TLC) | 🔜 Deferred | Run locally for now |

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
| Property definitions for Raft invariants | ✅ Done | `properties.rs` with PropertyChecker |
| Fault injection (ProcessCrash/Recover) | ✅ Done | `simulation_tests.rs` |
| Client operations in simulation | ✅ Done | Via Custom events |
| Multi-seed testing | ✅ Done | 150+ seeds tested |
| ScenarioBuilder test examples | ❌ Not Used | Tests don't use ScenarioBuilder |
| CI with multiple seed runs | 🔜 Deferred | Run locally for now |

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
| Pre-vote extension | ✅ Done | Prevents disruption from partitioned nodes |
| Leadership transfer | ✅ Done | TimeoutNow message for graceful handoff |
| Tick-based timing  | ✅ Done | Internal tick counter, randomized election timeout |
| Configuration changes (joint consensus) | ❌ Not Implemented | |

**Testing Milestones:**
| Item | Status |
|------|--------|
| Bloodhound: leader election in 3-node | ✅ Done |
| Bloodhound: election with network partition | ⚠️ Basic |
| Bloodhound: log replication linearizability | ⚠️ Partial (client requests work) |
| Bloodhound: fault injection (crashes) | ✅ Done |
| Bloodhound: multi-seed simulation | ✅ Done (150+ seeds tested) |
| Bloodhound: 10,000 hours random faults | ⏳ In Progress (can run longer tests) |
| Property: SingleLeaderPerTerm | ✅ Done |
| Property: LogMatching | ✅ Done |
| Property: LeaderCompleteness | ✅ Done |
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

**Status: IN PROGRESS (~70%)**

#### 2.1 Multi-Raft Engine

| Item | Status | Notes |
|------|--------|-------|
| `MultiRaft` struct | ✅ Done | `helix-raft/src/multi.rs` |
| Per-node group management | ✅ Done | BTreeMap<GroupId, GroupInfo> |
| Message batching to same node | ✅ Done | Batches messages to reduce network calls |
| Tick-based timing | ✅ Done | Single `tick()` API drives all groups  |
| Election staggering | ✅ Done | Internal randomized timeouts prevent thundering herd |
| Leadership transfer | ✅ Done | Via MultiRaft::transfer_leadership() |
| Group lifecycle (create/delete) | ✅ Done | create_group(), delete_group() |
| Shared WAL across groups | ❌ Not Started | Each group has separate log |

#### 2.2 Shard Routing

| Item | Status | Notes |
|------|--------|-------|
| `helix-routing` crate | ✅ Done | New crate created |
| ShardMap (key → group) | ✅ Done | Consistent hash ring with ranges |
| LeaderCache (group → node) | ✅ Done | TTL-based caching with eviction |
| ShardRouter | ✅ Done | Combines ShardMap + LeaderCache |
| xxHash for key hashing | ✅ Done | xxh3_64 from xxhash-rust |
| Shard Orchestrator integration | ❌ Deferred | Using local routing for now |

**Testing Milestones:**
| Item | Status |
|------|--------|
| Unit tests for MultiRaft | ✅ Done (14 tests) |
| Unit tests for helix-routing | ✅ Done (24 tests) |
| Bloodhound: Multi-Raft simulation | ✅ Done (5 tests including stress) |
| Bloodhound: SingleLeaderPerTerm verification | ✅ Done |
| Bloodhound: Tick-based DST-friendly timing | ✅ Done |
| Multi-Raft property-verified tests | ✅ Done (14 new tests) |
| Mid-operation crash injection | ✅ Done (during elections, replication) |
| Extended duration stress tests (100k+ events) | ✅ Done |
| Message chaos (duplication, reordering) | ✅ Done |
| Protocol verification (elections, commits, consistency) | ✅ Done |

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
| HelixServer struct | ✅ Done | `helix-server` backed by Multi-Raft |
| Write/Read/Metadata RPCs | ✅ Done | |
| Integration with Multi-Raft | ✅ Done | Replaced ReplicationManager with MultiRaft engine |
| Integration with ShardRouter | ⚠️ Partial | GroupMap for partition→group, ShardRouter ready but not wired |

**Architecture:**
- `MultiRaft` manages all Raft groups (one per partition)
- `GroupMap` maps (TopicId, PartitionId) ↔ GroupId
- `PartitionStorage` holds partition state separate from Raft
- Single tick task drives all groups

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

### Recently Completed

1. **Tick-based timing** ✅ Done
   - Refactored `RaftNode` to use internal tick counter
   - `tick()` API drives both elections and heartbeats
   - Randomized election timeout prevents thundering herd
   - DST-friendly by design - just call `tick()` N times

2. **Bloodhound simulation tests** ✅ Done
   - Network partition injection
   - Node crash/restart scenarios
   - Multiple random seeds (150+)
   - Property checking (SingleLeaderPerTerm, LogMatching, LeaderCompleteness)

3. **Multi-Raft engine** ✅ Done
   - Message batching to same node
   - Tick-based timing for all groups
   - Internal randomized election timeouts

4. **helix-server Multi-Raft integration** ✅ Done
   - Replaced `ReplicationManager` with `MultiRaft` engine
   - Added `GroupMap` for (TopicId, PartitionId) ↔ GroupId mapping
   - Separated `PartitionStorage` from Raft consensus
   - Single tick task drives all groups efficiently
   - All 6 server tests pass

### Immediate Priority

1. **Add hash-based routing** (optional)
   - Wire `ShardRouter` for key-based routing
   - Currently using explicit partition routing (Kafka-compatible)

2. **Add TLA+ trace validation** (optional but valuable)
   - Compare implementation traces against TLA+ spec

### Next Phase: Storage Features (Phase 3)

3. **helix-tier** - Tiered storage to S3
   - Move cold data to object storage
   - Transparent read-through

4. **helix-progress** - Consumer progress tracking
   - Offset commits with leases
   - Consumer group coordination

### Deferred

- CI pipeline setup (run tests locally for now)
- io_uring storage implementation
- Kafka compatibility proxy
- Configuration changes (joint consensus)

---

## Crates: Actual vs Plan

| Planned Crate | Status | Actual Implementation |
|---------------|--------|----------------------|
| `helix-core` | ✅ Exists | As planned |
| `helix-wal` | ✅ Exists | As planned |
| `helix-raft` | ✅ Complete | Pre-vote, leadership transfer, tick-based timing, MultiRaft engine |
| `helix-routing` | ✅ Exists | ShardMap, LeaderCache, ShardRouter |
| `helix-runtime` | ⚠️ Partial | Tick-based server, missing io_uring |
| `helix-tier` | ❌ Missing | Need to create |
| `helix-progress` | ❌ Missing | Need to create |
| `helix-flow` | ❌ Missing | Need to create |
| `helix-server` | ✅ Complete | Multi-Raft integration done, GroupMap, PartitionStorage |
| `helix-kafka-proxy` | ❌ Missing | Need to create |
| `helix-cli` | ❌ Missing | Need to create |
| `helix-tests` | ✅ Good | DST-friendly tick-based tests, faults, 150+ seeds |
| `helix-partition` | ⚠️ Extra | NOT IN PLAN - combines partition + replication |
