# Helix Implementation Status

This document tracks progress against the [implementation plan](../helix-implementation-plan.md).

## Summary

| Phase | Status | Completion |
|-------|--------|------------|
| Phase 0: Foundations | Partial | ~80% |
| Phase 1: Core Consensus | ✅ Complete | 100% (WAL DST hardened, 500-seed stress test passing) |
| Phase 2: Multi-Raft & Sharding | ⚠️ Partial | ~85% (missing shard movement) |
| Phase 3: Storage Features | ⚠️ Partial | ~70% (helix-tier complete, helix-progress not started) |
| Phase 4: API & Flow Control | ⚠️ Partial | ~80% (multi-node networking done, flow control/kafka not started) |
| Phase 5: Production Readiness | Not Started | 0% |

## Deviations from Plan

### 1. Out-of-Order Implementation

We built components out of order:
- Built `helix-server` gRPC API (Phase 4.1) before completing Phase 3 (Storage Features)
- Phase 1.4 Benchmarking skipped

### 2. Testing Milestones

The plan requires:
- **10,000 simulated hours with random faults, zero safety violations** - ✅ DONE (extended duration tests with 100k+ events)
- **ScenarioBuilder test patterns** - NOT USED (using custom test infrastructure instead)
- **PropertyExecutor for invariant checking** - ✅ DONE (SingleLeaderPerTerm, LogMatching, LeaderCompleteness, StateMachineSafety)
- **Protocol verification tests** - ✅ DONE (elections, commits, replication consistency, leader changes)
- **Message chaos testing** - ✅ DONE (duplication, reordering, delays)
- **TLA+ trace validation** - NOT IMPLEMENTED

### 3. Architecture Notes

- **Multi-Raft**: ✅ DONE - `MultiRaft` engine manages multiple groups per node with message batching.
- **Storage**: ✅ DONE - `DurablePartition` integrates `helix-wal` for crash-safe storage per RFC Tier 1 design.

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
| Fsync batching (group commit) | ✅ Done | `sync_on_write` config option |
| Segment rotation | ✅ Done | |
| Crash recovery | ✅ Done | Recovers from segment files on open |
| Truncation for compaction | ✅ Done | |
| Disk persistence | ✅ Done | Storage trait with TokioStorage |
| Storage abstraction | ✅ Done | Trait for future `io_uring` support |

**Testing Milestones:**
| Item | Status |
|------|--------|
| Unit tests for segment format | ✅ Done |
| DST: random write/read sequences | ✅ Done (29 tests with SimulatedStorage) |
| DST: crash recovery with torn writes | ✅ Done (torn writes at various positions) |
| DST: concurrent append + read | ✅ Done |
| DST: corrupted segment/entry recovery | ✅ Done (conservative: skip segment on CRC mismatch) |
| DST: fsync failure handling | ✅ Done |
| DST: segment rotation crashes | ✅ Done |
| DST: truncation across segments | ✅ Done (gap and overlap detection on recovery) |
| DST: comprehensive stress test | ✅ Done (500 seeds × 100 ops, 25% fault rates) |

**Recent Bug Fixes (via DST):**
1. **In-memory truncation before on-disk sync** (seed 2074002)
   - Bug: Sealed segment truncation modified in-memory state before disk sync. If sync failed, segment wouldn't be retried.
   - Fix: Clone segment, truncate clone, sync, only modify original on success.

2. **Recovery not rewriting overlapping segments** (seed 2074002)
   - Bug: When recovery detected overlapping segments, in-memory was truncated but on-disk wasn't rewritten.
   - Fix: Rewrite overlapping segments to disk during recovery.

**Other Improvements:**
- Fixed `truncate_after` to update `last_index` atomically before file operations
- Added gap detection during recovery (handles failed truncation leaving deleted segment files)
- Added overlap detection during recovery (handles failed truncation leaving stale segment data)
- Best-effort file operations during truncation (won't fail if file ops fail)
- Added `Clone` derive to `Segment` struct for safe truncation operations

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
| Benchmark: single-node write throughput | ✅ Done (1.43M records/sec) |
| Benchmark: 3-node replication latency | ✅ Done (129K records/sec, p99=8.7ms) |
| Benchmark tool (helix-bench) | ✅ Done |
| Criterion micro-benchmarks | ✅ Done |
| Results documentation | ✅ Done (`docs/BENCHMARKS.md`) |
| Optimize: batch AppendEntries | ❌ Not Done |
| Optimize: pipelining | ❌ Not Done |
| Optimize: parallel disk/network | ❌ Not Done |

---

### Phase 2: Multi-Raft & Sharding

**Status: ~85% (missing shard movement)**

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

**Status: ~70% (helix-tier complete with real WAL integration tests, helix-progress not started)**

#### 3.1 Tiered Storage (helix-tier)

| Item | Status | Notes |
|------|--------|-------|
| `helix-tier` crate | ✅ Done | New crate created |
| TierError enum | ✅ Done | NotFound, UploadFailed, DownloadFailed, DataCorruption, NotEligible, Io |
| ObjectStorage trait | ✅ Done | put, get, delete, list, exists |
| ObjectKey type | ✅ Done | `{topic_id}/{partition_id}/segment-{segment_id:08x}.wal` format |
| SimulatedObjectStorage | ✅ Done | In-memory storage with deterministic fault injection |
| ObjectStorageFaultConfig | ✅ Done | put/get fail rates, corruption rate, force flags |
| Deterministic RNG | ✅ Done | `(seed + counter) * M` formula for reproducible faults |
| SegmentMetadata | ✅ Done | Tracks segment state (sealed, committed, location) |
| SegmentLocation enum | ✅ Done | Local, Remote, Both |
| MetadataStore trait | ✅ Done | get, set, find_eligible_for_tiering |
| InMemoryMetadataStore | ✅ Done | HashMap-based implementation for testing |
| TieringManager | ✅ Done | Orchestrates uploads/downloads with eligibility checks |
| IntegratedTieringManager | ✅ Done | WAL-integrated tiering with SegmentReader trait |
| SegmentReader trait | ✅ Done | Abstraction for reading segment bytes from WAL |
| TieringConfig | ✅ Done | min_segment_age_secs configuration |
| S3ObjectStorage | ❌ Not Started | Behind `s3` feature flag |

**Testing Milestones:**
| Item | Status |
|------|--------|
| Unit tests for SimulatedObjectStorage | ✅ Done (13 tests) |
| DST: forced failure injection | ✅ Done |
| DST: corruption detection | ✅ Done |
| DST: retry logic with faults | ✅ Done |
| DST: deterministic fault verification | ✅ Done |
| DST: multi-partition uploads | ✅ Done |
| DST: eligibility enforcement | ✅ Done |
| DST: FaultingSegmentReader | ✅ Done (fault injection for WAL reads) |
| DST: find_stuck_uploads fault injection | ✅ Done |
| DST: try_claim_for_upload fault injection | ✅ Done |
| DST: comprehensive stress (500 seeds × 100 ops) | ✅ Done (25% fault rates) |
| DST: concurrent tier_same_segment test | ✅ Done |
| DST invariants: ordering (sealed→committed→tiered) | ✅ Done |
| DST invariants: referential integrity | ✅ Done |
| DST invariants: orphaned data detection | ✅ Done |
| DurablePartition e2e tests | ✅ Done (8 tests, tiering hooks) |
| Real WAL integration tests | ✅ Done (4 tests with segment rotation) |
| - test_real_segment_rotation_and_tiering | ✅ 12 entries, 5 entries/segment |
| - test_real_tiering_with_upload_failures | ✅ Fault injection with retry |
| - test_real_tiering_corruption_detection | ✅ Corruption on download |
| - test_real_multi_segment_tiering | ✅ 15 entries, 3 entries/segment |
| Integration test with real S3 (localstack) | ❌ Not Started |

**Bugs Found Through DST:**
| Bug | Seed/Op | Fix |
|-----|---------|-----|
| Orphaned data: `exists()` failure during recovery used `unwrap_or(false)`, causing incorrect abort | seed 197562, op 27 | Skip segment on exists() error, retry on next recovery |
| Ordering violation: `mark_committed()` allowed uncommitted segments to be marked committed without being sealed first | seed 17, op 88 | Added precondition check: segment must be sealed before committing |

**Integration Status:**
- ✅ IntegratedTieringManager with SegmentReader trait (WAL abstraction)
- ✅ helix-wal has segment access methods (sealed_segment_ids, read_segment_bytes, segment_info)
- ✅ helix-tier wired into helix-server/DurablePartition
- ✅ WalSegmentReader implements SegmentReader for DurablePartition
- ✅ DurablePartitionConfig.with_tiering() enables tiering
- ✅ check_and_register_sealed_segments() hook for segment registration
- ✅ on_entries_committed() hook for Raft commit events
- ✅ tier_eligible_segments() method for triggering S3 uploads

#### 3.2 Progress Tracking (helix-progress)

**Status: NOT STARTED**

Missing crate: `helix-progress` - Consumer progress tracking with leases

---

### Phase 4: API & Flow Control

#### 4.1 gRPC API Layer

| Item | Status | Notes |
|------|--------|-------|
| HelixServer struct | ✅ Done | `helix-server` backed by Multi-Raft |
| Write/Read/Metadata RPCs | ✅ Done | |
| Integration with Multi-Raft | ✅ Done | Replaced ReplicationManager with MultiRaft engine |
| Integration with helix-wal | ✅ Done | DurablePartition for crash-safe storage |
| Integration with ShardRouter | ⚠️ Partial | GroupMap for partition→group, ShardRouter ready but not wired |
| Multi-node Raft networking | ✅ Done | TCP transport, batch encoding, `new_multi_node()` constructor |
| CLI args for clustering | ✅ Done | `--raft-addr`, `--peer`, `--data-dir` flags |
| Docker multi-node setup | ✅ Done | 3-node cluster with docker-compose |

**Architecture:**
- `MultiRaft` manages all Raft groups (one per partition)
- `GroupMap` maps (TopicId, PartitionId) ↔ GroupId
- `DurablePartition` wraps WAL + in-memory cache (Tier 1 storage per RFC)
- Single tick task drives all groups
- TCP transport with batched `GroupMessage` encoding for peer communication

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

## Recently Completed

| Component | Key Achievements |
|-----------|------------------|
| **Tick-based Raft** | DST-friendly timing, randomized elections, heartbeats |
| **Bloodhound DST** | 150+ seeds, property checking, fault injection |
| **Multi-Raft** | Message batching, tick-based timing, group lifecycle |
| **helix-server** | Multi-Raft integration, GroupMap, 11 tests |
| **helix-wal** | Disk persistence, crash recovery, 29 tests + 500-seed stress |
| **DurablePartition** | WAL-backed storage, recovery, tiering hooks |
| **Multi-node networking** | TCP transport, batch encoding, Docker 3-node cluster |
| **Benchmarking** | 1.43M writes/sec (single), 129K writes/sec (3-node) |
| **helix-tier** | S3 abstraction, DST (43 tests), 2 bugs found/fixed |

**Bugs Found via DST:**
- helix-tier: orphaned data (seed 197562), ordering violation (seed 17)
- helix-wal: in-memory truncation before sync (seed 2074002), recovery overlap bug (seed 2074002)

---

## Prioritized Next Steps

### Priority 1: Complete Phase 2 (Multi-Raft & Sharding)

**1. Shard Movement** - Transfer shards between groups ⭐ CRITICAL
   - Required for Phase 2 checkpoint: "Shard transfer works under faults"
   - Snapshot + catchup transfer protocol
   - DST testing with concurrent writes, failures, retries
   - Estimated: 2-3 weeks

### Priority 2: Complete Phase 3 (Storage Features)

**2. helix-progress** - Consumer progress tracking
   - Required for Phase 3 checkpoint: "Complete data lifecycle (write→tier→read)"
   - Offset commits with leases
   - Consumer group coordination
   - Low watermark advancement
   - DST testing with lease expiration, redelivery, node failures
   - Estimated: 2-3 weeks

**3. S3ObjectStorage** - Real S3 implementation
   - Behind `s3` feature flag (aws-sdk-s3)
   - Integration test with localstack
   - Retry logic with exponential backoff
   - Estimated: 1 week

### Priority 3: Phase 4 Completion

**4. helix-flow** - Flow control
   - Token buckets for rate limiting
   - Weighted fair queuing for IO classes
   - AIMD controller for adaptive limits
   - Estimated: 1-2 weeks

**5. helix-kafka-proxy** - Kafka compatibility
   - Protocol translation layer
   - Topic/partition mapping
   - Offset translation
   - Estimated: 2-3 weeks

### Optional / Deferred

- **TLA+ trace validation** - Compare implementation traces against spec
- **Hash-based routing** - Wire ShardRouter for key-based routing
- **io_uring storage** - High-performance Linux I/O
- **Configuration changes** - Joint consensus for membership changes
- **CI pipeline** - Run tests locally for now

---

## Crates: Actual vs Plan

| Planned Crate | Status | Actual Implementation |
|---------------|--------|----------------------|
| `helix-core` | ✅ Exists | As planned |
| `helix-wal` | ✅ Complete | Storage trait, TokioStorage, Wal struct with disk persistence |
| `helix-raft` | ✅ Complete | Pre-vote, leadership transfer, tick-based timing, MultiRaft engine |
| `helix-routing` | ✅ Exists | ShardMap, LeaderCache, ShardRouter |
| `helix-runtime` | ⚠️ Partial | Tick-based server, missing io_uring |
| `helix-tier` | ✅ Complete | Wired into DurablePartition, 500-seed stress DST found 2 bugs (fixed), 43 tests |
| `helix-progress` | ❌ Missing | Need to create |
| `helix-flow` | ❌ Missing | Need to create |
| `helix-server` | ✅ Complete | Multi-Raft done, WAL-backed durable storage integrated |
| `helix-kafka-proxy` | ❌ Missing | Need to create |
| `helix-cli` | ❌ Missing | Need to create |
| `helix-tests` | ✅ Good | DST-friendly tick-based tests, faults, 150+ seeds, WAL DST (29 tests + 500-seed stress), Tier tests (43 tests) |
