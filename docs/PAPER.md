# Helix: A Case Study in AI-Assisted Distributed Consensus with Deterministic Verification

**Authors:** Jai Menon and Claude (Anthropic, Opus 4.5 → Opus 4.6)

**Abstract:** We describe Helix, an experimental Kafka-compatible distributed log built in Rust and co-authored by a human systems engineer and Claude. The project began as a deliberate test: could human-Claude collaboration produce a consensus-based distributed system that survives not just simulation but real traffic? Helix implements the Kafka wire protocol (Produce, Fetch, Metadata, and nine other APIs) atop a Multi-Raft consensus engine with TLA+-verified leader election and log replication. Each partition is a Raft group backed by a crash-safe write-ahead log with CRC32 checksums and group commit. A per-partition actor model eliminates lock contention. Tiered storage streams sealed segments to S3 with transparent read fallback and startup recovery from object storage. Dedicated offset Raft groups handle consumer progress, supporting both Kafka-style cumulative and Pulsar-style individual acknowledgment. Correctness is established through a multi-layer verification pyramid: TLA+ model checking (327M+ states, zero violations), Bloodhound deterministic simulation testing with fault injection (1,000+ seeds), MadSim end-to-end simulation, and a workload verifier that checks produce/consume linearizability against real server processes. The system was deployed to a staging environment carrying ~25 MB/s of mirrored production traffic through the Kafka wire protocol, where it sustained 2.5ms server-side produce latency -- competitive with the production Kafka clusters it aims to replace. Thirteen bugs were caught by DST that would have shipped in a less-tested implementation. This paper documents the verification methodology, the architecture, the staging deployment, and an honest accounting of where human judgment was irreplaceable and where the acceleration was genuine.

---

## 1. Introduction

### 1.1 Genesis

By late 2025, AI code generation had crossed a threshold. Claude could sustain coherent implementations across tens of thousands of lines, maintain consistency across crate boundaries in a Rust workspace, hold architectural context for an entire distributed system, and reason about subtle correctness properties -- Raft leader completeness, WAL crash recovery invariants, CRC checksum boundaries. It could read a TLA+ specification and produce a Rust state machine that preserved the spec's safety properties. It could be asked to "implement the Pre-Vote extension from Section 9.6 of the Raft dissertation" and produce code that correctly prevented disrupted nodes from triggering unnecessary elections -- then wire it into the existing state machine without breaking the four safety invariants.

We wanted to test this on something harder than a key-value store. Distributed consensus is the high-water mark of systems programming: you must reason about network nondeterminism, partial failure, message reordering, split-brain scenarios, and invariants that hold across multiple machines over unbounded time. The interesting bugs -- a leader that commits an entry two followers haven't acknowledged, a WAL that truncates in memory before syncing to disk, a TCP connection that goes half-open when a peer restarts -- don't appear in unit tests. They appear under specific timing conditions that only randomized fault injection or formal model checking can systematically explore.

The question was not "can an AI write a Raft implementation that compiles?" That bar was cleared years ago. The question was: **can a human-AI team produce a consensus system that survives TLA+ model checking, deterministic simulation with fault injection, and deployment under real traffic?** Not code that looks right. Code that is right, as far as we can tell, under adversarial testing -- and then under real network conditions where the adversary is physics.

### 1.2 Why Kafka

Kafka was chosen as the compatibility target for several reasons:

**The protocol is a forcing function.** Kafka's wire protocol has sharp edges -- variable-length headers that differ across 13 API version ranges, RecordBatch compression that must be preserved byte-for-byte through the storage layer, idempotent producer sequence tracking with 5-batch deduplication windows. Implementing the protocol to the point where `kcat` and `librdkafka` work unmodified against the server is a concrete, falsifiable goal. Either the bytes on the wire are correct or the client rejects them.

**Consensus is required, not optional.** Unlike Redis, which can operate as a single-node system with optional replication, Kafka's core abstraction -- a replicated, partitioned, append-only log -- requires distributed consensus by design. Every write must be replicated to a configurable number of followers before being acknowledged. This makes the correctness bar higher: linearizable writes through a single leader, automatic failover when the leader fails, and log compaction via snapshots.

**The performance bar is known.** Production Kafka clusters have well-characterized throughput and latency profiles. A meaningful comparison is possible: same protocol, same clients, same workload patterns.

**The operational complexity is real.** Kafka's dependency on ZooKeeper (or KRaft) for metadata coordination, its per-partition leader election, its consumer group rebalancing -- these are the kinds of cross-cutting concerns that stress-test whether a human-AI team can maintain architectural coherence across a large codebase.

### 1.3 Why This Paper

Helix is not production-ready. Configuration changes (joint consensus) are not implemented. Linearizable reads require a leader round-trip that isn't wired yet. Transactional producers are not supported. We say this upfront because the redis-rust paper that inspired this one was careful to say the same, and that honesty was part of its value.

What we think is worth sharing is the *process*: how the verification pyramid evolved as the system grew more complex, what happened when simulation-tested code hit real infrastructure (spoiler: different bugs), where TigerStyle's assertion discipline caught AI-generated mistakes, and what it looks like to deploy an AI-co-authored consensus system into a staging environment carrying real traffic.

### 1.4 Contributions

This paper makes three contributions:

1. **A verification methodology for AI-assisted consensus systems.** The verification pyramid provides defense-in-depth with layers that have independent failure modes: TLA+ model checking catches protocol-level bugs, Bloodhound DST catches implementation bugs under fault injection, MadSim E2E catches integration bugs across async boundaries, and staging deployment catches infrastructure bugs that no simulation models. Thirteen bugs were caught by DST alone (Section 5.1), and a separate class of bugs emerged only in staging (Section 5.2).

2. **An architecture case study.** The synchronous-state-machine-at-core / async-at-edges design, Multi-Raft with shared WAL and per-partition actors, Kafka wire protocol with zero-copy blob storage, S3-backed WAL tiering with transparent recovery, and dedicated offset groups for consumer progress demonstrate how a human-Claude team navigated real distributed systems design decisions. We document the trade-offs explicitly, including the architectural dead ends.

3. **An honest accounting from simulation to deployment.** The gap between "passes DST" and "runs under real traffic" is the paper's second act. Half-open TCP connections, TLS certificate SAN mismatches, broken load generators, and the discovery that `librdkafka`'s batching adds 600ms of client-side latency that no simulation can model -- these are documented because they are the lessons that matter for practitioners.

---

## 2. Architecture

### 2.1 Design Philosophy: Synchronous Core, Async Edges

The foundational architectural decision -- made before writing any code -- was to implement all core logic as synchronous state machines. Async I/O (network, disk, timers) is handled only at the runtime boundary. This enables deterministic simulation testing by construction: the same inputs to `RaftNode::handle_message()` always produce the same outputs, regardless of wall-clock time, network conditions, or disk latency.

```
                    ┌────────────────────────────────────┐
                    │           Runtime Layer             │
                    │  ┌──────────┐  ┌────────────────┐  │
  Network ──TCP──▶  │  │Transport │  │  Tick Timer    │  │
                    │  │(tokio)   │  │  (100ms)       │  │
                    │  └────┬─────┘  └──────┬─────────┘  │
                    └───────┼───────────────┼────────────┘
                            │               │
                    ┌───────┼───────────────┼────────────┐
                    │       ▼               ▼            │
                    │  ┌────────────────────────────┐    │
                    │  │    Synchronous Core         │    │
                    │  │                             │    │
                    │  │  RaftNode::handle_message() │    │
                    │  │  RaftNode::tick()            │    │
                    │  │      → Vec<RaftOutput>       │    │
                    │  │                             │    │
                    │  │  (deterministic: same input  │    │
                    │  │   → same output, always)     │    │
                    │  └────────────────────────────┘    │
                    │       State Machine Layer          │
                    └───────────────────────────────────┘
```

This pattern -- borrowed from TigerBeetle and FoundationDB -- means the Raft state machine can be tested under Bloodhound's discrete event simulation without a tokio runtime, without real network sockets, and without wall-clock time. The `tick()` API accepts no arguments; it advances the node's internal tick counter by one. Election timeouts and heartbeat intervals are expressed in ticks, not milliseconds. A simulation can drive 10,000 virtual hours of Raft operation in seconds.

### 2.2 Multi-Raft: One Group Per Partition

Helix maps each topic-partition to a dedicated Raft group. A `MultiRaft` engine manages up to 10,000 groups per node, driving all groups from a single `tick()` call with message batching to reduce network round-trips.

```
  Node 1                        Node 2                        Node 3
  ┌─────────────────────┐       ┌─────────────────────┐       ┌─────────────────────┐
  │     MultiRaft       │       │     MultiRaft       │       │     MultiRaft       │
  │  ┌───────┐          │       │  ┌───────┐          │       │  ┌───────┐          │
  │  │Group 0│ Controller│◀────▶│  │Group 0│ Controller│◀────▶│  │Group 0│ Controller│
  │  │(Raft) │ metadata  │      │  │(Raft) │ metadata  │      │  │(Raft) │ metadata  │
  │  ├───────┤          │       │  ├───────┤          │       │  ├───────┤          │
  │  │Group 1│ topic-0/0│◀────▶│  │Group 1│ topic-0/0│◀────▶│  │Group 1│ topic-0/0│
  │  │(Raft) │          │       │  │(Raft) │          │       │  │(Raft) │          │
  │  ├───────┤          │       │  ├───────┤          │       │  ├───────┤          │
  │  │Group 2│ topic-0/1│◀────▶│  │Group 2│ topic-0/1│◀────▶│  │Group 2│ topic-0/1│
  │  │(Raft) │          │       │  │(Raft) │          │       │  │(Raft) │          │
  │  └───────┘          │       │  └───────┘          │       │  └───────┘          │
  └─────────────────────┘       └─────────────────────┘       └─────────────────────┘
         ▲                              ▲                              ▲
         └─────── TCP transport (batch-encoded GroupMessages) ─────────┘
```

Group 0 is the controller partition -- a Raft group replicated across all cluster nodes that coordinates topic creation, partition assignment, and replica placement. When a client creates a topic with N partitions and replication factor R, the controller's leader assigns each partition to R nodes using a round-robin algorithm, commits the assignment through Raft, and each node in the replica set creates its local Raft group.

Message batching is critical at scale: rather than sending one TCP message per Raft RPC per group, the `MultiRaft` engine collects all outbound messages to the same destination node and sends them as a single `GroupMessageBatch`. With 100 partitions and a 100ms heartbeat interval, this reduces per-tick network calls from 100 to 1.

### 2.3 Per-Partition Actors

Each partition runs as an isolated tokio task that exclusively owns its `RaftNode`, `DurablePartition` (WAL-backed storage), and `ProgressManager` (consumer tracking). There are no locks within per-partition data storage.

```
  Kafka Client
       │
       ▼
  ┌──────────────────────────────────────────────────────┐
  │  KafkaHandler (connection-level, stateless)          │
  │  ┌────────────┐  ┌────────────┐  ┌────────────┐     │
  │  │ ApiVersions│  │  Produce   │  │   Fetch    │     │
  │  │  handler   │  │  handler   │  │  handler   │     │
  │  └────────────┘  └─────┬──────┘  └─────┬──────┘     │
  └────────────────────────┼────────────────┼────────────┘
                           │ partition_id   │
                    ┌──────┼────────────────┼──────┐
                    │      ▼                ▼      │
                    │  PartitionRouter              │
                    │  (lock-free dispatch)         │
                    └──────┬────────────────┬──────┘
                           │                │
                    ┌──────▼──────┐  ┌──────▼──────┐
                    │PartitionActor│  │PartitionActor│
                    │ (tokio task) │  │ (tokio task) │
                    │              │  │              │
                    │ RaftNode     │  │ RaftNode     │
                    │ DurablePart  │  │ DurablePart  │
                    │ ProgressMgr  │  │ ProgressMgr  │
                    │ WalActor ────┼──┤ WalActor ────┤─▶ SharedWAL
                    └──────────────┘  └──────────────┘   (amortized fsync)
```

Before actors, 8 partitions at 110 MB/s showed 10-15% throughput degradation from lock contention on the shared `MultiRaft` instance. After actors, each partition scales independently. The `WalActor` batches writes from multiple partition actors into a single shared WAL, amortizing the fsync cost across partitions.

### 2.4 Write-Ahead Log

The WAL provides crash-safe persistence with two operating modes: dedicated (one WAL per partition) and shared (multiple partitions append to the same WAL file, amortizing fsync).

**Entry format (24-byte header, dedicated WAL):**

```
  ┌──────────┬──────────┬──────────┬──────────┬───────────────┐
  │  CRC32   │  Length  │   Term   │  Index   │   Payload     │
  │ (4 bytes)│ (4 bytes)│ (8 bytes)│ (8 bytes)│  (N bytes)    │
  └──────────┴──────────┴──────────┴──────────┴───────────────┘
```

**Shared entry format (40-byte header):**

```
  ┌──────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
  │  CRC32   │  Length  │ GroupId  │   Term   │  Index   │RaftIndex │ Payload  │
  │ (4 bytes)│ (4 bytes)│ (8 bytes)│ (8 bytes)│ (8 bytes)│ (8 bytes)│(N bytes) │
  └──────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘
```

The CRC32 covers everything after the checksum field. Per-entry checksumming means a torn write from a crash corrupts only the last entry; the reader stops at the first corrupt entry and recovers everything before it. This is a deliberate trade-off (ADR-0005: Conservative WAL Corruption Recovery): rather than attempting to recover entries after corruption, we skip the entire segment. Raft handles the gap by re-replicating from peers.

Segments rotate at 4 MB by default. The `SharedWalPool` manages K WAL files (default K=4) across all partitions, with a global index that maps `(GroupId, entry_index)` to the physical WAL entry.

**S3-backed tiering and recovery.** Sealed segments are uploaded to S3 by a coordinator-level tiering process that runs every 5 seconds in a background task (never blocking Raft ticks or causing election timeouts). A segment becomes eligible for upload once all Raft groups that have entries in it have committed past the segment's last entry. After upload, the local copy can be deleted -- reads that hit a deleted segment fall back transparently to S3. On startup, `download_missing_segments()` fetches any segments present in S3 but missing locally, enabling a fresh node to recover its full WAL state from object storage before replaying Raft entries. Each node uses a pod-specific S3 key prefix (`node-{id}/shared/{pool_index}/`) to isolate its namespace.

### 2.5 Kafka Wire Protocol: Zero-Copy Blob Storage

A key design decision was to store Kafka `RecordBatch` bytes as opaque blobs rather than deserializing them into internal record types. The produce path reads raw bytes from the network, wraps them in a `StoredBlob` with offset metadata, proposes the blob through Raft, and stores it in the WAL. The fetch path reads the blob back and returns the original bytes. No serialization overhead on the hot path.

```
  Produce:  Client bytes ──▶ StoredBlob ──▶ Raft propose ──▶ WAL append
  Fetch:    WAL read ──▶ StoredBlob ──▶ Client bytes
                    (same bytes, zero deserialization)
```

A `BlobIndex` maps Kafka offsets to WAL entry indices using a sorted array of 20-byte entries -- 800x smaller than caching full blobs. Binary search gives O(log n) lookup by offset.

**Idempotent producers.** The produce path supports Kafka-compatible idempotent producers with a 5-batch deduplication window per partition per producer. Sequence numbers are checked *before* Raft proposal -- duplicates return the cached offset immediately without consuming log space, and out-of-order or fenced requests are rejected with proper Kafka error codes. Producer state is rebuilt from the Raft log after leadership changes: the new leader replays committed entries to reconstruct each producer's dedup window before accepting new proposals.

### 2.6 Controller Partition

Group 0 coordinates cluster metadata through Raft consensus:

```rust
enum ControllerCommand {
    CreateTopic { name, partition_count, replication_factor },
    DeleteTopic { name },
    AssignPartition { topic_id, partition_id, group_id, replicas },
    UpdatePartitionLeader { topic_id, partition_id, leader },
    BrokerHeartbeat { node_id, timestamp_ms },
}
```

This is architecturally similar to Kafka's KRaft (KIP-500): metadata is replicated through the same consensus protocol as data, eliminating the ZooKeeper dependency. Broker heartbeats track liveness; nodes without recent heartbeats are considered dead for replica assignment.

### 2.7 Offset Groups

Consumer offset commits in Kafka are written to `__consumer_offsets`, a dedicated internal topic with 50 partitions. Routing offset commits through the controller partition (Group 0) would serialize all offset writes behind a single Raft leader -- a bottleneck for any meaningful number of consumer groups.

Helix uses N=3 dedicated Raft groups for offset storage, replicated across all cluster nodes. `xxh3(consumer_group_id) % 3` routes each consumer group's commits to a deterministic offset group, distributing the write load. Each offset group maintains an in-memory `HashMap<(TopicId, PartitionId, ConsumerGroupId), (Offset, CommittedMetadata)>` that is replayed from its Raft log on startup and periodically snapshotted. Snapshots use a compact binary format with CRC32 integrity checks, uploaded to S3 alongside the WAL segments.

This mirrors Kafka's separation of metadata (controller) from offset storage (`__consumer_offsets`) -- the same architectural insight, adapted to the Multi-Raft model where adding a new Raft group is cheaper than adding a new partition.

### 2.8 Raft Snapshot Installation

When a follower lags too far behind the leader's log (because the leader has compacted entries the follower needs), the leader sends an `InstallSnapshot` containing the full partition state. The follower replaces its local state with the snapshot and resumes normal log replication from the snapshot's last included index.

This is critical for operational robustness: a node that was down for maintenance or suffered extended disk issues can rejoin the cluster without requiring a full data copy. The snapshot mechanism is shared across data partitions, the controller, and offset groups -- each group type serializes its state machine differently but uses the same Raft-level snapshot protocol.

### 2.9 Explicit Trade-offs

| Aspect | Kafka | Helix | Rationale |
|--------|-------|-------|-----------|
| Consensus | ISR-based (ZK/KRaft) | Raft per partition | Formally verified, simpler failure model |
| Thread model | Partition-level threads + I/O threads | Actor-per-partition (tokio) | Lock-free scaling, DST-compatible |
| WAL | Per-partition log segments | Shared WAL pool (K=4) | Amortized fsync across partitions |
| Record storage | Deserialized, indexed | Opaque blob + offset index | Zero-copy, 800x smaller index |
| Consumer progress | Offset commit to __consumer_offsets | Offset groups (N=3 Raft groups) + ProgressManager | Supports individual ack (Pulsar-style) |
| Metadata | ZK/KRaft controller | Controller partition (Group 0) | Same consensus, no external dependency |
| Configuration changes | Online partition reassignment | Snapshot-based shard transfer | Simpler, but requires data copy |
| Blocking ops | Consumer group rebalancing | Not implemented | Requires cross-partition coordination |
| Exactly-once | Transactional producer | Idempotent producers (no transactions) | Full 5-batch dedup, epoch fencing; transaction coordinator not implemented |
| Linearizable reads | Via ISR acks=all | Not implemented (leader reads only) | Requires read index or lease |

---

## 3. Verification Methodology

### 3.1 The Core Principle

AI-generated code must be verified by methods with objective, mechanical acceptance criteria.

This principle is the same one articulated in the redis-rust paper, and it bears repeating because the trap is the same: the model that misunderstands a protocol invariant can write a TLA+ spec that encodes the same misunderstanding. The escape is not *who* writes the verification but *what kind* of verification it is.

Each layer in our pyramid was chosen because it has **mechanical pass/fail criteria independent of the author's intent**:

- **TLA+ / TLC**: Exhaustively enumerates every reachable state. If the spec's safety invariants are correct (and they are small enough to inspect by hand), a passing TLC run provides a mathematical guarantee.
- **Bloodhound DST**: Drives the *real* Rust state machine (not a model) through randomized fault scenarios with seeded determinism. A failing seed reproduces the exact same execution. The invariant checker runs after every simulated event.
- **MadSim E2E**: Runs the *full async service* (KafkaHandler, PartitionActors, Transport) under a deterministic async runtime. Catches bugs at the async boundary that DST misses because DST only tests the synchronous core.
- **Workload verification**: Spawns *real server processes*, runs produce/consume workloads, and checks that consumed records match produced records. The server binary is the same one that runs in staging.
- **Staging deployment**: Real traffic, real network, real TLS, real DNS. No simulation models any of this.

The layers have independent failure modes. A wrong TLA+ spec will pass TLC but fail to catch bugs in the implementation -- which Bloodhound DST would then catch. A correct DST that only tests the sync core will miss async boundary bugs -- which MadSim would catch. MadSim can't model real network behavior -- which staging deployment reveals. Each layer catches bugs the others miss.

```
  ┌─────────────────────────────────────────────────────┐
  │              Verification Pyramid                   │
  │                                                     │
  │                 ╱╲    TLA+ / TLC                    │  Protocol bugs
  │                ╱  ╲   (327M+ states)                │
  │               ╱────╲                                │
  │              ╱      ╲  Bloodhound DST               │  State machine bugs
  │             ╱        ╲ (1,000+ seeds, faults)       │
  │            ╱──────────╲                             │
  │           ╱            ╲ MadSim E2E                 │  Async boundary bugs
  │          ╱              ╲(500+ seeds, full svc)     │
  │         ╱────────────────╲                          │
  │        ╱   Workload E2E   ╲ WAL DST                 │  Integration +
  │       ╱    (real binary)   ╲(500 seeds, crash)      │  durability bugs
  │      ╱──────────────────────╲                       │
  │     ╱    Staging Deployment   ╲                     │  Infrastructure bugs
  │    ╱     (25 MB/s real traffic)╲                    │
  │   ╱────────────────────────────╲                    │
  │  ╱         Unit Tests            ╲                  │  Logic bugs
  │ ╱          (200+ tests)           ╲                 │
  │╱──────────────────────────────────╲                 │
  └─────────────────────────────────────────────────────┘
    More tests, faster              Fewer, slower, deeper
```

### 3.2 Layer 1: TLA+ Model Checking

The Raft consensus implementation has a TLA+ specification (`specs/raft.tla`) that models a 3-node cluster with two extensions to standard Raft:

**Pre-Vote** (Section 9.6 of the Raft dissertation): Before incrementing its term, a candidate gathers "pre-votes" from a majority. If the pre-vote fails, the node stays a follower without disrupting the cluster. This prevents a partitioned node from incrementing its term repeatedly, then forcing unnecessary elections when it rejoins.

**Leadership Transfer** (Section 3.10): A leader can hand off to a specific follower by sending a `TimeoutNow` message, but only if the follower's log is caught up (`matchIndex >= log length`). The follower bypasses pre-vote and immediately starts an election.

Four safety properties are verified:

| Property | Invariant |
|----------|-----------|
| `SingleLeaderPerTerm` | At most one leader elected per term |
| `LogMatching` | Same index + term → identical log prefixes |
| `LeaderCompleteness` | Committed entries present in all future leaders |
| `StateMachineSafety` | No two different values committed at same index |

TLC explored 327M+ states with zero violations. The model configuration (`MaxTerm=3`, `MaxLogLength=4`) is small enough to complete in hours but large enough to exercise all state transitions including pre-vote and leadership transfer.

### 3.3 Layer 2: Bloodhound Deterministic Simulation

Bloodhound is a discrete event simulation framework that drives the synchronous Raft state machine through randomized scenarios with seeded determinism. Given seed 42, the ten-thousandth event is always the same, across platforms, across runs.

**Architecture:**

```
  ┌──────────┐     ┌──────────────────┐     ┌──────────────────────┐
  │ Seed: 42 │────▶│ DiscreteSimEngine │────▶│ Generate events:     │
  └──────────┘     │ (virtual time)    │     │ - Raft messages      │
                   └──────────────────┘     │ - Client proposals   │
                                            │ - Tick advances      │
                                            │ - Crash / Recover    │
                                            │ - Network partitions │
                                            └──────────┬───────────┘
                                                       │
                           ┌───────────────────────────┼──────────────────┐
                           ▼                           ▼                  ▼
                    ┌──────────────┐           ┌──────────────┐   ┌──────────────┐
                    │  RaftNode 1  │           │  RaftNode 2  │   │  RaftNode 3  │
                    │  (real impl) │           │  (real impl) │   │  (real impl) │
                    └──────┬───────┘           └──────┬───────┘   └──────┬───────┘
                           │                          │                  │
                           └──────────────────────────┼──────────────────┘
                                                      ▼
                                            ┌────────────────────┐
                                            │  PropertyChecker   │
                                            │  (after EVERY step)│
                                            │                    │
                                            │  SingleLeaderPerTerm│
                                            │  LogMatching       │
                                            │  LeaderCompleteness│
                                            │  StateMachineSafety│
                                            └────────────────────┘
```

The `PropertyChecker` runs after every simulated event, not just at the end. It tracks `leaders_by_term` (detecting split-brain), `committed_entries` (detecting log divergence), and pairwise log comparison (detecting replication corruption). A violation at any step halts the simulation and prints the seed for deterministic reproduction.

**Fault injection scenarios:**

| Scenario | Configuration | Seeds |
|----------|--------------|-------|
| Basic election | 3-node, no faults | 10 |
| Single crash/recovery | Crash at 2s, recover at 5s | 100 |
| Leader crash | Crash leader at 3s, recover at 7s | 100 |
| Sequential crashes | Crash node 0 (2-4s), then node 1 (6-8s) | 100 |
| Network partition | Isolate minority | 100 |
| Split vote | Multiple simultaneous candidates | 100 |
| Partition heal | Partition, then rejoin | 100 |
| 5-node with failures | 5 nodes, tolerate 2 crashes | 100 |
| Extended stress | 60s virtual time, heavy faults | 50 |
| Mega-seed sweep | All scenarios, random selection | 1,000 |

### 3.4 Layer 3: WAL Deterministic Simulation

A separate DST layer targets the write-ahead log specifically, because WAL correctness has different failure modes from consensus correctness. The `SimulatedStorage` wraps the WAL's `Storage` trait with fault injection:

- **Write failure**: Append returns I/O error (simulates disk full)
- **Partial write**: Only some bytes written before failure (simulates torn write)
- **Fsync failure**: Data in OS cache but not on disk (simulates power loss)
- **Corruption**: Random byte flip in segment data (simulates bit rot)
- **Crash**: Truncate all files to last-synced position, then recover

The key invariant: after crash and recovery, every entry that was acknowledged (fsync returned success) must be present and intact.

500 seeds, 100 operations per seed, 25% fault rate. Two bugs found (detailed in Section 5.1).

### 3.5 Layer 4: MadSim End-to-End Simulation

MadSim operates at a higher level than Bloodhound: it runs the *full async service* -- `HelixService` instances with real `KafkaHandler`, `PartitionActor`, and `Transport` components -- under a deterministic async runtime that controls time, network delivery, and fault injection.

This catches bugs at the async boundary that Bloodhound DST misses because Bloodhound only drives the synchronous state machine core. For example: a race condition where two partition actors both try to read from the shared WAL during recovery, or a timeout that fires between the Raft commit and the WAL sync.

Test configurations:

| Scenario | Partitions | Seeds | Fault patterns |
|----------|-----------|-------|---------------|
| Random faults | 1 | 100-500 | Crash, partition, delay |
| Multi-partition | 3 | 100-200 | Per-partition faults |
| Concurrent faults | 1-3 | 100-200 | Faults during in-flight Raft messages |
| Extended stress | 1-3 | 1,000 | All patterns combined |

Verification: produced records are consumed back, payload hashes match, at least 50% of produced records are recoverable after fault scenarios.

### 3.6 Layer 5: Workload Verification

The `helix-workload` crate spawns real `helix-server` processes (the same binary deployed to staging), runs produce/consume workloads against them, and verifies:

1. Every produced record is consumable (no data loss)
2. Consumed records match produced records byte-for-byte (no corruption)
3. Offsets are monotonically increasing (no reordering)
4. Multiple partitions receive data (routing correctness)

This layer catches bugs that no simulation models: actual tokio scheduling behavior, real TCP socket handling, real filesystem I/O, and process lifecycle issues (the test harness kills server processes with SIGKILL to simulate crashes).

### 3.7 Layer 6: Staging Deployment

The final verification layer is deployment to a staging environment carrying real production traffic. This is described in Section 4.

### 3.8 DST Skepticism

A lesson learned early and documented in the project's coding guidelines: **be deeply skeptical of DST tests that pass on the first attempt.** DST is only valuable if it actually exercises the code under test with high fidelity.

Red flags that a DST test is not testing anything:
- Tests complete in milliseconds (real DST takes seconds to minutes)
- Zero events processed
- No faults actually injected (fault counters at zero)
- State never changes during simulation

Before declaring DST tests complete, we deliberately break invariants and verify the tests catch the breakage. This "mutation testing by hand" caught two cases where the property checker was tracking leaders but not actually comparing them across terms.

---

## 4. Staging Deployment

### 4.1 The Gap Between Simulation and Reality

Helix was deployed to a staging Kubernetes environment on the Datadog streaming platform. The deployment used the "mirror pattern": a skeleton service consumed from an existing production Kafka stream (~25 MB/s), re-produced the traffic to Helix through `librdkafka`, and a second skeleton service consumed from Helix and verified delivery.

```
  Production Kafka                     Helix Cluster (3 pods)
  ┌───────────────┐                    ┌──────────────────────┐
  │ source stream  │                   │  helix stream        │
  │  (~25 MB/s)    │                   │                      │
  └───────┬────────┘                   │  ┌────┐ ┌────┐ ┌────┐│
          │                            │  │Pod1│ │Pod2│ │Pod3││
          ▼                            │  │Raft│ │Raft│ │Raft││
  ┌───────────────────┐    produce     │  └────┘ └────┘ └────┘│
  │ Mirror Producer   │──────────────▶ │                      │
  │ (skeleton pod 1)  │  librdkafka    └──────────┬───────────┘
  │ consumer group:   │                           │
  │ helix-e2e-mirror  │                           │ consume
  └───────────────────┘                           ▼
                                       ┌───────────────────┐
                                       │ Helix Consumer    │
                                       │ (skeleton pod 2)  │
                                       │ consumer group:   │
                                       │ helix-e2e-consumer│
                                       └───────────────────┘
```

This architecture was not the first attempt. The initial plan used `PRODUCER_LOADGEN=true` on the skeleton service, which generates synthetic traffic. That flag turned out to be broken across the staging environment -- even non-Helix skeletons had the same issue. The hardcoded `producer.write("normal", ...)` in the load generator ignored the configured lane type. Rather than debugging the load generator, we pivoted to the mirror pattern, which matches the proven production deployment on other clusters.

The deployment evolved through several iterations. The initial deployment used 2 topics. The current deployment uses a 64-partition stream (`helix-staging-s2-64`) with S3-backed WAL tiering active -- sealed segments are uploaded to S3, local copies are deleted after confirmation, and fetch requests for evicted segments fall back transparently to S3. The cluster runs in steady state with a mirror producer and a verification consumer.

### 4.2 Infrastructure Bugs

Several categories of bugs emerged in staging that no simulation could have caught:

**TLS certificate SAN mismatch.** The staging environment uses mutual TLS. The certificate SAN prefix defaults to the Kubernetes ServiceAccount name, not the K8s Service name. Helix's ServiceAccount was `helix-admin`, but the platform's admin service expected to connect to `helix-admin-headless` -- the headless Service name. The fix was a one-line annotation override.

**Pod IP addressing.** Production Kafka brokers advertise their pod IP, not a service-level DNS name, because Kafka clients need to connect to specific brokers (not load-balanced across the cluster). Helix initially advertised a DNS name that round-robins across pods -- breaking per-broker addressing. The fix: advertise `${POD_IP}:9092` using the Kubernetes downward API.

**Half-open TCP connections.** When a peer pod restarts, the leader's TCP connection becomes half-open. Raft heartbeats are silently sent into a dead socket. The leader never detects the failure; controller state never replicates to the restarted pod. The fix required a multi-layer approach combining OS-level keepalive, kernel-level send timeout, and application-level write timeout.

**StatefulSet rolling update deadlock.** When a pod is in crash-loop-backoff and the StatefulSet's `maxUnavailable=1`, the rollout is deadlocked -- the failing pod counts as unavailable, preventing the controller from updating higher-ordinal pods. No amount of waiting resolves this; manual intervention is required. This is a Kubernetes operational subtlety that doesn't exist in simulation.

None of these are consensus bugs. They are infrastructure integration bugs. The verification pyramid catches protocol-level and implementation-level correctness; staging catches whether the correct implementation can actually run in the environment it's deployed to.

### 4.3 Latency Results

Server-side produce latency (from tracing spans on the leader node):

| Component | Latency |
|-----------|---------|
| Total per-produce | ~2.5 ms |
| Raft replication (`wait_commit`) | 2.0-2.6 ms (I/O idle) |
| CPU work (`time.busy`) | 70-90 us |
| Storage apply (`apply_entry`) | 8-38 us |

Client-side latency (`libstreaming.stream_producer.writes.latency`): 600-800 ms.

The gap between 2.5 ms server-side and 600-800 ms client-side is entirely client-side: `librdkafka` batching (`linger_ms=100ms`), queue wait, and Fabric DNS round-trip. For comparison, production Kafka clusters on the same platform show 100-800 ms client-side latency from the same `libstreaming` metric. The server-side 2.5 ms is competitive with production Kafka's 3-4 ms average from workload analysis.

Mirror producer throughput: ~38-44K messages per 10-second interval published to Helix.
Consumer throughput: ~83-110K messages pulled per interval.

---

## 5. Lessons Learned

### 5.1 What DST Caught

Thirteen bugs were caught by deterministic simulation testing across the WAL, tiering, and server layers:

**WAL bugs (Bloodhound DST):**

1. **In-memory truncation before on-disk sync** (seed 2074002). When a sealed segment was truncated, the in-memory state was modified before the disk sync. If the sync failed, the segment wouldn't be retried on the next pass. Fix: clone the segment, truncate the clone, sync, then swap.

2. **Recovery not rewriting overlapping segments** (seed 2074002). When recovery detected overlapping segments (from a crash during truncation), the in-memory state was truncated but the on-disk files were left stale. Fix: rewrite overlapping segments to disk during recovery.

**Tiering bugs (DST):**

3. **Orphaned data from exists() failure** (seed 197562, op 27). During recovery, an `exists()` call to object storage used `unwrap_or(false)` on failure, causing incorrect abort of tiering. Fix: skip the segment on `exists()` error, retry on next recovery pass.

4. **Ordering violation: committed before sealed** (seed 17, op 88). `mark_committed()` allowed uncommitted segments to be marked committed without being sealed first. Fix: added precondition check.

**Eviction bugs (integration tests):**

5. **Offset range never populated.** Sealed segments had no offset range metadata, making progress-aware eviction impossible. Fix: call `set_offset_range()` during segment registration.

6. **Segments without offset info were evictable.** `can_evict_with_progress()` returned `true` when `end_offset` was `None`. Fix: return `false` when offset info is missing.

7. **Off-by-one in eviction boundary.** `<=` instead of `<` in the boundary comparison. One-character fix.

**Server bugs (DST + integration):**

8. **Commit notification race in actor-based partition.** The actor processed a commit notification before the WAL sync completed, leading to a read of an entry that wasn't yet durable.

9. **Non-actor mode storage duplicates during failover** (commit 278ad0f). During leader failover, the old leader's in-flight proposals could be re-applied by the new leader, creating duplicate entries.

10. **AppendEntries message size limit** (commit f268f62). Large log entries exceeded the 16 MB message size limit, causing replication to stall silently. Fix: chunk AppendEntries batches to fit within the limit.

11. **Raft log trailing window too large** (commit 6d909e6). The trailing window kept more entries in memory than necessary, causing memory pressure. Fix: reduce window to match actual entry sizes.

12. **BlobIndex not trimmed after shared WAL segment deletion** (commit f169f3b). After the shared WAL deleted old segments, the in-memory BlobIndex still referenced the deleted entries, causing stale reads. Fix: trim BlobIndex in sync with WAL segment deletion.

13. **SharedWAL term monotonicity assertion** (commit 803ae5b). The shared WAL asserted that `term >= last_term` for every appended entry. But Raft's `PREVIOUS_TERM` commits -- entries proposed by an old leader that were never committed, then committed by a new leader -- legitimately carry a lower term than the preceding entry. The assertion was wrong. This is a subtle Raft invariant that the TLA+ spec models correctly (committed entries can have any term) but that the implementation got wrong by assuming monotonicity.

Of these thirteen, bugs 1-2 (WAL) and 8-10 (server) would likely have caused data loss or silent corruption in production. Bug 13 (SharedWAL assertion) would have caused crashes under leader changes with uncommitted entries -- a common scenario during rolling deploys. Bugs 3-4 (tiering) would have caused operational issues. Bugs 5-7 (eviction) were off-by-one errors that would have caused premature data deletion. Bugs 11-12 were resource leaks that would have caused degradation over time.

### 5.2 What Staging Caught That DST Didn't

The infrastructure bugs from Section 4.2 (TLS SAN mismatch, pod IP addressing, half-open TCP, StatefulSet deadlock) represent a category of bugs that no simulation can model: they depend on the specific behavior of Kubernetes, TLS infrastructure, DNS, and the Linux TCP stack. DST verified that the Raft protocol is correct. Staging verified that the correct protocol can actually communicate over the network it's deployed on.

A fourth staging-only bug: the `PRODUCER_LOADGEN` dead end. The skeleton service's load generator hardcoded `producer.write("normal", ...)`, ignoring the configured lane type. This wasn't a Helix bug -- it was a bug in the test infrastructure that affected all services on the staging platform. But it cost a full day of debugging before we pivoted to the mirror pattern. The lesson: when debugging staging, first check how working services do it, then debug your service.

### 5.3 What Worked

**Claude excels at pattern replication with constraints.** Once the first Kafka API handler was implemented (ApiVersions), Claude could replicate the pattern across twelve more APIs with high accuracy. The mechanical aspects -- request parsing, response encoding, version range checks -- are exactly the kind of repetitive work where Claude performs well. TigerStyle's constraints (70-line functions, 2 assertions per function, explicit bounds) acted as guardrails that channeled Claude's output into a consistent shape.

**The synchronous core / async edge split paid for itself many times over.** Every DST bug was reproducible with a seed because the core state machine is deterministic. Without this split, we would have needed a deterministic async runtime (which MadSim provides, but at higher cost and lower simulation throughput).

**TigerStyle assertions caught Claude mistakes in real time.** The 2-assertions-per-function discipline means Claude-generated code includes precondition and postcondition checks that fire during testing. Several bugs (the eviction off-by-one, the ordering violation) were caught by assertions that Claude itself wrote, at the explicit request of the TigerStyle guidelines. This is a productive tension: Claude's default mode is to produce code that works; TigerStyle forces it to produce code that *proves* it works at runtime.

### 5.4 What Didn't Work

**Claude doesn't naturally write TigerStyle.** Precondition assertions, postcondition checks, explicit bounds on every loop, `expect()` instead of `unwrap()` with descriptive messages -- all of this had to be explicitly requested, repeatedly. Without the CLAUDE.md guidelines encoding TigerStyle requirements, Claude would produce clean, idiomatic Rust that compiles and passes tests but lacks the runtime verification that catches bugs early.

**Large structural changes required human architectural reasoning.** Moving from a shared `Arc<RwLock<MultiRaft>>` to per-partition actors was a cross-cutting change that Claude could not plan or execute autonomously. It required understanding the interaction between connection lifecycle, Raft group ownership, WAL batching, and transport routing. The human designed the actor architecture; Claude implemented it file by file.

**The TLA+ spec and the implementation can drift.** The TLA+ spec models 3 nodes with `MaxTerm=3` and `MaxLogLength=4`. The implementation supports 7 nodes, unbounded terms, and millions of log entries. Changes to the Raft state machine (e.g., adding a quorum loss detection timeout) were not always reflected back to the TLA+ spec. This is the spec maintenance problem: the spec is only useful if it tracks the implementation, and maintaining that correspondence is manual work.

**DST skepticism is essential but expensive.** Verifying that DST tests are actually testing something -- by deliberately breaking invariants, checking event counters, and adding instrumentation -- roughly doubled the time spent on each DST layer. But without it, we would have had false confidence from tests that passed because they weren't exercising the code.

---

## 6. Scale of the Collaboration

The project comprises approximately 98,000 lines of Rust across 11 crates, plus a TLA+ specification, Docker configurations, deployment tooling, and documentation. The crate breakdown:

| Crate | Lines | Purpose |
|-------|-------|---------|
| helix-core | 1,422 | Strongly-typed IDs, limits, record types |
| helix-wal | 10,318 | Write-ahead log, CRC checksums, shared WAL |
| helix-raft | 7,228 | Raft consensus, pre-vote, leadership transfer |
| helix-routing | 1,744 | Shard routing, leader caching |
| helix-runtime | 3,513 | TCP transport, message codec |
| helix-server | 14,932 | Kafka API, gRPC admin, partition actors |
| helix-progress | 3,535 | Consumer progress, lease management |
| helix-flow | 2,890 | Token bucket, fair queue, AIMD |
| helix-tier | 3,615 | S3 tiering, segment lifecycle |
| helix-tests | 17,500+ | DST (Bloodhound + MadSim), integration |
| helix-workload | 4,637 | E2E load testing, history verification |

The human's primary contributions were architectural decisions (ADRs), the DST skepticism methodology, debugging staging infrastructure, and the overall system design. Claude's primary contributions were implementation across all crates, TLA+ specification, DST test harnesses, and Kafka wire protocol handling. The boundary was not clean -- many implementations required iterative human feedback on correctness, and many architectural decisions were informed by Claude's analysis of trade-offs.

---

## 7. Conclusion and Future Work

This project is a case study in human-Claude systems programming applied to distributed consensus -- a domain where subtle bugs cause silent data loss and where the interesting failures only appear under specific timing conditions.

The verification methodology is the main contribution. The multi-layer pyramid -- TLA+ model checking, Bloodhound DST, MadSim E2E, workload verification, and staging deployment -- forms a pipeline where each layer catches bugs the others miss. TLA+ catches protocol-level errors. DST catches implementation bugs under fault injection. MadSim catches async boundary bugs. Staging catches infrastructure integration bugs. The thirteen bugs caught by DST (Section 5.1) and the infrastructure bugs caught by staging (Section 4.2) are different in kind, not just degree.

The staging deployment is the result that matters most. It is one thing to pass 327 million TLC states. It is another to sustain 2.5ms server-side produce latency under 25 MB/s of real traffic mirrored from production Kafka. The former proves the protocol is correct. The latter proves the implementation can run.

Future work includes implementing joint consensus for online configuration changes, adding linearizable reads via read index, transactional producer support, and expanding to multi-node production deployment. On the verification side, TLA+ trace validation -- comparing implementation execution traces against the spec -- would close the gap between the formal model and the running system.

The open question from the redis-rust paper remains: as model capabilities improve and the generated code grows more complex, does the verification pyramid remain sufficient? We don't have a definitive answer, but we have a data point: for a 98,000-line consensus system deployed under real traffic, the pyramid held. The bugs it caught were real. The bugs it missed were infrastructure, not logic. Whether that ratio holds at 500,000 lines or 5 million is an empirical question we expect to revisit.

---

**Source code:** [repository URL]

**Verification harness:** See `helix-tests/` for DST harnesses, `specs/raft.tla` for the TLA+ specification, and `helix-workload/` for end-to-end verification.
