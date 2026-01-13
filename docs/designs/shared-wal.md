# Shared WAL Design

## Problem Statement

Currently each partition has its own WAL, resulting in:
- **1 fsync per partition per flush** - On a node with 100 partitions, that's 100 fsyncs per flush interval
- **Limited batching** - Each WAL can only batch writes from its single partition
- **NVMe underutilization** - Modern NVMe can handle 100K+ IOPS, but sequential fsyncs serialize operations

## Goal

Allow multiple partitions to share a WAL, amortizing fsync cost across partitions:
- **N partitions sharing 1 WAL** = 1 fsync instead of N fsyncs
- **Cross-partition batching** = Larger write batches, better throughput
- **Configurable sharing** = 1 shared WAL per node, or K shared WALs (e.g., 1 per NUMA node)

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Entry format | New `SharedEntry` type via `WalEntry` trait | Reuse all WAL logic, only entry format differs |
| Index space | Partition-local indices | Entries store partition's Raft index; SharedWal maintains per-partition position index for O(1) lookup |
| Index assignment | Caller-specified + panic on violation | Matches Raft semantics; TigerStyle assertion catches caller bugs |
| Truncation | Append-only + aging | No truncation; old segments expire via retention policy (Kafka semantics) |
| Composition | SharedWal wraps `Wal<S, SharedEntry>` | Own batching logic for cross-partition semantics |
| Code reuse | Generic WAL via traits | `Wal<S, E: WalEntry>` works with any entry type |

### Design Note: Index Assignment and Write Failure Handling

**Decision**: Caller provides partition-local indices; SharedWal panics if indices are not sequential.

**Context**: When `append(partition, term, index, payload)` is called, SharedWal asserts that
`index == last_index + 1` for that partition. If the assertion fails, the process panics.

**Alternatives Considered**:

| Option | Behavior | Trade-off |
|--------|----------|-----------|
| Caller-specified + panic (chosen) | Panic if index != expected | Catches bugs immediately; matches HashiCorp raft-wal |
| Caller-specified + error | Return error if index != expected | Caller might ignore error |
| Auto-assign indices | WAL assigns next index | Simpler API but awkward for Raft (leader already knows index) |

**Why panic is safe**:

The panic occurs when the caller violates the sequential index contract. This is a **caller bug**,
not a WAL bug. At the system level:

1. **If leader panics**: Followers timeout → new election → new leader elected → writes resume.
   Old leader restarts, recovers from WAL, rejoins as follower.

2. **If follower panics**: Leader continues with remaining quorum (no interruption if majority
   intact). Follower restarts, recovers, catches up from leader.

Raft is designed for node failures. A panic is just another form of node crash - the system
self-heals via Raft consensus. Crashing on a bug is **safer** than silently corrupting the
log with index gaps.

**Write failure handling**:

If `append()` returns an error (e.g., disk I/O failure), the entry was NOT written. The caller
must retry with the **same index**, not skip to the next. This matches Raft semantics: if the
leader can't persist an entry, it cannot commit it, so it retries or steps down.

```
append(p1, term=1, index=1) → Ok      // WAL: [1]
append(p1, term=1, index=2) → Err     // I/O error, WAL still: [1]
append(p1, term=1, index=3) → PANIC   // Bug: skipped index 2
append(p1, term=1, index=2) → Ok      // Correct: retry same index
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         helix-wal                                    │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                     WalEntry trait                           │    │
│  │  fn encode(&self, buf: &mut BytesMut)                        │    │
│  │  fn decode(buf: &mut impl Buf, offset: u64) -> Result<Self>  │    │
│  │  fn total_size(&self) -> u64                                 │    │
│  │  fn index(&self) -> u64                                      │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                    ▲                         ▲                       │
│                    │                         │                       │
│         ┌─────────┴─────────┐     ┌─────────┴─────────┐             │
│         │      Entry        │     │   SharedEntry      │             │
│         │ (single partition)│     │ (multi-partition)  │             │
│         │                   │     │                    │             │
│         │ [CRC|Len|Term|Idx]│     │[CRC|Len|PID|Term|Idx]           │
│         │     24 bytes      │     │     32 bytes       │             │
│         └─────────┬─────────┘     └─────────┬──────────┘             │
│                   │                         │                        │
│                   ▼                         ▼                        │
│         ┌─────────────────┐       ┌─────────────────┐               │
│         │  Wal<S, Entry>  │       │Wal<S, SharedEntry>              │
│         │   (default)     │       │                  │               │
│         └────────┬────────┘       └────────┬─────────┘               │
│                  │                         │                         │
│                  │              ┌──────────┴──────────┐              │
│                  │              │     SharedWal       │              │
│                  │              │ (coordination layer)│              │
│                  │              │ - per-partition handles           │
│                  │              │ - cross-partition batching        │
│                  │              │ - durability notifications        │
│                  │              └──────────┬──────────┘              │
│                  │                         │                         │
│                  └────────────┬────────────┘                         │
│                               │                                      │
│                               ▼                                      │
│                  ┌─────────────────────────┐                         │
│                  │     Storage trait       │                         │
│                  │ (TokioStorage, IoUring) │                         │
│                  └─────────────────────────┘                         │
└─────────────────────────────────────────────────────────────────────┘
```

## Entry Formats

### Existing Entry (unchanged)

```
+──────────────+──────────────+──────────────+──────────────+──────────────+
│    CRC32     │    Length    │     Term     │    Index     │   Payload    │
│  (4 bytes)   │  (4 bytes)   │  (8 bytes)   │  (8 bytes)   │  (N bytes)   │
+──────────────+──────────────+──────────────+──────────────+──────────────+
Header: 24 bytes
```

### New SharedEntry

```
+──────────────+──────────────+──────────────+──────────────+──────────────+──────────────+
│    CRC32     │    Length    │ Partition ID │     Term     │    Index     │   Payload    │
│  (4 bytes)   │  (4 bytes)   │  (8 bytes)   │  (8 bytes)   │  (8 bytes)   │  (N bytes)   │
+──────────────+──────────────+──────────────+──────────────+──────────────+──────────────+
Header: 32 bytes
```

- CRC covers: Length + PartitionId + Term + Index + Payload
- PartitionId: Identifies which partition owns this entry
- Term/Index: Partition's Raft term and **partition-local** log index

## Access Patterns

Since `SharedEntry` uses partition-local indices, the WAL cannot use index-based lookup directly.
Instead, SharedWal relies on sequential access via `wal.entries()`.

**Performance Comparison:**

| Operation | Per-partition WAL | SharedWal |
|-----------|-------------------|-----------|
| Append | O(1) | O(1) |
| Recovery | O(n) scan | O(n) scan, group by partition |
| Slow follower catch-up | O(1) from memory | O(1) from memory (normal) |
| | | O(n) scan (if evicted from memory) |

**Future optimization (if needed)**: Per-partition position index for O(1) random access:

```rust
/// Per-partition position index (NOT currently implemented).
struct PartitionIndex {
    first_index: u64,
    positions: Vec<WalPosition>,  // ~16 bytes per entry overhead
}
```

### Design Note: No Per-Partition Index Required

**Key insight**: The WAL is for **durability only**, not for serving reads during normal operation.

**Recovery process** (sequential scan, no random access needed):
```
1. Open SharedWal
2. Sequential scan: wal.entries()
3. For each entry: partition_map[entry.partition_id()].push(entry)
4. Each partition rebuilds its in-memory Raft log
5. Done - WAL now only used for appends
```

**After recovery**: All reads come from in-memory Raft log. Leader sends entries to
followers from memory, not WAL.

**When random WAL access would matter**: Only if a follower is so far behind that
entries have been evicted from the leader's in-memory Raft log. This is rare.

**Cost of sequential scan for slow follower catch-up**:
- Scan from known segment/offset, discard entries until reaching target partition+index
- For 100 partitions, ~99% of scanned entries are discarded (not for this partition)
- Cost = time to scan and discard up to the recovery point
- Sequential I/O is fast; checking partition_id + index is trivial

**Decision**: No in-memory index. Use `wal.entries()` iterator for recovery and rare
slow follower catch-up. Zero memory overhead. If random access becomes a bottleneck
in production, we can add lazy index building or on-disk index files.

## Core Types

### WalEntry Trait

```rust
/// Trait for WAL entry serialization/deserialization.
///
/// Allows the WAL to be generic over entry format while reusing
/// all segment management, rotation, and recovery logic.
pub trait WalEntry: Sized + Clone + Send + Sync {
    /// Size of the fixed header (before variable-length payload).
    const HEADER_SIZE: usize;

    /// Encodes the entry to bytes.
    fn encode(&self, buf: &mut BytesMut);

    /// Decodes an entry from bytes.
    fn decode(buf: &mut impl Buf, offset: u64) -> WalResult<Self>;

    /// Returns the total size in bytes (header + payload).
    fn total_size(&self) -> u64;

    /// Returns the log index of this entry.
    fn index(&self) -> u64;

    /// Returns the payload length in bytes.
    fn payload_len(&self) -> u32;

    /// Whether this entry type uses WAL-global sequential indices.
    ///
    /// Returns true for Entry (single-partition), false for SharedEntry.
    /// When false, the WAL skips sequential index assertions and relies
    /// on the coordination layer for per-partition sequentiality.
    fn uses_global_index() -> bool { true }
}
```

### SharedEntry

```rust
/// Header for shared WAL entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedEntryHeader {
    pub crc: u32,
    pub length: u32,
    pub partition_id: u64,
    pub term: u64,
    pub index: u64,
}

/// Entry in a shared WAL, tagged with partition ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedEntry {
    pub header: SharedEntryHeader,
    pub payload: Bytes,
}

impl SharedEntry {
    /// Creates a new shared entry.
    pub fn new(
        partition_id: PartitionId,
        term: u64,
        index: u64,
        payload: Bytes,
    ) -> WalResult<Self>;

    /// Returns the partition ID.
    pub fn partition_id(&self) -> PartitionId;
}

impl WalEntry for SharedEntry {
    const HEADER_SIZE: usize = 32;
    // ... implementations
}
```

### SharedWal Coordination Layer

```rust
/// Configuration for a shared WAL.
#[derive(Debug, Clone)]
pub struct SharedWalConfig {
    /// Base WAL configuration (directory, segment size, etc.).
    pub wal_config: WalConfig,
    /// Maximum time to wait before flushing buffered entries.
    pub flush_interval: Duration,
    /// Maximum entries to buffer before forcing a flush.
    pub max_buffer_entries: usize,
    /// Maximum bytes to buffer before forcing a flush.
    pub max_buffer_bytes: usize,
}

/// A partition's handle to a shared WAL.
///
/// Clone + Send + Sync - can be used from multiple tasks.
#[derive(Clone)]
pub struct SharedWalHandle {
    partition_id: PartitionId,
    inner: Arc<SharedWalInner>,
}

/// Acknowledgment that an entry is durable.
#[derive(Debug, Clone, Copy)]
pub struct DurableAck {
    /// The partition's local index that was made durable.
    pub index: u64,
    /// The global WAL offset where the entry was written.
    pub wal_offset: u64,
}

/// The shared WAL coordinator.
pub struct SharedWal<S: Storage> {
    inner: Arc<SharedWalInner<S>>,
}

struct SharedWalInner<S: Storage> {
    /// Underlying WAL with SharedEntry format.
    wal: Mutex<Wal<S, SharedEntry>>,
    /// Buffered entries waiting to be flushed.
    buffer: Mutex<WriteBuffer>,
    /// Notification for flush task.
    flush_notify: Notify,
    /// Configuration.
    config: SharedWalConfig,
}

struct WriteBuffer {
    entries: VecDeque<PendingWrite>,
    bytes: usize,
}

struct PendingWrite {
    entry: SharedEntry,
    /// Notified when entry is durable.
    durable_tx: oneshot::Sender<WalResult<DurableAck>>,
}
```

## API

```rust
impl<S: Storage + Clone + Send + Sync + 'static> SharedWal<S> {
    /// Opens or creates a shared WAL.
    pub async fn open(storage: S, config: SharedWalConfig) -> WalResult<Self>;

    /// Gets a handle for a partition.
    ///
    /// Each partition should call this once and clone the handle as needed.
    pub fn handle(&self, partition_id: PartitionId) -> SharedWalHandle;

    /// Recovers entries from the WAL, grouped by partition.
    ///
    /// Called during node startup to restore partition state.
    pub async fn recover(&self) -> WalResult<HashMap<PartitionId, Vec<SharedEntry>>>;

    /// Forces a flush of all buffered entries.
    pub async fn flush(&self) -> WalResult<()>;

    /// Closes the shared WAL, flushing remaining entries.
    pub async fn close(&self) -> WalResult<()>;
}

impl SharedWalHandle {
    /// Appends an entry for this partition.
    ///
    /// Returns when the entry is durable (fsync'd to disk).
    pub async fn append(&self, term: u64, index: u64, payload: Bytes) -> WalResult<DurableAck>;

    /// Appends an entry without waiting for durability.
    ///
    /// Use `wait_durable()` to wait for durability if needed.
    pub fn append_async(
        &self,
        term: u64,
        index: u64,
        payload: Bytes,
    ) -> WalResult<oneshot::Receiver<WalResult<DurableAck>>>;

    /// Returns the partition ID for this handle.
    pub fn partition_id(&self) -> PartitionId;
}
```

## Batching and Flush

```rust
async fn flush_loop<S: Storage>(inner: Arc<SharedWalInner<S>>) {
    loop {
        // Wait for flush interval or buffer threshold.
        tokio::select! {
            _ = tokio::time::sleep(inner.config.flush_interval) => {}
            _ = inner.flush_notify.notified() => {}
        }

        // Drain buffer atomically.
        let pending: Vec<PendingWrite> = {
            let mut buffer = inner.buffer.lock().await;
            std::mem::take(&mut buffer.entries).into()
        };

        if pending.is_empty() {
            continue;
        }

        // Write all entries to WAL (no fsync yet).
        let mut wal = inner.wal.lock().await;
        let mut results: Vec<(oneshot::Sender<_>, WalResult<DurableAck>)> = Vec::new();

        for pw in pending {
            match wal.append(pw.entry.clone()).await {
                Ok(offset) => {
                    results.push((pw.durable_tx, Ok(DurableAck {
                        index: pw.entry.index(),
                        wal_offset: offset,
                    })));
                }
                Err(e) => {
                    results.push((pw.durable_tx, Err(e)));
                }
            }
        }

        // Single fsync for entire batch.
        if let Err(e) = wal.sync().await {
            // Notify all waiters of failure.
            for (tx, _) in results {
                let _ = tx.send(Err(e.clone()));
            }
            continue;
        }

        // Notify all waiters of success.
        for (tx, result) in results {
            let _ = tx.send(result);
        }
    }
}
```

## Recovery

```rust
impl<S: Storage> SharedWal<S> {
    pub async fn recover(&self) -> WalResult<HashMap<PartitionId, Vec<SharedEntry>>> {
        let wal = self.inner.wal.lock().await;
        let mut by_partition: HashMap<PartitionId, Vec<SharedEntry>> = HashMap::new();

        // Read all entries from WAL.
        let first = wal.first_index();
        let last = match wal.last_index() {
            Some(idx) => idx,
            None => return Ok(by_partition), // Empty WAL.
        };

        for idx in first..=last {
            if let Ok(entry) = wal.read(idx) {
                by_partition
                    .entry(entry.partition_id())
                    .or_default()
                    .push(entry.clone());
            }
        }

        Ok(by_partition)
    }
}
```

## Configuration Strategies

| Strategy | WAL Count | fsyncs/flush | Use Case |
|----------|-----------|--------------|----------|
| Single shared | 1 | 1 | Small nodes, few partitions |
| Per-NUMA | 2-4 | 2-4 | Large multi-socket servers |
| Configurable K | K | K | Tuning to workload |

```rust
/// Pool of shared WALs for partitioning across multiple files.
pub struct SharedWalPool<S: Storage> {
    wals: Vec<SharedWal<S>>,
}

impl<S: Storage> SharedWalPool<S> {
    /// Gets handle for a partition (routes to appropriate WAL by hash).
    pub fn handle(&self, partition_id: PartitionId) -> SharedWalHandle {
        let idx = partition_id.as_u64() as usize % self.wals.len();
        self.wals[idx].handle(partition_id)
    }
}
```

## Throughput Analysis

Assumptions:
- NVMe fsync latency: 50μs
- Flush interval: 1ms
- 100 partitions per node
- 1000 writes/partition/second = 100K writes/second total

| Configuration | fsyncs/second | Effective IOPS |
|---------------|---------------|----------------|
| Individual WALs (current) | 100,000 | ~20K (fsync-bound) |
| 1 shared WAL | 1,000 | ~100K+ |
| 4 shared WALs | 4,000 | ~400K+ (parallel) |

## Implementation Plan

### Phase 1: WalEntry Trait
- Define `WalEntry` trait
- Make `Entry` implement `WalEntry`
- Make `Wal<S>` generic: `Wal<S, E: WalEntry = Entry>`
- **Gate**: All existing WAL tests pass unchanged (zero behavior change)

### Phase 2: SharedEntry
- Implement `SharedEntryHeader` and `SharedEntry`
- Implement `WalEntry for SharedEntry`
- Unit tests for encode/decode roundtrip
- **Gate**: `Wal<S, SharedEntry>` can write/read entries

### Phase 3: SharedWal Coordinator
- `SharedWalConfig`, `SharedWalHandle`, `SharedWal`
- Batching with flush task
- Per-partition durability notifications
- Recovery support
- **Gate**: Unit tests for all coordinator logic

### Phase 4: DST Infrastructure
- `SharedWalActor` for Bloodhound simulation
- `SharedWalPropertyState` tracking
- Property checkers: durability, ordering, no-phantoms, isolation
- `SharedWalFaultConfig` with crash scenarios
- **Gate**: DST framework compiles and runs basic test

### Phase 5: DST Verification
- Basic durability tests (single partition, crash/recovery)
- Multi-partition tests (10+ partitions, interleaved writes)
- Torn write recovery tests
- Extensive seed testing (100+ seeds)
- **Gate**: 100 seeds with aggressive faults, zero violations

### Phase 5.5: Feature Parity for Full DST (NEW)
To achieve comprehensive stress testing matching the per-partition WAL DST (1000 seeds × 100 ops, 25% faults),
SharedWal needs additional features:

**Missing Features:**
1. `truncate_after(partition_id, index)` - Raft log truncation support
2. `read(partition_id, index)` - Direct read by partition and index (not just iterator)
3. Per-partition durable index tracking - Know what's durable per partition

**Current State (as of Phase 5.5):**
| Feature | Status | Notes |
|---------|--------|-------|
| `append()` | ✅ Done | With TigerStyle assertions |
| `sync()` | ✅ Done | Updates per-partition durable indices |
| `recover()` | ✅ Done | Groups entries by partition, last-write-wins for duplicates |
| `durable_index()` | ✅ Done | WAL-global index |
| `partition_durable_index(pid)` | ✅ Done | Per-partition durable index |
| `read(pid, idx)` | ✅ Done | Direct read, last-write-wins for duplicates |
| `entries_for_partition()` | ✅ Done | Returns entries, last-write-wins for duplicates |
| `truncate_after(pid, idx)` | ✅ Done | Logical truncation via valid range + last-write-wins |

**DST Test Coverage (Current):**
| Test | Seeds | Fault Types | Status |
|------|-------|-------------|--------|
| `test_dst_multi_seed_sync_durability` | 100 | None | ✅ |
| `test_dst_multi_seed_fsync_failures` | 100 | fsync 30% | ✅ |
| `test_dst_multi_seed_torn_writes` | 100 | torn 20% | ✅ |
| Unit tests for truncation | N/A | N/A | ✅ (4 tests) |
| `test_dst_comprehensive_stress` | 1000 | fsync 10% + torn 10% | ✅ |
| `test_dst_stress_with_truncation` | 100 | None | ✅ |
| `test_dst_multi_crash_recovery_cycles` | 100 | None | ✅ |
| Coordinator unit tests | N/A | N/A | ✅ (8 tests) |

**Phase 3 Coordination Layer: ✅ Complete**
- `SharedWalCoordinator` - Production-ready concurrent access
- `SharedWalHandle` - Clone + Send + Sync per-partition handles
- `DurableAck` - Durability acknowledgment
- `WriteBuffer` + `flush_loop` - Background batching and fsync coalescing
- `CoordinatorConfig` - Configurable flush interval, buffer limits

**Gate**: Phase 3 complete ✅, Phase 5.5 DST complete ✅

**Next Steps:**
1. Phase 6: SharedWalPool + benchmarks
2. Phase 7: 10K hours stress testing target

### Phase 6: SharedWalPool + Integration
- `SharedWalPool` for multiple shared WALs
- Integration with io_uring worker pool
- Benchmarks comparing configurations
- **Gate**: Benchmarks show expected fsync reduction

### Phase 7: Stress Testing
- 10,000 simulated hours target
- Integration with Raft groups
- Add regression seeds as bugs are found
- **Gate**: 10K hours with zero property violations

## Deterministic Simulation Testing (DST)

SharedWal requires rigorous DST with failure injection and property verification,
following the same patterns used for Raft consensus testing.

### Properties to Verify

#### 1. Durability Guarantee
Once `append()` returns `DurableAck`, the entry MUST survive crashes.

```rust
/// Property: Acknowledged entries survive crashes.
///
/// If append() returns DurableAck { index: i }, then after any crash
/// and recovery, recover() MUST return an entry with that index for
/// that partition.
pub fn check_durability_guarantee(state: &SharedWalPropertyState) -> Vec<DurabilityViolation> {
    let mut violations = Vec::new();

    for ack in &state.acknowledged_entries {
        let partition_entries = state.recovered_entries.get(&ack.partition_id);
        let found = partition_entries
            .map(|entries| entries.iter().any(|e| e.index() == ack.index))
            .unwrap_or(false);

        if !found {
            violations.push(DurabilityViolation {
                partition_id: ack.partition_id,
                index: ack.index,
                ack_time: ack.time,
            });
        }
    }

    violations
}
```

#### 2. Per-Partition Ordering
Entries for each partition are recovered in index order.

```rust
/// Property: Per-partition entries are ordered by index.
///
/// For each partition, recovered entries must have strictly increasing indices.
pub fn check_partition_ordering(state: &SharedWalPropertyState) -> Vec<OrderingViolation> {
    let mut violations = Vec::new();

    for (partition_id, entries) in &state.recovered_entries {
        let mut prev_index = 0;
        for entry in entries {
            if entry.index() <= prev_index {
                violations.push(OrderingViolation {
                    partition_id: *partition_id,
                    prev_index,
                    current_index: entry.index(),
                });
            }
            prev_index = entry.index();
        }
    }

    violations
}
```

#### 3. No Phantom Entries
Only entries that were appended can be recovered.

```rust
/// Property: No phantom entries appear after recovery.
///
/// Every recovered entry must have been appended (buffered or acknowledged).
pub fn check_no_phantom_entries(state: &SharedWalPropertyState) -> Vec<PhantomViolation> {
    let mut violations = Vec::new();

    for (partition_id, entries) in &state.recovered_entries {
        for entry in entries {
            let was_appended = state.appended_entries
                .get(partition_id)
                .map(|set| set.contains(&entry.index()))
                .unwrap_or(false);

            if !was_appended {
                violations.push(PhantomViolation {
                    partition_id: *partition_id,
                    index: entry.index(),
                });
            }
        }
    }

    violations
}
```

#### 4. Partition Isolation
One partition's crash/failure doesn't lose another partition's acknowledged entries.

```rust
/// Property: Partition isolation.
///
/// If partition A's append fails mid-batch, partition B's acknowledged
/// entries in the same batch are still durable.
pub fn check_partition_isolation(state: &SharedWalPropertyState) -> Vec<IsolationViolation>;
```

#### 5. Batch Atomicity (Per-Entry)
Each entry in a batch is independently durable once acknowledged.

```rust
/// Property: Per-entry atomicity within batches.
///
/// If a crash occurs mid-batch, entries with DurableAck are durable,
/// entries without DurableAck may or may not be durable.
pub fn check_batch_atomicity(state: &SharedWalPropertyState) -> Vec<AtomicityViolation>;
```

### Failure Injection

#### Storage Faults (via SimulatedStorage)

Reuse existing `FaultConfig` from `helix-wal/src/storage/simulated.rs`:

```rust
pub struct SharedWalFaultConfig {
    /// Base storage fault config.
    pub storage: FaultConfig,
    /// Crash during flush (after some entries written, before fsync).
    pub crash_during_flush_rate: f64,
    /// Crash after fsync but before notifications sent.
    pub crash_after_sync_rate: f64,
}

impl SharedWalFaultConfig {
    pub fn none() -> Self {
        Self {
            storage: FaultConfig::none(),
            crash_during_flush_rate: 0.0,
            crash_after_sync_rate: 0.0,
        }
    }

    pub fn aggressive() -> Self {
        Self {
            storage: FaultConfig {
                torn_write_rate: 0.02,
                fsync_fail_rate: 0.01,
                read_corruption_rate: 0.001,
                ..Default::default()
            },
            crash_during_flush_rate: 0.05,
            crash_after_sync_rate: 0.02,
        }
    }
}
```

#### Crash Scenarios

| Scenario | Description | Property at Risk |
|----------|-------------|------------------|
| Crash before flush | Entries buffered but not written | Unacknowledged entries lost (OK) |
| Crash during write | Partial batch written to disk | Torn write recovery |
| Crash after write, before fsync | All entries written, not synced | Durability guarantee |
| Crash after fsync, before notify | Entries durable, clients not notified | Durability (entries exist) |
| Crash during recovery | Crash while reading WAL | Recovery idempotence |

#### Test Structure

```rust
/// DST test configuration for SharedWal.
pub struct SharedWalDstConfig {
    pub seed: u64,
    pub partition_count: usize,
    pub entries_per_partition: usize,
    pub max_time_secs: u64,
    pub fault_config: SharedWalFaultConfig,
    pub crash_times_ms: Vec<u64>,
    pub recover_times_ms: Vec<u64>,
}

/// Run a single DST test with property verification.
pub fn run_shared_wal_dst(config: &SharedWalDstConfig) -> DstResult {
    let mut engine = DiscreteSimulationEngine::with_config(
        EngineConfig::new(config.seed)
            .with_max_time(Duration::from_secs(config.max_time_secs))
    );

    // Create simulated storage with fault injection.
    let storage = SimulatedStorage::with_faults(config.fault_config.storage.clone());

    // Create SharedWal actor.
    let wal_actor = SharedWalActor::new(storage, config.partition_count);
    let actor_id = engine.register(wal_actor);

    // Schedule writes from each partition.
    for partition in 0..config.partition_count {
        for i in 0..config.entries_per_partition {
            engine.schedule_after(
                Duration::from_millis(i as u64 * 10),
                EventKind::Custom {
                    name: "AppendEntry",
                    data: encode_append(partition, i),
                },
            );
        }
    }

    // Schedule crashes and recoveries.
    for (crash_ms, recover_ms) in config.crash_times_ms.iter().zip(&config.recover_times_ms) {
        engine.schedule_after(Duration::from_millis(*crash_ms), EventKind::ProcessCrash { actor: actor_id });
        engine.schedule_after(Duration::from_millis(*recover_ms), EventKind::ProcessRecover { actor: actor_id });
    }

    // Run simulation.
    let result = engine.run();

    // Verify properties.
    let state = wal_actor.property_state();
    let violations = verify_all_properties(&state);

    DstResult {
        success: result.success && violations.is_empty(),
        events_processed: result.stats.events_processed,
        violations,
    }
}
```

### Test Categories

#### Unit-Level DST (helix-wal/src/shared_wal.rs tests)

```rust
#[test]
fn test_shared_wal_dst_basic_durability() {
    // Single partition, no faults, verify acknowledged = recovered.
    let config = SharedWalDstConfig {
        seed: 42,
        partition_count: 1,
        entries_per_partition: 100,
        max_time_secs: 5,
        fault_config: SharedWalFaultConfig::none(),
        crash_times_ms: vec![2000],
        recover_times_ms: vec![3000],
    };
    let result = run_shared_wal_dst(&config);
    assert!(result.success, "violations: {:?}", result.violations);
}

#[test]
fn test_shared_wal_dst_multi_partition_crash() {
    // Multiple partitions, crash during flush.
    let config = SharedWalDstConfig {
        seed: 42,
        partition_count: 10,
        entries_per_partition: 50,
        max_time_secs: 10,
        fault_config: SharedWalFaultConfig::aggressive(),
        crash_times_ms: vec![1000, 3000, 5000],
        recover_times_ms: vec![2000, 4000, 6000],
    };
    let result = run_shared_wal_dst(&config);
    assert!(result.success);
}

#[test]
fn test_shared_wal_dst_torn_write_recovery() {
    // Force torn write at specific offset, verify recovery.
    let mut config = SharedWalDstConfig::default();
    config.fault_config.storage.force_torn_write_at = Some(1024);
    let result = run_shared_wal_dst(&config);
    assert!(result.success);
}
```

#### Integration DST (helix-tests/src/shared_wal_tests.rs)

```rust
#[test]
fn test_shared_wal_dst_with_raft() {
    // Multiple Raft groups sharing a WAL, verify both Raft and WAL properties.
}

#[test]
fn test_shared_wal_dst_extensive_seeds() {
    // Run 100 seeds with aggressive fault injection.
    for seed in 0..100 {
        let config = SharedWalDstConfig {
            seed,
            fault_config: SharedWalFaultConfig::aggressive(),
            ..Default::default()
        };
        let result = run_shared_wal_dst(&config);
        assert!(result.success, "seed {} failed: {:?}", seed, result.violations);
    }
}

#[test]
#[ignore] // Long-running stress test
fn test_shared_wal_dst_10k_hours() {
    // Target: 10,000 simulated hours with zero property violations.
    let mut total_violations = 0;
    for seed in 0..10_000 {
        let config = SharedWalDstConfig {
            seed,
            max_time_secs: 3600, // 1 hour simulated
            fault_config: SharedWalFaultConfig::aggressive(),
            ..Default::default()
        };
        let result = run_shared_wal_dst(&config);
        total_violations += result.violations.len();
    }
    assert_eq!(total_violations, 0, "10K hours with {} violations", total_violations);
}
```

### Property State Tracking

```rust
/// State tracked during simulation for property verification.
pub struct SharedWalPropertyState {
    /// Entries that were appended (partition_id -> set of indices).
    pub appended_entries: HashMap<PartitionId, HashSet<u64>>,

    /// Entries that received DurableAck.
    pub acknowledged_entries: Vec<AcknowledgedEntry>,

    /// Entries recovered after crash.
    pub recovered_entries: HashMap<PartitionId, Vec<SharedEntry>>,

    /// Crash events (time, entries_in_buffer, entries_in_flight).
    pub crash_events: Vec<CrashEvent>,

    /// Total simulation events processed.
    pub events_processed: u64,
}

#[derive(Debug)]
pub struct AcknowledgedEntry {
    pub partition_id: PartitionId,
    pub index: u64,
    pub wal_offset: u64,
    pub time: Duration,
}

#[derive(Debug)]
pub struct CrashEvent {
    pub time: Duration,
    pub buffered_count: usize,
    pub in_flight_count: usize,
}
```

### Regression Seeds

```rust
/// Seeds that previously found bugs or edge cases.
pub const SHARED_WAL_REGRESSION_SEEDS: &[u64] = &[
    42,           // Basic
    0xDEAD_BEEF,  // Boundary conditions
    0xCAFE_BABE,  // Multi-partition interleaving
    // Add seeds as bugs are found
];
```

### DST Skepticism Checklist

Before marking DST tests complete, verify:

- [ ] **Fidelity**: Entries are actually being written and recovered
- [ ] **Fault injection**: Crashes cause observable state changes
- [ ] **Property verification**: Intentionally broken invariants trigger violations
- [ ] **Coverage**: All code paths exercised (buffering, flushing, recovery)
- [ ] **Event count**: Tests process meaningful number of events (not zero)
- [ ] **Duration**: Tests run for sufficient simulated time

```rust
#[test]
fn test_shared_wal_dst_property_catches_bug() {
    // Intentionally break durability to verify property catches it.
    let config = SharedWalDstConfig {
        seed: 42,
        // Use a buggy storage that "forgets" acknowledged entries
        fault_config: SharedWalFaultConfig::with_durability_bug(),
        ..Default::default()
    };
    let result = run_shared_wal_dst(&config);
    assert!(!result.success, "should have caught durability violation");
    assert!(!result.violations.is_empty());
}
```

## Truncation Safety Analysis

### Design Decision: Logical Truncation Only

`truncate_after(partition_id, after_index)` is an **in-memory operation only**. It does NOT
persist a truncation marker to disk. This is an intentional design decision.

### Behavior After Crash

Consider this scenario:
```
1. Write entries 1-10 at term 1 → sync
2. truncate_after(5)            → in-memory only
3. Write entries 6-8 at term 2  → sync
4. CRASH
5. Recovery: WAL contains [1-10@term1, 6-8@term2]
6. Last-write-wins: 1-5@term1, 6-8@term2, 9-10@term1 (stale!)
```

Entries 9-10 from term 1 **reappear** after recovery because they were never physically
deleted or overwritten.

### Why This Is Safe (Raft Protocol Guarantees)

The WAL relies on the Raft protocol for correctness, not on physical truncation:

1. **Stale entries cannot help win elections**:
   - After recovery: node's `last_term = 1` (from stale entry 10)
   - Other nodes: `last_term = 2` (from entry 8)
   - Raft election: candidate must have log "at least as up-to-date"
   - Node with `last_term=1` cannot beat node with `last_term=2`
   - **Result: Node with stale entries cannot become leader**

2. **Stale entries will be overwritten**:
   - When leader sends entries 9+ at term ≥2
   - Follower detects conflict: `log[9].term=1 ≠ leader.term=2`
   - Follower truncates after 8, appends new entries
   - **Result: Stale entries replaced with correct ones**

3. **Stale entries were never committed**:
   - If entries 9-10 were committed at term 1, a quorum had them
   - For term 2 leader to exist, it won election
   - To win, it needed log at least as up-to-date as quorum
   - If quorum had entries 9-10, leader would have them too
   - Leader wouldn't truncate follower at 5 if it had entries 9-10
   - **Result: Uncommitted entries can safely reappear and be overwritten**

### What The WAL Guarantees

| Property | Description |
|----------|-------------|
| Durability | Acknowledged (synced) entries survive crashes |
| Last-Write-Wins | Same `(partition, index)` written twice → last one wins |
| No Phantoms | Only appended entries appear in recovery |
| Valid Prefix | Recovery returns sequential entries starting from 1 |

### What The WAL Does NOT Guarantee

| Non-Guarantee | Explanation |
|---------------|-------------|
| Truncation persistence | The truncation point is NOT persisted |
| Physical deletion | Old entries remain on disk until segment expires |
| Term consistency suffix | Stale entries may have older terms than preceding entries |

### Stale Entry Lifecycle

Stale entries go through three phases:

| Phase | Timeframe | Mechanism |
|-------|-----------|-----------|
| Logical filtering | Immediate | Last-write-wins during reads ignores older duplicates |
| Raft overwrite | Seconds-minutes | Leader sends correct entries, follower overwrites stale ones |
| Physical deletion | Hours-days | Segment retention policy deletes old segments |

This is efficient because:
- **No per-entry deletion**: Rewriting segments or maintaining tombstones is expensive
- **No truncation fsync**: Would serialize operations and hurt throughput
- **Segment-level cleanup is O(1)**: Just delete the file when retention expires
- **Same as Kafka**: Proven approach at massive scale

### Alternative: Persisted Truncation (Not Implemented)

We considered persisting truncation markers but rejected it because:

1. **Complexity**: Requires writing special marker entries or per-partition metadata files
2. **Performance**: Additional fsync for truncation operations
3. **Unnecessary**: Raft protocol already guarantees safety
4. **Redundant**: Stale entries age out anyway via retention policy

The current design keeps the WAL simple and fast, delegating correctness to Raft.

### DST Verification

The test `test_dst_partial_overwrite_stale_entries_reappear` explicitly verifies that stale
entries CAN reappear after crash recovery, documenting this as expected behavior. The test
also verifies the properties that ensure Raft can handle this safely.

## Open Questions

1. **Segment cleanup**: When can a segment be deleted?
   - Answer: When retention period expires (time-based or size-based)
   - All partitions implicitly advance as time passes

2. **Per-partition reads**: How to efficiently read only one partition's entries?
   - Option A: Scan and filter (simple, O(n))
   - Option B: Secondary index (partition_id → offsets)
   - For recovery, scan is acceptable; for hot reads, use in-memory state

3. **Backpressure**: What happens when buffer is full?
   - Block `append()` until space available
   - Or return error and let caller retry
