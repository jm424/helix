# Actor Model Architecture for Multi-Partition Scalability

**Status**: Proposal
**Created**: 2026-01-19
**Author**: Claude + Jai

## Problem Statement

### Current Bottlenecks

Multi-partition throughput is ~10-15% lower than single-partition despite doing the same total work:

| Config | Throughput | Settings |
|--------|------------|----------|
| Single partition | 125 MB/s | 6 prod, 2000 inflight |
| 8 partitions | 110 MB/s | 4 prod, 1000 inflight |

Root causes:

1. **Global MultiRaft lock** - All partition operations serialize through `Arc<RwLock<MultiRaft>>`
2. **Per-partition batching** - Traffic splits N ways, smaller batches, more Raft rounds

### Current Architecture

```
                    ┌─────────────────────────────────────┐
                    │         Arc<RwLock<MultiRaft>>      │
                    │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐   │
Batcher ──────────► │  │Raft0│ │Raft1│ │Raft2│ │Raft3│   │
Tick Task ────────► │  └─────┘ └─────┘ └─────┘ └─────┘   │
Transport ────────► │         (one writer at a time)      │
                    └─────────────────────────────────────┘
                                      │
                                      ▼
                    ┌─────────────────────────────────────┐
                    │           SharedWalPool             │
                    │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐   │
                    │  │WAL 0│ │WAL 1│ │WAL 2│ │WAL 3│   │
                    │  └─────┘ └─────┘ └─────┘ └─────┘   │
                    └─────────────────────────────────────┘
```

**Problems:**
- Single writer lock = all partitions serialize
- Batcher holds lock during Raft propose
- Tick task holds lock during state machine advance
- No parallelism across partitions

## Proposed Architecture

### Actor Model Overview

Replace shared mutable state with message-passing actors:

```
                         ┌──────────────┐
        Propose ────────►│PartitionActor│──────► WalActor 0
        Tick ───────────►│     (0)      │            │
        Messages ───────►└──────────────┘            ▼
                                                   Disk
                         ┌──────────────┐
        Propose ────────►│PartitionActor│──────► WalActor 0
        Tick ───────────►│     (1)      │            │
        Messages ───────►└──────────────┘            │
                                                     │
                         ┌──────────────┐            │
        Propose ────────►│PartitionActor│──────► WalActor 1
        Tick ───────────►│     (2)      │            │
        Messages ───────►└──────────────┘            ▼
                                                   Disk
                         ┌──────────────┐
        Propose ────────►│PartitionActor│──────► WalActor 1
        Tick ───────────►│     (3)      │
        Messages ───────►└──────────────┘
```

**Benefits:**
- Each partition actor runs independently (no lock contention)
- Full parallelism across N partitions
- WAL actors batch writes from multiple partitions
- Clean separation of concerns

### Component Design

#### 1. PartitionActor

Owns the Raft state machine for one partition. Runs in its own tokio task.

```rust
pub struct PartitionActor {
    group_id: GroupId,
    raft: RaftState,

    // Inbound commands
    cmd_rx: mpsc::Receiver<PartitionCommand>,

    // Outbound
    wal_tx: mpsc::Sender<WalCommand>,
    transport_tx: mpsc::Sender<TransportCommand>,

    // Pending client responses
    pending_proposals: HashMap<LogIndex, oneshot::Sender<Result<Offset>>>,

    // Storage for reads
    partition_storage: PartitionStorage,
}

pub enum PartitionCommand {
    /// Client propose request
    Propose {
        entry: Entry,
        reply: oneshot::Sender<Result<Offset>>,
    },
    /// Raft tick (leader election, heartbeats)
    Tick,
    /// Inbound Raft message from peer
    RaftMessage {
        from: NodeId,
        message: RaftMessage,
    },
    /// WAL write completed
    WalWriteComplete {
        log_index: LogIndex,
        result: Result<Offset>,
    },
    /// Shutdown
    Shutdown,
}
```

#### 2. WalActor

Owns one shared WAL. Batches writes from multiple partition actors.

```rust
pub struct WalActor {
    wal_id: u32,
    wal: SharedWal,

    // Inbound write requests
    cmd_rx: mpsc::Receiver<WalCommand>,

    // Batch configuration
    max_batch_size: usize,
    linger_ms: u64,
}

pub enum WalCommand {
    /// Append entry to WAL
    Append {
        group_id: GroupId,
        entry: Entry,
        reply: oneshot::Sender<Result<Offset>>,
    },
    /// Flush pending writes
    Flush,
    /// Shutdown
    Shutdown,
}
```

#### 3. PartitionRouter

Routes requests to the correct partition actor. Replaces `Arc<RwLock<MultiRaft>>`.

```rust
pub struct PartitionRouter {
    /// Partition actors by group_id
    partitions: HashMap<GroupId, mpsc::Sender<PartitionCommand>>,

    /// WAL actors
    wal_actors: Vec<mpsc::Sender<WalCommand>>,
}

impl PartitionRouter {
    pub fn partition(&self, group_id: GroupId) -> &mpsc::Sender<PartitionCommand> {
        &self.partitions[&group_id]
    }

    pub fn wal_for(&self, group_id: GroupId) -> &mpsc::Sender<WalCommand> {
        let wal_id = group_id.get() as usize % self.wal_actors.len();
        &self.wal_actors[wal_id]
    }

    /// Broadcast tick to all partitions (parallel)
    pub async fn tick_all(&self) {
        let futures: Vec<_> = self.partitions.values()
            .map(|tx| tx.send(PartitionCommand::Tick))
            .collect();
        join_all(futures).await;
    }
}
```

### Message Flow

#### Write Path (Propose)

```
Client                 Batcher              PartitionActor         WalActor
  │                      │                       │                    │
  │──Produce Request────►│                       │                    │
  │                      │                       │                    │
  │                      │──PartitionCommand────►│                    │
  │                      │   ::Propose           │                    │
  │                      │                       │                    │
  │                      │                       │──RaftMessage──────►│ (to followers)
  │                      │                       │   (via transport)  │
  │                      │                       │                    │
  │                      │                       │◄─RaftMessage───────│ (acks from followers)
  │                      │                       │                    │
  │                      │                       │   [majority acked] │
  │                      │                       │                    │
  │                      │                       │──WalCommand───────►│
  │                      │                       │   ::Append         │
  │                      │                       │                    │──write to disk
  │                      │                       │                    │
  │                      │                       │◄─Result<Offset>────│
  │                      │                       │                    │
  │                      │◄──Result<Offset>──────│                    │
  │                      │                       │                    │
  │◄─Produce Response────│                       │                    │
```

#### Tick Path

```
TickTask              PartitionRouter           PartitionActors
   │                       │                    ┌────┐ ┌────┐ ┌────┐
   │──tick_all()──────────►│                    │ P0 │ │ P1 │ │ P2 │
   │                       │                    └────┘ └────┘ └────┘
   │                       │──Tick (parallel)───►│      │      │
   │                       │──Tick──────────────────────►│      │
   │                       │──Tick─────────────────────────────►│
   │                       │                    │      │      │
   │                       │                    │ (each processes independently)
```

#### Inbound Raft Messages

```
Transport              PartitionRouter         PartitionActor
   │                       │                       │
   │──RaftMessage─────────►│                       │
   │  (group_id=2)         │                       │
   │                       │                       │
   │                       │──PartitionCommand────►│ (routed to P2)
   │                       │   ::RaftMessage       │
   │                       │                       │
   │                       │                       │──process message
   │                       │                       │──update Raft state
   │                       │                       │──maybe send responses
```

### WAL Batching Strategy

The WalActor batches writes from multiple partitions for efficient disk I/O:

```rust
impl WalActor {
    async fn run(mut self) {
        let mut batch: Vec<PendingWrite> = Vec::with_capacity(100);
        let mut linger_deadline: Option<Instant> = None;

        loop {
            let timeout = linger_deadline
                .map(|d| d.saturating_duration_since(Instant::now()))
                .unwrap_or(Duration::from_secs(3600));

            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(WalCommand::Append { group_id, entry, reply }) => {
                            batch.push(PendingWrite { group_id, entry, reply });

                            // Set linger deadline on first write
                            if linger_deadline.is_none() {
                                linger_deadline = Some(
                                    Instant::now() + Duration::from_millis(self.linger_ms)
                                );
                            }

                            // Flush if batch full
                            if batch.len() >= self.max_batch_size {
                                self.flush_batch(&mut batch).await;
                                linger_deadline = None;
                            }
                        }
                        Some(WalCommand::Flush) | None => {
                            self.flush_batch(&mut batch).await;
                            linger_deadline = None;
                            if cmd.is_none() { break; }
                        }
                        Some(WalCommand::Shutdown) => break,
                    }
                }

                // Linger timeout - flush partial batch
                _ = sleep_until(linger_deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600))) => {
                    if !batch.is_empty() {
                        self.flush_batch(&mut batch).await;
                        linger_deadline = None;
                    }
                }
            }
        }
    }

    async fn flush_batch(&mut self, batch: &mut Vec<PendingWrite>) {
        if batch.is_empty() { return; }

        // Group by partition for efficient WAL writes
        // (WAL may optimize writes from same partition)
        batch.sort_by_key(|w| w.group_id);

        // Write all entries to WAL
        for write in batch.drain(..) {
            match self.wal.append(write.group_id, &write.entry).await {
                Ok(offset) => { let _ = write.reply.send(Ok(offset)); }
                Err(e) => { let _ = write.reply.send(Err(e)); }
            }
        }
    }
}
```

## Implementation Plan

### Phase 1: PartitionActor Foundation

**Goal:** Create PartitionActor that wraps existing RaftState

**Files to modify:**
- `helix-server/src/service/partition_actor.rs` (new)
- `helix-server/src/service/mod.rs`

**Tasks:**
1. Define `PartitionCommand` enum
2. Implement `PartitionActor` struct with message loop
3. Handle `Propose`, `Tick`, `RaftMessage` commands
4. Add unit tests for actor message handling

**Verification:**
- Unit tests pass
- Actor processes commands correctly in isolation

### Phase 2: WalActor with Batching

**Goal:** Create WalActor that batches writes from multiple partitions

**Files to modify:**
- `helix-server/src/service/wal_actor.rs` (new)
- `helix-server/src/service/mod.rs`

**Tasks:**
1. Define `WalCommand` enum
2. Implement `WalActor` with batching logic
3. Configurable linger_ms and max_batch_size
4. Add unit tests for batching behavior

**Verification:**
- Unit tests for batching (linger timeout, size threshold)
- Verify writes are correctly ordered

### Phase 3: PartitionRouter

**Goal:** Replace `Arc<RwLock<MultiRaft>>` with router to actors

**Files to modify:**
- `helix-server/src/service/router.rs` (new)
- `helix-server/src/service/mod.rs`
- `helix-server/src/service/handlers/*.rs`

**Tasks:**
1. Implement `PartitionRouter` struct
2. Add methods: `partition()`, `wal_for()`, `tick_all()`, `create_partition()`
3. Update `HelixService` to use router instead of `Arc<RwLock<MultiRaft>>`
4. Update all handlers to use router

**Verification:**
- Existing tests pass (functional equivalence)
- No lock contention in flame graphs

### Phase 4: Batcher Integration

**Goal:** Update batcher to send to partition actors instead of locking MultiRaft

**Files to modify:**
- `helix-server/src/service/batcher.rs`

**Tasks:**
1. Replace `Arc<RwLock<MultiRaft>>` with `PartitionRouter`
2. Update `flush_batch` to send `PartitionCommand::Propose`
3. Await responses via oneshot channels
4. Remove lock-based batching logic

**Verification:**
- Throughput test passes
- No lock contention

### Phase 5: Transport Integration

**Goal:** Route inbound Raft messages to partition actors

**Files to modify:**
- `helix-server/src/service/tick.rs`
- `helix-runtime/src/transport.rs` (if needed)

**Tasks:**
1. Update message receive loop to route via `PartitionRouter`
2. Send `PartitionCommand::RaftMessage` to correct actor
3. Update tick loop to use `router.tick_all()`

**Verification:**
- Multi-node cluster tests pass
- DST tests pass

### Phase 6: Performance Validation

**Goal:** Verify multi-partition throughput matches single-partition

**Tasks:**
1. Run single-partition throughput test (baseline)
2. Run multi-partition throughput test
3. Compare results - expect <5% gap
4. Profile with flamegraph - verify no lock contention
5. Run DST stress tests

**Success Criteria:**
- Multi-partition throughput within 5% of single-partition
- Zero lock contention in profiles
- All existing tests pass
- DST finds no new issues

## Migration Strategy

### Backwards Compatibility

The actor model is an internal refactor. External API unchanged:
- Kafka protocol: unchanged
- gRPC API: unchanged
- CLI flags: unchanged

### Rollout

1. Feature flag: `--use-actor-model` (default: false)
2. Run both paths in parallel during testing
3. Compare throughput and correctness
4. Enable by default once validated
5. Remove old code path

### Rollback

If issues found:
1. Disable feature flag
2. Falls back to lock-based MultiRaft
3. No data migration needed (WAL format unchanged)

## Performance Expectations

| Metric | Current | Expected |
|--------|---------|----------|
| Single-partition throughput | 125 MB/s | 125 MB/s (unchanged) |
| 8-partition throughput | 110 MB/s | 120+ MB/s |
| 16-partition throughput | ~100 MB/s (est) | 120+ MB/s |
| Lock contention | High | Zero |
| Latency p99 | Variable | More consistent |

## Open Questions

1. **Channel buffer sizes** - How large should partition actor channels be? Too small = backpressure, too large = memory.

2. **Tick distribution** - Should ticks be staggered or simultaneous? Staggered may reduce burst CPU usage.

3. **Error propagation** - How to handle actor panics? Supervisor pattern?

4. **Metrics** - How to expose per-actor metrics for debugging?

5. **Dynamic partition creation** - How to spawn new actors at runtime when topics are created?

## References

- [Throughput Improvement Plan](./throughput-improvement-plan.md)
- [Shared WAL Design](./shared-wal.md)
- [Akka Actor Model](https://doc.akka.io/docs/akka/current/typed/actors.html)
- [Tokio Actors](https://ryhl.io/blog/actors-with-tokio/)
