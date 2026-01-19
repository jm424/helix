# Vote State Persistence

**Status:** Proposed
**Author:** AI-assisted design
**Date:** 2026-01-17

## Context

Helix uses Raft consensus for partition replication. Raft requires two pieces of state to survive crashes to guarantee safety:

- `current_term`: The latest term the node has seen
- `voted_for`: The candidate the node voted for in the current term (if any)

Without persisting these, a restarted node can vote twice in the same term, allowing two candidates to both achieve quorum and become leader. This violates Raft's core safety guarantee: at most one leader per term.

### Current State

Helix currently does not persist vote state. This works for process crashes only if:
- The test infrastructure doesn't simulate crashes (DST limitation)
- Production deployments never experience disk loss

### Deployment Model

Helix targets Kubernetes with StatefulSets and local NVMe SSDs:

| Event | Pod name | Disk | Impact |
|-------|----------|------|--------|
| Process crash | Same | Same | Common, need fast recovery |
| Pod rescheduled (node failure) | Same | **Lost** | Rare, need safe recovery |

StatefulSets maintain stable pod identities (`helix-0`, `helix-1`, `helix-2`). When a pod is rescheduled to a different physical node, the pod name stays the same but the local SSD data is lost.

### Why Not Dynamic Membership?

Helix currently has static Raft membership - groups are created with a fixed peer list. Without dynamic membership (joint consensus), a node with a new identity cannot join an existing group. This means:

- Rescheduled pods must keep the same `node_id`
- `node_id` is derived from pod name
- Disk loss requires vote state recovery, not node replacement

Dynamic membership is significant work (Raft paper Section 6) and is out of scope for this design.

## Goals

1. **Safety**: Prevent double-voting after any restart scenario
2. **Fast recovery**: Process crashes recover in milliseconds (local disk)
3. **Automatic recovery**: Disk loss recovers without operator intervention
4. **Minimal latency impact**: Vote persistence on critical path should be ~1-5ms

## Non-Goals

1. Dynamic membership changes
2. Persisting Raft log entries (handled separately by WAL)
3. Cross-region replication of vote state

## Design Overview

Two-tier persistence:

```
Vote/term change:
  ├─→ Local file + fsync (synchronous, ~1-5ms) ← critical path
  └─→ S3 upload (async, background)            ← off critical path

Startup:
  ├─→ Local file exists? Use it (fast path)
  └─→ Fetch from S3 → observation mode (safe recovery)
```

### Why Two Tiers?

| Tier | Latency | Survives | Use case |
|------|---------|----------|----------|
| Local file | ~1-5ms | Process crash | Common case, fast |
| S3 backup | ~50-200ms | Disk loss | Rare case, safe |

This matches Kafka/Redpanda's local persistence approach, plus adds S3 backup for automated disk loss recovery (which they don't have - they require manual node replacement).

## Detailed Design

### Data Format

Binary format, ~24 bytes per group:

```rust
/// Persistent vote state for all groups on this node.
struct VoteState {
    /// Format version for forward compatibility.
    version: u32,
    /// Monotonically increasing sequence number.
    /// Used to detect stale S3 data.
    sequence: u64,
    /// Per-group vote state.
    groups: Vec<GroupVoteState>,
    /// CRC32 checksum of above fields.
    checksum: u32,
}

/// Vote state for a single Raft group.
struct GroupVoteState {
    group_id: u64,
    term: u64,
    /// 0 = None (haven't voted), non-zero = voted for this node_id
    voted_for: u64,
}
```

For 10,000 groups: ~240KB total.

### File Locations

```
Local:  {data_dir}/vote-state.bin
S3:     s3://{bucket}/helix/vote-state/node-{node_id}.bin
```

### Write Path

```rust
impl VoteStore {
    /// Called by RaftNode when term or voted_for changes.
    /// MUST complete local write before returning.
    pub fn save(&mut self, group_id: GroupId, term: TermId, voted_for: Option<NodeId>) {
        // 1. Update in-memory state
        self.state.sequence += 1;
        self.state.update_group(group_id, term, voted_for);

        // 2. Write local file with fsync (BLOCKING)
        self.write_local_file()?;

        // 3. Queue async S3 upload (NON-BLOCKING)
        self.s3_upload_tx.send(self.state.clone());
    }
}
```

**S3 Upload Worker:**

```rust
/// Background task that serializes S3 uploads.
async fn s3_upload_worker(
    rx: Receiver<VoteState>,
    storage: Arc<dyn ObjectStorage>,
    node_id: NodeId,
) {
    let mut pending: Option<VoteState> = None;

    loop {
        // Wait for work
        let state = match pending.take() {
            Some(s) => s,
            None => rx.recv().await?,
        };

        // Drain channel - only upload latest state
        while let Ok(newer) = rx.try_recv() {
            pending = Some(newer);
        }
        let state = pending.take().unwrap_or(state);

        // Upload to S3
        let key = ObjectKey::new(format!("helix/vote-state/node-{}.bin", node_id.get()));
        let data = state.serialize();

        match storage.put(&key, data).await {
            Ok(()) => { /* success */ }
            Err(e) => {
                // Retry on next state change, or with backoff
                tracing::warn!("S3 upload failed: {e}, will retry");
                pending = Some(state);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
```

Key properties:
- Single worker ensures serialized uploads (no out-of-order writes)
- Coalesces rapid state changes (only uploads latest)
- Retries on failure

### Read Path

```rust
impl VoteStore {
    /// Load vote state on startup.
    /// Returns (state, recovered_from_s3).
    pub async fn load(
        data_dir: &Path,
        storage: &dyn ObjectStorage,
        node_id: NodeId,
    ) -> Result<(VoteState, bool)> {
        // 1. Try local file first
        let local_path = data_dir.join("vote-state.bin");
        if let Ok(data) = std::fs::read(&local_path) {
            let state = VoteState::deserialize(&data)?;
            state.verify_checksum()?;
            return Ok((state, false)); // Not from S3
        }

        // 2. Local not found - try S3
        let key = ObjectKey::new(format!("helix/vote-state/node-{}.bin", node_id.get()));
        match storage.get(&key).await {
            Ok(data) => {
                let state = VoteState::deserialize(&data)?;
                state.verify_checksum()?;

                // Write to local for next restart
                std::fs::write(&local_path, &data)?;

                Ok((state, true)) // Recovered from S3
            }
            Err(TierError::NotFound { .. }) => {
                // New node, start fresh
                Ok((VoteState::new(), false))
            }
            Err(e) => Err(e.into()),
        }
    }
}
```

### Observation Mode

When recovering from S3, the vote state may be stale (S3 upload was async). To prevent double-voting:

```rust
impl RaftNode {
    fn handle_request_vote(&mut self, req: RequestVoteRequest) -> Vec<RaftOutput> {
        // Don't vote during observation period
        if self.observation_mode {
            return vec![self.reject_vote(req, "in observation mode")];
        }

        // ... normal vote handling
    }

    fn tick(&mut self) -> Vec<RaftOutput> {
        // Track observation period
        if self.observation_mode {
            self.observation_elapsed += 1;
            if self.observation_elapsed >= self.config.election_tick {
                self.observation_mode = false;
                tracing::info!("Observation period complete, ready to vote");
            }
        }

        // ... normal tick handling
    }
}
```

**When observation mode activates:**

| Startup scenario | Observation mode? | Why |
|------------------|-------------------|-----|
| Local file exists | No | Definitely current |
| S3 file exists | Yes | May be stale |
| Neither exists | No | New node, fresh state |

**During observation mode:**
- Receives messages normally
- Updates term from heartbeats (learns current term)
- Rejects vote requests
- After one election timeout, participates normally

**Cold start handling:**
- All nodes start fresh (no local, no S3)
- No observation mode
- Normal Raft bootstrap proceeds

### Integration Points

**RaftNode changes:**

```rust
pub struct RaftNode {
    // ... existing fields ...

    /// Whether in observation mode (recovered from S3).
    observation_mode: bool,
    /// Ticks elapsed in observation mode.
    observation_elapsed: u32,
}

impl RaftNode {
    /// Creates a RaftNode with restored vote state.
    pub fn with_vote_state(
        config: RaftConfig,
        term: TermId,
        voted_for: Option<NodeId>,
        observation_mode: bool,
    ) -> Self {
        Self {
            current_term: term,
            voted_for,
            observation_mode,
            observation_elapsed: 0,
            // ... other fields initialized normally
        }
    }
}
```

**MultiRaft changes:**

```rust
impl MultiRaft {
    /// Creates a group with restored vote state.
    pub fn create_group_with_state(
        &mut self,
        group_id: GroupId,
        peers: Vec<NodeId>,
        term: TermId,
        voted_for: Option<NodeId>,
        observation_mode: bool,
    ) -> Result<(), MultiRaftError>;
}
```

**HelixService changes:**

```rust
impl HelixService {
    pub async fn new_multi_node(...) -> Self {
        // Load vote state
        let (vote_state, from_s3) = VoteStore::load(&data_dir, &storage, node_id).await?;

        // Create groups with restored state
        for group_state in vote_state.groups {
            multi_raft.create_group_with_state(
                group_state.group_id,
                peers.clone(),
                group_state.term,
                group_state.voted_for,
                from_s3, // observation mode if recovered from S3
            )?;
        }

        // Start S3 upload worker
        let vote_store = VoteStore::new(data_dir, storage, node_id, vote_state);
        tokio::spawn(vote_store.run_upload_worker());

        // ... rest of initialization
    }
}
```

## Deterministic Simulation Testing (DST)

Full DST support enables comprehensive testing of all failure scenarios with deterministic reproducibility.

### Storage Abstractions

To enable DST, both local and remote storage use trait abstractions:

```rust
/// Local vote state storage abstraction.
/// Enables deterministic simulation of disk operations.
pub trait VoteStorage: Send + Sync {
    /// Read vote state from storage.
    fn read(&self) -> Result<Option<Bytes>, VoteStoreError>;

    /// Write vote state to storage with fsync.
    fn write(&self, data: &[u8]) -> Result<(), VoteStoreError>;

    /// Delete vote state (for testing disk loss).
    fn delete(&self) -> Result<(), VoteStoreError>;
}

/// Production implementation using local filesystem.
pub struct LocalFileVoteStorage {
    path: PathBuf,
}

impl VoteStorage for LocalFileVoteStorage {
    fn read(&self) -> Result<Option<Bytes>, VoteStoreError> {
        match std::fs::read(&self.path) {
            Ok(data) => Ok(Some(Bytes::from(data))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(VoteStoreError::Io(e)),
        }
    }

    fn write(&self, data: &[u8]) -> Result<(), VoteStoreError> {
        let file = std::fs::File::create(&self.path)?;
        file.write_all(data)?;
        file.sync_all()?; // fsync
        Ok(())
    }

    fn delete(&self) -> Result<(), VoteStoreError> {
        std::fs::remove_file(&self.path).ok();
        Ok(())
    }
}

/// Simulated implementation for DST.
pub struct SimulatedVoteStorage {
    /// In-memory storage with interior mutability.
    data: Arc<Mutex<Option<Vec<u8>>>>,
    /// Fault injection configuration.
    fault_config: Arc<Mutex<VoteStorageFaultConfig>>,
    /// RNG seed for deterministic faults.
    seed: u64,
    /// Operation counter for deterministic RNG.
    counter: Arc<AtomicU64>,
}

/// Fault injection configuration for simulated storage.
pub struct VoteStorageFaultConfig {
    /// Probability of read failing.
    pub read_fail_rate: f64,
    /// Probability of write failing.
    pub write_fail_rate: f64,
    /// Force next read to fail (one-shot).
    pub force_read_fail: bool,
    /// Force next write to fail (one-shot).
    pub force_write_fail: bool,
}
```

### Remote Storage

Remote (S3) storage reuses the existing `helix_tier::ObjectStorage` trait:

- `helix_tier::S3Storage` - Production S3 implementation
- `helix_tier::SimulatedObjectStorage` - DST implementation with fault injection

This avoids duplicating the object storage abstraction.

### DST Test Scenarios

The storage abstractions enable testing all failure scenarios deterministically:

| Scenario | Local Storage | Remote Storage | Expected Behavior |
|----------|---------------|----------------|-------------------|
| Normal startup | Has data | Has data | Use local, no observation |
| Process crash recovery | Has data | May be stale | Use local, no observation |
| Disk loss (pod reschedule) | Empty | Has data | Use S3, enter observation mode |
| Cold start (new cluster) | Empty | Empty | Start fresh, no observation |
| Local write failure | Inject fault | N/A | Fail vote, maintain safety |
| S3 upload failure | Works | Inject fault | Local works, S3 retries |
| S3 unavailable on startup | Empty | Inject fault | Fail to start |
| Corrupted local file | Bad checksum | Has data | Fall back to S3 |
| Corrupted S3 file | Empty | Bad checksum | Fail to start |
| S3 has stale data | Empty | Old term | Observation mode prevents double-vote |

### Example DST Test Implementation

```rust
#[test]
fn test_vote_persistence_prevents_double_vote_dst() {
    // Create simulated storage
    let local = Arc::new(SimulatedVoteStorage::new(42));
    let s3 = Arc::new(SimulatedObjectStorage::new(42));

    // Setup: Node 1 has voted in term 5
    let initial_state = VoteState::new()
        .with_group(GroupId::new(1), TermId::new(5), Some(NodeId::new(2)));
    local.write(&initial_state.serialize()).unwrap();

    // Create vote store and load state
    let vote_store = VoteStore::new(local.clone(), s3.clone(), NodeId::new(1));
    let (state, from_s3) = vote_store.load().unwrap();

    assert!(!from_s3); // Loaded from local
    assert_eq!(state.get_group(GroupId::new(1)).unwrap().term, TermId::new(5));
    assert_eq!(state.get_group(GroupId::new(1)).unwrap().voted_for, Some(NodeId::new(2)));

    // Create RaftNode with restored state
    let config = RaftConfig::new(NodeId::new(1), vec![...]);
    let mut node = RaftNode::with_vote_state(
        config,
        TermId::new(5),
        Some(NodeId::new(2)),
        false, // not from S3, no observation
    );

    // Receive RequestVote for same term from different candidate
    let req = RequestVoteRequest::new(
        TermId::new(5),
        NodeId::new(3), // Different candidate
        NodeId::new(1),
        LogIndex::new(0),
        TermId::new(0),
    );

    let outputs = node.handle_message(Message::RequestVote(req));

    // Verify: Vote rejected (already voted for Node 2 in term 5)
    assert!(outputs.iter().any(|o| matches!(
        o,
        RaftOutput::SendMessage(Message::RequestVoteResponse(resp))
        if !resp.vote_granted
    )));
}

#[test]
fn test_s3_recovery_observation_mode_dst() {
    let local = Arc::new(SimulatedVoteStorage::new(42));
    let s3 = Arc::new(SimulatedObjectStorage::new(42));

    // Setup: S3 has term 5 state (simulating stale backup)
    // Local is empty (simulating disk loss)
    let s3_state = VoteState::new()
        .with_group(GroupId::new(1), TermId::new(5), Some(NodeId::new(2)));
    let key = ObjectKey::new("helix/vote-state/node-1.bin");
    s3.put(&key, Bytes::from(s3_state.serialize())).await.unwrap();

    // Load - should recover from S3
    let vote_store = VoteStore::new(local, s3, NodeId::new(1));
    let (state, from_s3) = vote_store.load().await.unwrap();

    assert!(from_s3); // Recovered from S3

    // Create RaftNode with observation mode
    let mut node = RaftNode::with_vote_state(
        config,
        TermId::new(5),
        Some(NodeId::new(2)),
        true, // from S3, observation mode enabled
    );

    // Receive RequestVote for term 6
    let req = RequestVoteRequest::new(
        TermId::new(6),
        NodeId::new(3),
        NodeId::new(1),
        LogIndex::new(0),
        TermId::new(0),
    );

    let outputs = node.handle_message(Message::RequestVote(req));

    // Verify: Vote rejected during observation
    assert!(outputs.iter().any(|o| matches!(
        o,
        RaftOutput::SendMessage(Message::RequestVoteResponse(resp))
        if !resp.vote_granted
    )));

    // Tick through observation period
    for _ in 0..node.config().election_tick {
        node.tick();
    }

    // Now can vote
    let req2 = RequestVoteRequest::new(
        TermId::new(7),
        NodeId::new(3),
        NodeId::new(1),
        LogIndex::new(0),
        TermId::new(0),
    );

    let outputs = node.handle_message(Message::RequestVote(req2));

    // Verify: Vote granted after observation period
    assert!(outputs.iter().any(|o| matches!(
        o,
        RaftOutput::SendMessage(Message::RequestVoteResponse(resp))
        if resp.vote_granted
    )));
}

#[test]
fn test_disk_loss_during_active_election_dst() {
    // Full simulation test with multiple nodes
    let seed = 12345u64;

    // Create simulated storage for each node
    let storages: Vec<_> = (1..=3).map(|i| {
        let local = Arc::new(SimulatedVoteStorage::new(seed + i));
        let s3 = Arc::new(SimulatedObjectStorage::new(seed + i));
        (local, s3)
    }).collect();

    // Create Bloodhound simulation
    let mut engine = SimulationEngine::new(seed);

    // ... setup actors with vote stores ...

    // Schedule events:
    // 1. Let election complete, node 1 becomes leader
    // 2. Node 1 votes in term 2
    // 3. Simulate disk loss on node 1 (clear local storage)
    // 4. S3 upload was slow, still has term 1 data
    // 5. Node 1 recovers from S3 with stale state
    // 6. New election starts for term 2

    engine.schedule_after(Duration::from_millis(5000), |ctx| {
        // Simulate disk loss
        storages[0].0.delete().unwrap();
    });

    engine.schedule_after(Duration::from_millis(5100), |ctx| {
        // Node 1 recovers
        // ... trigger recovery ...
    });

    let result = engine.run();

    // Verify: No safety violations despite stale S3 state
    // (observation mode should prevent double-voting)
    let violations = check_safety_properties(&result);
    assert!(violations.is_empty(), "Safety violations: {:?}", violations);
}
```

### Fault Injection Patterns

```rust
// Test local disk failure during vote
#[test]
fn test_local_write_failure_maintains_safety() {
    let local = Arc::new(SimulatedVoteStorage::new(42));
    local.fault_config().force_write_fail = true;

    let mut vote_store = VoteStore::new(local, s3, node_id);

    // Attempt to save vote - should fail
    let result = vote_store.save(group_id, term, voted_for);
    assert!(result.is_err());

    // Vote should NOT be granted if local persistence failed
    // (safety: don't acknowledge vote without durability)
}

// Test S3 failure during upload
#[test]
fn test_s3_upload_failure_retries() {
    let s3 = Arc::new(SimulatedObjectStorage::new(42));
    s3.fault_config().force_put_fail = true;

    let vote_store = VoteStore::new(local, s3.clone(), node_id);
    vote_store.save(group_id, term, voted_for).unwrap(); // Local succeeds

    // S3 upload fails, but local is durable
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(s3.get(&key).await.is_err()); // Not in S3 yet

    // Clear fault, let retry succeed
    s3.fault_config().force_put_fail = false;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Now should be in S3
    assert!(s3.get(&key).await.is_ok());
}
```

## Testing

### Unit Tests

```rust
#[test]
fn test_vote_state_roundtrip() {
    let state = VoteState { ... };
    let bytes = state.serialize();
    let recovered = VoteState::deserialize(&bytes).unwrap();
    assert_eq!(state, recovered);
}

#[test]
fn test_checksum_detects_corruption() {
    let mut bytes = valid_state.serialize();
    bytes[10] ^= 0xFF; // Corrupt one byte
    assert!(VoteState::deserialize(&bytes).is_err());
}
```

### DST Tests

```rust
#[test]
fn test_vote_persistence_prevents_double_vote() {
    // 1. Node 1 votes for Node 2 in term 5
    // 2. Node 1 crashes
    // 3. Node 1 recovers with persisted state
    // 4. Node 1 receives RequestVote for term 5 from Node 3
    // 5. Verify: Node 1 rejects (already voted in term 5)
}

#[test]
fn test_s3_recovery_with_observation_mode() {
    // 1. Node 1 votes in term 5, S3 upload completes
    // 2. Node 1 votes in term 6, crashes before S3 upload
    // 3. Disk is lost (simulated)
    // 4. Node 1 recovers from S3 (has term 5 state)
    // 5. Verify: Node 1 enters observation mode
    // 6. Verify: Node 1 rejects votes during observation
    // 7. Verify: Node 1 learns term 7 from heartbeats
    // 8. Verify: Node 1 can vote in term 8 after observation
}

#[test]
fn test_cold_start_no_observation() {
    // 1. Three nodes start fresh (no local, no S3)
    // 2. Verify: No observation mode
    // 3. Verify: Election proceeds normally
    // 4. Verify: Leader elected within expected time
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_process_crash_recovery() {
    // 1. Start 3-node cluster
    // 2. Perform some writes
    // 3. Kill node 1 process
    // 4. Restart node 1
    // 5. Verify: Fast recovery (local file)
    // 6. Verify: No observation mode
    // 7. Verify: Cluster healthy
}

#[tokio::test]
async fn test_disk_loss_recovery() {
    // 1. Start 3-node cluster
    // 2. Perform some writes, wait for S3 sync
    // 3. Delete node 1's local vote-state.bin
    // 4. Restart node 1
    // 5. Verify: Recovers from S3
    // 6. Verify: Enters observation mode
    // 7. Verify: Cluster healthy after observation
}
```

## Alternatives Considered

### 1. Delayed Participation (No Persistence)

After restart, don't vote for one election timeout. Learn current term from heartbeats.

**Rejected because:**
- Breaks cold start (no heartbeats to observe)
- Multiple simultaneous restarts cause quorum loss
- Industry consensus (Kafka, Redpanda, etcd) is to persist

### 2. S3 Only (No Local File)

Write directly to S3 on every vote change.

**Rejected because:**
- 50-200ms latency on critical path
- Would need to increase election timeout significantly
- Process crashes (common) would pay S3 latency

### 3. Local Only (No S3)

Persist locally, require manual intervention on disk loss.

**Rejected because:**
- K8s pod rescheduling is a normal event, not exceptional
- Operator burden for routine infrastructure events
- Want automated recovery for better operability

### 4. Dynamic Membership

Treat disk loss as node replacement. Add new node, remove old.

**Rejected because:**
- Requires implementing Raft joint consensus (significant work)
- Out of scope for current phase
- May revisit in Phase 5+

## Implementation Plan

### Phase 1: Local Persistence (~2 days)

1. Add `VoteStore` struct with local file read/write
2. Add `VoteState` serialization with checksum
3. Add `RaftNode::with_vote_state()` constructor
4. Add `MultiRaft::create_group_with_state()`
5. Integrate with `HelixService` startup
6. Unit tests for serialization/deserialization

### Phase 2: S3 Backup (~2 days)

1. Add S3 upload worker with serialization
2. Add S3 recovery path in `VoteStore::load()`
3. Wire up `ObjectStorage` from helix-tier
4. Integration tests with simulated S3

### Phase 3: Observation Mode (~1 day)

1. Add `observation_mode` field to `RaftNode`
2. Reject votes during observation
3. Exit observation after election timeout
4. DST tests for double-vote prevention

### Phase 4: DST Validation (~1 day)

1. Fix existing `test_crash_during_election`
2. Add comprehensive crash/recovery DST tests
3. Verify safety properties hold under all scenarios

## Open Questions

1. **S3 bucket configuration**: Should this be a separate bucket/prefix from tiered segments, or reuse the same?

2. **Startup behavior if S3 unavailable**: Fail to start? Start with observation mode? Currently proposing fail-to-start for safety.

3. **Metrics**: What metrics should we expose? Suggested:
   - `helix_vote_state_local_writes_total`
   - `helix_vote_state_s3_uploads_total`
   - `helix_vote_state_s3_upload_errors_total`
   - `helix_vote_state_s3_recoveries_total`
   - `helix_vote_state_observation_mode_seconds`

## References

- [Raft Paper](https://raft.github.io/raft.pdf) - Section 5.2 (Leader Election), Section 5.4 (Safety)
- [KIP-595: KRaft](https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum) - Kafka's Raft implementation
- [KIP-856: KRaft Disk Failure Recovery](https://cwiki.apache.org/confluence/display/KAFKA/KIP-856%3A+KRaft+Disk+Failure+Recovery) - Kafka's approach to disk loss
- [Redpanda Architecture](https://docs.redpanda.com/current/get-started/architecture/) - Redpanda's Raft implementation
