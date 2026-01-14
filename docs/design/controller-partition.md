# Controller Partition Implementation Plan

**Status**: In Progress
**Pattern**: Controller partition for metadata coordination
**Goal**: Coordinate metadata and partition assignments across cluster using Raft

## Overview

The controller partition is a special Raft group (group ID 0) that ALL nodes in the cluster participate in. It stores all metadata commands and ensures all nodes have a consistent view of:
- Topics and their configurations
- Partition assignments (which nodes host which partitions)
- Partition leadership

This replaces the current "auto-create" approach where each node independently creates topics/partitions, leading to inconsistent state.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Controller Partition (Group 0)                │
│                                                                  │
│  Members: ALL cluster nodes                                      │
│  Leader: Elected via Raft (handles all write commands)           │
│                                                                  │
│  Commands:                                                       │
│    - CreateTopic { name, partitions, replication_factor }        │
│    - DeleteTopic { name }                                        │
│    - AssignPartition { topic, partition, replicas[], leader }    │
│                                                                  │
│  State (rebuilt from log):                                       │
│    - topics: HashMap<TopicName, TopicMetadata>                   │
│    - assignments: HashMap<(Topic, Partition), Assignment>        │
└─────────────────────────────────────────────────────────────────┘
                              │
        When assignment committed, nodes in replica set create:
                              ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Topic A:0   │  │  Topic A:1   │  │  Topic B:0   │
│  Group ID: 1 │  │  Group ID: 2 │  │  Group ID: 3 │
│  Replicas:   │  │  Replicas:   │  │  Replicas:   │
│   1,2,3      │  │   2,3,1      │  │   3,1,2      │
└──────────────┘  └──────────────┘  └──────────────┘
     (Data Raft groups - only on assigned nodes)
```

## Implementation Phases

### Phase 1: Controller Commands and State
**Status**: [x] Complete

Define the command types and state machine for the controller.

**Files to modify/create:**
- `helix-server/src/controller.rs` (new)

**Tasks:**
1. [x] Define `ControllerCommand` enum:
   ```rust
   pub enum ControllerCommand {
       CreateTopic {
           name: String,
           partition_count: u32,
           replication_factor: u32,
       },
       DeleteTopic {
           name: String,
       },
       // Internal: assigned by controller leader when processing CreateTopic
       AssignPartition {
           topic_id: TopicId,
           partition_id: PartitionId,
           group_id: GroupId,
           replicas: Vec<NodeId>,  // First node is preferred leader
       },
   }
   ```

2. [x] Define `ControllerState`:
   ```rust
   pub struct ControllerState {
       /// All topics in the cluster.
       topics: HashMap<String, TopicInfo>,
       /// Partition assignments: (topic_id, partition_id) -> Assignment
       assignments: HashMap<(TopicId, PartitionId), PartitionAssignment>,
       /// Next topic ID to allocate.
       next_topic_id: u64,
       /// Next group ID to allocate (starts at 1, 0 is controller).
       next_group_id: u64,
   }

   pub struct TopicInfo {
       topic_id: TopicId,
       partition_count: u32,
       replication_factor: u32,
   }

   pub struct PartitionAssignment {
       group_id: GroupId,
       replicas: Vec<NodeId>,
       leader: Option<NodeId>,
   }
   ```

3. [x] Implement command encoding/decoding (similar to PartitionCommand)

4. [x] Implement `ControllerState::apply(command)` to update state

---

### Phase 2: Controller Partition Initialization
**Status**: [x] Complete

Create the controller partition (group 0) when nodes start.

**Files to modify:**
- `helix-server/src/service.rs`

**Tasks:**
1. [x] Define `CONTROLLER_GROUP_ID = GroupId::new(0)` in `controller.rs`

2. [x] In `new_multi_node()`, create controller group with all cluster nodes

3. [x] Add `ControllerState` to `HelixService`

4. [x] In tick task, apply committed controller entries to update state via `process_outputs_multi_node`

5. [ ] Wait for controller leader election before accepting requests (deferred to Phase 5)

---

### Phase 3: Topic Creation Through Controller
**Status**: [x] Complete

Route topic creation through the controller Raft group.

**Files modified:**
- `helix-server/src/service.rs` - Added `create_topic_via_controller()` method
- `helix-server/src/kafka/handler.rs` - Updated to use controller in multi-node mode

**Tasks:**
1. [x] Added `create_topic_via_controller()` to propose `CreateTopic` command to controller
2. [x] Replica assignment algorithm implemented in `ControllerState::assign_replicas()`
3. [x] When `CreateTopic` is committed, controller leader generates `AssignPartition` commands (in `apply()`)
4. [ ] Implement waiting for command commit (proposal tracking) - deferred to Phase 6

---

### Phase 4: Data Partition Creation from Controller State
**Status**: [x] Complete

When partition assignments are committed, create data Raft groups.

**Files to modify:**
- `helix-server/src/service.rs`

**Tasks:**
1. [x] In `process_outputs_multi_node`, handle `AssignPartition` commits:
   - Check if this node is in the replica set
   - Create data Raft group if it doesn't exist
   - Update `GroupMap` with topic/partition mapping
   - Create partition storage

2. [x] Added `GroupMap::insert()` method for controller-assigned group IDs

3. [ ] Remove old auto-create logic (deferred - will coexist during transition)

---

### Phase 5: Kafka Handler Integration
**Status**: [~] Partially Complete

Update Kafka protocol handler to work with controller.

**Files modified:**
- `helix-server/src/kafka/handler.rs`
- `helix-server/src/service.rs` - Added `is_multi_node()` helper

**Tasks:**
1. [ ] Metadata requests: Read from controller state (not local topic map) - deferred

2. [x] Auto-create topics: Uses `create_topic_via_controller()` in multi-node mode
   - Updated metadata request handler
   - Updated produce request handler

3. [ ] Produce requests: Check controller state for leader - deferred
   - Currently relies on Raft for leader election

4. [ ] Fetch requests: Read from local partition storage (if we're a replica) - already works

---

### Phase 6: Wait for Raft Commit
**Status**: [x] Complete

Fix `append_blob` to wait for Raft commit before returning success.

**Files modified:**
- `helix-server/src/service.rs`

**Implementation:**
1. [x] Added `PendingProposal` struct with `log_index` and `result_tx: oneshot::Sender<ServerResult<Offset>>`
2. [x] Added `pending_proposals: Arc<RwLock<HashMap<GroupId, Vec<PendingProposal>>>>` to `HelixService`
3. [x] Updated `tick_task_multi_node` and `process_outputs_multi_node` to:
   - Apply committed entries
   - Find matching pending proposal by (group_id, index)
   - Send the resulting offset through the channel
4. [x] Updated `append_blob` (multi-node path) to:
   - Create oneshot channel
   - Propose to Raft and get log index
   - Register pending proposal
   - Await channel with 30s timeout

**Key design decisions:**
- Tick task applies entries AND notifies waiters (avoids double-apply race)
- 30-second timeout for commit wait
- Channel closed error handled gracefully

---

### Phase 7: Leader Tracking and Failover
**Status**: [ ] Not Started

Track partition leaders and handle failover.

**Tasks:**
1. [ ] When `BecameLeader` output is received for a data partition:
   - Update controller state with new leader (propose command)
   - Or: track locally and report in metadata responses

2. [ ] When `SteppedDown` output is received:
   - Clear leader from local tracking
   - Reject writes until new leader is elected

3. [ ] Metadata responses should include current leader from Raft state

---

### Phase 8: Testing
**Status**: [~] Partially Complete

Update tests for controller partition pattern.

**Files modified:**
- `helix-workload/src/bin/integration_test.rs`
- `helix-server/src/controller.rs` (unit tests)

**Tasks:**
1. [x] Update integration test:
   - Removed `create_topic_on_all_nodes` (no longer needed)
   - Removed hardcoded sleeps - uses proper polling with `wait_ready` and `wait_for_leader`
   - Topic auto-creates through controller when metadata is requested

2. [x] Add unit tests for controller state machine (8 tests in controller.rs)

3. [ ] Add tests for leader failover

4. [ ] Run integration test to verify end-to-end

5. [ ] Add tests for partition assignment distribution

---

## Key Design Decisions

### 1. Single vs Multi-Partition Controller

**Decision**: Single partition (group 0) with all nodes.

**Rationale**: Simpler implementation, sufficient for moderate cluster sizes. Can optimize to multiple partitions later if needed.

### 2. Command vs Event Sourcing

**Decision**: Commands are proposed, state is rebuilt from committed log.

**Rationale**: Standard Raft pattern. On restart, replay committed entries to rebuild state. Snapshots can optimize recovery.

### 3. Leader Assignment

**Decision**: First replica in list is preferred leader.

**Rationale**: Simple deterministic assignment. Raft will elect actual leader, but we can use transfer_leadership to move to preferred.

### 4. Proposal Tracking

**Decision**: Track pending proposals with oneshot channels.

**Rationale**: Allows async wait for commit. Alternative is polling, which is less efficient.

---

## Migration Path

For existing single-node deployments:
1. Controller partition is created with single node
2. Single node is always controller leader
3. Existing topics continue to work
4. New topics go through controller

For new multi-node deployments:
1. All nodes start and create controller group
2. Controller leader is elected
3. Topics created through controller
4. All nodes have consistent view

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Controller leader becomes bottleneck | Monitor latency; can shard controller later |
| Split-brain during network partition | Raft quorum prevents this |
| Slow commit wait impacts latency | Tune Raft tick interval; ensure fast network |
| State divergence | Raft guarantees consistency; add assertions |

---

## Success Criteria

1. [ ] Multi-node integration test passes with zero violations
2. [ ] All nodes report same topic/partition metadata
3. [ ] Leader failover works correctly
4. [ ] No data loss or inconsistency under normal operations
5. [ ] Clean shutdown and restart preserves state
