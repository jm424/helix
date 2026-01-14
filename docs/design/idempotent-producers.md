# Idempotent Producer Design for Helix

## Executive Summary

This design proposes a server-side idempotent producer implementation that integrates naturally with Helix's existing architecture. The key insight is to perform deduplication BEFORE proposing to Raft, not after, which prevents offset gaps entirely.

## Current Architecture Analysis

### Request Flow (Current)

```
KafkaHandler::handle_produce()
    -> produce_with_auto_create()
        -> append_and_get_offset()
            -> HelixService::append_blob()
                -> check leader
                -> propose to Raft (multi_raft.propose())
                -> wait for commit
                -> tick task applies entry
                -> return offset
```

### Problem Statement

Currently, when a client retries a produce request (e.g., due to timeout), the server has no way to detect duplicates. Each retry creates a new Raft proposal, leading to:
1. Duplicate entries in the Raft log
2. Offset gaps (non-contiguous offsets)
3. Data duplication visible to consumers

## Design Proposal

### Core Data Structures

#### 1. Producer ID Type (in `helix-core/src/types.rs`)

```rust
define_id!(ProducerId, "pid", "Unique identifier for an idempotent producer.");
define_id!(ProducerEpoch, "epoch", "Epoch number for a producer (fence old instances).");
```

#### 2. Producer State (new file: `helix-server/src/producer_state.rs`)

```rust
/// Tracks sequence numbers for an idempotent producer on a single partition.
#[derive(Debug, Clone)]
pub struct PartitionProducerState {
    /// Last sequence number successfully committed.
    pub last_sequence: i32,
    /// First offset of the last successfully committed batch.
    pub last_base_offset: Offset,
    /// Timestamp of last activity (for cleanup).
    pub last_timestamp_ms: i64,
}

/// Per-partition producer state keyed by (ProducerId, ProducerEpoch).
pub type ProducerStateMap = HashMap<(ProducerId, ProducerEpoch), PartitionProducerState>;

/// Idempotent producer state for a single partition.
#[derive(Debug, Default)]
pub struct IdempotentProducerState {
    /// Map of producer states.
    producers: ProducerStateMap,
    /// Maximum number of producers to track (bounded, TigerStyle).
    max_producers: usize,
}

/// Result of sequence validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceCheckResult {
    /// Sequence is valid, proceed with proposal.
    Valid,
    /// Sequence is a duplicate, return cached offset without proposing.
    Duplicate { cached_offset: Offset },
    /// Sequence is out of order (gap detected).
    OutOfSequence { expected: i32, received: i32 },
    /// Producer epoch is stale (fenced).
    ProducerFenced { current_epoch: ProducerEpoch },
}
```

### Integration Points

#### 1. Where to Store Producer State

**Decision: In `PartitionStorage` (in-memory with Raft-persisted state)**

The producer state should be stored alongside partition data in `PartitionStorage`. This keeps related state together and follows existing patterns.

```rust
// In partition_storage.rs
pub struct PartitionStorage {
    topic_id: TopicId,
    partition_id: PartitionId,
    pub(crate) inner: PartitionStorageInner,
    last_applied: LogIndex,
    // NEW: Idempotent producer tracking
    producer_state: IdempotentProducerState,
}
```

**Why this location:**
- Producer state is per-partition, matching `PartitionStorage` scope
- State can be rebuilt from Raft log on recovery
- Deduplication check happens before Raft proposal
- No new crates or major abstractions needed

#### 2. Where Deduplication Check Happens

**Decision: In `HelixService::append_blob()` BEFORE calling `propose()`**

```rust
// In handlers/blob.rs
impl HelixService {
    pub async fn append_blob(
        &self,
        topic: &str,
        partition: i32,
        record_count: u32,
        data: Bytes,
        // NEW: Idempotent producer fields
        producer_id: Option<ProducerId>,
        producer_epoch: Option<ProducerEpoch>,
        base_sequence: Option<i32>,
    ) -> ServerResult<u64> {
        // ... existing validation ...

        // NEW: Sequence check BEFORE proposing to Raft
        if let (Some(pid), Some(epoch), Some(seq)) = (producer_id, producer_epoch, base_sequence) {
            let storage = self.partition_storage.read().await;
            if let Some(ps) = storage.get(&group_id) {
                match ps.check_sequence(pid, epoch, seq) {
                    SequenceCheckResult::Valid => { /* proceed */ }
                    SequenceCheckResult::Duplicate { cached_offset } => {
                        // Return success with cached offset - don't propose again
                        return Ok(cached_offset.get());
                    }
                    SequenceCheckResult::OutOfSequence { expected, received } => {
                        return Err(ServerError::OutOfOrderSequenceNumber {
                            expected, received
                        });
                    }
                    SequenceCheckResult::ProducerFenced { .. } => {
                        return Err(ServerError::ProducerFenced { producer_id: pid });
                    }
                }
            }
        }

        // ... existing propose logic ...
    }
}
```

#### 3. How to Handle `InitProducerId` Request

**Decision: Route through Raft controller partition for cluster-wide uniqueness**

The `InitProducerId` API needs to assign globally unique producer IDs. This should go through the controller partition (group 0) to ensure consistency across the cluster.

```rust
// In controller.rs - new command variant
pub enum ControllerCommand {
    // ... existing variants ...

    /// Allocate a new producer ID.
    AllocateProducerId {
        /// Client-provided transactional ID (optional, for transactions).
        transactional_id: Option<String>,
    },

    /// Response: producer ID allocated.
    ProducerIdAllocated {
        /// The allocated producer ID.
        producer_id: ProducerId,
        /// Initial epoch (always 0 for new producers).
        epoch: ProducerEpoch,
    },
}

// ControllerState gains:
struct ControllerState {
    // ... existing fields ...
    next_producer_id: u64,
}
```

**In protocol handler (`handler.rs`):**

```rust
// New handler for InitProducerId (API key 22)
async fn handle_init_producer_id(&self, request: &DecodedRequest) -> KafkaResult<BytesMut> {
    // Decode request
    let init_request = InitProducerIdRequest::decode(&mut body, api_version)?;

    // Propose to controller partition
    let (producer_id, epoch) = self.service
        .allocate_producer_id(init_request.transactional_id.map(|s| s.to_string()))
        .await?;

    // Build response
    let mut response = InitProducerIdResponse::default();
    response.producer_id = producer_id.get() as i64;
    response.producer_epoch = epoch.get() as i16;
    response.error_code = 0;

    encode_response(request.api_key, request.api_version, request.correlation_id, &response)
}
```

#### 4. Persisting Producer State Across Restarts

**Decision: Encode producer state updates in `PartitionCommand::AppendBlob`**

When a batch is committed, the producer state update is part of the Raft log entry. On recovery, producer state is rebuilt by replaying the log.

```rust
// Modified PartitionCommand in storage.rs
pub enum PartitionCommand {
    // ... existing variants ...

    /// Append a blob with idempotent producer tracking.
    AppendBlobIdempotent {
        blob: Bytes,
        record_count: u32,
        format: BlobFormat,
        // Idempotent producer fields
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        base_sequence: i32,
    },
}
```

When applying this command:
1. Append the blob to storage
2. Update the producer state with new sequence number and offset

On recovery:
1. Replay Raft log entries
2. Rebuild producer state from `AppendBlobIdempotent` commands

### Deduplication Window

A 5-batch deduplication window handles out-of-order retries when multiple requests are in flight. For simplicity, Helix can start with a simpler approach:

**Phase 1 (Minimal):** Track only the last committed sequence per producer. This handles in-order retries.

**Phase 2 (Full compatibility):** Add a circular buffer of the last 5 sequences per producer.

```rust
// Phase 2 enhancement
pub struct PartitionProducerState {
    /// Last sequence number successfully committed.
    pub last_sequence: i32,
    /// Circular buffer of recent sequences (for out-of-order retries).
    /// Each entry is (sequence, base_offset).
    pub recent_batches: [(i32, Offset); 5],
    pub recent_index: usize,
}
```

### Error Codes

New error variants needed in `ServerError`:

```rust
pub enum ServerError {
    // ... existing variants ...

    /// Duplicate sequence number (already committed).
    DuplicateSequenceNumber {
        producer_id: ProducerId,
        sequence: i32,
    },

    /// Sequence number out of order.
    OutOfOrderSequenceNumber {
        expected: i32,
        received: i32,
    },

    /// Producer has been fenced by a newer epoch.
    ProducerFenced {
        producer_id: ProducerId,
    },

    /// Invalid producer epoch.
    InvalidProducerEpoch {
        producer_id: ProducerId,
        expected: ProducerEpoch,
        received: ProducerEpoch,
    },
}
```

Map to wire protocol error codes in `handle_produce()`:
- `DuplicateSequenceNumber` -> return success with cached offset (not an error to client)
- `OutOfOrderSequenceNumber` -> error code 45 (`OUT_OF_ORDER_SEQUENCE_NUMBER`)
- `ProducerFenced` -> error code 57 (`INVALID_PRODUCER_EPOCH`)

### Complete Request Flow (After Implementation)

```
KafkaHandler::handle_produce()
    -> extract producer_id, epoch, sequence from request
    -> produce_with_auto_create()
        -> append_and_get_offset()
            -> HelixService::append_blob()
                -> check leader
                -> CHECK SEQUENCE (NEW)
                    -> if duplicate: return cached offset (don't propose)
                    -> if out of order: return error
                    -> if valid: continue
                -> propose AppendBlobIdempotent to Raft
                -> wait for commit
                -> tick task applies entry (updates producer state)
                -> return offset
```

## Implementation Steps

### Step 1: Core Types
1. Add `ProducerId` and `ProducerEpoch` to `helix-core/src/types.rs`
2. Create `helix-server/src/producer_state.rs` with state tracking structures

### Step 2: Storage Integration
1. Add `IdempotentProducerState` to `PartitionStorage`
2. Add `AppendBlobIdempotent` variant to `PartitionCommand`
3. Implement `check_sequence()` method
4. Update `apply_entry_async()` to handle producer state updates

### Step 3: Request Handling
1. Modify `append_blob()` to accept producer fields
2. Add sequence check before Raft proposal
3. Handle duplicate detection (return cached offset)

### Step 4: `InitProducerId` API
1. Add `AllocateProducerId` command to controller
2. Implement `handle_init_producer_id()` in protocol handler
3. Add to `SUPPORTED_APIS` constant

### Step 5: Testing
1. Unit tests for sequence checking logic
2. Integration test with idempotent producer enabled
3. DST test for retry scenarios

## Design Rationale

**Why check BEFORE Raft proposal?**
- Prevents duplicate entries in Raft log entirely
- No offset gaps - offsets remain contiguous
- Simpler state management - no post-commit reconciliation

**Why store in `PartitionStorage`?**
- Natural fit - producer state is per-partition
- Follows existing patterns (similar to `last_applied`)
- State automatically persisted via Raft log replay

**Why use controller for `InitProducerId`?**
- Ensures cluster-wide unique producer IDs
- Reuses existing controller partition infrastructure
- Consistent with existing Helix patterns for cluster-wide state

**Why minimal approach first?**
- Solves the immediate problem (retry duplicates)
- Can be extended to full 5-batch window later
- Simpler to implement and test

## Current Implementation Status

### Producer ID Allocation (Simplified)

The current implementation uses a **timestamp-based approach** for producer ID allocation:

```rust
// Format: (node_id << 48) | (timestamp_micros & 0x0000_FFFF_FFFF_FFFF)
// - 16 bits for node_id (up to 65k nodes)
// - 48 bits for microsecond timestamp (~8900 years from epoch)
let base_producer_id = (node_id << 48) | (timestamp_micros & 0x0000_FFFF_FFFF_FFFF);
```

**Pros:**
- Simple, no persistence required
- Survives restarts (timestamp always increases)
- No coordination with controller needed

**Cons:**
- Clock skew between nodes could cause issues (unlikely in practice)
- Not suitable for transactional IDs (which need stable mapping)
- Lost producer state on restart (clients must re-init)

### TODO: Robust Implementation

For production use, producer ID allocation should be **persisted in the controller partition**:

1. **Controller stores `next_producer_id` counter** in Raft-replicated state
2. **`InitProducerId` proposes to controller** to allocate ID
3. **Transactional ID mapping** stored in controller for stable IDs

This ensures:
- Cluster-wide uniqueness guaranteed by Raft consensus
- Survives any node restart or failure
- Stable producer IDs for transactions

See "How to Handle `InitProducerId` Request" section above for the target design.

## Critical Files for Implementation

| File | Changes |
|------|---------|
| `helix-core/src/types.rs` | Add `ProducerId` and `ProducerEpoch` using `define_id!` macro |
| `helix-server/src/producer_state.rs` | New file with `IdempotentProducerState` and `SequenceCheckResult` |
| `helix-server/src/partition_storage.rs` | Add producer state field and `check_sequence()` method |
| `helix-server/src/storage.rs` | Add `AppendBlobIdempotent` variant to `PartitionCommand` |
| `helix-server/src/service/handlers/blob.rs` | Modify `append_blob()` to check sequence BEFORE Raft proposal |
| `helix-server/src/kafka/handler.rs` | Add `handle_init_producer_id()` protocol handler (API key 22) |
| `helix-server/src/controller.rs` | Add `AllocateProducerId` command for PID allocation |
| `helix-server/src/error.rs` | Add new error variants for sequence/producer errors |
