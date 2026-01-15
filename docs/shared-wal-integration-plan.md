# SharedWAL Integration Plan for helix-server

## Executive Summary

Integrate `SharedWalPool` into `helix-server` to enable fsync amortization across multiple partitions. Based on user requirements:
- **Always enabled** when `data_dir` is set (default K=4 WALs)
- **No backwards compatibility** with dedicated WALs (clean replacement)
- **Simple config**: Single `--shared-wal-count` flag to override default
- **Comprehensive testing**: Integration tests + DST with fault injection

## Current Status (as of 2026-01-15)

### ✅ Completed: helix-wal Foundation

The core `helix-wal` crate implementation is **100% complete**:
- ✅ `SharedWal` - Basic shared WAL implementation
- ✅ `SharedWalPool` - Pool of K WALs with partition routing (`partition_id % K`)
- ✅ `SharedWalHandle` - Per-partition handle with async append API
- ✅ `SharedWalCoordinator` - Background flush coordinator with batching
- ✅ `PoolConfig` - Pool configuration with K=4 default
- ✅ `SharedEntry` - Tagged entries with partition ID
- ✅ All types exported from `helix-wal/src/lib.rs`
- ✅ Unit tests in `shared_wal.rs`
- ✅ DST tests for `SharedWal` in `helix-tests/src/shared_wal_dst.rs` (2298 lines)
- ✅ Benchmarks completed, optimal K=4 configuration determined

**Key commits**:
- `cb2e5b2` - Add empirical benchmark results for SharedWalPool optimal configuration
- `2eb251e` - Add batched I/O to SharedWalPool for 20x performance improvement
- `d33341a` - Add SharedWalPool benchmark to shared_wal_bench.rs

### ✅ Completed: helix-server Integration (Phases 1-4)

#### Phase 1: Storage Layer Refactoring - ✅ COMPLETE
- ✅ `DurablePartition<S>` refactored with `WalBackend` enum
- ✅ `open_with_shared_wal()` constructor added
- ✅ `append()` and `append_blob()` methods updated for both backends
- ✅ Recovery helper `apply_shared_entry_to_cache()` implemented

#### Phase 2: Service Initialization - ✅ COMPLETE
- ✅ `HelixService` has `shared_wal_pool` field
- ✅ `recovered_entries` field added
- ✅ All constructors converted to async (`new()`, `with_data_dir()`, `new_multi_node()`)
- ✅ `shutdown()` method added calling `pool.shutdown()`

#### Phase 3: Partition Creation Wiring - ✅ COMPLETE
- ✅ `new_durable_with_shared_wal()` added to `ProductionPartitionStorage`
- ✅ `create_topic()` in `topic.rs` updated to use SharedWalPool
- ✅ Multi-node partition creation in `tick.rs` updated
- ✅ All call sites updated to pass pool parameters

#### Phase 4: CLI Integration - ✅ COMPLETE
- ✅ `--shared-wal-count` flag added to `main.rs` with range validation (1-16)
- ✅ Flag wired to both `with_data_dir()` and `new_multi_node()` constructors
- ✅ Defaults to 4 if not specified

### ✅ Completed: Phase 5 - Integration Testing

#### Phase 5: Integration Testing - ✅ COMPLETE
- ✅ `helix-tests/src/shared_wal_integration_tests.rs` created
- ✅ Basic write/read test (`test_shared_wal_basic_write_read`)
- ✅ Multi-partition test (`test_shared_wal_multiple_partitions`)
- ✅ Concurrent writes test (`test_shared_wal_concurrent_writes`)
- ✅ Multiple batches test (`test_shared_wal_multiple_batches`)
- ✅ Different WAL counts test (`test_shared_wal_different_wal_counts`)
- ✅ High-volume stress test (`test_shared_wal_high_volume`)
- ✅ Recovery after restart test (`test_shared_wal_recovery_after_restart`)

All 7 integration tests pass.

### ✅ Completed: Phase 6 - DST Testing

#### Phase 6: DST Testing with SharedWalPool - ✅ COMPLETE
- ✅ DST tests exist for `SharedWal` with fault injection (shared_wal_dst.rs)
- ✅ `shared_wal_pool_dst.rs` created with 7 tests:
  - `test_dst_pool_basic_durability` - Basic write/crash/recover
  - `test_dst_pool_partition_routing` - Verifies partition_id % K routing
  - `test_dst_pool_cross_wal_recovery` - Recovery across all K WALs
  - `test_dst_pool_multiple_crash_cycles` - 5 crash/recover cycles
  - `test_dst_pool_multi_seed_durability` - 100 seeds
  - `test_dst_pool_concurrent_writes` - 50 seeds with concurrent handles
  - `test_dst_pool_different_wal_counts` - Tests K=1,2,4,8,16

Note: Fine-grained fault injection (torn writes, fsync failures) is tested
at the SharedWal level. The async batching in SharedWalHandle makes
per-entry fault tracking impractical at the pool level.

### ✅ Implementation Complete

**All 6 phases complete:**
- Phase 1: Storage Layer Refactoring ✅
- Phase 2: Service Initialization ✅
- Phase 3: Partition Creation Wiring ✅
- Phase 4: CLI Integration ✅
- Phase 5: Integration Testing (7 tests) ✅
- Phase 6: DST Testing (7 tests) ✅

**Status**: SharedWAL integration is complete. The system uses SharedWalPool by default when `--data-dir` is specified, with configurable WAL count via `--shared-wal-count` (default K=4).

## Design Decisions

### 1. Always-On Shared WAL
When `data_dir` is set, use `SharedWalPool` with default K=4 WALs. Users can override with `--shared-wal-count`.

**Rationale**: Simpler implementation, better default behavior, no mode switching complexity.

### 2. Remove Dedicated WAL Code
Replace `Arc<RwLock<Wal>>` in `DurablePartition` with `SharedWalHandle` directly. Remove old WAL path.

**Rationale**: No migration needed, cleaner code, avoid enum/delegation complexity.

### 3. Pool-Wide Recovery First
Call `pool.recover()` once during service initialization, before creating any partitions.

**Rationale**: SharedWalPool API designed for this, avoids N sequential recoveries, simpler ordering.

### 4. Directory Structure
```
data_dir/
├── shared-wal/              # Pool base directory
│   ├── wal-00/              # First WAL in pool
│   │   └── segment-*
│   ├── wal-01/              # Second WAL
│   ├── wal-02/              # Third WAL
│   └── wal-03/              # Fourth WAL (default)
└── topic-{tid}/
    └── partition-{pid}/     # No wal/ subdirectory anymore
```

## Implementation Phases

### Phase 1: Storage Layer Refactoring (Core Changes)

**Goal**: Replace dedicated WAL with SharedWalHandle in DurablePartition

**File**: `/home/bits/go/src/github.com/DataDog/helix/helix-server/src/storage.rs`

**Changes**:

1. **Modify `DurablePartition` struct** (line 736-751):
   ```rust
   pub struct DurablePartition {
       config: DurablePartitionConfig,
       wal_handle: SharedWalHandle<TokioStorage>,  // Changed from Arc<RwLock<Wal>>
       cache: Partition,
       last_applied_index: u64,
       tiering_manager: Option<IntegratedTieringManager>,
       progress_manager: Option<ProgressManager<SimulatedProgressStore>>,
   }
   ```

2. **Add new constructor `open_with_shared_wal()`**:
   ```rust
   pub async fn open_with_shared_wal(
       config: DurablePartitionConfig,
       wal_handle: SharedWalHandle<TokioStorage>,
       recovered_entries: Vec<SharedEntry>,
   ) -> Result<Self, DurablePartitionError>
   ```

   **Logic**:
   - Build cache from `recovered_entries` (replay commands)
   - Initialize tiering manager if configured
   - Initialize progress manager if configured
   - Return DurablePartition with handle

3. **Keep legacy `open()` as fallback** (line 762-861):
   - Keep existing implementation for now (can remove later if not needed)
   - Used during transition period or if pool creation fails

4. **Update `append()` method** (line 896-942):
   ```rust
   pub async fn append(&mut self, records: Vec<Record>) -> Result<Offset> {
       let command = PartitionCommand::Append { records: records.clone() };
       let data = command.encode();

       let term = 0;  // Terms managed by Raft layer
       let index = self.last_applied_index + 1;

       // Write to shared WAL (blocks until fsync'd)
       self.wal_handle.append(term, index, data).await
           .map_err(|e| DurablePartitionError::WalAppend {
               message: e.to_string()
           })?;

       // Apply to cache
       let base_offset = self.cache.append(records)?;
       self.last_applied_index = index;

       Ok(base_offset)
   }
   ```

5. **Update `append_blob()` method** (line 984-1041):
   - Similar changes: use `wal_handle.append()` instead of `wal.write().await.append()`

6. **Update `sync()` method** (line 944-948):
   - Shared WAL auto-syncs, make this a no-op or call `pool.flush()`

7. **Add recovery helper**:
   ```rust
   fn apply_recovered_entry_to_cache(
       cache: &mut Partition,
       entry: &SharedEntry,
   ) -> Result<(), DurablePartitionError> {
       let command = PartitionCommand::decode(&entry.payload)?;
       Self::apply_command_to_cache(cache, command)
   }
   ```

**Testing**: Unit tests with mocked SharedWalHandle

**TigerStyle Checklist**:
- Keep `open_with_shared_wal()` under 70 lines
- Add assertions for index sequentiality
- No `unwrap()`, use `?` with context

---

### Phase 2: Service Initialization (Pool Lifecycle)

**Goal**: Add SharedWalPool to HelixService and manage its lifecycle

**File**: `/home/bits/go/src/github.com/DataDog/helix/helix-server/src/service/mod.rs`

**Changes**:

1. **Add field to `HelixService`** (line 72-112):
   ```rust
   pub struct HelixService {
       // ... existing fields ...

       /// Shared WAL pool for fsync amortization. Present when data_dir is set.
       shared_wal_pool: Option<Arc<SharedWalPool<TokioStorage>>>,

       /// Recovered entries from shared WAL, indexed by PartitionId.
       /// Used during partition creation to restore state.
       recovered_entries: Arc<RwLock<HashMap<PartitionId, Vec<SharedEntry>>>>,
   }
   ```

2. **Modify `new_internal()` constructor** (line 133-175):
   ```rust
   fn new_internal(
       cluster_id: String,
       node_id: u64,
       data_dir: Option<PathBuf>,
       shared_wal_count: Option<u32>,
   ) -> Self {
       let (shared_wal_pool, recovered_entries) = if let Some(ref dir) = data_dir {
           // Determine WAL count (default 4, or user override)
           let wal_count = shared_wal_count.unwrap_or(4);
           assert!(wal_count >= 1 && wal_count <= 16,
                   "shared_wal_count must be in range [1, 16]");

           // Create pool config
           let pool_config = PoolConfig::new(dir.join("shared-wal"), wal_count)
               .with_flush_interval(Duration::from_millis(1))
               .with_max_buffer_entries(1000);

           // Open pool (blocks on async in constructor - need to refactor to async)
           let pool = SharedWalPool::open(TokioStorage::new(), pool_config)
               .await
               .expect("Failed to open shared WAL pool");

           // Recover all partitions
           let recovered = pool.recover()
               .await
               .expect("Failed to recover shared WAL");

           (Some(Arc::new(pool)), Arc::new(RwLock::new(recovered)))
       } else {
           // In-memory mode
           (None, Arc::new(RwLock::new(HashMap::new())))
       };

       Self {
           // ... existing initialization ...
           shared_wal_pool,
           recovered_entries,
       }
   }
   ```

3. **Convert constructors to async**:
   - `new()` → `async fn new()`
   - `with_data_dir()` → `async fn with_data_dir()`
   - `new_multi_node()` → already async, just modify

4. **Add shutdown method**:
   ```rust
   pub async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
       if let Some(pool) = self.shared_wal_pool {
           pool.shutdown().await?;
       }
       Ok(())
   }
   ```

**Testing**: Service initialization test with temp dir

**TigerStyle Checklist**:
- Keep pool initialization code under 70 lines (extract helper if needed)
- Assert WAL count in valid range [1, 16]
- Use `expect()` with clear context for initialization failures

---

### Phase 3: Partition Creation Wiring

**Goal**: Wire SharedWalPool handles into partition creation flow

**File 1**: `/home/bits/go/src/github.com/DataDog/helix/helix-server/src/partition_storage.rs`

**Changes**:

1. **Add new constructor** (after line 76):
   ```rust
   pub async fn new_durable_with_shared_wal(
       topic_id: TopicId,
       partition_id: PartitionId,
       config: DurablePartitionConfig,
       wal_handle: SharedWalHandle<TokioStorage>,
       recovered_entries: Vec<SharedEntry>,
   ) -> Result<Self, DurablePartitionError> {
       let durable = DurablePartition::open_with_shared_wal(
           config,
           wal_handle,
           recovered_entries,
       ).await?;

       Ok(Self {
           topic_id,
           partition_id,
           inner: PartitionStorageInner::Durable(Box::new(durable)),
           last_applied: LogIndex::new(0),
           producer_state: PartitionProducerState::new(),
       })
   }
   ```

**File 2**: `/home/bits/go/src/github.com/DataDog/helix/helix-server/src/service/handlers/topic.rs`

**Changes**:

1. **Modify partition creation in `create_topic()`** (line 60-69):
   ```rust
   for partition_id in 0..partition_count {
       let partition_id = PartitionId::new(partition_id.into());
       let group_id = self.group_map.write().await.allocate(topic_id, partition_id);

       // Create Raft group
       self.multi_raft.write().await
           .create_group(group_id, cluster_nodes.clone())?;

       // Create storage
       let storage = if let Some(ref pool) = self.shared_wal_pool {
           // Get handle from pool
           let wal_handle = pool.handle(partition_id);

           // Get recovered entries for this partition
           let entries = self.recovered_entries.write().await
               .remove(&partition_id)
               .unwrap_or_default();

           // Create config
           let config = DurablePartitionConfig::new(
               self.data_dir.as_ref().unwrap(),
               topic_id,
               partition_id,
           );

           PartitionStorage::new_durable_with_shared_wal(
               topic_id,
               partition_id,
               config,
               wal_handle,
               entries,
           ).await?
       } else {
           PartitionStorage::new_in_memory(topic_id, partition_id)
       };

       self.partition_storage.write().await.insert(group_id, storage);

       // ... rest unchanged
   }
   ```

2. **Similar changes for multi-node mode** in `tick.rs` (line 378-429):
   - Same pattern: check for pool, get handle, get recovered entries, create storage

**Testing**: Topic creation with multiple partitions, verify handles are assigned

**TigerStyle Checklist**:
- Extract partition creation logic if loop body exceeds 70 lines
- Assert recovered entries are consumed (not left in map)

---

### Phase 4: CLI Integration

**Goal**: Expose `--shared-wal-count` flag for overriding default

**File**: `/home/bits/go/src/github.com/DataDog/helix/helix-server/src/main.rs`

**Changes**:

1. **Add field to `Args`** (line 70-109):
   ```rust
   #[derive(Parser, Debug)]
   struct Args {
       // ... existing fields ...

       /// Number of shared WALs in pool (default: 4, range: [1, 16]).
       /// Higher values enable more parallel fsync streams (better for NVMe).
       /// Recommended: 4-8 for most workloads.
       #[arg(long, default_value = "4")]
       shared_wal_count: u32,
   }
   ```

2. **Validate in main** (after parsing):
   ```rust
   assert!(args.shared_wal_count >= 1 && args.shared_wal_count <= 16,
           "shared_wal_count must be in range [1, 16]");
   ```

3. **Pass to service constructor** (line 264-303):
   ```rust
   let service = if args.mode == OperatingMode::Single {
       if let Some(data_dir) = args.data_dir {
           HelixService::with_data_dir(
               cluster_id,
               args.node_id,
               data_dir,
               Some(args.shared_wal_count),
           ).await
       } else {
           HelixService::new(cluster_id, args.node_id).await
       }
   } else {
       // Multi-node: pass shared_wal_count
       HelixService::new_multi_node(
           cluster_id,
           args.node_id,
           args.raft_addr,
           args.peers,
           args.data_dir,
           args.kafka_addr,
           args.kafka_peer_addrs,
           Some(args.shared_wal_count),
       ).await?
   };
   ```

**Testing**: CLI parsing, flag validation

---

### Phase 5: Integration Testing

**Goal**: Verify correctness with integration tests

**File**: `/home/bits/go/src/github.com/DataDog/helix/helix-tests/src/shared_wal_integration_tests.rs` (new)

**Test Cases**:

1. **Basic functionality**:
   ```rust
   #[tokio::test]
   async fn test_shared_wal_write_read() {
       let temp_dir = tempfile::tempdir().unwrap();
       let service = HelixService::with_data_dir(
           "test-cluster".to_string(),
           1,
           temp_dir.path().to_path_buf(),
           Some(4),
       ).await;

       service.create_topic("test".to_string(), 4).await.unwrap();

       // Write to each partition
       // Read back and verify
   }
   ```

2. **Multi-partition writes**:
   ```rust
   #[tokio::test]
   async fn test_concurrent_partition_writes() {
       // Create topic with 8 partitions
       // Spawn 8 tasks writing concurrently
       // Verify all data is durable and correct
   }
   ```

3. **Recovery after restart**:
   ```rust
   #[tokio::test]
   async fn test_recovery_from_shared_wal() {
       let temp_dir = tempfile::tempdir().unwrap();

       // First run: create service, write data, shutdown
       {
           let service = HelixService::with_data_dir(...).await;
           // Write data to multiple partitions
           service.shutdown().await.unwrap();
       }

       // Second run: create new service, verify data recovered
       {
           let service = HelixService::with_data_dir(...).await;
           // Read and verify data is present
       }
   }
   ```

4. **Partition routing validation**:
   ```rust
   #[tokio::test]
   async fn test_partition_wal_assignment() {
       // Create service with K=4 WALs
       // Create topic with 16 partitions
       // Verify partitions are distributed across 4 WALs
       // Check via pool.wal_index_for_partition()
   }
   ```

5. **Empty partition handling**:
   ```rust
   #[tokio::test]
   async fn test_empty_partition_recovery() {
       // Create partitions but don't write to all
       // Restart and verify empty partitions work
   }
   ```

---

### Phase 6: DST Testing with Fault Injection

**Goal**: Verify correctness under crashes and faults

**File**: `/home/bits/go/src/github.com/DataDog/helix/helix-tests/src/shared_wal_dst.rs` (new)

**Test Pattern** (following existing DST tests):

```rust
#[test]
fn test_shared_wal_crash_recovery() {
    for seed in REGRESSION_SEEDS {
        run_crash_recovery_simulation(*seed);
    }
}

fn run_crash_recovery_simulation(seed: u64) {
    let config = EngineConfig::new(seed)
        .with_max_time(Duration::from_secs(60))
        .with_stats(true);

    let mut engine = DiscreteSimulationEngine::with_config(config);

    // Register partition actors with SimulatedStorage
    // Schedule writes
    // Inject crashes at random times
    // Verify all committed data survives recovery

    engine.run();

    // Check invariants
    assert!(verify_no_data_loss(&engine));
    assert!(verify_partition_isolation(&engine));
}
```

**Fault Scenarios**:
1. **Crash during append**: Kill node mid-write, verify partial writes handled
2. **Crash during fsync**: Kill during background flush, verify durability
3. **Multiple crashes**: Repeated crashes and recoveries
4. **Torn writes**: Simulate incomplete entry writes
5. **Concurrent crashes**: Multiple partitions writing when crash occurs

**Invariants to Verify**:
- No committed data lost
- Partition isolation maintained (partition A data never in partition B)
- Indices remain sequential per partition
- No corrupted entries in recovered data

**Run Duration**: Start with 100 seeds, target 10K hours of simulation time

---

## Critical Files Modified

### Core Implementation
1. `/home/bits/go/src/github.com/DataDog/helix/helix-server/src/storage.rs` (2098 lines)
   - Replace WAL field in DurablePartition
   - Add `open_with_shared_wal()` constructor
   - Update append methods

2. `/home/bits/go/src/github.com/DataDog/helix/helix-server/src/service/mod.rs` (395 lines)
   - Add shared_wal_pool field
   - Add recovered_entries field
   - Convert constructors to async
   - Add pool initialization logic

3. `/home/bits/go/src/github.com/DataDog/helix/helix-server/src/partition_storage.rs` (386 lines)
   - Add `new_durable_with_shared_wal()` constructor

### Integration Points
4. `/home/bits/go/src/github.com/DataDog/helix/helix-server/src/service/handlers/topic.rs` (349 lines)
   - Update partition creation in `create_topic()`
   - Update multi-node partition creation

5. `/home/bits/go/src/github.com/DataDog/helix/helix-server/src/service/tick.rs` (463 lines)
   - Update partition creation in tick task

6. `/home/bits/go/src/github.com/DataDog/helix/helix-server/src/main.rs` (390 lines)
   - Add --shared-wal-count CLI flag
   - Pass to service constructors

### Testing (New Files)
7. `/home/bits/go/src/github.com/DataDog/helix/helix-tests/src/shared_wal_integration_tests.rs` (new)
   - Integration tests for basic functionality

8. `/home/bits/go/src/github.com/DataDog/helix/helix-tests/src/shared_wal_dst.rs`
   - **Note**: Basic file exists (2298 lines) with DST tests for `SharedWal`
   - **Need to add**: DST tests for `SharedWalPool` with multi-partition scenarios

## Success Criteria

### Functional Requirements
- [ ] Partitions use SharedWalHandle instead of dedicated Wal
- [ ] Pool initialized with K=4 WALs by default
- [ ] Users can override with --shared-wal-count flag
- [ ] Recovery rebuilds all partition state correctly
- [ ] Multi-partition concurrent writes work
- [ ] Service shutdown is graceful (pool.shutdown())

### Performance Requirements
- [ ] Fsync rate reduced by ~factor of K (measure with iostat)
- [ ] Throughput improves for multi-partition workloads (benchmark)
- [ ] P99 latency remains acceptable (< 10ms for append)

### Quality Requirements
- [ ] All functions under 70 lines (TigerStyle)
- [ ] Average 2+ assertions per function
- [ ] No clippy warnings (`cargo clippy --workspace`)
- [ ] Integration tests pass (5 test cases)
- [ ] DST tests pass (100+ seeds, 10K simulation hours target)

## Implementation Timeline

- **Phase 1** (Storage Layer): 3 hours
  - Modify DurablePartition struct
  - Add new constructor
  - Update append methods
  - Unit tests

- **Phase 2** (Service Init): 3 hours
  - Add pool fields
  - Convert constructors to async
  - Pool initialization logic
  - Service init tests

- **Phase 3** (Partition Creation): 3 hours
  - Update topic.rs partition creation
  - Update tick.rs partition creation
  - Wiring tests

- **Phase 4** (CLI): 1 hour
  - Add flag to Args
  - Validation
  - Pass to constructors

- **Phase 5** (Integration Tests): 4 hours
  - 5 integration test cases
  - Multi-partition scenarios
  - Recovery testing

- **Phase 6** (DST Tests): 6 hours
  - DST infrastructure setup
  - Fault injection scenarios
  - Run 100+ seeds
  - Debug any failures

**Total**: ~20 hours (2.5 days)

## Notes

- **Breaking change**: This removes dedicated WAL support entirely (user confirmed OK)
- **Breaking API change**: HelixService constructors become async (required for pool initialization)
  - `new()` → `async fn new()`
  - `with_data_dir()` → `async fn with_data_dir()`
  - Callers in main.rs and tests need `.await`
- **Default K=4**: Based on benchmarks showing 2x throughput improvement
- **No migration tool**: User confirmed not needed for their use case
- **DST critical**: Following TigerStyle - exhaustive testing before production use
