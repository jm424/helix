# io_uring Implementation Plan for Helix WAL

## Executive Summary

This document outlines the plan for implementing high-performance io_uring support in helix-wal, based on research from TigerBeetle, ScyllaDB, RocksDB, and recent academic work on database I/O optimization.

### Scope

**In scope (execution-level improvements):**
- io_uring as drop-in replacement for tokio::fs (same semantics)
- Registered buffers (kernel-side optimization)
- Linked SQEs where semantically equivalent
- Reduced syscall/thread-pool overhead
- Integration with existing `BufferedWal` flush intervals
- Tuning flush intervals for io_uring characteristics

**Out of scope (architectural changes for later consideration):**
- Shared WAL across partitions (multiple Raft groups sharing one WAL)
- Group commit across unrelated operations
- Fundamentally changing durability architecture

### Performance Target

**Realistic: 20-50% throughput improvement** over TokioStorage with identical durability semantics.

This is not the 11x improvement cited in research papers - that comparison was against synchronous blocking I/O. Our baseline (tokio::fs) already uses async I/O via thread pool.

## Research Summary

### Key Sources

1. **[High-Performance DBMSs with io_uring](https://arxiv.org/abs/2512.04859)** (Dec 2025) - Comprehensive study showing 11x throughput improvement for out-of-memory OLTP workloads
2. **[TigerBeetle's I/O Abstraction](https://tigerbeetle.com/blog/2022-11-23-a-friendly-abstraction-over-iouring-and-kqueue/)** - Single-threaded event loop with callback-based completions
3. **[ScyllaDB io_uring Integration](https://www.scylladb.com/2020/05/05/how-io_uring-and-ebpf-will-revolutionize-programming-in-linux/)** - 50% improvement via Seastar framework
4. **[RocksDB Async I/O](https://rocksdb.org/blog/2022/10/07/asynchronous-io-in-rocksdb.html)** - Coroutine-based MultiGet with io_uring
5. **[tokio-uring Design](https://github.com/tokio-rs/tokio-uring/blob/master/DESIGN.md)** - Thread-per-core model with buffer ownership

### Key Patterns from Production Systems

| System | Architecture | Key Pattern |
|--------|--------------|-------------|
| TigerBeetle | Single-threaded event loop | Zero-syscall batched I/O, unified storage+network |
| ScyllaDB | Thread-per-core (Seastar) | Direct I/O, linux-aio migration to io_uring |
| RocksDB | Worker thread pool | Coroutine-based async with io_uring MultiRead |
| PostgreSQL 18 | Worker threads | Fixed buffer pools, shared rings |

### Performance Findings

From the arxiv paper (AMD 3.7GHz, 8x PCIe 5.0 NVMe):

| Configuration | Throughput | Improvement |
|--------------|------------|-------------|
| Synchronous baseline | 16.5k tx/s | - |
| Batched writes | 18.8k tx/s | +14% |
| User-level fibers (async) | 183k tx/s | **11x** |
| + Registered buffers | 203k tx/s | +11% |
| + IOPOLL | 246k tx/s | +21% |
| + SQPOLL (dedicated core) | 546k tx/s | +32% |

### When io_uring Helps vs. Doesn't Help

**Best for:**
- I/O-bound workloads (>70% page fault rate)
- WAL with fsync batching (amortizes syscall cost)
- High write throughput with durability requirements

**Avoid when:**
- Cache-resident workloads (I/O < 10% of execution time)
- Small message network I/O (< 1KB)
- Latency-critical with unpredictable I/O patterns

---

## Current State

### What Exists

```
helix-wal/src/storage/
├── mod.rs              # Storage/StorageFile traits (Send + Sync required)
├── tokio_storage.rs    # TokioStorage (thread-pool based async)
├── simulated.rs        # SimulatedStorage (for DST)
└── io_uring_storage.rs # Basic IoUringStorage (!Send, !Sync)
```

### The Problem

`tokio_uring` is fundamentally single-threaded (uses `Rc` internally), but our `Storage` trait requires `Send + Sync`:

```rust
#[async_trait]
pub trait Storage: Send + Sync { ... }

#[async_trait]
pub trait StorageFile: Send + Sync { ... }
```

The existing `IoUringStorage` mirrors the trait API but cannot implement the traits.

---

## Architecture Options

### Option A: Worker Thread Pattern (Recommended)

**Pattern**: Spawn dedicated io_uring thread(s), communicate via channels.

```
┌─────────────────────────────────────────────────────────┐
│                    Tokio Runtime                        │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                 │
│  │ Task 1  │  │ Task 2  │  │ Task 3  │  ...            │
│  └────┬────┘  └────┬────┘  └────┬────┘                 │
│       │            │            │                       │
│       └────────────┼────────────┘                       │
│                    │ mpsc channel                       │
│                    ▼                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │           IoUringWorker (Send + Sync)           │   │
│  │  ┌─────────────────────────────────────────┐    │   │
│  │  │ Worker Thread (tokio_uring::start)      │    │   │
│  │  │                                         │    │   │
│  │  │  IoUringStorage + IoUringFile (!Send)   │    │   │
│  │  │                                         │    │   │
│  │  │  Submission Queue ←→ Completion Queue   │    │   │
│  │  └─────────────────────────────────────────┘    │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

**Pros:**
- Clean integration with existing multi-threaded tokio
- Implements `Storage`/`StorageFile` traits
- Natural batching via channel buffering
- Single io_uring instance = simpler debugging

**Cons:**
- Channel overhead (~100-200ns per operation)
- Single worker can become bottleneck
- Cross-thread memory copies

**Used by:** PostgreSQL (worker threads), RocksDB (thread pool)

### Option B: Thread-Per-Core

**Pattern**: Each tokio worker thread owns an io_uring instance.

```
┌──────────────────────────────────────────────────────────┐
│                    Tokio Runtime                         │
│                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │   Core 0     │  │   Core 1     │  │   Core 2     │   │
│  │              │  │              │  │              │   │
│  │ io_uring[0] │  │ io_uring[1] │  │ io_uring[2] │   │
│  │              │  │              │  │              │   │
│  │ Tasks pinned│  │ Tasks pinned│  │ Tasks pinned│   │
│  │ to core     │  │ to core     │  │ to core     │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
└──────────────────────────────────────────────────────────┘
```

**Pros:**
- Zero cross-thread communication
- Maximum throughput potential
- Natural affinity with NVMe queues

**Cons:**
- Requires tokio runtime changes or custom runtime
- Load balancing complexity
- Each partition must be pinned to a core
- Incompatible with current `Storage` trait design

**Used by:** TigerBeetle, ScyllaDB (Seastar)

### Option C: Hybrid (Future Enhancement)

Combine both: worker threads for general use, thread-per-core for hot partitions.

---

## Recommended Implementation: Option A (Worker Thread)

### Phase 1: Basic Worker Implementation

Create `io_uring_worker.rs` with:

```rust
/// Commands sent to the io_uring worker thread.
enum IoCommand {
    Open { path: PathBuf, reply: oneshot::Sender<WalResult<FileHandle>> },
    WriteAt { handle: FileHandle, offset: u64, data: Bytes, reply: oneshot::Sender<WalResult<()>> },
    ReadAt { handle: FileHandle, offset: u64, len: usize, reply: oneshot::Sender<WalResult<Bytes>> },
    Sync { handle: FileHandle, reply: oneshot::Sender<WalResult<()>> },
    Close { handle: FileHandle },
    // Batch operations for efficiency
    WriteBatch { ops: Vec<WriteOp>, reply: oneshot::Sender<WalResult<()>> },
}

/// Send + Sync wrapper that implements Storage trait.
pub struct IoUringWorkerStorage {
    command_tx: mpsc::Sender<IoCommand>,
    // Worker thread handle for cleanup
    worker_handle: Option<std::thread::JoinHandle<()>>,
}

impl Storage for IoUringWorkerStorage { ... }
```

### Phase 2: Execution Optimizations

#### Registered Buffers (+11% throughput)

Pre-register memory buffers with the kernel to avoid per-operation page pinning:

```rust
impl IoUringWorker {
    fn setup_registered_buffers(&mut self, count: usize, size: usize) {
        // Allocate aligned buffers
        // Register with io_uring
        // Use buffer indices in operations
    }
}
```

#### Linked Operations (Semantically Equivalent)

Chain write + fsync as linked SQEs - same durability, fewer syscalls:

```rust
// Submit as linked: write completes → fsync runs (single io_uring_enter)
sqe1.flags |= IOSQE_IO_LINK;
ring.submit_write(sqe1);
ring.submit_fsync(sqe2);
```

This is semantically equivalent to separate write+fsync calls but reduces syscall overhead.

#### SQPOLL Mode (Deferred)

SQPOLL dedicates a CPU core to poll the submission queue (+32% in benchmarks).
Deferred because it requires operational decisions about CPU allocation.

---

## Implementation Phases

### Phase 1: Core Worker Implementation

**Goal:** `IoUringWorkerStorage` that implements `Storage` trait with identical semantics to `TokioStorage`.

Files to create:
- `helix-wal/src/storage/io_uring_worker.rs`

Tasks:
1. [ ] Define `IoCommand` enum for all operations
2. [ ] Implement worker thread with `tokio_uring::start()`
3. [ ] Implement `IoUringWorkerStorage` with `Storage` trait
4. [ ] Implement `IoUringWorkerFile` with `StorageFile` trait
5. [ ] Add graceful shutdown handling
6. [ ] Port existing `io_uring_storage.rs` tests
7. [ ] Verify all existing WAL tests pass with new backend

### Phase 2: Registered Buffers

**Goal:** Eliminate per-operation memory pinning overhead.

Tasks:
1. [ ] Implement buffer pool with pre-registered memory
2. [ ] Add buffer acquisition/release logic
3. [ ] Update read/write paths to use registered buffers
4. [ ] Benchmark: registered vs. unregistered buffers

### Phase 3: Benchmarking & Validation

**Goal:** Quantify improvements and validate correctness.

#### Correctness Tests

Run existing WAL tests with io_uring backend on Linux:
1. [ ] `cargo test --features io-uring -p helix-wal` - all unit tests pass
2. [ ] WAL crash recovery tests - identical behavior to TokioStorage
3. [ ] Segment rotation tests - files created/managed correctly
4. [ ] BufferedWal integration - flush intervals work correctly

#### Benchmarks

Integrate with existing benchmarks in `helix-wal/benches/`:
1. [ ] `wal_write.rs` - compare TokioStorage vs IoUringWorkerStorage
2. [ ] `wal_read.rs` - read throughput comparison
3. [ ] `buffered_wal_bench.rs` - batched write performance

Benchmark scenarios:
- Sequential writes with fsync-per-write (worst case for io_uring overhead)
- Sequential writes with periodic fsync (expected sweet spot)
- Random reads
- Mixed read/write workloads

#### Validation Checklist

- [ ] All existing tests pass with `--features io-uring`
- [ ] No data corruption under concurrent access
- [ ] Graceful fallback works (test on Docker without io_uring)
- [ ] 20-50% throughput improvement target met (or understand why not)
- [ ] Update `docs/BENCHMARKS.md` with io_uring results
- [ ] Update `IMPLEMENTATION_STATUS.md`

### Future (Out of Scope for Now)

These require architectural decisions:
- Shared WAL across partitions (multiple Raft groups sharing one WAL)
- Group commit across unrelated operations
- SQPOLL mode (dedicates CPU core, operational decision)
- Thread-per-core architecture (requires runtime changes)

---

## API Design

### Storage Factory

```rust
/// Creates the appropriate storage backend for the platform.
///
/// Feature-flag driven: compiling with `io-uring` feature on Linux
/// automatically enables io_uring (with fallback on init failure).
pub fn create_storage() -> Box<dyn Storage> {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        match IoUringWorkerStorage::try_new() {
            Ok(storage) => return Box::new(storage),
            Err(e) => tracing::warn!("io_uring init failed ({e}), using tokio::fs"),
        }
    }
    Box::new(TokioStorage::new())
}
```

### io_uring Worker Defaults

Until a config system exists, use sensible defaults:

```rust
impl IoUringWorkerStorage {
    /// Default SQ entries (power of 2).
    const DEFAULT_SQ_ENTRIES: u32 = 256;

    /// Default registered buffer count.
    const DEFAULT_REGISTERED_BUFFERS: usize = 64;

    /// Default buffer size (matches typical WAL entry size).
    const DEFAULT_BUFFER_SIZE: usize = 64 * 1024; // 64 KB
}
```

**Note:** These can be made configurable later when Helix config system is designed.

### Portable Baseline (Default)

`TokioStorage` remains the default, portable storage backend:
- Works on all platforms (macOS, Linux, Windows)
- Primary development and testing target (macOS)
- No feature flags required
- Always available as fallback

### io_uring via Feature Flag (Linux Production)

io_uring is enabled by compiling with the `io-uring` feature:

```toml
# Cargo.toml
[features]
default = []  # io_uring NOT included by default
io-uring = ["dep:tokio-uring"]  # Explicit opt-in
```

```bash
# Development (macOS) - portable baseline
cargo build
cargo test

# Production (Linux) - with io_uring
cargo build --features io-uring
```

### Storage Selection Logic

```rust
/// Creates the appropriate storage backend.
///
/// On Linux with `io-uring` feature: tries io_uring, falls back on failure.
/// Otherwise: uses portable TokioStorage.
pub fn create_storage() -> Box<dyn Storage> {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        match IoUringWorkerStorage::try_new() {
            Ok(storage) => {
                tracing::info!("using io_uring storage backend");
                return Box::new(storage);
            }
            Err(e) => {
                tracing::warn!("io_uring init failed ({e}), falling back to tokio::fs");
            }
        }
    }

    tracing::info!("using tokio::fs storage backend");
    Box::new(TokioStorage::new())
}
```

### Platform Behavior Matrix

| Platform | Without feature | With `--features io-uring` |
|----------|-----------------|---------------------------|
| macOS | TokioStorage | TokioStorage (feature ignored) |
| Linux | TokioStorage | IoUringWorkerStorage |
| Linux (init fail) | TokioStorage | TokioStorage (fallback) |

**Note:** Runtime configuration (tuning io_uring parameters) deferred until Helix config system is designed.

---

## Testing Strategy

### Unit Tests

1. All existing `TokioStorage` tests must pass with `IoUringWorkerStorage`
2. Test graceful degradation on non-Linux platforms
3. Test worker shutdown with in-flight operations
4. Test batch timeout behavior

### Integration Tests

1. Run full WAL append/read/sync cycles
2. Test crash recovery with io_uring storage
3. Test concurrent access from multiple tasks

### Benchmarks

Create `benches/io_uring_bench.rs`:

```rust
// Compare storage backends
fn bench_storage_backends(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage");

    // TokioStorage baseline
    group.bench_function("tokio", |b| { ... });

    // IoUringWorkerStorage
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    group.bench_function("io_uring", |b| { ... });

    // IoUringWorkerStorage with batching
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    group.bench_function("io_uring_batched", |b| { ... });
}
```

---

## Security Considerations

### Docker Compatibility

io_uring syscalls are blocked by default in Docker. Document:

```bash
# Enable io_uring in Docker (requires --privileged or specific seccomp profile)
docker run --security-opt seccomp=unconfined ...
```

### Kernel Requirements

- Minimum: Linux 5.1 (basic io_uring)
- Recommended: Linux 5.6+ (SQPOLL, registered buffers)
- Optimal: Linux 5.11+ (improved performance)

---

## Success Criteria

1. **Correctness**: All WAL tests pass with io_uring backend (identical semantics)
2. **Performance**: 20-50% throughput improvement over TokioStorage (same durability)
3. **Graceful degradation**: Automatic fallback on unsupported systems
4. **Observability**: Metrics for queue depth, batch sizes, latencies
5. **No durability changes**: fsync behavior identical to TokioStorage

---

## Open Questions

1. **Single vs. Multiple Workers**: Should we support multiple worker threads for scalability? (Start with single, measure if bottleneck)
2. **Direct I/O**: Should we support O_DIRECT for bypassing page cache? (Requires aligned buffers, deferred)
3. **Thread-per-core**: Is it worth adding as a future option for extreme performance? (Requires runtime changes, deferred)
4. **Channel backpressure**: How to handle slow consumer (worker overloaded)? Bounded channel with block vs. drop?

---

## References

- [io_uring for High-Performance DBMSs (2025)](https://arxiv.org/abs/2512.04859)
- [TigerBeetle I/O Abstraction](https://tigerbeetle.com/blog/2022-11-23-a-friendly-abstraction-over-iouring-and-kqueue/)
- [ScyllaDB io_uring](https://www.scylladb.com/2020/05/05/how-io_uring-and-ebpf-will-revolutionize-programming-in-linux/)
- [RocksDB Async I/O](https://rocksdb.org/blog/2022/10/07/asynchronous-io-in-rocksdb.html)
- [tokio-uring Design](https://github.com/tokio-rs/tokio-uring/blob/master/DESIGN.md)
- [PostgreSQL io_uring](https://medium.com/@aditimishra_541/postgresqls-new-i-o-revolution-unlocking-database-performance-with-io-uring-d31d40ab60a0)
