# Helix Development Guide

## Project Overview

Helix is a Kafka replacement built with Deterministic Simulation Testing (DST) using the Bloodhound framework. The goal is correctness-first infrastructure that can be exhaustively tested.

### Crate Structure

- `helix-core` - Shared types, IDs, limits, records
- `helix-wal` - Write-ahead log with CRC checksums
- `helix-raft` - Raft consensus implementation (TLA+ verified)
- `helix-partition` - Kafka-style partitions, producer, consumer, replication
- `helix-runtime` - Production runtime with async timers
- `helix-tests` - Deterministic simulation tests using Bloodhound

## TigerStyle for Rust

This project follows TigerStyle principles adapted for Rust. See `TIGER_STYLE.md` for the full philosophy.

### Core Principles

**Safety > Performance > Developer Experience** (in that order)

### Explicit Limits

Put a limit on everything. All loops, queues, and buffers must have fixed upper bounds.

```rust
// GOOD: Explicit limit
for i in 0..self.config.max_segments {
    // ...
}

// BAD: Unbounded
loop {
    // ...
}
```

Define limits in `helix-core/src/limits.rs` and use them consistently:

```rust
pub struct Limits {
    pub segment_size_bytes_max: u64,
    pub batch_records_count_max: u32,
    // ...
}
```

### Assertions

**Target: Average 2 assertions per function.** Assert preconditions, postconditions, and invariants.

```rust
pub fn read(&self, offset: Offset, max_records: u32) -> Result<Vec<Record>> {
    // Preconditions
    assert!(max_records > 0, "max_records must be positive");
    assert!(max_records <= self.config.limits.batch_records_count_max);

    // ... implementation ...

    // Postcondition
    assert!(records.len() <= max_records as usize);
    Ok(records)
}
```

**Pair assertions**: Assert the same property in multiple places (e.g., before write AND after read).

**Assert compile-time constants**:
```rust
const_assert!(BATCH_HEADER_SIZE == 33);
const_assert!(MAX_RECORD_SIZE <= SEGMENT_SIZE);
```

### Explicit Types

Use explicitly-sized types. Avoid `usize` except for slice indexing.

```rust
// GOOD
count: u32,
offset: u64,
size_bytes: u64,

// BAD
count: usize,  // Architecture-dependent
```

### Function Length

**Hard limit: 70 lines per function.** This ensures functions fit on one screen.

Split large functions by:
1. Extracting helper functions for non-branching logic
2. Keeping control flow (if/match) in the parent function
3. Keeping state manipulation in the parent, use helpers for computation

### Control Flow

- **No recursion** - All executions must be bounded
- **Simple, explicit control flow** - Minimize nesting depth
- **Handle all errors** - No `unwrap()` in production code, use `expect()` with context

```rust
// GOOD: Explicit error handling
let value = map.get(&key).ok_or(Error::NotFound { key })?;

// BAD: Silent panic
let value = map.get(&key).unwrap();
```

### Memory Allocation

Prefer static allocation. Minimize dynamic allocation after initialization.

```rust
// GOOD: Pre-allocated with capacity
let mut segments: Vec<Segment> = Vec::with_capacity(config.max_segments);

// BAD: Growing unbounded
let mut segments: Vec<Segment> = Vec::new();
```

### Naming Conventions

- Use `snake_case` for functions, variables, modules
- Use descriptive names - no abbreviations
- Add units/qualifiers last: `latency_ms_max`, `size_bytes`, `count_records`
- Related names should have equal length for alignment

```rust
// GOOD: Units and alignment
let source_offset: u64;
let target_offset: u64;

// BAD: Inconsistent
let src_off: u64;
let dest_offset: u64;
```

### Comments

- Comments explain **WHY**, not what
- Use doc comments (`///`) for public API
- Comments are complete sentences with punctuation

```rust
/// Reads records starting at the given offset.
///
/// Returns at most `max_records` records. The offset must be within
/// the valid range [log_start_offset, log_end_offset).
///
/// # Why this design
///
/// We read into a pre-allocated buffer to avoid allocation in the hot path.
/// The caller owns the buffer lifetime, preventing use-after-free.
pub fn read(&self, offset: Offset, max_records: u32) -> Result<Vec<Record>> {
```

### Line Length

**Hard limit: 100 columns.** Nothing should require horizontal scrolling.

### No Unsafe Code

```rust
#![forbid(unsafe_code)]
```

### Compiler Warnings

Enable all warnings and treat them as errors:

```rust
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
```

## Testing Requirements

### Unit Tests

Every public function should have tests covering:
1. Happy path
2. Edge cases (empty input, max values, boundaries)
3. Error conditions

### Deterministic Simulation Testing

Use Bloodhound for simulation tests:
- All randomness through seeded RNG
- Inject network partitions, delays, crashes
- Run thousands of iterations with different seeds
- Verify invariants hold under all conditions

### Test Naming

```rust
#[test]
fn test_<function>_<scenario>() {
    // test_read_empty_partition
    // test_append_at_capacity_limit
    // test_commit_without_position
}
```

## Code Review Checklist

Before submitting code:

- [ ] All functions under 70 lines
- [ ] Average 2+ assertions per function
- [ ] No `usize` except for slice indexing
- [ ] No `unwrap()` in production code
- [ ] All loops have explicit bounds
- [ ] Comments explain why, not what
- [ ] Line length under 100 columns
- [ ] All tests pass
- [ ] No new compiler warnings

## Architecture Decisions

### Why Raft for Partition Replication?

Raft provides:
- Linearizable writes through leader
- Automatic leader election
- Log compaction (via snapshots)
- Well-understood correctness properties (TLA+ verified)

### Why Not Async in Core?

The core Raft and partition code is synchronous state machines. Benefits:
- Deterministic testing (no async runtime in tests)
- Easier reasoning about state transitions
- Async only at the edges (network, timers)

### Why Explicit Offset Types?

`Offset`, `LogIndex`, `TermId` are newtype wrappers preventing:
- Mixing incompatible values
- Accidental arithmetic errors
- Better documentation through types
