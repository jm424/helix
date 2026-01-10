# Helix Development Guide

## Project Overview

Helix is a Kafka replacement built with Deterministic Simulation Testing (DST) using the Bloodhound framework. The goal is correctness-first infrastructure that can be exhaustively tested.

### Crate Structure

- `helix-core` - Shared types, IDs, limits, records
- `helix-wal` - Write-ahead log with CRC checksums (durable storage)
- `helix-raft` - Raft consensus implementation (TLA+ verified)
- `helix-routing` - Shard routing and leader caching
- `helix-runtime` - Production runtime with async timers
- `helix-server` - gRPC API server
- `helix-tests` - Deterministic simulation tests using Bloodhound

## Implementation Plan

**IMPORTANT**: All development must follow the implementation plan at `/Users/jai.menon/expt/helix-implementation-plan.md`.

### Before Starting Any Work

1. **Read the implementation plan** to understand the current phase
2. **Check `IMPLEMENTATION_STATUS.md`** for what's done vs remaining
3. **Complete current phase before moving to next** - don't skip ahead
4. **Testing milestones are mandatory** - don't mark a phase complete without them

### Phase Order (DO NOT SKIP)

```
Phase 0: Foundations (TLA+ specs, core types, Bloodhound integration)
    ↓
Phase 1: Core Consensus (WAL, Raft state machine, 10K hours simulation)
    ↓
Phase 2: Multi-Raft & Sharding (scale to thousands of groups)
    ↓
Phase 3: Storage Features (tiering, progress tracking)
    ↓
Phase 4: API & Flow Control (gRPC, Kafka proxy)
    ↓
Phase 5: Production Readiness (observability, load testing)
```

### Testing Requirements Per Phase

Each phase has specific testing milestones that MUST be completed:

**Phase 1 Critical Milestone**:
> "Bloodhound: 10,000 hours with random faults, zero safety violations"

This means running extensive simulation tests with:
- Network partitions
- Node crashes
- Message delays
- Disk faults

And verifying these properties ALWAYS hold:
- `SingleLeaderPerTerm` - At most one leader per term
- `LogMatching` - Same index/term means identical logs
- `LeaderCompleteness` - Committed entries in all future leaders

### What NOT to Do

- Do NOT build gRPC API before Multi-Raft is complete
- Do NOT skip Bloodhound simulation testing
- Do NOT create new crates not in the plan without discussion
- Do NOT mark phases complete without testing milestones

### Deferred Items

These items from the plan are intentionally deferred:
- CI pipeline setup (run tests locally for now)
- io_uring storage implementation (use standard file I/O)

## TLA+ Model Checking

The Raft consensus implementation has a TLA+ specification in `specs/raft.tla` that includes:
- Pre-vote extension (prevents disruption from partitioned nodes)
- Leadership transfer (graceful handoff via TimeoutNow)

### Running the TLC Model Checker

The TLA+ tools jar is located in the `specs/` directory. To run the model checker:

```bash
cd specs
java -XX:+UseParallelGC -Xmx4g -jar tla2tools.jar -workers auto -config raft.cfg raft.tla
```

The model checker will run until completion or you stop it. For quick verification, running until 10-50 million states is usually sufficient. The checker verifies:
- `SingleLeaderPerTerm` - at most one leader per term
- `LogMatching` - identical prefixes for entries with same index/term
- `StateMachineSafety` - no two different values committed at same index

### After Making Raft Changes

When modifying the Raft implementation, update `specs/raft.tla` to match and run the model checker to verify correctness. Any safety violation will be reported with a counterexample trace.

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

**Running clippy**: Before committing, ensure no clippy warnings:

```bash
cargo clippy --workspace
```

Common fixes:
- Add `const fn` where clippy suggests
- Use inline format args: `"{var}"` instead of `"{}", var`
- Add `# Errors` and `# Panics` doc sections for fallible functions
- Use `#[allow(clippy::cast_possible_truncation)]` with a safety comment when casts are bounded by limits
- Add backticks around code references in doc comments (e.g., `` `TigerStyle` ``)
- For generated code (e.g., protobuf), suppress warnings at module level:
  ```rust
  #![allow(clippy::all)]
  #![allow(clippy::pedantic)]
  #![allow(clippy::nursery)]
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
- [ ] `cargo clippy --workspace` reports no warnings

## Architecture Decision Records

Significant architectural decisions are documented in `docs/adr/`. See [docs/adr/README.md](docs/adr/README.md) for the full index.

Key decisions:
- [ADR-0001](docs/adr/0001-raft-for-partition-replication.md): Use Raft for Partition Replication
- [ADR-0002](docs/adr/0002-synchronous-state-machines.md): Synchronous State Machines in Core
- [ADR-0003](docs/adr/0003-strongly-typed-identifiers.md): Strongly-Typed Identifiers
- [ADR-0004](docs/adr/0004-tick-based-raft-timing.md): Tick-Based Raft Timing

### When to Write an ADR

Write an ADR when:
- Choosing between multiple viable approaches
- Making a decision that would be costly to reverse
- The decision affects multiple components
- Future developers will ask "why did we do it this way?"

### ADR Process

1. Copy `docs/adr/0000-template.md` to `docs/adr/NNNN-title.md`
2. Fill in Context, Decision, Consequences, and Options Considered
3. Set status to "Proposed"
4. After review/discussion, update status to "Accepted"
5. Update `docs/adr/README.md` index

### ADR Quality Checklist

- [ ] Context explains the forces at play (not just the problem)
- [ ] Decision is clear and actionable
- [ ] Consequences include both benefits AND tradeoffs
- [ ] At least 2 alternatives considered with pros/cons
- [ ] References link to relevant resources
