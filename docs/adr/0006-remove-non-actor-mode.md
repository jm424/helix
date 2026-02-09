# ADR-0006: Remove Non-Actor Mode

## Status

Accepted

## Context

Helix had two parallel architectures for multi-node partition management:

1. **Lock-based mode (non-actor)**: Uses `Arc<RwLock<MultiRaft>>` for all partition
   groups, with `tick_task_multi_node` processing ticks and outputs for every
   group under a single lock. A `BatcherHandle` serializes produce requests.

2. **Actor-based mode**: Each data partition gets a dedicated `PartitionActor`
   with its own Raft state machine, communicating via channels. The controller
   partition still uses `MultiRaft` via a separate `tick_task_controller`. An
   `OutputProcessor` handles committed entries from all actors.

Both modes were wired through an `actor_mode: bool` flag that threaded through
constructors, tick tasks, topic creation handlers, and the Kafka protocol
handler. This created duplicated initialization paths, conditional branches in
hot paths, and a maintenance burden where every change to multi-node behavior
had to be validated against both architectures.

Actor mode was introduced to eliminate lock contention as partition counts grow,
and had become the well-tested production path with 17 E2E correctness tests,
7 controller failover tests, and 10+ DST simulation tests covering crashes,
storage faults, network partitions, and stress scenarios.

## Decision

Remove non-actor mode entirely. Actor-based architecture becomes the only
multi-node path. Single-node mode (used for development and simple tests)
continues to use the direct `MultiRaft` path since it does not need actor
infrastructure.

Specifically:

- Remove `actor_mode: bool` field from `HelixService`
- Remove `is_actor_mode()` method
- Remove `enable_actor_mode` parameter from all constructors
- Remove `--actor-mode` CLI flag
- Delete `tick_task_multi_node`, `tick_task_multi_node_dst`, and
  `process_outputs_multi_node` from tick.rs
- Remove non-actor branches from DST `HelixServiceActor` and test configs
- Remove `actor_mode` from workload test executor

## Consequences

**Benefits:**

- ~500-700 lines of duplicated code removed
- Single initialization path for multi-node: easier to reason about and debug
- No more `actor_mode` flag threading through constructors and handlers
- Reduced cognitive load: contributors only need to understand one architecture
- Test configs are simpler with fewer boolean flags

**Tradeoffs:**

- Single-node mode still uses lock-based `MultiRaft` directly (not actors),
  creating a small divergence between single-node and multi-node paths
- DST tests that previously tested non-actor mode now implicitly test actor
  mode; the non-actor code path is no longer exercised at all
- Any future need to revert to lock-based mode would require re-implementing
  the removed code (unlikely given actor mode's maturity)

## Options Considered

### Option 1: Remove non-actor mode entirely (chosen)

**Pros:**
- Maximum code simplification
- Single architecture to maintain and test
- Actor mode is already the well-tested production path

**Cons:**
- Loses the simpler lock-based path as a fallback
- Small risk if actor mode has undiscovered issues

### Option 2: Keep both modes (status quo)

**Pros:**
- Lock-based mode serves as a simpler fallback
- Some DST tests have broader fault coverage in non-actor mode

**Cons:**
- Ongoing maintenance burden for two parallel architectures
- Every feature must be implemented and tested twice
- `actor_mode` flag creates complexity throughout the codebase

### Option 3: Keep single-node non-actor, remove multi-node non-actor

**Pros:**
- Preserves simple single-node path
- Removes the most complex duplication (multi-node)

**Cons:**
- Still leaves conditional logic for actor vs non-actor in some handlers
- This is essentially what we chose, since single-node already uses the
  direct `MultiRaft` path without actors
