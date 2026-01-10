# ADR-0002: Synchronous State Machines in Core

## Status

Accepted

## Context

Helix uses Deterministic Simulation Testing (DST) via the Bloodhound framework to exhaustively test distributed system behavior. DST requires deterministic execution - the same inputs must produce the same outputs.

The core components (Raft consensus, partition management) need to be testable under simulation with:
- Injected network partitions and delays
- Controlled timer expirations
- Reproducible failure scenarios

## Decision

Implement core Raft and partition logic as synchronous state machines. Async I/O (network, disk, timers) is handled only at the edges by the runtime layer.

The pattern is:
```rust
// Core: pure state machine
impl RaftNode {
    pub fn handle_message(&mut self, msg: Message) -> Vec<RaftOutput> {
        // Deterministic: same input → same output
    }
}

// Runtime: async wrapper
impl RaftServer {
    async fn run(&mut self) {
        let msg = self.network.recv().await;  // Async at edge
        let outputs = self.raft.handle_message(msg);  // Sync core
        for output in outputs {
            self.handle_output(output).await;  // Async at edge
        }
    }
}
```

## Consequences

**Easier:**
- Deterministic testing without async runtime in tests
- Easier reasoning about state transitions
- Can run thousands of simulated scenarios quickly
- State machine can be formally verified
- No async cancellation complexity in core logic

**Harder:**
- Must manually thread async context through runtime
- Can't use async libraries directly in core
- Slightly more boilerplate at runtime layer
- Must be careful not to block in "sync" code

## Options Considered

### Option 1: Synchronous State Machines (Chosen)

**Pros:**
- Deterministic execution for DST
- Simpler reasoning about correctness
- Matches TigerBeetle and FoundationDB approach
- Easy to unit test without async runtime

**Cons:**
- Async/sync boundary requires care
- Can't use async ecosystem in core

### Option 2: Fully Async with Deterministic Runtime

**Pros:**
- Natural async Rust idioms everywhere
- Can use async libraries directly

**Cons:**
- Determinism requires custom async runtime
- Harder to reason about interleaving
- tokio-test has limitations for complex scenarios
- Async cancellation adds complexity

### Option 3: Actor Model (e.g., Actix)

**Pros:**
- Natural message-passing model
- Built-in concurrency

**Cons:**
- Framework lock-in
- Harder to make deterministic
- More indirection

## References

- [TigerBeetle Design](https://tigerbeetle.com/blog/2020-07-16-why-we-built-tigerbeetle/)
- [FoundationDB Testing](https://apple.github.io/foundationdb/testing.html)
- [Deterministic Simulation Testing](https://www.scattered-thoughts.net/writing/moving-faster/)
