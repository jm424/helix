# ADR-0004: Tick-Based Raft Timing

## Status

Accepted

## Context

Raft consensus requires two types of timeouts:
- **Election timeout**: Followers start elections if they don't hear from a leader
- **Heartbeat interval**: Leaders send periodic heartbeats to maintain authority

The implementation must handle these timeouts while remaining compatible with Deterministic Simulation Testing (DST). The challenge is that traditional timer-based approaches introduce non-determinism that breaks simulation testing.

We observed that when the Raft library emitted "reset timer" outputs and tests called timeout handlers directly, all nodes would start elections simultaneously, preventing any from completing. This violated the core Raft assumption that election timeouts are randomized across nodes.

## Decision

Use internal tick counting with randomized election timeouts:

1. **Single `tick()` API**: The application calls `tick()` at regular intervals (e.g., every 100ms)
2. **Internal counters**: The library tracks `election_elapsed` and `heartbeat_elapsed`
3. **Randomized timeout**: Election timeout is randomized in `[election_tick, 2*election_tick)` using a seeded RNG
4. **No timer outputs**: The library never tells the application to reset timers

```rust
impl RaftNode {
    pub fn tick(&mut self) -> Vec<RaftOutput> {
        match self.state {
            RaftState::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.config.heartbeat_tick {
                    self.heartbeat_elapsed = 0;
                    self.send_heartbeats()
                }
            }
            _ => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.randomized_election_timeout {
                    self.election_elapsed = 0;
                    self.reset_randomized_election_timeout();
                    self.start_election()
                }
            }
        }
    }
}
```

## Consequences

**Easier:**
- DST-friendly by design: just call `tick()` N times deterministically
- Randomized timeouts prevent thundering herd without external coordination
- Simpler application code: one timer instead of managing election/heartbeat timers separately
- Reproducible behavior with seeded RNG

**Harder:**
- Application must call `tick()` regularly (but this is straightforward)
- Tick interval affects timing granularity (100ms tick = 100ms resolution)

## Options Considered

### Option 1: Timer Reset Outputs (Rejected)

The library emits `ResetElectionTimer` and `ResetHeartbeatTimer` outputs, and the application manages actual timers.

**Pros:**
- Application has full control over timing
- Can use native OS timers

**Cons:**
- Non-deterministic in simulation (timers fire based on wall clock)
- Application must implement randomization correctly
- More complex application code
- In tests, calling timeout handlers for all nodes simultaneously breaks Raft

### Option 2: Tick-Based with Internal Counters (Chosen)

**Pros:**
- Deterministic: same sequence of `tick()` calls produces same behavior
- Randomization handled internally with seeded RNG
- Simple API: just call `tick()` periodically
- Proven pattern used by production Raft implementations

**Cons:**
- Timing granularity limited by tick interval
- Application must remember to call `tick()`

### Option 3: Injected Time Source

Pass a time source interface that can be mocked in tests.

**Pros:**
- Can use real time in production
- Testable with mock time

**Cons:**
- More complex API
- Must carefully mock all time-dependent behavior
- Randomization still needs separate handling

## References

- [Raft Paper](https://raft.github.io/raft.pdf) - Section 5.2 on election timeouts
- ADR-0002: Synchronous State Machines - Related decision on DST-friendly design
