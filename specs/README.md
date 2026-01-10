# Helix TLA+ Specifications

This directory contains TLA+ specifications for Helix's distributed protocols.

## Specifications

| File | Description | Status |
|------|-------------|--------|
| `raft.tla` | Core Raft consensus | Planned |
| `raft_log.tla` | Log replication invariants | Planned |
| `multi_raft.tla` | Multi-group coordination | Planned |
| `progress.tla` | Consumer progress tracking | Planned |
| `tiering.tla` | Tiered storage consistency | Planned |

## Running Model Checking

Install TLA+ Toolbox or use the command-line TLC model checker:

```bash
# Install TLC (requires Java)
# Download from: https://github.com/tlaplus/tlaplus/releases

# Run model checking on Raft spec
java -jar tla2tools.jar -config raft.cfg raft.tla
```

## Key Properties

### Raft Safety Properties

1. **SingleLeaderPerTerm**: At most one leader per term
2. **LogMatching**: Logs with same index/term are identical up to that point
3. **LeaderCompleteness**: Committed entries present in all future leaders
4. **StateMachineSafety**: Same index → same command applied

### Liveness Properties (with fairness)

1. **EventualLeader**: Eventually a leader is elected
2. **EventualCommit**: Valid proposals eventually commit

## Design Principles

Following TigerStyle and FoundationDB approach:

1. **Spec before code**: Write TLA+ spec before implementation
2. **Model check first**: Run TLC to find bugs in the spec itself
3. **Trace validation**: Compare implementation traces against spec
4. **Living specs**: Update specs as design evolves

## References

- [Original Raft TLA+ Spec](https://github.com/ongardie/raft.tla)
- [TLA+ Hyperbook](https://lamport.azurewebsites.net/tla/hyperbook.html)
- [FoundationDB Testing](https://apple.github.io/foundationdb/testing.html)
