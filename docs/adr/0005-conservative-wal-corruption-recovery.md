# ADR-0005: Conservative WAL Corruption Recovery

## Status

Accepted

## Context

The Write-Ahead Log (WAL) must handle corruption discovered during recovery. When opening a WAL after a crash, entries are validated using CRC32 checksums. The question is: what should happen when a corrupted entry is found mid-segment?

Two scenarios where corruption occurs:

1. **Torn write**: Process crashed mid-write, leaving partial/garbage bytes
2. **Media corruption**: Disk sector failure, bit flip, or other hardware issue

In both cases, the CRC check fails. The WAL must decide how much data to discard.

Forces at play:

- **Safety**: Raft relies on WAL for durability guarantees
- **Availability**: Losing valid entries delays recovery
- **Simplicity**: Complex recovery logic is bug-prone
- **Replication**: Helix runs Raft, so missing entries can be re-fetched from peers

## Decision

Skip the entire segment when any CRC mismatch is detected during recovery.

When `Wal::open()` encounters a corrupted entry:
1. Log a warning with segment path, entry index, and corruption details
2. Stop reading that segment
3. Do not load any entries from the corrupted segment
4. Continue with subsequent segments (if any)

The WAL does NOT attempt to recover entries that precede the corruption point within the same segment.

## Consequences

### Benefits

- **Simplicity**: No backward-scanning or "find last valid entry" logic
- **Predictability**: Deterministic behavior - corruption means segment skip
- **Safety margin**: Adjacent entries may also be suspect (incomplete flush, write reordering)
- **Raft-friendly**: Missing entries trigger re-replication from leader, not permanent loss
- **Testability**: Easy to verify in DST - corrupt any byte, observe segment skip

### Trade-offs

- **Data loss potential**: Valid entries before corruption point are discarded
- **Recovery gap**: Single corrupted byte can skip hundreds of valid entries
- **Single-node impact**: Without replication, discarded entries are permanently lost
- **Large segment penalty**: Bigger segments = more data at risk per corruption event

### Mitigations

For deployments sensitive to these trade-offs:

1. **Use replication (RF >= 3)**: Raft re-fetches missing entries from peers
2. **Smaller segments**: Configure `segment_size_bytes_max` lower to bound loss
3. **Frequent fsync**: Reduce window of unflushed entries (performance trade-off)

## Options Considered

### Option 1: Conservative - Skip Entire Segment (Chosen)

**Pros:**
- Simple implementation, fewer bugs
- If one entry is bad, adjacent entries are suspect
- Raft handles missing entries via replication
- Deterministic, easy to test

**Cons:**
- Loses valid entries that precede corruption
- Worse for single-node or large-segment configurations

### Option 2: Partial Recovery - Keep Valid Prefix

Scan entries until CRC fails, keep all valid entries before corruption point, truncate segment at corruption.

**Pros:**
- Preserves more data
- Better for single-node deployments
- Maximizes recovery

**Cons:**
- Complex implementation (boundary detection, truncation)
- False confidence: entries before corruption may also be suspect
- Write reordering can make "valid prefix" unreliable
- More code paths = more bugs in critical recovery logic

### Option 3: Scan Forward - Find Next Valid Entry

Skip corrupted entry, continue scanning for next valid entry header.

**Pros:**
- Could recover entries after corruption gap

**Cons:**
- Very complex (how to detect valid header vs garbage?)
- High false positive risk (random bytes matching header pattern)
- Unclear semantics for log index continuity
- Not worth the complexity for a replicated log

## References

- [helix-wal/src/wal.rs](../../helix-wal/src/wal.rs) - Recovery implementation
- [helix-tests/src/wal_tests.rs](../../helix-tests/src/wal_tests.rs) - DST tests for corruption scenarios
- [SQLite WAL Recovery](https://www.sqlite.org/wal.html) - Similar conservative approach
- [RocksDB WAL Recovery Modes](https://github.com/facebook/rocksdb/wiki/WAL-Recovery-Modes) - Multiple recovery modes for different use cases
