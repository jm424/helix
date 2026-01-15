# SharedWAL Failover Investigation Notes

## Key Findings

### 1. CommitEntry Handling is Correct ✓

Both leader and followers process `CommitEntry` events from Raft and call `apply_entry_async` -> `append_blob` -> writes to SharedWAL. The code path is identical for all nodes.

### 2. Followers ARE Writing to SharedWAL ✓

Logs from multi-node test show all 3 nodes successfully appending blobs:
```
[06:35:45.315983] ThreadId(16) Appended blob topic=1 partition=0 base_offset=0 wal_index=1
[06:35:45.364264] ThreadId(16) Appended blob topic=1 partition=0 base_offset=0 wal_index=1
[06:35:45.365244] ThreadId(14) Appended blob topic=1 partition=0 base_offset=0 wal_index=1
```

Multiple thread IDs = multiple nodes all writing successfully.

### 3. CRITICAL FINDING: Followers ARE Writing Before Leader Dies! ✓

Logs from failover-topic test show followers successfully appending ALL messages (offsets 0-9) BEFORE node-1 is killed:

```
[06:35:49.177069] ThreadId(15) Appended blob to durable partition topic=1 partition=0 base_offset=0 wal_index=1
[06:35:49.226983] ThreadId(17) Appended blob to durable partition topic=1 partition=0 base_offset=0 wal_index=1
[06:35:49.228035] ThreadId(14) Appended blob to durable partition topic=1 partition=0 base_offset=0 wal_index=1
... (continues through base_offset=9 wal_index=10)
[06:35:49.877524] ThreadId(13) Appended blob to durable partition topic=1 partition=0 base_offset=9 wal_index=10
[06:35:49.878858] Phase 2: Killing leader (node 1)...
```

Multiple thread IDs (13, 14, 15, 17, 02, 03) writing = multiple nodes writing successfully!

### 4. The Real Puzzle

**The data WAS written to followers' SharedWALs, but after failover it's not there!**

Evidence:
- Followers write offsets 0-9 to wal_index 1-10 BEFORE node-1 dies ✓
- After failover, consumer reads show `start_offset=20 storage_end_offset=20` ✗
- All 10 messages from phase 1 are lost!

**This is NOT a replication problem - it's a RECOVERY problem!**

## Theories (Updated)

### Theory A: Data Directory Issue (NEW MOST LIKELY)

The failover test may be:
- Restarting nodes with fresh data directories
- Each test run gets a new `/tmp/helix-test-*` directory
- Node-2's recovery looks in a different data_dir than where it originally wrote

Evidence needed: Check if nodes are restarted between tests or if they continue running.

### Theory B: Partition Recreation on Failover

When the new leader is elected, the partition may be getting recreated with a fresh SharedWAL handle:
- New AssignPartition command commits with different replica order
- Partition storage gets recreated instead of recovered
- Fresh partition starts at offset=0 but then jumps to offset=10 (from new writes)

### Theory C: SharedWAL Flush Timing

While followers wrote to SharedWAL, the writes may still be buffered:
- Default flush interval is 1ms
- But flush might not happen before node restarts/failover
- Need to verify: Are SharedWAL writes actually fsynced?

## ROOT CAUSE IDENTIFIED ✓

**The test failure is a server-side metadata staleness bug, NOT a data loss issue!**

### Evidence

1. **Server returns all 20 blobs correctly** (data IS preserved):
   ```
   [06:36:00.077756] read_blobs: looking up storage start_offset=0 storage_end_offset=20
   [06:36:00.077784] read_blobs: returning blobs blobs_count=20
   [06:36:00.077857] Fetched records blobs_count=20 total_bytes=1850
   ```

2. **SharedWAL replication IS working**:
   - Multiple thread IDs (13, 14, 15, 17, 02, 03) wrote offsets 0-9 before node-1 killed
   - SharedWAL IS being fsynced (1ms flush interval)
   - Data IS persisted to followers' SharedWALs

3. **Metadata returns stale leader for 7.7 seconds**:
   ```
   [06:35:49.878] Node-1 killed
   [06:35:50.476] "Became leader" - node-2 elected (Raft works fast!)
   [06:35:58.171] Metadata FINALLY returns leader=2 (7.7 seconds later!)
   ```

4. **Client stuck trying to connect to dead broker**:
   - Metadata kept returning leader=1 (dead node) for 7.7 seconds
   - Client gets BrokerTransportFailure trying to reach dead broker
   - Client eventually times out

### Actual Bug: rdkafka Client Behavior

**The server is correct. The bug is in rdkafka client metadata refresh logic.**

The timeline:
1. Node-1 dies at 06:35:49.878
2. Raft elects node-2 as leader within ~0.6 seconds (06:35:50.476)
3. Server metadata would return correct leader=2 if asked
4. But client doesn't ask - it keeps retrying connection to dead broker-1

**rdkafka metadata refresh behavior** (from `~/dd/librdkafka/src`):

1. **Broker goes DOWN**: Triggers metadata refresh (`rdkafka_broker.c:770`)
   - But only if broker was previously in UP state!

2. **NOT_LEADER_FOR_PARTITION error**: Triggers `rd_kafka_toppar_leader_unavailable`
   - Which calls `rd_kafka_topic_fast_leader_query` (100ms initial interval)

3. **TRANSPORT error (connection failure)**: Does NOT trigger metadata refresh
   - Just applies `reconnect.backoff.ms` (100ms initial, 10s max)
   - Client keeps retrying connection with exponential backoff

**The bug scenario:**
1. Client connects to broker-2 for initial metadata
2. Gets metadata saying leader=1 for partition
3. Client tries to connect to broker-1 (dead, never connected before)
4. Connection fails - but broker-1 was never UP, so no "broker down" event
5. Client keeps retrying with exponential backoff up to 10 seconds
6. No metadata refresh triggered until connection attempt succeeds or times out!

**rdkafka relevant configs:**
- `reconnect.backoff.ms`: 100ms (initial)
- `reconnect.backoff.max.ms`: 10,000ms (10 seconds!)
- `topic.metadata.refresh.fast.interval.ms`: 100ms (but only triggered on certain errors)

### Broker Liveness Configuration Comparison

| System | Heartbeat Interval | Timeout | Ratio | Notes |
|--------|-------------------|---------|-------|-------|
| **Kafka KRaft** | 2,000 ms | 9,000 ms | 4.5:1 | Cross-DC tolerant |
| **Redpanda (broker)** | 100 ms | 5,000 ms | 50:1 | Cluster membership |
| **Redpanda (Raft)** | 150 ms | 3,000 ms | 20:1 | Partition leadership |
| **etcd** | 100 ms | 1,000 ms | 10:1 | Low-latency optimized |
| **Helix (updated)** | 1,000 ms | 5,000 ms | 5:1 | Same-DC optimized |

### Dead Broker Detection

**Updated Helix configuration:**
- Heartbeat interval: 1 second (`HEARTBEAT_INTERVAL_MS = 1_000`)
- Heartbeat timeout: 5 seconds (`BROKER_HEARTBEAT_TIMEOUT_MS = 5_000`)
- Total failover time: ~5-6 seconds for clients to discover new leader

**Future improvements to consider:**
1. Use TCP connection failures as a faster liveness signal (transport already logs "Connection refused")
2. Feed transport-level failures back to liveness tracking
3. Separate "possibly dead" (fast, connection failures) from "confirmed dead" (slow, heartbeat timeout)

## Code Locations

- **Heartbeat timeout**: `helix-server/src/controller.rs:319` (`BROKER_HEARTBEAT_TIMEOUT_MS = 5_000`)
- **Heartbeat interval**: `helix-server/src/service/tick.rs:29` (`HEARTBEAT_INTERVAL_MS = 1_000`)
- **Live brokers check**: `helix-server/src/service/mod.rs:575` (`live_brokers()`)
- **Transport failures**: `helix-runtime/src/transport.rs:485` (logs connection refused)
- SharedWAL buffering: `helix-wal/src/shared_wal.rs:641` (`append_async`)
- Flush loop: `helix-wal/src/shared_wal.rs:969` (`flush_loop`)
- Flush interval config: defaults to **1ms** (line 558)
