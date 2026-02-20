//! DST: fresh-follower snapshot delivery.
//!
//! Verifies that when a follower restarts fresh and the leader's in-memory log
//! has been compacted past the follower's `next_index`, the leader:
//!
//! 1. Emits `NeedEntries` on the next heartbeat cycle
//! 2. `handle_need_entries` detects `start_index < log.first_index()` → snapshot
//! 3. Calls `provide_snapshot`, sending `InstallSnapshot` to the follower
//! 4. Follower advances `persisted_commit_index` and `commit_index`
//! 5. Subsequent `AppendEntries` carry the remaining in-memory entries
//! 6. All nodes converge to the same `commit_index`

// Test-specific lint allowances.
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::uninlined_format_args)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bloodhound::simulation::discrete::engine::{DiscreteSimulationEngine, EngineConfig};
use bloodhound::simulation::discrete::event::{ActorId, EventKind};
use helix_core::NodeId;
use helix_raft::RaftConfig;

use crate::properties::{check_shared_state, PropertyCheckResult};
use crate::raft_actor::{
    custom_events, NetworkState, PropertyState, RaftActor, SharedNetworkState, SharedPropertyState,
};

// ============================================================================
// Helpers
// ============================================================================

/// Creates a `CLIENT_REQUEST` custom event.
fn client_request_event(actor: ActorId, data: &[u8]) -> EventKind {
    EventKind::Custom {
        actor,
        name: custom_events::CLIENT_REQUEST.to_string(),
        data: data.to_vec(),
    }
}

/// Creates a `COMPACT_LOG` custom event.
fn compact_log_event(actor: ActorId) -> EventKind {
    EventKind::Custom {
        actor,
        name: custom_events::COMPACT_LOG.to_string(),
        data: vec![],
    }
}

/// Panics with a detailed message if any property violations exist.
fn assert_no_violations(result: &PropertyCheckResult, context: &str) {
    if !result.is_valid() {
        let mut msg = format!("Property violations in {context}:\n");
        for v in &result.leader_violations {
            msg.push_str(&format!("  - {v}\n"));
        }
        for v in &result.log_violations {
            msg.push_str(&format!("  - {v}\n"));
        }
        for v in &result.state_machine_violations {
            msg.push_str(&format!("  - {v}\n"));
        }
        panic!("{msg}");
    }
}

/// Creates a 3-node cluster configured for rapid log compaction.
///
/// `log_trailing_entries = 5` means only the 5 most recent committed entries
/// are retained in the in-memory log. After committing ≥ 6 entries and calling
/// `compact_log`, the log floor advances, forcing fresh followers to receive
/// a snapshot before catching up via `AppendEntries`.
fn create_snapshot_simulation(
    seed: u64,
    max_time_secs: u64,
) -> (
    DiscreteSimulationEngine,
    Vec<ActorId>,
    SharedNetworkState,
    SharedPropertyState,
) {
    let engine_config = EngineConfig::new(seed)
        .with_max_time(Duration::from_secs(max_time_secs))
        .with_stats(true);
    let mut engine = DiscreteSimulationEngine::with_config(engine_config);

    let network_state: SharedNetworkState = Arc::new(Mutex::new(NetworkState::new()));
    let property_state: SharedPropertyState = Arc::new(Mutex::new(PropertyState::new()));

    let node_ids: Vec<NodeId> = (1..=3u64).map(NodeId::new).collect();
    let actor_ids: Vec<ActorId> = (1..=3u64).map(ActorId::new).collect();

    let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
        .iter()
        .zip(actor_ids.iter())
        .map(|(&n, &a)| (n, a))
        .collect();

    for (&node_id, &actor_id) in node_ids.iter().zip(actor_ids.iter()) {
        let raft_config = RaftConfig::new(node_id, node_ids.clone())
            .with_tick_config(5, 1) // election_tick=5 ticks, heartbeat every tick
            .with_random_seed(seed + node_id.get()) // per-node seed for determinism
            .with_log_trailing_entries(5); // tiny window forces compaction quickly
        let mut actor = RaftActor::new(actor_id, raft_config, node_to_actor.clone());
        actor.set_network_state(Arc::clone(&network_state));
        actor.set_property_state(Arc::clone(&property_state));
        engine.register_actor(Box::new(actor));
    }

    (engine, actor_ids, network_state, property_state)
}

// ============================================================================
// Randomized scenario runner
// ============================================================================

/// Minimal xorshift64 PRNG seeded from the test seed.
///
/// Used to randomize scenario parameters (which node crashes, when, how many
/// entries it misses) independently of the Bloodhound engine seed, so that
/// each seed produces a genuinely different fault injection pattern rather
/// than just different leader election timing.
struct Prng(u64);

impl Prng {
    fn new(seed: u64) -> Self {
        // Mix the seed to avoid degenerate states; xorshift64 is undefined for 0.
        let s = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut p = Self(if s == 0 { 1 } else { s });
        // Warm up to improve entropy for small seeds.
        for _ in 0..8 {
            p.step();
        }
        p
    }

    fn step(&mut self) {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
    }

    /// Returns a uniform value in `[lo, hi)`.
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(hi > lo, "range requires hi > lo");
        self.step();
        lo + self.0 % (hi - lo)
    }

    /// Returns `true` with probability `1/n`.
    fn one_in(&mut self, n: u64) -> bool {
        self.range(0, n) == 0
    }
}

/// Runs one randomized snapshot scenario and asserts all invariants hold.
///
/// Each seed produces a different fault pattern:
/// - How many entries are committed before compaction (12–24)
/// - Which node crashes (any of the 3)
/// - When it crashes relative to compaction
/// - How many entries it misses while crashed
/// - Whether a second distinct node also crashes sequentially (≈1/3 of seeds)
///
/// Returns `(events_processed, commit_index, install_snapshots_sent, n_crashes)`.
fn run_randomized_scenario(seed: u64) -> (u64, u64, u64, usize) {
    let mut rng = Prng::new(seed);

    // --- Derive scenario parameters ---

    // Pre-crash requests: well above trailing=5 to guarantee compaction advances.
    let n_pre: u64 = rng.range(12, 25);
    let pre_ms: u64 = rng.range(60, 180);
    let t0: u64 = 500;
    let t_last_pre = t0 + n_pre * pre_ms;

    // Compact after entries have had time to commit and replicate.
    let t_compact = t_last_pre + rng.range(400, 900);

    // First crash: any node, shortly after compaction.
    let crash1 = rng.range(0, 3) as usize;
    let t_crash1 = t_compact + rng.range(100, 500);
    let n_post1: u64 = rng.range(3, 10);
    let post_ms: u64 = rng.range(120, 350);
    let t_last_post1 = t_crash1 + 300 + n_post1 * post_ms;
    let t_recover1 = t_last_post1 + rng.range(600, 2500);

    // Optionally crash a second (different) node after the first recovers.
    let do_second = rng.one_in(3);
    let (t_last_recovery, second_crash) = if do_second {
        let offset = rng.range(1, 3); // 1 or 2, so crash2 != crash1
        let crash2 = ((crash1 as u64 + offset) % 3) as usize;
        let t_crash2 = t_recover1 + rng.range(400, 1000);
        let n_post2: u64 = rng.range(2, 6);
        let t_last_post2 = t_crash2 + 300 + n_post2 * post_ms;
        let t_recover2 = t_last_post2 + rng.range(300, 1000);
        (t_recover2, Some((crash2, t_crash2, n_post2)))
    } else {
        (t_recover1, None)
    };

    let n_crashes = if do_second { 2usize } else { 1usize };
    // 7s buffer after the last recovery for convergence.
    let max_time_secs = t_last_recovery / 1000 + 7;

    // --- Build simulation ---
    let (mut engine, actor_ids, _network_state, property_state) =
        create_snapshot_simulation(seed, max_time_secs);

    // Pre-crash requests to all nodes (only the leader accepts).
    for i in 0..n_pre {
        let t = t0 + i * pre_ms;
        for &actor in &actor_ids {
            engine.schedule_after(
                Duration::from_millis(t),
                client_request_event(actor, format!("pre-{i}").as_bytes()),
            );
        }
    }

    // Compact all nodes.
    for &actor in &actor_ids {
        engine.schedule_after(Duration::from_millis(t_compact), compact_log_event(actor));
    }

    // First crash, post-crash requests to surviving nodes, then recovery.
    engine.schedule_after(
        Duration::from_millis(t_crash1),
        EventKind::ProcessCrash { actor: actor_ids[crash1] },
    );
    let alive1: Vec<ActorId> =
        (0..3usize).filter(|&i| i != crash1).map(|i| actor_ids[i]).collect();
    for i in 0..n_post1 {
        let t = t_crash1 + 300 + i * post_ms;
        for &actor in &alive1 {
            engine.schedule_after(
                Duration::from_millis(t),
                client_request_event(actor, format!("post1-{i}").as_bytes()),
            );
        }
    }
    engine.schedule_after(
        Duration::from_millis(t_recover1),
        EventKind::ProcessRecover { actor: actor_ids[crash1] },
    );

    // Optional second crash and recovery.
    if let Some((crash2, t_crash2, n_post2)) = second_crash {
        engine.schedule_after(
            Duration::from_millis(t_crash2),
            EventKind::ProcessCrash { actor: actor_ids[crash2] },
        );
        let alive2: Vec<ActorId> =
            (0..3usize).filter(|&i| i != crash2).map(|i| actor_ids[i]).collect();
        for i in 0..n_post2 {
            let t = t_crash2 + 300 + i * post_ms;
            for &actor in &alive2 {
                engine.schedule_after(
                    Duration::from_millis(t),
                    client_request_event(actor, format!("post2-{i}").as_bytes()),
                );
            }
        }
        engine.schedule_after(
            Duration::from_millis(t_last_recovery),
            EventKind::ProcessRecover { actor: actor_ids[crash2] },
        );
    }

    // --- Run and verify ---
    let result = engine.run();
    assert!(result.success, "seed {seed}: simulation did not complete");

    let check = check_shared_state(&property_state).expect("lock poisoned");
    assert_no_violations(&check, &format!("seed {seed}"));

    let state = property_state.lock().expect("lock poisoned");

    // Fidelity: compaction must have advanced the log floor past the trailing window.
    let trailing: u64 = 5;
    for node in state.nodes.values().filter(|n| !n.crashed) {
        assert!(
            node.log_first_index > trailing,
            "seed {seed} node {}: log_first_index={} not past trailing={trailing}",
            node.node_id,
            node.log_first_index,
        );
    }

    // Fidelity: snapshot delivery path must have been taken at least once.
    assert!(
        state.install_snapshots_sent >= 1,
        "seed {seed}: snapshot path never taken (install_snapshots_sent=0)"
    );

    // Liveness: all non-crashed nodes must converge to the same commit_index.
    let commit_indices: Vec<u64> = state
        .nodes
        .values()
        .filter(|n| !n.crashed)
        .map(|n| n.commit_index)
        .collect();
    assert!(!commit_indices.is_empty(), "seed {seed}: no non-crashed nodes");
    let max_commit = commit_indices.iter().max().copied().unwrap_or(0);
    let min_commit = commit_indices.iter().min().copied().unwrap_or(0);
    assert!(
        max_commit >= n_pre,
        "seed {seed}: max_commit={max_commit} < n_pre={n_pre}"
    );
    assert_eq!(
        max_commit,
        min_commit,
        "seed {seed}: commit indices diverged: {commit_indices:?}"
    );

    (result.stats.events_processed, max_commit, state.install_snapshots_sent, n_crashes)
}

// ============================================================================
// Tests
// ============================================================================

/// Core scenario: commit entries, compact log, crash follower, recover fresh.
///
/// Timeline (simulated ms):
///  - T=1000–2900ms : 20 client requests submitted (100ms apart) to all nodes;
///                    only the leader accepts. All 20 commit and replicate.
///  - T=3500ms      : COMPACT_LOG on all nodes. With trailing=5 and
///                    commit_index≥20, first_index advances to ≥16.
///  - T=4000ms      : Crash node 3 (actor 2, 0-indexed). Volatile state lost.
///  - T=4500–5900ms : 5 more requests submitted to the alive nodes 1 and 2.
///                    They form a quorum; entries commit and replicate between
///                    nodes 1 and 2. Node 3 misses all of these.
///  - T=8000ms      : Recover node 3 with a completely fresh log.
///                    next_index=1, but leader's log.first_index≥16.
///                    Leader sends AppendEntries → node 3 rejects (empty log) →
///                    leader resets next_index=1 → NeedEntries fires →
///                    handle_need_entries sends InstallSnapshot →
///                    node 3 advances persisted_commit_index → catches up.
///  - T=20s         : Simulation ends. Assert all nodes at same commit_index.
#[test]
fn test_dst_snapshot_fresh_follower_catches_up() {
    let seed = 42;
    let (mut engine, actor_ids, _network_state, property_state) =
        create_snapshot_simulation(seed, 20);

    // Phase 1: submit 20 requests to all nodes.
    // The leader will accept them; followers reject silently.
    for i in 0..20u64 {
        let time_ms = 1000 + i * 100;
        for &actor_id in &actor_ids {
            let data = format!("req-{i}");
            engine.schedule_after(
                Duration::from_millis(time_ms),
                client_request_event(actor_id, data.as_bytes()),
            );
        }
    }

    // Phase 2: compact the log on all nodes (T=3500ms).
    // All 20 entries should be committed and replicated by now.
    // With trailing=5: compact_to ≥ 15, first_index ≥ 16.
    for &actor_id in &actor_ids {
        engine.schedule_after(Duration::from_millis(3500), compact_log_event(actor_id));
    }

    // Phase 3: crash node 3 (T=4000ms).
    engine.schedule_after(
        Duration::from_millis(4000),
        EventKind::ProcessCrash { actor: actor_ids[2] },
    );

    // Phase 4: submit 5 more requests to the two alive nodes (T=4500–5700ms).
    // Nodes 1 and 2 form a quorum; they commit. Node 3 misses these entirely.
    for i in 0..5u64 {
        let time_ms = 4500 + i * 300;
        for &actor_id in &actor_ids[..2] {
            let data = format!("post-crash-{i}");
            engine.schedule_after(
                Duration::from_millis(time_ms),
                client_request_event(actor_id, data.as_bytes()),
            );
        }
    }

    // Phase 5: recover node 3 with a fresh empty log (T=8000ms).
    // It will restart as a follower with commit_index=0, next_index unknown to
    // the leader. The snapshot path carries it past the compacted floor.
    engine.schedule_after(
        Duration::from_millis(8000),
        EventKind::ProcessRecover { actor: actor_ids[2] },
    );

    // Run to T=20s.
    let result = engine.run();
    assert!(result.success, "simulation did not complete successfully");

    // --- Safety checks ---
    let check = check_shared_state(&property_state).expect("lock poisoned");
    assert_no_violations(&check, "fresh-follower snapshot");

    // --- Liveness checks ---
    let state = property_state.lock().expect("lock poisoned");

    assert!(
        state.events_processed > 0,
        "simulation processed no events — check actor setup"
    );

    // --- Fidelity check: compaction actually happened ---
    // Nodes 1 and 2 were alive throughout; with trailing=5 and 20+ entries,
    // compact_log() must have advanced first_index beyond 5.
    let trailing: u64 = 5;
    for node in state.nodes.values().filter(|n| !n.crashed) {
        assert!(
            node.log_first_index > trailing,
            "node {}: expected log_first_index > trailing={trailing} after compaction, \
             got log_first_index={}",
            node.node_id,
            node.log_first_index,
        );
    }

    // --- Fidelity check: snapshot path was taken ---
    // If install_snapshots_sent == 0, the test passed vacuously — node 3 caught up
    // via normal AppendEntries because compaction was either a no-op or the leader
    // didn't detect the gap. Either way the snapshot code was never exercised.
    assert!(
        state.install_snapshots_sent > 0,
        "snapshot path was never taken: install_snapshots_sent=0. \
         Check that compact_log() advanced the log floor and that \
         handle_need_entries correctly detects start_index < log_first_index."
    );

    // Collect commit indices for all non-crashed nodes.
    let mut commit_indices: Vec<u64> = state
        .nodes
        .values()
        .filter(|n| !n.crashed)
        .map(|n| n.commit_index)
        .collect();
    commit_indices.sort_unstable();

    assert!(
        !commit_indices.is_empty(),
        "no non-crashed nodes found in property state"
    );
    assert!(
        commit_indices.iter().all(|&ci| ci > 0),
        "some nodes have zero commit_index: {commit_indices:?}"
    );

    let max_commit = *commit_indices.last().expect("checked non-empty above");
    let min_commit = *commit_indices.first().expect("checked non-empty above");

    // At least the 20 pre-compaction entries should be committed.
    assert!(
        max_commit >= 20,
        "expected at least 20 committed entries, got max_commit={max_commit}"
    );

    // All nodes should have converged to the same commit_index.
    // Node 3 received a snapshot and caught up via AppendEntries.
    assert_eq!(
        max_commit,
        min_commit,
        "commit indices diverged after recovery: max={max_commit} min={min_commit} \
         all={commit_indices:?}"
    );

    println!(
        "test_dst_snapshot_fresh_follower_catches_up: \
         {} events, commit_index={}, install_snapshots_sent={}, leaders_by_term={:?}",
        result.stats.events_processed,
        max_commit,
        state.install_snapshots_sent,
        state.leaders_by_term
    );
}

/// Run 100 randomized scenarios with genuinely different fault patterns.
///
/// Each seed independently varies: which node crashes, when it crashes relative
/// to compaction, how many entries it misses, and whether a second distinct node
/// also crashes sequentially (≈1/3 of seeds, exercising two snapshots per run).
/// The seed also drives the Bloodhound engine internals (message ordering, timer
/// delivery), so no two runs are structurally identical.
#[test]
fn test_dst_snapshot_multiple_seeds() {
    let mut double_crash_count: usize = 0;
    let mut total_snapshots: u64 = 0;

    for seed in 0u64..100 {
        let (events, commit, snapshots, n_crashes) = run_randomized_scenario(seed);
        if n_crashes == 2 {
            double_crash_count += 1;
        }
        total_snapshots += snapshots;
        println!(
            "seed={seed:3}: events={events:6}, commit={commit:3}, \
             snapshots={snapshots}, crashes={n_crashes}",
        );
    }

    println!(
        "\n100 seeds: double_crash={double_crash_count}/100, total_snapshots={total_snapshots}"
    );

    // Roughly 1/3 of seeds should produce a double-crash scenario.
    assert!(
        double_crash_count > 0,
        "no double-crash scenarios produced across 100 seeds — check Prng"
    );
}
