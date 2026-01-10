//! Reusable test scenarios for Helix simulation.
//!
//! Scenarios define common fault patterns and cluster configurations
//! that can be composed to create comprehensive tests.

/// Standard cluster configurations.
pub mod clusters {
    /// 3-node cluster configuration.
    pub const THREE_NODE: &[&str] = &["node1", "node2", "node3"];

    /// 5-node cluster configuration for stronger fault tolerance.
    pub const FIVE_NODE: &[&str] = &["node1", "node2", "node3", "node4", "node5"];
}

/// Common fault injection patterns.
pub mod faults {
    /// Leader isolation: Partition the current leader from the rest of the cluster.
    pub struct LeaderIsolation {
        /// Duration of the partition in simulated seconds.
        pub duration_secs: u64,
    }

    /// Rolling restart: Restart each node one at a time.
    pub struct RollingRestart {
        /// Delay between restarts in simulated seconds.
        pub delay_secs: u64,
    }

    /// Network flapping: Rapidly alternate between partitioned and connected states.
    pub struct NetworkFlapping {
        /// Number of partition/heal cycles.
        pub cycles: u32,
        /// Duration of each partition in simulated seconds.
        pub partition_secs: u64,
        /// Duration of each heal in simulated seconds.
        pub heal_secs: u64,
    }

    /// Asymmetric partition: Node A can send to B, but B cannot send to A.
    pub struct AsymmetricPartition {
        /// Node that can only send.
        pub sender: &'static str,
        /// Node that can only receive.
        pub receiver: &'static str,
        /// Duration in simulated seconds.
        pub duration_secs: u64,
    }

    /// Slow node: One node has elevated latency for all operations.
    pub struct SlowNode {
        /// The slow node.
        pub node: &'static str,
        /// Added latency in simulated milliseconds.
        pub latency_ms: u64,
        /// Duration in simulated seconds.
        pub duration_secs: u64,
    }
}

/// Seeds for reproducible testing.
pub mod seeds {
    /// Standard test seeds that have historically found bugs.
    pub const REGRESSION_SEEDS: &[u64] = &[
        42,
        12345,
        0xDEAD_BEEF,
        999,
        7777,
        0x1337,
        0xCAFE_BABE,
        1,
        u64::MAX,
        0,
    ];

    /// Number of random seeds to test in CI.
    pub const CI_SEED_COUNT: u32 = 100;
}
