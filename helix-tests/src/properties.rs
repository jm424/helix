//! Property definitions for Helix simulation tests.
//!
//! Properties are invariants that must hold throughout simulation.
//! They are checked after each simulation step.

/// Raft safety properties.
pub mod raft {
    /// `SingleLeaderPerTerm`: At most one leader can be elected in a given term.
    ///
    /// This is the fundamental safety property of Raft. Violating this means
    /// split-brain and potential data loss.
    pub struct SingleLeaderPerTerm;

    /// `LogMatching`: If two logs contain an entry with the same index and term,
    /// then the logs are identical in all entries up through that index.
    ///
    /// This ensures that committed entries are never lost or overwritten.
    pub struct LogMatching;

    /// `LeaderCompleteness`: If a log entry is committed in a given term,
    /// that entry will be present in the logs of all leaders for all
    /// higher-numbered terms.
    ///
    /// This ensures durability of committed entries across leader changes.
    pub struct LeaderCompleteness;

    /// `StateMachineSafety`: If a server has applied a log entry at a given
    /// index to its state machine, no other server will ever apply a
    /// different log entry for the same index.
    ///
    /// This ensures linearizability of the replicated state machine.
    pub struct StateMachineSafety;
}

/// WAL integrity properties.
pub mod wal {
    /// `WalIntegrity`: After crash recovery, the WAL contains exactly the
    /// entries that were synced before the crash, with no corruption.
    pub struct WalIntegrity;

    /// `WalMonotonicity`: Log indices are strictly increasing. No gaps,
    /// no duplicates.
    pub struct WalMonotonicity;
}

/// Progress tracking properties.
pub mod progress {
    /// `NoLostMessages`: Every acked message is delivered exactly once.
    /// No message is delivered without being produced.
    pub struct NoLostMessages;

    /// `EventualProgress`: If messages continue to be produced, the
    /// low watermark eventually advances (no stuck consumers).
    pub struct EventualProgress;
}
