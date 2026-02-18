//! WAL Actor for message-passing based WAL access.
//!
//! `WalActor` provides a command-channel interface to the shared WAL,
//! enabling the actor-model architecture. It wraps `SharedWalHandle`
//! which already provides batching via `SharedWalCoordinator`.
//!
//! # Design
//!
//! ```text
//! PartitionActor(0) ──┐
//!                     │     ┌───────────┐
//! PartitionActor(1) ──┼────►│ WalActor  │────► SharedWalCoordinator
//!                     │     └───────────┘        (batching + flush)
//! PartitionActor(2) ──┘
//! ```
//!
//! The WalActor receives append commands from multiple partition actors,
//! delegates to `SharedWalHandle`, and returns results via oneshot channels.

use std::collections::HashMap;

use bytes::Bytes;
use helix_core::GroupId;
use helix_wal::{DurableAck, SharedWalHandle, SharedWalPool, Storage, WalResult};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

/// Commands that can be sent to a `WalActor`.
#[derive(Debug)]
pub enum WalCommand {
    /// Append an entry to the WAL for a partition.
    Append {
        /// Group ID for the entry.
        group_id: GroupId,
        /// Raft term.
        term: u64,
        /// Log index (partition-local).
        index: u64,
        /// Actual Raft log index.
        raft_index: u64,
        /// Entry payload.
        payload: Bytes,
        /// Channel to send the result.
        reply: oneshot::Sender<WalResult<DurableAck>>,
    },
    /// Shutdown the actor.
    Shutdown,
}

/// Configuration for `WalActor`.
#[derive(Debug, Clone)]
pub struct WalActorConfig {
    /// Command channel capacity.
    pub channel_capacity: usize,
}

impl Copy for WalActorConfig {}

impl Default for WalActorConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 1024,
        }
    }
}

/// Handle for sending commands to a `WalActor`.
#[derive(Clone)]
pub struct WalActorHandle {
    tx: mpsc::Sender<WalCommand>,
}

impl WalActorHandle {
    /// Creates a new handle from a command sender.
    #[must_use]
    pub const fn new(tx: mpsc::Sender<WalCommand>) -> Self {
        Self { tx }
    }

    /// Appends an entry to the WAL.
    ///
    /// Returns when the entry is durable (batched and synced by the coordinator).
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has shut down or the WAL write fails.
    pub async fn append(
        &self,
        group_id: GroupId,
        term: u64,
        index: u64,
        raft_index: u64,
        payload: Bytes,
    ) -> WalResult<DurableAck> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.tx
            .send(WalCommand::Append {
                group_id,
                term,
                index,
                raft_index,
                payload,
                reply: reply_tx,
            })
            .await
            .map_err(|_| helix_wal::WalError::Shutdown)?;

        reply_rx.await.map_err(|_| helix_wal::WalError::Shutdown)?
    }

    /// Requests the actor to shut down.
    pub async fn shutdown(&self) {
        let _ = self.tx.send(WalCommand::Shutdown).await;
    }
}

/// WAL actor that provides command-channel interface to shared WAL.
///
/// Internally uses `SharedWalPool` which handles batching and sync.
struct WalActor<S: Storage> {
    /// Command receiver.
    cmd_rx: mpsc::Receiver<WalCommand>,
    /// Per-group handles to the shared WAL pool.
    handles: HashMap<GroupId, SharedWalHandle<S>>,
    /// The pool (for creating new handles).
    pool: SharedWalPool<S>,
}

impl<S: Storage + Clone + Send + Sync + 'static> WalActor<S> {
    /// Creates a new WAL actor.
    fn new(cmd_rx: mpsc::Receiver<WalCommand>, pool: SharedWalPool<S>) -> Self {
        Self {
            cmd_rx,
            handles: HashMap::new(),
            pool,
        }
    }

    /// Gets or creates a handle for a group.
    fn handle_for(&mut self, group_id: GroupId) -> &SharedWalHandle<S> {
        self.handles
            .entry(group_id)
            .or_insert_with(|| self.pool.handle(group_id))
    }

    /// Runs the actor message loop.
    async fn run(mut self) {
        debug!("WalActor started");

        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                WalCommand::Append {
                    group_id,
                    term,
                    index,
                    raft_index,
                    payload,
                    reply,
                } => {
                    let handle = self.handle_for(group_id).clone();
                    // Spawn the append to not block the actor loop.
                    // The handle's append already does batching via the coordinator.
                    tokio::spawn(async move {
                        let result = handle.append(term, index, raft_index, payload).await;
                        let _ = reply.send(result);
                    });
                }
                WalCommand::Shutdown => {
                    debug!("WalActor shutting down");
                    break;
                }
            }
        }

        debug!("WalActor stopped");
    }
}

/// Spawns a `WalActor` and returns a handle to communicate with it.
///
/// The actor wraps a `SharedWalPool` and provides command-channel access.
///
/// # Arguments
///
/// * `pool` - The shared WAL pool to use for writes.
/// * `config` - Actor configuration.
///
/// # Returns
///
/// A handle for sending commands to the actor.
#[must_use]
pub fn spawn_wal_actor<S: Storage + Clone + Send + Sync + 'static>(
    pool: SharedWalPool<S>,
    config: WalActorConfig,
) -> WalActorHandle {
    let (tx, rx) = mpsc::channel(config.channel_capacity);

    let actor = WalActor::new(rx, pool);
    tokio::spawn(actor.run());

    WalActorHandle::new(tx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_wal::{PoolConfig, SimulatedStorage};
    use tempfile::TempDir;

    async fn setup_pool() -> (TempDir, SharedWalPool<SimulatedStorage>) {
        let temp_dir = TempDir::new().unwrap();
        let storage = SimulatedStorage::new(42);

        let pool_config = PoolConfig::new(temp_dir.path(), 2);
        let pool = SharedWalPool::open(storage, pool_config).await.unwrap();
        (temp_dir, pool)
    }

    #[tokio::test]
    async fn test_wal_actor_append() {
        let (_temp_dir, pool) = setup_pool().await;
        let handle = spawn_wal_actor(pool, WalActorConfig::default());

        let partition = GroupId::new(1);
        let result = handle
            .append(partition, 1, 1, 0, Bytes::from("test data"))
            .await;

        assert!(result.is_ok());
        let ack = result.unwrap();
        assert_eq!(ack.group_id, partition);
        assert_eq!(ack.index, 1);

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_wal_actor_multiple_partitions() {
        let (_temp_dir, pool) = setup_pool().await;
        let handle = spawn_wal_actor(pool, WalActorConfig::default());

        let p1 = GroupId::new(1);
        let p2 = GroupId::new(2);

        // Append to both partitions.
        let r1 = handle.append(p1, 1, 1, 0, Bytes::from("p1 data")).await;
        let r2 = handle.append(p2, 1, 1, 0, Bytes::from("p2 data")).await;

        assert!(r1.is_ok());
        assert!(r2.is_ok());
        assert_eq!(r1.unwrap().group_id, p1);
        assert_eq!(r2.unwrap().group_id, p2);

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_wal_actor_sequential_indices() {
        let (_temp_dir, pool) = setup_pool().await;
        let handle = spawn_wal_actor(pool, WalActorConfig::default());

        let partition = GroupId::new(1);

        // Append multiple entries sequentially.
        for i in 1..=5 {
            let result = handle
                .append(partition, 1, i, 0, Bytes::from(format!("data {i}")))
                .await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap().index, i);
        }

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_wal_actor_concurrent_appends() {
        let (_temp_dir, pool) = setup_pool().await;
        let handle = spawn_wal_actor(pool, WalActorConfig::default());

        // Use different partitions to avoid index conflicts with concurrent appends.
        // Each partition gets its own sequential index space.
        let mut join_handles = Vec::new();
        for i in 1..=10_u64 {
            let h = handle.clone();
            let partition = GroupId::new(i);
            join_handles.push(tokio::spawn(async move {
                h.append(partition, 1, 1, 0, Bytes::from(format!("data {i}")))
                    .await
            }));
        }

        // Wait for all to complete.
        for jh in join_handles {
            let result = jh.await.unwrap();
            assert!(result.is_ok());
        }

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_wal_actor_handle_clone() {
        let (_temp_dir, pool) = setup_pool().await;
        let handle1 = spawn_wal_actor(pool, WalActorConfig::default());
        let handle2 = handle1.clone();

        let partition = GroupId::new(1);

        // Both handles should work.
        let r1 = handle1.append(partition, 1, 1, 0, Bytes::from("data1")).await;
        let r2 = handle2.append(partition, 1, 2, 0, Bytes::from("data2")).await;

        assert!(r1.is_ok());
        assert!(r2.is_ok());

        handle1.shutdown().await;
    }
}
