//! Fault injection methods and `FaultInjectable` trait implementations.

use std::sync::Arc;

use helix_core::NodeId;
use helix_server::kafka::KafkaHandler;
use helix_wal::FaultConfig;
use tokio::sync::mpsc;
use tracing::info;

use crate::madsim_transport::MadSimTransport;

use super::cluster::{E2ECluster, E2ENode};
use super::helpers::message_bridge_task;

impl E2ECluster {
    // ========================================================================
    // Fault Injection
    // ========================================================================

    /// Partitions the given nodes from each other.
    pub fn partition(&self, nodes: &[NodeId]) {
        let mut state = self.network_state.lock().expect("lock poisoned");
        state.partition(nodes);
        info!(?nodes, "Network partition created");
    }

    /// Heals a partition between the given nodes.
    pub fn heal(&self, nodes: &[NodeId]) {
        let mut state = self.network_state.lock().expect("lock poisoned");
        state.heal(nodes);
        info!(?nodes, "Network partition healed");
    }

    /// Heals all partitions in the cluster.
    pub fn heal_all(&self) {
        let mut state = self.network_state.lock().expect("lock poisoned");
        // Heal all pairs.
        for i in 0..self.node_ids.len() {
            for j in (i + 1)..self.node_ids.len() {
                state.heal(&[self.node_ids[i], self.node_ids[j]]);
            }
        }
        info!("All network partitions healed");
    }

    /// Crashes a node (stops receiving messages and reverts unsynced storage).
    ///
    /// This calls `SimulatedStorage::simulate_crash()` to revert any writes
    /// that were not fsync'd, simulating a real power failure. The node's
    /// in-memory state (HelixService) continues running but is network-isolated.
    /// Use `restart_node()` after `recover_node()` for full crash recovery
    /// with WAL replay.
    pub fn crash_node(&self, node_id: NodeId) {
        // Revert unsynced writes in storage.
        if let Some(node) = self.nodes.get(&node_id) {
            node.storage.simulate_crash();
        }
        let mut state = self.network_state.lock().expect("lock poisoned");
        state.crash_node(node_id);
        info!(node = node_id.get(), "Node crashed (storage crash-reverted)");
    }

    /// Recovers a crashed node (network only — no WAL replay).
    ///
    /// This re-enables network communication for the node but does NOT restart
    /// the service. The existing `HelixService` continues with its in-memory
    /// state. For full crash recovery with WAL replay, use `restart_node()`.
    pub fn recover_node(&self, node_id: NodeId) {
        let mut state = self.network_state.lock().expect("lock poisoned");
        state.recover_node(node_id);
        info!(node = node_id.get(), "Node recovered (network only)");
    }

    /// Restarts a crashed node with a fresh `HelixService` instance.
    ///
    /// This simulates a real crash recovery:
    /// 1. Gets the node's `SimulatedStorage` (Arc-backed, survives drop)
    /// 2. Drops the old `E2ENode` (stops old service tasks)
    /// 3. Creates new mailbox channel, swaps sender into shared mailboxes
    /// 4. Spawns new `message_bridge_task`
    /// 5. Creates new `MadSimTransport` with same shared state
    /// 6. Calls `HelixService::new_multi_node_with_transport()` — this
    ///    opens `SharedWalPool` which replays from the crash-reverted files
    /// 7. Creates new `KafkaHandler`, inserts new `E2ENode`
    /// 8. Clears the crashed flag in network state
    ///
    /// The node replays its WAL from the last fsync'd state, catching up
    /// via Raft replication for entries that were lost in the crash.
    pub async fn restart_node(&mut self, node_id: NodeId) {
        use helix_core::WriteDurability;
        use helix_server::service::HelixService;

        // Get storage from the old node (Arc-backed, survives drop).
        let storage = self
            .nodes
            .get(&node_id)
            .expect("node must exist to restart")
            .storage
            .clone();

        let node_data_dir = self.base_data_dir.as_ref().map(|base| {
            base.join(format!("node-{}", node_id.get()))
        });

        // Drop the old node (stops background tasks).
        self.nodes.remove(&node_id);

        // Create new mailbox and swap into shared mailboxes.
        let (tx, madsim_rx) = mpsc::channel(self.config.mailbox_capacity);
        self.shared_mailboxes.replace(node_id, tx);

        // Bridge MadSim messages to helix_runtime format.
        let (incoming_tx, incoming_rx) = mpsc::channel(self.config.mailbox_capacity);
        tokio::spawn(message_bridge_task(madsim_rx, incoming_tx));

        // Create new transport with same shared state.
        let transport = MadSimTransport::new(
            node_id,
            self.network_state.clone(),
            self.shared_mailboxes.clone(),
        );

        // Create new service — this replays WAL from crash-reverted storage.
        let service = HelixService::new_multi_node_with_transport(
            "e2e-cluster".to_string(),
            node_id.get(),
            self.node_ids.clone(),
            transport.clone(),
            incoming_rx,
            node_data_dir.clone(),
            self.config.shared_wal_count,
            WriteDurability::Fsync,
            storage.clone(),
        )
        .await;

        let actor_router = service.actor_router().cloned();
        let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);

        let service = Arc::new(service);
        let handler = Arc::new(KafkaHandler::new(
            Arc::clone(&service),
            "127.0.0.1".to_string(),
            9092,
            true, // auto_create_topics
            1,    // auto_create_partitions
        ));

        self.nodes.insert(
            node_id,
            E2ENode {
                node_id,
                service,
                handler,
                storage,
                transport,
                shutdown_tx,
                data_dir: node_data_dir,
                actor_router,
            },
        );

        // Clear the crashed flag.
        {
            let mut state = self.network_state.lock().expect("lock poisoned");
            state.recover_node(node_id);
        }

        info!(
            node = node_id.get(),
            "Node restarted with fresh service (WAL replayed)"
        );
    }

    /// Sets storage fault configuration for a node.
    pub fn set_storage_faults(&self, node_id: NodeId, faults: FaultConfig) {
        if let Some(node) = self.nodes.get(&node_id) {
            *node.storage.fault_config() = faults;
            info!(node = node_id.get(), "Storage faults updated");
        }
    }
}

// ============================================================================
// FaultInjectable Implementation for E2ECluster
// ============================================================================

impl crate::madsim_scenarios::FaultInjectable for E2ECluster {
    fn node_ids(&self) -> &[NodeId] {
        &self.node_ids
    }

    fn partition(&self, nodes: &[NodeId]) {
        E2ECluster::partition(self, nodes);
    }

    fn heal(&self, nodes: &[NodeId]) {
        E2ECluster::heal(self, nodes);
    }

    fn crash_node(&self, node_id: NodeId) {
        E2ECluster::crash_node(self, node_id);
    }

    fn recover_node(&self, node_id: NodeId) {
        E2ECluster::recover_node(self, node_id);
    }

    fn set_storage_faults(&self, node_id: NodeId, config: FaultConfig) {
        E2ECluster::set_storage_faults(self, node_id, config);
    }

    fn set_node_latency(&self, node_id: NodeId, multiplier: u32) {
        let mut state = self.network_state.lock().expect("lock poisoned");
        state.set_node_latency(node_id, multiplier);
        info!(
            node = node_id.get(),
            multiplier, "Node latency multiplier set"
        );
    }
}

// ============================================================================
// Handle Creators (test-only)
// ============================================================================

#[cfg(all(test, madsim))]
impl E2ECluster {
    /// Creates a fault injector handle that can be moved into a spawned task.
    /// Implements `FaultInjectable` using only `Arc`-cloned state.
    pub(crate) fn create_fault_injector_handle(
        &self,
    ) -> super::concurrent::FaultInjectorHandle {
        let storages: Vec<_> = self
            .nodes
            .iter()
            .map(|(&id, node)| (id, node.storage.clone()))
            .collect();
        super::concurrent::FaultInjectorHandle {
            network_state: self.network_state.clone(),
            node_ids: self.node_ids.clone(),
            storages,
        }
    }

    /// Creates a handle that can be moved into a spawned concurrent producer
    /// task. The handle holds only `Arc`-cloned references, so the `E2ECluster`
    /// remains exclusively owned by the main task for fault injection.
    pub(crate) fn create_producer_handle(
        &self,
    ) -> super::concurrent::ConcurrentProducerHandle {
        let handlers: Vec<_> = self
            .nodes
            .iter()
            .map(|(&id, node)| (id, Arc::clone(&node.handler)))
            .collect();
        let services: Vec<_> = self
            .nodes
            .iter()
            .map(|(&id, node)| (id, Arc::clone(&node.service)))
            .collect();
        super::concurrent::ConcurrentProducerHandle {
            handlers,
            services,
            network_state: self.network_state.clone(),
            node_ids: self.node_ids.clone(),
            property_state: self.property_state.clone(),
        }
    }
}
