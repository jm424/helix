//! Fault injection methods (`E2ECluster`) and `FaultInjectable` trait impls.
//!
//! Partitions go through `NetSim::clog_link(src, dst)` with helix→madsim
//! `NodeId` translation. Crashes use `NetSim::clog_node(madsim_id)` plus
//! `SimulatedStorage::simulate_crash()` to revert unsynced writes. Restart
//! re-spawns the service inside the madsim node via `Handle::restart`.
//!
//! # Why this exists
//!
//! The previous implementation maintained a custom `BTreeSet<(NodeId,
//! NodeId)>` of partitioned helix-NodeId pairs that the custom mpsc
//! transport consulted at delivery time. With the `Endpoint`-based
//! transport, partitions are enforced by madsim's network simulator, and
//! this file is the translation layer between helix's `NodeId`-keyed API
//! and madsim's `MadsimNodeId`-keyed API.

use std::sync::Arc;

use helix_core::NodeId;
use helix_server::kafka::KafkaHandler;
use helix_wal::FaultConfig;
use madsim::net::{Endpoint, NetSim};
use madsim::runtime::Handle;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::madsim_endpoint_transport::{spawn_receive_loops, MadSimEndpointTransport};

use super::cluster::{E2ECluster, E2ENode};

impl E2ECluster {
    // ========================================================================
    // Fault Injection
    // ========================================================================

    /// Partitions the given nodes from each other (bidirectional clog).
    pub fn partition(&self, nodes: &[NodeId]) {
        let net = NetSim::current();
        let mut clogged = self.clogged_links.lock().expect("lock poisoned");
        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                let a = nodes[i];
                let b = nodes[j];
                let m_a = *self.madsim_node_ids.get(&a).expect("madsim node id missing");
                let m_b = *self.madsim_node_ids.get(&b).expect("madsim node id missing");
                net.clog_link(m_a, m_b);
                net.clog_link(m_b, m_a);
                clogged.insert((a, b));
                clogged.insert((b, a));
            }
        }
        info!(?nodes, "Network partition created");
    }

    /// Heals the partition between the given nodes (bidirectional unclog).
    pub fn heal(&self, nodes: &[NodeId]) {
        let net = NetSim::current();
        let mut clogged = self.clogged_links.lock().expect("lock poisoned");
        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                let a = nodes[i];
                let b = nodes[j];
                let m_a = *self.madsim_node_ids.get(&a).expect("madsim node id missing");
                let m_b = *self.madsim_node_ids.get(&b).expect("madsim node id missing");
                net.unclog_link(m_a, m_b);
                net.unclog_link(m_b, m_a);
                clogged.remove(&(a, b));
                clogged.remove(&(b, a));
            }
        }
        info!(?nodes, "Network partition healed");
    }

    /// Heals all partitions in the cluster.
    pub fn heal_all(&self) {
        for i in 0..self.node_ids.len() {
            for j in (i + 1)..self.node_ids.len() {
                self.heal(&[self.node_ids[i], self.node_ids[j]]);
            }
        }
        info!("All network partitions healed");
    }

    /// Crashes a node (network-isolated + reverts unsynced storage).
    ///
    /// The node's `HelixService` keeps running but its madsim node is
    /// clogged, so all incoming/outgoing traffic is dropped. Use
    /// `restart_node()` for full crash recovery with WAL replay.
    pub fn crash_node(&self, node_id: NodeId) {
        if let Some(node) = self.nodes.get(&node_id) {
            node.storage.simulate_crash();
        }
        let m_id = *self
            .madsim_node_ids
            .get(&node_id)
            .expect("madsim node id missing");
        NetSim::current().clog_node(m_id);
        self.crashed_nodes
            .lock()
            .expect("lock poisoned")
            .insert(node_id);
        info!(
            node = node_id.get(),
            "Node crashed (storage crash-reverted, network isolated)"
        );
    }

    /// Recovers a crashed node (re-enables network, no WAL replay).
    pub fn recover_node(&self, node_id: NodeId) {
        let m_id = *self
            .madsim_node_ids
            .get(&node_id)
            .expect("madsim node id missing");
        NetSim::current().unclog_node(m_id);
        self.crashed_nodes
            .lock()
            .expect("lock poisoned")
            .remove(&node_id);
        info!(node = node_id.get(), "Node recovered (network only)");
    }

    /// Restarts a crashed node with a fresh `HelixService` (full WAL replay).
    ///
    /// The madsim node is restarted via `Handle::restart`, killing any
    /// in-flight tasks. The new service is built inside the same madsim node
    /// (preserving its IP/identity for `clog_link`-based partition routing)
    /// and replays the WAL from the crash-reverted storage.
    pub async fn restart_node(&mut self, node_id: NodeId) {
        use helix_core::WriteDurability;
        use helix_server::service::HelixService;

        let storage = self
            .nodes
            .get(&node_id)
            .expect("node must exist to restart")
            .storage
            .clone();
        let node_data_dir = self
            .base_data_dir
            .as_ref()
            .map(|base| base.join(format!("node-{}", node_id.get())));

        // Drop old E2ENode (releases handler/service Arcs; tasks die when
        // their owning madsim node is restarted below).
        self.nodes.remove(&node_id);

        let m_id = *self
            .madsim_node_ids
            .get(&node_id)
            .expect("madsim node id missing");
        let addr = *self.peers.get(&node_id).expect("peer addr missing");

        let madsim_handle = Handle::current();
        // Restart the madsim node: kills all its tasks, allows new spawns.
        // The node retains its NodeId and bound IP.
        madsim_handle.restart(m_id);
        // madsim's `Handle::restart` only swaps the task-scheduler state for
        // the node — it does NOT clear the network simulator's per-node
        // socket table or the clog-node sets. Without these two calls the
        // new spawn's `Endpoint::bind(addr)` returns `AddrInUse` (the
        // crashed node's socket entry is still there), and even if bind
        // somehow succeeded the node would remain network-isolated from
        // the prior `crash_node`'s `clog_node`. `Handle::kill`'s code path
        // (`kill_id` in madsim/src/sim/task/mod.rs) calls `sim.reset_node`
        // through every registered simulator; `restart` does not. We
        // replicate the cleanup explicitly here.
        let net = NetSim::current();
        net.reset_node(m_id);
        net.unclog_node(m_id);
        let madsim_node = madsim_handle
            .get_node(m_id)
            .expect("madsim node missing after restart");

        let peers = self.peers.clone();
        let node_ids = self.node_ids.clone();
        let mailbox_capacity = self.config.mailbox_capacity;
        let shared_wal_count = self.config.shared_wal_count;
        let storage_for_service = storage.clone();
        let storage_for_node = storage.clone();
        let node_data_dir_for_spawn = node_data_dir.clone();

        let join = madsim_node.spawn(async move {
            let endpoint = Endpoint::bind(addr).await.expect("bind endpoint");
            let transport = MadSimEndpointTransport::new(node_id, endpoint.clone(), peers);

            let (incoming_tx, incoming_rx) = mpsc::channel(mailbox_capacity);
            spawn_receive_loops(endpoint, incoming_tx);

            let service = HelixService::new_multi_node_with_transport(
                "e2e-cluster".to_string(),
                node_id.get(),
                node_ids,
                transport.clone(),
                incoming_rx,
                node_data_dir_for_spawn.clone(),
                shared_wal_count,
                WriteDurability::Fsync,
                storage_for_service,
            )
            .await;

            let actor_router = service.actor_router().cloned();
            let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);
            let service = Arc::new(service);
            let handler = Arc::new(KafkaHandler::new(
                Arc::clone(&service),
                "127.0.0.1".to_string(),
                9092,
                true,
                1,
            ));

            E2ENode {
                node_id,
                madsim_node_id: m_id,
                service,
                handler,
                storage: storage_for_node,
                transport,
                shutdown_tx,
                data_dir: node_data_dir_for_spawn,
                actor_router,
            }
        });

        let new_node = join.await.expect("restart task panicked");
        self.nodes.insert(node_id, new_node);

        // Clear the crashed flag (madsim node is unclogged by `restart`).
        self.crashed_nodes
            .lock()
            .expect("lock poisoned")
            .remove(&node_id);

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

    fn set_node_latency(&self, node_id: NodeId, _multiplier: u32) {
        // madsim's send_latency is a global Range<Duration>, not per-node.
        // The plan calls for buggify-driven destination-side delay in Phase 2;
        // for now this is a no-op, so existing SlowFollower scenarios produce
        // the same global latency as everything else.
        warn!(
            node = node_id.get(),
            "set_node_latency is a no-op under the Endpoint-based transport \
             (Phase 2 will reintroduce via buggify destination-side delay)"
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
            node_ids: self.node_ids.clone(),
            madsim_node_ids: self.madsim_node_ids.clone(),
            storages,
            crashed_nodes: self.crashed_nodes.clone(),
            clogged_links: self.clogged_links.clone(),
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
            crashed_nodes: self.crashed_nodes.clone(),
            node_ids: self.node_ids.clone(),
            property_state: self.property_state.clone(),
        }
    }
}
