//! Concurrent producer infrastructure (test-only).
//!
//! Handles for background producer tasks that run concurrently with fault
//! injection. These hold only `Arc`-cloned references so the `E2ECluster`
//! owner can inject faults on the main task without contention.

#![cfg(all(test, madsim))]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use helix_core::{NodeId, Offset};
use helix_wal::{FaultConfig, SimulatedStorage};
use madsim::net::NetSim;
use madsim::task::NodeId as MadsimNodeId;

use crate::properties::SharedHelixPropertyState;

use super::cluster::{E2ECluster, E2EHelixService, E2EKafkaHandler};
use super::helpers::create_test_record_batch;

/// Handle for a background producer task that runs concurrently with fault
/// injection. Holds only `Arc`-cloned references so the `E2ECluster` owner
/// can inject faults on the main task without contention.
pub(crate) struct ConcurrentProducerHandle {
    /// Node handlers for producing (`Arc`-cloned from `E2ECluster`).
    pub(crate) handlers: Vec<(NodeId, Arc<E2EKafkaHandler>)>,
    /// Node services for topic_id lookup (`Arc`-cloned from `E2ECluster`).
    pub(crate) services: Vec<(NodeId, Arc<E2EHelixService>)>,
    /// Shared crashed-node bookkeeping (cloned Arc from `E2ECluster`).
    pub(crate) crashed_nodes: Arc<Mutex<BTreeSet<NodeId>>>,
    /// All node IDs.
    pub(crate) node_ids: Vec<NodeId>,
    /// Property state for recording acked produces.
    pub(crate) property_state: SharedHelixPropertyState,
}

impl ConcurrentProducerHandle {
    /// Returns node IDs that are not crashed.
    fn get_available_nodes(&self) -> Vec<NodeId> {
        let crashed = self.crashed_nodes.lock().expect("lock poisoned");
        self.node_ids
            .iter()
            .filter(|&&node_id| !crashed.contains(&node_id))
            .copied()
            .collect()
    }

    /// Produces a record, retrying across available nodes.
    /// Returns `Ok(offset)` on ack, `Err(reason)` on failure.
    async fn produce(
        &self,
        topic: &str,
        partition: u32,
        record_batch: Bytes,
    ) -> Result<Offset, String> {
        use helix_server::kafka::KafkaError;

        #[allow(clippy::cast_possible_wrap)]
        let partition_i32 = partition as i32;

        let available_nodes = self.get_available_nodes();
        let mut current_node_id = available_nodes
            .first()
            .or_else(|| self.node_ids.first())
            .copied();
        let mut tried_nodes = std::collections::BTreeSet::new();
        let mut last_error = String::new();

        for _attempt in 0..5 {
            let Some(node_id) = current_node_id else {
                return Err("No node available".to_string());
            };

            let Some((_, handler)) = self.handlers.iter().find(|(id, _)| *id == node_id) else {
                return Err(format!("Node {} not found", node_id.get()));
            };

            tried_nodes.insert(node_id);

            match handler
                .produce(topic, partition_i32, record_batch.clone())
                .await
            {
                Ok(offset) => return Ok(Offset::new(offset)),
                Err(KafkaError::Protocol { error_code, .. })
                    if error_code == 6 || error_code == 3 || error_code == 5 || error_code == -1 =>
                {
                    // NOT_LEADER (6), UNKNOWN_TOPIC (3), LEADER_NOT_AVAILABLE (5), UNKNOWN (-1).
                    current_node_id = available_nodes
                        .iter()
                        .find(|&&n| !tried_nodes.contains(&n))
                        .copied()
                        .or_else(|| {
                            let idx = self
                                .node_ids
                                .iter()
                                .position(|&n| n == node_id)
                                .unwrap_or(0);
                            self.node_ids.get((idx + 1) % self.node_ids.len()).copied()
                        });
                    last_error = format!("retriable error_code={error_code}");
                }
                Err(KafkaError::Protocol {
                    error_code,
                    ref message,
                }) if error_code == 9 =>
                {
                    // BROKER_NOT_AVAILABLE — try another node.
                    current_node_id = available_nodes
                        .iter()
                        .find(|&&n| !tried_nodes.contains(&n))
                        .copied();
                    last_error = format!("broker_not_available: {message}");
                }
                Err(e) => {
                    return Err(format!("produce failed: {e}"));
                }
            }
        }

        Err(format!("no leader after retries: {last_error}"))
    }

    /// Produces with retries, sleeping between attempts to let Raft advance.
    async fn produce_with_retry(
        &self,
        topic: &str,
        partition: u32,
        record_batch: Bytes,
        max_retries: u32,
    ) -> Result<Offset, String> {
        let mut last_error = String::new();
        for _attempt in 0..max_retries {
            match self.produce(topic, partition, record_batch.clone()).await {
                Ok(offset) => return Ok(offset),
                Err(e) => {
                    last_error = e;
                    madsim::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
        Err(format!(
            "produce failed after {max_retries} retries: {last_error}"
        ))
    }

    /// Produces a record, retries across nodes, and records the ack in
    /// property state for later verification.
    async fn produce_and_track(
        &self,
        topic: &str,
        partition: u32,
        data: Bytes,
        max_retries: u32,
    ) -> Result<Offset, String> {
        let payload_hash = E2ECluster::simple_hash(&data);
        let record_batch = create_test_record_batch(&data);

        let offset = self
            .produce_with_retry(topic, partition, record_batch, max_retries)
            .await?;

        // Look up topic_id from controller state — try all services.
        let topic_id = {
            let mut found = None;
            for (_, service) in &self.services {
                let state = service.controller_state().read().await;
                if let Some(info) = state.get_topic(topic) {
                    found = Some(info.topic_id.get());
                    break;
                }
            }
            found
        };

        let Some(topic_id) = topic_id else {
            return Err(format!("topic_id lookup failed for topic={topic}"));
        };

        if let Ok(mut state) = self.property_state.lock() {
            state.record_client_ack(topic_id, u64::from(partition), offset.get(), payload_hash);
            state.record_expected_payload(topic_id, u64::from(partition), offset.get(), data);
        }

        Ok(offset)
    }
}

/// Lightweight handle for injecting faults from a spawned task.
///
/// Implements `FaultInjectable` so it can be used with `ScenarioExecutor`.
/// Only holds `Arc`-cloned references, so the `E2ECluster` owner can
/// still access the cluster after creating this handle.
pub(crate) struct FaultInjectorHandle {
    /// All helix node IDs.
    pub(crate) node_ids: Vec<NodeId>,
    /// helix → madsim NodeId map for clog_link/clog_node translation.
    pub(crate) madsim_node_ids: BTreeMap<NodeId, MadsimNodeId>,
    /// Per-node storage for `simulate_crash()`.
    pub(crate) storages: Vec<(NodeId, SimulatedStorage)>,
    /// Shared crashed-node bookkeeping (cloned Arc from `E2ECluster`).
    pub(crate) crashed_nodes: Arc<Mutex<BTreeSet<NodeId>>>,
    /// Shared clogged-link bookkeeping (cloned Arc from `E2ECluster`).
    pub(crate) clogged_links: Arc<Mutex<BTreeSet<(NodeId, NodeId)>>>,
}

impl crate::madsim_scenarios::FaultInjectable for FaultInjectorHandle {
    fn node_ids(&self) -> &[NodeId] {
        &self.node_ids
    }

    fn partition(&self, nodes: &[NodeId]) {
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
    }

    fn heal(&self, nodes: &[NodeId]) {
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
    }

    fn crash_node(&self, node_id: NodeId) {
        if let Some((_, storage)) = self.storages.iter().find(|(id, _)| *id == node_id) {
            storage.simulate_crash();
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
    }

    fn recover_node(&self, node_id: NodeId) {
        let m_id = *self
            .madsim_node_ids
            .get(&node_id)
            .expect("madsim node id missing");
        NetSim::current().unclog_node(m_id);
        self.crashed_nodes
            .lock()
            .expect("lock poisoned")
            .remove(&node_id);
    }

    fn set_storage_faults(&self, node_id: NodeId, config: FaultConfig) {
        if let Some((_, storage)) = self.storages.iter().find(|(id, _)| *id == node_id) {
            *storage.fault_config() = config;
        }
    }

    fn set_node_latency(&self, _node_id: NodeId, _multiplier: u32) {
        // No-op under Endpoint-based transport (madsim's send_latency is a
        // global Range). Phase 2 will reintroduce this via buggify.
    }
}

/// Result from a concurrent background producer task.
pub(crate) struct ProducerResult {
    /// Number of records successfully acked.
    pub(crate) acked: u64,
    /// Number of records that failed (timeout, no leader, etc.).
    pub(crate) failed: u64,
}

/// Background producer task that continuously pushes records concurrently
/// with fault injection on the main task.
///
/// Runs until signaled to stop via the shutdown channel. Under MadSim's
/// deterministic scheduler, this task interleaves with the fault injection
/// loop whenever the main task calls `sleep()`.
pub(crate) async fn background_producer_task(
    handle: ConcurrentProducerHandle,
    topic: String,
    partition_count: u32,
    produce_interval: Duration,
    producer_id: u32,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> ProducerResult {
    let mut result = ProducerResult { acked: 0, failed: 0 };
    let mut record_num: u64 = 0;

    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        let partition = (record_num as u32) % partition_count;
        let payload = format!("concurrent-p{producer_id}-r{record_num}");

        match handle
            .produce_and_track(&topic, partition, Bytes::from(payload), 5)
            .await
        {
            Ok(_offset) => result.acked += 1,
            Err(_) => result.failed += 1,
        }

        record_num += 1;
        madsim::time::sleep(produce_interval).await;
    }

    result
}
