//! Workload generation and execution.
//!
//! Provides deterministic workload generation with various patterns for
//! testing different aspects of Kafka semantics.

use std::time::{Duration, Instant};

use bytes::Bytes;
use hdrhistogram::Histogram;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::executor::WorkloadExecutor;
use crate::history::History;
use crate::operation::{OperationKind, OperationResult};
use crate::payload::VerifiablePayload;
use crate::verification::{Verification, Violation};

/// Configuration for a topic.
#[derive(Debug, Clone)]
pub struct TopicConfig {
    /// Topic name.
    pub name: String,
    /// Number of partitions.
    pub partitions: i32,
    /// Replication factor.
    pub replication_factor: i16,
}

impl TopicConfig {
    /// Creates a new topic configuration.
    #[must_use]
    pub fn new(name: impl Into<String>, partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.into(),
            partitions,
            replication_factor,
        }
    }
}

/// Distribution for message sizes.
#[derive(Debug, Clone)]
pub enum SizeDistribution {
    /// Fixed size for all messages.
    Fixed(usize),
    /// Uniform distribution between min and max.
    Uniform {
        /// Minimum size.
        min: usize,
        /// Maximum size.
        max: usize,
    },
    /// Bimodal distribution (small and large messages).
    Bimodal {
        /// Size of small messages.
        small: usize,
        /// Size of large messages.
        large: usize,
        /// Probability of large message (0.0 to 1.0).
        large_ratio: f64,
    },
}

impl SizeDistribution {
    /// Samples a size from the distribution.
    #[must_use]
    pub fn sample<R: Rng>(&self, rng: &mut R) -> usize {
        match self {
            Self::Fixed(size) => *size,
            Self::Uniform { min, max } => rng.gen_range(*min..=*max),
            Self::Bimodal {
                small,
                large,
                large_ratio,
            } => {
                if rng.gen::<f64>() < *large_ratio {
                    *large
                } else {
                    *small
                }
            }
        }
    }
}

impl Default for SizeDistribution {
    fn default() -> Self {
        Self::Fixed(100)
    }
}

/// Workload pattern to execute.
#[derive(Debug, Clone)]
#[derive(Default)]
pub enum WorkloadPattern {
    /// Sequential writes to single partition, then read back.
    #[default]
    Sequential,

    /// Multiple producers writing concurrently.
    ConcurrentProducers {
        /// Number of concurrent producers.
        producers: u32,
        /// Keys per producer.
        keys_per_producer: u32,
    },

    /// Producers and consumers running simultaneously.
    ProducerConsumer {
        /// Target producer rate (ops/sec, 0 = unlimited).
        producer_rate: u32,
        /// Number of consumers.
        consumer_count: u32,
    },

    /// Consumer crash and resume pattern.
    ConsumerResume {
        /// Commit every N messages.
        commit_interval: u32,
        /// Simulate crash after N messages.
        crash_after: u32,
    },

    /// High throughput stress test.
    HighThroughput {
        /// Target rate (ops/sec, 0 = unlimited).
        target_rate: u32,
        /// Duration in seconds.
        duration_secs: u32,
    },

    /// Many partitions with leader distribution.
    ManyPartitions {
        /// Number of partitions to use.
        partition_count: i32,
        /// Operations per partition.
        operations_per_partition: u32,
    },
}


/// Workload configuration.
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    /// Random seed for deterministic generation.
    pub seed: u64,
    /// Topics to use.
    pub topics: Vec<TopicConfig>,
    /// Number of operations to generate (for non-duration patterns).
    pub operations: u64,
    /// Workload pattern.
    pub pattern: WorkloadPattern,
    /// Message size distribution.
    pub message_size: SizeDistribution,
    /// Consumer group ID for consumer operations.
    pub consumer_group: String,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            topics: vec![TopicConfig::new("test-topic", 1, 1)],
            operations: 1000,
            pattern: WorkloadPattern::default(),
            message_size: SizeDistribution::default(),
            consumer_group: "test-group".to_string(),
        }
    }
}

/// Builder for creating workloads.
#[derive(Debug, Default)]
pub struct WorkloadBuilder {
    config: WorkloadConfig,
}

impl WorkloadBuilder {
    /// Creates a new workload builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the random seed.
    #[must_use]
    pub const fn seed(mut self, seed: u64) -> Self {
        self.config.seed = seed;
        self
    }

    /// Sets the topics.
    #[must_use]
    pub fn topics(mut self, topics: Vec<TopicConfig>) -> Self {
        self.config.topics = topics;
        self
    }

    /// Sets a single topic (replaces any existing topics).
    #[must_use]
    pub fn topic(mut self, name: impl Into<String>, partitions: i32) -> Self {
        // Clear existing topics and set this one.
        self.config.topics = vec![TopicConfig::new(name, partitions, 3)];
        self
    }

    /// Sets the number of operations.
    #[must_use]
    pub const fn operations(mut self, operations: u64) -> Self {
        self.config.operations = operations;
        self
    }

    /// Sets the workload pattern.
    #[must_use]
    pub const fn pattern(mut self, pattern: WorkloadPattern) -> Self {
        self.config.pattern = pattern;
        self
    }

    /// Sets the message size distribution.
    #[must_use]
    pub const fn message_size(mut self, size: SizeDistribution) -> Self {
        self.config.message_size = size;
        self
    }

    /// Sets the consumer group.
    #[must_use]
    pub fn consumer_group(mut self, group: impl Into<String>) -> Self {
        self.config.consumer_group = group.into();
        self
    }

    /// Builds the workload.
    #[must_use]
    pub fn build(self) -> Workload {
        Workload::new(self.config)
    }
}

/// A verifiable workload for testing.
pub struct Workload {
    config: WorkloadConfig,
    rng: ChaCha8Rng,
    history: History,
    sequence: u64,
}

impl Workload {
    /// Creates a new workload from configuration.
    #[must_use]
    pub fn new(config: WorkloadConfig) -> Self {
        let rng = ChaCha8Rng::seed_from_u64(config.seed);
        Self {
            config,
            rng,
            history: History::new(),
            sequence: 0,
        }
    }

    /// Creates a workload builder.
    #[must_use]
    pub fn builder() -> WorkloadBuilder {
        WorkloadBuilder::new()
    }

    /// Returns the workload configuration.
    #[must_use]
    pub const fn config(&self) -> &WorkloadConfig {
        &self.config
    }

    /// Returns the history of executed operations.
    #[must_use]
    pub const fn history(&self) -> &History {
        &self.history
    }

    /// Runs the workload against an executor.
    ///
    /// # Panics
    ///
    /// Panics if histogram creation fails (should not happen with valid parameters).
    #[allow(clippy::future_not_send)]
    #[allow(clippy::cast_precision_loss)]
    #[allow(clippy::cast_possible_truncation)] // duration_ms won't overflow u64.
    pub async fn run<E: WorkloadExecutor>(&mut self, executor: &E) -> WorkloadStats {
        let start = Instant::now();
        let mut send_latencies = Histogram::<u64>::new(3).expect("histogram creation");
        let mut poll_latencies = Histogram::<u64>::new(3).expect("histogram creation");

        match &self.config.pattern {
            WorkloadPattern::HighThroughput {
                target_rate,
                duration_secs,
            } => {
                self.run_high_throughput(
                    executor,
                    *target_rate,
                    *duration_secs,
                    &mut send_latencies,
                )
                .await;
            }
            WorkloadPattern::ManyPartitions {
                partition_count,
                operations_per_partition,
            } => {
                self.run_many_partitions(
                    executor,
                    *partition_count,
                    *operations_per_partition,
                    &mut send_latencies,
                    &mut poll_latencies,
                )
                .await;
            }
            WorkloadPattern::ConcurrentProducers {
                producers,
                keys_per_producer,
            } => {
                self.run_concurrent_producers(
                    executor,
                    *producers,
                    *keys_per_producer,
                    &mut send_latencies,
                    &mut poll_latencies,
                )
                .await;
            }
            WorkloadPattern::ProducerConsumer {
                producer_rate,
                consumer_count,
            } => {
                self.run_producer_consumer(
                    executor,
                    *producer_rate,
                    *consumer_count,
                    &mut send_latencies,
                    &mut poll_latencies,
                )
                .await;
            }
            // Sequential and other patterns use the sequential implementation.
            WorkloadPattern::Sequential | WorkloadPattern::ConsumerResume { .. } => {
                self.run_sequential(executor, &mut send_latencies, &mut poll_latencies)
                    .await;
            }
        }

        let duration = start.elapsed();
        let violations = Verification::verify(&self.history);
        let stats = self.history.stats();

        WorkloadStats {
            operations_total: stats.total_ops,
            operations_ok: stats.successful_ops,
            operations_failed: stats.failed_ops,
            sends_ok: stats.send_count,
            polls_ok: stats.poll_count,
            send_latency_p50: send_latencies.value_at_percentile(50.0) as f64 / 1000.0,
            send_latency_p95: send_latencies.value_at_percentile(95.0) as f64 / 1000.0,
            send_latency_p99: send_latencies.value_at_percentile(99.0) as f64 / 1000.0,
            send_latency_max: send_latencies.max() as f64 / 1000.0,
            poll_latency_p50: poll_latencies.value_at_percentile(50.0) as f64 / 1000.0,
            poll_latency_p95: poll_latencies.value_at_percentile(95.0) as f64 / 1000.0,
            poll_latency_p99: poll_latencies.value_at_percentile(99.0) as f64 / 1000.0,
            poll_latency_max: poll_latencies.max() as f64 / 1000.0,
            send_throughput_ops: stats.send_count as f64 / duration.as_secs_f64(),
            send_throughput_bytes: 0.0, // TODO: track bytes
            duration_ms: duration.as_millis() as u64,
            violations,
        }
    }

    /// Runs sequential pattern: write N messages, then read them all back.
    #[allow(clippy::future_not_send)]
    #[allow(clippy::cast_possible_truncation)] // high_watermark bounded by operations count.
    async fn run_sequential<E: WorkloadExecutor>(
        &mut self,
        executor: &E,
        send_latencies: &mut Histogram<u64>,
        poll_latencies: &mut Histogram<u64>,
    ) {
        let topic = self
            .config
            .topics
            .first().map_or_else(|| "test".to_string(), |t| t.name.clone());
        let partition = 0;

        // Send phase.
        for _ in 0..self.config.operations {
            let payload = self.generate_payload();
            let latency = self.execute_send(executor, &topic, partition, payload).await;
            if let Some(us) = latency {
                let _ = send_latencies.record(us);
            }
        }

        // Poll phase: read everything back.
        // Start from the minimum offset we wrote (not 0) to handle persistent storage.
        let high_watermark = self.history.high_watermark(&topic, partition);
        let low_watermark = self.history.low_watermark(&topic, partition).unwrap_or(0);
        if high_watermark > low_watermark {
            #[allow(clippy::cast_possible_truncation)] // bounded by operations count
            let count = (high_watermark - low_watermark) as u32;
            let latency = self
                .execute_poll(executor, &topic, partition, low_watermark, count)
                .await;
            if let Some(us) = latency {
                let _ = poll_latencies.record(us);
            }
        }
    }

    /// Runs high throughput pattern: send as fast as possible for duration.
    #[allow(clippy::future_not_send)]
    async fn run_high_throughput<E: WorkloadExecutor>(
        &mut self,
        executor: &E,
        _target_rate: u32,
        duration_secs: u32,
        send_latencies: &mut Histogram<u64>,
    ) {
        let topic = self
            .config
            .topics
            .first().map_or_else(|| "test".to_string(), |t| t.name.clone());

        let partitions: i32 = self.config.topics.first().map_or(1, |t| t.partitions);

        let deadline = Instant::now() + Duration::from_secs(u64::from(duration_secs));

        while Instant::now() < deadline {
            let partition = self.rng.gen_range(0..partitions);
            let payload = self.generate_payload();
            let latency = self.execute_send(executor, &topic, partition, payload).await;
            if let Some(us) = latency {
                let _ = send_latencies.record(us);
            }
        }
    }

    /// Runs many partitions pattern: distribute load across partitions.
    #[allow(clippy::future_not_send)]
    #[allow(clippy::cast_possible_truncation)] // high_watermark bounded by operations count.
    async fn run_many_partitions<E: WorkloadExecutor>(
        &mut self,
        executor: &E,
        partition_count: i32,
        operations_per_partition: u32,
        send_latencies: &mut Histogram<u64>,
        poll_latencies: &mut Histogram<u64>,
    ) {
        let topic = self
            .config
            .topics
            .first().map_or_else(|| "test".to_string(), |t| t.name.clone());

        // Generate partition order randomly for deterministic but varied access.
        let mut partitions: Vec<i32> = (0..partition_count).collect();
        partitions.shuffle(&mut self.rng);

        // Send to each partition.
        for partition in &partitions {
            for _ in 0..operations_per_partition {
                let payload = self.generate_payload();
                let latency = self.execute_send(executor, &topic, *partition, payload).await;
                if let Some(us) = latency {
                    let _ = send_latencies.record(us);
                }
            }
        }

        // Poll from each partition.
        // Start from the minimum offset we wrote (not 0) to handle persistent storage.
        for partition in &partitions {
            let high_watermark = self.history.high_watermark(&topic, *partition);
            let low_watermark = self.history.low_watermark(&topic, *partition).unwrap_or(0);
            if high_watermark > low_watermark {
                #[allow(clippy::cast_possible_truncation)] // bounded by operations count
                let count = (high_watermark - low_watermark) as u32;
                let latency = self
                    .execute_poll(executor, &topic, *partition, low_watermark, count)
                    .await;
                if let Some(us) = latency {
                    let _ = poll_latencies.record(us);
                }
            }
        }
    }

    /// Runs concurrent producers pattern: multiple producers writing to assigned partitions.
    ///
    /// Simulates multiple producers by distributing operations across partitions.
    /// Each "producer" writes to its assigned partitions in round-robin fashion.
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)] // partitions are positive
    #[allow(clippy::future_not_send)] // Single-threaded context, Send not required.
    async fn run_concurrent_producers<E: WorkloadExecutor>(
        &mut self,
        executor: &E,
        producer_count: u32,
        _keys_per_producer: u32,
        send_latencies: &mut Histogram<u64>,
        poll_latencies: &mut Histogram<u64>,
    ) {
        let topic = self
            .config
            .topics
            .first()
            .map_or_else(|| "test".to_string(), |t| t.name.clone());
        let partitions: i32 = self.config.topics.first().map_or(1, |t| t.partitions);
        let total_ops = self.config.operations;

        // Assign partitions to producers (round-robin).
        let mut producer_partitions: Vec<Vec<i32>> = vec![Vec::new(); producer_count as usize];
        for p in 0..partitions {
            producer_partitions[(p as usize) % (producer_count as usize)].push(p);
        }

        // Simulate concurrent producers by interleaving writes.
        // Each iteration writes one message per producer (if they have ops remaining).
        let ops_per_producer = total_ops / u64::from(producer_count);
        let mut producer_op_counts: Vec<u64> = vec![0; producer_count as usize];

        let mut ops_done = 0u64;
        while ops_done < total_ops {
            for producer_id in 0..producer_count as usize {
                if producer_op_counts[producer_id] >= ops_per_producer {
                    continue;
                }

                let assigned = &producer_partitions[producer_id];
                let local_op = producer_op_counts[producer_id];
                let partition = if assigned.is_empty() {
                    0
                } else {
                    assigned[(local_op as usize) % assigned.len()]
                };

                let payload = self.generate_payload();
                let latency = self
                    .execute_send(executor, &topic, partition, payload)
                    .await;
                if let Some(us) = latency {
                    let _ = send_latencies.record(us);
                }

                producer_op_counts[producer_id] += 1;
                ops_done += 1;

                if ops_done >= total_ops {
                    break;
                }
            }
        }

        // Poll phase: read back all data from each partition.
        for partition in 0..partitions {
            let high_watermark = self.history.high_watermark(&topic, partition);
            let low_watermark = self.history.low_watermark(&topic, partition).unwrap_or(0);
            if high_watermark > low_watermark {
                let count = (high_watermark - low_watermark) as u32;
                let latency = self
                    .execute_poll(executor, &topic, partition, low_watermark, count)
                    .await;
                if let Some(us) = latency {
                    let _ = poll_latencies.record(us);
                }
            }
        }
    }

    /// Runs producer-consumer pattern: produces first, then consumes all data.
    ///
    /// Produces messages at the specified rate (or unlimited if 0), then
    /// reads back all data from all partitions to verify no lost writes.
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::future_not_send)] // Single-threaded context, Send not required.
    async fn run_producer_consumer<E: WorkloadExecutor>(
        &mut self,
        executor: &E,
        producer_rate: u32,
        _consumer_count: u32,
        send_latencies: &mut Histogram<u64>,
        poll_latencies: &mut Histogram<u64>,
    ) {
        let topic = self
            .config
            .topics
            .first()
            .map_or_else(|| "test".to_string(), |t| t.name.clone());
        let partitions: i32 = self.config.topics.first().map_or(1, |t| t.partitions);
        let total_ops = self.config.operations;

        // Production phase with optional rate limiting.
        let interval = if producer_rate > 0 {
            Duration::from_micros(1_000_000 / u64::from(producer_rate))
        } else {
            Duration::ZERO
        };

        for i in 0..total_ops {
            let partition = (i as i32) % partitions;
            let payload = self.generate_payload();

            let latency = self
                .execute_send(executor, &topic, partition, payload)
                .await;
            if let Some(us) = latency {
                let _ = send_latencies.record(us);
            }

            // Rate limiting.
            if !interval.is_zero() {
                tokio::time::sleep(interval).await;
            }
        }

        // Consumption phase: read back all data from all partitions.
        for partition in 0..partitions {
            let high_watermark = self.history.high_watermark(&topic, partition);
            let low_watermark = self.history.low_watermark(&topic, partition).unwrap_or(0);
            if high_watermark > low_watermark {
                let count = (high_watermark - low_watermark) as u32;
                let latency = self
                    .execute_poll(executor, &topic, partition, low_watermark, count)
                    .await;
                if let Some(us) = latency {
                    let _ = poll_latencies.record(us);
                }
            }
        }
    }

    /// Generates a verifiable payload.
    fn generate_payload(&mut self) -> Bytes {
        let size = self.config.message_size.sample(&mut self.rng);
        let seq = self.sequence;
        self.sequence += 1;

        // Use a deterministic timestamp based on sequence.
        let timestamp = seq * 1000;

        let payload = VerifiablePayload::new(seq, seq, timestamp, size, &mut self.rng);
        payload.encode()
    }

    /// Executes a send operation and returns latency in microseconds.
    #[allow(clippy::future_not_send)]
    async fn execute_send<E: WorkloadExecutor>(
        &mut self,
        executor: &E,
        topic: &str,
        partition: i32,
        payload: Bytes,
    ) -> Option<u64> {
        let invoked_at = executor.now();

        let op_id = self.history.record_invoke(
            OperationKind::Send {
                topic: topic.to_string(),
                partition,
                payload: payload.clone(),
            },
            invoked_at,
        );

        let result = executor.send(topic, partition, payload).await;
        let completed_at = executor.now();

        let op_result = match result {
            Ok(offset) => OperationResult::SendOk { offset },
            Err(e) => OperationResult::SendError {
                code: e.error_code(),
                message: e.to_string(),
            },
        };

        self.history.record_complete(op_id, completed_at, op_result);

        Some(completed_at.saturating_sub(invoked_at))
    }

    /// Executes a poll operation and returns latency in microseconds.
    #[allow(clippy::future_not_send)]
    async fn execute_poll<E: WorkloadExecutor>(
        &mut self,
        executor: &E,
        topic: &str,
        partition: i32,
        start_offset: u64,
        max_messages: u32,
    ) -> Option<u64> {
        let invoked_at = executor.now();

        let op_id = self.history.record_invoke(
            OperationKind::Poll {
                topic: topic.to_string(),
                partition,
                start_offset,
                max_messages,
            },
            invoked_at,
        );

        let result = executor.poll(topic, partition, start_offset, max_messages).await;
        let completed_at = executor.now();

        let op_result = match result {
            Ok(messages) => OperationResult::PollOk { messages },
            Err(e) => OperationResult::PollError {
                code: e.error_code(),
                message: e.to_string(),
            },
        };

        self.history.record_complete(op_id, completed_at, op_result);

        Some(completed_at.saturating_sub(invoked_at))
    }

    /// Verifies the workload history and returns any violations.
    #[must_use]
    pub fn verify(&self) -> Vec<Violation> {
        Verification::verify(&self.history)
    }
}

/// Statistics from a workload execution.
#[derive(Debug, Clone)]
pub struct WorkloadStats {
    /// Total operations attempted (sends + polls).
    pub operations_total: u64,
    /// Successful operations (sends + polls).
    pub operations_ok: u64,
    /// Failed operations.
    pub operations_failed: u64,
    /// Successful sends only.
    pub sends_ok: u64,
    /// Successful polls only.
    pub polls_ok: u64,

    /// Send latency p50 in milliseconds.
    pub send_latency_p50: f64,
    /// Send latency p95 in milliseconds.
    pub send_latency_p95: f64,
    /// Send latency p99 in milliseconds.
    pub send_latency_p99: f64,
    /// Send latency max in milliseconds.
    pub send_latency_max: f64,

    /// Poll latency p50 in milliseconds.
    pub poll_latency_p50: f64,
    /// Poll latency p95 in milliseconds.
    pub poll_latency_p95: f64,
    /// Poll latency p99 in milliseconds.
    pub poll_latency_p99: f64,
    /// Poll latency max in milliseconds.
    pub poll_latency_max: f64,

    /// Send throughput in operations per second.
    pub send_throughput_ops: f64,
    /// Send throughput in bytes per second.
    pub send_throughput_bytes: f64,

    /// Total duration in milliseconds.
    pub duration_ms: u64,

    /// Verification violations found.
    pub violations: Vec<Violation>,
}

impl WorkloadStats {
    /// Prints a human-readable summary.
    pub fn print_summary(&self) {
        println!("=== Workload Statistics ===");
        println!(
            "Operations: {} total, {} ok, {} failed (sends: {}, polls: {})",
            self.operations_total, self.operations_ok, self.operations_failed,
            self.sends_ok, self.polls_ok
        );
        println!(
            "Send latency: p50={:.2}ms p95={:.2}ms p99={:.2}ms max={:.2}ms",
            self.send_latency_p50,
            self.send_latency_p95,
            self.send_latency_p99,
            self.send_latency_max
        );
        println!(
            "Poll latency: p50={:.2}ms p95={:.2}ms p99={:.2}ms max={:.2}ms",
            self.poll_latency_p50,
            self.poll_latency_p95,
            self.poll_latency_p99,
            self.poll_latency_max
        );
        println!("Throughput: {:.0} ops/sec", self.send_throughput_ops);
        println!("Duration: {}ms", self.duration_ms);
        println!("Violations: {}", self.violations.len());
        for v in &self.violations {
            println!("  - {v}");
        }
    }
}
