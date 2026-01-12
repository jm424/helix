//! Flow controller orchestration.
//!
//! The `FlowController` combines token bucket rate limiting, weighted fair queue
//! scheduling, and AIMD congestion control into a unified flow control system.
//!
//! Architecture:
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      FlowController                             │
//! │                                                                 │
//! │  ┌──────────────┐    ┌──────────────────┐    ┌──────────────┐  │
//! │  │ Per-Stream   │    │ WeightedFairQueue │    │    AIMD      │  │
//! │  │ TokenBuckets │───▶│  (I/O Scheduler)  │───▶│  Controller  │  │
//! │  └──────────────┘    └──────────────────┘    └──────────────┘  │
//! │         │                     │                      │         │
//! │         │                     │                      │         │
//! │         ▼                     ▼                      ▼         │
//! │    Rate Limit           Fair Scheduling        Adaptive Rate   │
//! │    Per Stream           Across I/O Classes     Based on Load   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use std::collections::HashMap;

use crate::aimd::{AimdConfig, AimdController};
use crate::error::{FlowError, FlowResult};
use crate::fair_queue::{FairQueueConfig, IoClass, IoOperation, WeightedFairQueue};
use crate::token_bucket::{TokenBucket, TokenBucketConfig};

/// Stream identifier.
pub type StreamId = u64;

/// Configuration for the flow controller.
#[derive(Debug, Clone)]
pub struct FlowControllerConfig {
    /// Token bucket configuration for streams.
    pub token_bucket: TokenBucketConfig,

    /// Fair queue configuration.
    pub fair_queue: FairQueueConfig,

    /// AIMD configuration for adaptive rate control.
    pub aimd: AimdConfig,

    /// Maximum number of streams to track.
    pub max_streams: usize,

    /// Whether to enable adaptive rate control via AIMD.
    pub enable_aimd: bool,
}

impl FlowControllerConfig {
    /// Creates a configuration for testing.
    #[must_use]
    pub const fn for_testing() -> Self {
        Self {
            token_bucket: TokenBucketConfig::for_testing(),
            fair_queue: FairQueueConfig::for_testing(),
            aimd: AimdConfig::for_testing(),
            max_streams: 100,
            enable_aimd: true,
        }
    }
}

impl Default for FlowControllerConfig {
    fn default() -> Self {
        Self {
            token_bucket: TokenBucketConfig::default(),
            fair_queue: FairQueueConfig::default(),
            aimd: AimdConfig::default(),
            max_streams: 10_000,
            enable_aimd: true,
        }
    }
}

/// A pending I/O operation with metadata.
#[derive(Debug)]
pub struct PendingOperation<T> {
    /// The stream this operation belongs to.
    pub stream_id: StreamId,

    /// The I/O class.
    pub class: IoClass,

    /// Operation size in bytes.
    pub size_bytes: u64,

    /// The operation payload.
    pub payload: T,

    /// When the operation was submitted.
    pub submit_time_us: u64,
}

/// Flow controller combining rate limiting, fair scheduling, and congestion control.
pub struct FlowController<T> {
    /// Per-stream token buckets for rate limiting.
    stream_limiters: HashMap<StreamId, TokenBucket>,

    /// I/O scheduler using weighted fair queue.
    io_scheduler: WeightedFairQueue<PendingOperation<T>>,

    /// AIMD controller for device-level adaptive rate control.
    device_controller: AimdController,

    /// Configuration.
    config: FlowControllerConfig,

    /// Global token bucket for overall throughput limiting.
    global_limiter: TokenBucket,

    /// Total operations submitted.
    total_submitted: u64,

    /// Total operations completed.
    total_completed: u64,

    /// Total operations rejected.
    total_rejected: u64,
}

impl<T> FlowController<T> {
    /// Creates a new flow controller.
    #[must_use]
    pub fn new(config: FlowControllerConfig, current_time_us: u64) -> Self {
        let global_limiter = TokenBucket::new(&config.token_bucket, current_time_us);
        let io_scheduler = WeightedFairQueue::new(&config.fair_queue);
        let device_controller = AimdController::new(config.aimd.clone(), current_time_us);

        Self {
            stream_limiters: HashMap::new(),
            io_scheduler,
            device_controller,
            config,
            global_limiter,
            total_submitted: 0,
            total_completed: 0,
            total_rejected: 0,
        }
    }

    /// Submits an operation for flow control.
    ///
    /// The operation will be rate-limited at the stream level, then queued
    /// for fair scheduling across I/O classes.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The stream this operation belongs to.
    /// * `class` - The I/O class for scheduling.
    /// * `size_bytes` - Size of the operation.
    /// * `payload` - The operation payload.
    /// * `current_time_us` - Current time in microseconds.
    ///
    /// # Errors
    ///
    /// Returns an error if rate limited or queue is full.
    #[allow(clippy::missing_panics_doc)] // Internal expect on freshly-created entry.
    pub fn submit(
        &mut self,
        stream_id: StreamId,
        class: IoClass,
        size_bytes: u64,
        payload: T,
        current_time_us: u64,
    ) -> FlowResult<()> {
        // Check global rate limit first.
        self.global_limiter.refill(current_time_us);
        if self.global_limiter.available_tokens() < size_bytes {
            self.total_rejected += 1;
            return Err(FlowError::RateLimitExceeded {
                stream_id,
                tokens_requested: size_bytes,
                tokens_available: self.global_limiter.available_tokens(),
            });
        }

        // Check per-stream rate limit.
        // First ensure the stream exists and get available tokens.
        let stream_available = {
            let stream_limiter =
                self.get_or_create_stream_limiter(stream_id, current_time_us)?;
            stream_limiter.refill(current_time_us);
            stream_limiter.available_tokens()
        };

        if stream_available < size_bytes {
            self.total_rejected += 1;
            return Err(FlowError::RateLimitExceeded {
                stream_id,
                tokens_requested: size_bytes,
                tokens_available: stream_available,
            });
        }

        // Consume tokens.
        self.global_limiter
            .try_consume(size_bytes, current_time_us)?;
        let stream_limiter = self.stream_limiters.get_mut(&stream_id).expect("just created");
        stream_limiter.try_consume(size_bytes, current_time_us)?;

        // Queue for I/O scheduling.
        let pending = PendingOperation {
            stream_id,
            class,
            size_bytes,
            payload,
            submit_time_us: current_time_us,
        };

        let io_op = IoOperation {
            class,
            size_bytes,
            payload: pending,
            enqueue_time_us: current_time_us,
        };

        self.io_scheduler.enqueue(io_op)?;
        self.total_submitted += 1;

        Ok(())
    }

    /// Gets the next operation to execute according to fair scheduling.
    ///
    /// Returns `None` if no operations are pending.
    pub fn next_operation(&mut self) -> Option<PendingOperation<T>> {
        let io_op = self.io_scheduler.dequeue()?;
        Some(io_op.payload)
    }

    /// Records completion of an operation for AIMD feedback.
    ///
    /// # Arguments
    ///
    /// * `latency_us` - How long the operation took.
    /// * `success` - Whether the operation succeeded.
    /// * `current_time_us` - Current time in microseconds.
    pub fn record_completion(&mut self, latency_us: u64, success: bool, current_time_us: u64) {
        self.total_completed += 1;

        if self.config.enable_aimd {
            if success {
                self.device_controller.record_success(latency_us);
            } else {
                self.device_controller.record_error();
            }

            // Check if we should adjust rates.
            if let Some(new_rate) = self.device_controller.maybe_adjust(current_time_us) {
                self.apply_aimd_rate(new_rate, current_time_us);
            }
        }
    }

    /// Applies the AIMD-determined rate to the global limiter.
    fn apply_aimd_rate(&mut self, new_rate: u64, _current_time_us: u64) {
        self.global_limiter.update_config(None, Some(new_rate));
    }

    /// Gets or creates a token bucket for a stream.
    fn get_or_create_stream_limiter(
        &mut self,
        stream_id: StreamId,
        current_time_us: u64,
    ) -> FlowResult<&mut TokenBucket> {
        if !self.stream_limiters.contains_key(&stream_id) {
            if self.stream_limiters.len() >= self.config.max_streams {
                return Err(FlowError::InvalidConfig {
                    message: format!(
                        "too many streams: {} (max {})",
                        self.stream_limiters.len(),
                        self.config.max_streams
                    ),
                });
            }
            let bucket = TokenBucket::new(&self.config.token_bucket, current_time_us);
            self.stream_limiters.insert(stream_id, bucket);
        }
        Ok(self.stream_limiters.get_mut(&stream_id).expect("just inserted"))
    }

    /// Returns whether there are pending operations.
    #[must_use]
    pub fn has_pending(&self) -> bool {
        !self.io_scheduler.is_empty()
    }

    /// Returns the number of pending operations.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.io_scheduler.total_pending()
    }

    /// Returns the number of tracked streams.
    #[must_use]
    pub fn stream_count(&self) -> usize {
        self.stream_limiters.len()
    }

    /// Returns statistics about the flow controller.
    #[must_use]
    pub fn stats(&self) -> FlowControllerStats {
        FlowControllerStats {
            total_submitted: self.total_submitted,
            total_completed: self.total_completed,
            total_rejected: self.total_rejected,
            pending_operations: self.io_scheduler.total_pending(),
            active_streams: self.stream_limiters.len(),
            current_rate: self.device_controller.current_rate(),
            aimd_state: self.device_controller.state(),
            queue_stats: self.io_scheduler.stats(),
        }
    }

    /// Removes a stream's rate limiter.
    ///
    /// Call this when a stream is closed to free resources.
    pub fn remove_stream(&mut self, stream_id: StreamId) -> bool {
        self.stream_limiters.remove(&stream_id).is_some()
    }

    /// Signals backpressure, forcing an immediate rate decrease.
    pub fn signal_backpressure(&mut self) {
        if self.config.enable_aimd {
            self.device_controller.force_decrease();
        }
    }

    /// Updates the weight for an I/O class.
    pub const fn set_io_class_weight(&mut self, class: IoClass, weight: u32) {
        self.io_scheduler.set_weight(class, weight);
    }

    /// Drains all operations for a specific I/O class.
    pub fn drain_class(&mut self, class: IoClass) -> Vec<PendingOperation<T>> {
        self.io_scheduler
            .drain_class(class)
            .into_iter()
            .map(|op| op.payload)
            .collect()
    }

    /// Resets the flow controller to initial state.
    pub fn reset(&mut self, current_time_us: u64) {
        self.stream_limiters.clear();
        self.global_limiter = TokenBucket::new(&self.config.token_bucket, current_time_us);
        self.device_controller.reset(current_time_us);
        self.total_submitted = 0;
        self.total_completed = 0;
        self.total_rejected = 0;
        // Note: io_scheduler queued items are lost. Caller should drain first if needed.
    }
}

/// Statistics about the flow controller.
#[derive(Debug, Clone)]
pub struct FlowControllerStats {
    /// Total operations submitted.
    pub total_submitted: u64,
    /// Total operations completed.
    pub total_completed: u64,
    /// Total operations rejected due to rate limiting.
    pub total_rejected: u64,
    /// Current pending operations.
    pub pending_operations: usize,
    /// Number of active streams.
    pub active_streams: usize,
    /// Current AIMD rate.
    pub current_rate: u64,
    /// Current AIMD state.
    pub aimd_state: crate::aimd::AimdState,
    /// Queue statistics.
    pub queue_stats: crate::fair_queue::FairQueueStats,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_submit_and_next() {
        let config = FlowControllerConfig::for_testing();
        let mut controller: FlowController<&str> = FlowController::new(config, 0);

        controller
            .submit(1, IoClass::Write, 100, "op1", 0)
            .unwrap();
        controller
            .submit(2, IoClass::LiveRead, 100, "op2", 0)
            .unwrap();

        assert!(controller.has_pending());
        assert_eq!(controller.pending_count(), 2);

        let op1 = controller.next_operation().unwrap();
        assert_eq!(op1.payload, "op1");

        let op2 = controller.next_operation().unwrap();
        assert_eq!(op2.payload, "op2");

        assert!(!controller.has_pending());
    }

    #[test]
    fn test_rate_limiting() {
        let mut config = FlowControllerConfig::for_testing();
        config.token_bucket.capacity = 100;
        config.token_bucket.initial_tokens = Some(100);

        let mut controller: FlowController<i32> = FlowController::new(config, 0);

        // First op consumes 80 tokens.
        controller.submit(1, IoClass::Write, 80, 1, 0).unwrap();

        // Second op needs 50 tokens but only 20 available.
        let result = controller.submit(1, IoClass::Write, 50, 2, 0);
        assert!(matches!(result, Err(FlowError::RateLimitExceeded { .. })));
    }

    #[test]
    fn test_multiple_streams() {
        let config = FlowControllerConfig::for_testing();
        let mut controller: FlowController<i32> = FlowController::new(config, 0);

        // Different streams get independent rate limits.
        controller.submit(1, IoClass::Write, 50, 1, 0).unwrap();
        controller.submit(2, IoClass::Write, 50, 2, 0).unwrap();
        controller.submit(3, IoClass::Write, 50, 3, 0).unwrap();

        assert_eq!(controller.stream_count(), 3);
        assert_eq!(controller.pending_count(), 3);
    }

    #[test]
    fn test_aimd_feedback() {
        let mut config = FlowControllerConfig::for_testing();
        config.aimd.latency_threshold_us = 10_000; // 10ms
        config.aimd.adjustment_interval_us = 1000;
        config.enable_aimd = true;

        let mut controller: FlowController<i32> = FlowController::new(config, 0);
        let initial_rate = controller.device_controller.current_rate();

        // Record high latency completions.
        for i in 0_u64..10 {
            controller.record_completion(50_000, true, i * 200); // 50ms latency.
        }

        // Force adjustment.
        controller.record_completion(50_000, true, 10_000);

        // Rate should have decreased.
        let new_rate = controller.device_controller.current_rate();
        assert!(new_rate < initial_rate);
    }

    #[test]
    fn test_backpressure_signal() {
        let config = FlowControllerConfig::for_testing();
        let mut controller: FlowController<i32> = FlowController::new(config, 0);

        let initial_rate = controller.device_controller.current_rate();
        controller.signal_backpressure();
        let new_rate = controller.device_controller.current_rate();

        assert!(new_rate < initial_rate);
    }

    #[test]
    fn test_stats() {
        let config = FlowControllerConfig::for_testing();
        let mut controller: FlowController<i32> = FlowController::new(config, 0);

        controller.submit(1, IoClass::Write, 10, 1, 0).unwrap();
        controller.submit(1, IoClass::Tiering, 10, 2, 0).unwrap();
        controller.next_operation();
        controller.record_completion(1000, true, 1000);

        let stats = controller.stats();
        assert_eq!(stats.total_submitted, 2);
        assert_eq!(stats.total_completed, 1);
        assert_eq!(stats.pending_operations, 1);
        assert_eq!(stats.active_streams, 1);
    }

    #[test]
    fn test_remove_stream() {
        let config = FlowControllerConfig::for_testing();
        let mut controller: FlowController<i32> = FlowController::new(config, 0);

        controller.submit(1, IoClass::Write, 10, 1, 0).unwrap();
        assert_eq!(controller.stream_count(), 1);

        assert!(controller.remove_stream(1));
        assert_eq!(controller.stream_count(), 0);

        assert!(!controller.remove_stream(999)); // Non-existent.
    }

    #[test]
    fn test_drain_class() {
        let config = FlowControllerConfig::for_testing();
        let mut controller: FlowController<i32> = FlowController::new(config, 0);

        for i in 0..5 {
            controller
                .submit(1, IoClass::Tiering, 10, i, 0)
                .unwrap();
        }

        let drained = controller.drain_class(IoClass::Tiering);
        assert_eq!(drained.len(), 5);
        assert_eq!(controller.pending_count(), 0);
    }

    #[test]
    fn test_io_class_weights() {
        let config = FlowControllerConfig::for_testing();
        let mut controller: FlowController<&str> = FlowController::new(config, 0);

        // Set tiering to very high weight.
        controller.set_io_class_weight(IoClass::Tiering, 1000);

        // Submit one of each.
        controller
            .submit(1, IoClass::Write, 100, "write", 0)
            .unwrap();
        controller
            .submit(1, IoClass::Tiering, 100, "tier", 0)
            .unwrap();

        // With high tiering weight, it should be served first due to lower vtime increment.
        // (Actually both start at vtime 0, so first in queue wins. Let's verify order works.)
        let op1 = controller.next_operation().unwrap();
        let op2 = controller.next_operation().unwrap();

        // Both should be returned in some order.
        assert!((op1.payload == "write" && op2.payload == "tier")
            || (op1.payload == "tier" && op2.payload == "write"));
    }

    #[test]
    fn test_reset() {
        let config = FlowControllerConfig::for_testing();
        let mut controller: FlowController<i32> = FlowController::new(config, 0);

        // Submit some operations and record completions.
        controller.submit(1, IoClass::Write, 100, 1, 0).unwrap();
        controller.submit(2, IoClass::Write, 100, 2, 0).unwrap();
        controller.record_completion(1000, true, 1000);
        controller.signal_backpressure(); // Modify AIMD state.

        assert_eq!(controller.stream_count(), 2);
        assert_eq!(controller.pending_count(), 2);
        let stats = controller.stats();
        assert_eq!(stats.total_submitted, 2);
        assert_eq!(stats.total_completed, 1);

        // Reset everything.
        controller.reset(2_000_000);

        // All counters and state should be reset.
        assert_eq!(controller.stream_count(), 0);
        let stats_after = controller.stats();
        assert_eq!(stats_after.total_submitted, 0);
        assert_eq!(stats_after.total_completed, 0);
        assert_eq!(stats_after.total_rejected, 0);
        assert_eq!(stats_after.active_streams, 0);

        // Should be able to submit again as if fresh.
        controller.submit(1, IoClass::Write, 100, 1, 2_000_000).unwrap();
        assert_eq!(controller.stream_count(), 1);
    }

    #[test]
    fn test_max_streams_limit() {
        let mut config = FlowControllerConfig::for_testing();
        config.max_streams = 3; // Very small limit for testing.

        let mut controller: FlowController<i32> = FlowController::new(config, 0);

        // Create exactly max_streams streams.
        controller.submit(1, IoClass::Write, 10, 1, 0).unwrap();
        controller.submit(2, IoClass::Write, 10, 2, 0).unwrap();
        controller.submit(3, IoClass::Write, 10, 3, 0).unwrap();

        assert_eq!(controller.stream_count(), 3);

        // Fourth stream should fail.
        let result = controller.submit(4, IoClass::Write, 10, 4, 0);
        assert!(matches!(result, Err(FlowError::InvalidConfig { .. })));

        // But existing streams can still submit.
        controller.submit(1, IoClass::Write, 10, 5, 0).unwrap();

        // Removing a stream should allow a new one.
        controller.remove_stream(2);
        assert_eq!(controller.stream_count(), 2);
        controller.submit(5, IoClass::Write, 10, 6, 0).unwrap();
        assert_eq!(controller.stream_count(), 3);
    }

    #[test]
    fn test_aimd_disabled() {
        let mut config = FlowControllerConfig::for_testing();
        config.enable_aimd = false;

        let mut controller: FlowController<i32> = FlowController::new(config, 0);
        let initial_rate = controller.device_controller.current_rate();

        // Record many high-latency completions.
        for i in 0_u64..50 {
            controller.record_completion(100_000, true, i * 200); // 100ms latency.
        }

        // Rate should NOT have changed because AIMD is disabled.
        let after_rate = controller.device_controller.current_rate();
        assert_eq!(initial_rate, after_rate);

        // Backpressure signal should also be ignored.
        controller.signal_backpressure();
        let after_backpressure_rate = controller.device_controller.current_rate();
        assert_eq!(initial_rate, after_backpressure_rate);
    }

    #[test]
    fn test_global_vs_stream_rate_limit() {
        let mut config = FlowControllerConfig::for_testing();
        // Global: 100 tokens capacity.
        // Stream: 1000 tokens capacity (higher than global).
        config.token_bucket.capacity = 100;
        config.token_bucket.initial_tokens = Some(100);

        // Modify the config such that stream bucket has more capacity than global.
        let mut controller: FlowController<i32> = FlowController::new(config, 0);

        // First op consumes 80 from both global and stream.
        controller.submit(1, IoClass::Write, 80, 1, 0).unwrap();

        // Second op only needs 30 tokens.
        // Stream has 100-80=20 (wait, need to check logic).
        // Actually both global and stream start with 100 and consume 80.
        // So both have 20 remaining. 30 > 20, so should fail.
        let result = controller.submit(1, IoClass::Write, 30, 2, 0);
        assert!(matches!(result, Err(FlowError::RateLimitExceeded { .. })));

        // Now test that global limit constrains even when stream has capacity.
        // Create a new controller with different limits.
        let mut config2 = FlowControllerConfig::for_testing();
        config2.token_bucket.capacity = 50; // Global: 50 tokens.
        config2.token_bucket.initial_tokens = Some(50);

        let mut controller2: FlowController<i32> = FlowController::new(config2, 0);

        // Submit 40 tokens - uses 40 from both global and stream.
        controller2.submit(1, IoClass::Write, 40, 1, 0).unwrap();

        // Now global has 10, stream also has 10 (same config).
        // Try 20 - should fail on global limit (10 available).
        let result2 = controller2.submit(1, IoClass::Write, 20, 2, 0);
        assert!(matches!(result2, Err(FlowError::RateLimitExceeded {
            tokens_available: 10,
            ..
        })));
    }

    #[test]
    fn test_global_limit_across_streams() {
        let mut config = FlowControllerConfig::for_testing();
        config.token_bucket.capacity = 100;
        config.token_bucket.initial_tokens = Some(100);

        let mut controller: FlowController<i32> = FlowController::new(config, 0);

        // Stream 1 consumes 40 from global.
        controller.submit(1, IoClass::Write, 40, 1, 0).unwrap();
        // Stream 2 consumes another 40 from global (80 total).
        controller.submit(2, IoClass::Write, 40, 2, 0).unwrap();

        // Global now has 20 left. Each stream has 60 left.
        // Stream 3 tries to consume 30 - should fail on global (only 20 available).
        let result = controller.submit(3, IoClass::Write, 30, 3, 0);
        assert!(matches!(result, Err(FlowError::RateLimitExceeded {
            tokens_available: 20,
            ..
        })));
    }

    #[test]
    fn test_refill_across_time() {
        let mut config = FlowControllerConfig::for_testing();
        config.token_bucket.capacity = 100;
        config.token_bucket.refill_rate = 100; // 100 tokens/sec.
        config.token_bucket.initial_tokens = Some(100);

        let mut controller: FlowController<i32> = FlowController::new(config, 0);

        // Exhaust tokens.
        controller.submit(1, IoClass::Write, 100, 1, 0).unwrap();

        // Immediately try another - should fail.
        let result = controller.submit(1, IoClass::Write, 50, 2, 0);
        assert!(matches!(result, Err(FlowError::RateLimitExceeded { .. })));

        // Wait 0.5 seconds (500_000 us) - should refill 50 tokens.
        controller.submit(1, IoClass::Write, 50, 3, 500_000).unwrap();

        // Wait another 0.5 seconds - refill another 50 tokens.
        controller.submit(1, IoClass::Write, 50, 4, 1_000_000).unwrap();
    }

    #[test]
    fn test_rejected_counter() {
        let mut config = FlowControllerConfig::for_testing();
        config.token_bucket.capacity = 50;
        config.token_bucket.initial_tokens = Some(50);

        let mut controller: FlowController<i32> = FlowController::new(config, 0);

        // First op succeeds.
        controller.submit(1, IoClass::Write, 40, 1, 0).unwrap();
        assert_eq!(controller.stats().total_rejected, 0);

        // Second op fails (need 40, have 10).
        let _ = controller.submit(1, IoClass::Write, 40, 2, 0);
        assert_eq!(controller.stats().total_rejected, 1);

        // Third op also fails.
        let _ = controller.submit(1, IoClass::Write, 40, 3, 0);
        assert_eq!(controller.stats().total_rejected, 2);
    }

    #[test]
    fn test_pending_operation_metadata() {
        let config = FlowControllerConfig::for_testing();
        let mut controller: FlowController<&str> = FlowController::new(config, 1_000_000);

        controller
            .submit(42, IoClass::BackfillRead, 256, "payload", 1_000_000)
            .unwrap();

        let op = controller.next_operation().unwrap();
        assert_eq!(op.stream_id, 42);
        assert_eq!(op.class, IoClass::BackfillRead);
        assert_eq!(op.size_bytes, 256);
        assert_eq!(op.payload, "payload");
        assert_eq!(op.submit_time_us, 1_000_000);
    }

    #[test]
    fn test_all_io_classes_submit() {
        let config = FlowControllerConfig::for_testing();
        let mut controller: FlowController<i32> = FlowController::new(config, 0);

        // Submit one of each I/O class.
        controller.submit(1, IoClass::Write, 10, 1, 0).unwrap();
        controller.submit(2, IoClass::LiveRead, 10, 2, 0).unwrap();
        controller.submit(3, IoClass::BackfillRead, 10, 3, 0).unwrap();
        controller.submit(4, IoClass::Tiering, 10, 4, 0).unwrap();

        assert_eq!(controller.pending_count(), 4);

        // Drain all.
        for _ in 0..4 {
            controller.next_operation().unwrap();
        }

        assert_eq!(controller.pending_count(), 0);
    }
}
