//! AIMD (Additive Increase Multiplicative Decrease) controller.
//!
//! AIMD is a feedback control algorithm used for congestion control, similar to
//! TCP congestion control. It works by:
//!
//! 1. **Additive Increase**: When no congestion is detected, increase the rate
//!    linearly (e.g., +10 tokens/sec per interval).
//! 2. **Multiplicative Decrease**: When congestion is detected (latency spikes,
//!    errors, backpressure), decrease the rate multiplicatively (e.g., *0.5).
//!
//! This creates a sawtooth pattern that probes for available capacity while
//! quickly backing off when overloaded.

/// Configuration for the AIMD controller.
#[derive(Debug, Clone)]
pub struct AimdConfig {
    /// Initial rate limit.
    pub initial_rate: u64,

    /// Minimum rate limit (floor).
    pub min_rate: u64,

    /// Maximum rate limit (ceiling).
    pub max_rate: u64,

    /// Additive increase amount per successful interval.
    pub additive_increase: u64,

    /// Multiplicative decrease factor (e.g., 0.5 means halve the rate).
    /// Must be between 0.0 and 1.0.
    pub multiplicative_decrease: f64,

    /// Interval between rate adjustments (microseconds).
    pub adjustment_interval_us: u64,

    /// Latency threshold (microseconds) above which congestion is detected.
    pub latency_threshold_us: u64,

    /// Error rate threshold (0.0 to 1.0) above which congestion is detected.
    pub error_rate_threshold: f64,
}

impl AimdConfig {
    /// Creates a new AIMD configuration.
    #[must_use]
    pub const fn new(initial_rate: u64, min_rate: u64, max_rate: u64) -> Self {
        Self {
            initial_rate,
            min_rate,
            max_rate,
            additive_increase: 100,
            multiplicative_decrease: 0.5,
            adjustment_interval_us: 1_000_000, // 1 second.
            latency_threshold_us: 100_000,     // 100ms.
            error_rate_threshold: 0.05,        // 5%.
        }
    }

    /// Sets the additive increase amount.
    #[must_use]
    pub const fn with_additive_increase(mut self, amount: u64) -> Self {
        self.additive_increase = amount;
        self
    }

    /// Sets the multiplicative decrease factor.
    #[must_use]
    pub const fn with_multiplicative_decrease(mut self, factor: f64) -> Self {
        self.multiplicative_decrease = factor;
        self
    }

    /// Sets the adjustment interval.
    #[must_use]
    pub const fn with_adjustment_interval_us(mut self, interval_us: u64) -> Self {
        self.adjustment_interval_us = interval_us;
        self
    }

    /// Sets the latency threshold.
    #[must_use]
    pub const fn with_latency_threshold_us(mut self, threshold_us: u64) -> Self {
        self.latency_threshold_us = threshold_us;
        self
    }

    /// Creates a configuration for testing.
    #[must_use]
    pub const fn for_testing() -> Self {
        Self {
            initial_rate: 1000,
            min_rate: 100,
            max_rate: 10_000,
            additive_increase: 100,
            multiplicative_decrease: 0.5,
            adjustment_interval_us: 100_000, // 100ms for faster tests.
            latency_threshold_us: 50_000,    // 50ms.
            error_rate_threshold: 0.1,       // 10%.
        }
    }
}

impl Default for AimdConfig {
    fn default() -> Self {
        Self::new(1000, 100, 100_000)
    }
}

/// Metrics collected during an interval for AIMD decision-making.
#[derive(Debug, Clone, Default)]
pub struct IntervalMetrics {
    /// Number of successful operations.
    pub success_count: u64,

    /// Number of failed operations.
    pub error_count: u64,

    /// Sum of latencies (microseconds) for calculating average.
    pub latency_sum_us: u64,

    /// Maximum latency observed (microseconds).
    pub latency_max_us: u64,

    /// Number of latency samples.
    pub latency_samples: u64,
}

impl IntervalMetrics {
    /// Creates new empty metrics.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            success_count: 0,
            error_count: 0,
            latency_sum_us: 0,
            latency_max_us: 0,
            latency_samples: 0,
        }
    }

    /// Records a successful operation.
    pub fn record_success(&mut self, latency_us: u64) {
        self.success_count += 1;
        self.latency_sum_us = self.latency_sum_us.saturating_add(latency_us);
        self.latency_max_us = self.latency_max_us.max(latency_us);
        self.latency_samples += 1;
    }

    /// Records a failed operation.
    pub const fn record_error(&mut self) {
        self.error_count += 1;
    }

    /// Returns the average latency, or 0 if no samples.
    #[must_use]
    pub const fn average_latency_us(&self) -> u64 {
        if self.latency_samples == 0 {
            0
        } else {
            self.latency_sum_us / self.latency_samples
        }
    }

    /// Returns the error rate (0.0 to 1.0).
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // Precision loss acceptable for rate calculation.
    pub fn error_rate(&self) -> f64 {
        let total = self.success_count + self.error_count;
        if total == 0 {
            0.0
        } else {
            self.error_count as f64 / total as f64
        }
    }

    /// Returns the total operation count.
    #[must_use]
    pub const fn total_ops(&self) -> u64 {
        self.success_count + self.error_count
    }

    /// Resets all metrics.
    pub const fn reset(&mut self) {
        *self = Self::new();
    }
}

/// State of the AIMD controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AimdState {
    /// Probing for more capacity (additive increase phase).
    Probing,
    /// Recovering from congestion (after multiplicative decrease).
    Recovering,
    /// Stable at current rate.
    Stable,
}

/// AIMD controller for adaptive rate limiting.
///
/// Adjusts rate limits based on observed latency and error rates.
#[derive(Debug)]
pub struct AimdController {
    /// Configuration.
    config: AimdConfig,

    /// Current rate limit.
    current_rate: u64,

    /// Current state.
    state: AimdState,

    /// Metrics for the current interval.
    current_metrics: IntervalMetrics,

    /// Last adjustment time (microseconds).
    last_adjustment_us: u64,

    /// Number of consecutive successful intervals.
    consecutive_successes: u32,

    /// Number of consecutive congestion events.
    consecutive_congestions: u32,

    /// Total rate increases.
    total_increases: u64,

    /// Total rate decreases.
    total_decreases: u64,
}

impl AimdController {
    /// Creates a new AIMD controller.
    #[must_use]
    pub fn new(config: AimdConfig, current_time_us: u64) -> Self {
        let current_rate = config.initial_rate.clamp(config.min_rate, config.max_rate);

        Self {
            config,
            current_rate,
            state: AimdState::Probing,
            current_metrics: IntervalMetrics::new(),
            last_adjustment_us: current_time_us,
            consecutive_successes: 0,
            consecutive_congestions: 0,
            total_increases: 0,
            total_decreases: 0,
        }
    }

    /// Returns the current rate limit.
    #[must_use]
    pub const fn current_rate(&self) -> u64 {
        self.current_rate
    }

    /// Returns the current state.
    #[must_use]
    pub const fn state(&self) -> AimdState {
        self.state
    }

    /// Returns the current interval metrics.
    #[must_use]
    pub const fn metrics(&self) -> &IntervalMetrics {
        &self.current_metrics
    }

    /// Records a successful operation.
    pub fn record_success(&mut self, latency_us: u64) {
        self.current_metrics.record_success(latency_us);
    }

    /// Records a failed operation.
    pub const fn record_error(&mut self) {
        self.current_metrics.record_error();
    }

    /// Checks if it's time to adjust the rate and performs adjustment if needed.
    ///
    /// # Arguments
    ///
    /// * `current_time_us` - Current time in microseconds.
    ///
    /// # Returns
    ///
    /// The new rate if an adjustment was made, or `None` if no adjustment.
    pub fn maybe_adjust(&mut self, current_time_us: u64) -> Option<u64> {
        let elapsed = current_time_us.saturating_sub(self.last_adjustment_us);
        if elapsed < self.config.adjustment_interval_us {
            return None;
        }

        let new_rate = self.adjust(current_time_us);
        Some(new_rate)
    }

    /// Forces a rate adjustment based on current metrics.
    ///
    /// # Arguments
    ///
    /// * `current_time_us` - Current time in microseconds.
    ///
    /// # Returns
    ///
    /// The new rate after adjustment.
    pub fn adjust(&mut self, current_time_us: u64) -> u64 {
        let congested = self.is_congested();

        if congested {
            self.decrease();
            self.consecutive_successes = 0;
            self.consecutive_congestions += 1;
            self.state = AimdState::Recovering;
        } else {
            self.increase();
            self.consecutive_successes += 1;
            self.consecutive_congestions = 0;

            // Transition to stable after several successful intervals.
            if self.consecutive_successes >= 5 {
                self.state = AimdState::Stable;
            } else if self.state == AimdState::Recovering && self.consecutive_successes >= 2 {
                self.state = AimdState::Probing;
            }
        }

        self.current_metrics.reset();
        self.last_adjustment_us = current_time_us;

        self.current_rate
    }

    /// Checks if the system appears congested based on current metrics.
    fn is_congested(&self) -> bool {
        // Check error rate.
        if self.current_metrics.error_rate() > self.config.error_rate_threshold {
            return true;
        }

        // Check latency (use max latency for responsiveness).
        if self.current_metrics.latency_max_us > self.config.latency_threshold_us {
            return true;
        }

        // Check average latency too.
        if self.current_metrics.average_latency_us() > self.config.latency_threshold_us {
            return true;
        }

        false
    }

    /// Performs additive increase.
    fn increase(&mut self) {
        let new_rate = self
            .current_rate
            .saturating_add(self.config.additive_increase);
        self.current_rate = new_rate.min(self.config.max_rate);
        self.total_increases += 1;
    }

    /// Performs multiplicative decrease.
    fn decrease(&mut self) {
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let new_rate = (self.current_rate as f64 * self.config.multiplicative_decrease) as u64;
        self.current_rate = new_rate.max(self.config.min_rate);
        self.total_decreases += 1;
    }

    /// Forces an immediate decrease (e.g., on explicit backpressure signal).
    pub fn force_decrease(&mut self) {
        self.decrease();
        self.consecutive_successes = 0;
        self.consecutive_congestions += 1;
        self.state = AimdState::Recovering;
    }

    /// Resets the controller to its initial state.
    pub fn reset(&mut self, current_time_us: u64) {
        self.current_rate = self
            .config
            .initial_rate
            .clamp(self.config.min_rate, self.config.max_rate);
        self.state = AimdState::Probing;
        self.current_metrics.reset();
        self.last_adjustment_us = current_time_us;
        self.consecutive_successes = 0;
        self.consecutive_congestions = 0;
    }

    /// Returns statistics about the controller.
    #[must_use]
    pub const fn stats(&self) -> AimdStats {
        AimdStats {
            current_rate: self.current_rate,
            state: self.state,
            consecutive_successes: self.consecutive_successes,
            consecutive_congestions: self.consecutive_congestions,
            total_increases: self.total_increases,
            total_decreases: self.total_decreases,
        }
    }

    /// Updates configuration dynamically.
    pub fn update_config(&mut self, min_rate: Option<u64>, max_rate: Option<u64>) {
        if let Some(min) = min_rate {
            self.config.min_rate = min;
        }
        if let Some(max) = max_rate {
            self.config.max_rate = max;
        }
        // Clamp current rate to new bounds.
        self.current_rate = self
            .current_rate
            .clamp(self.config.min_rate, self.config.max_rate);
    }
}

/// Statistics about the AIMD controller.
#[derive(Debug, Clone)]
pub struct AimdStats {
    /// Current rate limit.
    pub current_rate: u64,
    /// Current state.
    pub state: AimdState,
    /// Consecutive successful intervals.
    pub consecutive_successes: u32,
    /// Consecutive congestion events.
    pub consecutive_congestions: u32,
    /// Total rate increases performed.
    pub total_increases: u64,
    /// Total rate decreases performed.
    pub total_decreases: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_controller() {
        let config = AimdConfig::for_testing();
        let controller = AimdController::new(config, 0);

        assert_eq!(controller.current_rate(), 1000);
        assert_eq!(controller.state(), AimdState::Probing);
    }

    #[test]
    fn test_additive_increase() {
        let config = AimdConfig::for_testing();
        let mut controller = AimdController::new(config, 0);

        // Record some good operations.
        for _ in 0..10 {
            controller.record_success(10_000); // 10ms latency, well under threshold.
        }

        let new_rate = controller.adjust(100_000);
        assert_eq!(new_rate, 1100); // 1000 + 100 additive increase.
    }

    #[test]
    fn test_multiplicative_decrease_on_high_latency() {
        let config = AimdConfig::for_testing();
        let mut controller = AimdController::new(config, 0);

        // Record operations with high latency.
        for _ in 0..10 {
            controller.record_success(100_000); // 100ms latency, above 50ms threshold.
        }

        let new_rate = controller.adjust(100_000);
        assert_eq!(new_rate, 500); // 1000 * 0.5 multiplicative decrease.
        assert_eq!(controller.state(), AimdState::Recovering);
    }

    #[test]
    fn test_multiplicative_decrease_on_errors() {
        let mut config = AimdConfig::for_testing();
        config.error_rate_threshold = 0.1;
        let mut controller = AimdController::new(config, 0);

        // Record 80% errors.
        for _ in 0..2 {
            controller.record_success(1000);
        }
        for _ in 0..8 {
            controller.record_error();
        }

        let new_rate = controller.adjust(100_000);
        assert_eq!(new_rate, 500);
    }

    #[test]
    fn test_rate_bounded_by_min_max() {
        let config = AimdConfig::for_testing(); // min=100, max=10000
        let mut controller = AimdController::new(config, 0);

        // Force many decreases.
        for i in 0_u64..20 {
            controller.record_error();
            controller.adjust(i * 100_000);
        }

        assert_eq!(controller.current_rate(), 100); // Bounded at min.

        // Reset and force increases.
        controller.reset(0);
        for i in 0_u64..100 {
            controller.record_success(1000);
            controller.adjust(i * 100_000);
        }

        assert_eq!(controller.current_rate(), 10_000); // Bounded at max.
    }

    #[test]
    fn test_maybe_adjust_respects_interval() {
        let config = AimdConfig::for_testing(); // interval = 100_000us
        let mut controller = AimdController::new(config, 0);

        controller.record_success(1000);

        // Too soon - no adjustment.
        let result = controller.maybe_adjust(50_000);
        assert!(result.is_none());

        // After interval - adjustment happens.
        let result = controller.maybe_adjust(100_000);
        assert!(result.is_some());
    }

    #[test]
    fn test_force_decrease() {
        let config = AimdConfig::for_testing();
        let mut controller = AimdController::new(config, 0);

        controller.force_decrease();
        assert_eq!(controller.current_rate(), 500);
        assert_eq!(controller.state(), AimdState::Recovering);
    }

    #[test]
    fn test_state_transitions() {
        let config = AimdConfig::for_testing();
        let mut controller = AimdController::new(config, 0);

        // Cause congestion.
        controller.record_error();
        controller.adjust(100_000);
        assert_eq!(controller.state(), AimdState::Recovering);

        // Two successful intervals to move to Probing.
        controller.record_success(1000);
        controller.adjust(200_000);
        assert_eq!(controller.state(), AimdState::Recovering);

        controller.record_success(1000);
        controller.adjust(300_000);
        assert_eq!(controller.state(), AimdState::Probing);

        // Five more to reach Stable.
        for i in 0_u64..5 {
            controller.record_success(1000);
            controller.adjust(400_000 + i * 100_000);
        }
        assert_eq!(controller.state(), AimdState::Stable);
    }

    #[test]
    fn test_interval_metrics() {
        let mut metrics = IntervalMetrics::new();

        metrics.record_success(100);
        metrics.record_success(200);
        metrics.record_success(300);
        metrics.record_error();

        assert_eq!(metrics.success_count, 3);
        assert_eq!(metrics.error_count, 1);
        assert_eq!(metrics.average_latency_us(), 200);
        assert_eq!(metrics.latency_max_us, 300);
        assert!((metrics.error_rate() - 0.25).abs() < 0.001);
    }

    #[test]
    fn test_stats() {
        let config = AimdConfig::for_testing();
        let mut controller = AimdController::new(config, 0);

        controller.record_success(1000);
        controller.adjust(100_000);

        let stats = controller.stats();
        assert_eq!(stats.current_rate, 1100);
        assert_eq!(stats.total_increases, 1);
        assert_eq!(stats.total_decreases, 0);
    }

    #[test]
    fn test_update_config() {
        let config = AimdConfig::for_testing(); // min=100, max=10000, initial=1000
        let mut controller = AimdController::new(config, 0);

        // Reduce max rate.
        controller.update_config(None, Some(500));

        // Rate should be clamped to new max.
        assert!(controller.current_rate() <= 500);
        assert_eq!(controller.current_rate(), 500);

        // Increase min rate (but keep it <= max).
        controller.update_config(Some(300), None);
        // Rate was 500, min is now 300, so rate stays at 500 (within 300-500 range).
        assert!(controller.current_rate() >= 300);
        assert!(controller.current_rate() <= 500);

        // Test updating both at once.
        controller.update_config(Some(600), Some(2000));
        // Rate was 500, new min is 600, so rate should be clamped up to 600.
        assert_eq!(controller.current_rate(), 600);
    }

    #[test]
    fn test_no_ops_interval() {
        let config = AimdConfig::for_testing();
        let mut controller = AimdController::new(config, 0);

        // Don't record any operations.
        let new_rate = controller.adjust(100_000);

        // With no ops, error rate is 0, latency is 0 - should increase.
        assert_eq!(new_rate, 1100);
    }

    #[test]
    fn test_reset_after_congestion() {
        let config = AimdConfig::for_testing();
        let mut controller = AimdController::new(config, 0);

        // Cause congestion and decrease rate.
        controller.force_decrease();
        assert_eq!(controller.current_rate(), 500);
        assert_eq!(controller.state(), AimdState::Recovering);

        // Reset should restore initial state.
        controller.reset(1_000_000);
        assert_eq!(controller.current_rate(), 1000);
        assert_eq!(controller.state(), AimdState::Probing);
    }

    #[test]
    fn test_average_latency_threshold() {
        let mut config = AimdConfig::for_testing();
        config.latency_threshold_us = 50_000; // 50ms
        let mut controller = AimdController::new(config, 0);

        // Record operations with average above threshold but max below.
        // Actually, the code checks both max AND average, so both can trigger.
        // Let's test with average just above threshold.
        for _ in 0..10 {
            controller.record_success(51_000); // 51ms average
        }

        let new_rate = controller.adjust(100_000);
        // Should decrease due to average latency above threshold.
        assert!(new_rate < 1000);
    }

    #[test]
    fn test_zero_latency_success() {
        let config = AimdConfig::for_testing();
        let mut controller = AimdController::new(config, 0);

        // Zero latency operations - should not cause issues.
        for _ in 0..10 {
            controller.record_success(0);
        }

        let new_rate = controller.adjust(100_000);
        // Should increase since 0 latency is well under threshold.
        assert_eq!(new_rate, 1100);
    }

    #[test]
    fn test_metrics_reset_after_adjust() {
        let config = AimdConfig::for_testing();
        let mut controller = AimdController::new(config, 0);

        controller.record_success(1000);
        controller.record_error();
        assert_eq!(controller.metrics().total_ops(), 2);

        controller.adjust(100_000);

        // Metrics should be reset after adjustment.
        assert_eq!(controller.metrics().total_ops(), 0);
    }

    #[test]
    fn test_consecutive_counters() {
        let config = AimdConfig::for_testing();
        let mut controller = AimdController::new(config, 0);

        // Multiple successful intervals.
        for i in 0_u64..3 {
            controller.record_success(1000);
            controller.adjust(i * 100_000);
        }

        let stats = controller.stats();
        assert_eq!(stats.consecutive_successes, 3);
        assert_eq!(stats.consecutive_congestions, 0);

        // One congestion resets success counter.
        controller.record_error();
        controller.adjust(300_000);

        let stats = controller.stats();
        assert_eq!(stats.consecutive_successes, 0);
        assert_eq!(stats.consecutive_congestions, 1);
    }

    #[test]
    fn test_config_builders() {
        let config = AimdConfig::new(500, 50, 5000)
            .with_additive_increase(50)
            .with_multiplicative_decrease(0.75)
            .with_adjustment_interval_us(50_000)
            .with_latency_threshold_us(25_000);

        assert_eq!(config.initial_rate, 500);
        assert_eq!(config.min_rate, 50);
        assert_eq!(config.max_rate, 5000);
        assert_eq!(config.additive_increase, 50);
        assert!((config.multiplicative_decrease - 0.75).abs() < 0.001);
        assert_eq!(config.adjustment_interval_us, 50_000);
        assert_eq!(config.latency_threshold_us, 25_000);
    }
}
