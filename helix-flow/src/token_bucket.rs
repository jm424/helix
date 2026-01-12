//! Token bucket rate limiter.
//!
//! A token bucket is a rate limiting algorithm that allows controlled bursting while
//! enforcing an average rate limit. Tokens are added to the bucket at a fixed rate,
//! and operations consume tokens. If insufficient tokens are available, the operation
//! is either rejected or waits.

use crate::error::{FlowError, FlowResult};

/// Configuration for a token bucket.
#[derive(Debug, Clone)]
pub struct TokenBucketConfig {
    /// Maximum number of tokens the bucket can hold.
    pub capacity: u64,

    /// Tokens added per second.
    pub refill_rate: u64,

    /// Initial number of tokens (defaults to capacity if not set).
    pub initial_tokens: Option<u64>,
}

impl TokenBucketConfig {
    /// Creates a new token bucket configuration.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum number of tokens the bucket can hold.
    /// * `refill_rate` - Tokens added per second.
    #[must_use]
    pub const fn new(capacity: u64, refill_rate: u64) -> Self {
        Self {
            capacity,
            refill_rate,
            initial_tokens: None,
        }
    }

    /// Sets the initial number of tokens.
    #[must_use]
    pub const fn with_initial_tokens(mut self, tokens: u64) -> Self {
        self.initial_tokens = Some(tokens);
        self
    }

    /// Creates a configuration for testing.
    #[must_use]
    pub const fn for_testing() -> Self {
        Self {
            capacity: 10_000,
            refill_rate: 1_000,
            initial_tokens: Some(10_000),
        }
    }
}

impl Default for TokenBucketConfig {
    fn default() -> Self {
        Self {
            capacity: 10_000,
            refill_rate: 1_000,
            initial_tokens: None,
        }
    }
}

/// A token bucket rate limiter.
///
/// The bucket starts with an initial number of tokens and refills at a constant rate.
/// Each operation consumes tokens, and if insufficient tokens are available,
/// the operation can either fail immediately or calculate how long to wait.
#[derive(Debug)]
pub struct TokenBucket {
    /// Current number of tokens in the bucket.
    tokens: u64,

    /// Maximum capacity.
    capacity: u64,

    /// Tokens added per second.
    refill_rate: u64,

    /// Last time tokens were refilled (microseconds since epoch).
    last_refill_us: u64,
}

impl TokenBucket {
    /// Creates a new token bucket with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The token bucket configuration.
    /// * `current_time_us` - Current time in microseconds.
    #[must_use]
    pub fn new(config: &TokenBucketConfig, current_time_us: u64) -> Self {
        let initial = config.initial_tokens.unwrap_or(config.capacity);
        let tokens = initial.min(config.capacity);

        Self {
            tokens,
            capacity: config.capacity,
            refill_rate: config.refill_rate,
            last_refill_us: current_time_us,
        }
    }

    /// Returns the current number of available tokens.
    ///
    /// This does NOT perform a refill. Use `refill()` first for an accurate count.
    #[must_use]
    pub const fn available_tokens(&self) -> u64 {
        self.tokens
    }

    /// Returns the bucket capacity.
    #[must_use]
    pub const fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Returns the refill rate (tokens per second).
    #[must_use]
    pub const fn refill_rate(&self) -> u64 {
        self.refill_rate
    }

    /// Refills the bucket based on elapsed time.
    ///
    /// # Arguments
    ///
    /// * `current_time_us` - Current time in microseconds.
    ///
    /// # Returns
    ///
    /// The number of tokens added.
    pub fn refill(&mut self, current_time_us: u64) -> u64 {
        // Handle time going backwards (e.g., in simulation).
        if current_time_us <= self.last_refill_us {
            return 0;
        }

        let elapsed_us = current_time_us - self.last_refill_us;
        // Convert elapsed time to tokens: (elapsed_us * rate) / 1_000_000.
        // Use u128 to avoid overflow for large elapsed times.
        let tokens_to_add =
            u64::try_from((u128::from(elapsed_us) * u128::from(self.refill_rate)) / 1_000_000)
                .unwrap_or(u64::MAX);

        if tokens_to_add > 0 {
            let old_tokens = self.tokens;
            self.tokens = self.tokens.saturating_add(tokens_to_add).min(self.capacity);
            self.last_refill_us = current_time_us;
            self.tokens - old_tokens
        } else {
            0
        }
    }

    /// Attempts to consume tokens from the bucket.
    ///
    /// # Arguments
    ///
    /// * `tokens` - Number of tokens to consume.
    /// * `current_time_us` - Current time in microseconds.
    ///
    /// # Returns
    ///
    /// `Ok(())` if tokens were consumed, or an error if insufficient tokens.
    ///
    /// # Errors
    ///
    /// Returns `FlowError::RateLimitExceeded` if not enough tokens are available.
    pub fn try_consume(&mut self, tokens: u64, current_time_us: u64) -> FlowResult<()> {
        // Refill first.
        self.refill(current_time_us);

        if self.tokens >= tokens {
            self.tokens -= tokens;
            Ok(())
        } else {
            Err(FlowError::RateLimitExceeded {
                stream_id: 0, // Will be filled by caller.
                tokens_requested: tokens,
                tokens_available: self.tokens,
            })
        }
    }

    /// Calculates how long to wait for the requested tokens to become available.
    ///
    /// # Arguments
    ///
    /// * `tokens` - Number of tokens needed.
    /// * `current_time_us` - Current time in microseconds.
    ///
    /// # Returns
    ///
    /// The wait time in microseconds, or `None` if the request exceeds capacity.
    #[must_use]
    pub fn wait_time_us(&mut self, tokens: u64, current_time_us: u64) -> Option<u64> {
        // Cannot satisfy requests larger than capacity.
        if tokens > self.capacity {
            return None;
        }

        // Refill first.
        self.refill(current_time_us);

        if self.tokens >= tokens {
            return Some(0);
        }

        let tokens_needed = tokens - self.tokens;
        // Time = (tokens_needed * 1_000_000) / refill_rate.
        // Use u128 to avoid overflow.
        #[allow(clippy::cast_possible_truncation)] // Result bounded by tokens_needed.
        let wait_us =
            (u128::from(tokens_needed) * 1_000_000 / u128::from(self.refill_rate.max(1))) as u64;

        Some(wait_us)
    }

    /// Consumes tokens, waiting if necessary.
    ///
    /// # Arguments
    ///
    /// * `tokens` - Number of tokens to consume.
    /// * `current_time_us` - Current time in microseconds.
    ///
    /// # Returns
    ///
    /// The time at which the tokens will be available.
    ///
    /// # Errors
    ///
    /// Returns `FlowError::InvalidConfig` if the request exceeds bucket capacity.
    pub fn consume_with_wait(&mut self, tokens: u64, current_time_us: u64) -> FlowResult<u64> {
        if tokens > self.capacity {
            return Err(FlowError::InvalidConfig {
                message: format!(
                    "requested {tokens} tokens exceeds bucket capacity {}",
                    self.capacity
                ),
            });
        }

        match self.wait_time_us(tokens, current_time_us) {
            Some(0) => {
                self.tokens -= tokens;
                Ok(current_time_us)
            }
            Some(wait_us) => {
                // Schedule consumption at future time.
                let ready_time = current_time_us + wait_us;
                // Pre-consume the tokens (they'll be refilled by then).
                self.tokens = 0;
                Ok(ready_time)
            }
            None => Err(FlowError::InvalidConfig {
                message: format!(
                    "requested {tokens} tokens exceeds bucket capacity {}",
                    self.capacity
                ),
            }),
        }
    }

    /// Resets the bucket to its initial state.
    ///
    /// # Arguments
    ///
    /// * `current_time_us` - Current time in microseconds.
    pub const fn reset(&mut self, current_time_us: u64) {
        self.tokens = self.capacity;
        self.last_refill_us = current_time_us;
    }

    /// Updates the configuration of the bucket.
    ///
    /// This is useful for dynamic rate adjustments (e.g., from AIMD controller).
    ///
    /// # Arguments
    ///
    /// * `capacity` - New capacity (if `Some`).
    /// * `refill_rate` - New refill rate (if `Some`).
    pub fn update_config(&mut self, capacity: Option<u64>, refill_rate: Option<u64>) {
        if let Some(cap) = capacity {
            self.capacity = cap;
            self.tokens = self.tokens.min(cap);
        }
        if let Some(rate) = refill_rate {
            self.refill_rate = rate;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_bucket_has_initial_tokens() {
        let config = TokenBucketConfig::new(100, 10);
        let bucket = TokenBucket::new(&config, 0);

        assert_eq!(bucket.available_tokens(), 100);
        assert_eq!(bucket.capacity(), 100);
        assert_eq!(bucket.refill_rate(), 10);
    }

    #[test]
    fn test_consume_reduces_tokens() {
        let config = TokenBucketConfig::new(100, 10);
        let mut bucket = TokenBucket::new(&config, 0);

        bucket.try_consume(30, 0).unwrap();
        assert_eq!(bucket.available_tokens(), 70);

        bucket.try_consume(50, 0).unwrap();
        assert_eq!(bucket.available_tokens(), 20);
    }

    #[test]
    fn test_consume_fails_when_insufficient() {
        let config = TokenBucketConfig::new(100, 10);
        let mut bucket = TokenBucket::new(&config, 0);

        let result = bucket.try_consume(150, 0);
        assert!(matches!(result, Err(FlowError::RateLimitExceeded { .. })));
    }

    #[test]
    fn test_refill_adds_tokens() {
        let config = TokenBucketConfig::new(100, 10).with_initial_tokens(0);
        let mut bucket = TokenBucket::new(&config, 0);

        assert_eq!(bucket.available_tokens(), 0);

        // After 1 second at 10 tokens/sec, should have 10 tokens.
        bucket.refill(1_000_000);
        assert_eq!(bucket.available_tokens(), 10);

        // After another 5 seconds, should have 60 tokens.
        bucket.refill(6_000_000);
        assert_eq!(bucket.available_tokens(), 60);
    }

    #[test]
    fn test_refill_caps_at_capacity() {
        let config = TokenBucketConfig::new(100, 10);
        let mut bucket = TokenBucket::new(&config, 0);

        // Wait a long time.
        bucket.refill(100_000_000); // 100 seconds.
        assert_eq!(bucket.available_tokens(), 100); // Capped at capacity.
    }

    #[test]
    fn test_wait_time_calculation() {
        let config = TokenBucketConfig::new(100, 10).with_initial_tokens(0);
        let mut bucket = TokenBucket::new(&config, 0);

        // Need 50 tokens at 10/sec = 5 seconds = 5_000_000 us.
        let wait = bucket.wait_time_us(50, 0);
        assert_eq!(wait, Some(5_000_000));
    }

    #[test]
    fn test_wait_time_none_if_exceeds_capacity() {
        let config = TokenBucketConfig::new(100, 10);
        let mut bucket = TokenBucket::new(&config, 0);

        let wait = bucket.wait_time_us(200, 0);
        assert_eq!(wait, None);
    }

    #[test]
    fn test_reset() {
        let config = TokenBucketConfig::new(100, 10);
        let mut bucket = TokenBucket::new(&config, 0);

        bucket.try_consume(80, 0).unwrap();
        assert_eq!(bucket.available_tokens(), 20);

        bucket.reset(1_000_000);
        assert_eq!(bucket.available_tokens(), 100);
    }

    #[test]
    fn test_update_config() {
        let config = TokenBucketConfig::new(100, 10);
        let mut bucket = TokenBucket::new(&config, 0);

        // Reduce capacity.
        bucket.update_config(Some(50), None);
        assert_eq!(bucket.capacity(), 50);
        assert_eq!(bucket.available_tokens(), 50); // Capped.

        // Change rate.
        bucket.update_config(None, Some(20));
        assert_eq!(bucket.refill_rate(), 20);
    }

    #[test]
    fn test_time_going_backwards() {
        let config = TokenBucketConfig::new(100, 10).with_initial_tokens(50);
        let mut bucket = TokenBucket::new(&config, 1_000_000);

        // Time goes backwards - should not panic, just no refill.
        let added = bucket.refill(500_000);
        assert_eq!(added, 0);
        assert_eq!(bucket.available_tokens(), 50);
    }

    #[test]
    fn test_consume_with_wait_immediate() {
        let config = TokenBucketConfig::new(100, 10);
        let mut bucket = TokenBucket::new(&config, 0);

        // Have enough tokens - should return immediately.
        let ready_time = bucket.consume_with_wait(50, 0).unwrap();
        assert_eq!(ready_time, 0);
        assert_eq!(bucket.available_tokens(), 50);
    }

    #[test]
    fn test_consume_with_wait_delayed() {
        let config = TokenBucketConfig::new(100, 10).with_initial_tokens(0);
        let mut bucket = TokenBucket::new(&config, 0);

        // Need to wait for tokens.
        let ready_time = bucket.consume_with_wait(50, 0).unwrap();
        // 50 tokens at 10/sec = 5 seconds.
        assert_eq!(ready_time, 5_000_000);
    }

    #[test]
    fn test_consume_with_wait_exceeds_capacity() {
        let config = TokenBucketConfig::new(100, 10);
        let mut bucket = TokenBucket::new(&config, 0);

        // Request more than capacity - should error.
        let result = bucket.consume_with_wait(200, 0);
        assert!(matches!(result, Err(FlowError::InvalidConfig { .. })));
    }

    #[test]
    fn test_consume_exact_available() {
        let config = TokenBucketConfig::new(100, 10);
        let mut bucket = TokenBucket::new(&config, 0);

        // Consume exactly what's available.
        bucket.try_consume(100, 0).unwrap();
        assert_eq!(bucket.available_tokens(), 0);

        // Next consume should fail.
        let result = bucket.try_consume(1, 0);
        assert!(matches!(result, Err(FlowError::RateLimitExceeded { .. })));
    }

    #[test]
    fn test_fractional_refill() {
        let config = TokenBucketConfig::new(100, 10).with_initial_tokens(0);
        let mut bucket = TokenBucket::new(&config, 0);

        // 0.5 seconds = 5 tokens at 10/sec.
        bucket.refill(500_000);
        assert_eq!(bucket.available_tokens(), 5);

        // 0.1 seconds = 1 token.
        bucket.refill(600_000);
        assert_eq!(bucket.available_tokens(), 6);
    }

    #[test]
    fn test_zero_refill_rate() {
        let config = TokenBucketConfig::new(100, 0).with_initial_tokens(50);
        let mut bucket = TokenBucket::new(&config, 0);

        // With zero refill rate, tokens should never increase.
        bucket.refill(10_000_000);
        assert_eq!(bucket.available_tokens(), 50);

        // Wait time should still be calculated (treating 0 as 1).
        bucket.try_consume(50, 0).unwrap();
        let wait = bucket.wait_time_us(10, 0);
        assert!(wait.is_some());
    }

    #[test]
    fn test_large_time_jump() {
        let config = TokenBucketConfig::new(100, 1000).with_initial_tokens(0);
        let mut bucket = TokenBucket::new(&config, 0);

        // Jump 1 hour (3600 seconds) - should cap at capacity.
        bucket.refill(3_600_000_000);
        assert_eq!(bucket.available_tokens(), 100);
    }
}
