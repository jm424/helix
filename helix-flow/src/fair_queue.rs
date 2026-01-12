//! Weighted fair queue for I/O scheduling.
//!
//! Implements a weighted fair queue (WFQ) scheduler that distributes I/O bandwidth
//! across different I/O classes based on their assigned weights. Higher weights
//! receive proportionally more bandwidth.
//!
//! Uses virtual time scheduling to ensure fairness: each class accumulates "virtual
//! time" based on its weight, and the class with the smallest virtual time is served
//! next.

use std::collections::VecDeque;

use crate::error::{FlowError, FlowResult};

/// I/O operation class.
///
/// Different operations have different priorities and scheduling behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoClass {
    /// Write operations (high priority, latency-sensitive).
    Write,
    /// Live read operations (high priority, latency-sensitive).
    LiveRead,
    /// Backfill read operations (lower priority, can be delayed).
    BackfillRead,
    /// Tiering operations (background, lowest priority).
    Tiering,
}

impl IoClass {
    /// Returns all I/O classes.
    #[must_use]
    pub const fn all() -> [Self; 4] {
        [Self::Write, Self::LiveRead, Self::BackfillRead, Self::Tiering]
    }

    /// Returns the default weight for this I/O class.
    #[must_use]
    #[allow(clippy::match_same_arms)] // Intentionally separate for clarity.
    pub const fn default_weight(&self) -> u32 {
        match self {
            Self::Write => 100,
            Self::LiveRead => 100,
            Self::BackfillRead => 50,
            Self::Tiering => 25,
        }
    }
}

/// An I/O operation in the queue.
#[derive(Debug, Clone)]
pub struct IoOperation<T> {
    /// The operation class.
    pub class: IoClass,

    /// Operation size in bytes (used for weighted scheduling).
    pub size_bytes: u64,

    /// The operation payload.
    pub payload: T,

    /// Enqueue timestamp (microseconds).
    pub enqueue_time_us: u64,
}

/// Configuration for the weighted fair queue.
#[derive(Debug, Clone)]
pub struct FairQueueConfig {
    /// Maximum queue depth per class.
    pub max_queue_depth: usize,

    /// Weight for Write operations.
    pub write_weight: u32,

    /// Weight for `LiveRead` operations.
    pub live_read_weight: u32,

    /// Weight for `BackfillRead` operations.
    pub backfill_read_weight: u32,

    /// Weight for Tiering operations.
    pub tiering_weight: u32,
}

impl FairQueueConfig {
    /// Creates a configuration for testing.
    #[must_use]
    pub const fn for_testing() -> Self {
        Self {
            max_queue_depth: 100,
            write_weight: 100,
            live_read_weight: 100,
            backfill_read_weight: 50,
            tiering_weight: 25,
        }
    }

    /// Returns the weight for a given I/O class.
    #[must_use]
    pub const fn weight_for(&self, class: IoClass) -> u32 {
        match class {
            IoClass::Write => self.write_weight,
            IoClass::LiveRead => self.live_read_weight,
            IoClass::BackfillRead => self.backfill_read_weight,
            IoClass::Tiering => self.tiering_weight,
        }
    }
}

impl Default for FairQueueConfig {
    fn default() -> Self {
        Self {
            max_queue_depth: 10_000,
            write_weight: IoClass::Write.default_weight(),
            live_read_weight: IoClass::LiveRead.default_weight(),
            backfill_read_weight: IoClass::BackfillRead.default_weight(),
            tiering_weight: IoClass::Tiering.default_weight(),
        }
    }
}

/// A per-class queue with virtual time tracking.
struct ClassQueue<T> {
    /// The I/O class (stored for debugging/introspection).
    #[allow(dead_code)]
    class: IoClass,

    /// Weight (higher = more bandwidth).
    weight: u32,

    /// Virtual time (lower = higher priority for next service).
    virtual_time: u64,

    /// Queue of pending operations.
    queue: VecDeque<IoOperation<T>>,
}

impl<T> ClassQueue<T> {
    #[allow(clippy::missing_const_for_fn)] // VecDeque::new() is not const.
    fn new(class: IoClass, weight: u32) -> Self {
        Self {
            class,
            weight,
            virtual_time: 0,
            queue: VecDeque::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn len(&self) -> usize {
        self.queue.len()
    }
}

/// A weighted fair queue for I/O scheduling.
///
/// Distributes I/O bandwidth fairly across different classes based on their
/// weights. Uses virtual time scheduling for fairness.
pub struct WeightedFairQueue<T> {
    /// Per-class queues.
    queues: [ClassQueue<T>; 4],

    /// Maximum queue depth per class.
    max_depth: usize,

    /// Total operations enqueued.
    total_enqueued: u64,

    /// Total operations dequeued.
    total_dequeued: u64,
}

impl<T> WeightedFairQueue<T> {
    /// Creates a new weighted fair queue.
    #[must_use]
    pub fn new(config: &FairQueueConfig) -> Self {
        Self {
            queues: [
                ClassQueue::new(IoClass::Write, config.write_weight),
                ClassQueue::new(IoClass::LiveRead, config.live_read_weight),
                ClassQueue::new(IoClass::BackfillRead, config.backfill_read_weight),
                ClassQueue::new(IoClass::Tiering, config.tiering_weight),
            ],
            max_depth: config.max_queue_depth,
            total_enqueued: 0,
            total_dequeued: 0,
        }
    }

    /// Returns the index for an I/O class.
    const fn class_index(class: IoClass) -> usize {
        match class {
            IoClass::Write => 0,
            IoClass::LiveRead => 1,
            IoClass::BackfillRead => 2,
            IoClass::Tiering => 3,
        }
    }

    /// Enqueues an operation.
    ///
    /// # Arguments
    ///
    /// * `operation` - The operation to enqueue.
    ///
    /// # Errors
    ///
    /// Returns `FlowError::QueueFull` if the class queue is at capacity.
    pub fn enqueue(&mut self, operation: IoOperation<T>) -> FlowResult<()> {
        let index = Self::class_index(operation.class);
        let queue = &mut self.queues[index];

        if queue.len() >= self.max_depth {
            return Err(FlowError::QueueFull {
                size: queue.len(),
                max_size: self.max_depth,
            });
        }

        queue.queue.push_back(operation);
        self.total_enqueued += 1;
        Ok(())
    }

    /// Dequeues the next operation according to weighted fair scheduling.
    ///
    /// Returns the operation from the class with the smallest virtual time
    /// that has pending operations.
    pub fn dequeue(&mut self) -> Option<IoOperation<T>> {
        // Precision multiplier to avoid losing precision with integer division.
        const PRECISION: u64 = 1_000_000;

        // Find the non-empty queue with the smallest virtual time.
        let mut best_index: Option<usize> = None;
        let mut best_vtime = u64::MAX;

        for (i, queue) in self.queues.iter().enumerate() {
            if !queue.is_empty() && queue.virtual_time < best_vtime {
                best_vtime = queue.virtual_time;
                best_index = Some(i);
            }
        }

        let index = best_index?;
        let queue = &mut self.queues[index];
        let operation = queue.queue.pop_front()?;

        // Update virtual time: vtime += size / weight.
        // Use u128 to avoid overflow, then scale appropriately.
        #[allow(clippy::cast_possible_truncation)] // Truncation bounded by PRECISION.
        let vtime_increment = if queue.weight > 0 {
            (u128::from(operation.size_bytes) * u128::from(PRECISION) / u128::from(queue.weight))
                as u64
        } else {
            PRECISION // Treat zero weight as weight 1.
        };
        queue.virtual_time = queue.virtual_time.saturating_add(vtime_increment);

        self.total_dequeued += 1;
        Some(operation)
    }

    /// Returns the number of pending operations for a class.
    #[must_use]
    pub fn pending(&self, class: IoClass) -> usize {
        self.queues[Self::class_index(class)].len()
    }

    /// Returns the total number of pending operations across all classes.
    #[must_use]
    pub fn total_pending(&self) -> usize {
        self.queues.iter().map(ClassQueue::len).sum()
    }

    /// Returns whether all queues are empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queues.iter().all(ClassQueue::is_empty)
    }

    /// Returns statistics about the queue.
    #[must_use]
    pub fn stats(&self) -> FairQueueStats {
        FairQueueStats {
            pending_write: self.pending(IoClass::Write),
            pending_live_read: self.pending(IoClass::LiveRead),
            pending_backfill_read: self.pending(IoClass::BackfillRead),
            pending_tiering: self.pending(IoClass::Tiering),
            total_enqueued: self.total_enqueued,
            total_dequeued: self.total_dequeued,
        }
    }

    /// Resets all virtual times to zero.
    ///
    /// This can be useful to prevent virtual time from growing unboundedly,
    /// though `saturating_add` prevents overflow.
    pub fn reset_virtual_times(&mut self) {
        for queue in &mut self.queues {
            queue.virtual_time = 0;
        }
    }

    /// Updates the weight for a specific I/O class.
    pub const fn set_weight(&mut self, class: IoClass, weight: u32) {
        self.queues[Self::class_index(class)].weight = weight;
    }

    /// Drains all operations from a specific class.
    pub fn drain_class(&mut self, class: IoClass) -> Vec<IoOperation<T>> {
        let queue = &mut self.queues[Self::class_index(class)];
        let drained: Vec<_> = queue.queue.drain(..).collect();
        self.total_dequeued += drained.len() as u64;
        drained
    }
}

/// Statistics about the fair queue.
#[derive(Debug, Clone, Default)]
pub struct FairQueueStats {
    /// Pending write operations.
    pub pending_write: usize,
    /// Pending live read operations.
    pub pending_live_read: usize,
    /// Pending backfill read operations.
    pub pending_backfill_read: usize,
    /// Pending tiering operations.
    pub pending_tiering: usize,
    /// Total operations enqueued.
    pub total_enqueued: u64,
    /// Total operations dequeued.
    pub total_dequeued: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enqueue_dequeue() {
        let config = FairQueueConfig::for_testing();
        let mut queue = WeightedFairQueue::new(&config);

        queue
            .enqueue(IoOperation {
                class: IoClass::Write,
                size_bytes: 1000,
                payload: "op1",
                enqueue_time_us: 0,
            })
            .unwrap();

        assert_eq!(queue.pending(IoClass::Write), 1);
        assert_eq!(queue.total_pending(), 1);

        let op = queue.dequeue().unwrap();
        assert_eq!(op.payload, "op1");
        assert_eq!(op.class, IoClass::Write);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_queue_full() {
        let mut config = FairQueueConfig::for_testing();
        config.max_queue_depth = 2;
        let mut queue: WeightedFairQueue<i32> = WeightedFairQueue::new(&config);

        queue
            .enqueue(IoOperation {
                class: IoClass::Write,
                size_bytes: 100,
                payload: 1,
                enqueue_time_us: 0,
            })
            .unwrap();
        queue
            .enqueue(IoOperation {
                class: IoClass::Write,
                size_bytes: 100,
                payload: 2,
                enqueue_time_us: 0,
            })
            .unwrap();

        let result = queue.enqueue(IoOperation {
            class: IoClass::Write,
            size_bytes: 100,
            payload: 3,
            enqueue_time_us: 0,
        });

        assert!(matches!(result, Err(FlowError::QueueFull { .. })));
    }

    #[test]
    fn test_weighted_fairness() {
        // Set up: Write weight 100, Tiering weight 25.
        // Write should get ~4x the bandwidth.
        let config = FairQueueConfig::for_testing();
        let mut queue: WeightedFairQueue<&str> = WeightedFairQueue::new(&config);

        // Enqueue same-sized operations from both classes.
        for _ in 0..4 {
            queue
                .enqueue(IoOperation {
                    class: IoClass::Write,
                    size_bytes: 1000,
                    payload: "write",
                    enqueue_time_us: 0,
                })
                .unwrap();
            queue
                .enqueue(IoOperation {
                    class: IoClass::Tiering,
                    size_bytes: 1000,
                    payload: "tier",
                    enqueue_time_us: 0,
                })
                .unwrap();
        }

        // Dequeue all and count.
        let mut write_count = 0;
        let mut tier_count = 0;
        let mut order = Vec::new();

        while let Some(op) = queue.dequeue() {
            order.push(op.class);
            match op.class {
                IoClass::Write => write_count += 1,
                IoClass::Tiering => tier_count += 1,
                _ => {}
            }
        }

        assert_eq!(write_count, 4);
        assert_eq!(tier_count, 4);

        // Due to weighted fair queuing, writes should generally be served more frequently.
        // With weight ratio 100:25 (4:1), after 4 write ops consume vtime 4000,
        // a single tier op consumes vtime 4000. So pattern should favor writes early.
        // First operation should be write (both start at vtime 0, but write is first in array).
        assert_eq!(order[0], IoClass::Write);
    }

    #[test]
    fn test_empty_queue_returns_none() {
        let config = FairQueueConfig::for_testing();
        let mut queue: WeightedFairQueue<i32> = WeightedFairQueue::new(&config);

        assert!(queue.dequeue().is_none());
        assert!(queue.is_empty());
    }

    #[test]
    fn test_stats() {
        let config = FairQueueConfig::for_testing();
        let mut queue: WeightedFairQueue<i32> = WeightedFairQueue::new(&config);

        queue
            .enqueue(IoOperation {
                class: IoClass::Write,
                size_bytes: 100,
                payload: 1,
                enqueue_time_us: 0,
            })
            .unwrap();
        queue
            .enqueue(IoOperation {
                class: IoClass::LiveRead,
                size_bytes: 100,
                payload: 2,
                enqueue_time_us: 0,
            })
            .unwrap();

        let stats = queue.stats();
        assert_eq!(stats.pending_write, 1);
        assert_eq!(stats.pending_live_read, 1);
        assert_eq!(stats.total_enqueued, 2);
        assert_eq!(stats.total_dequeued, 0);

        queue.dequeue();
        let stats = queue.stats();
        assert_eq!(stats.total_dequeued, 1);
    }

    #[test]
    fn test_drain_class() {
        let config = FairQueueConfig::for_testing();
        let mut queue: WeightedFairQueue<i32> = WeightedFairQueue::new(&config);

        for i in 0..5 {
            queue
                .enqueue(IoOperation {
                    class: IoClass::Tiering,
                    size_bytes: 100,
                    payload: i,
                    enqueue_time_us: 0,
                })
                .unwrap();
        }

        let drained = queue.drain_class(IoClass::Tiering);
        assert_eq!(drained.len(), 5);
        assert_eq!(queue.pending(IoClass::Tiering), 0);
    }

    #[test]
    fn test_set_weight() {
        let config = FairQueueConfig::for_testing();
        let mut queue: WeightedFairQueue<i32> = WeightedFairQueue::new(&config);

        queue.set_weight(IoClass::Tiering, 200);

        // After setting tiering weight higher, it should get more bandwidth.
        // But this test just verifies the method works.
        queue
            .enqueue(IoOperation {
                class: IoClass::Tiering,
                size_bytes: 100,
                payload: 1,
                enqueue_time_us: 0,
            })
            .unwrap();

        let op = queue.dequeue().unwrap();
        assert_eq!(op.class, IoClass::Tiering);
    }

    #[test]
    fn test_all_io_classes() {
        let config = FairQueueConfig::for_testing();
        let mut queue: WeightedFairQueue<&str> = WeightedFairQueue::new(&config);

        // Enqueue one of each class.
        queue
            .enqueue(IoOperation {
                class: IoClass::Write,
                size_bytes: 100,
                payload: "write",
                enqueue_time_us: 0,
            })
            .unwrap();
        queue
            .enqueue(IoOperation {
                class: IoClass::LiveRead,
                size_bytes: 100,
                payload: "live_read",
                enqueue_time_us: 0,
            })
            .unwrap();
        queue
            .enqueue(IoOperation {
                class: IoClass::BackfillRead,
                size_bytes: 100,
                payload: "backfill",
                enqueue_time_us: 0,
            })
            .unwrap();
        queue
            .enqueue(IoOperation {
                class: IoClass::Tiering,
                size_bytes: 100,
                payload: "tiering",
                enqueue_time_us: 0,
            })
            .unwrap();

        assert_eq!(queue.pending(IoClass::Write), 1);
        assert_eq!(queue.pending(IoClass::LiveRead), 1);
        assert_eq!(queue.pending(IoClass::BackfillRead), 1);
        assert_eq!(queue.pending(IoClass::Tiering), 1);
        assert_eq!(queue.total_pending(), 4);

        // Dequeue all.
        let mut classes = Vec::new();
        while let Some(op) = queue.dequeue() {
            classes.push(op.class);
        }
        assert_eq!(classes.len(), 4);
    }

    #[test]
    fn test_reset_virtual_times() {
        let config = FairQueueConfig::for_testing();
        let mut queue: WeightedFairQueue<i32> = WeightedFairQueue::new(&config);

        // Enqueue and dequeue some operations to accumulate virtual time.
        for i in 0..10 {
            queue
                .enqueue(IoOperation {
                    class: IoClass::Write,
                    size_bytes: 1000,
                    payload: i,
                    enqueue_time_us: 0,
                })
                .unwrap();
        }
        while queue.dequeue().is_some() {}

        // Reset virtual times.
        queue.reset_virtual_times();

        // Should still work correctly after reset.
        queue
            .enqueue(IoOperation {
                class: IoClass::Write,
                size_bytes: 100,
                payload: 99,
                enqueue_time_us: 0,
            })
            .unwrap();
        let op = queue.dequeue().unwrap();
        assert_eq!(op.payload, 99);
    }

    #[test]
    fn test_zero_weight_treated_as_one() {
        let mut config = FairQueueConfig::for_testing();
        config.tiering_weight = 0;
        let mut queue: WeightedFairQueue<i32> = WeightedFairQueue::new(&config);

        // Should not panic with zero weight.
        queue
            .enqueue(IoOperation {
                class: IoClass::Tiering,
                size_bytes: 100,
                payload: 1,
                enqueue_time_us: 0,
            })
            .unwrap();

        let op = queue.dequeue().unwrap();
        assert_eq!(op.class, IoClass::Tiering);
    }

    #[test]
    fn test_io_class_all() {
        let classes = IoClass::all();
        assert_eq!(classes.len(), 4);
        assert!(classes.contains(&IoClass::Write));
        assert!(classes.contains(&IoClass::LiveRead));
        assert!(classes.contains(&IoClass::BackfillRead));
        assert!(classes.contains(&IoClass::Tiering));
    }

    #[test]
    fn test_default_weights() {
        assert_eq!(IoClass::Write.default_weight(), 100);
        assert_eq!(IoClass::LiveRead.default_weight(), 100);
        assert_eq!(IoClass::BackfillRead.default_weight(), 50);
        assert_eq!(IoClass::Tiering.default_weight(), 25);
    }

    #[test]
    fn test_fairness_with_different_sizes() {
        let config = FairQueueConfig::for_testing();
        let mut queue: WeightedFairQueue<&str> = WeightedFairQueue::new(&config);

        // Large write operation.
        queue
            .enqueue(IoOperation {
                class: IoClass::Write,
                size_bytes: 10000,
                payload: "large_write",
                enqueue_time_us: 0,
            })
            .unwrap();

        // Small tiering operation.
        queue
            .enqueue(IoOperation {
                class: IoClass::Tiering,
                size_bytes: 100,
                payload: "small_tier",
                enqueue_time_us: 0,
            })
            .unwrap();

        // First should be write (both start at vtime 0, write is first).
        let op1 = queue.dequeue().unwrap();
        assert_eq!(op1.payload, "large_write");

        // After large write, tiering should go next (smaller vtime increment due to smaller size).
        let op2 = queue.dequeue().unwrap();
        assert_eq!(op2.payload, "small_tier");
    }

    #[test]
    fn test_per_class_queue_depth() {
        let mut config = FairQueueConfig::for_testing();
        config.max_queue_depth = 3;
        let mut queue: WeightedFairQueue<i32> = WeightedFairQueue::new(&config);

        // Fill Write queue.
        for i in 0..3 {
            queue
                .enqueue(IoOperation {
                    class: IoClass::Write,
                    size_bytes: 100,
                    payload: i,
                    enqueue_time_us: 0,
                })
                .unwrap();
        }

        // Write queue is full.
        let result = queue.enqueue(IoOperation {
            class: IoClass::Write,
            size_bytes: 100,
            payload: 99,
            enqueue_time_us: 0,
        });
        assert!(matches!(result, Err(FlowError::QueueFull { .. })));

        // But Tiering queue still has space.
        queue
            .enqueue(IoOperation {
                class: IoClass::Tiering,
                size_bytes: 100,
                payload: 1,
                enqueue_time_us: 0,
            })
            .unwrap();
        assert_eq!(queue.pending(IoClass::Tiering), 1);
    }
}
