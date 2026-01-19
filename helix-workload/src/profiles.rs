//! Benchmark profiles for load testing.
//!
//! Provides predefined configurations inspired by kafka-benchmark,
//! with sensible defaults for different testing scenarios.
//!
//! # Example
//!
//! ```ignore
//! use helix_workload::profiles::{BenchmarkProfile, load_profile};
//!
//! // Load a named profile
//! let profile = load_profile("throughput").unwrap();
//!
//! // Or load from a TOML file
//! let profile = BenchmarkProfile::from_file("custom.toml").unwrap();
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Producer configuration for benchmarks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProducerConfig {
    /// Number of producer threads/tasks.
    pub threads: usize,
    /// Messages in-flight per producer.
    pub inflight: usize,
    /// Message payload size in bytes.
    pub message_size: usize,
    /// Client-side linger (queue.buffering.max.ms).
    pub linger_ms: u64,
    /// Client-side batch size in bytes.
    pub batch_size: usize,
    /// Compression codec (none, lz4, snappy, zstd).
    pub compression: String,
    /// Required acks (-1 = all, 0 = none, 1 = leader).
    pub acks: i32,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            threads: 4,
            inflight: 1000,
            message_size: 1024,
            linger_ms: 5,
            batch_size: 524_288, // 512KB
            compression: "lz4".to_string(),
            acks: -1,
        }
    }
}

/// Cluster configuration for benchmarks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    /// Number of broker nodes.
    pub nodes: u32,
    /// Replication factor for topics.
    pub replication_factor: u32,
    /// Number of partitions per topic.
    pub partitions: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            replication_factor: 3,
            partitions: 1,
        }
    }
}

/// Test execution configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ExecutionConfig {
    /// Warmup duration in seconds.
    pub warmup_secs: u64,
    /// Test duration in seconds.
    pub duration_secs: u64,
    /// Number of times to repeat the test.
    pub repeat: u32,
    /// Pause between repeats in seconds.
    pub pause_secs: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            warmup_secs: 3,
            duration_secs: 10,
            repeat: 1,
            pause_secs: 5,
        }
    }
}

/// A complete benchmark profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BenchmarkProfile {
    /// Profile name.
    pub name: String,
    /// Description of what this profile tests.
    pub description: String,
    /// Producer configuration.
    pub producer: ProducerConfig,
    /// Cluster configuration.
    pub cluster: ClusterConfig,
    /// Execution configuration.
    pub execution: ExecutionConfig,
}

impl Default for BenchmarkProfile {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            description: "Default benchmark profile".to_string(),
            producer: ProducerConfig::default(),
            cluster: ClusterConfig::default(),
            execution: ExecutionConfig::default(),
        }
    }
}

impl BenchmarkProfile {
    /// Load a profile from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ProfileError> {
        let contents = std::fs::read_to_string(path.as_ref()).map_err(|e| ProfileError::Io {
            path: path.as_ref().display().to_string(),
            source: e,
        })?;
        Self::from_toml(&contents)
    }

    /// Parse a profile from a TOML string.
    ///
    /// # Errors
    ///
    /// Returns an error if the TOML cannot be parsed.
    pub fn from_toml(toml: &str) -> Result<Self, ProfileError> {
        toml::from_str(toml).map_err(|e| ProfileError::Parse {
            message: e.to_string(),
        })
    }

    /// Serialize the profile to a TOML string.
    #[must_use]
    pub fn to_toml(&self) -> String {
        toml::to_string_pretty(self).unwrap_or_default()
    }
}

/// Error type for profile operations.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    /// I/O error reading profile file.
    #[error("failed to read profile from {path}: {source}")]
    Io {
        /// File path.
        path: String,
        /// Underlying error.
        source: std::io::Error,
    },
    /// Parse error in TOML.
    #[error("failed to parse profile: {message}")]
    Parse {
        /// Error message.
        message: String,
    },
    /// Profile not found.
    #[error("profile not found: {name}")]
    NotFound {
        /// Profile name.
        name: String,
    },
}

#[allow(clippy::too_many_arguments)]
fn profile(
    name: &str,
    desc: &str,
    threads: u32,
    inflight: u32,
    linger_ms: u64,
    batch_size: u32,
    compression: &str,
    warmup: u64,
    duration: u64,
    repeat: u32,
    pause: u64,
) -> BenchmarkProfile {
    BenchmarkProfile {
        name: name.to_string(),
        description: desc.to_string(),
        producer: ProducerConfig {
            threads: threads as usize,
            inflight: inflight as usize,
            message_size: 1024,
            linger_ms,
            batch_size: batch_size as usize,
            compression: compression.to_string(),
            acks: -1,
        },
        cluster: ClusterConfig::default(),
        execution: ExecutionConfig {
            warmup_secs: warmup,
            duration_secs: duration,
            repeat,
            pause_secs: pause,
        },
    }
}

/// Built-in benchmark profiles for common testing scenarios.
#[must_use]
pub fn builtin_profiles() -> HashMap<&'static str, BenchmarkProfile> {
    HashMap::from([
        ("baseline", profile("baseline", "Quick sanity check with minimal load", 1, 100, 5, 65_536, "none", 2, 5, 1, 0)),
        ("throughput", profile("throughput", "Maximum throughput with batching and compression", 6, 2000, 5, 524_288, "lz4", 3, 10, 1, 0)),
        ("latency", profile("latency", "Low latency with minimal batching", 4, 100, 0, 16_384, "none", 2, 10, 1, 0)),
        ("stress", profile("stress", "High load stress test to find limits", 12, 4000, 5, 1_048_576, "lz4", 5, 30, 1, 0)),
        ("sustained", profile("sustained", "Extended run for stability validation", 6, 1000, 5, 524_288, "lz4", 5, 60, 3, 10)),
    ])
}

/// Load a built-in profile by name.
///
/// # Errors
///
/// Returns an error if the profile name is not found.
pub fn load_profile(name: &str) -> Result<BenchmarkProfile, ProfileError> {
    builtin_profiles()
        .remove(name)
        .ok_or_else(|| ProfileError::NotFound {
            name: name.to_string(),
        })
}

/// List all available built-in profile names.
#[must_use]
pub fn list_profiles() -> Vec<&'static str> {
    let mut names: Vec<_> = builtin_profiles().keys().copied().collect();
    names.sort_unstable();
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_profile() {
        let profile = BenchmarkProfile::default();
        assert_eq!(profile.producer.threads, 4);
        assert_eq!(profile.producer.message_size, 1024);
    }

    #[test]
    fn test_builtin_profiles() {
        let profiles = builtin_profiles();
        assert!(profiles.contains_key("baseline"));
        assert!(profiles.contains_key("throughput"));
        assert!(profiles.contains_key("latency"));
        assert!(profiles.contains_key("stress"));
        assert!(profiles.contains_key("sustained"));
    }

    #[test]
    fn test_load_profile() {
        let profile = load_profile("throughput").unwrap();
        assert_eq!(profile.name, "throughput");
        assert_eq!(profile.producer.threads, 6);
    }

    #[test]
    fn test_profile_not_found() {
        let result = load_profile("nonexistent");
        assert!(matches!(result, Err(ProfileError::NotFound { .. })));
    }

    #[test]
    fn test_toml_roundtrip() {
        let profile = load_profile("baseline").unwrap();
        let toml = profile.to_toml();
        let parsed = BenchmarkProfile::from_toml(&toml).unwrap();
        assert_eq!(parsed.name, profile.name);
        assert_eq!(parsed.producer.threads, profile.producer.threads);
    }
}
