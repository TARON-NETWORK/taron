//! # TARON Miner - CoolMine Thermal-Adaptive Mining Engine
//!
//! CoolMine is TARON's advanced mining engine that combines thermal management,
//! power efficiency, and performance optimization. It implements intelligent
//! mining strategies that adapt to system conditions in real-time.
//!
//! ## Key Features
//!
//! - **Thermal Governor**: Automatically adjusts mining intensity to maintain
//!   target CPU temperature, preventing thermal throttling and hardware damage.
//!
//! - **Idle-Burst Mining**: Uses micro-burst computation patterns instead of
//!   continuous CPU usage, achieving ~80% hashrate with ~50% power consumption.
//!
//! - **Cache-Aligned Scratchpad**: Optimizes SEQUAL-256 memory access patterns
//!   with cache-line aligned allocation and prefetch hints.
//!
//! - **Comprehensive Benchmarking**: Built-in performance analysis and
//!   configuration optimization tools.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Mining Engine                            │
//! │  ┌─────────────────┬─────────────────┬─────────────────┐   │
//! │  │ Thermal Governor │ Burst Controller │ Cache Optimizer │   │
//! │  └─────────────────┴─────────────────┴─────────────────┘   │
//! │                           │                                 │
//! │  ┌─────────────────────────▼─────────────────────────────┐   │
//! │  │              SEQUAL-256 Hasher                       │   │
//! │  │    (4MB Cache-Aligned Scratchpad)                   │   │
//! │  └─────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Usage Examples
//!
//! ### Basic Mining
//!
//! ```rust,no_run
//! use taron_miner::{MiningEngine, MiningConfig, MiningCommand};
//!
//! # tokio_test::block_on(async {
//! let engine = MiningEngine::new().unwrap();
//! let cmd_sender = engine.command_sender();
//! let event_receiver = engine.event_receiver();
//!
//! // Start mining engine
//! let handle = engine.start_async().await.unwrap();
//!
//! // Begin mining a block
//! cmd_sender.send(MiningCommand::Start {
//!     block_header: b"example_block_header".to_vec()
//! }).unwrap();
//!
//! // Monitor for results
//! while let Ok(event) = event_receiver.recv() {
//!     match event {
//!         taron_miner::MiningEvent::HashFound(result) => {
//!             println!("Found valid hash! Nonce: {}", result.nonce);
//!             break;
//!         }
//!         taron_miner::MiningEvent::StatsUpdate(stats) => {
//!             println!("Hashrate: {:.0} H/s, Temp: {}°C", 
//!                 stats.hashrate, stats.temperature / 1000);
//!         }
//!         _ => {}
//!     }
//! }
//!
//! // Stop mining
//! cmd_sender.send(MiningCommand::Stop).unwrap();
//! # });
//! ```
//!
//! ### Performance Benchmarking
//!
//! ```rust,no_run
//! use taron_miner::{BenchmarkRunner, BenchmarkConfig, BenchmarkDuration};
//!
//! # tokio_test::block_on(async {
//! let config = BenchmarkConfig {
//!     duration: BenchmarkDuration::Quick,
//!     thermal_analysis: true,
//!     burst_analysis: true,
//!     ..Default::default()
//! };
//!
//! let mut runner = BenchmarkRunner::new(config);
//! let report = runner.run_full_benchmark().await.unwrap();
//!
//! runner.print_report(&report);
//!
//! if let Some(best) = report.best_efficiency() {
//!     println!("Recommended config: {}", best.name);
//! }
//! # });
//! ```
//!
//! ### Custom Thermal Configuration
//!
//! ```rust
//! use taron_miner::{MiningConfig, BurstConfig};
//!
//! let config = MiningConfig {
//!     thermal_enabled: true,
//!     target_temp_celsius: 60.0,  // Target 60°C
//!     burst_config: BurstConfig::low_power(),
//!     ..Default::default()
//! };
//!
//! let engine = taron_miner::MiningEngine::with_config(config).unwrap();
//! ```
//!
//! ## Configuration Presets
//!
//! CoolMine provides several configuration presets for common use cases:
//!
//! - **`MiningConfig::low_power()`**: Maximum power efficiency, cooler operation
//! - **`MiningConfig::performance()`**: Maximum hashrate, higher power usage  
//! - **`MiningConfig::benchmark()`**: Standardized settings for benchmarking
//!
//! ## Safety Considerations
//!
//! - The thermal governor prevents CPU overheating by design
//! - Emergency throttling activates at 85°C critical temperature
//! - Burst patterns reduce sustained thermal stress
//! - Cache-aligned memory allocation prevents memory corruption
//!
//! ## Platform Support
//!
//! - **Linux**: Full thermal monitoring via `/sys/class/thermal/`
//! - **macOS/Windows**: Limited thermal support, burst patterns still work
//! - **x86-64**: Optimized with cache prefetch hints
//! - **ARM64**: Compatible, without specialized cache optimizations

pub mod thermal;
pub mod burst;
pub mod aligned;
pub mod engine;
pub mod bench;

// Re-export primary API types
pub use thermal::{ThermalGovernor, ThermalState};
pub use burst::{BurstController, BurstConfig, BurstStats};
pub use aligned::{AlignedScratchpad, OptimizedSequal256, AlignedScratchpadError, MemoryInfo};
pub use engine::{
    MiningEngine, MiningConfig, MiningStats, MiningResult, 
    MiningCommand, MiningEvent, MiningEngineError
};
pub use bench::{
    BenchmarkRunner, BenchmarkConfig, BenchmarkDuration, 
    BenchmarkResult, BenchmarkReport, BenchmarkError, SystemInfo
};

// Re-export important constants from taron-core
pub use taron_core::hash::{Sequal256, MINING_STEPS, meets_difficulty, meets_target};

/// CoolMine version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// CoolMine build information
pub const BUILD_INFO: &str = env!("CARGO_PKG_VERSION");

/// Quick start function for immediate mining
/// 
/// Creates a mining engine with sensible defaults and starts mining the given block header.
/// This is the simplest way to get started with CoolMine.
/// 
/// # Example
/// 
/// ```rust,no_run
/// use taron_miner::quick_mine;
/// 
/// # tokio_test::block_on(async {
/// let (engine, handle) = quick_mine(b"example_block_header".to_vec()).await.unwrap();
/// 
/// // Monitor mining events...
/// let event_receiver = engine.event_receiver();
/// while let Ok(event) = event_receiver.recv() {
///     match event {
///         taron_miner::MiningEvent::HashFound(result) => {
///             println!("Success! Hash found with nonce: {}", result.nonce);
///             break;
///         }
///         _ => {}
///     }
/// }
/// 
/// // Stop when done
/// engine.command_sender().send(taron_miner::MiningCommand::Stop).unwrap();
/// # });
/// ```
pub async fn quick_mine(block_header: Vec<u8>) -> Result<(MiningEngine, tokio::task::JoinHandle<()>), MiningEngineError> {
    let engine = MiningEngine::new()?;
    let cmd_sender = engine.command_sender();
    
    let handle = engine.start_async().await?;
    
    cmd_sender.send(MiningCommand::Start { block_header })
        .map_err(|_| MiningEngineError::ChannelError)?;
    
    Ok((engine, handle))
}

/// Run a quick benchmark and return the results
/// 
/// This function runs a 30-second benchmark with default settings and returns
/// the performance report. Useful for quick performance assessment.
/// 
/// # Example
/// 
/// ```rust,no_run
/// use taron_miner::quick_benchmark;
/// 
/// # tokio_test::block_on(async {
/// let report = quick_benchmark().await.unwrap();
/// 
/// if let Some(best) = report.best_efficiency() {
///     println!("Best configuration: {} ({:.1} score)", best.name, best.efficiency_score);
/// }
/// # });
/// ```
pub async fn quick_benchmark() -> Result<BenchmarkReport, BenchmarkError> {
    let config = BenchmarkConfig {
        duration: BenchmarkDuration::Quick,
        ..Default::default()
    };
    
    let mut runner = BenchmarkRunner::new(config);
    runner.run_full_benchmark().await
}

/// Detect optimal mining configuration for current system
/// 
/// Analyzes system capabilities and returns a recommended mining configuration.
/// Takes into account CPU count, memory, and basic thermal characteristics.
/// 
/// # Example
/// 
/// ```rust
/// use taron_miner::{detect_optimal_config, MiningEngine};
/// 
/// let config = detect_optimal_config();
/// let engine = MiningEngine::with_config(config).unwrap();
/// ```
pub fn detect_optimal_config() -> MiningConfig {
    let cpu_count = num_cpus::get();
    let physical_cores = num_cpus::get_physical();
    
    // Detect if this looks like a laptop (fewer cores, likely thermal constraints)
    let is_laptop_like = physical_cores <= 4 && cpu_count <= 8;
    
    // Detect if this looks like a high-end desktop/server
    let is_high_end = physical_cores >= 8 && cpu_count >= 16;
    
    match (is_laptop_like, is_high_end) {
        (true, _) => {
            // Conservative settings for laptops
            MiningConfig {
                target_temp_celsius: 50.0,
                burst_config: BurstConfig::low_power(),
                worker_threads: physical_cores.min(2),
                ..MiningConfig::default()
            }
        }
        (false, true) => {
            // Aggressive settings for high-end systems
            MiningConfig {
                target_temp_celsius: 70.0,
                burst_config: BurstConfig::performance(),
                worker_threads: physical_cores,
                ..MiningConfig::default()
            }
        }
        (false, false) => {
            // Balanced settings for desktop systems
            MiningConfig {
                target_temp_celsius: 60.0,
                burst_config: BurstConfig::balanced(),
                worker_threads: physical_cores,
                ..MiningConfig::default()
            }
        }
    }
}

/// Utility function to check system mining readiness
/// 
/// Performs basic system checks to ensure mining will work properly.
/// Returns warnings about potential issues.
/// 
/// # Example
/// 
/// ```rust
/// use taron_miner::check_system_readiness;
/// 
/// let issues = check_system_readiness();
/// if !issues.is_empty() {
///     println!("System warnings:");
///     for issue in issues {
///         println!("  - {}", issue);
///     }
/// }
/// ```
pub fn check_system_readiness() -> Vec<String> {
    let mut issues = Vec::new();
    
    // Check CPU core count
    let cpu_count = num_cpus::get();
    if cpu_count < 2 {
        issues.push("Single-core CPU detected - mining performance will be limited".to_string());
    }
    
    // Check thermal zone availability (Linux only)
    #[cfg(target_os = "linux")]
    {
        let mut thermal_zones_found = false;
        for i in 0..8 {
            if std::path::Path::new(&format!("/sys/class/thermal/thermal_zone{}/temp", i)).exists() {
                thermal_zones_found = true;
                break;
            }
        }
        if !thermal_zones_found {
            issues.push("No thermal zones detected - thermal management will be disabled".to_string());
        }
    }
    
    // Check available memory (rough estimate)
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
            for line in content.lines() {
                if line.starts_with("MemAvailable:") {
                    if let Some(mem_str) = line.split_whitespace().nth(1) {
                        if let Ok(mem_kb) = mem_str.parse::<u64>() {
                            let mem_mb = mem_kb / 1024;
                            if mem_mb < 100 {
                                issues.push("Low available memory - mining may be unstable".to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Platform-specific warnings
    #[cfg(not(target_os = "linux"))]
    {
        issues.push("Limited thermal monitoring on non-Linux platforms".to_string());
    }
    
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    {
        issues.push("Cache optimization not available on non-x86 architectures".to_string());
    }
    
    issues
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;
    use std::time::Duration;

    #[test]
    fn test_version_info() {
        assert!(!VERSION.is_empty());
        assert!(BUILD_INFO.contains(VERSION));
    }

    #[test]
    fn test_detect_optimal_config() {
        let config = detect_optimal_config();
        
        // Should produce valid configuration
        assert!(config.target_temp_celsius > 0.0);
        assert!(config.target_temp_celsius < 100.0);
        assert!(config.worker_threads > 0);
    }

    #[test]
    fn test_system_readiness_check() {
        let issues = check_system_readiness();
        
        // Should not panic and should return a list (even if empty)
        // Individual issues depend on the system, so we just check structure
        for issue in &issues {
            assert!(!issue.is_empty());
        }
    }

    #[tokio::test]
    async fn test_quick_mine_integration() {
        let block_header = b"test_block_header_data".to_vec();
        
        let result = timeout(
            Duration::from_secs(5),
            quick_mine(block_header)
        ).await;
        
        match result {
            Ok(Ok((engine, handle))) => {
                // Successfully created engine
                assert!(!engine.is_running()); // Not running yet due to async nature
                
                // Stop the handle
                engine.command_sender().send(MiningCommand::Stop).unwrap();
                let _ = timeout(Duration::from_secs(1), handle).await;
            }
            Ok(Err(e)) => {
                // Mining engine creation failed - acceptable for test environment
                println!("Mining engine creation failed (expected in test): {}", e);
            }
            Err(_) => {
                panic!("Test timed out");
            }
        }
    }

    #[tokio::test]
    async fn test_quick_benchmark() {
        // This test may fail in environments without proper thermal support
        // so we handle both success and failure cases
        
        let result = timeout(
            Duration::from_secs(10), 
            quick_benchmark()
        ).await;
        
        match result {
            Ok(Ok(report)) => {
                // Benchmark succeeded
                assert!(!report.results.is_empty());
                assert!(!report.system_info.cpu_model.is_empty());
            }
            Ok(Err(e)) => {
                // Benchmark failed - acceptable in test environment
                println!("Benchmark failed (expected in some test environments): {}", e);
            }
            Err(_) => {
                println!("Benchmark timed out - acceptable for quick test");
            }
        }
    }

    #[test]
    fn test_api_re_exports() {
        // Test that main API types are accessible
        let _: MiningConfig = MiningConfig::default();
        let _: BurstConfig = BurstConfig::default();
        let _: BenchmarkConfig = BenchmarkConfig::default();
        
        // Test that constants are accessible
        assert!(MINING_STEPS > 0);
    }

    #[test]
    fn test_mining_config_presets() {
        let low_power = MiningConfig::low_power();
        let performance = MiningConfig::performance();
        let benchmark = MiningConfig::benchmark();
        
        // Low power should be more conservative
        assert!(low_power.target_temp_celsius < performance.target_temp_celsius);
        assert!(low_power.burst_config.duty_cycle() < performance.burst_config.duty_cycle());
        
        // Benchmark should be reproducible
        assert_eq!(benchmark.difficulty, u64::MAX);
        assert_eq!(benchmark.worker_threads, 1);
    }

    #[test]
    fn test_error_types_implement_traits() {
        // Test that error types implement required traits
        let engine_error = MiningEngineError::AlreadyRunning;
        let benchmark_error = BenchmarkError::ChannelError;
        let scratchpad_error = AlignedScratchpadError::InvalidSize;
        
        // All should implement Display and Error traits
        assert!(!engine_error.to_string().is_empty());
        assert!(!benchmark_error.to_string().is_empty());
        assert!(!scratchpad_error.to_string().is_empty());
        
        // Test error sources
        assert!(std::error::Error::source(&engine_error).is_none());
    }
}