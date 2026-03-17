//! CoolMine Engine - Core mining loop that orchestrates all components
//!
//! The MiningEngine is the heart of CoolMine, combining:
//! - Thermal Governor for temperature management
//! - Idle-Burst pattern for power efficiency  
//! - Cache-aligned scratchpad for performance
//! - Mining statistics and reporting
//!
//! It provides both async and sync mining interfaces with comprehensive
//! monitoring and adaptive behavior.

use crate::aligned::{OptimizedSequal256, AlignedScratchpadError};
use crate::burst::{BurstController, BurstConfig};
use crate::thermal::ThermalGovernor;
use crossbeam_channel::{Receiver, Sender, unbounded};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use taron_core::hash::{Sequal256, MINING_STEPS, meets_target};
use tracing::{debug, info, error, trace};

/// Default difficulty target for mining
const DEFAULT_DIFFICULTY: u64 = 1u64 << (64 - 16);

/// Statistics update interval (1 second)
const STATS_UPDATE_INTERVAL: Duration = Duration::from_secs(1);

/// Mining engine configuration
#[derive(Debug, Clone)]
pub struct MiningConfig {
    /// Mining difficulty target (u64, lower = harder)
    pub difficulty: u64,
    /// Number of SEQUAL-256 steps per hash
    pub steps: u32,
    /// Enable thermal management
    pub thermal_enabled: bool,
    /// Burst mining configuration
    pub burst_config: BurstConfig,
    /// Target temperature for thermal control (Celsius)
    pub target_temp_celsius: f32,
    /// Worker thread count (0 = auto-detect)
    pub worker_threads: usize,
}

impl Default for MiningConfig {
    fn default() -> Self {
        Self {
            difficulty: DEFAULT_DIFFICULTY,
            steps: MINING_STEPS,
            thermal_enabled: true,
            burst_config: BurstConfig::balanced(),
            target_temp_celsius: 55.0,
            worker_threads: 0, // Auto-detect
        }
    }
}

impl MiningConfig {
    /// Create low-power mining configuration
    pub fn low_power() -> Self {
        Self {
            difficulty: DEFAULT_DIFFICULTY,
            steps: MINING_STEPS,
            thermal_enabled: true,
            burst_config: BurstConfig::low_power(),
            target_temp_celsius: 50.0,
            worker_threads: num_cpus::get().min(2),
        }
    }

    /// Create performance mining configuration
    pub fn performance() -> Self {
        Self {
            difficulty: DEFAULT_DIFFICULTY,
            steps: MINING_STEPS,
            thermal_enabled: true,
            burst_config: BurstConfig::performance(),
            target_temp_celsius: 70.0,
            worker_threads: num_cpus::get(),
        }
    }

    /// Create configuration for benchmarking
    pub fn benchmark() -> Self {
        Self {
            difficulty: u64::MAX, // No difficulty requirement for benchmarking
            steps: MINING_STEPS,
            thermal_enabled: true,
            burst_config: BurstConfig::balanced(),
            target_temp_celsius: 60.0,
            worker_threads: 1, // Single thread for consistent benchmarking
        }
    }
}

/// Mining statistics
#[derive(Debug, Default, Clone)]
pub struct MiningStats {
    pub total_hashes: u64,
    pub valid_hashes: u64,
    pub runtime: Duration,
    pub hashrate: f64, // Hashes per second
    pub effective_hashrate: f64, // Accounting for burst pattern
    pub temperature: i32, // Current temperature in millicelsius
    pub thermal_intensity: f32, // Current thermal intensity (0.1-1.0)
    pub power_efficiency: f64, // Estimated performance per watt
    pub burst_duty_cycle: f64, // Actual burst duty cycle percentage
}

impl MiningStats {
    /// Calculate hashrate from total hashes and runtime
    pub fn calculate_hashrate(&mut self) {
        if !self.runtime.is_zero() {
            self.hashrate = self.total_hashes as f64 / self.runtime.as_secs_f64();
            self.effective_hashrate = self.hashrate * (self.burst_duty_cycle / 100.0);
        }
    }

    /// Estimated power consumption in watts (rough CPU estimation)
    pub fn estimated_watts(&self) -> f64 {
        // Rough estimation: modern CPU at full load ~65W, scales with thermal intensity
        65.0 * self.thermal_intensity as f64
    }

    /// Hashes per watt efficiency metric
    pub fn hashes_per_watt(&self) -> f64 {
        let watts = self.estimated_watts();
        if watts > 0.0 {
            self.hashrate / watts
        } else {
            0.0
        }
    }
}

/// Mining result from a successful hash
#[derive(Debug, Clone)]
pub struct MiningResult {
    pub hash: [u8; 32],
    pub nonce: u64,
    pub difficulty: u64,
    pub steps: u32,
    pub timestamp: Instant,
}

/// Commands for controlling the mining engine
#[derive(Debug, Clone)]
pub enum MiningCommand {
    /// Start mining with given block header
    Start { block_header: Vec<u8> },
    /// Stop mining
    Stop,
    /// Update configuration
    UpdateConfig(MiningConfig),
    /// Get current statistics
    GetStats,
    /// Pause mining temporarily
    Pause,
    /// Resume mining
    Resume,
}

/// Events emitted by the mining engine
#[derive(Debug, Clone)]
pub enum MiningEvent {
    /// Mining started
    Started,
    /// Mining stopped
    Stopped,
    /// Valid hash found
    HashFound(MiningResult),
    /// Statistics update
    StatsUpdate(MiningStats),
    /// Error occurred
    Error(String),
    /// Mining paused
    Paused,
    /// Mining resumed
    Resumed,
}

/// Core mining engine that orchestrates all components
pub struct MiningEngine {
    config: Arc<RwLock<MiningConfig>>,
    stats: Arc<RwLock<MiningStats>>,
    thermal_governor: ThermalGovernor,
    burst_controller: Arc<RwLock<BurstController>>,
    is_running: Arc<AtomicBool>,
    is_paused: Arc<AtomicBool>,
    current_nonce: Arc<AtomicU64>,
    start_time: Arc<RwLock<Option<Instant>>>,
    
    // Communication channels
    command_tx: Sender<MiningCommand>,
    command_rx: Receiver<MiningCommand>,
    event_tx: Sender<MiningEvent>,
    event_rx: Receiver<MiningEvent>,
}

impl MiningEngine {
    /// Create new mining engine with default configuration
    pub fn new() -> Result<Self, MiningEngineError> {
        let config = MiningConfig::default();
        Self::with_config(config)
    }

    /// Create mining engine with custom configuration
    pub fn with_config(config: MiningConfig) -> Result<Self, MiningEngineError> {
        let thermal_governor = if config.thermal_enabled {
            ThermalGovernor::with_target_temp(config.target_temp_celsius)
        } else {
            ThermalGovernor::new()
        };

        let (command_tx, command_rx) = unbounded();
        let (event_tx, event_rx) = unbounded();

        let engine = Self {
            config: Arc::new(RwLock::new(config.clone())),
            stats: Arc::new(RwLock::new(MiningStats::default())),
            thermal_governor,
            burst_controller: Arc::new(RwLock::new(BurstController::with_config(config.burst_config))),
            is_running: Arc::new(AtomicBool::new(false)),
            is_paused: Arc::new(AtomicBool::new(false)),
            current_nonce: Arc::new(AtomicU64::new(0)),
            start_time: Arc::new(RwLock::new(None)),
            command_tx,
            command_rx,
            event_tx,
            event_rx,
        };

        info!("MiningEngine initialized with config: {:?}", config);
        Ok(engine)
    }

    /// Get command sender for external control
    pub fn command_sender(&self) -> Sender<MiningCommand> {
        self.command_tx.clone()
    }

    /// Get event receiver for monitoring
    pub fn event_receiver(&self) -> Receiver<MiningEvent> {
        self.event_rx.clone()
    }

    /// Get current statistics
    pub fn stats(&self) -> MiningStats {
        self.stats.read().clone()
    }

    /// Check if engine is currently running
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Acquire)
    }

    /// Check if engine is paused
    pub fn is_paused(&self) -> bool {
        self.is_paused.load(Ordering::Acquire)
    }

    /// Start the mining engine (async version)
    pub async fn start_async(&self) -> Result<JoinHandle<()>, MiningEngineError> {
        if self.is_running() {
            return Err(MiningEngineError::AlreadyRunning);
        }

        // Start thermal monitoring
        let _thermal_handle = self.thermal_governor.start_monitoring().await;

        // Start main mining loop
        let handle = self.start_mining_loop().await;
        
        Ok(handle)
    }

    /// Main mining loop (async)
    async fn start_mining_loop(&self) -> JoinHandle<()> {
        let engine = self.clone();
        
        tokio::spawn(async move {
            let mut current_block_header: Option<Vec<u8>> = None;
            let mut stats_timer = tokio::time::interval(STATS_UPDATE_INTERVAL);
            
            info!("Mining loop started");
            
            loop {
                tokio::select! {
                    // Handle commands
                    command = async {
                        match engine.command_rx.try_recv() {
                            Ok(cmd) => Some(cmd),
                            Err(_) => None,
                        }
                    } => {
                        if let Some(command) = command {
                        match command {
                            MiningCommand::Start { block_header } => {
                                current_block_header = Some(block_header);
                                engine.is_running.store(true, Ordering::Release);
                                *engine.start_time.write() = Some(Instant::now());
                                let _ = engine.event_tx.send(MiningEvent::Started);
                            }
                            MiningCommand::Stop => {
                                engine.is_running.store(false, Ordering::Release);
                                let _ = engine.event_tx.send(MiningEvent::Stopped);
                                break;
                            }
                            MiningCommand::Pause => {
                                engine.is_paused.store(true, Ordering::Release);
                                let _ = engine.event_tx.send(MiningEvent::Paused);
                            }
                            MiningCommand::Resume => {
                                engine.is_paused.store(false, Ordering::Release);
                                let _ = engine.event_tx.send(MiningEvent::Resumed);
                            }
                            MiningCommand::UpdateConfig(new_config) => {
                                *engine.config.write() = new_config.clone();
                                engine.burst_controller.write().update_config(new_config.burst_config);
                                info!("Mining configuration updated");
                            }
                            MiningCommand::GetStats => {
                                let stats = engine.stats();
                                let _ = engine.event_tx.send(MiningEvent::StatsUpdate(stats));
                            }
                        }
                        }
                    }
                    
                    // Statistics update timer
                    _ = stats_timer.tick() => {
                        if engine.is_running() {
                            engine.update_stats();
                            let stats = engine.stats();
                            let _ = engine.event_tx.send(MiningEvent::StatsUpdate(stats));
                        }
                    }
                    
                    // Mining work
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        if engine.is_running() && !engine.is_paused() {
                            if let Some(ref header) = current_block_header {
                                if let Err(e) = engine.mine_iteration(header).await {
                                    error!("Mining iteration failed: {}", e);
                                    let _ = engine.event_tx.send(MiningEvent::Error(e.to_string()));
                                }
                            }
                        }
                    }
                }
            }
            
            info!("Mining loop stopped");
        })
    }

    /// Single mining iteration
    async fn mine_iteration(&self, block_header: &[u8]) -> Result<(), MiningEngineError> {
        let config = self.config.read().clone();
        
        // Update thermal controller and burst pattern
        self.thermal_governor.update();
        let thermal_state = self.thermal_governor.state();
        
        // Adapt burst pattern to thermal conditions
        self.burst_controller.write().adapt_to_thermal_intensity(thermal_state.intensity);
        
        // Check if we should burst or idle - avoid holding lock across await
        let should_mine = {
            let mut controller = self.burst_controller.write();
            controller.cycle_blocking() // Use blocking version instead
        };
        
        if !should_mine || thermal_state.is_critical {
            return Ok(());
        }

        // Perform mining step
        let nonce = self.current_nonce.fetch_add(1, Ordering::Relaxed);
        
        // Use optimized SEQUAL-256 if possible, fallback to standard
        let hash = match self.mine_with_optimized_scratchpad(block_header, nonce, config.steps) {
            Ok(hash) => hash,
            Err(_) => {
                // Fallback to standard implementation
                Sequal256::mine_step(block_header, nonce, config.steps)
            }
        };

        // Update hash counter
        self.stats.write().total_hashes += 1;

        // Check if hash meets difficulty
        if meets_target(&hash, config.difficulty) {
            let result = MiningResult {
                hash,
                nonce,
                difficulty: config.difficulty,
                steps: config.steps,
                timestamp: Instant::now(),
            };
            
            self.stats.write().valid_hashes += 1;
            let _ = self.event_tx.send(MiningEvent::HashFound(result));
            
            debug!("Valid hash found: nonce={}, difficulty={}", nonce, config.difficulty);
        }

        Ok(())
    }

    /// Mine using optimized cache-aligned scratchpad
    fn mine_with_optimized_scratchpad(
        &self,
        block_header: &[u8],
        nonce: u64,
        steps: u32,
    ) -> Result<[u8; 32], AlignedScratchpadError> {
        thread_local! {
            static HASHER: std::cell::RefCell<Option<OptimizedSequal256>> = 
                std::cell::RefCell::new(None);
        }

        HASHER.with(|hasher| {
            let mut hasher = hasher.borrow_mut();
            if hasher.is_none() {
                *hasher = Some(OptimizedSequal256::new()?);
            }
            
            let hasher = hasher.as_mut().unwrap();
            Ok(hasher.mine_step_with_steps(block_header, nonce, steps))
        })
    }

    /// Update mining statistics
    fn update_stats(&self) {
        let mut stats = self.stats.write();
        
        // Update runtime
        if let Some(start_time) = *self.start_time.read() {
            stats.runtime = start_time.elapsed();
        }

        // Update thermal information
        let thermal_state = self.thermal_governor.state();
        stats.temperature = thermal_state.current_temp;
        stats.thermal_intensity = thermal_state.intensity;

        // Update burst statistics
        let burst_duty_cycle = {
            let burst_controller = self.burst_controller.read();
            burst_controller.stats().actual_duty_cycle()
        };
        stats.burst_duty_cycle = burst_duty_cycle;

        // Calculate hashrates
        stats.calculate_hashrate();
        stats.power_efficiency = stats.hashes_per_watt();

        trace!(
            "Stats updated: {} H/s, {}°C, {:.1}% duty cycle",
            stats.hashrate as u64,
            stats.temperature / 1000,
            stats.burst_duty_cycle
        );
    }
}

// Enable cloning for Arc usage
impl Clone for MiningEngine {
    fn clone(&self) -> Self {
        Self {
            config: Arc::clone(&self.config),
            stats: Arc::clone(&self.stats),
            thermal_governor: self.thermal_governor.clone(),
            burst_controller: Arc::clone(&self.burst_controller),
            is_running: Arc::clone(&self.is_running),
            is_paused: Arc::clone(&self.is_paused),
            current_nonce: Arc::clone(&self.current_nonce),
            start_time: Arc::clone(&self.start_time),
            command_tx: self.command_tx.clone(),
            command_rx: self.command_rx.clone(),
            event_tx: self.event_tx.clone(),
            event_rx: self.event_rx.clone(),
        }
    }
}

impl Default for MiningEngine {
    fn default() -> Self {
        Self::new().expect("Failed to create default MiningEngine")
    }
}

/// Errors that can occur in the mining engine
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MiningEngineError {
    /// Engine is already running
    AlreadyRunning,
    /// Engine is not running
    NotRunning,
    /// Scratchpad allocation failed
    ScratchpadError(AlignedScratchpadError),
    /// Configuration error
    ConfigError(String),
    /// Channel communication error
    ChannelError,
}

impl From<AlignedScratchpadError> for MiningEngineError {
    fn from(e: AlignedScratchpadError) -> Self {
        Self::ScratchpadError(e)
    }
}

impl std::fmt::Display for MiningEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyRunning => write!(f, "Mining engine is already running"),
            Self::NotRunning => write!(f, "Mining engine is not running"),
            Self::ScratchpadError(e) => write!(f, "Scratchpad error: {}", e),
            Self::ConfigError(msg) => write!(f, "Configuration error: {}", msg),
            Self::ChannelError => write!(f, "Channel communication error"),
        }
    }
}

impl std::error::Error for MiningEngineError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[test]
    fn test_mining_config_presets() {
        let low_power = MiningConfig::low_power();
        let performance = MiningConfig::performance();
        let benchmark = MiningConfig::benchmark();

        assert!(low_power.target_temp_celsius < performance.target_temp_celsius);
        assert_eq!(benchmark.difficulty, 0);
        assert_eq!(benchmark.worker_threads, 1);
    }

    #[test]
    fn test_mining_stats_calculations() {
        let mut stats = MiningStats {
            total_hashes: 1000,
            runtime: Duration::from_secs(10),
            thermal_intensity: 0.8,
            burst_duty_cycle: 75.0,
            ..Default::default()
        };

        stats.calculate_hashrate();
        
        assert_eq!(stats.hashrate, 100.0); // 1000 hashes / 10 seconds
        assert_eq!(stats.effective_hashrate, 75.0); // 100 * 0.75
        assert!(stats.estimated_watts() > 0.0);
        assert!(stats.hashes_per_watt() > 0.0);
    }

    #[tokio::test]
    async fn test_mining_engine_creation() {
        let engine = MiningEngine::new().unwrap();
        assert!(!engine.is_running());
        assert!(!engine.is_paused());
    }

    #[tokio::test]
    async fn test_command_channel() {
        let engine = MiningEngine::new().unwrap();
        let cmd_sender = engine.command_sender();
        
        // Send a command
        cmd_sender.send(MiningCommand::GetStats).unwrap();
        
        // Should be able to receive it (crossbeam doesn't have recv_async; use try_recv)
        std::thread::sleep(std::time::Duration::from_millis(10));
        let received = engine.command_rx.try_recv();
        assert!(received.is_ok());
    }

    #[tokio::test] 
    async fn test_mining_engine_start_stop() {
        let engine = MiningEngine::new().unwrap();
        let cmd_sender = engine.command_sender();
        let event_receiver = engine.event_receiver();
        
        // Start mining loop
        let handle = engine.start_async().await.unwrap();
        
        // Send start command
        cmd_sender.send(MiningCommand::Start {
            block_header: b"test block header".to_vec()
        }).unwrap();
        
        // Should receive started event.
        // Use spawn_blocking so we don't block the tokio executor (which would
        // prevent the mining loop from running in the same single-threaded runtime).
        let event = tokio::task::spawn_blocking(move || {
            event_receiver.recv_timeout(Duration::from_millis(2000))
        }).await.unwrap().unwrap();
        assert!(matches!(event, MiningEvent::Started));
        
        // Send stop command
        cmd_sender.send(MiningCommand::Stop).unwrap();
        
        // Wait for handle to complete
        let _ = timeout(Duration::from_secs(1), handle).await;
    }

    #[test]
    fn test_mining_result() {
        let result = MiningResult {
            hash: [0u8; 32],
            nonce: 12345,
            difficulty: 16,
            steps: MINING_STEPS,
            timestamp: Instant::now(),
        };
        
        assert_eq!(result.nonce, 12345);
        assert_eq!(result.difficulty, 16);
    }

    #[test]
    fn test_error_types() {
        let error = MiningEngineError::AlreadyRunning;
        assert_eq!(error.to_string(), "Mining engine is already running");
        
        let config_error = MiningEngineError::ConfigError("test error".to_string());
        assert!(config_error.to_string().contains("test error"));
    }
}