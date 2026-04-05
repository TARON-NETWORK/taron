//! TaronNode — main P2P node orchestrating TCP gossip, mempool, peer management,
//! and the blockchain (chain of blocks).

use std::collections::{HashMap as StdHashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::{Mutex, RwLock, Semaphore, mpsc};
use tokio::task::JoinHandle;
use taron_core::{Block, Blockchain, Transaction, Ledger, Wallet, TransactionStatus, TESTNET_DIFFICULTY, FINALITY_DEPTH, genesis_hash_hex};
use tracing::{debug, info, warn};

use crate::mempool::Mempool;
use crate::peer::{PeerDirection, PeerManager};
use crate::protocol::{self, Message};
use crate::seeds::resolve_seeds;
use crate::finality::NodeFinalityManager;

/// Default TCP listen port.
pub const DEFAULT_PORT: u16 = 8333;

/// In-memory cache: IP → ISO country code. Shared across all lookup calls.
static GEO_CACHE: std::sync::LazyLock<Mutex<StdHashMap<IpAddr, String>>> =
    std::sync::LazyLock::new(|| Mutex::new(StdHashMap::new()));

/// Seed Relay Lock — prevents fork propagation by locking a height for 2 seconds.
/// After the first valid block at height N is broadcast, competing blocks at the
/// same height are rejected for RELAY_LOCK_DURATION (unless they have a strictly
/// lower hash). Stores (height, hash, timestamp_of_first_broadcast).
static RELAY_LOCK: std::sync::LazyLock<Mutex<(u64, [u8; 32], Instant)>> =
    std::sync::LazyLock::new(|| Mutex::new((0, [0xff; 32], Instant::now())));

/// How long the relay lock holds after the first block at a height is broadcast.
const RELAY_LOCK_DURATION: std::time::Duration = std::time::Duration::from_secs(2);

/// Resolve an IP address to an ISO 3166-1 alpha-2 country code via ip-api.com.
/// Results are cached in-memory so each IP is looked up at most once.
async fn geo_lookup(ip: IpAddr) -> String {
    // Check cache first
    {
        let cache = GEO_CACHE.lock().await;
        if let Some(cc) = cache.get(&ip) {
            return cc.clone();
        }
    }
    // Skip private/loopback IPs
    if ip.is_loopback() || ip.is_unspecified() {
        return "LOCAL".into();
    }
    let url = format!("http://ip-api.com/json/{}?fields=countryCode", ip);
    let cc = match reqwest::get(&url).await {
        Ok(resp) => {
            resp.json::<serde_json::Value>().await
                .ok()
                .and_then(|v| v.get("countryCode").and_then(|c| c.as_str().map(String::from)))
                .unwrap_or_default()
        }
        Err(_) => String::new(),
    };
    let mut cache = GEO_CACHE.lock().await;
    cache.insert(ip, cc.clone());
    cc
}

/// Node configuration.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub listen_port: u16,
    pub seed_nodes: Vec<SocketAddr>,
    pub enable_discovery: bool,
    /// Data directory for persistent storage (chain.json, ledger.json).
    /// If None, data is kept in memory only.
    pub data_dir: Option<PathBuf>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            listen_port: DEFAULT_PORT,
            seed_nodes: Vec::new(),
            enable_discovery: true,
            data_dir: None,
        }
    }
}

/// Node status snapshot (for CLI display).
#[derive(Debug, Clone)]
pub struct NodeStatus {
    pub listen_port: u16,
    pub peer_count: usize,
    pub inbound_count: usize,
    pub outbound_count: usize,
    pub mempool_size: usize,
    pub account_count: usize,
    pub total_supply: u64,
    pub chain_height: u64,
    pub difficulty: u32,
    pub difficulty_target: u64,
    /// Hex-encoded hash of the chain tip block.
    pub best_hash: String,
}

/// The main TARON P2P node.
#[derive(Clone)]
pub struct TaronNode {
    config: NodeConfig,
    pub mempool: Arc<RwLock<Mempool>>,
    pub peers: Arc<Mutex<PeerManager>>,
    /// Tracks which peers have which tx hashes (for gossip dedup).
    peer_known_txs: Arc<RwLock<HashSet<(SocketAddr, String)>>>,
    /// Peers discovered via peer exchange, not yet connected.
    discovered_peers: Arc<RwLock<HashSet<SocketAddr>>>,
    /// The ledger containing all account states.
    pub ledger: Arc<RwLock<Ledger>>,
    /// The blockchain (ordered chain of validated blocks).
    pub blockchain: Arc<RwLock<Blockchain>>,
    /// Finality manager for transaction ACKs and double-spend prevention.
    pub finality: Arc<NodeFinalityManager>,
    /// Data directory for persistence.
    data_dir: Option<PathBuf>,
    /// True once initial sync is complete (IBD done or already at tip).
    /// Mining threads should wait for this before starting work.
    pub sync_ready: Arc<AtomicBool>,
    /// Limits concurrent block processing to 1 task at a time.
    /// Prevents 17 peers from all queuing for the write lock simultaneously,
    /// which starves RPC read operations.
    pub block_semaphore: Arc<Semaphore>,
    /// The peer currently driving IBD, with timestamp of last activity.
    /// None when synced. Only this peer can trigger reorgs or apply batched blocks.
    /// If no blocks received for 30s, the slot is automatically released.
    pub ibd_peer: Arc<Mutex<Option<(SocketAddr, Instant)>>>,
    /// Current chain height — updated atomically after each block, used for
    /// lock-free quick-reject of stale block submissions.
    pub chain_height: Arc<AtomicU64>,
    /// Cached difficulty — updated atomically after each block.
    pub cached_difficulty: Arc<AtomicU64>,
    /// Cached best hash — updated after each block for lock-free status reads.
    pub cached_best_hash: Arc<RwLock<String>>,
    /// Cached account count — updated atomically after each block for lock-free status reads.
    pub cached_account_count: Arc<AtomicU64>,
    /// Cached total supply — updated atomically after each block for lock-free status reads.
    pub cached_total_supply: Arc<AtomicU64>,
    /// True once a seed node has been seen during this session.
    /// Miners cannot claim the IBD slot until a seed has been seen or 30s elapsed.
    pub seed_seen: Arc<AtomicBool>,
    /// Node start time — used to enforce seed-wait delay at startup.
    pub start_time: Instant,
}

impl TaronNode {
    pub fn new(config: NodeConfig) -> Self {
        // Generate a node wallet for signing ACKs
        let node_wallet = Wallet::generate();
        let finality = NodeFinalityManager::new(1, node_wallet);

        // Load from disk if data_dir is set
        let (blockchain, ledger) = if let Some(ref dir) = config.data_dir {
            std::fs::create_dir_all(dir).ok();
            let chain_path = dir.join("chain.db");
            let ledger_path = dir.join("ledger.bin");
            let chain = Blockchain::load_or_create(&chain_path, TESTNET_DIFFICULTY);
            let ledger = Ledger::load_or_create_testnet(&ledger_path, &chain);
            info!("Loaded state from disk — height: {}, accounts: {}", chain.height(), ledger.account_count());
            (chain, ledger)
        } else {
            {
                use std::sync::atomic::{AtomicU64, Ordering};
                static COUNTER: AtomicU64 = AtomicU64::new(0);
                let n = COUNTER.fetch_add(1, Ordering::Relaxed);
                let tmp = std::env::temp_dir().join(format!("taron_node_{}_{}", std::process::id(), n));
                let chain = Blockchain::load_or_create(&tmp, TESTNET_DIFFICULTY);
                (chain, Ledger::new())
            }
        };

        let data_dir = config.data_dir.clone();
        let initial_height = blockchain.height();
        let initial_diff = blockchain.difficulty;
        let initial_hash = hex::encode(blockchain.tip().hash);
        let initial_accounts = ledger.account_count() as u64;
        let initial_supply = ledger.total_supply();
        Self {
            config,
            mempool: Arc::new(RwLock::new(Mempool::new())),
            peers: Arc::new(Mutex::new(PeerManager::new())),
            peer_known_txs: Arc::new(RwLock::new(HashSet::new())),
            discovered_peers: Arc::new(RwLock::new(HashSet::new())),
            ledger: Arc::new(RwLock::new(ledger)),
            blockchain: Arc::new(RwLock::new(blockchain)),
            finality: Arc::new(finality),
            data_dir,
            sync_ready: Arc::new(AtomicBool::new(false)),
            block_semaphore: Arc::new(Semaphore::new(1)),
            ibd_peer: Arc::new(Mutex::new(None)),
            chain_height: Arc::new(AtomicU64::new(initial_height)),
            cached_difficulty: Arc::new(AtomicU64::new(initial_diff)),
            cached_best_hash: Arc::new(RwLock::new(initial_hash)),
            cached_account_count: Arc::new(AtomicU64::new(initial_accounts)),
            cached_total_supply: Arc::new(AtomicU64::new(initial_supply)),
            seed_seen: Arc::new(AtomicBool::new(false)),
            start_time: Instant::now(),
        }
    }

    /// Create a new testnet node with genesis state.
    pub fn new_testnet(config: NodeConfig) -> Self {
        let ledger = Ledger::new_testnet();
        // Generate a node wallet for signing ACKs
        let node_wallet = Wallet::generate();
        let finality = NodeFinalityManager::new(1, node_wallet);
        let data_dir = config.data_dir.clone();
        let chain_path = data_dir.as_ref()
            .map(|d| d.join("chain.db"))
            .unwrap_or_else(|| std::env::temp_dir().join(format!("taron_genesis_{}", std::process::id())));

        let blockchain = Blockchain::load_or_create(&chain_path, TESTNET_DIFFICULTY);
        let initial_height = blockchain.height();
        let initial_diff = blockchain.difficulty;
        let initial_hash = hex::encode(blockchain.tip().hash);
        let initial_accounts = ledger.account_count() as u64;
        let initial_supply = ledger.total_supply();
        Self {
            config,
            mempool: Arc::new(RwLock::new(Mempool::new())),
            peers: Arc::new(Mutex::new(PeerManager::new())),
            peer_known_txs: Arc::new(RwLock::new(HashSet::new())),
            discovered_peers: Arc::new(RwLock::new(HashSet::new())),
            ledger: Arc::new(RwLock::new(ledger)),
            blockchain: Arc::new(RwLock::new(blockchain)),
            finality: Arc::new(finality),
            data_dir,
            sync_ready: Arc::new(AtomicBool::new(false)),
            block_semaphore: Arc::new(Semaphore::new(1)),
            ibd_peer: Arc::new(Mutex::new(None)),
            chain_height: Arc::new(AtomicU64::new(initial_height)),
            cached_difficulty: Arc::new(AtomicU64::new(initial_diff)),
            cached_best_hash: Arc::new(RwLock::new(initial_hash)),
            cached_account_count: Arc::new(AtomicU64::new(initial_accounts)),
            cached_total_supply: Arc::new(AtomicU64::new(initial_supply)),
            seed_seen: Arc::new(AtomicBool::new(false)),
            start_time: Instant::now(),
        }
    }

    /// Save blockchain and ledger to disk (if data_dir is set).
    pub async fn save_state(&self) {
        if let Some(ref dir) = self.data_dir {
            // Save chain and ledger with short-lived locks to avoid blocking RPC reads
            {
                let chain = self.blockchain.read().await;
                chain.save(&dir.join("chain.db"));
            }
            {
                let ledger = self.ledger.read().await;
                ledger.save(&dir.join("ledger.bin"));
            }
        }
    }

    /// Get current node status.
    /// Each lock is acquired and released individually to avoid holding
    /// multiple locks simultaneously, which causes cross-runtime deadlocks.
    pub async fn status(&self) -> NodeStatus {
        let (peer_count, inbound_count, outbound_count) = {
            let peers = self.peers.lock().await;
            (peers.count(), peers.inbound_count(), peers.outbound_count())
        };
        let mempool_size = self.mempool.read().await.len();
        // Use cached atomics — never blocks, always returns valid data
        let account_count = self.cached_account_count.load(Ordering::Relaxed) as usize;
        let total_supply = self.cached_total_supply.load(Ordering::Relaxed);
        // Use cached atomics for blockchain data — never blocks
        let chain_height = self.chain_height.load(Ordering::Relaxed);
        let difficulty_target = self.cached_difficulty.load(Ordering::Relaxed);
        let difficulty = taron_core::target_to_bits(difficulty_target) as u32;
        let best_hash = self.cached_best_hash.read().await.clone();
        NodeStatus {
            listen_port: self.config.listen_port,
            peer_count,
            inbound_count,
            outbound_count,
            mempool_size,
            account_count,
            total_supply,
            chain_height,
            difficulty,
            difficulty_target,
            best_hash,
        }
    }

    /// Start the node: listen for connections and connect to seed nodes.
    pub async fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.config.listen_port)).await?;
        info!("TARON node listening on port {}", self.config.listen_port);

        // Detect our own IPs to prevent self-connections
        let own_ips: HashSet<std::net::IpAddr> = {
            let mut ips = HashSet::new();
            ips.insert(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            ips.insert(std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST));
            // Detect external IPs by checking what the seed addresses resolve to
            if let Ok(output) = std::process::Command::new("hostname").arg("-I").output() {
                if let Ok(s) = std::str::from_utf8(&output.stdout) {
                    for part in s.split_whitespace() {
                        if let Ok(ip) = part.parse::<std::net::IpAddr>() {
                            ips.insert(ip);
                        }
                    }
                }
            }
            ips
        };
        let own_ips = Arc::new(own_ips);
        let listen_port = self.config.listen_port;

        // Register anchor IPs in PeerManager (all seed nodes are anchors).
        {
            let seed_addrs = resolve_seeds(&self.config.seed_nodes);
            let anchor_ips: std::collections::HashSet<std::net::IpAddr> =
                seed_addrs.iter().map(|a| a.ip()).collect();
            self.peers.lock().await.set_anchor_ips(anchor_ips);
        }

        // Connect to seed nodes — config seeds take priority; fall back to TESTNET_SEEDS.
        // Filter out self-connections (server connecting to itself via seed DNS).
        let seeds: Vec<_> = resolve_seeds(&self.config.seed_nodes)
            .into_iter()
            .filter(|a| !(own_ips.contains(&a.ip()) && a.port() == listen_port))
            .collect();
        for seed in seeds {
            let mempool = self.mempool.clone();
            let peers = self.peers.clone();
            let known = self.peer_known_txs.clone();
            let ledger = self.ledger.clone();
            let blockchain = self.blockchain.clone();
            let finality = self.finality.clone();
            let port = self.config.listen_port;
            let data_dir = self.data_dir.clone();
            let discovered = self.discovered_peers.clone();
            let sync_ready = self.sync_ready.clone();
            let block_sem = self.block_semaphore.clone();
            let ibd_peer = self.ibd_peer.clone();
            let chain_h = self.chain_height.clone();
            let cached_ac = self.cached_account_count.clone();
            let cached_ts = self.cached_total_supply.clone();
            let cached_diff = self.cached_difficulty.clone();
            let cached_bh = self.cached_best_hash.clone();
            tokio::spawn(async move {
                if let Err(e) = connect_to_peer(seed, port, mempool, peers, known, ledger, blockchain, finality, data_dir, discovered, sync_ready, block_sem, ibd_peer, chain_h, cached_ac, cached_ts, cached_diff, cached_bh).await {
                    warn!("Failed to connect to seed {}: {}", seed, e);
                }
            });
        }

        // Background reconnection task: maintain outbound peer count
        {
            let peers = self.peers.clone();
            let mempool = self.mempool.clone();
            let known = self.peer_known_txs.clone();
            let ledger = self.ledger.clone();
            let blockchain = self.blockchain.clone();
            let finality = self.finality.clone();
            let data_dir = self.data_dir.clone();
            let port = self.config.listen_port;
            let config_seeds = self.config.seed_nodes.clone();
            let discovered = self.discovered_peers.clone();
            let sync_ready = self.sync_ready.clone();
            let block_sem = self.block_semaphore.clone();
            let ibd_peer_rc = self.ibd_peer.clone();
            let chain_h = self.chain_height.clone();
            let cached_ac = self.cached_account_count.clone();
            let cached_ts = self.cached_total_supply.clone();
            let cached_diff = self.cached_difficulty.clone();
            let cached_bh = self.cached_best_hash.clone();
            let own_ips2 = own_ips.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

                    // Release stale IBD slot (peer disconnected without finishing)
                    {
                        let mut slot = ibd_peer_rc.lock().await;
                        if let Some((ibd_addr, last_activity)) = *slot {
                            if last_activity.elapsed() > std::time::Duration::from_secs(90) {
                                info!("[SYNC] IBD peer {} timed out (90s no activity) — releasing slot", ibd_addr);
                                *slot = None;
                            }
                        }
                    }

                    // Proactive IBD restart: if slot is free, not yet sync_ready, and we have
                    // connected peers → probe all peers for their chain height. Whichever peer
                    // responds with a height above ours will claim the IBD slot and restart sync.
                    // This handles the case where the IBD peer was banned mid-sync (oversized P2P
                    // attack) and no NewBlock arrived to trigger a natural slot takeover.
                    {
                        let slot_free = ibd_peer_rc.lock().await.is_none();
                        if slot_free && !sync_ready.load(Ordering::Relaxed) {
                            let pm = peers.lock().await;
                            let connected = pm.inbound_count() + pm.outbound_count();
                            if connected > 0 {
                                info!("[SYNC] IBD slot free but not sync_ready — probing {} peers for chain height", connected);
                                pm.broadcast(Message::GetChainHeight, None);
                            }
                        }
                    }

                    let (outbound, can_accept) = {
                        let pm = peers.lock().await;
                        (pm.outbound_count(), pm.can_accept(PeerDirection::Outbound))
                    };
                    if !can_accept { continue; }

                    // Collect candidates: discovered peers (seeds handled by anchor loop)
                    let mut candidates: Vec<SocketAddr> = Vec::new();
                    candidates.extend(resolve_seeds(&config_seeds));
                    // Drain discovered peers
                    {
                        let mut disc = discovered.write().await;
                        candidates.extend(disc.drain());
                    }

                    // Filter already-connected AND self-connections
                    let connected = peers.lock().await.all_addrs();
                    let connected_ips: HashSet<_> = connected.iter().map(|a| a.ip()).collect();
                    candidates.retain(|a| {
                        !connected_ips.contains(&a.ip())
                            && !(own_ips2.contains(&a.ip()) && a.port() == port)
                    });

                    if !candidates.is_empty() {
                        info!("[P2P] {} outbound peers — trying {} candidates", outbound, candidates.len());
                    }

                    for candidate in candidates {
                        if !peers.lock().await.can_accept(PeerDirection::Outbound) { break; }
                        let mempool = mempool.clone();
                        let peers = peers.clone();
                        let known = known.clone();
                        let ledger = ledger.clone();
                        let blockchain = blockchain.clone();
                        let finality = finality.clone();
                        let data_dir = data_dir.clone();
                        let discovered = discovered.clone();
                        let sync_ready = sync_ready.clone();
                        let block_sem = block_sem.clone();
                        let ibd_peer_rc = ibd_peer_rc.clone();
                        let chain_h = chain_h.clone();
                        let cached_ac = cached_ac.clone();
                        let cached_ts = cached_ts.clone();
                        let cached_diff = cached_diff.clone();
                        let cached_bh = cached_bh.clone();
                        tokio::spawn(async move {
                            if let Err(e) = connect_to_peer(candidate, port, mempool, peers, known, ledger, blockchain, finality, data_dir, discovered, sync_ready, block_sem, ibd_peer_rc, chain_h, cached_ac, cached_ts, cached_diff, cached_bh).await {
                                debug!("[P2P] Connect to {} failed: {}", candidate, e);
                            }
                        });
                    }
                }
            });
        }

        // Anchor maintenance loop — every 30s.
        // Ensures the 5 seed nodes are ALWAYS connected, even if tar11f888 (or any
        // attacker) fills all outbound slots (eclipse attack). If a seed is missing
        // and slots are full, the oldest non-anchor outbound peer is evicted.
        {
            let peers = self.peers.clone();
            let mempool = self.mempool.clone();
            let known = self.peer_known_txs.clone();
            let ledger = self.ledger.clone();
            let blockchain = self.blockchain.clone();
            let finality = self.finality.clone();
            let data_dir = self.data_dir.clone();
            let port = self.config.listen_port;
            let config_seeds = self.config.seed_nodes.clone();
            let discovered = self.discovered_peers.clone();
            let sync_ready = self.sync_ready.clone();
            let block_sem = self.block_semaphore.clone();
            let ibd_peer_rc = self.ibd_peer.clone();
            let chain_h = self.chain_height.clone();
            let cached_ac = self.cached_account_count.clone();
            let cached_ts = self.cached_total_supply.clone();
            let cached_diff = self.cached_difficulty.clone();
            let cached_bh = self.cached_best_hash.clone();
            let own_ips_anchor = own_ips.clone();
            tokio::spawn(async move {
                // Initial delay — let the node finish starting up
                tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
                loop {
                    let anchors = resolve_seeds(&config_seeds);
                    for anchor in &anchors {
                        // Skip self-connections
                        if own_ips_anchor.contains(&anchor.ip()) && anchor.port() == port { continue; }

                        let already_connected = peers.lock().await.is_connected(anchor);
                        if already_connected { continue; }

                        // Slot check — evict oldest non-anchor if needed
                        let need_evict = {
                            let pm = peers.lock().await;
                            !pm.can_accept(PeerDirection::Outbound)
                        };
                        if need_evict {
                            let evicted = peers.lock().await.evict_oldest_non_anchor_outbound();
                            if let Some((evicted_addr, tx)) = evicted {
                                warn!("[ANCHOR] Evicted {} to make room for seed {}", evicted_addr, anchor);
                                drop(tx); // closes the write side → peer disconnects
                                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                            } else {
                                // All outbound peers are anchors — nothing to evict
                                continue;
                            }
                        }

                        info!("[ANCHOR] Reconnecting to seed {}", anchor);
                        let mempool = mempool.clone();
                        let peers = peers.clone();
                        let known = known.clone();
                        let ledger = ledger.clone();
                        let blockchain = blockchain.clone();
                        let finality = finality.clone();
                        let data_dir = data_dir.clone();
                        let discovered = discovered.clone();
                        let sync_ready = sync_ready.clone();
                        let block_sem = block_sem.clone();
                        let ibd_peer_rc = ibd_peer_rc.clone();
                        let chain_h = chain_h.clone();
                        let cached_ac = cached_ac.clone();
                        let cached_ts = cached_ts.clone();
                        let cached_diff = cached_diff.clone();
                        let cached_bh = cached_bh.clone();
                        let anchor = *anchor;
                        tokio::spawn(async move {
                            if let Err(e) = connect_to_peer(anchor, port, mempool, peers, known, ledger, blockchain, finality, data_dir, discovered, sync_ready, block_sem, ibd_peer_rc, chain_h, cached_ac, cached_ts, cached_diff, cached_bh).await {
                                debug!("[ANCHOR] Connect to seed {} failed: {}", anchor, e);
                            }
                        });
                    }
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                }
            });
        }

        // Stale peer reaper + mempool cleanup: every 60 seconds
        {
            let peers = self.peers.clone();
            let mempool_evict = self.mempool.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                    mempool_evict.write().await.evict_expired();
                    let stale_addrs: Vec<SocketAddr> = {
                        let pm = peers.lock().await;
                        pm.all_peers()
                            .iter()
                            .filter(|p| p.last_seen.elapsed() > std::time::Duration::from_secs(600))
                            .map(|p| p.addr)
                            .collect()
                    };
                    if !stale_addrs.is_empty() {
                        info!("[P2P] Reaping {} stale peers (no activity for 5min)", stale_addrs.len());
                        let mut pm = peers.lock().await;
                        for addr in &stale_addrs {
                            // Drop the broadcast sender — this makes the writer task exit,
                            // and the reader will get an error on the next recv, triggering cleanup.
                            if let Some(peer) = pm.peers_mut().get_mut(addr) {
                                peer.take_broadcast_tx();
                            }
                            pm.remove_peer(addr);
                        }
                    }
                }
            });
        }

        // Start UDP discovery if enabled
        if self.config.enable_discovery {
            let port = self.config.listen_port;
            tokio::spawn(async move {
                if let Err(e) = crate::discovery::run_discovery_listener(port).await {
                    warn!("Discovery listener error: {}", e);
                }
            });
        }

        // Accept incoming connections — per-IP rate limiting (10/min → ban 1h)
        loop {
            let (stream, addr) = listener.accept().await?;

            let can_accept = {
                let mut peers = self.peers.lock().await;
                !peers.is_banned(addr.ip())
                    && peers.track_connection_attempt(addr.ip())
                    && peers.can_accept(PeerDirection::Inbound)
            };

            if !can_accept {
                debug!("Rejecting {}: banned, rate limited, or inbound limit reached", addr);
                drop(stream);
                continue;
            }

            // Register the peer — if add_peer returns false (per-IP limit, duplicate,
            // or race), drop the connection immediately to prevent FD leaks.
            let added = {
                let mut peers = self.peers.lock().await;
                peers.add_peer(addr, PeerDirection::Inbound)
            };
            if !added {
                debug!("Rejecting {}: per-IP limit or duplicate", addr);
                drop(stream);
                continue;
            }

            info!("Incoming connection from {}", addr);

            // Geo-lookup in background (non-blocking)
            {
                let peers_geo = self.peers.clone();
                let ip = addr.ip();
                let addr_geo = addr;
                tokio::spawn(async move {
                    let cc = geo_lookup(ip).await;
                    if !cc.is_empty() {
                        peers_geo.lock().await.set_country(&addr_geo, cc);
                    }
                });
            }

            let mempool = self.mempool.clone();
            let peers = self.peers.clone();
            let known = self.peer_known_txs.clone();
            let ledger = self.ledger.clone();
            let blockchain = self.blockchain.clone();
            let finality = self.finality.clone();
            let port = self.config.listen_port;
            let data_dir = self.data_dir.clone();
            let discovered = self.discovered_peers.clone();
            let sync_ready = self.sync_ready.clone();
            let block_sem = self.block_semaphore.clone();
            let ibd_peer_in = self.ibd_peer.clone();
            let chain_h = self.chain_height.clone();
            let cached_ac = self.cached_account_count.clone();
            let cached_ts = self.cached_total_supply.clone();
            let cached_diff = self.cached_difficulty.clone();
            let cached_bh = self.cached_best_hash.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_peer(stream, addr, port, mempool, peers.clone(), known, ledger, blockchain, finality, data_dir, discovered, sync_ready, block_sem, ibd_peer_in, chain_h, cached_ac, cached_ts, cached_diff, cached_bh).await {
                    debug!("Peer {} disconnected: {}", addr, e);
                }
                peers.lock().await.remove_peer(&addr);
            });
        }
    }

    /// Broadcast a transaction to all connected peers via persistent connections.
    pub async fn broadcast_tx(&self, tx: &Transaction) {
        let peers = self.peers.lock().await;
        peers.broadcast(Message::Tx { tx: tx.clone() }, None);
    }

    /// Broadcast a newly-mined block to all connected peers.
    /// Validate and apply a block submitted externally (e.g. from the pool server),
    /// then broadcast it to peers. Returns true if accepted.
    pub async fn submit_mined_block(&self, block: Block) -> bool {
        // Lock-free quick-reject: if block height doesn't match, skip entirely
        let current = self.chain_height.load(Ordering::Relaxed);
        if block.index != current + 1 {
            return false;
        }
        // No semaphore needed — the write lock provides sufficient serialization.
        // Using the semaphore here caused "busy" rejections when P2P held it.
        let mut chain = self.blockchain.write().await;
        let mut ledger = self.ledger.write().await;
        match chain.apply_block(&block, &mut ledger) {
            Ok(()) => {
                self.chain_height.store(block.index, Ordering::Release);
                self.cached_difficulty.store(chain.difficulty, Ordering::Release);
                self.cached_account_count.store(ledger.account_count() as u64, Ordering::Release);
                self.cached_total_supply.store(ledger.total_supply(), Ordering::Release);
                let hash = hex::encode(&block.hash);
                drop(chain);
                drop(ledger);
                *self.cached_best_hash.write().await = hash;
                self.save_state().await;
                // Purge included txs from mempool
                {
                    let mut mp = self.mempool.write().await;
                    for tx in &block.transactions {
                        mp.remove(&tx.hash_hex());
                    }
                }
                self.broadcast_block(&block).await;
                info!("Pool block #{} accepted and broadcast", block.index);
                true
            }
            Err(e) => {
                info!("Pool block #{} rejected: {:?}", block.index, e);
                false
            }
        }
    }

    pub async fn broadcast_block(&self, block: &Block) {
        let block_hash = hex::encode(&block.hash[..8]);

        // Update relay lock (pool-submitted blocks are always best)
        {
            let mut lock = RELAY_LOCK.lock().await;
            *lock = (block.index, block.hash, Instant::now());
        }

        let peers = self.peers.lock().await;
        info!(
            "[BROADCAST] Block #{} hash: {}… → {} peers",
            block.index,
            block_hash,
            peers.count()
        );

        peers.broadcast(Message::NewBlock(block.clone()), None);
    }
}

/// Properly shut down a peer writer task: wait up to 5s, then abort to free the FD.
/// Dropping a JoinHandle does NOT cancel the task — it detaches it and the socket leaks.
/// We must explicitly abort() to ensure write_half is dropped.
async fn shutdown_writer(handle: JoinHandle<()>, addr: SocketAddr) {
    // Get abort handle BEFORE consuming JoinHandle with timeout
    let abort = handle.abort_handle();
    match tokio::time::timeout(std::time::Duration::from_secs(5), handle).await {
        Ok(_) => {} // writer exited cleanly
        Err(_) => {
            debug!("[P2P] Writer to {} stuck — aborting task to free FD", addr);
            abort.abort(); // forcefully cancel — drops write_half, closes socket
        }
    }
}

/// Set TCP keepalive on a stream so the OS detects dead connections automatically.
/// This prevents half-open connections from leaking FDs forever.
fn set_tcp_keepalive(stream: &TcpStream) {
    let sock_ref = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(30))
        .with_interval(std::time::Duration::from_secs(10));
    if let Err(e) = sock_ref.set_tcp_keepalive(&keepalive) {
        debug!("[P2P] Failed to set TCP keepalive: {}", e);
    }
}

/// Connect to a peer as outbound.
async fn connect_to_peer(
    addr: SocketAddr,
    our_port: u16,
    mempool: Arc<RwLock<Mempool>>,
    peers: Arc<Mutex<PeerManager>>,
    known: Arc<RwLock<HashSet<(SocketAddr, String)>>>,
    ledger: Arc<RwLock<Ledger>>,
    blockchain: Arc<RwLock<Blockchain>>,
    finality: Arc<NodeFinalityManager>,
    data_dir: Option<PathBuf>,
    discovered_peers: Arc<RwLock<HashSet<SocketAddr>>>,
    sync_ready: Arc<AtomicBool>,
    block_semaphore: Arc<Semaphore>,
    ibd_peer: Arc<Mutex<Option<(SocketAddr, Instant)>>>,
    chain_height: Arc<AtomicU64>,
    cached_account_count: Arc<AtomicU64>,
    cached_total_supply: Arc<AtomicU64>,
    cached_difficulty: Arc<AtomicU64>,
    cached_best_hash: Arc<RwLock<String>>,
) -> io::Result<()> {
    {
        let mut pm = peers.lock().await;
        if !pm.add_peer(addr, PeerDirection::Outbound) {
            return Ok(());
        }
    }

    let stream = TcpStream::connect(addr).await?;

    // Self-connection detection: if we connected to our own listen port, bail
    if stream.local_addr().ok().map(|a| a.ip()) == stream.peer_addr().ok().map(|a| a.ip())
        && addr.port() == our_port
    {
        debug!("[P2P] Self-connection detected to {} — disconnecting", addr);
        peers.lock().await.remove_peer(&addr);
        return Ok(());
    }

    // Set TCP keepalive so OS detects dead connections (30s idle, 10s interval)
    set_tcp_keepalive(&stream);

    info!("Connected to peer {}", addr);

    // Geo-lookup in background (non-blocking)
    {
        let peers_geo = peers.clone();
        let ip = addr.ip();
        let addr_geo = addr;
        tokio::spawn(async move {
            let cc = geo_lookup(ip).await;
            if !cc.is_empty() {
                peers_geo.lock().await.set_country(&addr_geo, cc);
            }
        });
    }

    // Split stream into read/write halves for concurrent access
    let (mut read_half, mut write_half) = stream.into_split();

    // Create broadcast channel for this peer
    let (btx, mut brx) = mpsc::unbounded_channel::<Message>();
    {
        let mut pm = peers.lock().await;
        pm.set_broadcast_tx(&addr, btx.clone());
    }

    // Spawn writer task — sends messages from the channel to the stream
    let writer_addr = addr;
    let writer_handle = tokio::spawn(async move {
        while let Some(msg) = brx.recv().await {
            if protocol::send_message(&mut write_half, &msg).await.is_err() {
                debug!("[P2P] Writer to {} failed — closing", writer_addr);
                break;
            }
        }
    });

    let _ = btx.send(Message::Hello {
        version: 2,
        listen_port: our_port,
        user_agent: "taron/0.3.0".into(),
        genesis_hash: genesis_hash_hex(),
    });
    let _ = btx.send(Message::GetChainHeight);
    let _ = btx.send(Message::GetPeers);
    let _ = btx.send(Message::GetTxHashes);

    let result = handle_messages(&mut read_half, &btx, addr, our_port, mempool, peers.clone(), known, ledger, blockchain, finality, data_dir, discovered_peers, sync_ready, block_semaphore, ibd_peer.clone(), chain_height, cached_account_count, cached_total_supply, cached_difficulty, cached_best_hash).await;

    // Release IBD slot if this peer was driving IBD
    {
        let mut slot = ibd_peer.lock().await;
        if slot.map(|(a,_)| a) == Some(addr) {
            *slot = None;
        }
    }
    // Gracefully shut down the writer task: drop ALL senders so brx.recv() returns None,
    // then abort if stuck — prevents CLOSE-WAIT FD leak.
    peers.lock().await.remove_peer(&addr); // drops the cloned sender in PeerManager
    drop(btx); // drops the local sender — now all senders are gone
    shutdown_writer(writer_handle, addr).await;
    result
}

/// Handle an accepted peer connection.
async fn handle_peer(
    stream: TcpStream,
    addr: SocketAddr,
    our_port: u16,
    mempool: Arc<RwLock<Mempool>>,
    peers: Arc<Mutex<PeerManager>>,
    known: Arc<RwLock<HashSet<(SocketAddr, String)>>>,
    ledger: Arc<RwLock<Ledger>>,
    blockchain: Arc<RwLock<Blockchain>>,
    finality: Arc<NodeFinalityManager>,
    data_dir: Option<PathBuf>,
    discovered_peers: Arc<RwLock<HashSet<SocketAddr>>>,
    sync_ready: Arc<AtomicBool>,
    block_semaphore: Arc<Semaphore>,
    ibd_peer: Arc<Mutex<Option<(SocketAddr, Instant)>>>,
    chain_height: Arc<AtomicU64>,
    cached_account_count: Arc<AtomicU64>,
    cached_total_supply: Arc<AtomicU64>,
    cached_difficulty: Arc<AtomicU64>,
    cached_best_hash: Arc<RwLock<String>>,
) -> io::Result<()> {
    // Set TCP keepalive so OS detects dead connections (30s idle, 10s interval)
    set_tcp_keepalive(&stream);

    // Split stream into read/write halves
    let (mut read_half, mut write_half) = stream.into_split();

    // Create broadcast channel for this peer
    let (btx, mut brx) = mpsc::unbounded_channel::<Message>();
    {
        let mut pm = peers.lock().await;
        pm.set_broadcast_tx(&addr, btx.clone());
    }

    // Spawn writer task
    let writer_addr = addr;
    let writer_handle = tokio::spawn(async move {
        while let Some(msg) = brx.recv().await {
            if protocol::send_message(&mut write_half, &msg).await.is_err() {
                debug!("[P2P] Writer to {} failed — closing", writer_addr);
                break;
            }
        }
    });

    let _ = btx.send(Message::Hello {
        version: 2,
        listen_port: our_port,
        user_agent: "taron/0.3.0".into(),
        genesis_hash: genesis_hash_hex(),
    });
    let _ = btx.send(Message::GetChainHeight);

    let peers_cleanup = peers.clone();
    let result = handle_messages(&mut read_half, &btx, addr, our_port, mempool, peers, known, ledger, blockchain, finality, data_dir, discovered_peers, sync_ready, block_semaphore, ibd_peer.clone(), chain_height, cached_account_count, cached_total_supply, cached_difficulty, cached_best_hash).await;

    // Release IBD slot if this peer was driving IBD
    {
        let mut slot = ibd_peer.lock().await;
        if slot.map(|(a,_)| a) == Some(addr) {
            *slot = None;
        }
    }
    // Gracefully shut down the writer task, then abort if stuck — prevents FD leak.
    {
        let mut pm = peers_cleanup.lock().await;
        if let Some(peer) = pm.peers_mut().get_mut(&addr) {
            peer.take_broadcast_tx(); // drop the cloned sender
        }
    }
    drop(btx); // drop the local sender — all senders gone, writer exits
    shutdown_writer(writer_handle, addr).await;
    result
}

/// Send a response message to a peer via its channel. Returns Err if channel closed.
fn send_to_peer(tx: &mpsc::UnboundedSender<Message>, msg: Message) -> io::Result<()> {
    tx.send(msg).map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "peer channel closed"))
}

/// Message processing loop for a peer connection.
async fn handle_messages(
    reader: &mut OwnedReadHalf,
    out_tx: &mpsc::UnboundedSender<Message>,
    addr: SocketAddr,
    _our_port: u16,
    mempool: Arc<RwLock<Mempool>>,
    peers: Arc<Mutex<PeerManager>>,
    known: Arc<RwLock<HashSet<(SocketAddr, String)>>>,
    ledger: Arc<RwLock<Ledger>>,
    blockchain: Arc<RwLock<Blockchain>>,
    finality: Arc<NodeFinalityManager>,
    data_dir: Option<PathBuf>,
    discovered_peers: Arc<RwLock<HashSet<SocketAddr>>>,
    sync_ready: Arc<AtomicBool>,
    block_semaphore: Arc<Semaphore>,
    ibd_peer: Arc<Mutex<Option<(SocketAddr, Instant)>>>,
    chain_height_atomic: Arc<AtomicU64>,
    cached_account_count: Arc<AtomicU64>,
    cached_total_supply: Arc<AtomicU64>,
    cached_difficulty: Arc<AtomicU64>,
    cached_best_hash: Arc<RwLock<String>>,
) -> io::Result<()> {
    // Track the peer's reported chain height so IBD can continue chunk by chunk.
    let mut peer_height: Option<u64> = None;
    let mut last_recv = Instant::now();
    // Rate-limit BlockLocator responses: ignore repeated GetBlockLocator from same peer
    // within 10 seconds. Prevents fast-miner flood from looping all peers in IBD restart.
    let mut last_locator_sent: Option<Instant> = None;
    let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(45));
    ping_interval.tick().await; // first tick is immediate, skip it

    loop {
        // Use select! to interleave message reading with periodic pings.
        // This keeps connections alive and detects dead peers faster.
        let msg = tokio::select! {
            result = protocol::recv_message(reader) => {
                match result {
                    Ok(m) => {
                        last_recv = Instant::now();
                        m
                    }
                    Err(e) if e.kind() == io::ErrorKind::ConnectionReset => {
                        // Stream corrupted (message too large) — ban immediately for 1 hour.
                        // Attackers sending 4GB/2GB messages from different IPs never reach
                        // the penalty threshold — immediate ban on first offence.
                        tracing::warn!("[P2P] {} sent oversized/corrupted message — banning 1h: {}", addr, e);
                        {
                            let mut pm = peers.lock().await;
                            pm.ban_ip(addr.ip());
                        }
                        return Err(e);
                    }
                    Err(e) => return Err(e),
                }
            }
            _ = ping_interval.tick() => {
                // Check if peer is idle too long (no messages received)
                if last_recv.elapsed() > std::time::Duration::from_secs(120) {
                    debug!("[P2P] Peer {} idle for 120s — disconnecting", addr);
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "peer idle timeout"));
                }
                // Send keepalive ping
                let nonce = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH).unwrap_or_default()
                    .as_nanos() as u64;
                if send_to_peer(out_tx, Message::Ping { nonce }).is_err() {
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "ping send failed"));
                }
                continue;
            }
        };

        match msg {
            Message::Hello { version, user_agent, genesis_hash, .. } => {
                // Reject peers on a different chain (fork at genesis level)
                if !genesis_hash.is_empty() && genesis_hash != genesis_hash_hex() {
                    warn!("[P2P] {} sent wrong genesis {} — banning 1h", addr, &genesis_hash[..16]);
                    peers.lock().await.ban_ip(addr.ip());
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "genesis mismatch"));
                }
                peers.lock().await.update_hello(&addr, version, user_agent);
            }

            Message::Ping { nonce } => {
                send_to_peer(out_tx, Message::Pong { nonce })?;
                peers.lock().await.touch(&addr);
            }

            Message::Pong { .. } => {
                peers.lock().await.touch(&addr);
            }

            Message::GetPeers => {
                let addrs = peers.lock().await.all_addrs();
                send_to_peer(out_tx, Message::Peers { addrs })?;
            }

            Message::Peers { addrs } => {
                let current_addrs = peers.lock().await.all_addrs();
                let current_ips: HashSet<_> = current_addrs.iter().map(|a| a.ip()).collect();
                let new_peers: Vec<_> = addrs.into_iter()
                    .filter(|a| !current_ips.contains(&a.ip()))
                    .collect();
                if !new_peers.is_empty() {
                    info!("[P2P] Discovered {} new peers from {}", new_peers.len(), addr);
                    let mut disc = discovered_peers.write().await;
                    for p in new_peers {
                        disc.insert(p);
                    }
                }
            }

            Message::Tx { tx } => {
                let tx_hash = tx.hash();
                let tx_hash_hex = tx.hash_hex();

                // Check for double-spend via sequence number
                if let Some(original) = finality.check_double_spend(&tx).await {
                    warn!(
                        "[DOUBLE-SPEND] tx {} rejected — conflicts with {}",
                        &tx_hash_hex[..16], hex::encode(&original[..8])
                    );
                    finality.reject(tx_hash, "double-spend".into()).await;
                    continue;
                }

                // Validate against current ledger state
                let ledger_validation = {
                    let ledger_state = ledger.read().await;
                    let mut ledger_copy = ledger_state.clone();
                    ledger_copy.apply_tx(&tx)
                };

                match ledger_validation {
                    Ok(()) => {
                        let mut pool = mempool.write().await;
                        match pool.insert(tx.clone()) {
                            Ok(true) => {
                                info!("[TX] {} from peer {} — validated ✓", &tx_hash_hex[..16], addr);

                                // Record for double-spend prevention
                                finality.record_seen(&tx).await;

                                {
                                    let mut ledger_state = ledger.write().await;
                                    if let Err(e) = ledger_state.apply_tx(&tx) {
                                        warn!("Ledger application failed for {}: {}", tx_hash_hex, e);
                                        drop(ledger_state);
                                        return Ok(());
                                    }
                                }

                                known.write().await.insert((addr, tx_hash_hex.clone()));

                                // Send ACK back to originator
                                let ack = finality.create_ack(tx_hash);
                                send_to_peer(out_tx, Message::TxAck(ack.clone()))?;
                                debug!("[ACK] Sent ACK for {} to {}", &tx_hash_hex[..16], addr);

                                // Relay tx and ACK to other peers via persistent connections
                                drop(pool);
                                {
                                    let pm = peers.lock().await;
                                    pm.broadcast(Message::Tx { tx: tx.clone() }, Some(&addr));
                                    pm.broadcast(Message::TxAck(ack), Some(&addr));
                                }
                            }
                            Ok(false) => {
                                debug!("Duplicate tx {} from {}", tx_hash_hex, addr);
                            }
                            Err(e) => {
                                warn!("Mempool validation failed for tx {} from {}: {}", tx_hash_hex, addr, e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Ledger validation failed for tx {} from {}: {}", tx_hash_hex, addr, e);
                    }
                }
            }

            Message::GetTxHashes => {
                let hashes = mempool.read().await.tx_hashes();
                send_to_peer(out_tx, Message::TxHashes { hashes })?;
            }

            Message::TxHashes { hashes } => {
                let missing: Vec<String> = {
                    let pool = mempool.read().await;
                    hashes.into_iter().filter(|h| !pool.contains(h)).collect()
                };
                if !missing.is_empty() {
                    send_to_peer(out_tx, Message::GetTxs { hashes: missing })?;
                }
            }

            Message::GetTxs { hashes } => {
                let pool = mempool.read().await;
                let txs = pool.get_txs(&hashes);
                for tx in txs {
                    send_to_peer(out_tx, Message::Tx { tx })?;
                }
            }

            // ── Block propagation ────────────────────────────────────────────

            Message::NewBlock(block) => {
                // Always process NewBlock — even during IBD.
                // IBD handles batch sync (Blocks message), NewBlock is real-time.
                // Ignoring NewBlock during IBD caused seeds to fall behind each other.
                let block_index = block.index;
                let block_hash_hex = hex::encode(&block.hash[..8]);

                // Lock-free pre-check using atomic height
                let our_height = chain_height_atomic.load(Ordering::Relaxed);

                // Block is ahead of our tip — request sync, don't process yet
                if block_index > our_height + 1 {
                    // Only request blocks if no other peer is already syncing
                    let (can_sync, already_ibd) = {
                        let mut slot = ibd_peer.lock().await;
                        match *slot {
                            None => { *slot = Some((addr, Instant::now())); (true, false) }
                            Some((a, _)) if a == addr => (true, true), // IBD already in progress with this peer
                            _ => (false, false),
                        }
                    };
                    if !can_sync { continue; }
                    // If IBD is already running with this peer, don't restart the locator negotiation.
                    // The Blocks handler will continue delivering batches; a new GetBlockLocator
                    // would interrupt mid-stream and restart from scratch.
                    // BUT: always update peer_height so IBD doesn't declare "complete" prematurely.
                    // Without this, the IBD finishes at the height seen when it started, even though
                    // the peer has mined hundreds more blocks during the download.
                    if already_ibd {
                        if block_index > peer_height.unwrap_or(0) {
                            peer_height = Some(block_index);
                            debug!("[SYNC] IBD in progress — updated peer_height to #{}", block_index);
                        }
                        continue;
                    }
                    peer_height = Some(block_index);
                    info!(
                        "[SYNC] NewBlock #{} from {} is ahead of our height {} — negotiating common ancestor",
                        block_index, addr, our_height
                    );
                    send_to_peer(out_tx, Message::GetBlockLocator)?;
                    continue;
                }

                // Block is behind our tip — skip stale blocks silently
                if block_index < our_height {
                    continue;
                }

                // Use semaphore to serialize block processing — only 1 task at a time
                // can acquire the write lock. Others skip immediately (they'll get the
                // block from the next sync or broadcast anyway).
                let permit = block_semaphore.try_acquire();
                if permit.is_err() {
                    // Another peer task is already processing a block — skip this one
                    debug!("[BLOCK] Skipping #{} from {} — another block is being processed", block_index, addr);
                    continue;
                }
                let _permit = permit.unwrap();

                // Re-check with read lock after acquiring semaphore (state may have changed)
                let (our_height, tip_hash) = {
                    let chain = blockchain.read().await;
                    (chain.height(), chain.tip().hash)
                };

                let result = if block.index == our_height + 1 && block.prev_hash == tip_hash {
                    let mut chain = blockchain.write().await;
                    let mut ledger_state = ledger.write().await;
                    chain.apply_block(&block, &mut *ledger_state)
                } else if block.index <= our_height {
                    Err(taron_core::TaronError::InvalidBlock)
                } else {
                    let mut chain = blockchain.write().await;
                    let mut ledger_state = ledger.write().await;
                    chain.apply_block(&block, &mut *ledger_state)
                };

                match result {
                    Ok(()) => {
                        // Update all cached atomics immediately so other tasks see fresh data
                        chain_height_atomic.store(block_index, Ordering::Release);
                        {
                            let chain = blockchain.read().await;
                            cached_difficulty.store(chain.difficulty, Ordering::Release);
                        }
                        *cached_best_hash.write().await = hex::encode(&block.hash);
                        {
                            let ledger_state = ledger.read().await;
                            cached_account_count.store(ledger_state.account_count() as u64, Ordering::Release);
                            cached_total_supply.store(ledger_state.total_supply(), Ordering::Release);
                        }

                        info!(
                            "[BLOCK] #{} accepted from {} | hash: {}… | reward: {:.2} TAR",
                            block_index,
                            addr,
                            block_hash_hex,
                            block.reward as f64 / 1_000_000.0
                        );

                        // Purge included txs from mempool
                        {
                            let mut mp = mempool.write().await;
                            for tx in &block.transactions {
                                mp.remove(&tx.hash_hex());
                            }
                        }

                        // Persist to disk (short-lived locks)
                        if let Some(ref dir) = data_dir {
                            {
                                let chain = blockchain.read().await;
                                chain.save(&dir.join("chain.db"));
                            }
                            {
                                let ledger_state = ledger.read().await;
                                ledger_state.save(&dir.join("ledger.bin"));
                            }
                        }

                        // Seed Relay Lock: only relay this block if no better block was
                        // broadcast at this height within the last 2 seconds.
                        {
                            let mut lock = RELAY_LOCK.lock().await;
                            let dominated = block_index == lock.0
                                && block.hash >= lock.1
                                && lock.2.elapsed() < RELAY_LOCK_DURATION;
                            if block_index > lock.0 || !dominated {
                                // New height or better hash — update lock and relay
                                *lock = (block_index, block.hash, Instant::now());
                                drop(lock);
                                let pm = peers.lock().await;
                                pm.broadcast(Message::NewBlock(block.clone()), Some(&addr));
                            } else {
                                // Lock active — suppress relay and send our better block back
                                drop(lock);
                                let tip = blockchain.read().await.tip();
                                if tip.index == block_index {
                                    info!(
                                        "[RELAY-LOCK] Blocked relay of #{} ({}…) — lock active for 2s",
                                        block_index, block_hash_hex
                                    );
                                    send_to_peer(out_tx, Message::NewBlock(tip))?;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let our_h = blockchain.read().await.height();
                        if block_index > our_h + 1 {
                            let can_sync = {
                                let mut slot = ibd_peer.lock().await;
                                match *slot {
                                    None => { *slot = Some((addr, Instant::now())); true }
                                    Some((a, _)) if a == addr => true,
                                    _ => false,
                                }
                            };
                            if can_sync {
                                peer_height = Some(block_index);
                                info!(
                                    "[SYNC] NewBlock #{} from {} is ahead of our height {} — requesting blocks {}..{}",
                                    block_index, addr, our_h, our_h + 1, block_index
                                );
                                let from = our_h + 1;
                                let to = (from + crate::sync::IBD_CHUNK_SIZE - 1).min(block_index);
                                send_to_peer(out_tx, Message::GetBlocks { from, to })?;
                            }
                        } else if block_index == our_h + 1 {
                            // Next block but doesn't link to our tip — peer is on a fork
                            // Don't request reorg blocks if IBD is running with another peer
                            let can_reorg = {
                                let slot = ibd_peer.lock().await;
                                slot.is_none() || slot.map(|(a,_)| a) == Some(addr)
                            };
                            if can_reorg {
                                let fork_start = if our_h > 10 { our_h - 10 } else { 1 };
                                info!(
                                    "[FORK] NewBlock #{} from {} doesn't chain to our tip — requesting blocks {}..{} for reorg comparison",
                                    block_index, addr, fork_start, block_index
                                );
                                send_to_peer(out_tx, Message::GetBlocks { from: fork_start, to: block_index })?;
                            }
                        } else if block_index == our_h {
                            // Competing block at same height — check if it has a better (lower) hash
                            let current_tip = blockchain.read().await.tip();
                            if block.hash < current_tip.hash {
                                // Better hash → reorg tip
                                let mut chain = blockchain.write().await;
                                let mut ledger_state = ledger.write().await;

                                // Verify the competing block links to our parent
                                let parent = chain.block_at(our_h - 1);
                                if let Some(parent) = parent {
                                    if block.prev_hash == parent.hash
                                        && block.is_valid(&parent, chain.difficulty)
                                        && block.reward == taron_core::TESTNET_REWARD
                                    {
                                        // Revert current tip
                                        if let Ok(reverted) = chain.revert_tip(&mut *ledger_state) {
                                            // Apply the better competing block
                                            if chain.apply_block(&block, &mut *ledger_state).is_ok() {
                                                chain_height_atomic.store(block_index, Ordering::Release);
                                                cached_difficulty.store(chain.difficulty, Ordering::Release);
                                                cached_account_count.store(ledger_state.account_count() as u64, Ordering::Release);
                                                cached_total_supply.store(ledger_state.total_supply(), Ordering::Release);
                                                info!(
                                                    "[REORG] Tip #{} replaced: {}… → {}… (lower hash wins)",
                                                    block_index,
                                                    hex::encode(&reverted.hash[..8]),
                                                    hex::encode(&block.hash[..8])
                                                );
                                                drop(chain);
                                                drop(ledger_state);
                                                *cached_best_hash.write().await = hex::encode(&block.hash);

                                                // Persist
                                                if let Some(ref dir) = data_dir {
                                                    let chain = blockchain.read().await;
                                                    let ledger_state = ledger.read().await;
                                                    chain.save(&dir.join("chain.db"));
                                                    ledger_state.save(&dir.join("ledger.bin"));
                                                }

                                                // Re-add reverted block's txs to mempool
                                                {
                                                    let mut mp = mempool.write().await;
                                                    for tx in &reverted.transactions {
                                                        mp.remove(&tx.hash_hex());
                                                    }
                                                    for tx in &block.transactions {
                                                        mp.remove(&tx.hash_hex());
                                                    }
                                                }

                                                // Relay the better block — update relay lock
                                                {
                                                    let mut lock = RELAY_LOCK.lock().await;
                                                    *lock = (block_index, block.hash, Instant::now());
                                                    drop(lock);
                                                    let pm = peers.lock().await;
                                                    pm.broadcast(Message::NewBlock(block.clone()), Some(&addr));
                                                }
                                            } else {
                                                warn!("[REORG] Failed to apply competing block #{} — reverting", block_index);
                                                // Re-apply the original tip
                                                let _ = chain.apply_block(&reverted, &mut *ledger_state);
                                            }
                                        }
                                    } else {
                                        debug!("[BLOCK] Competing #{} from {} doesn't link to parent — ignored", block_index, addr);
                                    }
                                }
                            } else {
                                debug!("[BLOCK] Competing #{} from {} has worse hash — ignored", block_index, addr);
                            }
                        } else {
                            warn!(
                                "[BLOCK] #{} rejected from {} ({})",
                                block_index, addr, e
                            );
                            // Don't penalize for rejected NewBlock — the peer may be on
                            // a stale fork (common during IBD or after restart). Penalizing
                            // causes immediate ban (3 blocks × 40 = -120 > -100 threshold)
                            // which disconnects the peer before IBD can start.
                        }
                    }
                }
            }

            Message::GetChainHeight => {
                let height = blockchain.read().await.height();
                send_to_peer(out_tx, Message::ChainHeight(height))?;
            }

            Message::ChainHeight(peer_h) => {
                peer_height = Some(peer_h);
                let our_h = blockchain.read().await.height();
                if peer_h > our_h {
                    let claimed = {
                        let mut slot = ibd_peer.lock().await;
                        if slot.is_none() {
                            *slot = Some((addr, Instant::now()));
                            true
                        } else if slot.map(|(a,_)| a) == Some(addr) {
                            true
                        } else {
                            let (current_addr, last_activity) = slot.unwrap();
                            let still_connected = peers.lock().await.is_connected(&current_addr);
                            if !still_connected {
                                info!("[SYNC] IBD peer {} disconnected, releasing slot for {}", current_addr, addr);
                                *slot = Some((addr, Instant::now()));
                                true
                            } else if last_activity.elapsed() > std::time::Duration::from_secs(90) {
                                info!("[SYNC] IBD peer {} timed out, releasing slot for {}", current_addr, addr);
                                *slot = Some((addr, Instant::now()));
                                true
                            } else {
                                false
                            }
                        }
                    };
                    if !claimed {
                        debug!("[SYNC] Peer {} wants IBD but another peer is syncing — skipping", addr);
                        continue;
                    }
                    info!(
                        "[SYNC] Peer {} reports height {} — we are at {} — negotiating common ancestor",
                        addr, peer_h, our_h
                    );
                    send_to_peer(out_tx, Message::GetBlockLocator)?;
                } else {
                    info!("[SYNC] Peer {} height {} — already in sync (height {})", addr, peer_h, our_h);
                    if !sync_ready.load(Ordering::Relaxed) {
                        sync_ready.store(true, Ordering::Release);
                        info!("[SYNC] Sync ready — mining can start");
                    }
                }
            }

            Message::GetBlockLocator => {
                // Rate-limit: ignore if we already responded within the last 10 seconds.
                // This breaks the IBD-restart loop caused by fast miners on peers with old code.
                if last_locator_sent.is_some() {
                    // IBD in progress for this peer — ignore until last batch resets the throttle
                    info!("[SYNC] GetBlockLocator from {} throttled — IBD in progress", addr);
                    continue;
                }
                let locator = blockchain.read().await.generate_block_locator();
                send_to_peer(out_tx, Message::BlockLocator { hashes: locator })?;
                last_locator_sent = Some(Instant::now());
            }

            Message::BlockLocator { hashes } => {
                // Only process if we hold the IBD slot for this peer.
                {
                    let slot = ibd_peer.lock().await;
                    if slot.map(|(a, _)| a) != Some(addr) {
                        debug!("[SYNC] Ignoring BlockLocator from {} — not our IBD peer", addr);
                        continue;
                    }
                }
                let peer_h = match peer_height {
                    Some(h) => h,
                    None => {
                        warn!("[SYNC] BlockLocator from {} but peer_height unknown — skipping", addr);
                        continue;
                    }
                };
                let common = blockchain.read().await.find_common_ancestor_from_locator(&hashes);
                match common {
                    None => {
                        warn!("[SYNC] No common ancestor with {} — different genesis or deep fork, disconnecting", addr);
                        *ibd_peer.lock().await = None;
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "no common ancestor: incompatible chain",
                        ));
                    }
                    Some(ancestor_h) => {
                        let our_h = blockchain.read().await.height();
                        // Limit deep reorgs — but ONLY when roughly synced.
                        // During IBD (peer significantly ahead), allow any reorg depth
                        // so the node can reach the canonical chain from a dead branch.
                        let reorg_depth = our_h.saturating_sub(ancestor_h);
                        let peer_far_ahead = peer_height.map(|ph| ph > our_h + 20).unwrap_or(false);
                        if reorg_depth > 100 && !peer_far_ahead {
                            warn!("[SYNC] Reorg depth {} > 100 from {} — rejecting (synced mode)", reorg_depth, addr);
                            *ibd_peer.lock().await = None;
                            continue;
                        }
                        if reorg_depth > 100 {
                            info!("[SYNC] Deep reorg {} blocks from {} — allowed (IBD mode, peer ahead)", reorg_depth, addr);
                        }
                        let from = ancestor_h + 1;
                        let to = (from + crate::sync::IBD_CHUNK_SIZE - 1).min(peer_h);
                        if ancestor_h < our_h {
                            info!("[SYNC] Fork detected with {} at height {} — peer at {} vs our {} — reorging from {}", addr, ancestor_h, peer_h, our_h, from);
                        } else {
                            info!("[SYNC] Common ancestor at {} with {} — downloading blocks {}..{}", ancestor_h, addr, from, to);
                        }
                        send_to_peer(out_tx, Message::GetBlocks { from, to })?;
                    }
                }
            }

            Message::GetBlocks { from, to } => {
                // Limit to 500 blocks per request to prevent abuse
                const MAX_GETBLOCKS_RANGE: u64 = 500;
                let to = to.min(from + MAX_GETBLOCKS_RANGE);
                let chain = blockchain.read().await;
                let our_tip = chain.height();
                let blocks = chain.blocks_range(from, to);
                let is_last_batch = to >= our_tip;
                send_to_peer(out_tx, Message::Blocks(blocks))?;
                // IBD complete — reset GetBlockLocator throttle so peer can resync freely next time
                if is_last_batch {
                    last_locator_sent = None;
                    info!("[SYNC] Last batch sent to {} (tip reached) — locator throttle reset", addr);
                }
            }

            Message::Blocks(blocks) => {
                // Only one peer at a time can drive block sync.
                // If no IBD peer set, claim it. If another peer has it, skip.
                {
                    let mut slot = ibd_peer.lock().await;
                    match *slot {
                        Some((ibd_addr, _)) if ibd_addr != addr => {
                            debug!("[SYNC] Ignoring Blocks from {} — IBD peer is {}", addr, ibd_addr);
                            continue;
                        }
                        None if !blocks.is_empty() => {
                            *slot = Some((addr, Instant::now()));
                        }
                        _ => {}
                    }
                }
                // Batch block sync — apply each in order, with deep reorg support
                if blocks.is_empty() {
                    let h = {
                        let mut chain = blockchain.write().await;
                        chain.recalibrate_abc();
                        let h = chain.height();
                        cached_difficulty.store(chain.difficulty, Ordering::Release);
                        h
                    };
                    // Only declare sync complete if we're actually close to the peer's height.
                    // An empty batch while peer is still far ahead means the peer has no more
                    // blocks for us right now — not that we're fully synced.
                    let peer_h_val = peer_height.unwrap_or(0);
                    let close_enough = peer_h_val == 0 || h + 10 >= peer_h_val;
                    info!("[SYNC] Sync complete — height: {} peer: {} close_enough: {}", h, peer_h_val, close_enough);
                    *ibd_peer.lock().await = None;
                    if close_enough && !sync_ready.load(Ordering::Relaxed) {
                        sync_ready.store(true, Ordering::Release);
                        info!("[SYNC] Sync ready — mining can start");
                    } else if !close_enough {
                        info!("[SYNC] Still {} blocks behind peer — not marking sync ready", peer_h_val.saturating_sub(h));
                    }
                } else {
                    let mut applied = 0usize;
                    let mut last_height = 0u64;
                    let mut fork_handled = false;

                    for block in &blocks {
                        let result = {
                            let mut chain = blockchain.write().await;
                            let mut ledger_state = ledger.write().await;
                            let r = chain.apply_block_ibd(block, &mut *ledger_state);
                            if r.is_ok() {
                                cached_account_count.store(ledger_state.account_count() as u64, Ordering::Release);
                                cached_total_supply.store(ledger_state.total_supply(), Ordering::Release);
                                cached_difficulty.store(chain.difficulty, Ordering::Release);
                            }
                            r
                        };
                        match result {
                            Ok(()) => {
                                applied += 1;
                                last_height = block.index;
                                chain_height_atomic.store(block.index, Ordering::Release);
                                *cached_best_hash.write().await = hex::encode(&block.hash);
                                let hash_prefix = hex::encode(&block.hash[..5]);
                                info!("[SYNC] Applied block #{} | hash: {}…", block.index, hash_prefix);
                            }
                            Err(_) if !fork_handled => {
                                // Block doesn't apply — try deep reorg if incoming chain is longer
                                let mut chain = blockchain.write().await;
                                let incoming_max = blocks.iter().map(|b| b.index).max().unwrap_or(0);
                                if incoming_max <= chain.height() {
                                    warn!("[SYNC] Incoming chain tip {} ≤ our height {} — no reorg", incoming_max, chain.height());
                                    break;
                                }
                                let fork_point = chain.find_fork_point(&blocks);
                                if let Some(fp) = fork_point {
                                    // AFD: cap reorg depth.
                                    // Fresh nodes (height < FINALITY_DEPTH): reorg freely during IBD.
                                    // Synced nodes: cap at SYNCED_MAX_REORG to block selfish mining
                                    // (private chain release attacks).
                                    const SYNCED_MAX_REORG: u64 = 10;
                                    let reorg_depth = chain.height() - fp;
                                    let max_reorg = if chain.height() < FINALITY_DEPTH {
                                        chain.height() // fresh node — no finalized blocks yet
                                    } else {
                                        SYNCED_MAX_REORG // synced — reject deep reorgs
                                    };
                                    if reorg_depth <= max_reorg {
                                        info!(
                                            "[REORG] Fork detected at height {} (our tip: {}) — reverting {} blocks",
                                            fp, chain.height(), reorg_depth
                                        );
                                        let mut ledger_state = ledger.write().await;
                                        match chain.revert_to_height(fp, &mut *ledger_state) {
                                            Ok(reverted) => {
                                                info!("[REORG] Reverted {} blocks to height {}", reverted.len(), fp);
                                                // Now apply all incoming blocks from the fork point
                                                for new_block in &blocks {
                                                    if new_block.index <= fp { continue; }
                                                    match chain.apply_block_ibd(new_block, &mut *ledger_state) {
                                                        Ok(()) => {
                                                            applied += 1;
                                                            last_height = new_block.index;
                                                            chain_height_atomic.store(new_block.index, Ordering::Release);
                                                            cached_difficulty.store(chain.difficulty, Ordering::Release);
                                                            cached_account_count.store(ledger_state.account_count() as u64, Ordering::Release);
                                                            cached_total_supply.store(ledger_state.total_supply(), Ordering::Release);
                                                            info!("[REORG] Applied block #{}", new_block.index);
                                                        }
                                                        Err(e) => {
                                                            warn!("[REORG] Block #{} failed after reorg: {}", new_block.index, e);
                                                            break;
                                                        }
                                                    }
                                                }
                                                fork_handled = true;
                                            }
                                            Err(e) => {
                                                warn!("[REORG] Revert failed: {} — keeping current chain", e);
                                            }
                                        }
                                    } else {
                                        warn!("[SYNC] Fork too deep (fork_point: {}, tip: {}) — skipping", fp, chain.height());
                                    }
                                } else {
                                    // No common ancestor in this batch.
                                    // Revert to genesis ONLY if at early IBD (height < 200).
                                    // Never revert an established chain — just reject the batch
                                    // and let the next IBD chunk resolve it.
                                    if incoming_max > chain.height() + 10 && chain.height() < 200 {
                                        warn!(
                                            "[SYNC] No common ancestor — peer chain {} vs our height {}. Reverting to genesis for full resync.",
                                            incoming_max, chain.height()
                                        );
                                        let mut ledger_state = ledger.write().await;
                                        match chain.revert_to_height(0, &mut *ledger_state) {
                                            Ok(reverted) => {
                                                // Reset difficulty to initial after full revert to genesis
                                                chain.difficulty = taron_core::TESTNET_TARGET;
                                                chain_height_atomic.store(0, Ordering::Release);
                                                cached_difficulty.store(chain.difficulty, Ordering::Release);
                                                cached_account_count.store(ledger_state.account_count() as u64, Ordering::Release);
                                                cached_total_supply.store(ledger_state.total_supply(), Ordering::Release);
                                                *cached_best_hash.write().await = hex::encode(&chain.tip().hash);
                                                info!("[SYNC] Reverted {} blocks to genesis — resyncing from block 1", reverted.len());
                                                fork_handled = true;
                                                last_height = 0;
                                                applied = 1; // trigger IBD continuation from height 0
                                            }
                                            Err(e) => {
                                                warn!("[SYNC] Revert to genesis failed: {}", e);
                                            }
                                        }
                                    } else {
                                        warn!("[SYNC] Block #{} rejected: no common ancestor found", block.index);
                                    }
                                }
                                if fork_handled { break; } else { break; }
                            }
                            Err(e) => {
                                warn!("[SYNC] Block #{} rejected: {}", block.index, e);
                                break;
                            }
                        }
                    }
                    // Always refresh IBD slot timestamp when we receive blocks (even if none applied)
                    {
                        let mut slot = ibd_peer.lock().await;
                        if slot.map(|(a,_)| a) == Some(addr) {
                            *slot = Some((addr, Instant::now()));
                        }
                    }

                    if applied > 0 {
                        // Update cached best hash and relay lock
                        {
                            let chain = blockchain.read().await;
                            let tip = chain.tip();
                            *cached_best_hash.write().await = hex::encode(&tip.hash);
                            let mut lock = RELAY_LOCK.lock().await;
                            *lock = (tip.index, tip.hash, Instant::now());
                        }
                        info!("[SYNC] Applied {} blocks from {} — height now: {}", applied, addr, last_height);

                        // Persist to disk after sync batch
                        if let Some(ref dir) = data_dir {
                            let chain = blockchain.read().await;
                            let ledger_state = ledger.read().await;
                            chain.save(&dir.join("chain.db"));
                            ledger_state.save(&dir.join("ledger.bin"));
                        }

                        // Continue IBD if peer has more blocks
                        let our_h = blockchain.read().await.height();
                        if let Some(ph) = peer_height {
                            if our_h < ph {
                                let from = our_h + 1;
                                let to = (from + crate::sync::IBD_CHUNK_SIZE - 1).min(ph);
                                info!("[SYNC] Continuing IBD: downloading blocks {}..{} from {}", from, to, addr);
                                send_to_peer(out_tx, Message::GetBlocks { from, to })?;
                            } else {
                                info!("[SYNC] IBD complete — height: {}", our_h);
                                {
                                    let mut ch = blockchain.write().await;
                                    ch.recalibrate_abc();
                                    cached_difficulty.store(ch.difficulty, Ordering::Release);
                                    info!("[SYNC] ABC recalibrated — target_block_ms: {}ms difficulty: {}", ch.target_block_ms, ch.difficulty);
                                }
                                *ibd_peer.lock().await = None;
                                if !sync_ready.load(Ordering::Relaxed) {
                                    sync_ready.store(true, Ordering::Release);
                                    info!("[SYNC] Sync ready — mining can start");
                                }
                            }
                        } else {
                            // peer_height unknown — ask the peer so IBD can continue
                            info!("[SYNC] peer_height unknown after applying blocks — requesting ChainHeight");
                            send_to_peer(out_tx, Message::GetChainHeight)?;
                        }
                    }
                }
            }

            // ── Transaction finality messages ────────────────────────────────

            Message::TxAck(ack) => {
                let tx_hash_hex = ack.tx_hash_hex();
                
                // Record the ACK
                if let Some(status) = finality.record_ack(ack.clone()).await {
                    match &status {
                        TransactionStatus::Confirmed { acks, finality_ms } => {
                            info!(
                                "[FINALITY] tx {} CONFIRMED — {} ACKs, {}ms finality",
                                &tx_hash_hex[..16], acks, finality_ms
                            );
                        }
                        TransactionStatus::Pending { acks, quorum } => {
                            debug!(
                                "[FINALITY] tx {} — {}/{} ACKs",
                                &tx_hash_hex[..16], acks, quorum
                            );
                        }
                        _ => {}
                    }

                    // Relay ACK to other peers via persistent connections
                    {
                        let pm = peers.lock().await;
                        pm.broadcast(Message::TxAck(ack), Some(&addr));
                    }
                }
            }

            Message::GetTxStatus { tx_hash } => {
                let hash_bytes: [u8; 32] = hex::decode(&tx_hash)
                    .ok()
                    .and_then(|v| v.try_into().ok())
                    .unwrap_or([0u8; 32]);
                
                let (acks, is_final) = if let Some(status) = finality.get_status(&hash_bytes).await {
                    match status {
                        TransactionStatus::Confirmed { acks, .. } => (acks, true),
                        TransactionStatus::Pending { acks, .. } => (acks, false),
                        _ => (0, false),
                    }
                } else {
                    (0, false)
                };

                send_to_peer(out_tx, Message::TxStatus {
                    tx_hash,
                    acks,
                    is_final,
                })?;
            }

            Message::TxStatus { tx_hash, acks, is_final } => {
                debug!(
                    "[STATUS] tx {} — {} ACKs, final: {}",
                    &tx_hash[..16.min(tx_hash.len())], acks, is_final
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_config_default() {
        let config = NodeConfig::default();
        assert_eq!(config.listen_port, 8333);
        assert!(config.seed_nodes.is_empty());
        assert!(config.enable_discovery);
    }

    #[tokio::test]
    async fn test_node_status() {
        let node = TaronNode::new(NodeConfig::default());
        let status = node.status().await;
        assert_eq!(status.peer_count, 0);
        assert_eq!(status.mempool_size, 0);
        assert_eq!(status.chain_height, 0);
    }

    #[tokio::test]
    async fn test_node_mempool_insert() {
        let node = TaronNode::new(NodeConfig::default());
        let sender = taron_core::Wallet::generate();
        let recipient = taron_core::Wallet::generate();
        let tx = taron_core::TxBuilder::new(&sender)
            .recipient(recipient.public_key())
            .amount(1_000_000)
            .sequence(1)
            .build_and_prove()
            .unwrap();

        {
            let mut pool = node.mempool.write().await;
            assert!(pool.insert(tx).unwrap());
        }

        let status = node.status().await;
        assert_eq!(status.mempool_size, 1);
    }

    #[tokio::test]
    async fn test_two_nodes_handshake() {
        // Start node A
        let node_a = Arc::new(TaronNode::new(NodeConfig {
            listen_port: 0,
            seed_nodes: vec![],
            enable_discovery: false,
            data_dir: None,
        }));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_a = listener.local_addr().unwrap();

        let mempool_a = node_a.mempool.clone();
        let peers_a = node_a.peers.clone();

        // Accept one connection on node A
        let server = tokio::spawn(async move {
            let (mut stream, addr) = listener.accept().await.unwrap();
            {
                let mut pm = peers_a.lock().await;
                pm.add_peer(addr, PeerDirection::Inbound);
            }
            protocol::send_message(&mut stream, &Message::Hello {
                version: 1,
                listen_port: addr_a.port(),
                user_agent: "node-a".into(),
                genesis_hash: genesis_hash_hex(),
            }).await.unwrap();
            let msg = protocol::recv_message(&mut stream).await.unwrap();
            match msg {
                Message::Hello { user_agent, .. } => assert_eq!(user_agent, "node-b"),
                other => panic!("expected Hello, got {:?}", other),
            }
            mempool_a.read().await.len()
        });

        // Node B connects
        let mut stream_b = TcpStream::connect(addr_a).await.unwrap();
        let msg = protocol::recv_message(&mut stream_b).await.unwrap();
        match msg {
            Message::Hello { user_agent, .. } => assert_eq!(user_agent, "node-a"),
            other => panic!("expected Hello, got {:?}", other),
        }
        protocol::send_message(&mut stream_b, &Message::Hello {
            version: 1,
            listen_port: 9999,
            user_agent: "node-b".into(),
            genesis_hash: genesis_hash_hex(),
        }).await.unwrap();

        let pool_size = server.await.unwrap();
        assert_eq!(pool_size, 0);
    }
}
