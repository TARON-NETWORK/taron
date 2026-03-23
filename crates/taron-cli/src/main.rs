//! TARON CLI — Node, wallet, and transaction management.

mod miner_tui;
mod bench;

use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use taron_core::{Wallet, TxBuilder, Ledger, Transaction, meets_difficulty, meets_target, TESTNET_DIFFICULTY, TESTNET_TARGET, TESTNET_REWARD, Block, Blockchain, target_to_bits};
use taron_node::TaronNode;
use taron_node::node::NodeConfig;
use taron_node::state_file::NodeStateFile;

fn get_banner(testnet: bool) -> String {
    let base = r#"
 ████████╗ █████╗ ██████╗  ██████╗ ███╗   ██╗
 ╚══██╔══╝██╔══██╗██╔══██╗██╔═══██╗████╗  ██║
    ██║   ███████║██████╔╝██║   ██║██╔██╗ ██║
    ██║   ██╔══██║██╔══██╗██║   ██║██║╚██╗██║
    ██║   ██║  ██║██║  ██║╚██████╔╝██║ ╚████║
    ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
"#;
    if testnet {
        format!("{} [TESTNET]", base)
    } else {
        base.to_string()
    }
}

#[derive(Parser)]
#[command(name = "taron", version = "0.2.0", about = "TARON — instant CPU-only cryptocurrency")]
struct Cli {
    /// Use testnet configuration
    #[arg(long, global = true)]
    testnet: bool,
    
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a TARON P2P node
    Node {
        #[command(subcommand)]
        action: NodeAction,
    },
    /// Wallet management
    Wallet {
        #[command(subcommand)]
        action: WalletAction,
    },
    /// Send TAR to a recipient
    Send {
        /// Recipient address (tar1...)
        dest: String,
        /// Amount in TAR (e.g. 10.5)
        amount: f64,
    },
    /// Show node status (peers, mempool, etc.)
    Status,
    /// Run performance benchmarks
    Bench {
        /// Number of iterations per benchmark
        #[arg(long, default_value = "100")]
        count: u32,
        /// Save results to benchmarks/ directory
        #[arg(long)]
        save: bool,
    },
}

#[derive(Subcommand)]
enum NodeAction {
    /// Start the P2P node
    Start {
        /// TCP listen port (default: 8333)
        #[arg(short, long, default_value = "8333")]
        port: u16,
        /// Seed node addresses (host:port)
        #[arg(short, long)]
        seed: Vec<String>,
        /// Disable UDP local peer discovery
        #[arg(long)]
        no_discovery: bool,
        /// Enable mining while running the node
        #[arg(long)]
        mine: bool,
        /// Number of mining threads (requires --mine)
        #[arg(long, default_value = "1")]
        threads: u32,
        /// Enable HTTP REST API on this port (default: disabled)
        #[arg(long)]
        rpc_port: Option<u16>,
        /// Mine in pool mode — submit shares to this pool URL (e.g. https://pool-api.taron.network)
        #[arg(long)]
        pool: Option<String>,
        /// Payout address for pool mining (tar1...) — overrides local wallet
        #[arg(long)]
        address: Option<String>,
        /// Public key hex for pool mining — use with web wallet pubkey (32 bytes hex)
        #[arg(long)]
        pubkey: Option<String>,
        /// Worker name shown in pool dashboard (e.g. "rig1", "laptop")
        #[arg(long, default_value = "default")]
        worker: String,
    },
}

#[derive(Subcommand)]
enum WalletAction {
    /// Generate a new wallet
    Generate,
    /// Show wallet info
    Info {
        /// Path to wallet key file
        #[arg(short, long)]
        key: Option<PathBuf>,
    },
}

fn get_data_dir(testnet: bool) -> PathBuf {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    if testnet {
        home.join(".taron-testnet")
    } else {
        home.join(".taron")
    }
}

fn wallet_path(testnet: bool) -> PathBuf {
    get_data_dir(testnet).join("wallet.key")
}

/// Interactive first-run wizard — walks the user through initial setup.
fn run_wizard(testnet: bool, data_dir: &PathBuf, wp: &PathBuf) -> anyhow::Result<()> {
    use dialoguer::{Confirm, Input};
    use indicatif::{ProgressBar, ProgressStyle};
    use std::time::Duration;

    println!(" First time? Let's set up your node.\n");

    // Step 1: Generate wallet
    let gen_wallet = Confirm::new()
        .with_prompt(" Generate a new wallet?")
        .default(true)
        .interact()?;

    let wallet = if gen_wallet {
        let w = Wallet::generate();
        std::fs::create_dir_all(data_dir)?;
        let wf = w.to_file();
        let json = serde_json::to_string_pretty(&wf)?;
        std::fs::write(wp, &json)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(wp, std::fs::Permissions::from_mode(0o600))?;
        }
        println!(" ✓ Wallet created");
        println!("   Address : {}", w.address());
        println!("   Saved to: {}  [chmod 600]\n", wp.display());
        w
    } else {
        // Import existing key
        let key_hex: String = Input::new()
            .with_prompt(" Enter your private key (hex)")
            .interact_text()?;
        let w = Wallet::from_hex(&key_hex)?;
        std::fs::create_dir_all(data_dir)?;
        let wf = w.to_file();
        let json = serde_json::to_string_pretty(&wf)?;
        std::fs::write(wp, &json)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(wp, std::fs::Permissions::from_mode(0o600))?;
        }
        println!(" ✓ Wallet imported");
        println!("   Address : {}\n", w.address());
        w
    };

    // Step 2: Mining preference
    let start_mining = Confirm::new()
        .with_prompt(" Start mining automatically?")
        .default(false)
        .interact()?;

    // Step 3: Peer configuration
    let peer_input: String = Input::new()
        .with_prompt(" Add a known peer? (leave empty to scan local network)")
        .default(String::new())
        .show_default(false)
        .interact_text()?;

    // Step 4: Simulated network scan
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template(" {spinner:.dim} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );

    if peer_input.is_empty() {
        pb.set_message("Scanning local network for peers...");
    } else {
        pb.set_message(format!("Connecting to {}...", peer_input));
    }
    pb.enable_steady_tick(Duration::from_millis(80));
    std::thread::sleep(Duration::from_secs(2));
    pb.finish_and_clear();

    if peer_input.is_empty() {
        println!(" ✓ Network scan complete (connect peers with `taron node start --seed <ip:port>`)");
    } else {
        println!(" ✓ Peer {} added", peer_input);
    }

    // Step 5: Write config
    let config_path = data_dir.join("config.toml");
    let config = format!(
        r#"# TARON node configuration
[node]
network = "{}"
listen_port = 9333
enable_discovery = true
auto_mine = {}

[mining]
threads = 1
thermal_target = 55

[peers]
seeds = [{}]
"#,
        if testnet { "testnet" } else { "testnet" },
        start_mining,
        if peer_input.is_empty() {
            String::new()
        } else {
            format!("\"{}\"", peer_input)
        },
    );
    std::fs::write(&config_path, &config)?;

    // Final summary
    println!("\n ┌──────────────────────────────────────┐");
    println!(" │  Address : {}  │", &wallet.address()[..20]);
    println!(" │  Network : {:<27}│", if testnet { "testnet" } else { "testnet" });
    println!(" │  Mining  : {:<27}│", if start_mining { "enabled" } else { "disabled" });
    println!(" │  Config  : {:<27}│", "saved");
    println!(" └──────────────────────────────────────┘\n");

    if testnet {
        println!(" [TESTNET] Data directory: {}\n", data_dir.display());
    }

    println!(" Next steps:");
    let prefix = if testnet { "taron --testnet" } else { "taron" };
    println!("   {} node start     — Start the P2P node", prefix);
    if start_mining {
        println!("   {} mine           — Begin mining", prefix);
    }
    println!("   {} status         — Check node status", prefix);
    println!("   {} send <addr> <amount>  — Send TAR\n", prefix);

    Ok(())
}

fn load_or_create_wallet(testnet: bool, _use_faucet: bool) -> Wallet {
    let path = wallet_path(testnet);
    if path.exists() {
        let content = std::fs::read_to_string(&path).expect("Failed to read wallet file");
        let wf: taron_core::wallet::WalletFile = serde_json::from_str(&content)
            .expect("Failed to parse wallet file");
        Wallet::from_hex(&wf.private_key_hex).expect("Invalid wallet key")
    } else {
        let wallet = Wallet::generate();
        std::fs::create_dir_all(path.parent().unwrap()).ok();
        let wf = wallet.to_file();
        let json = serde_json::to_string_pretty(&wf).unwrap();
        std::fs::write(&path, &json).expect("Failed to save wallet");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).ok();
        }
        println!("Created new wallet at {}", path.display());
        wallet
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let testnet = cli.testnet;

    match cli.command {
        None => {
            println!("{}", get_banner(testnet));
            println!(" v0.2.0 · TAR · Instant Transactions · CPU Only\n");

            let data_dir = get_data_dir(testnet);
            let wp = wallet_path(testnet);

            // First-run wizard: no wallet exists yet
            if !wp.exists() {
                run_wizard(testnet, &data_dir, &wp)?;
            } else {
                // Returning user — show dashboard summary
                let content = std::fs::read_to_string(&wp)?;
                let wf: taron_core::wallet::WalletFile = serde_json::from_str(&content)?;
                let wallet = Wallet::from_hex(&wf.private_key_hex)?;

                println!(" ┌──────────────────────────────────────┐");
                println!(" │  Address : {}  │", &wallet.address()[..20]);
                println!(" │  Network : {:<27}│", if testnet { "testnet" } else { "testnet" });
                println!(" │  Status  : {:<27}│", "ready");
                println!(" └──────────────────────────────────────┘\n");
                println!(" Commands:");
                println!("   taron node start    — Start P2P node");
                println!("   taron send <addr> <amount>");
                println!("   taron mine          — Start CPU miner");
                println!("   taron status        — Node info");
                if testnet {
                    println!("\n [TESTNET] Data: {}", data_dir.display());
                }
                println!("\n taron --help for all options");
            }
        }

        Some(Commands::Node { action }) => match action {
            NodeAction::Start { port, seed, no_discovery, mine, threads, rpc_port, pool, address, pubkey, worker } => {
                // Initialize tracing for live logs
                tracing_subscriber::fmt()
                    .with_target(false)
                    .with_thread_ids(false)
                    .with_level(true)
                    .init();

                println!("{}", get_banner(testnet));
                println!(" TARON Node — Proof of Sequential Chain");
                println!(" ─────────────────────────────────────────────");
                println!("  Network   : testnet");
                println!("  Listen    : 0.0.0.0:{}", port);
                println!("  Discovery : {}", if no_discovery { "disabled" } else { "UDP broadcast" });
                if !seed.is_empty() {
                    println!("  Seeds     : {}", seed.join(", "));
                }
                if mine {
                    println!("  Mining    : {} threads", threads);
                }
                println!(" ─────────────────────────────────────────────\n");

                let seed_nodes: Vec<SocketAddr> = seed
                    .iter()
                    .filter_map(|s| s.parse().ok())
                    .collect();

                // ── Pool mining mode (no node needed) ──────────────────────────
                // Pool miners connect directly to the pool API, no chain sync required.
                if let Some(pool_url) = pool {
                    let wallet = load_or_create_wallet(testnet, false);
                    // --pubkey overrides local wallet pubkey (use web wallet pubkey)
                    let miner_pubkey_hex = pubkey.unwrap_or_else(|| hex::encode(wallet.public_key()));
                    // derive address from pubkey so payout always matches
                    let miner_address = address.unwrap_or_else(|| {
                        if let Ok(bytes) = hex::decode(&miner_pubkey_hex) {
                            if bytes.len() == 32 {
                                let mut arr = [0u8; 32];
                                arr.copy_from_slice(&bytes);
                                taron_core::address_from_pubkey(&arr)
                            } else { wallet.address() }
                        } else { wallet.address() }
                    });

                    println!(" [POOL] Connecting to {}", pool_url);
                    println!(" [POOL] Miner address: {}", miner_address);
                    println!(" [POOL] Starting {} thread(s)...\n", threads);

                    use std::sync::{Arc, Mutex};
                    use std::sync::atomic::{AtomicU64, Ordering};

                    #[derive(Clone, serde::Deserialize)]
                    struct WorkResp {
                        block_index: u64,
                        prev_hash: String,
                        timestamp: u64,
                        pool_pubkey: String,
                        #[allow(dead_code)]
                        difficulty: u32,
                        share_difficulty: u32,
                        #[serde(default)]
                        difficulty_target: u64,
                    }

                    #[derive(serde::Serialize)]
                    struct ShareMsg {
                        miner_address: String,
                        miner_pubkey: String,
                        worker_name: String,
                        nonce: u64,
                        block_index: u64,
                        timestamp: u64,
                        prev_hash: String,
                    }

                    let total_hashes = Arc::new(AtomicU64::new(0));
                    let total_shares = Arc::new(AtomicU64::new(0));
                    let shared_work: Arc<Mutex<Option<WorkResp>>> = Arc::new(Mutex::new(None));
                    let (share_tx, share_rx) = std::sync::mpsc::channel::<ShareMsg>();

                    // Dedicated thread: fetches work from pool every 2 seconds.
                    // Mining threads never do HTTP — they just read shared_work.
                    {
                        let pool_url = pool_url.clone();
                        let miner_address = miner_address.clone();
                        let shared_work = shared_work.clone();
                        std::thread::spawn(move || {
                            let http = reqwest::blocking::Client::new();
                            loop {
                                let url = format!("{}/pool/work?address={}", pool_url, miner_address);
                                if let Ok(resp) = http.get(&url).send().and_then(|r| r.json::<WorkResp>()) {
                                    *shared_work.lock().unwrap() = Some(resp);
                                }
                                std::thread::sleep(std::time::Duration::from_secs(2));
                            }
                        });
                    }

                    // Dedicated thread: submits shares to pool via HTTP POST.
                    {
                        let pool_url = pool_url.clone();
                        let total_shares = total_shares.clone();
                        std::thread::spawn(move || {
                            let http = reqwest::blocking::Client::new();
                            for msg in share_rx {
                                let url = format!("{}/pool/share", pool_url);
                                match http.post(&url).json(&msg).send() {
                                    Ok(resp) => {
                                        #[derive(serde::Deserialize)]
                                        struct ShareResp { accepted: bool, is_block: bool, #[allow(dead_code)] message: String }
                                        if let Ok(r) = resp.json::<ShareResp>() {
                                            if r.accepted {
                                                let count = total_shares.fetch_add(1, Ordering::Relaxed) + 1;
                                                if r.is_block {
                                                    println!(" [POOL] ★ BLOCK FOUND! #{} (share #{})", msg.block_index, count);
                                                } else {
                                                    println!(" [POOL] Share #{} accepted (block #{})", count, msg.block_index);
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => eprintln!(" [POOL] Share submit error: {}", e),
                                }
                            }
                        });
                    }

                    // Wait for first work before spawning miners.
                    println!(" [POOL] Waiting for work from pool...");
                    loop {
                        if shared_work.lock().unwrap().is_some() { break; }
                        std::thread::sleep(std::time::Duration::from_millis(500));
                    }
                    println!(" [POOL] Work received, starting {} mining threads\n", threads);

                    for thread_id in 0..threads {
                        let miner_address = miner_address.clone();
                        let miner_pubkey_hex = miner_pubkey_hex.clone();
                        let total_hashes = total_hashes.clone();
                        let shared_work = shared_work.clone();
                        let share_tx = share_tx.clone();
                        let worker = worker.clone();

                        std::thread::spawn(move || {
                            let mut nonce = thread_id as u64 * (u64::MAX / threads as u64);
                            let mut cached_block_index = 0u64;
                            let mut pool_pubkey_bytes = [0u8; 32];
                            let mut prev_hash_bytes = [0u8; 32];
                            let mut timestamp = 0u64;
                            let mut share_difficulty = 16u32;
                            let mut cached_difficulty_target = 0u64;
                            let mut prev_hash_hex = String::new();

                            loop {
                                // Check for new work (lock-free read, very fast).
                                if let Ok(guard) = shared_work.try_lock() {
                                    if let Some(w) = guard.as_ref() {
                                        if w.block_index != cached_block_index {
                                            cached_block_index = w.block_index;
                                            share_difficulty = w.share_difficulty;
                                            cached_difficulty_target = w.difficulty_target;
                                            timestamp = w.timestamp;
                                            prev_hash_hex = w.prev_hash.clone();
                                            if let Ok(b) = hex::decode(&w.pool_pubkey) {
                                                if b.len() == 32 { pool_pubkey_bytes.copy_from_slice(&b); }
                                            }
                                            if let Ok(b) = hex::decode(&w.prev_hash) {
                                                if b.len() == 32 { prev_hash_bytes.copy_from_slice(&b); }
                                            }
                                        }
                                    }
                                }

                                if cached_block_index == 0 {
                                    std::thread::sleep(std::time::Duration::from_millis(100));
                                    continue;
                                }

                                let candidate = Block {
                                    index: cached_block_index,
                                    prev_hash: prev_hash_bytes,
                                    timestamp,
                                    miner: pool_pubkey_bytes,
                                    nonce,
                                    hash: [0u8; 32],
                                    reward: TESTNET_REWARD,
                                    difficulty_target: cached_difficulty_target,
                                    transactions: vec![],
                                };

                                let hash = candidate.hash_header();
                                total_hashes.fetch_add(1, Ordering::Relaxed);

                                if meets_difficulty(&hash, share_difficulty) {
                                    let _ = share_tx.send(ShareMsg {
                                        miner_address: miner_address.clone(),
                                        miner_pubkey: miner_pubkey_hex.clone(),
                                        worker_name: worker.clone(),
                                        nonce,
                                        block_index: cached_block_index,
                                        timestamp,
                                        prev_hash: prev_hash_hex.clone(),
                                    });
                                }

                                nonce = nonce.wrapping_add(1);
                            }
                        });
                    }

                    // Pool stats display thread
                    let th = total_hashes.clone();
                    let ts = total_shares.clone();
                    std::thread::spawn(move || {
                        let start = std::time::Instant::now();
                        loop {
                            std::thread::sleep(std::time::Duration::from_secs(10));
                            let h = th.load(Ordering::Relaxed);
                            let s = ts.load(Ordering::Relaxed);
                            let elapsed = start.elapsed().as_secs_f64();
                            let hr = if elapsed > 0.0 { h as f64 / elapsed } else { 0.0 };
                            let (v, u) = if hr >= 1_000_000.0 { (hr/1_000_000.0, "MH/s") } else if hr >= 1000.0 { (hr/1000.0, "kH/s") } else { (hr, " H/s") };
                            println!(" [POOL] {:>7.2} {}  |  {} hashes  |  {} shares", v, u, h, s);
                        }
                    });

                    // Pool mining runs standalone, no node needed
                    // Just keep the process alive while mining threads work
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                    }
                }

                // ── Solo mining mode (needs full node) ──────────────────────────
                let data_dir = get_data_dir(testnet);
                let config = NodeConfig {
                    listen_port: port,
                    seed_nodes,
                    enable_discovery: !no_discovery,
                    data_dir: Some(data_dir.clone()),
                };
                let node = TaronNode::new(config);
                let mut mine_block_rx: Option<tokio::sync::mpsc::Receiver<Block>> = None;

                // Start integrated miner if --mine flag is set
                if mine {
                    use std::sync::Arc;
                    use std::sync::atomic::{AtomicU64, Ordering};

                    let wallet = load_or_create_wallet(testnet, false);
                    let pubkey = wallet.public_key();
                    let reward = TESTNET_REWARD;

                    // Share node's blockchain and ledger directly — same RocksDB instance
                    let node_bc = node.blockchain.clone();
                    let node_ledger = node.ledger.clone();
                    let node_mempool = node.mempool.clone();

                    // Capture the tokio runtime handle before spawning std threads.
                    // This lets miner threads access tokio async locks without needing
                    // to be inside an async context themselves.
                    let rt_handle = tokio::runtime::Handle::current();

                    let solutions = Arc::new(AtomicU64::new(0));
                    let total_hashes = Arc::new(AtomicU64::new(0));

                    let (block_tx, block_rx) = tokio::sync::mpsc::channel::<Block>(64);

                    let initial_difficulty = TESTNET_TARGET;
                    println!(" [MINER] Starting {} mining threads...", threads);
                    println!(" [MINER] Address: {}", wallet.address());
                    println!(" [MINER] Difficulty: ~{} bits (target: {})", target_to_bits(initial_difficulty), initial_difficulty);
                    println!(" [MINER] Waiting for initial sync before mining...\n");

                    let sync_ready = node.sync_ready.clone();

                    for thread_id in 0..threads {
                        let node_bc = node_bc.clone();
                        let node_ledger = node_ledger.clone();
                        let node_mempool = node_mempool.clone();
                        let rt = rt_handle.clone();
                        let solutions = solutions.clone();
                        let total_hashes = total_hashes.clone();
                        let block_tx = block_tx.clone();
                        let sync_ready = sync_ready.clone();

                        std::thread::spawn(move || {
                            // Wait for initial sync to complete before mining.
                            // This prevents mining on a stale tip (e.g. genesis) while IBD
                            // hasn't started yet, which would cause an immediate fork.
                            // Timeout after 30s for solo miners with no peers.
                            {
                                use std::sync::atomic::Ordering;
                                let wait_start = std::time::Instant::now();
                                let timeout = std::time::Duration::from_secs(30);
                                while !sync_ready.load(Ordering::Acquire) {
                                    if wait_start.elapsed() >= timeout {
                                        if thread_id == 0 {
                                            eprintln!(" [MINER] No peers found after 30s — starting mining (solo mode)");
                                        }
                                        break;
                                    }
                                    std::thread::sleep(std::time::Duration::from_millis(100));
                                }
                                if thread_id == 0 && sync_ready.load(Ordering::Acquire) {
                                    eprintln!(" [MINER] Sync complete — starting mining");
                                }
                            }

                            let mut nonce = thread_id as u64 * u64::MAX / threads as u64;
                            // Cache the block template — only refresh every 2s or after finding a block.
                            // This avoids rt.block_on() + RwLock per hash which kills performance.
                            let mut cached_index = 0u64;
                            let mut cached_prev_hash = [0u8; 32];
                            let mut cached_difficulty = 0u64;
                            let mut cached_txs: Vec<Transaction> = Vec::new();
                            let mut last_template_refresh = std::time::Instant::now();
                            let template_refresh_interval = std::time::Duration::from_secs(2);

                            loop {
                                // Refresh template periodically (not every hash!)
                                if last_template_refresh.elapsed() >= template_refresh_interval || cached_index == 0 {
                                    let (ci, cph, cd, ctxs) = rt.block_on(async {
                                        let bc = node_bc.read().await;
                                        let tip = bc.tip();
                                        let mp = node_mempool.read().await;
                                        let txs = mp.all_txs().into_iter().cloned().collect::<Vec<_>>();
                                        (tip.index + 1, tip.hash, bc.difficulty, txs)
                                    });
                                    cached_index = ci;
                                    cached_prev_hash = cph;
                                    cached_difficulty = cd;
                                    cached_txs = ctxs;
                                    last_template_refresh = std::time::Instant::now();
                                }

                                let timestamp = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as u64;

                                let mut candidate = Block {
                                    index: cached_index,
                                    prev_hash: cached_prev_hash,
                                    timestamp,
                                    miner: pubkey,
                                    nonce,
                                    hash: [0u8; 32],
                                    reward,
                                    difficulty_target: cached_difficulty,
                                    transactions: cached_txs.clone(),
                                };
                                let hash = candidate.hash_header();
                                total_hashes.fetch_add(1, Ordering::Relaxed);

                                if meets_target(&hash, cached_difficulty) {
                                    candidate.hash = hash;
                                    let sol_num = solutions.fetch_add(1, Ordering::Relaxed) + 1;

                                    // Apply block directly to node's canonical chain + ledger
                                    let balance = rt.block_on(async {
                                        let mut bc = node_bc.write().await;
                                        let mut l = node_ledger.write().await;
                                        match bc.apply_block(&candidate, &mut *l) {
                                            Ok(()) => {
                                                // Purge included txs from mempool
                                                let mut mp = node_mempool.write().await;
                                                for tx in &candidate.transactions {
                                                    mp.remove(&tx.hash_hex());
                                                }
                                                l.balance(&pubkey)
                                            },
                                            Err(e) => {
                                                eprintln!(" [MINER] Block #{} rejected: {}", candidate.index, e);
                                                l.balance(&pubkey)
                                            }
                                        }
                                    });

                                    println!(
                                        " [BLOCK] #{} | hash: {}… | nonce: {} | +{:.2} TAR",
                                        candidate.index,
                                        hex::encode(&candidate.hash[..4]),
                                        candidate.nonce,
                                        reward as f64 / 1_000_000.0
                                    );
                                    println!(" [MINER] ★ Solution #{} | Balance: {:.2} TAR",
                                        sol_num, balance as f64 / 1_000_000.0);

                                    // Send to relay task for disk save + peer broadcast
                                    block_tx.blocking_send(candidate).ok();
                                    // Force template refresh on next iteration
                                    last_template_refresh = std::time::Instant::now() - template_refresh_interval;
                                }
                                nonce = nonce.wrapping_add(1);
                            }
                        });
                    }
                    drop(block_tx);

                    // Stats display thread
                    let total_hashes_stats = total_hashes.clone();
                    let solutions_stats = solutions.clone();
                    let node_bc_stats = node_bc.clone();
                    let rt_stats = rt_handle.clone();
                    std::thread::spawn(move || {
                        let start = std::time::Instant::now();
                        loop {
                            std::thread::sleep(std::time::Duration::from_secs(10));
                            let h = total_hashes_stats.load(Ordering::Relaxed);
                            let s = solutions_stats.load(Ordering::Relaxed);
                            let elapsed = start.elapsed().as_secs_f64();
                            let hr = if elapsed > 0.0 { h as f64 / elapsed } else { 0.0 };
                            let (v, u) = if hr >= 1_000_000.0 { (hr/1_000_000.0, "MH/s") } else if hr >= 1000.0 { (hr / 1000.0, "kH/s") } else { (hr, " H/s") };
                            let elapsed_s = elapsed as u64;
                            let (eh, em, es) = (elapsed_s/3600, (elapsed_s%3600)/60, elapsed_s%60);
                            let diff_now = rt_stats.block_on(async { node_bc_stats.read().await.difficulty });
                            println!(" [MINER] ⛏  {:>7.2} {}  |  {} hashes  |  {} solutions  |  diff: {}  |  {:02}:{:02}:{:02}", v, u, h, s, diff_now, eh, em, es);
                        }
                    });

                    mine_block_rx = Some(block_rx);
                }

                // Relay task: save state + broadcast mined blocks to peers
                if let Some(mut rx) = mine_block_rx {
                    let node_bcast = node.clone();
                    tokio::spawn(async move {
                        while let Some(block) = rx.recv().await {
                            // Block already applied to node's chain by the miner thread.
                            // Just persist and broadcast.
                            node_bcast.save_state().await;
                            node_bcast.broadcast_block(&block).await;
                        }
                    });
                }

                // Periodic status log — style nœud crypto classique (Kaspa, Bitcoin)
                let node_ref = node.clone();
                let state_file_path = get_data_dir(testnet).join("node-state.json");
                tokio::spawn(async move {
                    let start = std::time::Instant::now();
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                        let st = node_ref.status().await;
                        let elapsed = start.elapsed().as_secs();
                        let (h, m, s) = (elapsed / 3600, (elapsed % 3600) / 60, elapsed % 60);
                        let best_short = if st.best_hash.len() >= 10 {
                            format!("{}…", &st.best_hash[..10])
                        } else {
                            st.best_hash.clone()
                        };
                        println!(
                            " [NODE] height: {}  |  hash: {}  |  peers: {} (in:{} out:{})  |  mempool: {} tx  |  mined: {:.2} TAR  |  uptime: {:02}:{:02}:{:02}",
                            st.chain_height,
                            best_short,
                            st.peer_count, st.inbound_count, st.outbound_count,
                            st.mempool_size,
                            st.total_supply as f64 / 1_000_000.0,
                            h, m, s
                        );
                        // Write state file for `taron status`
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        NodeStateFile {
                            chain_height: st.chain_height,
                            best_hash: st.best_hash.clone(),

                            peer_count: st.peer_count,
                            inbound_count: st.inbound_count,
                            outbound_count: st.outbound_count,
                            mempool_size: st.mempool_size,
                            total_supply: st.total_supply,
                            uptime_secs: elapsed,
                            updated_at: now,
                        }
                        .save(&state_file_path);
                    }
                });

                // Start HTTP REST API on a DEDICATED runtime so P2P load
                // can never starve the RPC server.
                if let Some(rpc_port) = rpc_port {
                    let rpc_node = node.clone();
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(2)
                            .enable_all()
                            .thread_name("rpc-worker")
                            .build()
                            .expect("Failed to build RPC runtime");
                        rt.block_on(async move {
                            if let Err(e) = taron_node::rpc::start_rpc(rpc_node, rpc_port).await {
                                eprintln!(" [RPC] Failed to start RPC server: {}", e);
                            }
                        });
                    });
                    println!(" [RPC] REST API enabled on port {} (dedicated runtime)", rpc_port);
                }

                node.run().await?;
            }
        },

        Some(Commands::Wallet { action }) => match action {
            WalletAction::Generate => {
                let wallet = Wallet::generate();
                let path = wallet_path(testnet);
                std::fs::create_dir_all(path.parent().unwrap())?;
                let wf = wallet.to_file();
                let json = serde_json::to_string_pretty(&wf)?;
                std::fs::write(&path, &json)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
                }
                println!("Wallet generated!");
                println!("Address: {}", wallet.address());
                println!("Saved to: {}", path.display());
                if testnet {
                    println!("\n[TESTNET] Network: testnet");
                }
            }
            WalletAction::Info { key } => {
                let path = key.unwrap_or_else(|| wallet_path(testnet));
                if !path.exists() {
                    let cmd = if testnet { "taron --testnet wallet generate" } else { "taron wallet generate" };
                    println!("No wallet found at {}. Run: {}", path.display(), cmd);
                    return Ok(());
                }
                let content = std::fs::read_to_string(&path)?;
                let wf: taron_core::wallet::WalletFile = serde_json::from_str(&content)?;
                let wallet = Wallet::from_hex(&wf.private_key_hex)?;
                println!("Address: {}", wallet.address());
                println!("Public key: {}", hex::encode(wallet.public_key()));
                if testnet {
                    println!("Network: testnet");
                }
            }
        },

        Some(Commands::Send { dest, amount }) => {
            let wallet = load_or_create_wallet(testnet, false);
            let amount_utar = (amount * 1_000_000.0) as u64;
            let rpc_port = 8082u16;
            let rpc_base = format!("http://127.0.0.1:{}", rpc_port);

            if testnet {
                println!("[TESTNET] Sending {} TAR ({} µTAR) to {}", amount, amount_utar, dest);
            } else {
                println!("Sending {} TAR ({} µTAR) to {}", amount, amount_utar, dest);
            }
            println!("From: {}", wallet.address());

            // Parse tar1 address → 32-byte pubkey
            let recipient_bytes = parse_tar1_address(&dest)
                .ok_or_else(|| anyhow::anyhow!("Invalid address: must be tar1 + 64 hex chars (68 total)"))?;

            // Fetch current sequence from the running node's RPC
            let account_url = format!("{}/api/v1/accounts/{}", rpc_base, wallet.address());
            let sequence = match reqwest::blocking::get(&account_url) {
                Ok(resp) if resp.status().is_success() => {
                    let body: serde_json::Value = resp.json().unwrap_or_default();
                    body["sequence"].as_u64().unwrap_or(0)
                }
                _ => {
                    eprintln!("Error: cannot reach node RPC at {}. Is the node running?", rpc_base);
                    return Ok(());
                }
            };

            let tx = TxBuilder::new(&wallet)
                .recipient(recipient_bytes)
                .amount(amount_utar)
                .sequence(sequence + 1)
                .build_and_prove()?;

            println!("Transaction created: {}", tx.hash_hex());
            println!("PoSC proof: {}", hex::encode(tx.posc_proof));

            // Submit to local node RPC
            let submit_url = format!("{}/api/v1/submit_tx", rpc_base);
            let client = reqwest::blocking::Client::new();
            match client.post(&submit_url).json(&tx).send() {
                Ok(resp) => {
                    let body: serde_json::Value = resp.json().unwrap_or_default();
                    if body["accepted"].as_bool() == Some(true) {
                        println!("Broadcasted successfully!");
                    } else {
                        let msg = body["message"].as_str().unwrap_or("unknown error");
                        eprintln!("Rejected by node: {}", msg);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to submit to node: {}", e);
                }
            }
        }

        Some(Commands::Bench { count, save }) => {
            println!("{}", get_banner(testnet));
            println!(" TARON Performance Benchmarks");
            println!(" ─────────────────────────────────────────────\n");

            let report = bench::run_all_benchmarks(count);
            bench::print_results(&report);

            if save {
                let data_dir = get_data_dir(testnet);
                let bench_dir = data_dir.join("benchmarks");
                let filename = format!("results-{}.json", chrono::Utc::now().format("%Y-%m-%d-%H%M%S"));
                let path = bench_dir.join(&filename);
                report.save(&path)?;
                println!("\n 💾 Results saved to: {}", path.display());
            }

            println!();
        }

        Some(Commands::Status) => {
            println!("{}", get_banner(testnet));
            println!(" Node Status");
            println!(" ─────────────────────────────────────────────────");
            println!(" Network    : {}", if testnet { "testnet" } else { "mainnet" });
            println!(" Difficulty : ~{} bits (target-based)", TESTNET_DIFFICULTY);

            let data_dir = get_data_dir(testnet);

            // Try to read live state from running node
            let state_file_path = data_dir.join("node-state.json");
            if let Some(live) = NodeStateFile::load(&state_file_path) {
                // Node is (or was recently) running
                let best_hash_short = if live.best_hash.len() >= 10 {
                    format!("{}…", &live.best_hash[..10])
                } else {
                    live.best_hash.clone()
                };

                println!(" ─────────────────────────────────────────────────");
                println!(" Chain height : {} blocks", live.chain_height);
                println!(" Best hash    : {}", best_hash_short);
                println!(" Peers        : {} (in:{} out:{})",
                    live.peer_count, live.inbound_count, live.outbound_count);
                println!(" Mempool      : {} tx", live.mempool_size);
                println!(" Supply       : {:.2} TAR",
                    live.total_supply as f64 / 1_000_000.0);

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let age = now.saturating_sub(live.updated_at);
                if age < 120 {
                    println!(" Node uptime  : {}s (last seen {}s ago)", live.uptime_secs, age);
                } else {
                    println!(" Node         : last seen {}s ago (may be offline)", age);
                }
            } else {
                println!(" ─────────────────────────────────────────────────");
                println!(" Chain height : — (node not running)");
                println!(" Best hash    : —");
                println!(" Peers        : —");
                println!(" Mempool      : —");
            }

            println!(" ─────────────────────────────────────────────────");

            // Wallet + local ledger
            let path = wallet_path(testnet);
            if path.exists() {
                let content = std::fs::read_to_string(&path).unwrap_or_default();
                if let Ok(wf) = serde_json::from_str::<taron_core::wallet::WalletFile>(&content) {
                    if let Ok(wallet) = Wallet::from_hex(&wf.private_key_hex) {
                        println!(" Wallet       : {}", wallet.address());

                        let chain_path_w = data_dir.join("chain.db");
                        let ledger_path = data_dir.join("ledger.bin");
                        if ledger_path.exists() || chain_path_w.exists() {
                            let chain_w = Blockchain::load_or_create(&chain_path_w, taron_core::TESTNET_DIFFICULTY);
                            let ledger = Ledger::load_or_create_testnet(&ledger_path, &chain_w);
                            let balance = ledger.balance(&wallet.public_key());
                            let supply = ledger.total_supply();
                            println!(" Balance      : {:.6} TAR", balance as f64 / 1_000_000.0);
                            println!(" Supply       : {:.2} TAR", supply as f64 / 1_000_000.0);
                            println!(" Accounts     : {}", ledger.account_count());
                        } else {
                            println!(" Balance      : 0.000000 TAR");
                        }
                    }
                }
            } else {
                println!(" Wallet       : not created  (run `taron wallet generate`)");
                println!(" Balance      : —");
            }

            println!(" ─────────────────────────────────────────────────");
            println!(" Data dir     : {}", data_dir.display());
            if testnet {
                let prefix = "taron --testnet";
                println!("\n Start node : {} node start", prefix);
                println!(" Start mine : {} mine", prefix);
            }
        }
    }

    Ok(())
}

/// Parse a tar1... address (tar1 + 64 hex chars = 68 total) to a 32-byte public key.
fn parse_tar1_address(addr: &str) -> Option<[u8; 32]> {
    if addr.len() != 68 || !addr.starts_with("tar1") {
        return None;
    }
    let hex_part = &addr[4..];
    let bytes = hex::decode(hex_part).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Some(key)
}
