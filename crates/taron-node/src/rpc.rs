//! HTTP REST API for the TARON node.

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderValue, Method, StatusCode},
    middleware,
    response::{IntoResponse, Json, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as TokioMutex;
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tracing::info;

// ── Rate limiting ─────────────────────────────────────────────────────────────

/// Per-IP request counter: (count_in_window, window_start).
type RateLimiter = Arc<TokioMutex<HashMap<IpAddr, (u32, Instant)>>>;

/// 600 requests per 60-second window per IP (~10 req/s sustained).
/// Localhost (pool backend, Traefik proxy) is exempt from rate limiting.
const RATE_WINDOW: Duration = Duration::from_secs(60);
const RATE_MAX_REQ: u32 = 600;

/// Maximum request body size: 512 KB. Protects expensive POST endpoints.
const MAX_BODY_BYTES: usize = 512 * 1024;

use taron_core::{wallet::{address_from_pubkey, pubkey_from_address}, Block, Transaction};
use crate::TaronNode;

// ── Response types ───────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct StatusResponse {
    pub chain_height: u64,
    pub best_hash: String,
    pub difficulty: u32,
    pub peer_count: usize,
    pub inbound_peers: usize,
    pub outbound_peers: usize,
    pub mempool_size: usize,
    pub account_count: usize,
    pub total_supply: u64,
    pub total_tx_count: u64,
}

#[derive(Serialize)]
pub struct BlockResponse {
    pub index: u64,
    pub hash: String,
    pub prev_hash: String,
    pub timestamp: u64,
    pub miner: String,
    pub miner_address: String,
    pub nonce: u64,
    pub reward: u64,
    pub tx_count: usize,
}

#[derive(Serialize)]
pub struct BlocksResponse {
    pub blocks: Vec<BlockResponse>,
    pub total: usize,
}

#[derive(Serialize)]
pub struct TxResponse {
    pub hash: String,
    pub sender: String,
    pub sender_address: String,
    pub recipient: String,
    pub recipient_address: String,
    pub amount: u64,
    pub fee: u64,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub posc_steps: u32,
    pub block_index: Option<u64>,
}

#[derive(Serialize)]
pub struct MempoolResponse {
    pub count: usize,
    pub transactions: Vec<TxResponse>,
}

#[derive(Serialize)]
pub struct TxWithBlock {
    pub hash: String,
    pub sender: String,
    pub sender_address: String,
    pub recipient: String,
    pub recipient_address: String,
    pub amount: u64,
    pub fee: u64,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub posc_steps: u32,
    pub block_index: u64,
}

#[derive(Serialize)]
pub struct AccountTxsResponse {
    pub transactions: Vec<TxWithBlock>,
    pub total: usize,
}

#[derive(Serialize)]
pub struct AccountResponse {
    pub pubkey: String,
    pub address: String,
    pub balance: u64,
    pub sequence: u64,
    pub blocks_mined: u64,
    pub tx_count: u64,
    pub first_seen: u64,
    pub last_seen: u64,
    pub last_tx_hash: String,
}

#[derive(Serialize)]
pub struct AccountsResponse {
    pub accounts: Vec<AccountResponse>,
    pub total: usize,
}

#[derive(Serialize)]
pub struct PeerResponse {
    pub country: String,
    pub direction: String,
    pub version: u8,
    pub user_agent: String,
    pub connected_secs: u64,
}

#[derive(Serialize)]
pub struct SupplyPoint {
    pub block_index: u64,
    pub timestamp_ms: u64,
    pub reward: u64,
    pub cumulative_supply: u64,
}

#[derive(Serialize)]
pub struct PeersResponse {
    pub peers: Vec<PeerResponse>,
    pub countries: std::collections::HashMap<String, usize>,
    pub total: usize,
}

#[derive(Deserialize)]
pub struct PaginationParams {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn block_to_response(b: taron_core::Block) -> BlockResponse {
    BlockResponse {
        index: b.index,
        hash: hex::encode(b.hash),
        prev_hash: hex::encode(b.prev_hash),
        timestamp: b.timestamp,
        miner: hex::encode(b.miner),
        miner_address: address_from_pubkey(&b.miner),
        nonce: b.nonce,
        reward: b.reward,
        tx_count: b.transactions.len(),
    }
}

fn tx_to_response(tx: &taron_core::Transaction) -> TxResponse {
    TxResponse {
        hash: tx.hash_hex(),
        sender: hex::encode(tx.sender),
        sender_address: tx.sender_address(),
        recipient: hex::encode(tx.recipient),
        recipient_address: tx.recipient_address(),
        amount: tx.amount,
        fee: tx.fee,
        sequence: tx.sequence,
        timestamp_ms: tx.timestamp_ms,
        posc_steps: tx.posc_steps,
        block_index: None,
    }
}

fn account_from_blocks(
    pubkey: &[u8; 32],
    state: &taron_core::AccountState,
    mined: Vec<taron_core::Block>,
    tx_timestamps: &[(u64, String)], // (timestamp_ms, tx_hash_hex) for all txs involving this account
) -> AccountResponse {
    // Combine block timestamps and tx timestamps to compute first/last seen
    let block_timestamps: Vec<u64> = mined.iter().map(|b| b.timestamp).collect();
    let tx_ts: Vec<u64> = tx_timestamps.iter().map(|(ts, _)| *ts).collect();

    let all_timestamps: Vec<u64> = block_timestamps.iter().chain(tx_ts.iter()).cloned().collect();
    let first_seen = all_timestamps.iter().copied().min().unwrap_or(0);
    let last_seen = all_timestamps.iter().copied().max().unwrap_or(0);

    // Last tx hash: prefer latest tx over latest mined block
    let last_tx_hash = {
        let latest_tx = tx_timestamps.iter().max_by_key(|(ts, _)| ts).map(|(_, h)| h.clone());
        let latest_block = mined.iter().max_by_key(|b| b.timestamp).map(|b| hex::encode(b.hash));
        match (latest_tx, latest_block) {
            (Some(tx_h), Some(blk_h)) => {
                let tx_ts_max = tx_timestamps.iter().max_by_key(|(ts, _)| ts).map(|(ts, _)| *ts).unwrap_or(0);
                let blk_ts_max = mined.iter().map(|b| b.timestamp).max().unwrap_or(0);
                if tx_ts_max >= blk_ts_max { tx_h } else { blk_h }
            }
            (Some(tx_h), None) => tx_h,
            (None, Some(blk_h)) => blk_h,
            (None, None) => String::new(),
        }
    };

    AccountResponse {
        pubkey: hex::encode(pubkey),
        address: address_from_pubkey(pubkey),
        balance: state.balance,
        sequence: state.sequence,
        blocks_mined: mined.len() as u64,
        tx_count: tx_timestamps.len() as u64,
        first_seen,
        last_seen,
        last_tx_hash,
    }
}

// ── Handlers ─────────────────────────────────────────────────────────────────

async fn get_status(State(node): State<TaronNode>) -> Json<StatusResponse> {
    let st = node.status().await;
    // status() already reads blockchain — reuse its data to avoid double-locking
    Json(StatusResponse {
        chain_height: st.chain_height,
        best_hash: st.best_hash,
        difficulty: st.difficulty,
        peer_count: st.peer_count,
        inbound_peers: st.inbound_count,
        outbound_peers: st.outbound_count,
        mempool_size: st.mempool_size,
        account_count: st.account_count,
        total_supply: st.total_supply,
        total_tx_count: st.chain_height,
    })
}

async fn get_blocks(
    State(node): State<TaronNode>,
    Query(params): Query<PaginationParams>,
) -> Json<BlocksResponse> {
    let chain = node.blockchain.read().await;
    let total = chain.total_blocks();
    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(20).min(100);
    let blocks = chain.blocks_paginated(offset, limit)
        .into_iter()
        .map(block_to_response)
        .collect();
    Json(BlocksResponse { blocks, total })
}

async fn get_block_by_index(
    State(node): State<TaronNode>,
    Path(index): Path<u64>,
) -> Json<Option<BlockResponse>> {
    let chain = node.blockchain.read().await;
    Json(chain.block_at(index).map(block_to_response))
}

async fn get_tx_by_hash(
    State(node): State<TaronNode>,
    Path(hash): Path<String>,
) -> Json<Option<TxResponse>> {
    // Search confirmed transactions in blockchain first
    let chain = node.blockchain.read().await;
    for i in (0..=chain.height).rev() {
        if let Some(block) = chain.block_at(i) {
            for tx in &block.transactions {
                if tx.hash_hex() == hash {
                    let mut resp = tx_to_response(tx);
                    resp.block_index = Some(block.index);
                    return Json(Some(resp));
                }
            }
        }
    }
    drop(chain);

    // Fall back to mempool (pending)
    let mempool = node.mempool.read().await;
    for tx in mempool.all_txs() {
        if tx.hash_hex() == hash {
            return Json(Some(tx_to_response(tx)));
        }
    }
    Json(None)
}

async fn get_mempool(State(node): State<TaronNode>) -> Json<MempoolResponse> {
    let mempool = node.mempool.read().await;
    let txs = mempool.all_txs().iter().map(|tx| tx_to_response(tx)).collect::<Vec<_>>();
    Json(MempoolResponse { count: txs.len(), transactions: txs })
}

/// Return full Transaction objects (for pool to include in blocks).
async fn get_mempool_raw(State(node): State<TaronNode>) -> Json<Vec<Transaction>> {
    let mempool = node.mempool.read().await;
    let txs: Vec<Transaction> = mempool.all_txs().into_iter().cloned().collect();
    Json(txs)
}

async fn get_accounts(
    State(node): State<TaronNode>,
    Query(params): Query<PaginationParams>,
) -> Json<AccountsResponse> {
    let ledger = node.ledger.read().await;
    let chain = node.blockchain.read().await;
    let all = ledger.all_accounts();
    let total = all.len();
    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(50).min(200);

    let mut accounts: Vec<AccountResponse> = all.iter()
        .map(|(pubkey, state)| {
            let mined = chain.blocks_by_miner(pubkey);
            // For the accounts list we skip full tx scan (expensive) — tx_count = 0 is acceptable here
            account_from_blocks(pubkey, state, mined, &[])
        })
        .collect();
    accounts.sort_by(|a, b| b.balance.cmp(&a.balance));

    let page = accounts.into_iter().skip(offset).take(limit).collect();
    Json(AccountsResponse { accounts: page, total })
}

async fn get_account_by_address(
    State(node): State<TaronNode>,
    Path(address): Path<String>,
) -> Json<Option<AccountResponse>> {
    let pubkey = match pubkey_from_address(&address) {
        Some(pk) => pk,
        None => return Json(None),
    };
    let ledger = node.ledger.read().await;
    let chain = node.blockchain.read().await;
    match ledger.get_account(&pubkey) {
        Some(state) => {
            let mined = chain.blocks_by_miner(&pubkey);
            // Collect (timestamp_ms, tx_hash) for all txs involving this address
            let mut tx_timestamps: Vec<(u64, String)> = Vec::new();
            for i in 0..=chain.height {
                if let Some(block) = chain.block_at(i) {
                    for tx in &block.transactions {
                        if tx.sender == pubkey || tx.recipient == pubkey {
                            tx_timestamps.push((tx.timestamp_ms, tx.hash_hex()));
                        }
                    }
                }
            }
            Json(Some(account_from_blocks(&pubkey, state, mined, &tx_timestamps)))
        }
        None => Json(None),
    }
}

async fn get_block_txs(
    State(node): State<TaronNode>,
    Path(index): Path<u64>,
) -> Json<Vec<TxResponse>> {
    let chain = node.blockchain.read().await;
    if let Some(block) = chain.block_at(index) {
        let txs = block.transactions.iter().map(|tx| {
            let mut resp = tx_to_response(tx);
            resp.block_index = Some(index);
            resp
        }).collect();
        Json(txs)
    } else {
        Json(vec![])
    }
}

async fn get_account_txs(
    State(node): State<TaronNode>,
    Path(address): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Json<AccountTxsResponse> {
    let pubkey = match pubkey_from_address(&address) {
        Some(pk) => pk,
        None => return Json(AccountTxsResponse { transactions: vec![], total: 0 }),
    };

    let chain = node.blockchain.read().await;
    let mut all_txs: Vec<TxWithBlock> = Vec::new();

    // Scan all blocks for transactions involving this address
    for i in 0..=chain.height {
        if let Some(block) = chain.block_at(i) {
            for tx in &block.transactions {
                if tx.sender == pubkey || tx.recipient == pubkey {
                    all_txs.push(TxWithBlock {
                        hash: tx.hash_hex(),
                        sender: hex::encode(tx.sender),
                        sender_address: tx.sender_address(),
                        recipient: hex::encode(tx.recipient),
                        recipient_address: tx.recipient_address(),
                        amount: tx.amount,
                        fee: tx.fee,
                        sequence: tx.sequence,
                        timestamp_ms: tx.timestamp_ms,
                        posc_steps: tx.posc_steps,
                        block_index: block.index,
                    });
                }
            }
        }
    }
    drop(chain);

    // Also include mempool txs involving this address
    let mempool = node.mempool.read().await;
    for tx in mempool.all_txs() {
        if tx.sender == pubkey || tx.recipient == pubkey {
            all_txs.push(TxWithBlock {
                hash: tx.hash_hex(),
                sender: hex::encode(tx.sender),
                sender_address: tx.sender_address(),
                recipient: hex::encode(tx.recipient),
                recipient_address: tx.recipient_address(),
                amount: tx.amount,
                fee: tx.fee,
                sequence: tx.sequence,
                timestamp_ms: tx.timestamp_ms,
                posc_steps: tx.posc_steps,
                block_index: 0,
            });
        }
    }

    // Sort newest first
    all_txs.sort_by(|a, b| b.timestamp_ms.cmp(&a.timestamp_ms));

    let total = all_txs.len();
    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(20).min(100);
    let page = all_txs.into_iter().skip(offset).take(limit).collect();
    Json(AccountTxsResponse { transactions: page, total })
}

async fn get_account_blocks(
    State(node): State<TaronNode>,
    Path(address): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Json<BlocksResponse> {
    let chain = node.blockchain.read().await;

    let pubkey = pubkey_from_address(&address);

    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(20).min(100);

    let all_mined: Vec<BlockResponse> = pubkey
        .map(|pk| chain.blocks_by_miner(&pk))
        .unwrap_or_default()
        .into_iter()
        .rev()
        .map(block_to_response)
        .collect();

    let total = all_mined.len();
    let page = all_mined.into_iter().skip(offset).take(limit).collect();
    Json(BlocksResponse { blocks: page, total })
}

async fn get_peers(State(node): State<TaronNode>) -> Json<PeersResponse> {
    let pm = node.peers.lock().await;
    let now = Instant::now();
    let mut countries: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let peers: Vec<PeerResponse> = pm.all_peers().iter()
        .map(|p| {
            let cc = if p.country.is_empty() { "??".to_string() } else { p.country.clone() };
            *countries.entry(cc.clone()).or_insert(0) += 1;
            PeerResponse {
                country: cc,
                direction: format!("{:?}", p.direction).to_lowercase(),
                version: p.version,
                user_agent: p.user_agent.clone(),
                connected_secs: now.duration_since(p.connected_at).as_secs(),
            }
        })
        .collect();
    let total = peers.len();
    Json(PeersResponse { peers, countries, total })
}

async fn get_supply_history(State(node): State<TaronNode>) -> Json<Vec<SupplyPoint>> {
    use taron_core::PREMINE_BALANCE;
    let chain = node.blockchain.read().await;
    let mut cumulative: u64 = PREMINE_BALANCE;
    let mut points: Vec<SupplyPoint> = Vec::new();
    // Genesis point includes premine
    if let Some(block) = chain.block_at(0) {
        points.push(SupplyPoint {
            block_index: 0,
            timestamp_ms: block.timestamp,
            reward: 0,
            cumulative_supply: PREMINE_BALANCE,
        });
    }
    // Sample every block but cap to 500 points max (downsample for large chains)
    let total = chain.total_blocks() as u64;
    let step = if total <= 500 { 1u64 } else { total / 500 };
    for i in (1..=chain.height).step_by(step.max(1) as usize) {
        if let Some(block) = chain.block_at(i) {
            cumulative += block.reward;
            points.push(SupplyPoint {
                block_index: block.index,
                timestamp_ms: block.timestamp,
                reward: block.reward,
                cumulative_supply: cumulative,
            });
        }
    }
    Json(points)
}

// ── Submit transaction ───────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct SubmitTxResponse {
    pub hash: String,
    pub accepted: bool,
    pub message: String,
}

async fn submit_tx(
    State(node): State<TaronNode>,
    Json(tx): Json<Transaction>,
) -> (StatusCode, Json<SubmitTxResponse>) {
    let hash = tx.hash_hex();

    // Validate against current ledger (balance + sequence)
    {
        let ledger = node.ledger.read().await;
        let total_cost = tx.total_cost();
        let balance = ledger.balance(&tx.sender);
        if balance < total_cost {
            return (StatusCode::BAD_REQUEST, Json(SubmitTxResponse {
                hash,
                accepted: false,
                message: format!("Insufficient balance: have {} µTAR, need {} µTAR", balance, total_cost),
            }));
        }
        let acc = ledger.get_account(&tx.sender);
        let expected_seq = acc.map_or(1, |a| a.sequence + 1);
        if tx.sequence != expected_seq {
            return (StatusCode::BAD_REQUEST, Json(SubmitTxResponse {
                hash,
                accepted: false,
                message: format!("Invalid sequence: expected {}, got {}", expected_seq, tx.sequence),
            }));
        }
    }

    // Insert into mempool (validates signature + PoSC)
    let mut mempool = node.mempool.write().await;
    match mempool.insert(tx.clone()) {
        Ok(true) => {
            drop(mempool);
            node.broadcast_tx(&tx).await;
            info!("Accepted tx {} into mempool", &hash[..16]);
            (StatusCode::OK, Json(SubmitTxResponse {
                hash,
                accepted: true,
                message: "Transaction accepted".into(),
            }))
        }
        Ok(false) => (StatusCode::OK, Json(SubmitTxResponse {
            hash,
            accepted: false,
            message: "Duplicate transaction".into(),
        })),
        Err(e) => (StatusCode::BAD_REQUEST, Json(SubmitTxResponse {
            hash,
            accepted: false,
            message: format!("Invalid transaction: {}", e),
        })),
    }
}

// ── Submit block (pool) ───────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct SubmitBlockResponse {
    pub accepted: bool,
    pub message: String,
}

async fn submit_block(
    State(node): State<TaronNode>,
    Json(block): Json<Block>,
) -> (StatusCode, Json<SubmitBlockResponse>) {
    let accepted = node.submit_mined_block(block).await;
    if accepted {
        (StatusCode::OK, Json(SubmitBlockResponse {
            accepted: true,
            message: "Block accepted".into(),
        }))
    } else {
        (StatusCode::BAD_REQUEST, Json(SubmitBlockResponse {
            accepted: false,
            message: "Block rejected".into(),
        }))
    }
}

// ── Rate limit middleware ─────────────────────────────────────────────────────

async fn check_rate_limit(
    limiter: RateLimiter,
    req: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    use axum::extract::ConnectInfo;

    // ConnectInfo is populated by into_make_service_with_connect_info.
    // Falls back to 0.0.0.0 when called without connection info (tests).
    let ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip())
        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));

    // Exempt localhost and private networks (pool backend, Traefik proxy)
    if ip.is_loopback() || matches!(ip, IpAddr::V4(v4) if v4.octets()[0] == 10 || v4.octets()[0] == 172 || (v4.octets()[0] == 192 && v4.octets()[1] == 168)) {
        return next.run(req).await;
    }

    let mut map = limiter.lock().await;
    let now = Instant::now();
    let entry = map.entry(ip).or_insert((0, now));

    if now.duration_since(entry.1) > RATE_WINDOW {
        *entry = (1, now);
    } else {
        entry.0 += 1;
        if entry.0 > RATE_MAX_REQ {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                "Rate limit exceeded — max 120 requests per minute.",
            )
                .into_response();
        }
    }
    drop(map);

    next.run(req).await
}

// ── Server ───────────────────────────────────────────────────────────────────

pub async fn start_rpc(node: TaronNode, port: u16) -> std::io::Result<()> {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_origin([
            "https://explorer.taron.network".parse::<HeaderValue>().unwrap(),
            "https://pool.taron.network".parse::<HeaderValue>().unwrap(),
            "https://wallet.taron.network".parse::<HeaderValue>().unwrap(),
            "https://taron.network".parse::<HeaderValue>().unwrap(),
        ])
        .allow_headers(Any);

    // Per-IP rate limiter shared across all requests.
    let limiter: RateLimiter = Arc::new(TokioMutex::new(HashMap::new()));

    let app = Router::new()
        .route("/api/v1/status", get(get_status))
        .route("/api/v1/blocks", get(get_blocks))
        .route("/api/v1/blocks/{index}", get(get_block_by_index))
        .route("/api/v1/tx/{hash}", get(get_tx_by_hash))
        .route("/api/v1/mempool", get(get_mempool))
        .route("/api/v1/mempool/raw", get(get_mempool_raw))
        .route("/api/v1/accounts", get(get_accounts))
        .route("/api/v1/accounts/{address}", get(get_account_by_address))
        .route("/api/v1/blocks/{index}/txs", get(get_block_txs))
        .route("/api/v1/accounts/{address}/blocks", get(get_account_blocks))
        .route("/api/v1/accounts/{address}/txs", get(get_account_txs))
        .route("/api/v1/supply/history", get(get_supply_history))
        .route("/api/v1/peers", get(get_peers))
        .route("/api/v1/submit_tx", post(submit_tx))
        .route("/api/v1/submit_block", post(submit_block))
        // Rate limiter applies to all matched routes.
        .route_layer({
            let limiter = limiter.clone();
            middleware::from_fn(move |req: axum::extract::Request, next: middleware::Next| {
                let limiter = limiter.clone();
                async move { check_rate_limit(limiter, req, next).await }
            })
        })
        // Request body size cap — protects against large payload attacks.
        .layer(RequestBodyLimitLayer::new(MAX_BODY_BYTES))
        .layer(cors)
        .with_state(node);

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    info!("RPC server listening on port {}", port);
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    ).await
}
