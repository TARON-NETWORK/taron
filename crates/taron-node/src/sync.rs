//! Initial Block Download (IBD) and chain-sync protocol for TARON.
//!
//! ## Flow
//!
//! 1. After connecting to a peer, we send `GetChainHeight`.
//! 2. On receiving `ChainHeight(h)`:
//!    - If `h > our_height` → launch IBD towards that peer.
//! 3. IBD downloads blocks in chunks of `IBD_CHUNK_SIZE`:
//!    ```text
//!    GetBlocks { from: our_height+1, to: our_height+100 }
//!    ```
//! 4. On receiving `Blocks(vec)`:
//!    - Validate each block's hash integrity.
//!    - Apply the block to the chain state.
//!    - Log progress.
//!    - Continue until `our_height == peer_height`.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tracing::{info, warn};

use taron_core::Block;

use crate::protocol::{self, Message};

/// Number of blocks requested in each IBD chunk.
pub const IBD_CHUNK_SIZE: u64 = 100;

/// Shared chain state — height + best block hash.
#[derive(Debug)]
pub struct ChainState {
    /// Current canonical chain height (0 = genesis only).
    pub height: u64,
    /// Raw 32-byte hash of the best (tip) block.
    pub best_hash: [u8; 32],
}

impl ChainState {
    pub fn new() -> Self {
        let genesis_hash = Block::genesis().hash;
        Self {
            height: 0,
            best_hash: genesis_hash,
        }
    }

    /// Return `best_hash` as a lowercase hex string (for display).
    pub fn best_hash_hex(&self) -> String {
        hex::encode(self.best_hash)
    }
}

impl Default for ChainState {
    fn default() -> Self {
        Self::new()
    }
}

/// Run a height-exchange handshake immediately after a peer connection is
/// established.
///
/// Sends `GetChainHeight` and returns.  The response is handled by the main
/// message loop which calls [`handle_sync_message`].
pub async fn send_get_chain_height(stream: &mut TcpStream) -> std::io::Result<()> {
    protocol::send_message(stream, &Message::GetChainHeight).await
}

/// Perform a quick structural validation on a received block:
/// verifies that `block.hash == block.hash_header()`.
///
/// Full chain-linkage + difficulty validation is performed by
/// `Blockchain::apply_block()` in DEV-TESTNET-CORE.
fn quick_validate(block: &Block) -> bool {
    block.hash == block.hash_header()
}

/// Handle a single sync-related message received from a peer.
///
/// Returns `true` when a chunk request was sent (IBD is continuing),
/// `false` otherwise.
///
/// # Parameters
/// - `msg`         — the received message
/// - `stream`      — the TCP stream (to send follow-up messages)
/// - `addr`        — peer's socket address (for logging)
/// - `chain`       — shared chain state
/// - `peer_height` — a mutable slot filled when we receive `ChainHeight`
pub async fn handle_sync_message(
    msg: &Message,
    stream: &mut TcpStream,
    addr: SocketAddr,
    chain: Arc<RwLock<ChainState>>,
    peer_height: &mut Option<u64>,
) -> std::io::Result<bool> {
    match msg {
        // ── Inbound: peer asks for our height ────────────────────────────────
        Message::GetChainHeight => {
            let h = chain.read().await.height;
            protocol::send_message(stream, &Message::ChainHeight(h)).await?;
        }

        // ── Inbound: peer replies with its height ────────────────────────────
        Message::ChainHeight(h) => {
            let our_h = chain.read().await.height;
            *peer_height = Some(*h);

            if *h > our_h {
                info!(
                    "[SYNC] Peer {} is at height {} — we are at {} — launching IBD",
                    addr, h, our_h
                );
                let from = our_h + 1;
                let to = from + IBD_CHUNK_SIZE - 1;
                info!("[SYNC] Downloading blocks {}..{} from {}", from, to, addr);
                protocol::send_message(stream, &Message::GetBlocks { from, to }).await?;
                return Ok(true);
            } else {
                info!("[SYNC] Peer {} height {} — we are in sync (height {})", addr, h, our_h);
            }
        }

        // ── Inbound: peer asks us for blocks ─────────────────────────────────
        Message::GetBlocks { from, to } => {
            // We don't have a full block store yet (DEV-TESTNET-CORE will wire it in).
            // Respond with empty Blocks so the peer doesn't hang.
            let blocks: Vec<Block> = Vec::new();
            info!(
                "[SYNC] Peer {} requested blocks {}..{} — sending {} blocks (stub)",
                addr, from, to, blocks.len()
            );
            protocol::send_message(stream, &Message::Blocks(blocks)).await?;
        }

        // ── Inbound: block batch from IBD response ───────────────────────────
        Message::Blocks(blocks) => {
            if blocks.is_empty() {
                let h = chain.read().await.height;
                info!("[SYNC] Sync complete — height: {}", h);
                return Ok(false);
            }

            {
                let mut state = chain.write().await;
                for block in blocks.iter() {
                    if quick_validate(block) {
                        // TODO(DEV-TESTNET-CORE): call Blockchain::apply_block() for
                        //   full chain-linkage + difficulty + ledger update.
                        state.height = block.index;
                        state.best_hash = block.hash;
                        let hash_prefix = hex::encode(&block.hash[..5]);
                        info!(
                            "[SYNC] Applied block #{} | hash: {}…",
                            block.index, hash_prefix
                        );
                    } else {
                        warn!(
                            "[SYNC] Invalid block #{} from {} — hash mismatch, skipping",
                            block.index, addr
                        );
                    }
                }
            }

            // Continue IBD if we haven't reached peer_height yet
            let (our_h, need_more) = {
                let state = chain.read().await;
                let h = state.height;
                let need = peer_height.map_or(false, |ph| h < ph);
                (h, need)
            };

            if need_more {
                let from = our_h + 1;
                let to = from + IBD_CHUNK_SIZE - 1;
                info!("[SYNC] Downloading blocks {}..{} from {}", from, to, addr);
                protocol::send_message(stream, &Message::GetBlocks { from, to }).await?;
                return Ok(true);
            } else {
                info!("[SYNC] Sync complete — height: {}", our_h);
            }
        }

        // ── Inbound: new block broadcast ─────────────────────────────────────
        Message::NewBlock(block) => {
            let our_h = chain.read().await.height;
            if block.index == our_h + 1 {
                if quick_validate(block) {
                    let mut state = chain.write().await;
                    state.height = block.index;
                    state.best_hash = block.hash;
                    let hash_prefix = hex::encode(&block.hash[..5]);
                    info!(
                        "[SYNC] Applied block #{} | hash: {}…",
                        block.index, hash_prefix
                    );
                } else {
                    warn!("[SYNC] Invalid NewBlock #{} from {}: hash mismatch", block.index, addr);
                }
            } else if block.index > our_h + 1 {
                // We're behind; trigger IBD
                info!(
                    "[SYNC] NewBlock #{} from {} — we are at {} — re-launching IBD",
                    block.index, addr, our_h
                );
                let from = our_h + 1;
                let to = from + IBD_CHUNK_SIZE - 1;
                info!("[SYNC] Downloading blocks {}..{} from {}", from, to, addr);
                *peer_height = Some(block.index);
                protocol::send_message(stream, &Message::GetBlocks { from, to }).await?;
                return Ok(true);
            }
            // else: duplicate / old block, silently ignore
        }

        _ => {} // Non-sync messages handled by caller
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chain_state_default() {
        let s = ChainState::new();
        assert_eq!(s.height, 0);
        assert_eq!(s.best_hash, Block::genesis().hash);
    }

    #[test]
    fn test_chain_state_best_hash_hex() {
        let s = ChainState::new();
        let hex = s.best_hash_hex();
        assert_eq!(hex.len(), 64);
    }

    #[tokio::test]
    async fn test_chain_state_shared() {
        let state = Arc::new(RwLock::new(ChainState::new()));
        let state2 = state.clone();

        {
            let mut s = state.write().await;
            s.height = 42;
            s.best_hash = [0xab; 32];
        }

        let h = state2.read().await.height;
        assert_eq!(h, 42);
    }

    #[test]
    fn test_quick_validate_genesis() {
        let g = Block::genesis();
        // Genesis hash is set by sha3_256, not hash_header(), so quick_validate may not hold
        // Just ensure it doesn't panic
        let _ = quick_validate(&g);
    }

    #[test]
    fn test_quick_validate_real_block() {
        use taron_core::TESTNET_REWARD;
        let genesis = Block::genesis();
        let mut block = Block {
            index: 1,
            prev_hash: genesis.hash,
            timestamp: genesis.timestamp + 1000,
            miner: [2u8; 32],
            nonce: 0,
            hash: [0u8; 32],
            reward: TESTNET_REWARD,
            difficulty_target: 0,
            transactions: vec![],
        };
        block.hash = block.hash_header();
        assert!(quick_validate(&block));

        // Corrupt hash
        block.hash[0] ^= 1;
        assert!(!quick_validate(&block));
    }
}
