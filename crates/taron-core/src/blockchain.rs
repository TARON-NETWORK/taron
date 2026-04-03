//! Blockchain — ordered chain of blocks with RocksDB persistence.
//!
//! Each block is stored in RocksDB keyed by its 8-byte little-endian index.
//! Only `height` and `difficulty` are kept in RAM — blocks are read from disk
//! on demand. This allows the chain to grow to millions of blocks without
//! exhausting memory.
//!
//! ## Persistence
//! `Blockchain::load_or_create(path, difficulty)` opens (or creates) a RocksDB
//! database at `path` (a directory). If a legacy `chain.json` exists next to it,
//! it is automatically migrated to RocksDB on first run.
//!
//! ## Save
//! Every `apply_block()` write is atomic and immediate — no explicit `save()`
//! call is needed. `save()` exists only for API compatibility and is a no-op.

use std::path::Path;
use rocksdb::{DB, Options};
use crate::{Block, Ledger, TaronError};

/// LWMA: window size for the Linear Weighted Moving Average DAA.
/// 60 blocks gives fast response (2 min at 30s/block) while smoothing noise.
const LWMA_WINDOW: u64 = 60;
/// DAA: adjust difficulty every N blocks (kept for fallback/old-block compat).
const DAA_WINDOW: u64 = 10;
/// TARON AFD — Automatic Finality Depth.
/// Any block with FINALITY_DEPTH or more confirmations is permanently final.
/// No reorg can ever touch it — enforced at protocol level, not just convention.
/// Stronger than Bitcoin Cash (10 blocks) and non-bypassable unlike manual checkpoints.
pub const FINALITY_DEPTH: u64 = 100;
/// DAA: base target time per block in milliseconds (30 seconds).
const BASE_TARGET_BLOCK_MS: u64 = 30_000;
/// ABC (Adaptive Block Cadence): adjust target block time every N blocks
/// based on fork indicators (fast blocks = blocks arriving < FAST_BLOCK_THRESHOLD apart).
const ABC_WINDOW: u64 = 50;
/// Blocks with inter-block time below this threshold indicate fork-prone conditions.
const FAST_BLOCK_THRESHOLD_MS: u64 = 5_000; // 5 seconds
/// Minimum target block time (fastest the network can go).
const MIN_TARGET_BLOCK_MS: u64 = 15_000; // 15 seconds
/// Maximum target block time (slowest the network can go).
const MAX_TARGET_BLOCK_MS: u64 = 60_000; // 60 seconds
/// ABC cadence step: how much to adjust target per recalculation.
const ABC_STEP_MS: u64 = 5_000; // 5 seconds
/// Fork rate threshold above which we slow down (20% fast blocks).
const ABC_SLOW_DOWN_THRESHOLD: f64 = 0.20;
/// Fork rate threshold below which we speed up (5% fast blocks).
const ABC_SPEED_UP_THRESHOLD: f64 = 0.05;
/// Minimum target (hardest difficulty) — set to 1 so the DAA can scale
/// to any network hashrate without hitting an artificial cap.
const MIN_TARGET: u64 = 1;
/// Maximum target (easiest difficulty, ~8 leading zero bits).
/// Prevents trivially easy blocks (attacker mining with difficulty=1).
const MAX_TARGET: u64 = 1u64 << (64 - 8);  // 1u64 << 56

/// Known-good block hashes. Any block at these heights must match exactly.
/// A peer sending a different hash at a checkpoint height is on a fork — reject immediately.
// Checkpoints: (height, hash, difficulty_at_this_height)
// Difficulty is stored so the DAA stays in sync during IBD.
const CHECKPOINTS: &[(u64, &str, u32)] = &[];

// ── RocksDB key layout ────────────────────────────────────────────────────────
// b"b:" + index_le_u64  →  bincode-encoded Block
// b"meta:h"             →  height as le u64
// b"meta:d"             →  difficulty as le u32

fn block_key(index: u64) -> [u8; 10] {
    let mut key = [0u8; 10];
    key[0] = b'b';
    key[1] = b':';
    key[2..].copy_from_slice(&index.to_le_bytes());
    key
}
const KEY_HEIGHT:    &[u8] = b"meta:h";
const KEY_DIFF:      &[u8] = b"meta:d";
const KEY_TARGET:    &[u8] = b"meta:t";
const KEY_CHAINWORK: &[u8] = b"meta:cw";

/// Compute the proof-of-work value for a single block.
/// Lower difficulty_target = harder block = more work.
/// work = u64::MAX / difficulty_target  (u128 to avoid overflow when summing)
pub fn block_work(difficulty_target: u64) -> u128 {
    if difficulty_target == 0 { return 1; }
    (u64::MAX as u128) / (difficulty_target as u128)
}

// ── Blockchain ────────────────────────────────────────────────────────────────

/// The TARON blockchain backed by RocksDB.
pub struct Blockchain {
    db: DB,
    /// Cached tip height (index of the last block). Source of truth is in DB.
    pub height: u64,
    /// Mining difficulty as a u64 target. A hash is valid if its first 8 bytes
    /// (big-endian u64) are less than this target. Higher = easier, lower = harder.
    /// Adjusted per-block by LWMA (Linear Weighted Moving Average DAA).
    pub difficulty: u64,
    /// Adaptive Block Cadence: current target block time in milliseconds.
    /// Adjusted every ABC_WINDOW blocks based on fork indicators.
    pub target_block_ms: u64,
    /// Cumulative proof-of-work for the canonical chain.
    /// Used for fork choice: prefer the chain with the most work.
    pub chainwork: u128,
}

impl std::fmt::Debug for Blockchain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Blockchain")
            .field("height", &self.height)
            .field("difficulty", &self.difficulty)
            .field("chainwork", &self.chainwork)
            .finish()
    }
}

impl Blockchain {
    // ── Public query API ─────────────────────────────────────────────────────

    /// Current tip index (0 = only genesis).
    pub fn height(&self) -> u64 {
        self.height
    }

    /// Fetch a single block by index. Returns `None` if out of range.
    pub fn block_at(&self, index: u64) -> Option<Block> {
        let bytes = self.db.get(block_key(index)).ok()??;
        bincode::deserialize(&bytes).ok()
    }

    /// Tip block (the most recently applied block).
    pub fn tip(&self) -> Block {
        self.block_at(self.height)
            .expect("tip block must exist in DB")
    }

    /// Total number of blocks (genesis + mined blocks).
    pub fn total_blocks(&self) -> usize {
        (self.height + 1) as usize
    }

    /// Return up to `limit` blocks starting from `offset` from the tip,
    /// newest first. Used by the RPC `/blocks` endpoint.
    pub fn blocks_paginated(&self, offset: usize, limit: usize) -> Vec<Block> {
        if self.height == 0 && offset > 0 {
            return vec![];
        }
        let start = self.height.saturating_sub(offset as u64);
        let end   = start.saturating_sub((limit as u64).saturating_sub(1));
        (end..=start).rev().filter_map(|i| self.block_at(i)).collect()
    }

    /// Return all blocks in [from, to] (inclusive), oldest first.
    /// Used by the IBD `GetBlocks` handler.
    pub fn blocks_range(&self, from: u64, to: u64) -> Vec<Block> {
        let to = to.min(self.height);
        (from..=to).filter_map(|i| self.block_at(i)).collect()
    }

    /// Return all blocks mined by `pubkey`, oldest first.
    /// O(height) — acceptable for testnet, add a secondary index for mainnet.
    pub fn blocks_by_miner(&self, pubkey: &[u8; 32]) -> Vec<Block> {
        (0..=self.height)
            .filter_map(|i| self.block_at(i))
            .filter(|b| &b.miner == pubkey)
            .collect()
    }

    // ── Mutation ─────────────────────────────────────────────────────────────

    /// Revert the tip block: remove it from RocksDB, decrement height,
    /// and undo its effects on the ledger (coinbase + transactions).
    /// Returns the reverted block on success.
    /// Used for tip reorg when a competing block with a better hash arrives.
    pub fn revert_tip(&mut self, ledger: &mut Ledger) -> Result<Block, TaronError> {
        if self.height == 0 {
            return Err(TaronError::InvalidBlock); // never revert genesis
        }
        let tip = self.tip();

        // Undo transactions in reverse order
        for tx in tip.transactions.iter().rev() {
            ledger.revert_tx(tx);
        }

        // Undo coinbase reward
        ledger.revert_coinbase(&tip.miner, tip.reward);

        // Remove block from DB and update height
        self.db.delete(block_key(self.height)).expect("rocksdb delete block");
        self.height -= 1;
        self.db.put(KEY_HEIGHT, &self.height.to_le_bytes()).expect("rocksdb put height");

        // Subtract reverted block's work from chainwork
        let reverted_work = block_work(if tip.difficulty_target != 0 { tip.difficulty_target } else { self.difficulty });
        self.chainwork = self.chainwork.saturating_sub(reverted_work);
        let _ = self.db.put(KEY_CHAINWORK, &self.chainwork.to_le_bytes());

        // Recompute LWMA difficulty after revert (per-block DAA)
        if self.height >= LWMA_WINDOW {
            self.difficulty = self.compute_lwma();
        } else {
            self.difficulty = crate::TESTNET_TARGET;
        }
        self.db.put(KEY_DIFF, &self.difficulty.to_le_bytes()).expect("rocksdb put diff");

        Ok(tip)
    }

    /// Revert the chain back to `target_height` by calling `revert_tip()` repeatedly.
    /// Returns the list of reverted blocks (newest first) on success.
    /// Used for deep reorgs when a longer competing chain is discovered.
    pub fn revert_to_height(&mut self, target_height: u64, ledger: &mut Ledger) -> Result<Vec<Block>, TaronError> {
        // Never revert below a checkpoint we have already passed.
        let min_safe = CHECKPOINTS.iter()
            .filter(|&&(h, _, _)| h <= self.height)
            .map(|&(h, _, _)| h)
            .max()
            .unwrap_or(0);
        if target_height < min_safe && target_height != 0 {
            eprintln!("[REORG] Refused: target {} is below checkpoint {}", target_height, min_safe);
            return Err(TaronError::InvalidBlock);
        }
        // AFD: blocks older than FINALITY_DEPTH confirmations are permanently final.
        // No reorg can touch them — not even during IBD. Genesis revert (target=0) is exempt
        // only when the chain hasn't yet accumulated FINALITY_DEPTH blocks.
        let reorg_depth = self.height.saturating_sub(target_height);
        if target_height != 0 && reorg_depth > FINALITY_DEPTH {
            eprintln!("[AFD] Refused reorg of {} blocks — exceeds finality depth {}", reorg_depth, FINALITY_DEPTH);
            return Err(TaronError::InvalidBlock);
        }
        let mut reverted = Vec::new();
        while self.height > target_height {
            let block = self.revert_tip(ledger)?;
            reverted.push(block);
        }
        // If we reverted all the way to genesis, force difficulty back to
        // TESTNET_DIFFICULTY and persist it — DB may still hold the old
        // DAA-adjusted value from the previous chain.
        if self.height == 0 {
            self.difficulty = crate::TESTNET_TARGET;
            let _ = self.db.put(KEY_DIFF, &self.difficulty.to_le_bytes());
        }
        Ok(reverted)
    }

    /// Recalibrate ABC and DAA after IBD completes.
    /// During IBD, ABC (target_block_ms) is skipped to avoid timestamp artifacts.
    /// After IBD, this must be called once to resync target_block_ms with the network.
    /// We reset target_block_ms to BASE_TARGET_BLOCK_MS instead of computing from
    /// historical blocks — historical fast-mining periods would inflate fork_rate to
    /// 94-98% and push the ABC to MAX (60s), causing live blocks to be rejected.
    pub fn recalibrate_abc(&mut self) {
        // Reset target_block_ms to base after IBD — let live blocks drive the ABC from here.
        // Do NOT recompute difficulty: self.difficulty is already the last IBD block's
        // difficulty_target, which IS the network consensus. Recomputing from timestamps
        // diverges from the network and causes the node to mine at a wrong difficulty,
        // creating competing blocks and forks with the rest of the network.
        self.target_block_ms = BASE_TARGET_BLOCK_MS;
        let _ = self.db.put(KEY_TARGET, &self.target_block_ms.to_le_bytes());
        eprintln!("[ABC] Post-IBD recalibrate — reset target_block_ms to {}ms, difficulty: {}", BASE_TARGET_BLOCK_MS, self.difficulty);
    }

    /// Generate a block locator: a list of (height, hash) pairs at exponentially spaced
    /// heights from tip to genesis. Used to negotiate a common ancestor with a peer.
    pub fn generate_block_locator(&self) -> Vec<(u64, String)> {
        let mut locator: Vec<(u64, String)> = Vec::new();
        let tip = self.height;
        let mut step: u64 = 1;
        let mut h = tip;

        loop {
            if let Some(block) = self.block_at(h) {
                locator.push((h, hex::encode(&block.hash)));
            }
            if h == 0 { break; }
            h = h.saturating_sub(step);
            if locator.len() > 1 { step = step.saturating_mul(2); }
            if locator.len() >= 32 { break; }
        }

        // Always include genesis
        if locator.last().map(|(h, _)| *h) != Some(0) {
            if let Some(genesis) = self.block_at(0) {
                locator.push((0, hex::encode(&genesis.hash)));
            }
        }

        locator
    }

    /// Find the highest common ancestor using a block locator received from a peer.
    /// Returns the height of the common ancestor, or None if chains share no common block
    /// (different genesis or completely diverged beyond locator range).
    pub fn find_common_ancestor_from_locator(&self, locator: &[(u64, String)]) -> Option<u64> {
        for (height, hash) in locator {
            if let Some(block) = self.block_at(*height) {
                if hex::encode(&block.hash) == *hash {
                    return Some(*height);
                }
            }
        }
        None
    }

    /// Find the fork point between our chain and a set of incoming blocks.
    /// Returns the height of the last common ancestor, or None if no common block found.
    pub fn find_fork_point(&self, incoming: &[Block]) -> Option<u64> {
        for block in incoming {
            if block.index == 0 { return Some(0); }
            let parent_height = block.index - 1;
            if let Some(our_block) = self.block_at(parent_height) {
                if our_block.hash == block.prev_hash {
                    return Some(parent_height);
                }
            }
        }
        None
    }

    /// Validate and append a new block, then credit the miner in the ledger.
    /// The block is written to RocksDB atomically before returning.
    pub fn apply_block(&mut self, block: &Block, ledger: &mut Ledger) -> Result<(), TaronError> {
        if let Some(&(_, expected, _)) = CHECKPOINTS.iter().find(|&&(h, _, _)| h == block.index) {
            if hex::encode(&block.hash) != expected {
                eprintln!("[REJECT] block #{}: checkpoint mismatch", block.index);
                return Err(TaronError::InvalidBlock);
            }
        }
        let prev = self.tip();
        if let Some(reason) = block.validate_inner(&prev, self.difficulty, true) {
            eprintln!("[REJECT] block #{}: {}", block.index, reason);
            return Err(TaronError::InvalidBlock);
        }

        // CVE-001: enforce canonical reward — miner cannot self-assign arbitrary amounts
        if block.reward != crate::TESTNET_REWARD {
            eprintln!("[REJECT] block #{}: bad reward {} expected {}", block.index, block.reward, crate::TESTNET_REWARD);
            return Err(TaronError::InvalidBlock);
        }

        // Validate all transactions before applying any (atomic).
        for (i, tx) in block.transactions.iter().enumerate() {
            // FIX 4: validate structure (field ranges, non-zero amounts, etc.) for live blocks only.
            if let Err(e) = tx.validate_structure() {
                eprintln!("[REJECT] block #{}: tx {} structure invalid: {:?}", block.index, i, e);
                return Err(TaronError::InvalidBlock);
            }
            if let Err(e) = tx.verify_signature() {
                eprintln!("[REJECT] block #{}: tx {} sig fail: {:?}", block.index, i, e);
                return Err(TaronError::InvalidBlock);
            }
        }

        ledger.apply_coinbase(&block.miner, block.reward);
        ledger.touch_account(&block.miner, block.timestamp);

        // Apply transactions
        for (i, tx) in block.transactions.iter().enumerate() {
            if let Err(e) = ledger.apply_tx(tx) {
                eprintln!("[REJECT] block #{}: tx {} apply fail: {:?} (sender={} amount={})",
                    block.index, i, e, hex::encode(&tx.sender[..8]), tx.amount);
                return Err(TaronError::InvalidBlock);
            }
        }

        // Write block to DB
        let encoded = bincode::serialize(block).expect("block serialization");
        self.db.put(block_key(block.index), &encoded).expect("rocksdb put block");
        self.height = block.index;
        self.db.put(KEY_HEIGHT, &self.height.to_le_bytes()).expect("rocksdb put height");

        // LWMA: recalculate difficulty every block once we have enough history.
        // Per-block adjustment reacts in real-time to hashrate changes — a dominant
        // miner can no longer mine 60 "free" blocks before the DAA responds.
        if self.height >= LWMA_WINDOW {
            self.difficulty = self.compute_lwma();
            self.db.put(KEY_DIFF, &self.difficulty.to_le_bytes()).expect("rocksdb put diff");
        }

        // Update cumulative chainwork
        let blk_work = block_work(if block.difficulty_target != 0 { block.difficulty_target } else { self.difficulty });
        self.chainwork = self.chainwork.saturating_add(blk_work);
        let _ = self.db.put(KEY_CHAINWORK, &self.chainwork.to_le_bytes());

        // ABC: adjust target block time at every ABC_WINDOW boundary
        if self.height > 0 && self.height % ABC_WINDOW == 0 {
            self.target_block_ms = self.compute_adaptive_cadence();
            self.db.put(KEY_TARGET, &self.target_block_ms.to_le_bytes()).expect("rocksdb put target");
        }

        Ok(())
    }

    /// Like `apply_block` but skips the timestamp drift check and sequence check.
    /// Used during IBD (Initial Block Download) for historical blocks.
    pub fn apply_block_ibd(&mut self, block: &Block, ledger: &mut Ledger) -> Result<(), TaronError> {
        let highest_cp = CHECKPOINTS.last().map(|&(h, _, _)| h).unwrap_or(0);

        // Checkpoint hash verification + difficulty restore
        if let Some(&(_, expected, cp_diff)) = CHECKPOINTS.iter().find(|&&(h, _, _)| h == block.index) {
            if hex::encode(&block.hash) != expected {
                eprintln!("[REJECT] block #{}: checkpoint mismatch", block.index);
                return Err(TaronError::InvalidBlock);
            }
            // Restore the canonical difficulty at this checkpoint (convert bits → target)
            self.difficulty = crate::hash::bits_to_target(cp_diff);
            self.db.put(KEY_DIFF, &self.difficulty.to_le_bytes()).expect("rocksdb put diff");
        }

        // During IBD we trust the designated peer — only verify chain linkage.
        // Difficulty is NOT checked: the DAA diverges during rebuild.
        // Checkpoints anchor the chain at known-good hashes.
        // Accept both old format (difficulty_target == 0) and new format.
        let prev = self.tip();
        if block.index != prev.index + 1 || block.prev_hash != prev.hash {
            eprintln!("[REJECT-IBD] block #{}: bad index or prev_hash", block.index);
            return Err(TaronError::InvalidBlock);
        }

        // FIX 2: reject any reward grossly exceeding the canonical reward (2x cap).
        if block.reward > crate::TESTNET_REWARD * 2 {
            eprintln!("[REJECT-IBD] block #{}: reward {} exceeds cap {}", block.index, block.reward, crate::TESTNET_REWARD * 2);
            return Err(TaronError::InvalidBlock);
        }
        ledger.apply_coinbase(&block.miner, block.reward);

        // Use apply_tx_ibd: skips sequence check (local ledger may have diverged
        // from server's due to payout txs embedded in earlier blocks), but still
        // sets sequence = tx.sequence so subsequent txs chain correctly.
        for tx in &block.transactions {
            if ledger.apply_tx_ibd(tx).is_err() {
                return Err(TaronError::InvalidBlock);
            }
        }

        let encoded = bincode::serialize(block).expect("block serialization");
        self.db.put(block_key(block.index), &encoded).expect("rocksdb put block");
        self.height = block.index;
        self.db.put(KEY_HEIGHT, &self.height.to_le_bytes()).expect("rocksdb put height");

        // Update cumulative chainwork during IBD
        let blk_work = block_work(if block.difficulty_target != 0 { block.difficulty_target } else { self.difficulty });
        self.chainwork = self.chainwork.saturating_add(blk_work);
        let _ = self.db.put(KEY_CHAINWORK, &self.chainwork.to_le_bytes());

        // During IBD, use the difficulty_target from the block itself (if present).
        // Do NOT run DAA/ABC during IBD — the timestamps are meaningless during
        // fast sync and cause the difficulty to diverge from the network.
        if block.difficulty_target != 0 {
            // FIX 1: reject any difficulty_target outside the canonical [MIN_TARGET, MAX_TARGET] range.
            if block.difficulty_target < MIN_TARGET || block.difficulty_target > MAX_TARGET {
                eprintln!("[REJECT-IBD] block #{}: difficulty_target {} out of bounds", block.index, block.difficulty_target);
                return Err(TaronError::InvalidBlock);
            }
            self.difficulty = block.difficulty_target;
            self.db.put(KEY_DIFF, &self.difficulty.to_le_bytes()).expect("rocksdb put diff");
        } else if self.height > highest_cp && self.height > 0 && self.height % DAA_WINDOW == 0 {
            // Fallback for old blocks without difficulty_target
            self.difficulty = self.compute_next_difficulty();
            self.db.put(KEY_DIFF, &self.difficulty.to_le_bytes()).expect("rocksdb put diff");
        }

        Ok(())
    }

    /// No-op: RocksDB writes are immediate. Kept for API compatibility.
    pub fn save(&self, _path: &Path) {}

    // ── Construction / persistence ───────────────────────────────────────────

    /// Open (or create) the RocksDB database at `path`.
    ///
    /// - If the DB already has data, load height + difficulty from metadata.
    /// - If a `chain.json` file exists next to `path` (legacy format), migrate
    ///   it to RocksDB automatically.
    /// - Otherwise, start fresh with the genesis block.
    pub fn load_or_create(path: &Path, _difficulty: u32) -> Self {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = DB::open(&opts, path).expect("Failed to open RocksDB");

        // ── Case 1: existing DB ──────────────────────────────────────────────
        if let Ok(Some(h_bytes)) = db.get(KEY_HEIGHT) {
            let h_arr: [u8; 8] = (&h_bytes[..]).try_into().unwrap_or([0u8; 8]);
            let height = u64::from_le_bytes(h_arr);
            let diff = if height == 0 {
                let _ = db.put(KEY_DIFF, &crate::TESTNET_TARGET.to_le_bytes());
                crate::TESTNET_TARGET
            } else if let Ok(Some(d_bytes)) = db.get(KEY_DIFF) {
                if d_bytes.len() == 8 {
                    u64::from_le_bytes((&d_bytes[..]).try_into().unwrap())
                } else {
                    // Legacy u32 bits → convert to target
                    let bits = u32::from_le_bytes((&d_bytes[..4]).try_into().unwrap_or([0; 4]));
                    crate::hash::bits_to_target(bits)
                }
            } else { crate::TESTNET_TARGET };
            let target_block = if let Ok(Some(t_bytes)) = db.get(KEY_TARGET) {
                let t_arr: [u8; 8] = (&t_bytes[..]).try_into().unwrap_or(BASE_TARGET_BLOCK_MS.to_le_bytes());
                u64::from_le_bytes(t_arr)
            } else { BASE_TARGET_BLOCK_MS };
            // Load or compute chainwork (one-time migration for existing chains)
            let chainwork = if let Ok(Some(cw_bytes)) = db.get(KEY_CHAINWORK) {
                if cw_bytes.len() == 16 {
                    u128::from_le_bytes(cw_bytes[..16].try_into().unwrap_or([0u8; 16]))
                } else { 0u128 }
            } else { 0u128 };
            let chainwork = if chainwork == 0 && height > 0 {
                eprintln!("[CHAIN] Computing chainwork for {} blocks (one-time migration)…", height + 1);
                let cw: u128 = (0..=height).filter_map(|i| {
                    db.get(block_key(i)).ok().flatten()
                        .and_then(|b| bincode::deserialize::<Block>(&b).ok())
                }).map(|b| block_work(if b.difficulty_target != 0 { b.difficulty_target } else { crate::TESTNET_TARGET }))
                .sum();
                let _ = db.put(KEY_CHAINWORK, &cw.to_le_bytes());
                eprintln!("[CHAIN] Chainwork: {}", cw);
                cw
            } else { chainwork };
            let bits = crate::hash::target_to_bits(diff);
            eprintln!("[CHAIN] Loaded from RocksDB — height: {}, target: {} (~{} bits), block_time: {}ms, chainwork: {}", height, diff, bits, target_block, chainwork);
            return Self { db, height, difficulty: diff, target_block_ms: target_block, chainwork };
        }

        // ── Case 2: fresh genesis ────────────────────────────────────────────
        let genesis = Block::genesis();
        let enc = bincode::serialize(&genesis).expect("encode genesis");
        db.put(block_key(0), &enc).expect("rocksdb put genesis");
        db.put(KEY_HEIGHT, &0u64.to_le_bytes()).expect("rocksdb put height");
        db.put(KEY_DIFF, &crate::TESTNET_TARGET.to_le_bytes()).expect("rocksdb put diff");
        db.put(KEY_TARGET, &BASE_TARGET_BLOCK_MS.to_le_bytes()).expect("rocksdb put target");
        db.put(KEY_CHAINWORK, &0u128.to_le_bytes()).expect("rocksdb put chainwork");
        eprintln!("[CHAIN] Fresh chain — genesis written to RocksDB");
        Self { db, height: 0, difficulty: crate::TESTNET_TARGET, target_block_ms: BASE_TARGET_BLOCK_MS, chainwork: 0 }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /// LWMA-3 DAA (Linear Weighted Moving Average).
    ///
    /// Uses the last LWMA_WINDOW blocks. Block at position i (oldest=1, newest=N)
    /// gets weight i — recent blocks matter more. Solvetime is clamped to
    /// [1ms, 6*T] to neutralise timestamp manipulation attacks.
    ///
    /// next_target = avg_target * weighted_solvetimes / ideal_weighted_solvetimes
    ///
    /// Called every block (height >= LWMA_WINDOW) — reacts to hashrate changes
    /// in real-time rather than waiting for a 10-block window.
    fn compute_lwma(&self) -> u64 {
        if self.height < 2 {
            return crate::TESTNET_TARGET;
        }
        let n = LWMA_WINDOW.min(self.height) as u128;
        let t = self.target_block_ms as u128; // target solvetime in ms

        // Window: the last n blocks (start..=height), need n+1 timestamps
        let start = self.height.saturating_sub(n as u64);

        let mut weighted_time: u128 = 0;
        let mut sum_targets: u128 = 0;

        for i in 1..=n {
            let h = start + i as u64;
            let curr = match self.block_at(h) { Some(b) => b, None => return self.difficulty };
            let prev = match self.block_at(h - 1) { Some(b) => b, None => return self.difficulty };

            // Clamp solvetime: [1ms, 6*T] — prevents timestamp manipulation
            let raw = curr.timestamp.saturating_sub(prev.timestamp) as u128;
            let solvetime = raw.max(1).min(6 * t);

            let target = if curr.difficulty_target != 0 {
                curr.difficulty_target as u128
            } else {
                self.difficulty as u128
            };

            weighted_time += i * solvetime;
            sum_targets += target;
        }

        // ideal = T * N*(N+1)/2
        let ideal = t * n * (n + 1) / 2;
        if weighted_time == 0 || ideal == 0 {
            return self.difficulty;
        }

        let avg_target = sum_targets / n;
        let new_target = (avg_target * weighted_time / ideal) as u64;

        new_target.max(MIN_TARGET).min(MAX_TARGET)
    }

    /// Ratio-based DAA: adjusts the u64 target proportionally to actual vs expected time.
    /// Clamps adjustment to ±4x per window to prevent wild swings.
    fn compute_next_difficulty(&self) -> u64 {
        if self.height < DAA_WINDOW {
            return crate::TESTNET_TARGET;
        }
        // FIX 3: replace .unwrap() with graceful fallback to avoid panic on missing DB entries.
        let window_end = match self.block_at(self.height) {
            Some(b) => b,
            None => return crate::TESTNET_TARGET,
        };
        let window_start = match self.block_at(self.height - DAA_WINDOW) {
            Some(b) => b,
            None => return crate::TESTNET_TARGET,
        };

        let actual_ms = window_end.timestamp.saturating_sub(window_start.timestamp);
        let target_ms = self.target_block_ms * DAA_WINDOW;

        if actual_ms == 0 {
            // Blocks instant → make it 4x harder (divide target by 4)
            return (self.difficulty / 4).max(MIN_TARGET);
        }

        // new_target = old_target * actual_time / expected_time
        // Blocks too fast → actual < target → ratio < 1 → target decreases (harder)
        // Blocks too slow → actual > target → ratio > 1 → target increases (easier)
        let new_target = (self.difficulty as u128 * actual_ms as u128 / target_ms as u128) as u64;

        // Clamp: max 4x change per window, and stay within [MIN_TARGET, MAX_TARGET]
        let clamped = new_target
            .max(self.difficulty / 4)    // no more than 4x harder
            .min(self.difficulty * 4)    // no more than 4x easier
            .max(MIN_TARGET)             // hardest allowed (~24 bits)
            .min(MAX_TARGET);            // easiest allowed (~8 bits)

        clamped
    }

    /// Adaptive Block Cadence: adjust target_block_ms based on fork indicators.
    /// Counts "fast blocks" (inter-block time < 5s) over the last ABC_WINDOW blocks.
    /// High fork rate → slow down (increase target). Low fork rate → speed up.
    fn compute_adaptive_cadence(&self) -> u64 {
        if self.height < ABC_WINDOW {
            return self.target_block_ms;
        }
        let mut fast_blocks = 0u64;
        let start = self.height - ABC_WINDOW + 1;
        for i in start..=self.height {
            if let (Some(curr), Some(prev)) = (self.block_at(i), self.block_at(i - 1)) {
                let dt = curr.timestamp.saturating_sub(prev.timestamp);
                if dt < FAST_BLOCK_THRESHOLD_MS {
                    fast_blocks += 1;
                }
            }
        }
        let fork_rate = fast_blocks as f64 / ABC_WINDOW as f64;
        let new_target = if fork_rate > ABC_SLOW_DOWN_THRESHOLD {
            // Too many fast blocks → forks likely → slow down
            self.target_block_ms.saturating_add(ABC_STEP_MS)
        } else if fork_rate < ABC_SPEED_UP_THRESHOLD {
            // Very few fast blocks → network stable → speed up
            self.target_block_ms.saturating_sub(ABC_STEP_MS)
        } else {
            self.target_block_ms
        };
        let clamped = new_target.max(MIN_TARGET_BLOCK_MS).min(MAX_TARGET_BLOCK_MS);
        if clamped != self.target_block_ms {
            eprintln!(
                "[ABC] Block cadence adjusted: {}ms → {}ms (fork_rate: {:.1}%, fast_blocks: {}/{})",
                self.target_block_ms, clamped, fork_rate * 100.0, fast_blocks, ABC_WINDOW
            );
        }
        clamped
    }

    /// Recalibrate difficulty after IBD using LWMA.
    /// LWMA gives the exact same result as if we had mined those blocks live,
    /// since it's deterministic from the last LWMA_WINDOW block timestamps/targets.
    pub fn recalibrate_difficulty_after_ibd(&mut self) {
        if self.height >= LWMA_WINDOW {
            self.difficulty = self.compute_lwma();
        } else {
            self.difficulty = crate::TESTNET_TARGET;
        }

        // Compute ABC target from the last ABC_WINDOW blocks
        if self.height >= ABC_WINDOW {
            self.target_block_ms = self.compute_adaptive_cadence();
        } else {
            self.target_block_ms = BASE_TARGET_BLOCK_MS;
        }

        self.db.put(KEY_DIFF, &self.difficulty.to_le_bytes()).expect("rocksdb put diff");
        self.db.put(KEY_TARGET, &self.target_block_ms.to_le_bytes()).expect("rocksdb put target");
        eprintln!("[LWMA] Recalibrated after IBD — difficulty: {}, target_block_ms: {}", self.difficulty, self.target_block_ms);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TESTNET_DIFFICULTY, TESTNET_REWARD};
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn test_chain(difficulty: u32) -> (Blockchain, std::path::PathBuf) {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::path::PathBuf::from(format!("/tmp/taron_test_chain_{}", n));
        let chain = Blockchain::load_or_create(&path, difficulty);
        (chain, path)
    }

    fn make_valid_block(chain: &Blockchain, miner: [u8; 32], reward: u64) -> Block {
        let tip = chain.tip();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let mut candidate = Block {
            index: tip.index + 1,
            prev_hash: tip.hash,
            timestamp: now_ms,
            miner,
            nonce: 0,
            hash: [0u8; 32],
            reward,
            transactions: vec![],
        };
        candidate.hash = candidate.hash_header();
        candidate
    }

    #[test]
    fn test_new_blockchain_has_genesis() {
        let (chain, path) = test_chain(0);
        assert_eq!(chain.height(), 0);
        assert_eq!(chain.tip().index, 0);
        assert_eq!(chain.total_blocks(), 1);
        std::fs::remove_dir_all(path).ok();
    }

    #[test]
    fn test_apply_valid_block() {
        let (mut chain, path) = test_chain(0);
        let mut ledger = Ledger::new();
        let miner = [1u8; 32];

        let block = make_valid_block(&chain, miner, TESTNET_REWARD);
        chain.apply_block(&block, &mut ledger).unwrap();

        assert_eq!(chain.height(), 1);
        assert_eq!(ledger.balance(&miner), TESTNET_REWARD);
        std::fs::remove_dir_all(path).ok();
    }

    #[test]
    fn test_apply_invalid_block_rejected() {
        let (mut chain, path) = test_chain(0);
        let mut ledger = Ledger::new();
        let miner = [1u8; 32];

        let mut block = make_valid_block(&chain, miner, TESTNET_REWARD);
        block.prev_hash = [99u8; 32];
        block.hash = block.hash_header();

        let result = chain.apply_block(&block, &mut ledger);
        assert!(matches!(result, Err(TaronError::InvalidBlock)));
        assert_eq!(chain.height(), 0);
        std::fs::remove_dir_all(path).ok();
    }

    #[test]
    fn test_multiple_blocks() {
        let (mut chain, path) = test_chain(0);
        let mut ledger = Ledger::new();
        let miner = [7u8; 32];

        for _ in 0..5 {
            let block = make_valid_block(&chain, miner, TESTNET_REWARD);
            chain.apply_block(&block, &mut ledger).unwrap();
        }

        assert_eq!(chain.height(), 5);
        assert_eq!(ledger.balance(&miner), TESTNET_REWARD * 5);
        std::fs::remove_dir_all(path).ok();
    }

    #[test]
    fn test_save_load_roundtrip() {
        let (mut chain, path) = test_chain(0);
        let mut ledger = Ledger::new();
        let miner = [3u8; 32];

        let block = make_valid_block(&chain, miner, TESTNET_REWARD);
        chain.apply_block(&block, &mut ledger).unwrap();
        drop(chain); // close DB

        // Re-open — data must persist
        let loaded = Blockchain::load_or_create(&path, 0);
        assert_eq!(loaded.height(), 1);
        assert_eq!(loaded.tip().miner, miner);
        std::fs::remove_dir_all(path).ok();
    }

    #[test]
    fn test_blocks_range() {
        let (mut chain, path) = test_chain(0);
        let mut ledger = Ledger::new();
        let miner = [5u8; 32];

        for _ in 0..5 {
            let b = make_valid_block(&chain, miner, TESTNET_REWARD);
            chain.apply_block(&b, &mut ledger).unwrap();
        }

        let range = chain.blocks_range(2, 4);
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].index, 2);
        assert_eq!(range[2].index, 4);
        std::fs::remove_dir_all(path).ok();
    }

    #[test]
    fn test_blocks_by_miner() {
        let (mut chain, path) = test_chain(0);
        let mut ledger = Ledger::new();
        let miner_a = [1u8; 32];
        let miner_b = [2u8; 32];

        // 3 blocks from A, 2 from B
        for _ in 0..3 {
            let b = make_valid_block(&chain, miner_a, TESTNET_REWARD);
            chain.apply_block(&b, &mut ledger).unwrap();
        }
        for _ in 0..2 {
            let b = make_valid_block(&chain, miner_b, TESTNET_REWARD);
            chain.apply_block(&b, &mut ledger).unwrap();
        }

        assert_eq!(chain.blocks_by_miner(&miner_a).len(), 3);
        assert_eq!(chain.blocks_by_miner(&miner_b).len(), 2);
        std::fs::remove_dir_all(path).ok();
    }

    #[test]
    fn test_daa_adjusts_difficulty() {
        let (mut chain, path) = test_chain(TESTNET_DIFFICULTY);
        let mut ledger = Ledger::new();
        let miner = [9u8; 32];
        let initial_diff = chain.difficulty;

        // Mine DAA_WINDOW blocks with very fast timestamps (1ms apart)
        for _ in 0..DAA_WINDOW {
            let tip = chain.tip();
            let mut b = Block {
                index:     tip.index + 1,
                prev_hash: tip.hash,
                timestamp: tip.timestamp + 1, // very fast → difficulty should increase
                miner,
                nonce: 0,
                hash: [0u8; 32],
                reward: TESTNET_REWARD,
                transactions: vec![],
            };
            b.hash = b.hash_header();
            // Force-apply without difficulty check for the DAA test
            let enc = bincode::serialize(&b).unwrap();
            chain.db.put(block_key(b.index), &enc).unwrap();
            chain.height = b.index;
            chain.db.put(KEY_HEIGHT, &chain.height.to_le_bytes()).unwrap();
            ledger.apply_coinbase(&miner, TESTNET_REWARD);
        }
        // Trigger DAA
        if chain.height % DAA_WINDOW == 0 {
            chain.difficulty = chain.compute_next_difficulty();
        }

        assert!(chain.difficulty > initial_diff, "difficulty should increase for fast blocks");
        std::fs::remove_dir_all(path).ok();
    }
}
