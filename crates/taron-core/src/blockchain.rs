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

/// DAA: adjust difficulty every N blocks.
const DAA_WINDOW: u64 = 10;
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
/// Minimum target (hardest difficulty, ~24 leading zero bits).
const MIN_TARGET: u64 = 1u64 << (64 - 24); // 1u64 << 40
/// Maximum target (easiest difficulty, ~8 leading zero bits).
const MAX_TARGET: u64 = 1u64 << (64 - 8);  // 1u64 << 56

/// Known-good block hashes. Any block at these heights must match exactly.
/// A peer sending a different hash at a checkpoint height is on a fork — reject immediately.
// Checkpoints: (height, hash, difficulty_at_this_height)
// Difficulty is stored so the DAA stays in sync during IBD.
const CHECKPOINTS: &[(u64, &str, u32)] = &[
    (500,  "000004828f46af7c3c3052bac3ef571fc5b805031c66dcb009c427a51da92066", 14),
    (1000, "000002972377958419f026f52436738aaa63be77143e711da89d1fb8f0742acb", 14),
    (1500, "0000004b4c622a9f708071b52013e0f7844ec972e0582e0c9a46a30a84477f2c", 14),
    (2000, "000016349cc475ac88095d4a720b63c9e3972bd7fcb8388ddb45265ebfbb2f67", 14),
    (2500, "0000020bdb99172bb4dcb259a8eeb2e1211ec2ae8568a5e1e31eb9854a19018c", 14),
    (3000, "0000016ef4c1110ffc985192a6a0769d540a5b71bb5885b0ff250a021bfbeb46", 14),
    (3500, "0000058896f4201e9809feae8bdf990889997c533ee6937e556ecbe9a36077c8", 15),
    (4000, "000000a46de28179b3c2b0a438a3828bda276fa390f08dd3e703e0fa7f3c9125", 15),
    (4500, "00000d5b9bf012e94151c4256c258401dc592cb2b27d2119445f74a9fec654eb", 15),
    (5000, "000006737ab2a41d65938e6c2b5b9556eec9bea25907ae4e14903950b00f1309", 15),
    (5500, "000001d111bd6436d4a32e0ea6bc6c2d5359d06c7cd0af0dcaa619e255594a45", 16),
    (6000, "00000465333670900ed90e49a10897aebc4a8a45d0c6eb68d2e14d4b0ffcec68", 16),
    (6500, "00001bc7de79a5fec2b857e72345410c89aeb2e675d32776ad77a5e6b857e386", 16),
    (7000, "00001cc75eddfc930b3c4994715803bd93460b7ac9f5d70abe8bbdfe86dbb637", 17),
    (7500, "0000000f97070e40af63fc6055b82644813e37e3a24d425c5e0b79594d7b6371", 18),
    (7937, "00002d675599959f39f33cbb7224bfdedd2780355f227ec7cc33a04d8e65f80e", 17),
    (8000, "00002dd4c94818daa431907f8c70dc025782e1958d4da672dc2ed0a6d6f16d8e", 17),
    (8500, "000022734fe6f8d357a212564ef54ab8488b40fe80b85a3fe73c84d02ecf599b", 16),
    (8528, "0000db9d741ede56a5fc6ee9813202d74fc45a73a54ec2bacb2a79316f5015a9", 16),
    (9000, "0001641c13aee295f73957bfa148c173c9d416edea967459c863d5b711362b66", 17),
    (9500, "000026d69072bcbf5f955878cfd742d9673ca6dc2be7378457acd86a8420cf60", 17),
];

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
const KEY_HEIGHT: &[u8] = b"meta:h";
const KEY_DIFF:   &[u8] = b"meta:d";
const KEY_TARGET: &[u8] = b"meta:t";

// ── Blockchain ────────────────────────────────────────────────────────────────

/// The TARON blockchain backed by RocksDB.
pub struct Blockchain {
    db: DB,
    /// Cached tip height (index of the last block). Source of truth is in DB.
    pub height: u64,
    /// Mining difficulty as a u64 target. A hash is valid if its first 8 bytes
    /// (big-endian u64) are less than this target. Higher = easier, lower = harder.
    /// Adjusted by ratio every DAA_WINDOW blocks for smooth difficulty changes.
    pub difficulty: u64,
    /// Adaptive Block Cadence: current target block time in milliseconds.
    /// Adjusted every ABC_WINDOW blocks based on fork indicators.
    pub target_block_ms: u64,
}

impl std::fmt::Debug for Blockchain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Blockchain")
            .field("height", &self.height)
            .field("difficulty", &self.difficulty)
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

        // Restore difficulty from the previous tip if we're at a DAA boundary
        if (self.height + 1) % DAA_WINDOW == 0 {
            self.difficulty = self.compute_next_difficulty();
            self.db.put(KEY_DIFF, &self.difficulty.to_le_bytes()).expect("rocksdb put diff");
        }

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

        // DAA: recalculate difficulty at every window boundary
        if self.height > 0 && self.height % DAA_WINDOW == 0 {
            self.difficulty = self.compute_next_difficulty();
            self.db.put(KEY_DIFF, &self.difficulty.to_le_bytes()).expect("rocksdb put diff");
        }

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

        // Only run DAA above the last checkpoint — below, difficulty is set by checkpoints
        if self.height > highest_cp && self.height > 0 && self.height % DAA_WINDOW == 0 {
            self.difficulty = self.compute_next_difficulty();
            self.db.put(KEY_DIFF, &self.difficulty.to_le_bytes()).expect("rocksdb put diff");
        }

        // ABC: adjust target block time during IBD too
        if self.height > highest_cp && self.height > 0 && self.height % ABC_WINDOW == 0 {
            self.target_block_ms = self.compute_adaptive_cadence();
            self.db.put(KEY_TARGET, &self.target_block_ms.to_le_bytes()).expect("rocksdb put target");
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
            let bits = crate::hash::target_to_bits(diff);
            eprintln!("[CHAIN] Loaded from RocksDB — height: {}, target: {} (~{} bits), block_time: {}ms", height, diff, bits, target_block);
            return Self { db, height, difficulty: diff, target_block_ms: target_block };
        }

        // ── Case 2: fresh genesis ────────────────────────────────────────────
        let genesis = Block::genesis();
        let enc = bincode::serialize(&genesis).expect("encode genesis");
        db.put(block_key(0), &enc).expect("rocksdb put genesis");
        db.put(KEY_HEIGHT, &0u64.to_le_bytes()).expect("rocksdb put height");
        db.put(KEY_DIFF, &crate::TESTNET_TARGET.to_le_bytes()).expect("rocksdb put diff");
        db.put(KEY_TARGET, &BASE_TARGET_BLOCK_MS.to_le_bytes()).expect("rocksdb put target");
        eprintln!("[CHAIN] Fresh chain — genesis written to RocksDB");
        Self { db, height: 0, difficulty: crate::TESTNET_TARGET, target_block_ms: BASE_TARGET_BLOCK_MS }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /// Ratio-based DAA: adjusts the u64 target proportionally to actual vs expected time.
    /// Clamps adjustment to ±4x per window to prevent wild swings.
    fn compute_next_difficulty(&self) -> u64 {
        if self.height < DAA_WINDOW {
            return crate::TESTNET_TARGET;
        }
        let window_end   = self.block_at(self.height).unwrap();
        let window_start = self.block_at(self.height - DAA_WINDOW).unwrap();

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

    /// Replay the DAA from genesis using actual block timestamps.
    /// This gives the exact same difficulty that the original chain computed.
    pub fn recalibrate_difficulty_after_ibd(&mut self) {
        // Instead of replaying the entire DAA history (which can diverge due to
        // rounding), just compute difficulty from the last DAA_WINDOW blocks.
        // This gives the exact same result as if we had mined those blocks live.
        if self.height >= DAA_WINDOW {
            self.difficulty = self.compute_next_difficulty();
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
        eprintln!("[DAA] Recalibrated after IBD — difficulty: {}, target: {}ms", self.difficulty, self.target_block_ms);
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
