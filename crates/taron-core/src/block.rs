//! Block structure for the TARON blockchain.
//!
//! Each block contains:
//! - Chain position (index, prev_hash)
//! - Mining metadata (miner pubkey, nonce, timestamp)
//! - Mining reward in µTAR
//! - Block hash (computed from all other fields)
//!
//! ## Hash computation
//! `Block::hash_header()` runs SEQUAL-256 (fast variant) over the canonical
//! byte encoding of all fields **except** `hash` itself. This is what miners
//! iterate over when searching for a valid nonce.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::hash::{Sequal256, MINING_STEPS, sha3_256};
use crate::meets_target;
use crate::Transaction;

/// Maximum timestamp drift allowed for a block (±2 minutes).
pub const BLOCK_TIMESTAMP_TOLERANCE_MS: u64 = 120_000;

/// A TARON block.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Block {
    /// Chain height (0 = genesis).
    pub index: u64,
    /// Hash of the previous block (all zeros for genesis).
    pub prev_hash: [u8; 32],
    /// Unix timestamp in milliseconds.
    pub timestamp: u64,
    /// Ed25519 public key of the miner who found this block.
    pub miner: [u8; 32],
    /// Proof-of-work nonce.
    pub nonce: u64,
    /// SEQUAL-256 hash of the block header (all fields except this one).
    pub hash: [u8; 32],
    /// Mining reward credited to the miner, in µTAR.
    pub reward: u64,
    /// Difficulty target for this block (u64). Hash must be < this value.
    /// Stored in the header so all nodes agree on the exact difficulty per block.
    #[serde(default)]
    pub difficulty_target: u64,
    /// Transactions included in this block (empty for coinbase-only blocks).
    #[serde(default)]
    pub transactions: Vec<Transaction>,
}

impl Block {
    /// Compute the block hash from all fields except `hash`.
    ///
    /// This is the value that must satisfy the difficulty target for the block
    /// to be considered valid. During mining, callers increment `nonce` and
    /// call this until `meets_difficulty()` returns true.
    pub fn hash_header(&self) -> [u8; 32] {
        let mut data = Vec::with_capacity(8 + 32 + 8 + 32 + 8 + 8);
        data.extend_from_slice(&self.index.to_le_bytes());
        data.extend_from_slice(&self.prev_hash);
        data.extend_from_slice(&self.timestamp.to_le_bytes());
        data.extend_from_slice(&self.miner);
        data.extend_from_slice(&self.nonce.to_le_bytes());
        data.extend_from_slice(&self.reward.to_le_bytes());
        data.extend_from_slice(&self.difficulty_target.to_le_bytes());
        Sequal256::hash_fast(&data, MINING_STEPS)
    }

    /// Return the hardcoded genesis block (index = 0).
    ///
    /// The genesis block has a fixed timestamp (2026-03-03 18:00:00 UTC in ms),
    /// zero prev_hash, zero miner, zero reward, and its hash is derived from
    /// SHA3-256 of the constant sentinel string so it never changes.
    pub fn genesis() -> Self {
        // Sentinel: deterministic hash for genesis (not mined, no difficulty check)
        let genesis_hash = sha3_256(b"TARON_GENESIS_V2_2026_PREMINE");

        Block {
            index: 0,
            prev_hash: [0u8; 32],
            timestamp: 1_772_975_700_000, // 2026-03-08 13:15:00 UTC in ms
            miner: [0u8; 32],
            nonce: 0,
            hash: genesis_hash,
            reward: 0,
            difficulty_target: 0,
            transactions: vec![],
        }
    }

    /// Validate this block against its predecessor.
    ///
    /// Checks:
    /// 1. `self.index == prev_block.index + 1`
    /// 2. `self.prev_hash == prev_block.hash`
    /// 3. `self.hash == self.hash_header()`  (hash integrity)
    /// 4. `self.hash` meets `difficulty` leading zero bits
    /// 5. `self.timestamp` is within ±2 minutes of node clock (CVE-002)
    pub fn is_valid(&self, prev_block: &Block, difficulty: u64) -> bool {
        self.is_valid_inner(prev_block, difficulty, true)
    }

    /// Same as `is_valid` but skips the timestamp drift check.
    /// Use during IBD (Initial Block Download) — historical blocks are always old.
    pub fn is_valid_ibd(&self, prev_block: &Block, difficulty: u64) -> bool {
        self.is_valid_inner(prev_block, difficulty, false)
    }

    fn is_valid_inner(&self, prev_block: &Block, difficulty: u64, check_timestamp: bool) -> bool {
        self.validate_inner(prev_block, difficulty, check_timestamp).is_none()
    }

    /// Returns `None` if valid, or `Some(reason)` if invalid.
    pub fn validate_inner(&self, prev_block: &Block, difficulty: u64, check_timestamp: bool) -> Option<String> {
        if self.index != prev_block.index + 1 {
            return Some(format!("bad index: got {} expected {}", self.index, prev_block.index + 1));
        }
        if self.prev_hash != prev_block.hash {
            return Some(format!("bad prev_hash: got {} expected {}", hex::encode(&self.prev_hash[..8]), hex::encode(&prev_block.hash[..8])));
        }
        let computed = self.hash_header();
        if self.hash != computed {
            return Some(format!("bad hash: stored {} computed {}", hex::encode(&self.hash[..8]), hex::encode(&computed[..8])));
        }
        // Accept both old format (difficulty_target == 0) and new format
        let check_target = if self.difficulty_target != 0 { self.difficulty_target } else { difficulty };
        if !meets_target(&self.hash, check_target) {
            return Some(format!("insufficient difficulty: hash {} target {}", hex::encode(&self.hash[..8]), check_target));
        }
        if check_timestamp {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let delta = if self.timestamp > now_ms {
                self.timestamp - now_ms
            } else {
                now_ms - self.timestamp
            };
            if delta > BLOCK_TIMESTAMP_TOLERANCE_MS {
                return Some(format!("timestamp too far: block={} now={} delta={}ms max={}ms", self.timestamp, now_ms, delta, BLOCK_TIMESTAMP_TOLERANCE_MS));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_genesis_deterministic() {
        let g1 = Block::genesis();
        let g2 = Block::genesis();
        assert_eq!(g1, g2);
        assert_eq!(g1.index, 0);
        assert_eq!(g1.prev_hash, [0u8; 32]);
        assert_eq!(g1.miner, [0u8; 32]);
        assert_eq!(g1.reward, 0);
    }

    #[test]
    fn test_hash_header_changes_with_nonce() {
        let mut block = Block::genesis();
        block.index = 1;
        block.nonce = 0;
        let h1 = block.hash_header();
        block.nonce = 1;
        let h2 = block.hash_header();
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_is_valid_rejects_wrong_index() {
        let genesis = Block::genesis();
        let mut block = Block {
            index: 2, // Wrong — should be 1
            prev_hash: genesis.hash,
            timestamp: genesis.timestamp + 1000,
            miner: [0u8; 32],
            nonce: 0,
            hash: [0u8; 32],
            reward: 0,
            transactions: vec![],
        };
        block.hash = block.hash_header();
        assert!(!block.is_valid_ibd(&genesis, 0)); // 0 difficulty — no leading-zero requirement
    }

    #[test]
    fn test_is_valid_rejects_wrong_prev_hash() {
        let genesis = Block::genesis();
        let mut block = Block {
            index: 1,
            prev_hash: [1u8; 32], // Wrong
            timestamp: genesis.timestamp + 1000,
            miner: [0u8; 32],
            nonce: 0,
            hash: [0u8; 32],
            reward: 0,
            transactions: vec![],
        };
        block.hash = block.hash_header();
        assert!(!block.is_valid_ibd(&genesis, 0));
    }

    #[test]
    fn test_is_valid_zero_difficulty() {
        // Mine a trivially valid block (difficulty 0 = any hash passes)
        let genesis = Block::genesis();
        let mut block = Block {
            index: 1,
            prev_hash: genesis.hash,
            timestamp: genesis.timestamp + 1000,
            miner: [2u8; 32],
            nonce: 0,
            hash: [0u8; 32],
            reward: 15_850_000,
            transactions: vec![],
        };
        block.hash = block.hash_header();
        assert!(block.is_valid_ibd(&genesis, 0));
    }

    /// Reproduces block #460 and #461 from the canonical testnet chain to verify
    /// that the local code can validate them.
    #[test]
    fn test_validate_block_461() {
        fn from_hex(s: &str) -> [u8; 32] {
            let b = hex::decode(s).expect("invalid hex");
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&b);
            arr
        }

        let block460 = Block {
            index: 460,
            prev_hash: from_hex("00002be3f0931b77c21739887aabd107958cdf426c710df27276f2ddf0d881b3"),
            timestamp: 1772827667569,
            miner: from_hex("fcf9a8b4aebfca4ec05f7b3c280cb5769d6c68a4f1b37924a3411ce27f441811"),
            nonce: 15372286728091954526,
            hash: from_hex("0000336070e0863da37c8cd43b6ba6f658cf60b0eecbd9162408a3ac2e939b2f"),
            reward: 15_850_000,
            transactions: vec![],
        };

        let block461 = Block {
            index: 461,
            prev_hash: from_hex("0000336070e0863da37c8cd43b6ba6f658cf60b0eecbd9162408a3ac2e939b2f"),
            timestamp: 1772886231552,
            miner: from_hex("fcf9a8b4aebfca4ec05f7b3c280cb5769d6c68a4f1b37924a3411ce27f441811"),
            nonce: 13835058055282177489,
            hash: from_hex("000018853a3b6c41484ca521b7f3c9c40155ed37cd4880956659fbc13527e390"),
            reward: 15_850_000,
            transactions: vec![],
        };

        // Verify block460 hash integrity
        let computed460 = block460.hash_header();
        assert_eq!(computed460, block460.hash,
            "block460 hash mismatch: computed={}, stored={}",
            hex::encode(computed460), hex::encode(block460.hash));

        // Verify block461 hash integrity
        let computed461 = block461.hash_header();
        assert_eq!(computed461, block461.hash,
            "block461 hash mismatch: computed={}, stored={}",
            hex::encode(computed461), hex::encode(block461.hash));

        // Verify block461 is valid against block460 at difficulty 17
        assert!(block461.is_valid_ibd(&block460, 17),
            "block461 is_valid_ibd failed at difficulty 17");
    }
}
