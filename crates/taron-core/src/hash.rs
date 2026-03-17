//! SEQUAL-256 — Sequential CPU Hash Function
//!
//! SEQUAL-256 (Sequential Algorithm 256) is TARON's custom hash function.
//! It is designed to be:
//!
//! - **Sequentially dependent**: step N cannot begin before step N-1 completes.
//!   This eliminates any parallelism advantage from GPUs or multi-core setups.
//! - **Memory-accessing**: pseudo-random reads from a 4MB scratchpad (fits in L3
//!   cache but exceeds GPU per-core shared memory), creating memory-access patterns
//!   that are hostile to ASIC design.
//! - **Integer-heavy**: primarily 64-bit MUL, ADD, XOR, ROTATE operations where
//!   commodity CPUs excel.
//! - **256-bit output**: compatible with SHA-3 family output size.
//!
//! ## Security Note
//! SEQUAL-256 is a novel construction and has not yet undergone formal cryptographic
//! review. It is used here as a proof-of-concept. A formal review will be conducted
//! before mainnet launch.

use sha3::{Digest, Sha3_256};

/// Scratchpad size: 4MB for PoSC verification (fits L3, hostile to GPU shared mem)
const SCRATCHPAD_BYTES: usize = 4 * 1024 * 1024;
const SCRATCHPAD_U64S: usize = SCRATCHPAD_BYTES / 8;

/// Mining scratchpad: 256KB — fits in L2 cache for fast mining hashrate.
/// Security is maintained by sequential dependency, not memory size alone.
const MINING_SCRATCHPAD_BYTES: usize = 256 * 1024;
const MINING_SCRATCHPAD_U64S: usize = MINING_SCRATCHPAD_BYTES / 8;

/// Golden ratio constant (phi * 2^64) — good mixing properties
const GOLDEN_RATIO: u64 = 0x9e3779b97f4a7c15;

/// Number of sequential steps for PoSC transaction proof (~2ms on modern CPU).
/// Kept low to ensure sub-100ms transaction finality.
pub const POSC_STEPS: u32 = 2_000;

/// Number of sequential steps per mining hash.
/// Tuned so a modern CPU core achieves ~10-20 kH/s, comparable to RandomX (Monero).
/// Difficulty is adjusted via leading-zero-bit targets, not step count.
pub const MINING_STEPS: u32 = 1_000;

/// SEQUAL-256 hasher.
///
/// # Example
/// ```rust
/// use taron_core::Sequal256;
///
/// let input = b"hello taron";
/// let output = Sequal256::hash(input, 1000);
/// assert_eq!(output.len(), 32);
/// ```
pub struct Sequal256;

impl Sequal256 {
    /// Compute SEQUAL-256 over `seed` with `steps` sequential iterations.
    ///
    /// - `seed`: arbitrary input bytes (transaction data, mining nonce, etc.)
    /// - `steps`: number of sequential hash chain iterations
    ///
    /// Returns a 32-byte digest.
    pub fn hash(seed: &[u8], steps: u32) -> [u8; 32] {
        // Phase 1: Initialize scratchpad from seed via SHA3-256 expansion.
        // We use SHA3 here (not SEQUAL) to avoid a chicken-and-egg dependency.
        let mut scratchpad = vec![0u64; SCRATCHPAD_U64S];
        Self::expand_scratchpad(seed, &mut scratchpad);

        // Phase 2: Initialize 4-word state from seed.
        let seed_hash = sha3_256(seed);
        let mut state = [
            u64::from_le_bytes(seed_hash[0..8].try_into().unwrap()),
            u64::from_le_bytes(seed_hash[8..16].try_into().unwrap()),
            u64::from_le_bytes(seed_hash[16..24].try_into().unwrap()),
            u64::from_le_bytes(seed_hash[24..32].try_into().unwrap()),
        ];

        // Phase 3: Sequential mixing chain.
        // Each iteration depends on the previous — no parallelism possible.
        for i in 0..steps as u64 {
            // Non-linear mixing (ARX construction)
            state[0] = state[0].wrapping_add(GOLDEN_RATIO);
            state[0] ^= state[0].rotate_left(13);
            state[1] = state[1].wrapping_mul(GOLDEN_RATIO | 1); // odd multiplier
            state[1] ^= state[1].rotate_right(27);
            state[2] = state[2].wrapping_add(state[0] ^ state[1]);
            state[2] ^= state[2].rotate_left(37);
            state[3] = state[3].wrapping_mul(state[2] | 1);
            state[3] ^= state[3].rotate_right(19);

            // Pseudo-random scratchpad read (memory access pattern)
            // Address derived from current state — unpredictable, defeats prefetching
            let addr = (state[0].wrapping_add(i) as usize) % SCRATCHPAD_U64S;
            state[1] ^= scratchpad[addr];

            // Write-back: scratchpad evolves during computation (feed-forward)
            // This means earlier iterations affect later memory reads — prevents precomputation
            scratchpad[addr] = state[1]
                .wrapping_add(state[2])
                .rotate_left((i % 64) as u32);

            // Cross-word mixing every 4 steps (reduces differential attacks)
            if i % 4 == 3 {
                state[0] ^= state[3];
                state[1] ^= state[0];
                state[2] ^= state[1];
                state[3] ^= state[2];
            }
        }

        // Phase 4: Finalize — absorb state into SHA3-256 for output standardization.
        let mut final_input = Vec::with_capacity(32 + seed.len());
        for &word in &state {
            final_input.extend_from_slice(&word.to_le_bytes());
        }
        final_input.extend_from_slice(seed);

        sha3_256(&final_input)
    }

    /// Expand a seed into the scratchpad using repeated SHA3-256 hashing.
    /// This initializes the scratchpad from a short seed deterministically.
    /// Uses `scratchpad.len()` so it works with any slice size (full 4MB or test slices).
    fn expand_scratchpad(seed: &[u8], scratchpad: &mut [u64]) {
        let len = scratchpad.len();
        let mut current = sha3_256(seed);
        let mut idx = 0;

        while idx < len {
            // Fill up to 4 u64s per SHA3 output (32 bytes / 8 = 4)
            for chunk in current.chunks(8) {
                if idx >= len {
                    break;
                }
                scratchpad[idx] = u64::from_le_bytes(chunk.try_into().unwrap());
                idx += 1;
            }
            // Chain: next block = SHA3(previous block || counter)
            let counter = (idx as u64).to_le_bytes();
            let mut next_input = Vec::with_capacity(40);
            next_input.extend_from_slice(&current);
            next_input.extend_from_slice(&counter);
            current = sha3_256(&next_input);
        }
    }

    /// Compute a PoSC proof for a transaction (fixed steps, deterministic time).
    pub fn posc_proof(tx_bytes: &[u8]) -> [u8; 32] {
        Self::hash(tx_bytes, POSC_STEPS)
    }

    /// Compute SEQUAL-256 with a reduced 256KB scratchpad (L2-cache friendly).
    /// Used for mining where throughput matters more than memory-hardness.
    /// Sequential dependency is preserved — GPU/ASIC resistance comes from
    /// the chain structure, not scratchpad size alone.
    pub fn hash_fast(seed: &[u8], steps: u32) -> [u8; 32] {
        // Reuse thread-local scratchpad to avoid 256KB heap allocation per hash.
        // This gives ~5-10x speedup in tight mining loops.
        thread_local! {
            static SCRATCHPAD: std::cell::RefCell<Vec<u64>> = std::cell::RefCell::new(vec![0u64; MINING_SCRATCHPAD_U64S]);
        }
        SCRATCHPAD.with(|sp| {
            let mut scratchpad = sp.borrow_mut();
            Self::hash_fast_with_scratchpad(seed, steps, &mut scratchpad)
        })
    }

    /// hash_fast with a caller-provided scratchpad (avoids allocation).
    fn hash_fast_with_scratchpad(seed: &[u8], steps: u32, scratchpad: &mut [u64]) -> [u8; 32] {
        Self::expand_scratchpad(seed, scratchpad);

        let seed_hash = sha3_256(seed);
        let mut state = [
            u64::from_le_bytes(seed_hash[0..8].try_into().unwrap()),
            u64::from_le_bytes(seed_hash[8..16].try_into().unwrap()),
            u64::from_le_bytes(seed_hash[16..24].try_into().unwrap()),
            u64::from_le_bytes(seed_hash[24..32].try_into().unwrap()),
        ];

        for i in 0..steps as u64 {
            state[0] = state[0].wrapping_add(GOLDEN_RATIO);
            state[0] ^= state[0].rotate_left(13);
            state[1] = state[1].wrapping_mul(GOLDEN_RATIO | 1);
            state[1] ^= state[1].rotate_right(27);
            state[2] = state[2].wrapping_add(state[0] ^ state[1]);
            state[2] ^= state[2].rotate_left(37);
            state[3] = state[3].wrapping_mul(state[2] | 1);
            state[3] ^= state[3].rotate_right(19);

            let addr = (state[0].wrapping_add(i) as usize) % MINING_SCRATCHPAD_U64S;
            state[1] ^= scratchpad[addr];
            scratchpad[addr] = state[1]
                .wrapping_add(state[2])
                .rotate_left((i % 64) as u32);

            if i % 4 == 3 {
                state[0] ^= state[3];
                state[1] ^= state[0];
                state[2] ^= state[1];
                state[3] ^= state[2];
            }
        }

        let mut final_input = Vec::with_capacity(32 + seed.len());
        for &word in &state {
            final_input.extend_from_slice(&word.to_le_bytes());
        }
        final_input.extend_from_slice(seed);
        sha3_256(&final_input)
    }

    /// Compute a single mining iteration with a nonce (4MB scratchpad).
    /// Returns the hash — caller checks difficulty (leading zeros).
    pub fn mine_step(block_header: &[u8], nonce: u64, steps: u32) -> [u8; 32] {
        let mut input = Vec::with_capacity(block_header.len() + 8);
        input.extend_from_slice(block_header);
        input.extend_from_slice(&nonce.to_le_bytes());
        Self::hash(&input, steps)
    }

    /// Fast mining step with 256KB scratchpad (L2-friendly, higher throughput).
    /// Use this for mining — sequential dependency is fully preserved.
    pub fn mine_step_fast(block_header: &[u8], nonce: u64, steps: u32) -> [u8; 32] {
        let mut input = Vec::with_capacity(block_header.len() + 8);
        input.extend_from_slice(block_header);
        input.extend_from_slice(&nonce.to_le_bytes());
        Self::hash_fast(&input, steps)
    }

    /// Check if a hash meets a difficulty target (number of leading zero bits).
    pub fn meets_difficulty(hash: &[u8; 32], difficulty_bits: u32) -> bool {
        let full_bytes = (difficulty_bits / 8) as usize;
        let remaining_bits = difficulty_bits % 8;

        // Check full zero bytes
        for i in 0..full_bytes {
            if i >= hash.len() || hash[i] != 0 {
                return false;
            }
        }

        // Check remaining bits in the next byte
        if remaining_bits > 0 && full_bytes < hash.len() {
            let mask = 0xFF_u8 << (8 - remaining_bits);
            if hash[full_bytes] & mask != 0 {
                return false;
            }
        }

        true
    }

    /// Check if a hash is below a u64 target (fine-grained difficulty).
    /// Compares the first 8 bytes of the hash (big-endian) against the target.
    /// Lower target = harder. Higher target = easier.
    pub fn meets_target(hash: &[u8; 32], target: u64) -> bool {
        let hash_val = u64::from_be_bytes([
            hash[0], hash[1], hash[2], hash[3],
            hash[4], hash[5], hash[6], hash[7],
        ]);
        hash_val < target
    }
}

/// Convert a leading-zero-bits difficulty to a u64 target.
pub fn bits_to_target(bits: u32) -> u64 {
    if bits >= 64 { return 0; }
    1u64 << (64 - bits)
}

/// Convert a u64 target to approximate leading-zero-bits (for display).
pub fn target_to_bits(target: u64) -> u32 {
    if target == 0 { return 64; }
    (64 - (64 - target.leading_zeros())) as u32
}

/// Convenience wrapper for SHA3-256.
pub fn sha3_256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha3_256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Check if a hash meets a difficulty target (number of leading zero bits).
/// This is a standalone function wrapper around Sequal256::meets_difficulty.
pub fn meets_difficulty(hash: &[u8; 32], difficulty_bits: u32) -> bool {
    Sequal256::meets_difficulty(hash, difficulty_bits)
}

/// Standalone wrapper for target-based difficulty check.
pub fn meets_target(hash: &[u8; 32], target: u64) -> bool {
    Sequal256::meets_target(hash, target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequal256_deterministic() {
        let input = b"taron genesis";
        let h1 = Sequal256::hash(input, 100);
        let h2 = Sequal256::hash(input, 100);
        assert_eq!(h1, h2, "SEQUAL-256 must be deterministic");
    }

    #[test]
    fn test_sequal256_different_inputs() {
        let h1 = Sequal256::hash(b"input_a", 100);
        let h2 = Sequal256::hash(b"input_b", 100);
        assert_ne!(h1, h2, "Different inputs must produce different hashes");
    }

    #[test]
    fn test_sequal256_different_steps() {
        let h1 = Sequal256::hash(b"taron", 100);
        let h2 = Sequal256::hash(b"taron", 101);
        assert_ne!(h1, h2, "Different step counts must produce different hashes");
    }

    #[test]
    fn test_sequal256_output_length() {
        let h = Sequal256::hash(b"test", 10);
        assert_eq!(h.len(), 32, "Output must be 32 bytes");
    }

    #[test]
    fn test_sequal256_avalanche() {
        // 1-bit change in input should cause ~50% bit change in output
        let h1 = Sequal256::hash(&[0u8; 32], 100);
        let mut input2 = [0u8; 32];
        input2[0] = 1; // flip 1 bit
        let h2 = Sequal256::hash(&input2, 100);

        let diff_bits: u32 = h1
            .iter()
            .zip(h2.iter())
            .map(|(a, b)| (a ^ b).count_ones())
            .sum();

        // Should differ in ~50% of 256 bits (128 ± 64 acceptable range)
        assert!(
            diff_bits > 64 && diff_bits < 192,
            "Avalanche effect: expected ~128 bit difference, got {}",
            diff_bits
        );
    }

    #[test]
    fn test_difficulty_check() {
        // hash[0] = 0x00 → 8 leading zero bits
        // hash[1] = 0x0f = 0000_1111 → 4 more leading zero bits = 12 total
        let mut hash = [0u8; 32];
        hash[0] = 0x00;
        hash[1] = 0x0f;
        assert!(Sequal256::meets_difficulty(&hash, 0));   // trivially true
        assert!(Sequal256::meets_difficulty(&hash, 8));   // first byte all zeros
        assert!(Sequal256::meets_difficulty(&hash, 12));  // 8 + 4 leading zeros
        assert!(!Sequal256::meets_difficulty(&hash, 13)); // 13th bit is 1 → fails
        assert!(!Sequal256::meets_difficulty(&hash, 16)); // second byte not all zeros
    }

    #[test]
    fn test_scratchpad_expansion() {
        // Two different seeds produce different scratchpads
        let mut sp1 = vec![0u64; 1024];
        let mut sp2 = vec![0u64; 1024];
        Sequal256::expand_scratchpad(b"seed_a", &mut sp1);
        Sequal256::expand_scratchpad(b"seed_b", &mut sp2);
        assert_ne!(sp1[0], sp2[0]);
        assert_ne!(sp1[100], sp2[100]);
    }
}
