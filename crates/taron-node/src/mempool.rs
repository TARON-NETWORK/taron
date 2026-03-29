//! Mempool — verified transaction storage with deduplication.

use std::collections::HashMap;
use std::time::Instant;
use taron_core::{Transaction, PoscVerifier};

const TX_TTL_SECS: u64 = 3600;

/// In-memory pool of verified transactions, keyed by tx hash.
#[derive(Debug)]
pub struct Mempool {
    txs: HashMap<String, (Transaction, Instant)>,
}

impl Default for Mempool {
    fn default() -> Self {
        Self { txs: HashMap::new() }
    }
}

impl Mempool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn evict_expired(&mut self) {
        let before = self.txs.len();
        self.txs.retain(|_, (_, inserted)| inserted.elapsed().as_secs() < TX_TTL_SECS);
        let removed = before - self.txs.len();
        if removed > 0 {
            tracing::info!("[MEMPOOL] Evicted {} expired transactions", removed);
        }
    }

    /// Number of transactions in the mempool.
    pub fn len(&self) -> usize {
        self.txs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    pub fn contains(&self, tx_hash: &str) -> bool {
        self.txs.contains_key(tx_hash)
    }

    pub fn tx_hashes(&self) -> Vec<String> {
        self.txs.keys().cloned().collect()
    }

    pub fn get_txs(&self, hashes: &[String]) -> Vec<Transaction> {
        hashes.iter()
            .filter_map(|h| self.txs.get(h).map(|(tx, _)| tx.clone()))
            .collect()
    }

    pub fn insert(&mut self, tx: Transaction) -> Result<bool, String> {
        let hash = tx.hash_hex();
        if self.txs.contains_key(&hash) {
            return Ok(false);
        }
        tx.verify_signature().map_err(|e| format!("signature: {}", e))?;
        PoscVerifier::verify(&tx).map_err(|e| format!("posc: {}", e))?;
        self.txs.insert(hash, (tx, Instant::now()));
        Ok(true)
    }

    pub fn all_txs(&self) -> Vec<&Transaction> {
        self.txs.values().map(|(tx, _)| tx).collect()
    }

    pub fn remove(&mut self, tx_hash: &str) {
        self.txs.remove(tx_hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use taron_core::{Wallet, TxBuilder};

    fn make_valid_tx() -> Transaction {
        let sender = Wallet::generate();
        let recipient = Wallet::generate();
        TxBuilder::new(&sender)
            .recipient(recipient.public_key())
            .amount(1_000_000)
            .sequence(1)
            .build_and_prove()
            .unwrap()
    }

    #[test]
    fn test_mempool_insert_valid() {
        let mut pool = Mempool::new();
        let tx = make_valid_tx();
        assert!(pool.insert(tx).unwrap());
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_mempool_dedup() {
        let mut pool = Mempool::new();
        let tx = make_valid_tx();
        let tx2 = tx.clone();
        assert!(pool.insert(tx).unwrap());
        assert!(!pool.insert(tx2).unwrap()); // duplicate
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_mempool_reject_tampered() {
        let mut pool = Mempool::new();
        let mut tx = make_valid_tx();
        tx.amount += 1; // tamper
        assert!(pool.insert(tx).is_err());
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn test_mempool_tx_hashes() {
        let mut pool = Mempool::new();
        let tx = make_valid_tx();
        let hash = tx.hash_hex();
        pool.insert(tx).unwrap();
        assert!(pool.tx_hashes().contains(&hash));
    }

    #[test]
    fn test_mempool_get_txs() {
        let mut pool = Mempool::new();
        let tx = make_valid_tx();
        let hash = tx.hash_hex();
        pool.insert(tx).unwrap();
        let fetched = pool.get_txs(&[hash.clone(), "nonexistent".into()]);
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].hash_hex(), hash);
    }
}
