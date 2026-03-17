//! Genesis configuration and state for TARON testnet
//!
//! This module defines the initial state for the testnet including:
//! - Faucet wallet with predetermined seed for testing
//! - Testnet mining parameters
//! - Genesis block configuration

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Testnet mining difficulty — initial value (14 leading zero bits ≈ 30s at ~550 H/s)
/// Adjusts automatically via DAA every 10 blocks.
pub const TESTNET_DIFFICULTY: u32 = 14;

/// Testnet initial target (u64). Equivalent to 14 leading zero bits.
/// Higher = easier. DAA adjusts by ratio for smooth difficulty changes.
pub const TESTNET_TARGET: u64 = 1u64 << (64 - 14); // = 1u64 << 50

/// Testnet mining reward per solution (15.85 TAR in µTAR)
pub const TESTNET_REWARD: u64 = 15_850_000;

/// Development premine — 20,000,000 TAR (2% of 1B max supply) in µTAR
pub const PREMINE_BALANCE: u64 = 20_000_000_000_000;

/// Development fund public key (tar10c275dcfdd9776dcb847584afe317e1e6858f764d4edebfa4a1b8cb8ddeaf8e8)
/// Private key stored offline — not in this repository.
pub const PREMINE_PUBKEY: [u8; 32] = [
    0x0c, 0x27, 0x5d, 0xcf, 0xdd, 0x97, 0x76, 0xdc,
    0xb8, 0x47, 0x58, 0x4a, 0xfe, 0x31, 0x7e, 0x1e,
    0x68, 0x58, 0xf7, 0x64, 0xd4, 0xed, 0xeb, 0xfa,
    0x4a, 0x1b, 0x8c, 0xb8, 0xdd, 0xea, 0xf8, 0xe8,
];

/// Account state in the ledger
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountState {
    /// Account balance in µTAR
    pub balance: u64,
    /// Transaction sequence number (prevents replay attacks)
    pub sequence: u64,
    /// Hash of the last transaction from this account
    pub last_tx_hash: [u8; 32],
}

impl AccountState {
    /// Create a new account with given balance
    pub fn new(balance: u64) -> Self {
        Self {
            balance,
            sequence: 0,
            last_tx_hash: [0u8; 32],
        }
    }
    
    /// Check if account has sufficient balance for a transaction
    pub fn can_spend(&self, amount: u64) -> bool {
        self.balance >= amount
    }
    
    /// Update account after spending
    pub fn spend(&mut self, amount: u64, tx_hash: [u8; 32]) -> Result<(), &'static str> {
        if !self.can_spend(amount) {
            return Err("Insufficient balance");
        }
        self.balance -= amount;
        self.sequence += 1;
        self.last_tx_hash = tx_hash;
        Ok(())
    }
    
    /// Add funds to account
    pub fn credit(&mut self, amount: u64) {
        self.balance += amount;
    }
}

/// Genesis state for testnet
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenesisState {
    /// Account states by public key
    pub accounts: HashMap<[u8; 32], AccountState>,
}

impl GenesisState {
    /// Create the testnet genesis state with 2% development premine
    pub fn testnet() -> Self {
        let mut accounts = HashMap::new();
        accounts.insert(
            PREMINE_PUBKEY,
            AccountState::new(PREMINE_BALANCE)
        );

        GenesisState { accounts }
    }
    
    /// Get account state for a public key
    pub fn get_account(&self, pubkey: &[u8; 32]) -> Option<&AccountState> {
        self.accounts.get(pubkey)
    }
    
    /// Get mutable account state for a public key
    pub fn get_account_mut(&mut self, pubkey: &[u8; 32]) -> Option<&mut AccountState> {
        self.accounts.get_mut(pubkey)
    }
    
    /// Create new account if it doesn't exist
    pub fn ensure_account(&mut self, pubkey: [u8; 32]) -> &mut AccountState {
        self.accounts.entry(pubkey).or_insert_with(|| AccountState::new(0))
    }
}

/// Get the premine address as a tar1... string
pub fn premine_address() -> String {
    format!("tar1{}", hex::encode(PREMINE_PUBKEY))
}

/// Get testnet configuration parameters
#[derive(Debug, Clone)]
pub struct TestnetConfig {
    pub difficulty: u32,
    pub reward: u64,
    pub premine_balance: u64,
}

impl TestnetConfig {
    /// Get default testnet configuration
    pub fn default() -> Self {
        Self {
            difficulty: TESTNET_DIFFICULTY,
            reward: TESTNET_REWARD,
            premine_balance: PREMINE_BALANCE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_genesis_creation() {
        let genesis = GenesisState::testnet();
        let account = genesis.get_account(&PREMINE_PUBKEY).unwrap();
        assert_eq!(account.balance, PREMINE_BALANCE);
        assert_eq!(account.sequence, 0);
    }

    #[test]
    fn test_premine_address() {
        let addr = premine_address();
        assert!(addr.starts_with("tar1"));
        assert_eq!(addr.len(), 68);
    }

    #[test]
    fn test_account_operations() {
        let mut account = AccountState::new(1000);

        assert!(account.can_spend(500));
        assert!(!account.can_spend(1500));

        let tx_hash = [1u8; 32];
        account.spend(300, tx_hash).unwrap();
        assert_eq!(account.balance, 700);
        assert_eq!(account.sequence, 1);
        assert_eq!(account.last_tx_hash, tx_hash);

        account.credit(200);
        assert_eq!(account.balance, 900);
    }
}