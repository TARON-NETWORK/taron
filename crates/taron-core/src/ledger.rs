//! Ledger — Account-based state management for TARON
//!
//! The ledger maintains the global state of all accounts in the TARON network.
//! Each account is identified by a 32-byte Ed25519 public key and contains:
//! - Balance in micro-TAR (µTAR)
//! - Sequence number for transaction ordering
//! - Hash of the last transaction for PoSC chaining
//!
//! ## Account Model
//!
//! TARON uses an account-based model similar to Ethereum, where:
//! - Each account has a unique 32-byte public key identifier
//! - Balances are stored in micro-TAR (1 TAR = 1,000,000 µTAR)
//! - Sequence numbers prevent replay attacks and ensure transaction ordering
//! - Previous transaction hashes link transactions in a chain for PoSC verification

use std::collections::HashMap;
use std::path::Path;
use serde::{Deserialize, Serialize};
use crate::{Transaction, TaronError, AccountState, GenesisState};

/// The TARON ledger — account-based state kept in RAM, persisted via bincode.
///
/// The HashMap stays in memory for O(1) balance lookups. On every block,
/// the ledger is serialised with bincode (~4× smaller than JSON) to `ledger.bin`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ledger {
    accounts: HashMap<[u8; 32], AccountState>,
}

impl Ledger {
    /// Create a new empty ledger
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
        }
    }

    /// Create a testnet ledger with genesis premine
    pub fn new_testnet() -> Self {
        let genesis_state = GenesisState::testnet();
        Self {
            accounts: genesis_state.accounts,
        }
    }

    /// Load ledger from bincode, migrate from JSON, rebuild from chain, or create genesis.
    ///
    /// Priority order:
    /// 1. `ledger.bin` — fast binary load
    /// 2. `ledger.json` next to `path` — migrate to bincode automatically
    /// 3. Rebuild from `chain` by replaying all coinbase rewards
    /// 4. Fresh testnet genesis
    pub fn load_or_create_testnet(path: &Path, chain: &crate::Blockchain) -> Self {
        // 1. Try bincode
        if path.exists() {
            if let Ok(bytes) = std::fs::read(path) {
                if let Ok(ledger) = bincode::deserialize::<Ledger>(&bytes) {
                    eprintln!("[LEDGER] Loaded from bincode — {} accounts", ledger.accounts.len());
                    return ledger;
                }
            }
        }

        // 2. Migrate from legacy JSON (hex-keyed HashMap)
        let json_path = path.with_extension("json");
        if json_path.exists() {
            eprintln!("[LEDGER] Migrating ledger.json → bincode…");
            if let Ok(data) = std::fs::read_to_string(&json_path) {
                if let Ok(raw) = serde_json::from_str::<HashMap<String, AccountState>>(&data) {
                    let mut accounts: HashMap<[u8; 32], AccountState> = HashMap::new();
                    for (k, v) in raw {
                        if let Ok(b) = hex::decode(&k) {
                            if b.len() == 32 {
                                let mut arr = [0u8; 32];
                                arr.copy_from_slice(&b);
                                accounts.insert(arr, v);
                            }
                        }
                    }
                    let ledger = Ledger { accounts };
                    ledger.save(path);
                    eprintln!("[LEDGER] Migration complete — {} accounts", ledger.accounts.len());
                    return ledger;
                }
            }
        }

        // 3. Rebuild from chain (handles format-change restarts)
        if chain.height() > 0 {
            eprintln!("[LEDGER] Rebuilding from {} blocks…", chain.height());
            let ledger = Self::rebuild_from_chain(chain);
            ledger.save(path);
            eprintln!("[LEDGER] Rebuild complete — {} accounts", ledger.accounts.len());
            return ledger;
        }

        // 4. Fresh genesis
        let ledger = Self::new_testnet();
        ledger.save(path);
        ledger
    }

    /// Replay all coinbase rewards from `chain` to reconstruct the ledger.
    /// Starts from genesis state (includes premine) then applies all blocks.
    pub fn rebuild_from_chain(chain: &crate::Blockchain) -> Self {
        let mut ledger = Self::new_testnet();
        for i in 1..=chain.height() {
            if let Some(block) = chain.block_at(i) {
                ledger.apply_coinbase(&block.miner, block.reward);
            }
        }
        ledger
    }

    /// Persist ledger to a bincode file (atomic: write tmp then rename).
    pub fn save(&self, path: &Path) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let bytes = bincode::serialize(self).expect("Failed to serialize ledger");
        let tmp = path.with_extension("bin.tmp");
        std::fs::write(&tmp, &bytes).expect("Failed to write ledger tmp");
        std::fs::rename(&tmp, path).expect("Failed to rename ledger");
    }

    /// Get the balance of an account
    pub fn balance(&self, pubkey: &[u8; 32]) -> u64 {
        self.accounts.get(pubkey).map_or(0, |acc| acc.balance)
    }

    /// Get an account's full state
    pub fn get_account(&self, pubkey: &[u8; 32]) -> Option<&AccountState> {
        self.accounts.get(pubkey)
    }

    /// Get a mutable reference to an account, creating it if it doesn't exist
    fn get_account_mut(&mut self, pubkey: &[u8; 32]) -> &mut AccountState {
        self.accounts.entry(*pubkey).or_insert_with(|| AccountState::new(0))
    }

    /// Apply a transaction to the ledger
    ///
    /// This validates:
    /// - Sender has sufficient balance for amount + fee
    /// - Sender's sequence number is valid
    /// - Updates both sender and recipient accounts
    /// - Burns the transaction fee
    pub fn apply_tx(&mut self, tx: &Transaction) -> Result<(), TaronError> {
        let tx_hash = tx.hash();
        let total_cost = tx.total_cost();

        // Validate sender exists and has sufficient balance
        let sender_account = self.get_account(&tx.sender);
        if sender_account.is_none() {
            return Err(TaronError::InsufficientBalance {
                have: 0,
                need: total_cost,
            });
        }

        let sender_balance = sender_account.unwrap().balance;
        if sender_balance < total_cost {
            return Err(TaronError::InsufficientBalance {
                have: sender_balance,
                need: total_cost,
            });
        }

        // Validate sequence number
        let expected_sequence = sender_account.unwrap().sequence + 1;
        if tx.sequence != expected_sequence {
            return Err(TaronError::InvalidSequence {
                expected: expected_sequence,
                got: tx.sequence,
            });
        }

        // Apply changes to sender account (debit amount + fee)
        {
            let sender_account = self.get_account_mut(&tx.sender);
            sender_account.spend(total_cost, tx_hash).map_err(|_| {
                TaronError::InsufficientBalance {
                    have: sender_account.balance,
                    need: total_cost,
                }
            })?;
            sender_account.tx_count += 1;
            if sender_account.first_seen == 0 || tx.timestamp_ms < sender_account.first_seen {
                sender_account.first_seen = tx.timestamp_ms;
            }
            if tx.timestamp_ms > sender_account.last_seen {
                sender_account.last_seen = tx.timestamp_ms;
            }
        }

        // Apply changes to recipient account (credit amount only, fee is burned)
        {
            let recipient_account = self.get_account_mut(&tx.recipient);
            recipient_account.credit(tx.amount);
            recipient_account.last_tx_hash = tx_hash;
            recipient_account.tx_count += 1;
            if recipient_account.first_seen == 0 || tx.timestamp_ms < recipient_account.first_seen {
                recipient_account.first_seen = tx.timestamp_ms;
            }
            if tx.timestamp_ms > recipient_account.last_seen {
                recipient_account.last_seen = tx.timestamp_ms;
            }
        }

        Ok(())
    }

    /// Apply a transaction during IBD (Initial Block Download).
    ///
    /// Like `apply_tx` but skips the sequence number check — during IBD the
    /// local ledger may be at a different sequence than the server's ledger
    /// (due to payout transactions that were embedded in blocks but not stored
    /// in early chain snapshots). The sequence IS still set to `tx.sequence`
    /// so subsequent transactions in the same block chain correctly.
    pub fn apply_tx_ibd(&mut self, tx: &Transaction) -> Result<(), TaronError> {
        let tx_hash = tx.hash();
        let total_cost = tx.total_cost();

        let sender_balance = self.balance(&tx.sender);
        if sender_balance < total_cost {
            return Err(TaronError::InsufficientBalance {
                have: sender_balance,
                need: total_cost,
            });
        }

        // Apply debit (spend() increments sequence by 1, we'll override below)
        {
            let sender_account = self.get_account_mut(&tx.sender);
            sender_account.spend(total_cost, tx_hash).map_err(|_| {
                TaronError::InsufficientBalance {
                    have: sender_account.balance,
                    need: total_cost,
                }
            })?;
            sender_account.sequence = tx.sequence;
            sender_account.tx_count += 1;
            if sender_account.first_seen == 0 || tx.timestamp_ms < sender_account.first_seen {
                sender_account.first_seen = tx.timestamp_ms;
            }
            if tx.timestamp_ms > sender_account.last_seen {
                sender_account.last_seen = tx.timestamp_ms;
            }
        }

        {
            let recipient_account = self.get_account_mut(&tx.recipient);
            recipient_account.credit(tx.amount);
            recipient_account.last_tx_hash = tx_hash;
            recipient_account.tx_count += 1;
            if recipient_account.first_seen == 0 || tx.timestamp_ms < recipient_account.first_seen {
                recipient_account.first_seen = tx.timestamp_ms;
            }
            if tx.timestamp_ms > recipient_account.last_seen {
                recipient_account.last_seen = tx.timestamp_ms;
            }
        }

        Ok(())
    }

    /// Revert a transaction — undo the effects of apply_tx.
    /// Credits amount+fee back to sender, debits amount from recipient.
    pub fn revert_tx(&mut self, tx: &Transaction) {
        let total_cost = tx.total_cost();

        // Undo recipient credit
        let recipient = self.get_account_mut(&tx.recipient);
        recipient.balance = recipient.balance.saturating_sub(tx.amount);

        // Undo sender debit
        let sender = self.get_account_mut(&tx.sender);
        sender.balance += total_cost;
        sender.sequence = sender.sequence.saturating_sub(1);
    }

    /// Revert a coinbase reward — undo the effects of apply_coinbase.
    pub fn revert_coinbase(&mut self, pubkey: &[u8; 32], amount: u64) {
        let account = self.get_account_mut(pubkey);
        account.balance = account.balance.saturating_sub(amount);
    }

    /// Apply a coinbase transaction (mining reward)
    ///
    /// This creates or credits an account with mining rewards
    /// No sequence validation needed for coinbase transactions
    pub fn apply_coinbase(&mut self, pubkey: &[u8; 32], amount: u64) {
        let coinbase_hash = crate::hash::sha3_256(&[
            &pubkey[..],
            &amount.to_le_bytes(),
            b"coinbase",
        ].concat());

        let account = self.get_account_mut(pubkey);
        account.credit(amount);
        account.last_tx_hash = coinbase_hash;
        account.blocks_mined += 1;
    }

    /// Update first_seen/last_seen timestamps for an account
    pub fn touch_account(&mut self, pubkey: &[u8; 32], timestamp_ms: u64) {
        let account = self.get_account_mut(pubkey);
        if account.first_seen == 0 || timestamp_ms < account.first_seen {
            account.first_seen = timestamp_ms;
        }
        if timestamp_ms > account.last_seen {
            account.last_seen = timestamp_ms;
        }
    }

    /// Get the total number of accounts
    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    /// Get total supply (sum of all balances)
    pub fn total_supply(&self) -> u64 {
        self.accounts.values().map(|acc| acc.balance).sum()
    }

    /// Get all accounts (for debugging/testing)
    pub fn all_accounts(&self) -> &HashMap<[u8; 32], AccountState> {
        &self.accounts
    }

}

impl Default for Ledger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Wallet, TxBuilder};

    #[test]
    fn test_empty_ledger() {
        let ledger = Ledger::new();
        let pubkey = [1u8; 32];
        assert_eq!(ledger.balance(&pubkey), 0);
        assert!(ledger.get_account(&pubkey).is_none());
        assert_eq!(ledger.account_count(), 0);
        assert_eq!(ledger.total_supply(), 0);
    }

    #[test]
    fn test_testnet_ledger() {
        let ledger = Ledger::new_testnet();
        assert_eq!(ledger.balance(&crate::PREMINE_PUBKEY), crate::PREMINE_BALANCE);
        assert_eq!(ledger.account_count(), 1);
        assert_eq!(ledger.total_supply(), crate::PREMINE_BALANCE);

        let account = ledger.get_account(&crate::PREMINE_PUBKEY).unwrap();
        assert_eq!(account.balance, crate::PREMINE_BALANCE);
        assert_eq!(account.sequence, 0);
    }

    #[test]
    fn test_coinbase_application() {
        let mut ledger = Ledger::new();
        let miner_key = [2u8; 32];
        let reward = 50_000_000; // 50 TAR

        // Apply coinbase reward
        ledger.apply_coinbase(&miner_key, reward);

        assert_eq!(ledger.balance(&miner_key), reward);
        assert_eq!(ledger.account_count(), 1);
        assert_eq!(ledger.total_supply(), reward);

        let account = ledger.get_account(&miner_key).unwrap();
        assert_eq!(account.balance, reward);
        assert_eq!(account.sequence, 0); // Coinbase doesn't affect sequence
        assert_ne!(account.last_tx_hash, [0u8; 32]); // Should have a tx hash
    }

    #[test]
    fn test_transaction_application() {
        let mut ledger = Ledger::new_testnet();
        
        // Create sender and recipient wallets
        let sender = Wallet::generate();
        let recipient = Wallet::generate();

        // Give sender some funds first
        let initial_amount = 1_000_000; // 1 TAR
        ledger.apply_coinbase(&sender.public_key(), initial_amount);

        // Create a transaction
        let tx_amount = 500_000; // 0.5 TAR
        let tx = TxBuilder::new(&sender)
            .recipient(recipient.public_key())
            .amount(tx_amount)
            .fee(1000) // 0.001 TAR
            .sequence(1) // First tx for this sender
            .prev_tx_hash([0u8; 32])
            .build_and_prove()
            .unwrap();

        // Apply transaction
        ledger.apply_tx(&tx).unwrap();

        // Check balances
        assert_eq!(ledger.balance(&sender.public_key()), initial_amount - tx_amount - 1000);
        assert_eq!(ledger.balance(&recipient.public_key()), tx_amount);

        // Check sender sequence updated
        let sender_account = ledger.get_account(&sender.public_key()).unwrap();
        assert_eq!(sender_account.sequence, 1);
    }

    #[test]
    fn test_insufficient_balance() {
        let mut ledger = Ledger::new();
        let sender = Wallet::generate();
        let recipient = Wallet::generate();

        // Try to send money with no balance
        let tx = TxBuilder::new(&sender)
            .recipient(recipient.public_key())
            .amount(1_000_000)
            .sequence(1)
            .build_and_prove()
            .unwrap();

        let result = ledger.apply_tx(&tx);
        assert!(matches!(result, Err(TaronError::InsufficientBalance { have: 0, need: 1_000_001 })));
    }

    #[test]
    fn test_invalid_sequence() {
        let mut ledger = Ledger::new_testnet();
        let sender = Wallet::generate();
        let recipient = Wallet::generate();

        // Give sender some funds
        ledger.apply_coinbase(&sender.public_key(), 2_000_000);

        // Try to use wrong sequence number
        let tx = TxBuilder::new(&sender)
            .recipient(recipient.public_key())
            .amount(1_000_000)
            .sequence(5) // Should be 1
            .build_and_prove()
            .unwrap();

        let result = ledger.apply_tx(&tx);
        assert!(matches!(result, Err(TaronError::InvalidSequence { expected: 1, got: 5 })));
    }

    #[test]
    fn test_multiple_transactions() {
        let mut ledger = Ledger::new_testnet();
        let sender = Wallet::generate();
        let recipient = Wallet::generate();

        // Give sender funds
        let initial_amount = 10_000_000; // 10 TAR
        ledger.apply_coinbase(&sender.public_key(), initial_amount);

        // Send multiple transactions
        for i in 1..=3 {
            let tx = TxBuilder::new(&sender)
                .recipient(recipient.public_key())
                .amount(1_000_000) // 1 TAR each
                .fee(1000)
                .sequence(i)
                .prev_tx_hash(
                    ledger.get_account(&sender.public_key())
                        .map_or([0u8; 32], |acc| acc.last_tx_hash)
                )
                .build_and_prove()
                .unwrap();

            ledger.apply_tx(&tx).unwrap();
        }

        // Check final balances
        assert_eq!(ledger.balance(&sender.public_key()), initial_amount - 3 * (1_000_000 + 1000));
        assert_eq!(ledger.balance(&recipient.public_key()), 3_000_000);

        // Check sender sequence
        let sender_account = ledger.get_account(&sender.public_key()).unwrap();
        assert_eq!(sender_account.sequence, 3);
    }

    #[test]
    fn test_self_transfer() {
        let mut ledger = Ledger::new();
        let wallet = Wallet::generate();

        // Give wallet funds
        ledger.apply_coinbase(&wallet.public_key(), 2_000_000);

        // Send to self
        let tx = TxBuilder::new(&wallet)
            .recipient(wallet.public_key())
            .amount(1_000_000)
            .fee(1000)
            .sequence(1)
            .build_and_prove()
            .unwrap();

        ledger.apply_tx(&tx).unwrap();

        // Balance should be reduced by fee only
        assert_eq!(ledger.balance(&wallet.public_key()), 2_000_000 - 1000);
    }
}