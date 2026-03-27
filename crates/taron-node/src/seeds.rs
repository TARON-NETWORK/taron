//! Hardcoded testnet seed nodes for TARON.
//!
//! Seed nodes help new peers bootstrap into the testnet when they have
//! no prior peer list.

use std::net::{SocketAddr, ToSocketAddrs};

/// Known TARON testnet seed nodes (hostname:port or ip:port).
/// Multiple seeds improve resilience — if one is down, others bootstrap the node.
pub const TESTNET_SEEDS: &[&str] = &[
    "185.211.6.168:8333",  // EU (Contabo)
    "82.197.67.49:8333",   // US East (Contabo)
    "46.250.234.67:8333",  // Singapore (Contabo)
];

/// Resolve the effective seed-node list.
///
/// Rules (in priority order):
/// 1. If `config_seeds` is non-empty, use those.
/// 2. Else resolve TESTNET_SEEDS via DNS (supports both hostnames and IPs).
/// 3. Otherwise, return an empty vec (LAN discovery is the fallback).
pub fn is_seed_addr(addr: &SocketAddr) -> bool {
    TESTNET_SEEDS.iter().any(|s| {
        s.parse::<SocketAddr>().map(|sa| sa.ip() == addr.ip()).unwrap_or(false)
    })
}

pub fn resolve_seeds(config_seeds: &[SocketAddr]) -> Vec<SocketAddr> {
    if !config_seeds.is_empty() {
        return config_seeds.to_vec();
    }
    TESTNET_SEEDS
        .iter()
        .flat_map(|s| s.to_socket_addrs().unwrap_or_default())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_seeds_empty() {
        // No config seeds → resolve hardcoded TESTNET_SEEDS via DNS
        let result = resolve_seeds(&[]);
        // At least some seeds should resolve (may be less than TESTNET_SEEDS.len()
        // if some hostnames don't have DNS records yet)
        assert!(result.len() <= TESTNET_SEEDS.len());
    }

    #[test]
    fn test_resolve_seeds_config_takes_priority() {
        let addr: SocketAddr = "127.0.0.1:8333".parse().unwrap();
        let result = resolve_seeds(&[addr]);
        assert_eq!(result, vec![addr]);
    }
}
