//! Peer management — connection tracking, per-IP limits, scoring, and banning.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tracing::warn;

use crate::protocol::Message;

/// Maximum outbound connections.
pub const MAX_OUTBOUND: usize = 16;
/// Maximum inbound connections.
pub const MAX_INBOUND: usize = 1024;
/// Maximum simultaneous connections from a single IP.
/// Set to 2 to limit FD usage while still allowing NAT traversal retries.
const MAX_CONNECTIONS_PER_IP: u32 = 2;
/// Score threshold below which a peer IP is banned.
const SCORE_BAN_THRESHOLD: i32 = -100;
/// Duration a banned IP remains blocked.
const BAN_DURATION: Duration = Duration::from_secs(3_600); // 1 hour
/// Maximum new connection attempts from a single IP per minute.
const MAX_CONN_RATE_PER_MINUTE: usize = 10;
/// Time window for connection rate tracking.
const RATE_WINDOW: Duration = Duration::from_secs(60);

/// Behavior penalty values.
pub const PENALTY_INVALID_BLOCK: i32 = 40;
pub const PENALTY_BAD_MESSAGE:    i32 = 20;

/// Direction of a peer connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerDirection {
    Inbound,
    Outbound,
}

/// Information about a connected peer.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub addr: SocketAddr,
    pub direction: PeerDirection,
    pub connected_at: Instant,
    pub version: u8,
    pub user_agent: String,
    pub last_seen: Instant,
    /// Behavior score — starts at 0, decremented for misbehavior. Ban at -100.
    pub score: i32,
    /// ISO 3166-1 alpha-2 country code (resolved from IP via geo-lookup).
    pub country: String,
    /// Anchor peers (seed nodes) are never evicted and are always reconnected.
    pub is_anchor: bool,
    /// Channel for sending messages to this peer's writer task.
    /// None until the peer handler sets it up.
    #[allow(clippy::type_complexity)]
    broadcast_tx: Option<UnboundedSender<Message>>,
}

impl PeerInfo {
    /// Take the broadcast sender out (for graceful shutdown).
    pub fn take_broadcast_tx(&mut self) -> Option<UnboundedSender<Message>> {
        self.broadcast_tx.take()
    }
}

/// Manages connected peers: connection limits, per-IP caps, scoring, and banning.
#[derive(Debug, Default)]
pub struct PeerManager {
    peers: HashMap<SocketAddr, PeerInfo>,
    /// Active connection count per remote IP.
    connections_per_ip: HashMap<IpAddr, u32>,
    /// Banned IPs and the instant they were banned.
    banned: HashMap<IpAddr, Instant>,
    /// Connection attempt timestamps per IP (for rate limiting).
    connection_attempts: HashMap<IpAddr, Vec<Instant>>,
    /// IPs that are anchor seeds — always reconnected, never evicted.
    anchor_ips: HashSet<IpAddr>,
}

impl PeerManager {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
            connections_per_ip: HashMap::new(),
            banned: HashMap::new(),
            connection_attempts: HashMap::new(),
            anchor_ips: HashSet::new(),
        }
    }

    /// Register the set of anchor IPs (seed nodes).
    /// Must be called at startup before any peers connect.
    pub fn set_anchor_ips(&mut self, ips: HashSet<IpAddr>) {
        self.anchor_ips = ips;
    }

    /// Returns true if this IP is a known anchor seed.
    pub fn is_anchor_ip(&self, ip: IpAddr) -> bool {
        self.anchor_ips.contains(&ip)
    }

    /// How many anchor seeds are currently connected.
    pub fn anchor_connected_count(&self) -> usize {
        self.peers.values().filter(|p| p.is_anchor).count()
    }

    /// Evict the oldest non-anchor outbound peer to make room for an anchor.
    /// Returns the broadcast_tx of the evicted peer (drop it to close the connection).
    pub fn evict_oldest_non_anchor_outbound(&mut self) -> Option<(SocketAddr, Option<UnboundedSender<Message>>)> {
        let oldest_addr = self.peers.values()
            .filter(|p| p.direction == PeerDirection::Outbound && !p.is_anchor)
            .min_by_key(|p| p.connected_at)
            .map(|p| p.addr);
        if let Some(addr) = oldest_addr {
            if let Some(mut peer) = self.peers.remove(&addr) {
                let ip = addr.ip();
                let count = self.connections_per_ip.entry(ip).or_insert(0);
                *count = count.saturating_sub(1);
                if *count == 0 { self.connections_per_ip.remove(&ip); }
                let tx = peer.broadcast_tx.take();
                return Some((addr, tx));
            }
        }
        None
    }

    /// Returns true if the IP is currently banned (auto-expires after 1 hour).
    pub fn is_banned(&mut self, ip: IpAddr) -> bool {
        if let Some(&banned_at) = self.banned.get(&ip) {
            if banned_at.elapsed() < BAN_DURATION {
                return true;
            }
            self.banned.remove(&ip);
        }
        false
    }

    /// Track a new connection attempt from an IP.
    /// Returns false if the IP exceeds MAX_CONN_RATE_PER_MINUTE — caller should
    /// drop the connection. The IP is auto-banned for 1 hour on rate limit violation.
    pub fn track_connection_attempt(&mut self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let attempts = self.connection_attempts.entry(ip).or_default();
        attempts.retain(|t| now.duration_since(*t) < RATE_WINDOW);
        attempts.push(now);
        if attempts.len() > MAX_CONN_RATE_PER_MINUTE {
            warn!("[P2P] Rate limit exceeded from {} ({} attempts/min) — banning 1h", ip, attempts.len());
            self.banned.insert(ip, Instant::now());
            return false;
        }
        true
    }

    /// Ban an IP directly for 1 hour, regardless of whether it has a peer entry.
    /// Use for immediate bans (corrupted messages, wrong genesis, etc.).
    pub fn ban_ip(&mut self, ip: IpAddr) {
        self.banned.insert(ip, Instant::now());
    }

    /// Apply a behavior penalty to a peer.
    /// Returns true if the penalty pushed the score below the ban threshold —
    /// the caller should then disconnect the peer.
    pub fn penalize(&mut self, addr: &SocketAddr, points: i32) -> bool {
        let ip = addr.ip();
        if let Some(peer) = self.peers.get_mut(addr) {
            peer.score -= points;
            if peer.score < SCORE_BAN_THRESHOLD {
                warn!("[P2P] Banning {} for 1 hour (score {})", ip, peer.score);
                self.banned.insert(ip, Instant::now());
                return true;
            }
        }
        false
    }

    /// Number of connected peers.
    pub fn count(&self) -> usize {
        self.peers.len()
    }

    /// Count of inbound peers.
    pub fn inbound_count(&self) -> usize {
        self.peers.values().filter(|p| p.direction == PeerDirection::Inbound).count()
    }

    /// Count of outbound peers.
    pub fn outbound_count(&self) -> usize {
        self.peers.values().filter(|p| p.direction == PeerDirection::Outbound).count()
    }

    /// Check if we can accept a new connection of the given direction.
    pub fn can_accept(&self, direction: PeerDirection) -> bool {
        match direction {
            PeerDirection::Inbound => self.inbound_count() < MAX_INBOUND,
            PeerDirection::Outbound => self.outbound_count() < MAX_OUTBOUND,
        }
    }

    /// Register a new peer.
    /// Returns false if the global limit, per-IP limit, or a ban blocks it.
    pub fn add_peer(&mut self, addr: SocketAddr, direction: PeerDirection) -> bool {
        let ip = addr.ip();
        if self.is_banned(ip) {
            return false;
        }
        let ip_count = self.connections_per_ip.get(&ip).copied().unwrap_or(0);
        if ip_count >= MAX_CONNECTIONS_PER_IP {
            return false;
        }
        if self.peers.contains_key(&addr) || !self.can_accept(direction) {
            return false;
        }
        let now = Instant::now();
        let is_anchor = self.anchor_ips.contains(&addr.ip());
        self.peers.insert(addr, PeerInfo {
            addr,
            direction,
            connected_at: now,
            version: 0,
            user_agent: String::new(),
            last_seen: now,
            score: 0,
            country: String::new(),
            is_anchor,
            broadcast_tx: None,
        });
        *self.connections_per_ip.entry(ip).or_insert(0) += 1;
        true
    }

    /// Update peer info after receiving Hello.
    pub fn update_hello(&mut self, addr: &SocketAddr, version: u8, user_agent: String) {
        if let Some(peer) = self.peers.get_mut(addr) {
            peer.version = version;
            peer.user_agent = user_agent;
            peer.last_seen = Instant::now();
        }
    }

    /// Mark a peer as recently seen (for keepalive).
    pub fn touch(&mut self, addr: &SocketAddr) {
        if let Some(peer) = self.peers.get_mut(addr) {
            peer.last_seen = Instant::now();
        }
    }

    /// Remove a peer and update per-IP connection count.
    pub fn remove_peer(&mut self, addr: &SocketAddr) {
        if self.peers.remove(addr).is_some() {
            let ip = addr.ip();
            let count = self.connections_per_ip.entry(ip).or_insert(0);
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.connections_per_ip.remove(&ip);
            }
        }
    }

    /// Check if a peer is connected.
    pub fn is_connected(&self, addr: &SocketAddr) -> bool {
        self.peers.contains_key(addr)
    }

    /// Get all peer addresses.
    pub fn all_addrs(&self) -> Vec<SocketAddr> {
        self.peers.keys().copied().collect()
    }

    /// Get all peer infos.
    pub fn all_peers(&self) -> Vec<&PeerInfo> {
        self.peers.values().collect()
    }

    /// Set the broadcast channel sender for a peer (called after stream split).
    pub fn set_broadcast_tx(&mut self, addr: &SocketAddr, tx: UnboundedSender<Message>) {
        if let Some(peer) = self.peers.get_mut(addr) {
            peer.broadcast_tx = Some(tx);
        }
    }

    /// Set the country code for a peer (from geo-lookup).
    pub fn set_country(&mut self, addr: &SocketAddr, country: String) {
        if let Some(peer) = self.peers.get_mut(addr) {
            peer.country = country;
        }
    }

    /// Mutable access to the peers map (for targeted cleanup).
    pub fn peers_mut(&mut self) -> &mut HashMap<SocketAddr, PeerInfo> {
        &mut self.peers
    }

    /// Broadcast a message to all connected peers, optionally excluding one.
    pub fn broadcast(&self, msg: Message, exclude: Option<&SocketAddr>) {
        for (addr, peer) in &self.peers {
            if let Some(excl) = exclude {
                if addr == excl { continue; }
            }
            if let Some(ref tx) = peer.broadcast_tx {
                let _ = tx.send(msg.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    #[test]
    fn test_add_and_count() {
        let mut pm = PeerManager::new();
        assert!(pm.add_peer(addr(1000), PeerDirection::Outbound));
        assert!(pm.add_peer(addr(1001), PeerDirection::Inbound));
        assert_eq!(pm.count(), 2);
        assert_eq!(pm.outbound_count(), 1);
        assert_eq!(pm.inbound_count(), 1);
    }

    #[test]
    fn test_no_duplicate() {
        let mut pm = PeerManager::new();
        assert!(pm.add_peer(addr(1000), PeerDirection::Outbound));
        assert!(!pm.add_peer(addr(1000), PeerDirection::Outbound));
        assert_eq!(pm.count(), 1);
    }

    /// Helper: create addresses with unique IPs to avoid per-IP limit
    fn unique_addr(i: usize) -> SocketAddr {
        // Use 10.x.y.z to get unique IPs (up to 16M)
        let b1 = ((i >> 16) & 0xFF) as u8;
        let b2 = ((i >> 8) & 0xFF) as u8;
        let b3 = (i & 0xFF) as u8;
        format!("10.{}.{}.{}:8333", b1, b2, b3).parse().unwrap()
    }

    #[test]
    fn test_outbound_limit() {
        let mut pm = PeerManager::new();
        for i in 0..MAX_OUTBOUND {
            assert!(pm.add_peer(unique_addr(i), PeerDirection::Outbound));
        }
        assert!(!pm.can_accept(PeerDirection::Outbound));
        assert!(!pm.add_peer(unique_addr(9999), PeerDirection::Outbound));
    }

    #[test]
    fn test_inbound_limit() {
        let mut pm = PeerManager::new();
        for i in 0..MAX_INBOUND {
            assert!(pm.add_peer(unique_addr(1000 + i), PeerDirection::Inbound));
        }
        assert!(!pm.can_accept(PeerDirection::Inbound));
    }

    #[test]
    fn test_remove_peer() {
        let mut pm = PeerManager::new();
        pm.add_peer(addr(1000), PeerDirection::Outbound);
        assert!(pm.is_connected(&addr(1000)));
        pm.remove_peer(&addr(1000));
        assert!(!pm.is_connected(&addr(1000)));
        assert_eq!(pm.count(), 0);
    }

    #[test]
    fn test_update_hello() {
        let mut pm = PeerManager::new();
        pm.add_peer(addr(1000), PeerDirection::Outbound);
        pm.update_hello(&addr(1000), 1, "taron/0.1".into());
        let peers = pm.all_peers();
        assert_eq!(peers[0].version, 1);
        assert_eq!(peers[0].user_agent, "taron/0.1");
    }
}
