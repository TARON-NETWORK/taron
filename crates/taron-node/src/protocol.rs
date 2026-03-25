//! Wire protocol — length-prefixed JSON messages over TCP.
//!
//! Format: [4-byte big-endian length][JSON payload]
//! Max message size: 1MB.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use taron_core::{Block, Transaction, TxAck};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Maximum message payload size (1MB).
pub const MAX_MESSAGE_SIZE: u32 = 1_048_576;

/// Magic bytes identifying a valid TARON protocol frame.
/// If these bytes are not found, the stream is corrupted.
const MAGIC: [u8; 4] = [0x54, 0x41, 0x52, 0x4E]; // "TARN"

/// Protocol messages exchanged between TARON peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Initial handshake: node announces itself.
    Hello {
        version: u8,
        listen_port: u16,
        user_agent: String,
    },
    /// Request peer list from remote.
    GetPeers,
    /// Response with known peer addresses.
    Peers {
        addrs: Vec<SocketAddr>,
    },
    /// Broadcast a transaction.
    Tx {
        tx: Transaction,
    },
    /// Request all known transaction hashes (for state sync).
    GetTxHashes,
    /// Response with known transaction hashes.
    TxHashes {
        hashes: Vec<String>,
    },
    /// Request specific transactions by hash.
    GetTxs {
        hashes: Vec<String>,
    },
    /// Keepalive ping.
    Ping {
        nonce: u64,
    },
    /// Keepalive pong.
    Pong {
        nonce: u64,
    },

    // ── Chain sync messages ──────────────────────────────────────────────────

    /// Request the remote peer's current chain height.
    GetChainHeight,

    /// Response with the peer's current chain height (number of blocks).
    ChainHeight(u64),

    /// Request a range of blocks by height [from, to] (inclusive).
    GetBlocks {
        from: u64,
        to: u64,
    },

    /// Response carrying the requested blocks.
    Blocks(Vec<Block>),

    /// Announce a newly-mined block (broadcast).
    NewBlock(Block),

    // ── Transaction finality messages ────────────────────────────────────────

    /// Acknowledge a transaction (peer has validated it).
    /// Sent back to originator and broadcast to network.
    TxAck(TxAck),

    /// Request transaction status (for finality tracking).
    GetTxStatus {
        tx_hash: String,
    },

    /// Response with transaction status.
    TxStatus {
        tx_hash: String,
        /// Number of ACKs received (0 if unknown).
        acks: u32,
        /// Whether transaction is confirmed/final.
        is_final: bool,
    },
}

/// Send a message over a stream: [MAGIC 4B][LENGTH 4B][JSON payload].
pub async fn send_message(writer: &mut (impl AsyncWriteExt + Unpin), msg: &Message) -> std::io::Result<()> {
    let payload = serde_json::to_vec(msg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if payload.len() > MAX_MESSAGE_SIZE as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "message too large",
        ));
    }
    let len = (payload.len() as u32).to_be_bytes();
    // Write magic + length + payload in a single buffer to avoid partial writes
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.extend_from_slice(&MAGIC);
    frame.extend_from_slice(&len);
    frame.extend_from_slice(&payload);
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

/// Receive a message from a stream: [MAGIC 4B][LENGTH 4B][JSON payload].
/// Falls back to legacy format (no magic) for backward compatibility.
pub async fn recv_message(reader: &mut (impl AsyncReadExt + Unpin)) -> std::io::Result<Message> {
    let mut header = [0u8; 4];
    reader.read_exact(&mut header).await?;

    let len = if header == MAGIC {
        // New format: magic found, read the actual length next
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        u32::from_be_bytes(len_buf)
    } else {
        // Legacy format (no magic): these 4 bytes ARE the length
        let len = u32::from_be_bytes(header);
        if len > MAX_MESSAGE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                format!("message too large: {} bytes, stream corrupted", len),
            ));
        }
        len
    };

    if len > MAX_MESSAGE_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            format!("message too large: {} bytes", len),
        ));
    }
    let mut payload = vec![0u8; len as usize];
    reader.read_exact(&mut payload).await?;
    serde_json::from_slice(&payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn test_send_recv_hello() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            recv_message(&mut stream).await.unwrap()
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let msg = Message::Hello {
            version: 1,
            listen_port: 8333,
            user_agent: "taron/0.1.0".into(),
        };
        send_message(&mut client, &msg).await.unwrap();
        drop(client);

        let received = server.await.unwrap();
        match received {
            Message::Hello { version, listen_port, user_agent } => {
                assert_eq!(version, 1);
                assert_eq!(listen_port, 8333);
                assert_eq!(user_agent, "taron/0.1.0");
            }
            _ => panic!("expected Hello"),
        }
    }

    #[tokio::test]
    async fn test_send_recv_ping_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let msg = recv_message(&mut stream).await.unwrap();
            if let Message::Ping { nonce } = msg {
                send_message(&mut stream, &Message::Pong { nonce }).await.unwrap();
            }
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        send_message(&mut client, &Message::Ping { nonce: 42 }).await.unwrap();
        let resp = recv_message(&mut client).await.unwrap();
        match resp {
            Message::Pong { nonce } => assert_eq!(nonce, 42),
            _ => panic!("expected Pong"),
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_send_recv_peers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            recv_message(&mut stream).await.unwrap()
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let peer_addr: SocketAddr = "192.168.1.1:8333".parse().unwrap();
        let msg = Message::Peers { addrs: vec![peer_addr] };
        send_message(&mut client, &msg).await.unwrap();
        drop(client);

        let received = server.await.unwrap();
        match received {
            Message::Peers { addrs } => {
                assert_eq!(addrs.len(), 1);
                assert_eq!(addrs[0].to_string(), "192.168.1.1:8333");
            }
            _ => panic!("expected Peers"),
        }
    }
}
