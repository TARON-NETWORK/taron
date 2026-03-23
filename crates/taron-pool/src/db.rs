//! PostgreSQL/TimescaleDB persistence for the TARON mining pool.
//!
//! `shares` and `hashrate_snapshots` are TimescaleDB hypertables for
//! efficient time-range queries at scale. `payouts` is a regular table
//! (low write frequency, small size).
//!
//! Connection URL is read from the `DATABASE_URL` environment variable,
//! defaulting to `postgres://taron_pool:taron_pool@localhost/taron_pool`.

use sqlx::{PgPool, Row};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;

// ── Records ───────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ShareRecord {
    pub timestamp_ms: u64,
    pub miner_address: String,
    pub miner_pubkey: String,
    pub worker_name: String,
    pub nonce: u64,
    pub block_index: u64,
    pub is_block: bool,
}

pub struct PayoutRecord {
    pub timestamp_ms: u64,
    pub to_address: String,
    pub amount_micro: u64,
    pub tx_hash: String,
    pub block_index: u64,
}

pub struct HashrateSnapshot {
    pub timestamp_ms: u64,
    pub hashrate_hps: f64,
    pub active_miners: u32,
}

// ── Db ────────────────────────────────────────────────────────────────────────

pub struct Db {
    pool: PgPool,
}

impl Db {
    /// Connect to PostgreSQL and run schema migrations.
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(30)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(url)
            .await?;
        Self::migrate(&pool).await?;
        Ok(Self { pool })
    }

    /// Create tables and TimescaleDB hypertables if they don't exist yet.
    async fn migrate(pool: &PgPool) -> Result<(), sqlx::Error> {
        // Enable TimescaleDB extension. Silently ignored if not installed —
        // the pool still works as plain PostgreSQL in that case.
        let _ = sqlx::query("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
            .execute(pool)
            .await;

        // ── shares ────────────────────────────────────────────────────────────
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS shares (
                timestamp_ms  BIGINT  NOT NULL,
                miner_address TEXT    NOT NULL,
                miner_pubkey  TEXT    NOT NULL,
                worker_name   TEXT    NOT NULL DEFAULT 'default',
                nonce         BIGINT  NOT NULL,
                block_index   BIGINT  NOT NULL,
                is_block      BOOLEAN NOT NULL DEFAULT FALSE
            )",
        )
        .execute(pool)
        .await?;

        // Add worker_name to existing tables that were created before this migration
        let _ = sqlx::query(
            "ALTER TABLE shares ADD COLUMN IF NOT EXISTS worker_name TEXT NOT NULL DEFAULT 'default'",
        )
        .execute(pool)
        .await;

        // TimescaleDB hypertable: 1-day chunks (86 400 000 ms).
        // Ignored if already a hypertable or if TimescaleDB is absent.
        let _ = sqlx::query(
            "SELECT create_hypertable('shares', 'timestamp_ms', \
             chunk_time_interval => 86400000::bigint, if_not_exists => TRUE)",
        )
        .execute(pool)
        .await;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_shares_miner \
             ON shares(miner_address, timestamp_ms DESC)",
        )
        .execute(pool)
        .await?;

        // ── payouts ───────────────────────────────────────────────────────────
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS payouts (
                timestamp_ms  BIGINT NOT NULL,
                to_address    TEXT   NOT NULL,
                amount_micro  BIGINT NOT NULL,
                tx_hash       TEXT   NOT NULL,
                block_index   BIGINT NOT NULL
            )",
        )
        .execute(pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_payouts_miner ON payouts(to_address)",
        )
        .execute(pool)
        .await?;

        // ── hashrate_snapshots ────────────────────────────────────────────────
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS hashrate_snapshots (
                timestamp_ms  BIGINT           NOT NULL,
                hashrate_hps  DOUBLE PRECISION NOT NULL,
                active_miners INTEGER          NOT NULL
            )",
        )
        .execute(pool)
        .await?;

        // TimescaleDB hypertable: 1-hour chunks (3 600 000 ms).
        let _ = sqlx::query(
            "SELECT create_hypertable('hashrate_snapshots', 'timestamp_ms', \
             chunk_time_interval => 3600000::bigint, if_not_exists => TRUE)",
        )
        .execute(pool)
        .await;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_snapshots_ts \
             ON hashrate_snapshots(timestamp_ms DESC)",
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    // ── Writes ─────────────────────────────────────────────────────────────────

    pub async fn insert_share(&self, share: &ShareRecord) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO shares \
             (timestamp_ms, miner_address, miner_pubkey, worker_name, nonce, block_index, is_block) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(share.timestamp_ms as i64)
        .bind(&share.miner_address)
        .bind(&share.miner_pubkey)
        .bind(&share.worker_name)
        .bind(share.nonce as i64)
        .bind(share.block_index as i64)
        .bind(share.is_block)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_payout(&self, payout: &PayoutRecord) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO payouts \
             (timestamp_ms, to_address, amount_micro, tx_hash, block_index) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(payout.timestamp_ms as i64)
        .bind(&payout.to_address)
        .bind(payout.amount_micro as i64)
        .bind(&payout.tx_hash)
        .bind(payout.block_index as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_hashrate_snapshot(
        &self,
        snap: &HashrateSnapshot,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO hashrate_snapshots (timestamp_ms, hashrate_hps, active_miners) \
             VALUES ($1, $2, $3)",
        )
        .bind(snap.timestamp_ms as i64)
        .bind(snap.hashrate_hps)
        .bind(snap.active_miners as i32)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ── Reads ──────────────────────────────────────────────────────────────────

    /// Return all shares with `timestamp_ms >= since_ms`.
    pub async fn shares_since(&self, since_ms: u64) -> Result<Vec<ShareRecord>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT timestamp_ms, miner_address, miner_pubkey, worker_name, nonce, block_index, is_block \
             FROM shares WHERE timestamp_ms >= $1",
        )
        .bind(since_ms as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| ShareRecord {
                timestamp_ms:  r.get::<i64, _>("timestamp_ms") as u64,
                miner_address: r.get("miner_address"),
                miner_pubkey:  r.get("miner_pubkey"),
                worker_name:   r.get("worker_name"),
                nonce:         r.get::<i64, _>("nonce") as u64,
                block_index:   r.get::<i64, _>("block_index") as u64,
                is_block:      r.get("is_block"),
            })
            .collect())
    }

    /// Full share records for a miner since a given timestamp.
    pub async fn shares_by_miner_since_full(
        &self,
        address: &str,
        since_ms: u64,
    ) -> Result<Vec<ShareRecord>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT timestamp_ms, miner_address, miner_pubkey, worker_name, nonce, block_index, is_block \
             FROM shares WHERE miner_address = $1 AND timestamp_ms >= $2",
        )
        .bind(address)
        .bind(since_ms as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| ShareRecord {
                timestamp_ms:  r.get::<i64, _>("timestamp_ms") as u64,
                miner_address: r.get("miner_address"),
                miner_pubkey:  r.get("miner_pubkey"),
                worker_name:   r.get("worker_name"),
                nonce:         r.get::<i64, _>("nonce") as u64,
                block_index:   r.get::<i64, _>("block_index") as u64,
                is_block:      r.get("is_block"),
            })
            .collect())
    }

    /// Count shares by `address` with `timestamp_ms >= since_ms`.
    pub async fn shares_by_miner_since(
        &self,
        address: &str,
        since_ms: u64,
    ) -> Result<u64, sqlx::Error> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM shares \
             WHERE miner_address = $1 AND timestamp_ms >= $2",
        )
        .bind(address)
        .bind(since_ms as i64)
        .fetch_one(&self.pool)
        .await?;
        Ok(count as u64)
    }

    /// Return all payouts for `address`, newest first.
    pub async fn payouts_by_miner(
        &self,
        address: &str,
    ) -> Result<Vec<PayoutRecord>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT timestamp_ms, to_address, amount_micro, tx_hash, block_index \
             FROM payouts WHERE to_address = $1 ORDER BY timestamp_ms DESC",
        )
        .bind(address)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| PayoutRecord {
                timestamp_ms:  r.get::<i64, _>("timestamp_ms") as u64,
                to_address:    r.get("to_address"),
                amount_micro:  r.get::<i64, _>("amount_micro") as u64,
                tx_hash:       r.get("tx_hash"),
                block_index:   r.get::<i64, _>("block_index") as u64,
            })
            .collect())
    }

    pub async fn hashrate_snapshots_since(
        &self,
        since_ms: u64,
    ) -> Result<Vec<HashrateSnapshot>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT timestamp_ms, hashrate_hps, active_miners \
             FROM hashrate_snapshots WHERE timestamp_ms >= $1 \
             ORDER BY timestamp_ms ASC",
        )
        .bind(since_ms as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| HashrateSnapshot {
                timestamp_ms:  r.get::<i64, _>("timestamp_ms") as u64,
                hashrate_hps:  r.get("hashrate_hps"),
                active_miners: r.get::<i32, _>("active_miners") as u32,
            })
            .collect())
    }

    /// Last share timestamp for `address` (0 if none).
    /// Returns `[(worker_name, last_share_ms)]` for all distinct workers of `address`.
    pub async fn workers_for_miner(&self, address: &str) -> Result<Vec<(String, u64)>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT worker_name, MAX(timestamp_ms) AS last_ms \
             FROM shares WHERE miner_address = $1 \
             GROUP BY worker_name \
             ORDER BY last_ms DESC",
        )
        .bind(address)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(|r| (
            r.get::<String, _>("worker_name"),
            r.get::<i64, _>("last_ms") as u64,
        )).collect())
    }

    /// Shares count for `address` + `worker_name` since `since_ms`.
    pub async fn shares_by_worker_since(
        &self,
        address: &str,
        worker: &str,
        since_ms: u64,
    ) -> Result<u64, sqlx::Error> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM shares \
             WHERE miner_address = $1 AND worker_name = $2 AND timestamp_ms >= $3",
        )
        .bind(address)
        .bind(worker)
        .bind(since_ms as i64)
        .fetch_one(&self.pool)
        .await?;
        Ok(count as u64)
    }

    /// Return all payouts, newest first, with offset/limit.
    pub async fn payouts_paginated(
        &self,
        offset: i64,
        limit: i64,
    ) -> Result<(Vec<PayoutRecord>, u64), sqlx::Error> {
        let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM payouts")
            .fetch_one(&self.pool)
            .await?;
        let rows = sqlx::query(
            "SELECT timestamp_ms, to_address, amount_micro, tx_hash, block_index \
             FROM payouts ORDER BY timestamp_ms DESC LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        let records = rows.iter().map(|r| PayoutRecord {
            timestamp_ms:  r.get::<i64, _>("timestamp_ms") as u64,
            to_address:    r.get("to_address"),
            amount_micro:  r.get::<i64, _>("amount_micro") as u64,
            tx_hash:       r.get("tx_hash"),
            block_index:   r.get::<i64, _>("block_index") as u64,
        }).collect();
        Ok((records, total as u64))
    }

    /// Return blocks found by the pool (shares where is_block = true), newest first.
    pub async fn blocks_found_paginated(
        &self,
        offset: i64,
        limit: i64,
    ) -> Result<(Vec<ShareRecord>, u64), sqlx::Error> {
        let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM shares WHERE is_block = TRUE")
            .fetch_one(&self.pool)
            .await?;
        let rows = sqlx::query(
            "SELECT timestamp_ms, miner_address, miner_pubkey, worker_name, nonce, block_index, is_block \
             FROM shares WHERE is_block = TRUE ORDER BY timestamp_ms DESC LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        let records = rows.iter().map(|r| ShareRecord {
            timestamp_ms:  r.get::<i64, _>("timestamp_ms") as u64,
            miner_address: r.get("miner_address"),
            miner_pubkey:  r.get("miner_pubkey"),
            worker_name:   r.get("worker_name"),
            nonce:         r.get::<i64, _>("nonce") as u64,
            block_index:   r.get::<i64, _>("block_index") as u64,
            is_block:      r.get("is_block"),
        }).collect();
        Ok((records, total as u64))
    }

    /// Count total blocks found (shares where is_block = true).
    pub async fn count_blocks_found(&self) -> Result<u64, sqlx::Error> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM shares WHERE is_block = TRUE",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(count as u64)
    }

    pub async fn last_share_ms(&self, address: &str) -> Result<u64, sqlx::Error> {
        let ts: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(timestamp_ms) FROM shares WHERE miner_address = $1",
        )
        .bind(address)
        .fetch_one(&self.pool)
        .await?;
        Ok(ts.unwrap_or(0) as u64)
    }

    /// Returns `{address → pubkey_hex}` for all miners with shares since `since_ms`.
    pub async fn miner_pubkeys_in_round(
        &self,
        since_ms: u64,
    ) -> Result<HashMap<String, String>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT DISTINCT ON (miner_address) miner_address, miner_pubkey \
             FROM shares WHERE timestamp_ms >= $1 \
             ORDER BY miner_address, timestamp_ms DESC",
        )
        .bind(since_ms as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    /// Count distinct miner addresses with shares since `since_ms`.
    pub async fn distinct_miners_since(&self, since_ms: u64) -> Result<usize, sqlx::Error> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT miner_address) FROM shares WHERE timestamp_ms >= $1",
        )
        .bind(since_ms as i64)
        .fetch_one(&self.pool)
        .await?;
        Ok(count as usize)
    }

    /// Count distinct workers (miner_address:worker_name pairs) since `since_ms`.
    pub async fn distinct_workers_since(&self, since_ms: u64) -> Result<usize, sqlx::Error> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT (miner_address || ':' || worker_name)) FROM shares WHERE timestamp_ms >= $1",
        )
        .bind(since_ms as i64)
        .fetch_one(&self.pool)
        .await?;
        Ok(count as usize)
    }

    /// Per-worker share counts since `since_ms` for hashrate computation.
    /// Returns (worker_key, share_count, min_timestamp, max_timestamp).
    pub async fn worker_share_stats_since(&self, since_ms: u64) -> Result<Vec<(String, i64, i64, i64)>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT miner_address || ':' || worker_name AS wkey, \
                    COUNT(*) AS cnt, \
                    MIN(timestamp_ms) AS t_min, \
                    MAX(timestamp_ms) AS t_max \
             FROM shares WHERE timestamp_ms >= $1 \
             GROUP BY miner_address, worker_name",
        )
        .bind(since_ms as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(|r| {
            let wkey: String = r.get("wkey");
            let cnt: i64 = r.get("cnt");
            let t_min: i64 = r.get("t_min");
            let t_max: i64 = r.get("t_max");
            (wkey, cnt, t_min, t_max)
        }).collect())
    }

    /// Sum of `amount_micro` paid to `address` across all payouts.
    pub async fn total_paid(&self) -> Result<u64, sqlx::Error> {
        let total: Option<i64> = sqlx::query_scalar(
            "SELECT COALESCE(SUM(amount_micro), 0)::bigint FROM payouts",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(total.unwrap_or(0) as u64)
    }

    pub async fn total_paid_to_miner(&self, address: &str) -> Result<u64, sqlx::Error> {
        let total: Option<i64> = sqlx::query_scalar(
            "SELECT COALESCE(SUM(amount_micro), 0)::bigint FROM payouts WHERE to_address = $1",
        )
        .bind(address)
        .fetch_one(&self.pool)
        .await?;
        Ok(total.unwrap_or(0) as u64)
    }
}
