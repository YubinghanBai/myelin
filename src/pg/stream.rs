//! Logical replication loop: `START_REPLICATION` via `pgwire-replication`, pgoutput decode,
//! then [`ReplicationPublisher`] (JetStream ack or dry-run logging).
//!
//! # Applied LSN (two intentional call sites)
//!
//! - **[`ReplicationPublisher::on_xlog_data`]** calls [`JetStreamPublisher::process_xlog_data`] /
//!   [`LoggingPublisher::process_xlog_data`], which **after** successful materialization (and JetStream
//!   PubAck when enabled) invoke [`ReplicationClient::update_applied_lsn`] with that `XLogData` chunk’s
//!   **`wal_end`**.
//! - **[`ReplicationEvent::Commit`]** here invokes `update_applied_lsn` with **`end_lsn`**. This is
//!   **not** a duplicate “second opinion”: `pgwire-replication` may emit Commit as a **standalone**
//!   boundary event with **no** following `XLogData` for the same frame, and **`end_lsn` can be greater
//!   than the last `wal_end`** we already reported (commit WAL record after row data). Skipping Commit
//!   would under-flush the slot. The `pgwire-replication` client keeps reported progress as a
//!   **monotonic maximum** (`SharedProgress::update_applied`), so the two call sites compose safely.
//!
//! If `on_xlog_data` returns `Err` (e.g. [`crate::error::MyelinError::PayloadTooLarge`] under **stall**),
//! we **never** reach the Commit handler for that turn — the loop exits and the slot does not advance past
//! the failed work.

use std::time::Instant;

use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent};

use crate::config::JetStreamConfig;
use crate::config::PgReplicationConfig;
use crate::error::Result;
use crate::pg::publish::{JetStreamPublisher, LoggingPublisher};

/// How to emit decoded rows.
pub enum ReplicationPublisher {
    JetStream(Box<JetStreamPublisher>),
    Logging(LoggingPublisher),
}

impl ReplicationPublisher {
    pub async fn from_config(jet: Option<&JetStreamConfig>) -> Result<Self> {
        Ok(match jet {
            Some(c) => Self::JetStream(Box::new(JetStreamPublisher::new(c).await?)),
            None => Self::Logging(LoggingPublisher::default()),
        })
    }

    async fn on_xlog_data(
        &mut self,
        client: &ReplicationClient,
        wal_end: Lsn,
        tx_xid: Option<u32>,
        data: &[u8],
    ) -> Result<()> {
        match self {
            Self::JetStream(p) => p.process_xlog_data(client, wal_end, tx_xid, data).await,
            Self::Logging(p) => p.process_xlog_data(client, wal_end, tx_xid, data).await,
        }
    }
}

/// Consume replication until stop or error.
pub async fn run_replication(
    cfg: &PgReplicationConfig,
    publisher: &mut ReplicationPublisher,
) -> Result<()> {
    let repl = ReplicationConfig {
        host: cfg.host.clone(),
        port: cfg.port,
        user: cfg.user.clone(),
        password: cfg.password.clone(),
        database: cfg.database.clone(),
        slot: cfg.slot_name.clone(),
        publication: cfg.publication_name.clone(),
        start_lsn: Lsn::ZERO,
        ..Default::default()
    };

    let mut client = ReplicationClient::connect(repl).await?;
    tracing::info!(
        slot = %cfg.slot_name,
        publication = %cfg.publication_name,
        "streaming pgoutput"
    );

    let mut tx_xid: Option<u32> = None;

    while let Some(ev) = client.recv().await? {
        match ev {
            ReplicationEvent::Begin { xid, .. } => {
                metrics::counter!("myelin_replication_events_total", "kind" => "begin")
                    .increment(1);
                tx_xid = Some(xid);
                tracing::debug!(
                    target: "myelin::replication",
                    slot = %cfg.slot_name,
                    event = "begin",
                    tx_xid = xid,
                    "replication_protocol"
                );
            }
            ReplicationEvent::Commit { end_lsn, .. } => {
                metrics::counter!("myelin_replication_events_total", "kind" => "commit")
                    .increment(1);
                metrics::gauge!("myelin_replication_last_commit_end_lsn_raw")
                    .set(end_lsn.as_u64() as f64);
                tx_xid = None;
                tracing::debug!(
                    target: "myelin::replication",
                    slot = %cfg.slot_name,
                    event = "commit",
                    end_lsn = %end_lsn,
                    end_lsn_raw = end_lsn.as_u64(),
                    "applied_lsn_from_commit"
                );
                // See module doc: boundary-only Commit may carry end_lsn beyond last XLogData wal_end.
                client.update_applied_lsn(end_lsn);
            }
            ReplicationEvent::XLogData { wal_end, data, .. } => {
                metrics::counter!("myelin_replication_events_total", "kind" => "xlog_data")
                    .increment(1);
                metrics::histogram!("myelin_replication_xlog_chunk_bytes")
                    .record(data.len() as f64);
                let t0 = Instant::now();
                publisher
                    .on_xlog_data(&client, wal_end, tx_xid, &data)
                    .await?;
                let elapsed = t0.elapsed().as_secs_f64();
                metrics::histogram!("myelin_replication_xlog_chunk_process_seconds")
                    .record(elapsed);
                metrics::gauge!("myelin_replication_last_xlog_wal_end_raw")
                    .set(wal_end.as_u64() as f64);
                tracing::debug!(
                    target: "myelin::replication",
                    slot = %cfg.slot_name,
                    event = "xlog_data",
                    wal_end = %wal_end,
                    wal_end_raw = wal_end.as_u64(),
                    chunk_bytes = data.len(),
                    process_seconds = elapsed,
                    tx_xid,
                    "decoded_and_published_chunk"
                );
            }
            ReplicationEvent::KeepAlive { .. } => {}
            ReplicationEvent::Message { .. } => {}
            ReplicationEvent::StoppedAt { .. } => {
                metrics::counter!("myelin_replication_events_total", "kind" => "stopped")
                    .increment(1);
                tracing::info!(
                    target: "myelin::replication",
                    slot = %cfg.slot_name,
                    event = "stopped",
                    "replication_stream_ended"
                );
                break;
            }
        }
    }

    Ok(())
}
