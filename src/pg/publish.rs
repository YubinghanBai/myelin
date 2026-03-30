//! JetStream publish path: **ack before** advancing Postgres applied LSN for this **`XLogData`** chunk.
//!
//! [`JetStreamPublisher::process_xlog_data`] reports **`wal_end`** after all rows in the chunk are
//! published (PubAck). The replication loop in [`crate::pg::stream`] **also** reports
//! [`ReplicationEvent::Commit`]’s **`end_lsn`** so the slot can advance past the commit record when
//! pgwire emits Commit without another `XLogData` — see `stream.rs` module docs.

use std::collections::HashMap;

use pgwire_replication::{Lsn, ReplicationClient};

use crate::config::{JetStreamConfig, OversizedPayloadPolicy};
use crate::error::{MyelinError, Result};
use crate::pg::decode::RelationMeta;
use crate::pg::pgoutput::{ChangeEnvelope, materialize_messages};

fn dead_letter_notice_value(
    env: &ChangeEnvelope,
    bytes: usize,
    max: usize,
    subject_skipped: &str,
) -> serde_json::Value {
    serde_json::json!({
        "error": "MYELIN_OVERSIZED_PAYLOAD",
        "bytes": bytes,
        "max": max,
        "lsn_hex": env.lsn_hex,
        "tx_xid": env.tx_xid,
        "schema": env.schema,
        "table": env.table,
        "rel_id": env.rel_id,
        "op": env.op,
        "subject_skipped": subject_skipped,
    })
}

/// When `MYELIN_LOG_ENVELOPE=1`, log each JetStream publish like dry-run (`myelin::envelope`) — for e2e / debugging only.
fn log_jetstream_envelope_if_enabled(env: &ChangeEnvelope) -> Result<()> {
    if !matches!(std::env::var("MYELIN_LOG_ENVELOPE").as_deref(), Ok("1")) {
        return Ok(());
    }
    tracing::info!(
        target: "myelin::envelope",
        json = %serde_json::to_string(env)?,
        "cdc_envelope"
    );
    Ok(())
}

fn subject_token(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

pub struct JetStreamPublisher {
    jetstream: async_nats::jetstream::Context,
    /// Prefix for subjects, e.g. `myelin` → `myelin.public.events`.
    subject_prefix: String,
    max_payload_bytes: usize,
    oversized_policy: OversizedPayloadPolicy,
    dead_letter_subject: String,
    relations: HashMap<u32, RelationMeta>,
}

impl JetStreamPublisher {
    pub async fn new(cfg: &JetStreamConfig) -> Result<Self> {
        let client = async_nats::connect(cfg.url.as_str())
            .await
            .map_err(|e| MyelinError::Nats(e.to_string()))?;
        let js = async_nats::jetstream::new(client);

        let wildcard = format!("{}.>", cfg.subject_prefix.trim_end_matches('.'));
        js.get_or_create_stream(async_nats::jetstream::stream::Config {
            name: cfg.stream.clone(),
            subjects: vec![wildcard],
            ..Default::default()
        })
        .await
        .map_err(|e| MyelinError::Nats(e.to_string()))?;

        Ok(Self {
            jetstream: js,
            subject_prefix: cfg.subject_prefix.trim_end_matches('.').to_owned(),
            max_payload_bytes: cfg.max_payload_bytes,
            oversized_policy: cfg.oversized_policy,
            dead_letter_subject: cfg.dead_letter_subject.clone(),
            relations: HashMap::new(),
        })
    }

    fn subject_for(&self, env: &ChangeEnvelope) -> String {
        format!(
            "{}.{}.{}",
            self.subject_prefix,
            subject_token(&env.schema),
            subject_token(&env.table)
        )
    }

    /// Decode `XLogData`, publish each insert (awaiting JetStream ack), then
    /// `ReplicationClient::update_applied_lsn(wal_end)` for this chunk.
    ///
    /// Does **not** replace `Commit`’s `end_lsn` update in [`crate::pg::stream::run_replication`].
    pub async fn process_xlog_data(
        &mut self,
        repl: &ReplicationClient,
        wal_end: Lsn,
        tx_xid: Option<u32>,
        data: &[u8],
    ) -> Result<()> {
        let lsn_hex = wal_end.to_string();
        let envs = materialize_messages(&mut self.relations, &lsn_hex, tx_xid, data)?;
        metrics::counter!("myelin_envelopes_materialized_total").increment(envs.len() as u64);
        metrics::histogram!("myelin_envelopes_per_xlog_chunk").record(envs.len() as f64);
        for env in envs {
            let payload = serde_json::to_vec(&env)?;
            metrics::histogram!("myelin_envelope_json_bytes").record(payload.len() as f64);
            let subject = self.subject_for(&env);
            if payload.len() > self.max_payload_bytes {
                match self.oversized_policy {
                    OversizedPayloadPolicy::Stall => {
                        return Err(MyelinError::PayloadTooLarge {
                            bytes: payload.len(),
                            max: self.max_payload_bytes,
                        });
                    }
                    OversizedPayloadPolicy::DeadLetter => {
                        metrics::counter!("myelin_oversize_dlq_total").increment(1);
                        tracing::error!(
                            target: "myelin::dlq",
                            bytes = payload.len(),
                            max = self.max_payload_bytes,
                            schema = %env.schema,
                            table = %env.table,
                            %subject,
                            "MYELIN_OVERSIZED_PAYLOAD — publishing dead-letter notice; business subject skipped"
                        );
                        let notice = dead_letter_notice_value(
                            &env,
                            payload.len(),
                            self.max_payload_bytes,
                            &subject,
                        );
                        let dlq = serde_json::to_vec(&notice)?;
                        if dlq.len() > self.max_payload_bytes {
                            return Err(MyelinError::PayloadTooLarge {
                                bytes: dlq.len(),
                                max: self.max_payload_bytes,
                            });
                        }
                        let ack_fut = self
                            .jetstream
                            .publish(self.dead_letter_subject.clone(), dlq.into())
                            .await
                            .map_err(|e| MyelinError::Nats(e.to_string()))?;
                        ack_fut
                            .await
                            .map_err(|e| MyelinError::Nats(e.to_string()))?;
                    }
                }
                continue;
            }
            let ack_fut = self
                .jetstream
                .publish(subject, payload.into())
                .await
                .map_err(|e| MyelinError::Nats(e.to_string()))?;
            ack_fut
                .await
                .map_err(|e| MyelinError::Nats(e.to_string()))?;
            metrics::counter!("myelin_jetstream_publish_ack_total", "op" => env.op).increment(1);
            log_jetstream_envelope_if_enabled(&env)?;
        }
        repl.update_applied_lsn(wal_end);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::dead_letter_notice_value;
    use crate::pg::pgoutput::ChangeEnvelope;
    use serde_json::json;

    #[test]
    fn dead_letter_notice_stays_control_plane_sized() {
        let env = ChangeEnvelope {
            lsn_hex: "0/ABCDEF0".into(),
            tx_xid: Some(42),
            op: "insert",
            schema: "public".into(),
            table: "workflow_events".into(),
            rel_id: 16_421,
            row: serde_json::Map::new(),
            old_row: None,
        };
        let v =
            dead_letter_notice_value(&env, 9_000_000, 768 * 1024, "myelin.public.workflow_events");
        assert_eq!(v["error"], json!("MYELIN_OVERSIZED_PAYLOAD"));
        let bytes = serde_json::to_vec(&v).expect("dlq json");
        assert!(
            bytes.len() < 4096,
            "DLQ body must stay tiny (Claim Check): {}",
            bytes.len()
        );
    }
}

/// Stdout / tracing only — advances LSN after materialization (no NATS).
#[derive(Default)]
pub struct LoggingPublisher {
    relations: HashMap<u32, RelationMeta>,
}

impl LoggingPublisher {
    pub async fn process_xlog_data(
        &mut self,
        repl: &ReplicationClient,
        wal_end: Lsn,
        tx_xid: Option<u32>,
        data: &[u8],
    ) -> Result<()> {
        let lsn_hex = wal_end.to_string();
        let envs = materialize_messages(&mut self.relations, &lsn_hex, tx_xid, data)?;
        metrics::counter!("myelin_envelopes_materialized_total").increment(envs.len() as u64);
        metrics::histogram!("myelin_envelopes_per_xlog_chunk").record(envs.len() as f64);
        for env in &envs {
            metrics::counter!("myelin_logging_envelope_total", "op" => env.op).increment(1);
            tracing::info!(
                target: "myelin::envelope",
                json = %serde_json::to_string(env)?,
                "cdc_envelope"
            );
        }
        repl.update_applied_lsn(wal_end);
        Ok(())
    }
}
