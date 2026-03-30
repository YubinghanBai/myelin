use std::env;

use tracing_subscriber::EnvFilter;

use myelin::Result;
use myelin::config::{
    JetStreamConfig, OversizedPayloadPolicy, PgAdminConfig, PgReplicationConfig, PublishRetryConfig,
};
use myelin::init_metrics_from_env;
use myelin::pg::admin::{
    ensure_events_table, ensure_logical_slot, ensure_publication_includes_table,
};
use myelin::pg::stream::{ReplicationPublisher, run_replication};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let repl = PgReplicationConfig {
        host: env::var("PGHOST").unwrap_or_else(|_| "127.0.0.1".into()),
        port: env::var("PGPORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5432),
        user: env::var("PGUSER").unwrap_or_else(|_| "postgres".into()),
        password: env::var("PGPASSWORD").unwrap_or_default(),
        database: env::var("PGDATABASE").unwrap_or_else(|_| "postgres".into()),
        slot_name: env::var("PG_SLOT").unwrap_or_else(|_| "myelin_slot".into()),
        publication_name: env::var("PG_PUBLICATION").unwrap_or_else(|_| "myelin_pub".into()),
    };

    let admin = PgAdminConfig {
        conn_str: env::var("PGADMIN_URL").unwrap_or_else(|_| {
            format!(
                "host={} port={} user={} password={} dbname={}",
                repl.host, repl.port, repl.user, repl.password, repl.database
            )
        }),
    };

    let table = env::var("PG_TABLE").unwrap_or_else(|_| "public.events".into());

    let skip_schema = matches!(
        env::var("MYELIN_SKIP_SCHEMA").as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    );
    if skip_schema {
        tracing::info!("MYELIN_SKIP_SCHEMA set — not applying schema/events.sql");
    } else {
        ensure_events_table(&admin).await?;
    }

    ensure_publication_includes_table(&admin, &repl.publication_name, &table).await?;
    ensure_logical_slot(&admin, &repl).await?;

    let jet = env::var("NATS_URL").ok().map(|url| {
        let subject_prefix = env::var("NATS_SUBJECT_PREFIX").unwrap_or_else(|_| "myelin".into());
        let trim_pref = subject_prefix.trim_end_matches('.').to_owned();
        let dead_letter_subject =
            env::var("MYELIN_DLQ_SUBJECT").unwrap_or_else(|_| format!("{trim_pref}.dlq"));
        JetStreamConfig {
            url,
            stream: env::var("NATS_STREAM").unwrap_or_else(|_| "MYELIN".into()),
            subject_prefix,
            max_payload_bytes: env::var("MYELIN_MAX_PAYLOAD_BYTES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(768 * 1024),
            oversized_policy: OversizedPayloadPolicy::from_env(),
            dead_letter_subject,
            publish_retry: PublishRetryConfig::from_env(),
        }
    });

    let mut publisher = ReplicationPublisher::from_config(jet.as_ref()).await?;

    if jet.is_some() {
        tracing::info!("NATS_URL set — publishing to JetStream");
    } else {
        tracing::info!("NATS_URL unset — dry-run logging only (set NATS_URL for JetStream)");
    }

    if let Some(addr) = init_metrics_from_env()? {
        tracing::info!(%addr, "Prometheus metrics HTTP scrape endpoint (MYELIN_METRICS_ADDR)");
    }

    tracing::info!("starting replication (Ctrl+C or SIGTERM for graceful exit)");
    run_replication(&repl, &mut publisher).await?;

    Ok(())
}
