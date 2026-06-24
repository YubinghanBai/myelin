use tracing_subscriber::EnvFilter;

use myelin::Result;
use myelin::config::AppConfig;
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

    let cfg = AppConfig::from_env()?;

    if cfg.skip_schema {
        tracing::info!("MYELIN_SKIP_SCHEMA set — not applying schema/events.sql");
    } else {
        ensure_events_table(&cfg.admin).await?;
    }

    ensure_publication_includes_table(&cfg.admin, &cfg.replication.publication_name, &cfg.table)
        .await?;
    ensure_logical_slot(&cfg.admin, &cfg.replication).await?;

    let mut publisher = ReplicationPublisher::from_config(cfg.jetstream.as_ref()).await?;

    if cfg.jetstream.is_some() {
        tracing::info!("NATS_URL set — publishing to JetStream");
    } else {
        tracing::info!("NATS_URL unset — dry-run logging only (set NATS_URL for JetStream)");
    }

    if let Some(addr) = init_metrics_from_env()? {
        tracing::info!(%addr, "Prometheus metrics HTTP scrape endpoint (MYELIN_METRICS_ADDR)");
    }

    tracing::info!("starting replication (Ctrl+C or SIGTERM for graceful exit)");
    run_replication(&cfg.replication, &mut publisher).await?;

    Ok(())
}
