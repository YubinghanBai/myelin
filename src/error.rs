use thiserror::Error;

#[derive(Debug, Error)]
pub enum MyelinError {
    #[error("postgres admin client: {0}")]
    AdminDb(#[from] tokio_postgres::Error),
    #[error("logical replication client: {0}")]
    Replication(#[from] pgwire_replication::PgWireError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("nats: {0}")]
    Nats(String),
    #[error("payload {bytes} bytes exceeds max {max} (Claim Check — keep messages small)")]
    PayloadTooLarge { bytes: usize, max: usize },
    #[error("pgoutput parse: {0}")]
    PgOutputParse(String),
    #[error("metrics exporter: {0}")]
    MetricsExporter(String),
}

pub type Result<T> = std::result::Result<T, MyelinError>;
