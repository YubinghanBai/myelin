//! **myelin** — skeleton for Postgres logical replication (`pgoutput`) with extension
//! traits for NATS and LSN advancement.
//!
//! ## `tokio-postgres` note
//! Published `tokio-postgres` does **not** implement the CopyBoth half of the protocol
//! used by `START_REPLICATION`. Admin/catalog work uses `tokio-postgres`; the streaming
//! client uses `pgwire-replication` behind [`pg::stream`]. If an upstream or fork exposes
//! the same wire subset, swap the implementation behind [`pg::stream::run_replication`].

pub mod config;
pub mod error;
pub mod pg;
mod prom;

pub use config::{JetStreamConfig, OversizedPayloadPolicy, PublishRetryConfig};
pub use error::{MyelinError, Result};
pub use pg::stream::{ReplicationPublisher, run_replication};

pub use prom::init_from_env as init_metrics_from_env;
