//! DDL / catalog operations on a **normal** database session (`tokio-postgres`).
//! `CREATE_REPLICATION_SLOT` / `START_REPLICATION` over the replication wire are not
//! exposed here — slot creation uses the supported SQL catalog function; streaming is
//! `crate::pg::stream`.

use tokio_postgres::NoTls;
use tokio_postgres::error::SqlState;

use crate::config::{PgAdminConfig, PgReplicationConfig};
use crate::error::{MyelinError, Result};

/// Ensure a publication exists and includes `table_name` (simple MVP: one table).
pub async fn ensure_publication_includes_table(
    cfg: &PgAdminConfig,
    publication: &str,
    schema_table: &str,
) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(&cfg.conn_str, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "admin postgres connection ended");
        }
    });

    let sql = format!(
        "CREATE PUBLICATION {name} FOR TABLE {tbl};",
        name = validate_ident(publication)?,
        tbl = schema_table
    );

    match client.execute(&sql, &[]).await {
        Ok(_) => Ok(()),
        Err(e) => {
            if publication_create_failed_because_exists(&e) {
                tracing::info!(%publication, "publication already exists");
                Ok(())
            } else {
                Err(MyelinError::AdminDb(e))
            }
        }
    }
}

/// `CREATE PUBLICATION` on an existing name → SQLSTATE `42710` (`duplicate_object`).
/// `e.to_string()` does not always contain the substring `already exists`, so use the code.
fn publication_create_failed_because_exists(e: &tokio_postgres::Error) -> bool {
    if e.code() == Some(&SqlState::DUPLICATE_OBJECT) {
        return true;
    }
    let msg = e.to_string();
    msg.to_lowercase().contains("already exists")
}

/// Idempotent: create `pgoutput` logical slot if missing.
pub async fn ensure_logical_slot(cfg: &PgAdminConfig, repl: &PgReplicationConfig) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(&cfg.conn_str, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "admin postgres connection ended");
        }
    });

    let slot = validate_ident(&repl.slot_name)?;
    let exists: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pg_replication_slots WHERE slot_name = $1",
            &[&repl.slot_name],
        )
        .await?
        .get(0);

    if exists > 0 {
        tracing::info!(slot = %repl.slot_name, "replication slot already present");
        return Ok(());
    }

    let sql = format!(
        "SELECT * FROM pg_create_logical_replication_slot('{slot}', 'pgoutput')",
        slot = slot
    );

    client.simple_query(&sql).await?;
    Ok(())
}

/// Apply bundled `schema/events.sql` (idempotent `IF NOT EXISTS`).
pub async fn ensure_events_table(cfg: &PgAdminConfig) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(&cfg.conn_str, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "admin postgres connection ended");
        }
    });

    let ddl = include_str!("../../schema/events.sql");
    client.batch_execute(ddl).await?;
    Ok(())
}

/// MVP guard: only allow simple identifiers to avoid accidental SQL injection in DDL helpers.
fn validate_ident(s: &str) -> Result<&str> {
    if s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') && !s.is_empty() {
        Ok(s)
    } else {
        Err(MyelinError::PgOutputParse(
            "identifier must be [a-zA-Z0-9_]+ for admin helpers".into(),
        ))
    }
}
