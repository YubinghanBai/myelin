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

    let pub_ident = quote_ident(publication)?;
    let table = QualifiedTable::parse(schema_table)?;

    let publication_row = client
        .query_opt(
            "SELECT puballtables FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?;

    if let Some(row) = publication_row {
        let pub_all_tables: bool = row.get(0);
        if pub_all_tables {
            tracing::info!(%publication, "publication already includes all tables");
            return Ok(());
        }
    } else {
        let sql = create_publication_sql(&pub_ident, &table);
        match client.execute(&sql, &[]).await {
            Ok(_) => return Ok(()),
            Err(e) if duplicate_object(&e) => {
                tracing::info!(
                    %publication,
                    table = %table.display(),
                    "publication was created concurrently; checking table membership"
                );
            }
            Err(e) => return Err(MyelinError::AdminDb(e)),
        }
    }

    let included: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_publication_tables
                WHERE pubname = $1 AND schemaname = $2 AND tablename = $3
            )",
            &[&publication, &table.schema, &table.table],
        )
        .await?
        .get(0);

    if included {
        tracing::info!(%publication, table = %table.display(), "publication already includes table");
        return Ok(());
    }

    let sql = alter_publication_add_table_sql(&pub_ident, &table);
    match client.execute(&sql, &[]).await {
        Ok(_) => Ok(()),
        Err(e) => {
            if duplicate_object(&e) {
                tracing::info!(%publication, table = %table.display(), "publication already includes table");
                Ok(())
            } else {
                Err(MyelinError::AdminDb(e))
            }
        }
    }
}

/// `CREATE PUBLICATION` / `ALTER PUBLICATION ADD TABLE` duplicate paths use SQLSTATE `42710`.
fn duplicate_object(e: &tokio_postgres::Error) -> bool {
    if e.code() == Some(&SqlState::DUPLICATE_OBJECT) {
        return true;
    }
    let msg = e.to_string();
    let msg = msg.to_lowercase();
    msg.contains("already exists") || msg.contains("already member")
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct QualifiedTable {
    schema: String,
    table: String,
}

impl QualifiedTable {
    fn parse(raw: &str) -> Result<Self> {
        let (schema, table) = raw.split_once('.').ok_or_else(|| {
            MyelinError::PgOutputParse(
                "table name must be schema.table with simple identifiers".into(),
            )
        })?;
        if table.contains('.') {
            return Err(MyelinError::PgOutputParse(
                "table name must be schema.table with simple identifiers".into(),
            ));
        }
        Ok(Self {
            schema: validate_ident(schema)?.to_owned(),
            table: validate_ident(table)?.to_owned(),
        })
    }

    fn quoted(&self) -> String {
        format!(
            "{}.{}",
            quote_validated_ident(&self.schema),
            quote_validated_ident(&self.table)
        )
    }

    fn display(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

fn create_publication_sql(publication: &str, table: &QualifiedTable) -> String {
    format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table};",
        table = table.quoted()
    )
}

fn alter_publication_add_table_sql(publication: &str, table: &QualifiedTable) -> String {
    format!(
        "ALTER PUBLICATION {publication} ADD TABLE {table};",
        table = table.quoted()
    )
}

fn quote_ident(s: &str) -> Result<String> {
    validate_ident(s)?;
    Ok(quote_validated_ident(s))
}

fn quote_validated_ident(s: &str) -> String {
    format!("\"{s}\"")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified_table_quotes_schema_and_table() {
        let table = QualifiedTable::parse("public.events").unwrap();
        assert_eq!(table.schema, "public");
        assert_eq!(table.table, "events");
        assert_eq!(table.quoted(), "\"public\".\"events\"");
        assert_eq!(
            create_publication_sql("\"myelin_pub\"", &table),
            "CREATE PUBLICATION \"myelin_pub\" FOR TABLE \"public\".\"events\";"
        );
        assert_eq!(
            alter_publication_add_table_sql("\"myelin_pub\"", &table),
            "ALTER PUBLICATION \"myelin_pub\" ADD TABLE \"public\".\"events\";"
        );
    }

    #[test]
    fn qualified_table_rejects_unsafe_names() {
        for raw in [
            "events",
            "public.events.extra",
            "public.events;DROP TABLE events",
            "public.ev-ents",
            ".events",
            "public.",
        ] {
            assert!(QualifiedTable::parse(raw).is_err(), "{raw}");
        }
    }

    #[test]
    fn publication_ident_is_quoted_after_validation() {
        assert_eq!(quote_ident("myelin_pub").unwrap(), "\"myelin_pub\"");
        assert!(quote_ident("bad-pub").is_err());
    }
}
