use std::str::FromStr;

use crate::error::{MyelinError, Result};

/// What to do when a serialized CDC envelope exceeds [`JetStreamConfig::max_payload_bytes`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OversizedPayloadPolicy {
    /// Return [`crate::MyelinError::PayloadTooLarge`] and stop replication (no silent skip).
    #[default]
    Stall,
    /// Publish a **small** JSON notice to [`JetStreamConfig::dead_letter_subject`], await
    /// PubAck, and **do not** publish the normal subject — then continue and advance LSN.
    /// **Data loss on the business subject** by design; use for pipeline liveness with auditing.
    DeadLetter,
}

impl OversizedPayloadPolicy {
    /// Parse policy from text (e.g. env or CLI). Unknown values → [`Stall`].
    pub fn parse(raw: &str) -> Self {
        let s = raw.trim();
        if s.eq_ignore_ascii_case("dead_letter") || s.eq_ignore_ascii_case("dead-letter") {
            Self::DeadLetter
        } else {
            Self::Stall
        }
    }

    pub fn parse_strict(name: &str, raw: &str) -> Result<Self> {
        let s = raw.trim();
        if s.eq_ignore_ascii_case("stall") {
            Ok(Self::Stall)
        } else if s.eq_ignore_ascii_case("dead_letter") || s.eq_ignore_ascii_case("dead-letter") {
            Ok(Self::DeadLetter)
        } else {
            Err(config_err(format!(
                "{name} must be one of: stall, dead_letter"
            )))
        }
    }

    pub fn from_env() -> Self {
        match std::env::var("MYELIN_OVERSIZED_POLICY") {
            Ok(s) => Self::parse(&s),
            Err(_) => Self::Stall,
        }
    }
}

/// Connection targets for admin (SQL) vs logical replication stream.
#[derive(Clone, Debug)]
pub struct PgAdminConfig {
    pub conn_str: String,
}

#[derive(Clone, Debug)]
pub struct PgReplicationConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub slot_name: String,
    pub publication_name: String,
}

/// Backoff for JetStream publish + PubAck transient failures.
#[derive(Clone, Debug)]
pub struct PublishRetryConfig {
    /// Total attempts per message (first try + retries). Minimum 1.
    pub max_attempts: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for PublishRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            initial_delay_ms: 100,
            max_delay_ms: 5_000,
        }
    }
}

impl PublishRetryConfig {
    pub fn from_env() -> Self {
        let mut c = Self::default();
        if let Ok(s) = std::env::var("MYELIN_PUBLISH_MAX_ATTEMPTS")
            && let Ok(n) = s.trim().parse::<u32>()
        {
            c.max_attempts = n.max(1);
        }
        if let Ok(s) = std::env::var("MYELIN_PUBLISH_RETRY_INITIAL_MS")
            && let Ok(n) = s.trim().parse::<u64>()
        {
            c.initial_delay_ms = n.max(1);
        }
        if let Ok(s) = std::env::var("MYELIN_PUBLISH_RETRY_MAX_MS")
            && let Ok(n) = s.trim().parse::<u64>()
        {
            c.max_delay_ms = n.max(c.initial_delay_ms);
        }
        c
    }

    fn from_lookup<F>(get: &mut F) -> Result<Self>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let mut c = Self::default();
        if let Some(s) = get("MYELIN_PUBLISH_MAX_ATTEMPTS") {
            c.max_attempts = parse_positive("MYELIN_PUBLISH_MAX_ATTEMPTS", &s)?;
        }
        if let Some(s) = get("MYELIN_PUBLISH_RETRY_INITIAL_MS") {
            c.initial_delay_ms = parse_positive("MYELIN_PUBLISH_RETRY_INITIAL_MS", &s)?;
        }
        if let Some(s) = get("MYELIN_PUBLISH_RETRY_MAX_MS") {
            c.max_delay_ms = parse_positive("MYELIN_PUBLISH_RETRY_MAX_MS", &s)?;
        }
        if c.max_delay_ms < c.initial_delay_ms {
            return Err(config_err(
                "MYELIN_PUBLISH_RETRY_MAX_MS must be >= MYELIN_PUBLISH_RETRY_INITIAL_MS",
            ));
        }
        Ok(c)
    }
}

/// JetStream sink (binary enables this when `NATS_URL` is set).
#[derive(Clone, Debug)]
pub struct JetStreamConfig {
    pub url: String,
    pub stream: String,
    pub subject_prefix: String,
    pub max_payload_bytes: usize,
    pub oversized_policy: OversizedPayloadPolicy,
    pub log_envelopes: bool,
    /// JetStream subject for [`OversizedPayloadPolicy::DeadLetter`] notices (must match stream subject filter).
    pub dead_letter_subject: String,
    pub publish_retry: PublishRetryConfig,
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub replication: PgReplicationConfig,
    pub admin: PgAdminConfig,
    pub table: String,
    pub skip_schema: bool,
    pub jetstream: Option<JetStreamConfig>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup<F>(mut get: F) -> Result<Self>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let replication = PgReplicationConfig {
            host: get_or(&mut get, "PGHOST", "127.0.0.1"),
            port: match get("PGPORT") {
                Some(s) => parse_env("PGPORT", &s)?,
                None => 5432,
            },
            user: get_or(&mut get, "PGUSER", "postgres"),
            password: get("PGPASSWORD").unwrap_or_default(),
            database: get_or(&mut get, "PGDATABASE", "postgres"),
            slot_name: get_or(&mut get, "PG_SLOT", "myelin_slot"),
            publication_name: get_or(&mut get, "PG_PUBLICATION", "myelin_pub"),
        };

        let admin = PgAdminConfig {
            conn_str: get("PGADMIN_URL").unwrap_or_else(|| admin_conninfo(&replication)),
        };

        let table = get_or(&mut get, "PG_TABLE", "public.events");
        let skip_schema = match get("MYELIN_SKIP_SCHEMA") {
            Some(s) => parse_bool("MYELIN_SKIP_SCHEMA", &s)?,
            None => false,
        };

        let jetstream = match get("NATS_URL") {
            Some(url) => {
                require_non_empty("NATS_URL", &url)?;
                let subject_prefix = get_or(&mut get, "NATS_SUBJECT_PREFIX", "myelin");
                let subject_prefix = subject_prefix.trim_end_matches('.').to_owned();
                validate_nats_subject("NATS_SUBJECT_PREFIX", &subject_prefix)?;
                let dead_letter_subject =
                    get("MYELIN_DLQ_SUBJECT").unwrap_or_else(|| format!("{subject_prefix}.dlq"));
                validate_nats_subject("MYELIN_DLQ_SUBJECT", &dead_letter_subject)?;
                validate_dlq_under_prefix(&subject_prefix, &dead_letter_subject)?;

                Some(JetStreamConfig {
                    url,
                    stream: {
                        let stream = get_or(&mut get, "NATS_STREAM", "MYELIN");
                        require_non_empty("NATS_STREAM", &stream)?;
                        stream
                    },
                    subject_prefix,
                    max_payload_bytes: match get("MYELIN_MAX_PAYLOAD_BYTES") {
                        Some(s) => parse_positive("MYELIN_MAX_PAYLOAD_BYTES", &s)?,
                        None => 768 * 1024,
                    },
                    oversized_policy: match get("MYELIN_OVERSIZED_POLICY") {
                        Some(s) => {
                            OversizedPayloadPolicy::parse_strict("MYELIN_OVERSIZED_POLICY", &s)?
                        }
                        None => OversizedPayloadPolicy::Stall,
                    },
                    log_envelopes: match get("MYELIN_LOG_ENVELOPE") {
                        Some(s) => parse_bool("MYELIN_LOG_ENVELOPE", &s)?,
                        None => false,
                    },
                    dead_letter_subject,
                    publish_retry: PublishRetryConfig::from_lookup(&mut get)?,
                })
            }
            None => None,
        };

        Ok(Self {
            replication,
            admin,
            table,
            skip_schema,
            jetstream,
        })
    }
}

fn admin_conninfo(repl: &PgReplicationConfig) -> String {
    format!(
        "host={} port={} user={} password={} dbname={}",
        libpq_conninfo_value(&repl.host),
        repl.port,
        libpq_conninfo_value(&repl.user),
        libpq_conninfo_value(&repl.password),
        libpq_conninfo_value(&repl.database)
    )
}

fn libpq_conninfo_value(raw: &str) -> String {
    if !raw.is_empty()
        && raw
            .chars()
            .all(|c| !c.is_ascii_whitespace() && c != '\'' && c != '\\')
    {
        return raw.to_owned();
    }

    let mut out = String::with_capacity(raw.len() + 2);
    out.push('\'');
    for c in raw.chars() {
        if c == '\'' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

fn get_or<F>(get: &mut F, name: &str, default: &str) -> String
where
    F: FnMut(&str) -> Option<String>,
{
    get(name).unwrap_or_else(|| default.to_owned())
}

fn parse_env<T>(name: &str, raw: &str) -> Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    raw.trim()
        .parse::<T>()
        .map_err(|e| config_err(format!("{name} has invalid value {raw:?}: {e}")))
}

fn parse_positive<T>(name: &str, raw: &str) -> Result<T>
where
    T: FromStr + PartialOrd + From<u8> + Copy,
    T::Err: std::fmt::Display,
{
    let value = parse_env::<T>(name, raw)?;
    if value < T::from(1) {
        return Err(config_err(format!("{name} must be >= 1")));
    }
    Ok(value)
}

fn parse_bool(name: &str, raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(config_err(format!(
            "{name} must be boolean: true/false, yes/no, on/off, or 1/0"
        ))),
    }
}

fn require_non_empty(name: &str, raw: &str) -> Result<()> {
    if raw.trim().is_empty() {
        return Err(config_err(format!("{name} must not be empty")));
    }
    Ok(())
}

fn validate_nats_subject(name: &str, raw: &str) -> Result<()> {
    require_non_empty(name, raw)?;
    if raw
        .chars()
        .any(|c| c.is_ascii_whitespace() || c == '*' || c == '>')
    {
        return Err(config_err(format!(
            "{name} must be a concrete NATS subject without whitespace or wildcards"
        )));
    }
    if raw.split('.').any(str::is_empty) {
        return Err(config_err(format!("{name} must not contain empty tokens")));
    }
    Ok(())
}

fn validate_dlq_under_prefix(subject_prefix: &str, dead_letter_subject: &str) -> Result<()> {
    let prefix_with_dot = format!("{subject_prefix}.");
    if dead_letter_subject == subject_prefix || dead_letter_subject.starts_with(&prefix_with_dot) {
        return Ok(());
    }
    Err(config_err(format!(
        "MYELIN_DLQ_SUBJECT must be under NATS_SUBJECT_PREFIX ({subject_prefix}.>) so the JetStream stream captures it"
    )))
}

fn config_err(message: impl Into<String>) -> MyelinError {
    MyelinError::Config(message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn config_from(entries: &[(&str, &str)]) -> Result<AppConfig> {
        let env: HashMap<&str, &str> = entries.iter().copied().collect();
        AppConfig::from_lookup(|name| env.get(name).map(|value| (*value).to_owned()))
    }

    #[test]
    fn oversized_policy_parse() {
        assert_eq!(
            OversizedPayloadPolicy::parse(""),
            OversizedPayloadPolicy::Stall
        );
        assert_eq!(
            OversizedPayloadPolicy::parse("stall"),
            OversizedPayloadPolicy::Stall
        );
        assert_eq!(
            OversizedPayloadPolicy::parse("DEAD_LETTER"),
            OversizedPayloadPolicy::DeadLetter
        );
        assert_eq!(
            OversizedPayloadPolicy::parse("dead-letter"),
            OversizedPayloadPolicy::DeadLetter
        );
    }

    #[test]
    fn publish_retry_default() {
        let c = PublishRetryConfig::default();
        assert_eq!(c.max_attempts, 8);
        assert_eq!(c.initial_delay_ms, 100);
        assert_eq!(c.max_delay_ms, 5000);
    }

    #[test]
    fn default_app_config_is_dry_run() {
        let c = config_from(&[]).unwrap();
        assert_eq!(c.replication.host, "127.0.0.1");
        assert_eq!(c.replication.port, 5432);
        assert_eq!(c.table, "public.events");
        assert!(!c.skip_schema);
        assert!(c.jetstream.is_none());
        assert!(c.admin.conn_str.contains("host=127.0.0.1"));
    }

    #[test]
    fn app_config_validates_numbers() {
        let err = config_from(&[("PGPORT", "not-a-port")]).unwrap_err();
        assert!(err.to_string().contains("PGPORT"), "{err}");

        let err = config_from(&[
            ("NATS_URL", "nats://127.0.0.1:4222"),
            ("MYELIN_MAX_PAYLOAD_BYTES", "0"),
        ])
        .unwrap_err();
        assert!(
            err.to_string().contains("MYELIN_MAX_PAYLOAD_BYTES"),
            "{err}"
        );
    }

    #[test]
    fn app_config_validates_jetstream_subjects() {
        let c = config_from(&[
            ("NATS_URL", "nats://127.0.0.1:4222"),
            ("NATS_SUBJECT_PREFIX", "myelin."),
            ("MYELIN_OVERSIZED_POLICY", "dead_letter"),
            ("MYELIN_LOG_ENVELOPE", "on"),
            ("MYELIN_PUBLISH_MAX_ATTEMPTS", "3"),
        ])
        .unwrap();
        let jet = c.jetstream.unwrap();
        assert_eq!(jet.subject_prefix, "myelin");
        assert_eq!(jet.dead_letter_subject, "myelin.dlq");
        assert!(jet.log_envelopes);
        assert_eq!(jet.publish_retry.max_attempts, 3);

        let err = config_from(&[
            ("NATS_URL", "nats://127.0.0.1:4222"),
            ("MYELIN_DLQ_SUBJECT", "other.dlq"),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("MYELIN_DLQ_SUBJECT"), "{err}");
    }

    #[test]
    fn app_config_validates_booleans_and_retry_bounds() {
        let c = config_from(&[("MYELIN_SKIP_SCHEMA", "yes")]).unwrap();
        assert!(c.skip_schema);

        let err = config_from(&[("MYELIN_SKIP_SCHEMA", "maybe")]).unwrap_err();
        assert!(err.to_string().contains("MYELIN_SKIP_SCHEMA"), "{err}");

        let err = config_from(&[
            ("NATS_URL", "nats://127.0.0.1:4222"),
            ("MYELIN_PUBLISH_RETRY_INITIAL_MS", "200"),
            ("MYELIN_PUBLISH_RETRY_MAX_MS", "100"),
        ])
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("MYELIN_PUBLISH_RETRY_MAX_MS must be >="),
            "{err}"
        );
    }

    #[test]
    fn libpq_conninfo_value_quotes_empty_and_special_values() {
        assert_eq!(libpq_conninfo_value("postgres"), "postgres");
        assert_eq!(libpq_conninfo_value(""), "''");
        assert_eq!(libpq_conninfo_value("two words"), "'two words'");
        assert_eq!(libpq_conninfo_value("pa's\\word"), "'pa\\'s\\\\word'");
    }

    #[test]
    fn admin_conninfo_escapes_password() {
        let repl = PgReplicationConfig {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "postgres".into(),
            password: "p a's".into(),
            database: "postgres".into(),
            slot_name: "slot".into(),
            publication_name: "pub".into(),
        };
        assert!(admin_conninfo(&repl).contains("password='p a\\'s'"));
    }
}
