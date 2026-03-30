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

    pub fn from_env() -> Self {
        match std::env::var("MYELIN_OVERSIZED_POLICY") {
            Ok(s) => Self::parse(&s),
            Err(_) => Self::Stall,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::OversizedPayloadPolicy;

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

/// JetStream sink (binary enables this when `NATS_URL` is set).
#[derive(Clone, Debug)]
pub struct JetStreamConfig {
    pub url: String,
    pub stream: String,
    pub subject_prefix: String,
    pub max_payload_bytes: usize,
    pub oversized_policy: OversizedPayloadPolicy,
    /// JetStream subject for [`OversizedPayloadPolicy::DeadLetter`] notices (must match stream subject filter).
    pub dead_letter_subject: String,
}
