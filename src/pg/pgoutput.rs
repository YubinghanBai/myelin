//! `pgoutput` → JSON **envelope** for NATS / downstream consumers (control-plane sized payloads).

use std::collections::HashMap;

use serde::Serialize;

use crate::error::{MyelinError, Result};
use crate::pg::decode::{ColumnMeta, RawPgOutputMsg, RelationMeta, TupleCell, TupleData};

pub use crate::pg::decode::parse_pgoutput_messages;

// --- OIDs commonly used in `public.events` (extend as needed) ---
const INT2_OID: u32 = 21;
const INT4_OID: u32 = 23;
const INT8_OID: u32 = 20;
const BOOL_OID: u32 = 16;
const FLOAT4_OID: u32 = 700;
const FLOAT8_OID: u32 = 701;
const TEXT_OID: u32 = 25;
const VARCHAR_OID: u32 = 1043;
const JSON_OID: u32 = 114;
const JSONB_OID: u32 = 3802;
const UUID_OID: u32 = 2950;
const TIMESTAMPTZ_OID: u32 = 1184;
const TIMESTAMP_OID: u32 = 1114;

/// Single change record published to JetStream (Claim Check: `row` stays pointer-sized).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ChangeEnvelope {
    /// Hex-encoded Postgres LSN for this WAL chunk (aligned with `pg_lsn` semantics).
    pub lsn_hex: String,
    /// Postgres transaction id when available (from `ReplicationEvent::Begin`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_xid: Option<u32>,
    pub op: &'static str,
    pub schema: String,
    pub table: String,
    pub rel_id: u32,
    /// Column name → JSON value (primitive, object, or null).
    pub row: serde_json::Map<String, serde_json::Value>,
    /// For `update` when the wire carried old row (`O`) or replica key (`K`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_row: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Map logical tuple + relation metadata into JSON values.
pub fn tuple_to_row(
    rel: &RelationMeta,
    tuple: &TupleData,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    if rel.columns.len() != tuple.cells.len() {
        return Err(MyelinError::PgOutputParse(format!(
            "column count mismatch rel={} tuple={}",
            rel.columns.len(),
            tuple.cells.len()
        )));
    }
    let mut row = serde_json::Map::new();
    for (col, cell) in rel.columns.iter().zip(tuple.cells.iter()) {
        row.insert(col.name.clone(), cell_to_json(col, cell)?);
    }
    Ok(row)
}

fn cell_to_json(col: &ColumnMeta, cell: &TupleCell) -> Result<serde_json::Value> {
    match cell {
        TupleCell::Null | TupleCell::Unchanged => Ok(serde_json::Value::Null),
        TupleCell::Text(b) => decode_text_col(col.type_oid, b),
        TupleCell::Binary(b) => decode_binary_col(col.type_oid, b),
    }
}

fn decode_text_col(type_oid: u32, raw: &[u8]) -> Result<serde_json::Value> {
    let s = std::str::from_utf8(raw).map_err(|e| {
        MyelinError::PgOutputParse(format!("text cell not utf-8 oid={type_oid}: {e}"))
    })?;
    match type_oid {
        BOOL_OID => match s {
            "t" => Ok(serde_json::Value::Bool(true)),
            "f" => Ok(serde_json::Value::Bool(false)),
            _ => Err(MyelinError::PgOutputParse(format!(
                "invalid bool text {s:?}"
            ))),
        },
        INT2_OID | INT4_OID => {
            let n: i64 = s
                .parse()
                .map_err(|e| MyelinError::PgOutputParse(format!("int parse: {e}")))?;
            Ok(serde_json::json!(n))
        }
        INT8_OID => {
            let n: i64 = s
                .parse()
                .map_err(|e| MyelinError::PgOutputParse(format!("int8 parse: {e}")))?;
            Ok(serde_json::json!(n))
        }
        FLOAT4_OID | FLOAT8_OID => {
            let n: f64 = s
                .parse()
                .map_err(|e| MyelinError::PgOutputParse(format!("float parse: {e}")))?;
            Ok(serde_json::json!(n))
        }
        JSON_OID | JSONB_OID => {
            let v: serde_json::Value = serde_json::from_str(s)?;
            Ok(v)
        }
        UUID_OID | TEXT_OID | VARCHAR_OID | TIMESTAMP_OID | TIMESTAMPTZ_OID => {
            Ok(serde_json::Value::String(s.to_owned()))
        }
        _ => Ok(serde_json::Value::String(s.to_owned())),
    }
}

fn decode_binary_col(type_oid: u32, raw: &[u8]) -> Result<serde_json::Value> {
    match type_oid {
        BOOL_OID if raw.len() == 1 => Ok(serde_json::Value::Bool(raw[0] != 0)),
        INT4_OID if raw.len() == 4 => Ok(serde_json::json!(i32::from_be_bytes(
            raw.try_into().unwrap()
        ))),
        INT8_OID if raw.len() == 8 => Ok(serde_json::json!(i64::from_be_bytes(
            raw.try_into().unwrap()
        ))),
        JSONB_OID => {
            if raw.is_empty() {
                return Err(MyelinError::PgOutputParse("empty jsonb binary".into()));
            }
            let json_txt = &raw[1..];
            let txt = std::str::from_utf8(json_txt)
                .map_err(|e| MyelinError::PgOutputParse(format!("jsonb utf-8: {e}")))?;
            Ok(serde_json::from_str(txt)?)
        }
        UUID_OID if raw.len() == 16 => {
            let a: [u8; 16] = raw
                .try_into()
                .map_err(|_| MyelinError::PgOutputParse("uuid len".into()))?;
            Ok(serde_json::Value::String(uuid_binary_to_string(&a)))
        }
        _ => {
            use base64::{Engine, engine::general_purpose::STANDARD};
            Ok(serde_json::json!({
                "encoding": "base64",
                "type_oid": type_oid,
                "data": STANDARD.encode(raw),
            }))
        }
    }
}

fn uuid_binary_to_string(b: &[u8; 16]) -> String {
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Build envelopes from raw pgoutput messages (updates `relations`).
pub fn materialize_messages(
    relations: &mut HashMap<u32, RelationMeta>,
    lsn_hex: &str,
    tx_xid: Option<u32>,
    payload: &[u8],
) -> Result<Vec<ChangeEnvelope>> {
    let raw = parse_pgoutput_messages(payload)?;
    let mut out = Vec::new();
    for msg in raw {
        match msg {
            RawPgOutputMsg::Relation(rel) => {
                relations.insert(rel.id, rel);
            }
            RawPgOutputMsg::Insert { rel_id, tuple } => {
                let Some(rel) = relations.get(&rel_id) else {
                    return Err(MyelinError::PgOutputParse(format!(
                        "Insert for unknown relation id {rel_id} (missing prior Relation message)"
                    )));
                };
                let row = tuple_to_row(rel, &tuple)?;
                out.push(ChangeEnvelope {
                    lsn_hex: lsn_hex.to_owned(),
                    tx_xid,
                    op: "insert",
                    schema: rel.namespace.clone(),
                    table: rel.table.clone(),
                    rel_id,
                    row,
                    old_row: None,
                });
            }
            RawPgOutputMsg::Update {
                rel_id,
                old_tuple,
                new_tuple,
            } => {
                let Some(rel) = relations.get(&rel_id) else {
                    return Err(MyelinError::PgOutputParse(format!(
                        "Update for unknown relation id {rel_id} (missing prior Relation message)"
                    )));
                };
                let old_row = match &old_tuple {
                    Some(t) => Some(tuple_to_row(rel, t)?),
                    None => None,
                };
                let row = tuple_to_row(rel, &new_tuple)?;
                out.push(ChangeEnvelope {
                    lsn_hex: lsn_hex.to_owned(),
                    tx_xid,
                    op: "update",
                    schema: rel.namespace.clone(),
                    table: rel.table.clone(),
                    rel_id,
                    row,
                    old_row,
                });
            }
            RawPgOutputMsg::Delete { rel_id, tuple } => {
                let Some(rel) = relations.get(&rel_id) else {
                    return Err(MyelinError::PgOutputParse(format!(
                        "Delete for unknown relation id {rel_id} (missing prior Relation message)"
                    )));
                };
                let row = tuple_to_row(rel, &tuple)?;
                out.push(ChangeEnvelope {
                    lsn_hex: lsn_hex.to_owned(),
                    tx_xid,
                    op: "delete",
                    schema: rel.namespace.clone(),
                    table: rel.table.clone(),
                    rel_id,
                    row,
                    old_row: None,
                });
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg::decode::ColumnMeta;

    #[test]
    fn tuple_to_json_text_and_bool() {
        let rel = RelationMeta {
            id: 1,
            namespace: "public".into(),
            table: "events".into(),
            replica_identity: 0,
            columns: vec![
                ColumnMeta {
                    name: "kind".into(),
                    type_oid: TEXT_OID,
                    type_mod: -1,
                },
                ColumnMeta {
                    name: "ok".into(),
                    type_oid: BOOL_OID,
                    type_mod: -1,
                },
            ],
        };
        let tuple = TupleData {
            cells: vec![
                TupleCell::Text(b"task_done".to_vec()),
                TupleCell::Text(b"t".to_vec()),
            ],
        };
        let row = tuple_to_row(&rel, &tuple).unwrap();
        assert_eq!(row["kind"], serde_json::json!("task_done"));
        assert_eq!(row["ok"], serde_json::json!(true));
    }

    #[test]
    fn envelope_from_parse_pipeline() {
        let mut rel_buf = Vec::new();
        rel_buf.push(b'R');
        rel_buf.extend_from_slice(&9u32.to_be_bytes());
        let put_str = |v: &mut Vec<u8>, s: &str| {
            let b = s.as_bytes();
            v.extend_from_slice(&(b.len() as i32).to_be_bytes());
            v.extend_from_slice(b);
        };
        put_str(&mut rel_buf, "public");
        put_str(&mut rel_buf, "events");
        rel_buf.push(0u8);
        rel_buf.extend_from_slice(&1i16.to_be_bytes());
        rel_buf.push(0u8);
        put_str(&mut rel_buf, "kind");
        rel_buf.extend_from_slice(&25u32.to_be_bytes());
        rel_buf.extend_from_slice(&(-1i32).to_be_bytes());

        let mut ins = Vec::new();
        ins.push(b'I');
        ins.extend_from_slice(&9u32.to_be_bytes());
        ins.push(b'N');
        ins.extend_from_slice(&1i16.to_be_bytes());
        ins.push(b't');
        let t = b"x";
        ins.extend_from_slice(&(t.len() as i32).to_be_bytes());
        ins.extend_from_slice(t);

        let mut rels: HashMap<u32, RelationMeta> = HashMap::new();
        let _ = materialize_messages(&mut rels, "0/0", Some(42), &rel_buf).unwrap();
        let envs = materialize_messages(&mut rels, "1/A", Some(42), &ins).unwrap();
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].row["kind"], serde_json::json!("x"));
    }

    #[test]
    fn insert_without_relation_errors() {
        let mut ins = Vec::new();
        ins.push(b'I');
        ins.push(0u8);
        ins.extend_from_slice(&99u32.to_be_bytes());
        ins.extend_from_slice(&0i16.to_be_bytes());
        let mut rels = HashMap::new();
        let err = materialize_messages(&mut rels, "0/0", None, &ins).unwrap_err();
        assert!(err.to_string().contains("unknown relation"), "{}", err);
    }

    fn sample_relation_buf() -> Vec<u8> {
        let mut rel_buf = Vec::new();
        rel_buf.push(b'R');
        rel_buf.extend_from_slice(&9u32.to_be_bytes());
        let put_str = |v: &mut Vec<u8>, s: &str| {
            let b = s.as_bytes();
            v.extend_from_slice(&(b.len() as i32).to_be_bytes());
            v.extend_from_slice(b);
        };
        put_str(&mut rel_buf, "public");
        put_str(&mut rel_buf, "events");
        rel_buf.push(0u8);
        rel_buf.extend_from_slice(&1i16.to_be_bytes());
        rel_buf.push(0u8);
        put_str(&mut rel_buf, "kind");
        rel_buf.extend_from_slice(&25u32.to_be_bytes());
        rel_buf.extend_from_slice(&(-1i32).to_be_bytes());
        rel_buf
    }

    fn append_text_tuple(buf: &mut Vec<u8>, text: &[u8]) {
        buf.extend_from_slice(&1i16.to_be_bytes());
        buf.push(b't');
        buf.extend_from_slice(&(text.len() as i32).to_be_bytes());
        buf.extend_from_slice(text);
    }

    #[test]
    fn update_new_only_envelope() {
        let rel_buf = sample_relation_buf();
        let mut up = Vec::new();
        up.push(b'U');
        up.extend_from_slice(&9u32.to_be_bytes());
        up.push(b'N');
        append_text_tuple(&mut up, b"after");

        let mut rels: HashMap<u32, RelationMeta> = HashMap::new();
        let _ = materialize_messages(&mut rels, "0/0", None, &rel_buf).unwrap();
        let envs = materialize_messages(&mut rels, "1/B", Some(7), &up).unwrap();
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].op, "update");
        assert_eq!(envs[0].row["kind"], serde_json::json!("after"));
        assert!(envs[0].old_row.is_none());
    }

    #[test]
    fn update_old_and_new_envelope() {
        let rel_buf = sample_relation_buf();
        let mut up = Vec::new();
        up.push(b'U');
        up.extend_from_slice(&9u32.to_be_bytes());
        up.push(b'O');
        append_text_tuple(&mut up, b"before");
        up.push(b'N');
        append_text_tuple(&mut up, b"after");

        let mut rels: HashMap<u32, RelationMeta> = HashMap::new();
        let _ = materialize_messages(&mut rels, "0/0", None, &rel_buf).unwrap();
        let envs = materialize_messages(&mut rels, "1/C", None, &up).unwrap();
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].op, "update");
        assert_eq!(
            envs[0].old_row.as_ref().unwrap()["kind"],
            serde_json::json!("before")
        );
        assert_eq!(envs[0].row["kind"], serde_json::json!("after"));
    }

    #[test]
    fn delete_envelope() {
        let rel_buf = sample_relation_buf();
        let mut d = Vec::new();
        d.push(b'D');
        d.extend_from_slice(&9u32.to_be_bytes());
        d.push(b'K');
        append_text_tuple(&mut d, b"gone");

        let mut rels: HashMap<u32, RelationMeta> = HashMap::new();
        let _ = materialize_messages(&mut rels, "0/0", None, &rel_buf).unwrap();
        let envs = materialize_messages(&mut rels, "1/D", Some(9), &d).unwrap();
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].op, "delete");
        assert_eq!(envs[0].row["kind"], serde_json::json!("gone"));
    }
}
