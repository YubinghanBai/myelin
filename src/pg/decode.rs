//! Low-level `pgoutput` binary decoding (PostgreSQL logical replication message formats).

use crate::error::{MyelinError, Result};

/// Column metadata from a `Relation` (`R`) message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMeta {
    pub name: String,
    pub type_oid: u32,
    pub type_mod: i32,
}

/// Full relation catalog entry from `pgoutput`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationMeta {
    pub id: u32,
    pub namespace: String,
    pub table: String,
    pub replica_identity: i8,
    pub columns: Vec<ColumnMeta>,
}

/// One cell in logical `TupleData`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleCell {
    Null,
    /// TOAST unchanged — not expected on `Insert`; treated as null for row materialization.
    Unchanged,
    Text(Vec<u8>),
    Binary(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleData {
    pub cells: Vec<TupleCell>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawPgOutputMsg {
    Relation(RelationMeta),
    Insert {
        rel_id: u32,
        tuple: TupleData,
    },
    /// New row after update. `old_tuple` is set when the wire carried `O` (old full row) or `K` (key).
    Update {
        rel_id: u32,
        old_tuple: Option<TupleData>,
        new_tuple: TupleData,
    },
    /// Replica-identity `K` (key) or `O` (old row) tuple.
    Delete {
        rel_id: u32,
        tuple: TupleData,
    },
}

#[derive(Default)]
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

#[derive(Clone, Copy)]
enum NameEncoding {
    /// Wire uses PostgreSQL counted `String`: Int32 length + UTF-8 (tests / some paths).
    Counted,
    /// `pgoutput` sends relation/type **identifiers** null-terminated on the wire (PG16 observed).
    NullTerminated,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take_u8(&mut self) -> Result<u8> {
        let b = self
            .buf
            .get(self.pos)
            .ok_or_else(|| MyelinError::PgOutputParse("unexpected EOF (u8)".into()))?;
        self.pos += 1;
        Ok(*b)
    }

    fn take_i8(&mut self) -> Result<i8> {
        Ok(self.take_u8()? as i8)
    }

    fn take_u32(&mut self) -> Result<u32> {
        if self.pos + 4 > self.buf.len() {
            return Err(MyelinError::PgOutputParse("unexpected EOF (u32)".into()));
        }
        let v = u32::from_be_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    fn take_i32(&mut self) -> Result<i32> {
        Ok(self.take_u32()? as i32)
    }

    fn take_u64(&mut self) -> Result<u64> {
        if self.pos + 8 > self.buf.len() {
            return Err(MyelinError::PgOutputParse("unexpected EOF (u64)".into()));
        }
        let v = u64::from_be_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    fn take_i16(&mut self) -> Result<i16> {
        if self.pos + 2 > self.buf.len() {
            return Err(MyelinError::PgOutputParse("unexpected EOF (i16)".into()));
        }
        let v = i16::from_be_bytes(self.buf[self.pos..self.pos + 2].try_into().unwrap());
        self.pos += 2;
        Ok(v)
    }

    fn take_bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(MyelinError::PgOutputParse(format!(
                "unexpected EOF need {n} bytes"
            )));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// Protocol string: Int32 length + UTF-8 payload (length may be 0).
    fn take_string(&mut self) -> Result<String> {
        let len = self.take_i32()?;
        if len < 0 {
            return Err(MyelinError::PgOutputParse(format!(
                "invalid string length {len}"
            )));
        }
        let len = len as usize;
        let raw = self.take_bytes(len)?;
        String::from_utf8(raw.to_vec())
            .map_err(|e| MyelinError::PgOutputParse(format!("relation string not utf-8: {e}")))
    }

    /// C-style identifier: UTF-8 bytes until `NUL` (not included).
    fn take_c_string(&mut self) -> Result<String> {
        let start = self.pos;
        while self.pos < self.buf.len() && self.buf[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos >= self.buf.len() {
            return Err(MyelinError::PgOutputParse(
                "unexpected EOF in null-terminated string".into(),
            ));
        }
        let raw = &self.buf[start..self.pos];
        self.pos += 1;
        String::from_utf8(raw.to_vec())
            .map_err(|e| MyelinError::PgOutputParse(format!("C string not utf-8: {e}")))
    }

    fn take_name(&mut self, enc: NameEncoding) -> Result<String> {
        match enc {
            NameEncoding::Counted => self.take_string(),
            NameEncoding::NullTerminated => self.take_c_string(),
        }
    }

    fn consumed_from(&self, start: usize) -> usize {
        self.pos - start
    }
}

/// `Relation` (`R`): may be **v1** (`rel_oid`, strings, …) or **v2** (leading `xid`, `rel_oid`, …).
/// Heuristic `word_at_4 > 1024` catches common **table OIDs** but misses **small OIDs** and
/// does not apply to **`Y` (Type)** where the second word is often a **type OID under 1024**.
/// Also tries **counted** vs **null-terminated** identifier strings (PG sends the latter for
/// `Relation` / `Type` names on the wire in common builds; see `NameEncoding`).
fn parse_relation_body(body: &[u8]) -> Result<(RelationMeta, usize)> {
    let mut best: Option<(RelationMeta, usize, i32)> = None;
    for leading_xid in [false, true] {
        for enc in [NameEncoding::Counted, NameEncoding::NullTerminated] {
            if let Ok((ref m, n)) = parse_relation_inner(body, leading_xid, enc) {
                let p = relation_plausibility(m);
                if p < 0 {
                    continue;
                }
                if best.as_ref().is_none_or(|(_, _, pb)| p > *pb) {
                    best = Some((m.clone(), n, p));
                }
            }
        }
    }
    best.map(|(m, n, _)| (m, n))
        .ok_or_else(|| MyelinError::PgOutputParse("failed to parse Relation (all variants)".into()))
}

fn relation_plausibility(m: &RelationMeta) -> i32 {
    if m.table.is_empty() || m.table.len() > 120 {
        return -1;
    }
    if !m
        .table
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return -1;
    }
    if !(m.namespace.is_empty()
        || m.namespace
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_'))
    {
        return -1;
    }
    if m.columns.len() > 1024 {
        return -1;
    }
    let mut s: i32 = 0;
    if m.id > 0 {
        s += 1;
    }
    if m.columns.is_empty() {
        return if m.table.is_empty() { -1 } else { s + 1 };
    }
    for c in &m.columns {
        if c.name.is_empty() || c.name.len() > 120 || c.type_oid == 0 {
            return -1;
        }
        if !c
            .name
            .chars()
            .all(|x| x.is_ascii_alphanumeric() || x == '_')
        {
            return -1;
        }
        s += 1;
    }
    s
}

fn skip_type_body(body: &[u8]) -> Result<usize> {
    for leading_xid in [true, false] {
        // Counted strings first: `NullTerminated` can "match" prefix+len layouts and desync.
        for enc in [NameEncoding::Counted, NameEncoding::NullTerminated] {
            if let Ok(n) = skip_type_inner(body, leading_xid, enc) {
                return Ok(n);
            }
        }
    }
    Err(MyelinError::PgOutputParse(
        "failed to skip Type (Y) message".into(),
    ))
}

fn skip_type_inner(body: &[u8], leading_xid: bool, enc: NameEncoding) -> Result<usize> {
    let mut c = Cursor::new(body);
    let start = c.pos;
    if leading_xid {
        let _ = c.take_u32()?;
    }
    let _oid = c.take_u32()?;
    let _ = c.take_name(enc)?;
    let _ = c.take_name(enc)?;
    Ok(c.consumed_from(start))
}

fn skip_origin_body(body: &[u8]) -> Result<usize> {
    skip_origin_inner(body, NameEncoding::Counted)
        .or_else(|_| skip_origin_inner(body, NameEncoding::NullTerminated))
}

fn skip_origin_inner(body: &[u8], enc: NameEncoding) -> Result<usize> {
    let mut c = Cursor::new(body);
    let start = c.pos;
    let _lsn = c.take_u64()?;
    let _ = c.take_name(enc)?;
    Ok(c.consumed_from(start))
}

fn parse_relation_inner(
    body: &[u8],
    leading_xid: bool,
    name_enc: NameEncoding,
) -> Result<(RelationMeta, usize)> {
    let mut c = Cursor::new(body);
    let start = c.pos;
    if leading_xid {
        let _xid = c.take_u32()?;
    }
    let id = c.take_u32()?;
    let namespace = c.take_name(name_enc)?;
    let table = c.take_name(name_enc)?;
    let replica_identity = c.take_i8()?;
    let ncols = c.take_i16()?;
    if ncols < 0 {
        return Err(MyelinError::PgOutputParse(
            "negative column count in Relation".into(),
        ));
    }
    let ncols = ncols as usize;
    let mut columns = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let _flags = c.take_i8()?;
        let name = c.take_name(name_enc)?;
        let type_oid = c.take_u32()?;
        let type_mod = c.take_i32()?;
        columns.push(ColumnMeta {
            name,
            type_oid,
            type_mod,
        });
    }
    let rel = RelationMeta {
        id,
        namespace,
        table,
        replica_identity,
        columns,
    };
    Ok((rel, c.consumed_from(start)))
}

fn parse_tuple_data(c: &mut Cursor<'_>) -> Result<TupleData> {
    let natts = c.take_i16()?;
    if natts < 0 {
        return Err(MyelinError::PgOutputParse(
            "negative attribute count in TupleData".into(),
        ));
    }
    let natts = natts as usize;
    let mut cells = Vec::with_capacity(natts);
    for _ in 0..natts {
        let kind = c.take_u8()?;
        match kind {
            b'n' => cells.push(TupleCell::Null),
            b'u' => cells.push(TupleCell::Unchanged),
            b't' | b'b' => {
                let len = c.take_i32()?;
                if len < 0 {
                    return Err(MyelinError::PgOutputParse(format!(
                        "negative tuple chunk length {len}"
                    )));
                }
                let len = len as usize;
                let payload = c.take_bytes(len)?.to_vec();
                cells.push(if kind == b't' {
                    TupleCell::Text(payload)
                } else {
                    TupleCell::Binary(payload)
                });
            }
            other => {
                return Err(MyelinError::PgOutputParse(format!(
                    "unknown TupleData kind {other:#x}"
                )));
            }
        }
    }
    Ok(TupleData { cells })
}

/// `Insert` wire layout depends on server / `proto_version` (see PostgreSQL «Logical Replication Message Formats»).
///
/// - **Modern (PG14+, proto 1):** `Int32 relid`, `Byte1 'N'`, `TupleData`
/// - **Proto 2 streaming:** optional leading `Int32 xid`, then `Int32 relid`, `'N'`, `TupleData`
/// - **Legacy:** `Byte1 flags`, `Int32 relid`, `TupleData` (no `'N'` marker)
fn parse_insert_body(body: &[u8]) -> Result<(u32, TupleData, usize)> {
    let mut c = Cursor::new(body);
    let start = c.pos;

    // Proto 2 streaming: xid + oid + 'N' + tuple
    if body.len() >= 9 && body[8] == b'N' {
        let _xid = c.take_u32()?;
        let rel_id = c.take_u32()?;
        let marker = c.take_u8()?;
        if marker != b'N' {
            return Err(MyelinError::PgOutputParse(format!(
                "Insert: expected Ntuple marker after relid, got {marker:#x}"
            )));
        }
        let tuple = parse_tuple_data(&mut c)?;
        return Ok((rel_id, tuple, c.consumed_from(start)));
    }

    // Modern proto 1: oid + 'N' + tuple
    if body.len() >= 5 && body[4] == b'N' {
        let rel_id = c.take_u32()?;
        let marker = c.take_u8()?;
        if marker != b'N' {
            return Err(MyelinError::PgOutputParse(format!(
                "Insert: expected Ntuple marker, got {marker:#x}"
            )));
        }
        let tuple = parse_tuple_data(&mut c)?;
        return Ok((rel_id, tuple, c.consumed_from(start)));
    }

    // Legacy: flags byte + relid + tuple (no 'N')
    let _flags = c.take_u8()?;
    let rel_id = c.take_u32()?;
    let tuple = parse_tuple_data(&mut c)?;
    Ok((rel_id, tuple, c.consumed_from(start)))
}

/// `Update`: `relid`, then `N`+new only, or `O`/`K`+tuple then `N`+new (see PostgreSQL logical-rep docs).
fn parse_update_body(body: &[u8]) -> Result<(u32, Option<TupleData>, TupleData, usize)> {
    let mut c = Cursor::new(body);
    let start = c.pos;
    let proto2 = body.len() >= 9
        && !matches!(body.get(4).copied(), Some(b'O' | b'K' | b'N'))
        && matches!(body.get(8).copied(), Some(b'O' | b'K' | b'N'));
    if proto2 {
        let _xid = c.take_u32()?;
    }
    let rel_id = c.take_u32()?;
    let first = c.take_u8()?;
    let (old_tuple, new_tuple) = match first {
        b'O' | b'K' => {
            let old = parse_tuple_data(&mut c)?;
            let n = c.take_u8()?;
            if n != b'N' {
                return Err(MyelinError::PgOutputParse(format!(
                    "Update: expected N new-tuple marker after old/key, got {n:#x}"
                )));
            }
            let new_t = parse_tuple_data(&mut c)?;
            (Some(old), new_t)
        }
        b'N' => (None, parse_tuple_data(&mut c)?),
        _ => {
            return Err(MyelinError::PgOutputParse(format!(
                "Update: unexpected submessage tag {first:#x}"
            )));
        }
    };
    Ok((rel_id, old_tuple, new_tuple, c.consumed_from(start)))
}

/// `Delete`: `relid`, then `K` or `O` + [`TupleData`].
fn parse_delete_body(body: &[u8]) -> Result<(u32, TupleData, usize)> {
    let mut c = Cursor::new(body);
    let start = c.pos;
    let proto2 = body.len() >= 9
        && !matches!(body.get(4).copied(), Some(b'O' | b'K'))
        && matches!(body.get(8).copied(), Some(b'O' | b'K'));
    if proto2 {
        let _xid = c.take_u32()?;
    }
    let rel_id = c.take_u32()?;
    let tag = c.take_u8()?;
    if tag != b'O' && tag != b'K' {
        return Err(MyelinError::PgOutputParse(format!(
            "Delete: expected O or K, got {tag:#x}"
        )));
    }
    let tuple = parse_tuple_data(&mut c)?;
    Ok((rel_id, tuple, c.consumed_from(start)))
}

/// Parse **all** pgoutput messages packed into one logical chunk (single `XLogData.data` payload).
///
/// `pgwire-replication` already strips standalone `Begin` / `Commit` into
/// [`ReplicationEvent::Begin`] / [`ReplicationEvent::Commit`]; payloads here are normally
/// `Relation`, `Insert`, etc.
pub fn parse_pgoutput_messages(data: &[u8]) -> Result<Vec<RawPgOutputMsg>> {
    let mut out = Vec::new();
    let mut rest = data;
    while !rest.is_empty() {
        let tag = rest[0];
        let body = &rest[1..];
        match tag {
            b'R' => {
                let (rel, consumed) = parse_relation_body(body)?;
                out.push(RawPgOutputMsg::Relation(rel));
                rest = &rest[1 + consumed..];
            }
            b'I' => {
                let (rel_id, tuple, consumed) = parse_insert_body(body)?;
                out.push(RawPgOutputMsg::Insert { rel_id, tuple });
                rest = &rest[1 + consumed..];
            }
            // Type: sent before Relation when new types appear; must not break the chunk parser.
            b'Y' => {
                let n = skip_type_body(body)?;
                rest = &rest[1 + n..];
            }
            b'O' => {
                let n = skip_origin_body(body)?;
                rest = &rest[1 + n..];
            }
            b'U' => {
                let (rel_id, old_tuple, new_tuple, consumed) = parse_update_body(body)?;
                out.push(RawPgOutputMsg::Update {
                    rel_id,
                    old_tuple,
                    new_tuple,
                });
                rest = &rest[1 + consumed..];
            }
            b'D' => {
                let (rel_id, tuple, consumed) = parse_delete_body(body)?;
                out.push(RawPgOutputMsg::Delete { rel_id, tuple });
                rest = &rest[1 + consumed..];
            }
            b'T' => {
                return Err(MyelinError::PgOutputParse(
                    "Truncate (T) not implemented in this build".into(),
                ));
            }
            other => {
                return Err(MyelinError::PgOutputParse(format!(
                    "unknown pgoutput opcode {other:#x}"
                )));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put_string(bytes: &mut Vec<u8>, s: &str) {
        let b = s.as_bytes();
        bytes.extend_from_slice(&(b.len() as i32).to_be_bytes());
        bytes.extend_from_slice(b);
    }

    #[test]
    fn skip_origin_then_relation_cstring_name() {
        let mut buf = Vec::new();
        buf.push(b'O');
        buf.extend_from_slice(&1u64.to_be_bytes()); // lsn placeholder
        buf.extend_from_slice(b"my_origin\0");
        buf.push(b'R');
        buf.extend_from_slice(&16385u32.to_be_bytes());
        let put_c = |v: &mut Vec<u8>, s: &str| {
            v.extend_from_slice(s.as_bytes());
            v.push(0);
        };
        put_c(&mut buf, "public");
        put_c(&mut buf, "t1");
        buf.push(0x64);
        buf.extend_from_slice(&0i16.to_be_bytes());

        let msgs = parse_pgoutput_messages(&buf).expect("O+R");
        assert_eq!(msgs.len(), 1);
        assert!(matches!(&msgs[0], RawPgOutputMsg::Relation(r) if r.table == "t1"));
    }

    #[test]
    fn relation_null_terminated_names_pg16_wire() {
        // From live `pg_recvlogical` capture (proto_version 1): R + OID + C-strings for ns/table/cols.
        let mut buf: Vec<u8> = vec![
            b'R', 0x00, 0x00, 0x40, 0x25, // relid 16421
        ];
        for s in ["public", "events"] {
            buf.extend_from_slice(s.as_bytes());
            buf.push(0);
        }
        buf.push(0x64); // replica identity 'd' (DEFAULT)
        buf.extend_from_slice(&(1i16).to_be_bytes()); // ncols
        buf.push(0); // col flags
        buf.extend_from_slice(b"id\0");
        buf.extend_from_slice(&2950u32.to_be_bytes()); // uuid
        buf.extend_from_slice(&(-1i32).to_be_bytes()); // typmod

        let msgs = parse_pgoutput_messages(&buf).expect("null-term R");
        assert!(
            matches!(&msgs[0], RawPgOutputMsg::Relation(r) if r.table == "events" && r.id == 16421)
        );
    }

    #[test]
    fn skip_type_y_then_relation() {
        let mut buf = Vec::new();
        buf.push(b'Y');
        buf.extend_from_slice(&901u32.to_be_bytes()); // xid
        buf.extend_from_slice(&25u32.to_be_bytes()); // text oid
        put_string(&mut buf, "pg_catalog");
        put_string(&mut buf, "text");
        buf.push(b'R');
        buf.extend_from_slice(&42u32.to_be_bytes());
        put_string(&mut buf, "public");
        put_string(&mut buf, "events");
        buf.push(0u8);
        buf.extend_from_slice(&1i16.to_be_bytes());
        buf.push(0u8);
        put_string(&mut buf, "kind");
        buf.extend_from_slice(&25u32.to_be_bytes());
        buf.extend_from_slice(&(-1i32).to_be_bytes());

        let msgs = parse_pgoutput_messages(&buf).expect("Y+R");
        assert_eq!(msgs.len(), 1);
        assert!(matches!(&msgs[0], RawPgOutputMsg::Relation(r) if r.table == "events"));
    }

    #[test]
    fn relation_proto2_leading_xid() {
        let mut rel = Vec::new();
        rel.push(b'R');
        rel.extend_from_slice(&99u32.to_be_bytes()); // xid
        rel.extend_from_slice(&20_000u32.to_be_bytes()); // rel oid (>1024 → detect v2)
        put_string(&mut rel, "public");
        put_string(&mut rel, "events");
        rel.push(0u8);
        rel.extend_from_slice(&1i16.to_be_bytes());
        rel.push(0u8);
        put_string(&mut rel, "id");
        rel.extend_from_slice(&23u32.to_be_bytes());
        rel.extend_from_slice(&(-1i32).to_be_bytes());

        let msgs = parse_pgoutput_messages(&rel).unwrap();
        let RawPgOutputMsg::Relation(r) = &msgs[0] else {
            panic!();
        };
        assert_eq!(r.id, 20_000);
        assert_eq!(r.table, "events");
        assert_eq!(r.columns.len(), 1);
    }

    #[test]
    fn roundtrip_relation_and_insert() {
        let mut rel = Vec::new();
        rel.push(b'R');
        rel.extend_from_slice(&42u32.to_be_bytes());
        put_string(&mut rel, "public");
        put_string(&mut rel, "events");
        rel.push(0i8 as u8); // replica identity default
        rel.extend_from_slice(&2i16.to_be_bytes());
        // col1
        rel.push(0i8 as u8); // flags
        put_string(&mut rel, "id");
        rel.extend_from_slice(&25u32.to_be_bytes()); // text
        rel.extend_from_slice(&(-1i32).to_be_bytes());
        // col2
        rel.push(0i8 as u8);
        put_string(&mut rel, "claim_uri");
        rel.extend_from_slice(&25u32.to_be_bytes());
        rel.extend_from_slice(&(-1i32).to_be_bytes());

        let msgs = parse_pgoutput_messages(&rel).expect("relation");
        assert!(matches!(
            &msgs[0],
            RawPgOutputMsg::Relation(r) if r.table == "events" && r.columns.len() == 2
        ));

        let mut ins = Vec::new();
        ins.push(b'I');
        ins.extend_from_slice(&42u32.to_be_bytes());
        ins.push(b'N');
        ins.extend_from_slice(&2i16.to_be_bytes());
        ins.push(b't');
        let t = b"hello";
        ins.extend_from_slice(&(t.len() as i32).to_be_bytes());
        ins.extend_from_slice(t);
        ins.push(b't');
        let u = b"s3://bucket/k";
        ins.extend_from_slice(&(u.len() as i32).to_be_bytes());
        ins.extend_from_slice(u);

        let msgs = parse_pgoutput_messages(&ins).expect("insert");
        match &msgs[0] {
            RawPgOutputMsg::Insert { rel_id, tuple } => {
                assert_eq!(*rel_id, 42);
                assert_eq!(tuple.cells.len(), 2);
            }
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn relation_column_key_flag() {
        let mut rel = Vec::new();
        rel.push(b'R');
        rel.extend_from_slice(&7u32.to_be_bytes());
        put_string(&mut rel, "public");
        put_string(&mut rel, "t1");
        rel.push(0u8);
        rel.extend_from_slice(&1i16.to_be_bytes());
        rel.push(1u8); // replica identity full key — allowed flag in PG16
        put_string(&mut rel, "id");
        rel.extend_from_slice(&23u32.to_be_bytes());
        rel.extend_from_slice(&(-1i32).to_be_bytes());

        let msgs = parse_pgoutput_messages(&rel).unwrap();
        let RawPgOutputMsg::Relation(r) = &msgs[0] else {
            panic!();
        };
        assert_eq!(r.columns[0].name, "id");
    }

    #[test]
    fn insert_legacy_flags_format_still_parsed() {
        let mut ins = Vec::new();
        ins.push(b'I');
        ins.push(0u8);
        ins.extend_from_slice(&42u32.to_be_bytes());
        ins.extend_from_slice(&0i16.to_be_bytes());
        let msgs = parse_pgoutput_messages(&ins).unwrap();
        assert!(matches!(msgs[0], RawPgOutputMsg::Insert { rel_id: 42, .. }));
    }

    #[test]
    fn multi_message_buffer() {
        let mut buf = Vec::new();
        buf.push(b'R');
        buf.extend_from_slice(&1u32.to_be_bytes());
        put_string(&mut buf, "s");
        put_string(&mut buf, "t");
        buf.push(0u8);
        buf.extend_from_slice(&0i16.to_be_bytes());

        buf.push(b'I');
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&0i16.to_be_bytes());

        let msgs = parse_pgoutput_messages(&buf).unwrap();
        assert_eq!(msgs.len(), 2);
    }
}
