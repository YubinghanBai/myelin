use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use myelin::pg::pgoutput::ChangeEnvelope;

fn sample_envelope() -> ChangeEnvelope {
    let mut row = serde_json::Map::new();
    row.insert(
        "id".into(),
        serde_json::json!("550e8400-e29b-41d4-a716-446655440000"),
    );
    row.insert("correlation_id".into(), serde_json::json!("bench-cid-1"));
    row.insert("kind".into(), serde_json::json!("insert"));
    row.insert("claim_uri".into(), serde_json::json!("s3://bucket/key"));
    row.insert(
        "claim_meta".into(),
        serde_json::json!({ "x": 1, "tag": "criterion" }),
    );
    ChangeEnvelope {
        lsn_hex: "0/16ABCDEF".into(),
        tx_xid: Some(999_001),
        op: "insert",
        schema: "public".into(),
        table: "events".into(),
        rel_id: 16_384,
        row,
        old_row: None,
    }
}

fn envelope_json_serialize(c: &mut Criterion) {
    let env = sample_envelope();
    c.bench_function("envelope_json_serialize", |b| {
        b.iter(|| serde_json::to_vec(black_box(&env)).unwrap())
    });
}

criterion_group!(benches, envelope_json_serialize);
criterion_main!(benches);
