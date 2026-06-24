use pgwire_replication::{Lsn, ReplicationClient};

pub(crate) fn update_applied_lsn(client: &ReplicationClient, source: &'static str, lsn: Lsn) {
    client.update_applied_lsn(lsn);

    metrics::gauge!("myelin_replication_last_applied_lsn_raw").set(lsn.as_u64() as f64);
    metrics::gauge!("myelin_replication_last_applied_lsn_by_source_raw", "source" => source)
        .set(lsn.as_u64() as f64);
    metrics::counter!("myelin_replication_applied_lsn_updates_total", "source" => source)
        .increment(1);

    tracing::debug!(
        target: "myelin::replication",
        source,
        applied_lsn = %lsn,
        applied_lsn_raw = lsn.as_u64(),
        "reported applied replication lsn"
    );
}
