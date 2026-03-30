//! Optional Prometheus scrape endpoint (install global `metrics` recorder + HTTP listener).

use std::net::SocketAddr;

use crate::{MyelinError, Result};

/// If `MYELIN_METRICS_ADDR` is set (e.g. `127.0.0.1:9095`), install the Prometheus exporter
/// on the current Tokio runtime and return the bound address. If unset, returns `Ok(None)`.
pub fn init_from_env() -> Result<Option<SocketAddr>> {
    let raw = match std::env::var("MYELIN_METRICS_ADDR") {
        Ok(s) if !s.trim().is_empty() => s,
        _ => return Ok(None),
    };
    let addr: SocketAddr = raw.trim().parse().map_err(|e| {
        MyelinError::MetricsExporter(format!("invalid MYELIN_METRICS_ADDR {raw:?}: {e}"))
    })?;

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .map_err(|e| MyelinError::MetricsExporter(e.to_string()))?;

    Ok(Some(addr))
}
