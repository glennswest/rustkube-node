//! Endpoints syncer.
//!
//! Lists Services and Endpoints from the API server and updates the service
//! map. Whether the dataplane needs rewriting is decided by the proxy loop,
//! from the rules the map generates (`proxy.rs`).

use crate::client::ApiClient;
use crate::service_map::ServiceMap;
use tracing::debug;

/// Sync services and endpoints from the API server. An error (unreachable,
/// refused, not a list) leaves the map as it was.
pub async fn sync_services_and_endpoints(
    client: &ApiClient,
    service_map: &ServiceMap,
) -> anyhow::Result<()> {
    // Both lists first: a failed second list must not leave Services updated
    // with stale or missing backends.
    let services = client.list("/api/v1/services").await?;
    let endpoints = client.list("/api/v1/endpoints").await?;
    service_map.update_services(&services);
    service_map.update_endpoints(&endpoints);
    debug!(
        "listed {} services, {} endpoint sets",
        services.len(),
        endpoints.len()
    );
    Ok(())
}
