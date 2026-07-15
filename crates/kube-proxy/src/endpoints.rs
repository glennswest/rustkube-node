//! Endpoints syncer.
//!
//! Watches the API server for Service and Endpoints changes,
//! updating the service map and triggering iptables rule regeneration.

use crate::service_map::ServiceMap;
use serde_json::Value;
use tracing::{debug, info};

/// Sync services and endpoints from the API server.
pub async fn sync_services_and_endpoints(
    api_url: &str,
    service_map: &ServiceMap,
    client: &reqwest::Client,
) -> anyhow::Result<bool> {
    let mut changed = false;

    // Fetch all services
    let svc_resp: Value = client
        .get(format!("{api_url}/api/v1/services"))
        .send()
        .await?
        .json()
        .await?;

    let services = svc_resp["items"].as_array().cloned().unwrap_or_default();
    let svc_count = services.len();
    let old_map = service_map.get_all();
    service_map.update_services(&services);

    // Fetch all endpoints
    let ep_resp: Value = client
        .get(format!("{api_url}/api/v1/endpoints"))
        .send()
        .await?
        .json()
        .await?;

    let endpoints = ep_resp["items"].as_array().cloned().unwrap_or_default();
    let ep_count = endpoints.len();
    service_map.update_endpoints(&endpoints);

    let new_map = service_map.get_all();

    // Detect changes (simple length + endpoint count comparison)
    if old_map.len() != new_map.len() {
        changed = true;
    } else {
        for new_svc in &new_map {
            let old_svc = old_map
                .iter()
                .find(|s| s.key == new_svc.key);
            match old_svc {
                Some(old) if old.endpoints.len() != new_svc.endpoints.len() => {
                    changed = true;
                    break;
                }
                None => {
                    changed = true;
                    break;
                }
                _ => {}
            }
        }
    }

    if changed {
        info!("Service map updated: {svc_count} services, {ep_count} endpoint sets");
    } else {
        debug!("No service/endpoint changes");
    }

    Ok(changed)
}
