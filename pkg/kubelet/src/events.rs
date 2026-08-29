//! Events, as the kubelet is supposed to emit them.
//!
//! **The kubelet emitted none.** Every event on this cluster came from a
//! controller, so `oc describe pod` had an empty Events section for exactly
//! the failures a person is looking at it for: a volume that would not mount,
//! an image that could not be resolved, a container that would not start. The
//! reason existed somewhere — in a log on a node with no shell — and the one
//! surface built for showing it was blank.
//!
//! Reasons and message shapes follow upstream, because the point is that
//! somebody who knows Kubernetes reads the output and recognises it:
//!
//! | reason | when |
//! |---|---|
//! | `FailedMount` | a volume could not be set up |
//! | `Failed` | the container could not be started |
//! | `Pulling` / `Pulled` / `Failed` | image resolution |
//! | `Created` / `Started` | the ordinary lifecycle |

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::Mutex;
use tracing::debug;

/// How often a repeating event is written back.
///
/// Repeats are counted and flushed on this interval rather than written every
/// time, so a mount that fails on every sync costs two writes a minute instead
/// of twenty. This is what renders as `(x12 over 20m)`.
const AGGREGATION_INTERVAL: Duration = Duration::from_secs(30);

struct Seen {
    name: String,
    count: u64,
    first: String,
    last_written: Instant,
}

/// Posts core/v1 Events about pods, attributed to this node's kubelet.
#[derive(Clone)]
pub struct EventRecorder {
    client: reqwest::Client,
    api_url: String,
    node_name: String,
    seen: Arc<Mutex<HashMap<String, Seen>>>,
}

impl EventRecorder {
    pub fn new(client: reqwest::Client, api_url: &str, node_name: &str) -> Self {
        Self {
            client,
            api_url: api_url.trim_end_matches('/').to_string(),
            node_name: node_name.to_string(),
            seen: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Record an event about a pod.
    ///
    /// `involved` is the pod object, so the event carries its uid — an event
    /// that outlives a recreated pod of the same name must not attach itself
    /// to the new one.
    pub async fn pod_event(&self, pod: &Value, etype: &str, reason: &str, message: &str) {
        let meta = &pod["metadata"];
        let namespace = meta["namespace"].as_str().unwrap_or("default");
        let name = meta["name"].as_str().unwrap_or("");
        let uid = meta["uid"].as_str().unwrap_or("");
        if name.is_empty() {
            return;
        }
        // Two clocks, deliberately.
        //
        // `firstTimestamp` and `lastTimestamp` are `metav1.Time` — RFC3339 to
        // the second. `eventTime` is `metav1.MicroTime` and **requires
        // microseconds**: a plain RFC3339 there fails to unmarshal, and the
        // client discards the whole EventList rather than one field. That is
        // why `oc describe` printed `Events: <none>` while the API was
        // returning twenty-five of them and `oc get events` listed them fine.
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let now_micro = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
        let key = format!("{namespace}/{name}/{uid}/{etype}/{reason}/{message}");

        let mut seen = self.seen.lock().await;
        if let Some(prev) = seen.get_mut(&key) {
            prev.count += 1;
            if prev.last_written.elapsed() < AGGREGATION_INTERVAL {
                return;
            }
            prev.last_written = Instant::now();
            let url = format!(
                "{}/api/v1/namespaces/{namespace}/events/{}",
                self.api_url, prev.name
            );
            let patch = json!({
                "count": prev.count,
                "firstTimestamp": prev.first,
                "lastTimestamp": now,
                "eventTime": now_micro,
            });
            let _ = self
                .client
                .patch(&url)
                .header("content-type", "application/strategic-merge-patch+json")
                .json(&patch)
                .send()
                .await;
            return;
        }

        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let event_name = format!("{name}.{}", &suffix[..16]);
        seen.insert(
            key,
            Seen {
                name: event_name.clone(),
                count: 1,
                first: now.clone(),
                last_written: Instant::now(),
            },
        );
        drop(seen);

        let event = json!({
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": { "name": event_name, "namespace": namespace },
            "involvedObject": {
                "apiVersion": "v1",
                "kind": "Pod",
                "namespace": namespace,
                "name": name,
                "uid": uid,
            },
            "reason": reason,
            "message": message,
            "type": etype,
            // Upstream attributes kubelet events to the node, which is what
            // makes `oc describe` print "kubelet, <node>" as the source.
            "source": { "component": "kubelet", "host": self.node_name },
            "reportingComponent": "kubelet",
            "reportingInstance": self.node_name,
            "firstTimestamp": now,
            "lastTimestamp": now,
            "eventTime": now_micro,
            "count": 1,
        });

        let url = format!("{}/api/v1/namespaces/{namespace}/events", self.api_url);
        if let Err(e) = self.client.post(&url).json(&event).send().await {
            debug!("could not record event {reason} for {namespace}/{name}: {e}");
        }
    }
}

/// The message upstream's kubelet writes when a hostPath is not what the pod
/// declared it to be.
///
/// Matched deliberately: somebody who has read this line on a Kubernetes
/// cluster should recognise it here without being told it means the same
/// thing.
pub fn failed_mount_message(volume: &str, path: &str, declared: &str) -> String {
    let what = match declared {
        "FileOrCreate" | "File" => "is not a file",
        "" => "does not exist",
        _ => "is not a directory",
    };
    format!(
        "MountVolume.SetUp failed for volume \"{volume}\" : hostPath type check failed: \
         {path} {what}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wording is upstream's on purpose — it is what makes the output
    /// recognisable to somebody who has debugged this on Kubernetes.
    #[test]
    fn the_failed_mount_message_matches_upstream() {
        let m = failed_mount_message("lib-modules", "/lib/modules", "");
        assert!(m.starts_with("MountVolume.SetUp failed for volume \"lib-modules\""), "{m}");
        assert!(m.contains("hostPath type check failed"), "{m}");
        assert!(m.contains("/lib/modules"), "{m}");

        // A file-typed volume says "is not a file", which is the distinction
        // that tells you whether you created the wrong kind of thing.
        assert!(failed_mount_message("x", "/run/x.lock", "FileOrCreate").contains("is not a file"));
        assert!(failed_mount_message("x", "/d", "Directory").contains("is not a directory"));
    }
}
