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
use tracing::warn;

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
    /// The background sender behind [`EventRecorder::pod_event_later`],
    /// started on first use (so a recorder can be made outside a runtime).
    later: Arc<std::sync::OnceLock<tokio::sync::mpsc::UnboundedSender<Write>>>,
}

/// One write an event comes to: a new Event, or a repeat's count.
enum Write {
    Post { url: String, body: Value, reason: String, namespace: String, name: String },
    Patch { url: String, body: Value },
}

impl EventRecorder {
    pub fn new(client: reqwest::Client, api_url: &str, node_name: &str) -> Self {
        Self {
            client,
            api_url: api_url.trim_end_matches('/').to_string(),
            node_name: node_name.to_string(),
            seen: Arc::new(Mutex::new(HashMap::new())),
            later: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Record an event about a pod.
    ///
    /// `involved` is the pod object, so the event carries its uid — an event
    /// that outlives a recreated pod of the same name must not attach itself
    /// to the new one.
    pub async fn pod_event(&self, pod: &Value, etype: &str, reason: &str, message: &str) {
        self.object_event("v1", "Pod", pod, etype, reason, message).await;
    }

    /// Record an event about a pod without waiting for the apiserver (#134).
    ///
    /// For the ordinary lifecycle (`Pulled`, `Created`, `Started`): a start
    /// awaited three POSTs per container, serially, between creating one
    /// container and the next, and a pod reads Running no sooner for them.
    /// The event is stamped now and queued; one sender writes the queue in
    /// order, so `describe` reads them as before. What explains a failure
    /// stays on [`Self::pod_event`], written before the start moves on.
    pub async fn pod_event_later(&self, pod: &Value, etype: &str, reason: &str, message: &str) {
        let Some(write) = self.prepare("v1", "Pod", pod, etype, reason, message).await else {
            return;
        };
        let queue = self.later.get_or_init(|| {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Write>();
            let client = self.client.clone();
            tokio::spawn(async move {
                while let Some(write) = rx.recv().await {
                    send(&client, write).await;
                }
            });
            tx
        });
        if let Err(unsent) = queue.send(write) {
            // The sender's runtime is gone (a test's, say): write it here.
            send(&self.client, unsent.0).await;
        }
    }

    /// Record an event about any namespaced object, named by its apiVersion
    /// and kind — a `VirtualMachineSnapshot`, say, so `kubectl describe` on it
    /// shows what happened to it (#53).
    pub async fn object_event(
        &self,
        api_version: &str,
        kind: &str,
        obj: &Value,
        etype: &str,
        reason: &str,
        message: &str,
    ) {
        if let Some(write) = self.prepare(api_version, kind, obj, etype, reason, message).await {
            send(&self.client, write).await;
        }
    }

    /// The write an event comes to, stamped now, or `None` for a repeat
    /// inside [`AGGREGATION_INTERVAL`] (counted) or an object with no name.
    async fn prepare(
        &self,
        api_version: &str,
        kind: &str,
        obj: &Value,
        etype: &str,
        reason: &str,
        message: &str,
    ) -> Option<Write> {
        let meta = &obj["metadata"];
        let namespace = meta["namespace"].as_str().unwrap_or("default");
        let name = meta["name"].as_str().unwrap_or("");
        let uid = meta["uid"].as_str().unwrap_or("");
        if name.is_empty() {
            return None;
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
        let key = format!("{kind}/{namespace}/{name}/{uid}/{etype}/{reason}/{message}");

        let mut seen = self.seen.lock().await;
        if let Some(prev) = seen.get_mut(&key) {
            prev.count += 1;
            if prev.last_written.elapsed() < AGGREGATION_INTERVAL {
                return None;
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
            return Some(Write::Patch { url, body: patch });
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
                "apiVersion": api_version,
                "kind": kind,
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
        Some(Write::Post {
            url,
            body: event,
            reason: reason.to_string(),
            namespace: namespace.to_string(),
            name: name.to_string(),
        })
    }
}

/// Write one event to the apiserver.
async fn send(client: &reqwest::Client, write: Write) {
    let (url, event, reason, namespace, name) = match write {
        Write::Patch { url, body } => {
            let _ = client
                .patch(&url)
                .header("content-type", "application/strategic-merge-patch+json")
                .json(&body)
                .send()
                .await;
            return;
        }
        Write::Post { url, body, reason, namespace, name } => (url, body, reason, namespace, name),
    };
    // The *status*, not just the transport.
    //
    // This checked only for a send error, so an apiserver that accepted
    // the connection and rejected the object — a 422 on a field it did
    // not like, a 403, a 404 on a namespace — recorded nothing and said
    // nothing. "No events at all" then looks like a kubelet that never
    // tried, which is the one explanation the logs could not distinguish
    // it from. A rejected event is a bug in what is being sent, and it
    // has to be visible to be fixed.
    //
    // `warn`, not `debug`: events are how a node explains itself, and a
    // node that cannot explain itself has a problem worth a line at the
    // level somebody reads.
    match client.post(&url).json(&event).send().await {
        Err(e) => {
            warn!("could not record event {reason} for {namespace}/{name}: {e}");
        }
        Ok(r) if !r.status().is_success() => {
            let code = r.status();
            let body = r.text().await.unwrap_or_default();
            warn!(
                "apiserver rejected event {reason} for {namespace}/{name}: {code} {}",
                body.trim()
            );
        }
        Ok(_) => {}
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

    /// #134: queued events reach the apiserver, in the order recorded,
    /// without the caller waiting on any of them.
    #[tokio::test]
    async fn queued_events_are_written_in_order() {
        let got: Arc<std::sync::Mutex<Vec<String>>> = Default::default();
        let log = got.clone();
        let app = axum::Router::new().route(
            "/api/v1/namespaces/web/events",
            axum::routing::post(move |axum::Json(b): axum::Json<Value>| {
                let log = log.clone();
                async move {
                    log.lock().unwrap().push(b["reason"].as_str().unwrap_or("").to_string());
                    axum::Json(json!({}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let r = EventRecorder::new(reqwest::Client::new(), &url, "node-1");
        let pod = json!({"metadata": {"namespace": "web", "name": "p", "uid": "u"}});
        for reason in ["Pulled", "Created", "Started"] {
            r.pod_event_later(&pod, "Normal", reason, reason).await;
        }
        for _ in 0..200 {
            if got.lock().unwrap().len() == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(*got.lock().unwrap(), ["Pulled", "Created", "Started"]);
    }

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
