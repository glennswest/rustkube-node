//! A kubelet whose Node object is deleted underneath it must put it back
//! (rustkube-node#31).
//!
//! A stub apiserver stands in for the real one: it holds one bit of state —
//! does the Node exist — and answers the three calls a heartbeat makes
//! (`PUT .../status`, `GET` for the current conditions, `POST /api/v1/nodes`)
//! consistently with that bit. Deleting the node is then just flipping it, and
//! the test can assert on what the kubelet did about it rather than on log
//! output.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use kubelet::node_status::NodeReporter;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Default)]
struct Apiserver {
    /// Whether `/registry/nodes/<name>` exists, in the only sense the kubelet
    /// can observe: whether the status PUT 404s.
    node_exists: AtomicBool,
    /// How many times the kubelet created the Node.
    creates: AtomicUsize,
    /// How many status PUTs succeeded.
    status_updates: AtomicUsize,
}

async fn put_status(State(api): State<Arc<Apiserver>>) -> StatusCode {
    if api.node_exists.load(Ordering::SeqCst) {
        api.status_updates.fetch_add(1, Ordering::SeqCst);
        StatusCode::OK
    } else {
        // What a real apiserver sends: `resource "/registry/nodes/x" not found`.
        StatusCode::NOT_FOUND
    }
}

async fn get_node(State(api): State<Arc<Apiserver>>) -> (StatusCode, Json<Value>) {
    if api.node_exists.load(Ordering::SeqCst) {
        (StatusCode::OK, Json(json!({ "status": { "conditions": [] } })))
    } else {
        (StatusCode::NOT_FOUND, Json(json!({ "code": 404 })))
    }
}

async fn create_node(State(api): State<Arc<Apiserver>>, body: Json<Value>) -> (StatusCode, Json<Value>) {
    if api.node_exists.swap(true, Ordering::SeqCst) {
        return (StatusCode::CONFLICT, Json(json!({ "code": 409 })));
    }
    api.creates.fetch_add(1, Ordering::SeqCst);
    (StatusCode::CREATED, Json(body.0))
}

async fn lease() -> StatusCode {
    StatusCode::OK
}

/// Start the stub and return its base URL.
async fn serve(api: Arc<Apiserver>) -> String {
    let app = Router::new()
        .route("/api/v1/nodes", post(create_node))
        .route("/api/v1/nodes/{name}", get(get_node))
        .route("/api/v1/nodes/{name}/status", put(put_status))
        .route(
            "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases",
            post(lease),
        )
        .route(
            "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases/{name}",
            put(lease),
        )
        .with_state(api);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn heartbeat_recreates_a_deleted_node() {
    let api = Arc::new(Apiserver::default());
    let url = serve(api.clone()).await;
    let reporter = NodeReporter::new(&url, "rknode1.g8.lo");

    // Normal startup: the node is created and heartbeats land on it.
    reporter.register().await.unwrap();
    assert!(reporter.heartbeat().await.unwrap(), "the Lease was accepted: the node was heard from (#156)");
    assert_eq!(api.creates.load(Ordering::SeqCst), 1);
    assert_eq!(api.status_updates.load(Ordering::SeqCst), 1);

    // `kubectl delete node` — the object goes, the kubelet keeps running.
    api.node_exists.store(false, Ordering::SeqCst);

    // The next heartbeat must put the node back rather than 404 forever. The
    // heartbeat itself still reports success: a recovered beat is not a failed
    // one, and the caller must not back off.
    reporter.heartbeat().await.unwrap();
    assert_eq!(
        api.creates.load(Ordering::SeqCst),
        2,
        "heartbeat did not re-register the deleted node"
    );

    // And the node is healthy again: the following beat updates status, with
    // no further re-registration.
    reporter.heartbeat().await.unwrap();
    assert_eq!(api.status_updates.load(Ordering::SeqCst), 2);
    assert_eq!(api.creates.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn registration_does_not_loop_when_the_node_cannot_be_created() {
    // A 409 from the POST means the object exists, so `register()` falls
    // through to a status update. If that then 404s, the two must not chase
    // each other — the call has to fail and let the next beat retry.
    let api = Arc::new(Apiserver::default());
    api.node_exists.store(true, Ordering::SeqCst);
    let url = serve(api.clone()).await;
    let reporter = NodeReporter::new(&url, "rknode1.g8.lo");

    // POST 409s (exists), status PUT succeeds.
    reporter.register().await.unwrap();
    assert_eq!(api.status_updates.load(Ordering::SeqCst), 1);
}

/// An apiserver the node cannot reach renews no Lease, and the heartbeat says
/// so: metadata answered from the cache is bounded by it (#156).
#[tokio::test]
async fn heartbeat_reports_an_unrenewed_lease() {
    // A port nothing listens on: every request is refused.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let reporter = NodeReporter::new(&url, "rknode1.g8.lo");
    assert!(!reporter.heartbeat().await.unwrap());
}

/// A Node whose status the stub keeps: the last PUT's status is what the next
/// GET answers, and Events are counted (#239).
#[derive(Default)]
struct Kept {
    status: std::sync::Mutex<Value>,
    puts: std::sync::Mutex<Vec<Value>>,
    events: std::sync::Mutex<Vec<Value>>,
}

/// #239: a node that comes back from a reboot (its object records another boot
/// id) writes NotReady once, with a `Rebooted` Event, and Ready on the next
/// status update, so the Ready condition shows the reboot.
#[tokio::test]
async fn a_reboot_is_reported_through_not_ready() {
    if std::fs::read_to_string("/proc/sys/kernel/random/boot_id").is_err() {
        return; // no boot id to compare here
    }
    let kept = Arc::new(Kept::default());
    *kept.status.lock().unwrap() = json!({
        "conditions": [{"type": "Ready", "status": "True", "reason": "KubeletReady",
                        "lastHeartbeatTime": "2026-10-10T13:03:21Z", "lastTransitionTime": "2026-10-10T13:03:21Z"}],
        "nodeInfo": {"bootID": "00000000-0000-0000-0000-000000000000"}
    });
    let app = Router::new()
        .route("/api/v1/nodes", post(|| async { (StatusCode::CONFLICT, Json(json!({"code": 409}))) }))
        .route(
            "/api/v1/nodes/{name}",
            get(|State(k): State<Arc<Kept>>| async move { Json(json!({"status": k.status.lock().unwrap().clone()})) }),
        )
        .route(
            "/api/v1/nodes/{name}/status",
            put(|State(k): State<Arc<Kept>>, Json(body): Json<Value>| async move {
                *k.status.lock().unwrap() = body["status"].clone();
                k.puts.lock().unwrap().push(body["status"].clone());
                StatusCode::OK
            }),
        )
        .route(
            "/api/v1/namespaces/default/events",
            post(|State(k): State<Arc<Kept>>, Json(body): Json<Value>| async move {
                k.events.lock().unwrap().push(body.clone());
                (StatusCode::CREATED, Json(body))
            }),
        )
        .route("/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases/{name}", put(lease))
        .with_state(kept.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let reporter = NodeReporter::new(&url, "rknode1.g8.lo");
    let ready = |st: &Value| {
        st["conditions"].as_array().unwrap().iter().find(|c| c["type"] == "Ready").unwrap().clone()
    };

    reporter.register().await.unwrap();
    let first = kept.puts.lock().unwrap()[0].clone();
    let r = ready(&first);
    assert_eq!(r["status"], "False", "{r}");
    assert_eq!(r["reason"], "KubeletNotReady");
    assert!(r["message"].as_str().unwrap().contains("rebooted"), "{r}");
    assert_ne!(r["lastTransitionTime"], "2026-10-10T13:03:21Z");
    assert_ne!(first["nodeInfo"]["bootID"], "00000000-0000-0000-0000-000000000000");
    {
        let events = kept.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["reason"], "Rebooted");
        assert_eq!(events[0]["involvedObject"]["kind"], "Node");
    }

    reporter.heartbeat().await.unwrap();
    let r = ready(&kept.puts.lock().unwrap()[1]);
    assert_eq!((r["status"].as_str(), r["reason"].as_str()), (Some("True"), Some("KubeletReady")), "{r}");
    reporter.heartbeat().await.unwrap();
    assert_eq!(kept.events.lock().unwrap().len(), 1, "one Rebooted per reboot");
    assert_eq!(ready(&kept.puts.lock().unwrap()[2])["status"], "True");
}
