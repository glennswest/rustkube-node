//! Every container's own root filesystem (#104).
//!
//! **The owner's rule:** a container's root is its own copy-on-write clone of
//! its image's *sealed golden*, writable, deleted with the container. A
//! restart is a new container and gets a fresh clone, as upstream gives a
//! restarted container a fresh writable layer. Containers never share a root,
//! and exactly one layer sits between the golden and what the workload writes:
//! golden → container root, never golden → image clone → container clone.
//!
//! Before this, every container of an image ran on one directory: a pallet's
//! mount (`/pallets/<x>`), or the one clone a pull made per image
//! (`/run/stormpump/images/<clone>`), so a container writing its root wrote
//! every other container's.
//!
//! Where the golden is:
//!
//! - **A pull** (`template:<name>`): sbregistry's golden record names the
//!   fstemplate it sealed in this node's engine (`template_name`). Cloned
//!   through `POST /api/v1/fstemplates/{name}/clone`, which stamps the clone
//!   with its own filesystem UUID, as a claim's blank is.
//! - **A pallet** (`/pallets/<path>`): the slab holds `<vol>.golden`, sealed,
//!   and its first clone `<vol>`, which the initramfs mounts at `/p/<path>`
//!   as `rd.stormblock.mount=<vol>:/p/<path>` says (`/pallets` links to `/p`;
//!   `cilium-operator:/p/operator-generic` is why the path is not the name).
//!   Cloned through `POST /api/v1/volumes/{golden}/clone`.
//!
//! The clone is attached here over ublk and PID 1 mounts it at
//! `/run/stormpump/roots/<container>`; that registration is the root handle
//! the container is spawned on (`stormpump_runtime`).

use serde_json::{json, Value};
use std::collections::HashSet;

/// Where PID 1 mounts each container's root.
pub const ROOTS_MOUNT: &str = "/run/stormpump/roots";
/// The prefix of every container root volume: what the orphan sweep owns.
pub const VOLUME_PREFIX: &str = "ctr-";
/// What an image reference returned by a pull looks like: the fstemplate that
/// is the image's sealed golden in this node's engine.
pub const TEMPLATE_PREFIX: &str = "template:";
const PALLET_ROOT: &str = "/pallets";

/// The engine volume name of a container's root.
pub fn volume_name(container_id: &str) -> String {
    format!("{VOLUME_PREFIX}{container_id}")
}

/// Where PID 1 mounts a container's root.
pub fn mount_point(container_id: &str) -> String {
    format!("{ROOTS_MOUNT}/{container_id}")
}

/// What a container's root is cloned from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Golden {
    /// An fstemplate (a pulled image's golden, sealed by the registry).
    Template(String),
    /// A pallet: the slab volume mounted at its path. Its golden is
    /// `<vol>.golden`, else the volume's sealed parent.
    Pallet(String),
}

/// What an image reference (what `pull_image` returned) is cloned from.
/// `cmdline` is the kernel command line, for `rd.stormblock.mount=`.
pub fn golden_of(image: &str, cmdline: &str) -> Option<Golden> {
    if let Some(t) = image.strip_prefix(TEMPLATE_PREFIX) {
        return (!t.is_empty()).then(|| Golden::Template(t.to_string()));
    }
    let path = image.strip_prefix(PALLET_ROOT)?.strip_prefix('/')?;
    if path.is_empty() || path.contains('/') {
        return None;
    }
    let mounted = format!("/p/{path}");
    let volume = cmdline
        .split_whitespace()
        .find_map(|w| w.strip_prefix("rd.stormblock.mount="))
        .and_then(|list| {
            list.split(',').find_map(|e| {
                let (vol, at) = e.split_once(':')?;
                (at == mounted || at == format!("{PALLET_ROOT}/{path}")).then(|| vol.to_string())
            })
        })
        .unwrap_or_else(|| path.to_string());
    Some(Golden::Pallet(volume))
}

/// Which golden to clone for a pallet volume `vol`, from the engine's volume
/// listing: `<vol>.golden` when it is there and sealed, else `vol`'s sealed
/// parent. Never `vol` itself: that is the mounted first clone, and cloning
/// it would be a clone of a clone.
pub fn pallet_golden(listing: &Value, vol: &str) -> Result<String, String> {
    let items = listing["items"].as_array().ok_or("the engine's volume listing has no items")?;
    let by_name = |n: &str| items.iter().find(|v| v["name"].as_str() == Some(n));
    let sealed = |v: &Value| v["sealed"].as_bool() == Some(true);
    if let Some(g) = by_name(&format!("{vol}.golden")).filter(|v| sealed(v)) {
        return g["id"].as_str().map(str::to_string).ok_or_else(|| format!("{vol}.golden has no id"));
    }
    let clone = by_name(vol).ok_or_else(|| format!("no golden for pallet {vol}: the engine has neither {vol}.golden nor {vol}"))?;
    let parent = clone["parent"].as_str().ok_or_else(|| format!("pallet volume {vol} has no parent golden"))?;
    let p = items
        .iter()
        .find(|v| v["id"].as_str() == Some(parent))
        .ok_or_else(|| format!("pallet volume {vol}'s parent {parent} is not on this node"))?;
    if !sealed(p) {
        return Err(format!("pallet volume {vol}'s parent {parent} is not sealed"));
    }
    Ok(parent.to_string())
}

/// The root volumes nothing holds (#104): named [`VOLUME_PREFIX`], not one of
/// `known` (the containers this kubelet has, and the ones it is making), not
/// in use and not attached. What a kubelet that died between a create and a
/// removal leaves. Anything still attached or mounted is kept.
pub fn orphans(listing: &Value, known: &HashSet<String>) -> Vec<(String, String)> {
    listing["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| Some((v["name"].as_str()?, v["id"].as_str()?, v)))
        .filter(|(name, _, _)| name.starts_with(VOLUME_PREFIX) && !known.contains(*name))
        .filter(|(_, _, v)| {
            v["in_use"].as_bool() != Some(true)
                && v["attachments"].as_array().map_or(true, |a| a.is_empty())
        })
        .map(|(name, id, _)| (name.to_string(), id.to_string()))
        .collect()
}

/// A container root made: its engine volume and the device it is attached at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Made {
    pub volume_id: String,
    pub device: String,
}

/// The engine half of container roots.
pub struct Roots {
    engine: crate::engine::EngineClient,
    url: String,
    node: String,
    cmdline: String,
}

impl Roots {
    pub fn new(engine: crate::engine::EngineClient, node: impl Into<String>) -> Self {
        let cmdline = std::fs::read_to_string("/proc/cmdline").unwrap_or_default();
        Self::with_cmdline(engine, node, cmdline)
    }

    pub fn with_cmdline(engine: crate::engine::EngineClient, node: impl Into<String>, cmdline: String) -> Self {
        Self { url: engine.url().trim_end_matches('/').to_string(), engine, node: node.into(), cmdline }
    }

    async fn answer(&self, what: &str, resp: reqwest::Result<reqwest::Response>) -> Result<Value, String> {
        let resp = resp.map_err(|e| format!("{what}: stormblock did not answer: {e}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("{what}: stormblock answered {status}: {}", text.chars().take(300).collect::<String>()));
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    async fn listing(&self) -> Result<Value, String> {
        let r = self.engine.get(&format!("{}/api/v1/volumes", self.url)).await;
        self.answer("listing volumes", r).await
    }

    /// Clone `image`'s golden as `name` and attach it here. A clone that
    /// does not attach is deleted again: nothing half-made is left behind.
    pub async fn make(&self, name: &str, image: &str, owner: &Value) -> Result<Made, String> {
        let golden = golden_of(image, &self.cmdline)
            .ok_or_else(|| format!("image {image} names no golden on this node to clone a root from"))?;
        let created = match &golden {
            Golden::Template(t) => {
                let body = json!({ "name": name, "verify": true });
                let r = self.engine.post(&format!("{}/api/v1/fstemplates/{t}/clone", self.url), &body).await;
                self.answer(&format!("cloning golden {t} for {name}"), r).await?
            }
            Golden::Pallet(vol) => {
                let g = pallet_golden(&self.listing().await?, vol)?;
                let mut body = json!({ "name": name, "verify": true });
                if !owner.is_null() {
                    body["owner"] = owner.clone();
                }
                let r = self.engine.post(&format!("{}/api/v1/volumes/{g}/clone", self.url), &body).await;
                self.answer(&format!("cloning golden {vol} ({g}) for {name}"), r).await?
            }
        };
        let volume_id = created["volume_id"]
            .as_str()
            .or_else(|| created["id"].as_str())
            .map(str::to_string)
            .ok_or_else(|| format!("the clone {name} came back with no volume id: {created}"))?;
        if matches!(golden, Golden::Template(_)) && !owner.is_null() {
            // A template clone takes no owner; set it after (stormblock#115).
            let r = self.engine.put(&format!("{}/api/v1/volumes/{volume_id}/owner", self.url), owner).await;
            if let Err(e) = self.answer("owner", r).await {
                tracing::debug!(volume = %name, "container root owner not set: {e}");
            }
        }
        let attach = json!({ "node": self.node, "transport": "ublk" });
        let r = self.engine.post(&format!("{}/api/v1/volumes/{volume_id}/attach", self.url), &attach).await;
        let device = match self.answer(&format!("attaching {name}"), r).await {
            Ok(info) => info["device_hint"].as_str().map(str::to_string).ok_or_else(|| {
                format!("{name} did not attach as a local device: {info}")
            }),
            Err(e) => Err(e),
        };
        match device {
            Ok(device) => Ok(Made { volume_id, device }),
            Err(e) => {
                if let Err(undo) = self.destroy(&volume_id).await {
                    tracing::warn!(volume = %name, "a root that did not attach was not deleted: {undo}");
                }
                Err(e)
            }
        }
    }

    /// Detach a container's root and delete it. Already gone is done.
    pub async fn destroy(&self, volume_id: &str) -> Result<(), String> {
        for (what, path) in [
            ("detaching", format!("{}/api/v1/volumes/{volume_id}/attach", self.url)),
            ("deleting", format!("{}/api/v1/volumes/{volume_id}", self.url)),
        ] {
            let r = self
                .engine
                .delete(&path)
                .await
                .map_err(|e| format!("{what} root {volume_id}: stormblock did not answer: {e}"))?;
            let status = r.status();
            if !status.is_success() && status.as_u16() != 404 {
                let text = r.text().await.unwrap_or_default();
                return Err(format!("{what} root {volume_id}: stormblock answered {status}: {}", text.chars().take(300).collect::<String>()));
            }
        }
        Ok(())
    }

    /// Delete the [`orphans`]; how many went.
    pub async fn sweep(&self, known: &HashSet<String>) -> usize {
        let listing = match self.listing().await {
            Ok(l) => l,
            Err(e) => {
                tracing::debug!("container root sweep skipped: {e}");
                return 0;
            }
        };
        let mut gone = 0;
        for (name, id) in orphans(&listing, known) {
            match self.destroy(&id).await {
                Ok(()) => {
                    tracing::info!(volume = %name, "deleted a container root nothing holds");
                    gone += 1;
                }
                Err(e) => tracing::warn!(volume = %name, "orphan container root kept: {e}"),
            }
        }
        gone
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    const CMDLINE: &str = "root=/dev/ublkb0 rd.stormblock.mount=stormblock:/p/stormblock,busybox:/p/busybox,\
        cilium-operator:/p/operator-generic,kubelet-data:/var/lib/kubelet ip=dhcp rw";

    #[test]
    fn an_image_reference_names_its_golden() {
        assert_eq!(golden_of("template:sbr-quay.io-a-b-1", ""), Some(Golden::Template("sbr-quay.io-a-b-1".into())));
        assert_eq!(golden_of("/pallets/busybox", CMDLINE), Some(Golden::Pallet("busybox".into())));
        // The mount path is not the volume's name: the command line says which.
        assert_eq!(golden_of("/pallets/operator-generic", CMDLINE), Some(Golden::Pallet("cilium-operator".into())));
        assert_eq!(golden_of("/pallets/coredns", "no mount list"), Some(Golden::Pallet("coredns".into())));
        for bad in ["template:", "/pallets/", "/pallets/a/b", "busybox", "/run/stormpump/images/clone-x"] {
            assert_eq!(golden_of(bad, CMDLINE), None, "{bad}");
        }
    }

    #[test]
    fn a_pallets_golden_is_the_sealed_one_never_its_mounted_clone() {
        let listing = json!({"items": [
            {"name": "busybox", "id": "c1", "parent": "g1", "sealed": false},
            {"name": "busybox.golden", "id": "g1", "sealed": true},
            {"name": "coredns", "id": "c2", "parent": "g2", "sealed": false},
            {"name": "coredns-src", "id": "g2", "sealed": true},
            {"name": "loose", "id": "c3", "sealed": false},
            {"name": "drafty", "id": "c4", "parent": "g4", "sealed": false},
            {"name": "drafty-src", "id": "g4", "sealed": false},
        ]});
        assert_eq!(pallet_golden(&listing, "busybox").unwrap(), "g1");
        assert_eq!(pallet_golden(&listing, "coredns").unwrap(), "g2", "no <vol>.golden: the clone's sealed parent");
        assert!(pallet_golden(&listing, "loose").unwrap_err().contains("no parent"));
        assert!(pallet_golden(&listing, "drafty").unwrap_err().contains("not sealed"));
        assert!(pallet_golden(&listing, "absent").is_err());
    }

    #[test]
    fn only_unheld_container_roots_are_orphans() {
        let listing = json!({"items": [
            {"name": "ctr-gone", "id": "1", "in_use": false},
            {"name": "ctr-live", "id": "2", "in_use": false},
            {"name": "ctr-mounted", "id": "3", "in_use": true},
            {"name": "ctr-attached", "id": "4", "in_use": false, "attachments": [{"transport": "ublk"}]},
            {"name": "pvc-ns-data", "id": "5", "in_use": false},
        ]});
        let known: HashSet<String> = ["ctr-live".to_string()].into();
        assert_eq!(orphans(&listing, &known), vec![("ctr-gone".to_string(), "1".to_string())]);
    }

    /// A fake stormblock: records every call, clones and attaches.
    async fn fake_engine() -> (String, Arc<Mutex<Vec<String>>>) {
        use axum::{extract::Path, routing::{delete, get, post, put}, Json, Router};
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let n = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let (c1, c2, c3, c4, c5, c6) = (calls.clone(), calls.clone(), calls.clone(), calls.clone(), calls.clone(), calls.clone());
        let (n1, n2) = (n.clone(), n.clone());
        let app = Router::new()
            .route("/api/v1/volumes", get(move || {
                c1.lock().unwrap().push("list".into());
                async { Json(json!({"items": [
                    {"name": "busybox", "id": "c1", "parent": "g1", "sealed": false},
                    {"name": "busybox.golden", "id": "g1", "sealed": true}]})) }
            }))
            .route("/api/v1/fstemplates/{t}/clone", post(move |Path(t): Path<String>, Json(b): Json<Value>| {
                c2.lock().unwrap().push(format!("clone template {t} as {}", b["name"].as_str().unwrap()));
                let id = n1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move { Json(json!({"volume_id": format!("v{id}")})) }
            }))
            .route("/api/v1/volumes/{g}/clone", post(move |Path(g): Path<String>, Json(b): Json<Value>| {
                c3.lock().unwrap().push(format!("clone volume {g} as {}", b["name"].as_str().unwrap()));
                let id = n2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move { Json(json!({"id": format!("v{id}")})) }
            }))
            .route("/api/v1/volumes/{v}/attach", post(move |Path(v): Path<String>| {
                c4.lock().unwrap().push(format!("attach {v}"));
                async move { Json(json!({"device_hint": format!("/dev/ublkb-{v}")})) }
            }).delete(move |Path(v): Path<String>| {
                c5.lock().unwrap().push(format!("detach {v}"));
                async { Json(json!({})) }
            }))
            .route("/api/v1/volumes/{v}", delete(move |Path(v): Path<String>| {
                c6.lock().unwrap().push(format!("delete {v}"));
                async { Json(json!({})) }
            }))
            .route("/api/v1/volumes/{v}/owner", put(|| async { Json(json!({})) }));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        (url, calls)
    }

    /// The owner's acceptance (#104): two containers of one image get two
    /// roots, each **one** clone straight from the sealed golden, and a
    /// removed container's root is detached and deleted.
    #[tokio::test]
    async fn each_container_gets_one_clone_of_the_golden_and_loses_it_with_the_container() {
        let (url, calls) = fake_engine().await;
        let roots = Roots::with_cmdline(crate::engine::EngineClient::new(&url, crate::engine::TokenSource::none()), "n1", CMDLINE.into());
        let owner = json!({"kind": "Pod", "namespace": "ns", "name": "web", "uid": "u"});

        let a = roots.make(&volume_name("ct-a"), "/pallets/busybox", &owner).await.unwrap();
        let b = roots.make(&volume_name("ct-b"), "/pallets/busybox", &owner).await.unwrap();
        assert_ne!(a.volume_id, b.volume_id, "two containers, two roots");
        assert_ne!(a.device, b.device);
        let p = roots.make(&volume_name("ct-c"), "template:sbr-busybox-1", &owner).await.unwrap();

        let log = calls.lock().unwrap().clone();
        let clones: Vec<&String> = log.iter().filter(|c| c.starts_with("clone")).collect();
        assert_eq!(clones, vec![
            "clone volume g1 as ctr-ct-a", "clone volume g1 as ctr-ct-b", "clone template sbr-busybox-1 as ctr-ct-c",
        ], "one clone per container, each of the sealed golden (g1), never of the mounted pallet (c1)");

        roots.destroy(&a.volume_id).await.unwrap();
        roots.destroy(&p.volume_id).await.unwrap();
        let log = calls.lock().unwrap().clone();
        let tail: Vec<&String> = log.iter().filter(|c| c.starts_with("detach") || c.starts_with("delete")).collect();
        assert_eq!(tail, vec!["detach v0", "delete v0", "detach v2", "delete v2"]);
    }

    #[tokio::test]
    async fn an_image_with_no_golden_is_refused_before_anything_is_made() {
        let (url, calls) = fake_engine().await;
        let roots = Roots::with_cmdline(crate::engine::EngineClient::new(&url, crate::engine::TokenSource::none()), "n1", CMDLINE.into());
        let e = roots.make("ctr-x", "busybox", &Value::Null).await.unwrap_err();
        assert!(e.contains("names no golden"), "{e}");
        let e = roots.make("ctr-y", "/pallets/absent", &Value::Null).await.unwrap_err();
        assert!(e.contains("no golden for pallet absent"), "{e}");
        assert!(!calls.lock().unwrap().iter().any(|c| c.starts_with("clone")));
    }
}
