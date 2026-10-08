//! How much room the node's data slabs have for claims, published and enforced
//! (#62; the policy is the owner's, #108).
//!
//! A claim is a thin copy-on-write clone of its size class's blank, so any
//! number of large claims mount against a slab, and writes fail inside the
//! containers once it is full. So a claim is charged its **full class size**
//! when it is made (the class is its ceiling: it can write that much), at an
//! overcommit ratio of 1.0 by default, and clones keep the class size.
//!
//! - **What the node has** ([`Capacity::of`]): the data slabs' total and free
//!   bytes (`/api/v1/slabs`, role `data`), and what is committed: the virtual
//!   size of every writable data volume (claims, node service volumes, VM
//!   disks). Not sealed volumes, goldens or the class blanks (`pvc-ext4j-*`),
//!   which are what claims are cloned *from* and are never written. A blank
//!   is stormblock's fstemplate: formatted on `fstemplate-<name>-raw`, sealed
//!   as `fstemplate-<fs>-<name>`; both are sources, sealed yet or not, so a
//!   1 TiB blank being minted does not crowd out the claim that asked for it
//!   (#209).
//! - **What is left for a claim**: `min(total × ratio − committed, free) −
//!   reserve`. Both halves: the commitment rule, and the physical space that
//!   goldens and blanks also use.
//! - **Published** as a `CSIStorageCapacity` (`kube-system/stormblock-<node>`,
//!   class `stormblock`, this node's hostname topology) with
//!   `maximumVolumeSize` = the largest class that fits, which is what
//!   rustkube's scheduler compares a claim's request with. (It reads them once
//!   the CSIDriver says `storageCapacity: true`, stormcos#151.)
//! - **Enforced** by the kubelet at provision time too: static pods and pods
//!   written onto `spec.nodeName` never meet a scheduler check.
//! - **Alerted**: gauges, and a Warning Event on this node's stormblock PVs
//!   when the data slabs pass the alert percentage.

use serde_json::{json, Value};

/// How a claim is charged, and when to warn.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Policy {
    /// Committed bytes allowed per byte of data slab (`--storage-overcommit`).
    /// 1.0: no overcommit (#108).
    pub overcommit: f64,
    /// Percent of the data slabs kept back from claims
    /// (`--storage-reserve-percent`).
    pub reserve_percent: f64,
    /// Percent of the data slabs written past which the node warns
    /// (`--storage-alert-percent`).
    pub alert_percent: f64,
}

impl Default for Policy {
    fn default() -> Self {
        Policy { overcommit: 1.0, reserve_percent: 5.0, alert_percent: 85.0 }
    }
}

/// The node's data slabs, as the policy reads them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Capacity {
    pub total: u64,
    pub free: u64,
    pub committed: u64,
    pub reserve: u64,
    /// What a new claim may have.
    pub available: u64,
}

/// stormblock's prefix for a template's own volumes (its `TEMPLATE_PREFIX`):
/// `fstemplate-<name>-raw` while it formats, `fstemplate-<fs>-<name>` sealed.
const FSTEMPLATE_PREFIX: &str = "fstemplate-";

/// Is this volume what a claim is cloned from rather than a writable one?
fn is_source(v: &Value) -> bool {
    let name = v["name"].as_str().unwrap_or("");
    v["sealed"].as_bool().unwrap_or(false)
        || name.starts_with(FSTEMPLATE_PREFIX)
        || name.ends_with(".golden")
        || name.starts_with("standby-")
        || crate::storage::SIZE_CLASSES
            .iter()
            .any(|(c, _, _)| name == crate::storage::template_name(c))
}

impl Capacity {
    /// From the engine's slab and volume listings (their `items`).
    pub fn of(slabs: &[Value], volumes: &[Value], policy: &Policy) -> Capacity {
        let data = |v: &Value| v["role"].as_str() == Some("data");
        let total: u64 = slabs.iter().filter(|s| data(s)).filter_map(|s| s["total_bytes"].as_u64()).sum();
        let free: u64 = slabs.iter().filter(|s| data(s)).filter_map(|s| s["free_bytes"].as_u64()).sum();
        let committed: u64 = volumes
            .iter()
            .filter(|v| data(v) && !is_source(v))
            .filter_map(|v| v["virtual_size_bytes"].as_u64())
            .sum();
        let reserve = (total as f64 * policy.reserve_percent.clamp(0.0, 100.0) / 100.0) as u64;
        let allowed = (total as f64 * policy.overcommit.max(0.0)) as u64;
        let available = allowed.saturating_sub(committed).min(free).saturating_sub(reserve);
        Capacity { total, free, committed, reserve, available }
    }

    /// The largest size class a new claim can have here, if any.
    pub fn largest_class(&self) -> Option<(&'static str, u64)> {
        crate::storage::SIZE_CLASSES
            .iter()
            .rev()
            .find(|(_, b, _)| *b <= self.available)
            .map(|(c, b, _)| (*c, *b))
    }

    /// Percent of the data slabs written.
    pub fn used_percent(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        (self.total - self.free.min(self.total)) as f64 * 100.0 / self.total as f64
    }

    /// Why a claim of `class` (`bytes`) cannot be made here, or `None` when it
    /// can.
    pub fn refusal(&self, class: &str, bytes: u64, policy: &Policy) -> Option<String> {
        (bytes > self.available).then(|| {
            format!(
                "not enough room on this node's data slabs for the {class} class ({}): {} is left \
                 for claims ({} total, {} free, {} committed by volumes here, {} reserved, \
                 overcommit {})",
                q(bytes),
                q(self.available),
                q(self.total),
                q(self.free),
                q(self.committed),
                q(self.reserve),
                policy.overcommit
            )
        })
    }
}

fn q(b: u64) -> String {
    crate::system_claims::quantity(b)
}

/// The object's name for `node`.
pub fn object_name(node: &str) -> String {
    format!("stormblock-{node}")
}

/// The `CSIStorageCapacity` this node publishes.
pub fn object(node: &str, c: &Capacity) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1",
        "kind": "CSIStorageCapacity",
        "metadata": {
            "name": object_name(node),
            "namespace": crate::system_claims::NAMESPACE,
            "labels": { "storm.io/node": node, "storm.io/component": "kubelet" },
        },
        "storageClassName": crate::storage::STORAGE_CLASS,
        "nodeTopology": { "matchLabels": { "kubernetes.io/hostname": node } },
        "capacity": q(c.available),
        "maximumVolumeSize": q(c.largest_class().map_or(0, |(_, b)| b)),
    })
}

/// The engine's listings, or why not. A slab listing that is not there is an
/// engine too old to say, and is `Err`: no answer, no check.
pub async fn read(engine: &crate::engine::EngineClient, policy: &Policy) -> Result<Capacity, String> {
    let get = |path: &'static str| async move {
        let r = engine
            .get(&format!("{}{path}", engine.url()))
            .await
            .map_err(|e| format!("stormblock {path}: {e}"))?;
        if !r.status().is_success() {
            return Err(format!("stormblock {path}: {}", r.status()));
        }
        let v: Value = r.json().await.map_err(|e| format!("stormblock {path}: {e}"))?;
        Ok::<_, String>(v["items"].as_array().cloned().unwrap_or_default())
    };
    let slabs = get("/api/v1/slabs").await?;
    let volumes = get("/api/v1/volumes").await?;
    Ok(Capacity::of(&slabs, &volumes, policy))
}

/// Record the gauges (`kubelet_stormblock_data_bytes{kind}`).
pub fn observe(c: &Capacity) {
    if crate::metrics::handle().is_none() {
        return;
    }
    for (kind, v) in [
        ("total", c.total),
        ("free", c.free),
        ("committed", c.committed),
        ("reserve", c.reserve),
        ("available", c.available),
    ] {
        metrics::gauge!(crate::metrics::STORMBLOCK_DATA, "kind" => kind).set(v as f64);
    }
    metrics::gauge!(crate::metrics::STORMBLOCK_USED).set(c.used_percent());
}

/// One pass: publish the object, record the gauges, and say so on this node's
/// stormblock PVs when the slabs cross the alert line. `alerted` is whether
/// the last pass was past it; the new value is returned.
pub async fn publish(
    client: &reqwest::Client,
    api_url: &str,
    engine: &crate::engine::EngineClient,
    node: &str,
    policy: &Policy,
    events: Option<&crate::events::EventRecorder>,
    alerted: bool,
) -> Result<bool, String> {
    let c = read(engine, policy).await?;
    observe(&c);
    if c.total == 0 {
        // No data slab: nothing to offer, and nothing to say about filling.
        return Ok(false);
    }
    if !api_url.is_empty() {
        let want = object(node, &c);
        let base = format!(
            "{api_url}/apis/storage.k8s.io/v1/namespaces/{}/csistoragecapacities",
            crate::system_claims::NAMESPACE
        );
        let path = format!("{base}/{}", object_name(node));
        let have = match client.get(&path).send().await {
            Ok(r) if r.status().is_success() => r.json::<Value>().await.ok(),
            Ok(r) if r.status().as_u16() == 404 => None,
            Ok(r) => return Err(format!("GET {path}: {}", r.status())),
            Err(e) => return Err(format!("GET {path}: {e}")),
        };
        match have {
            Some(h) if h["capacity"] == want["capacity"] && h["maximumVolumeSize"] == want["maximumVolumeSize"] => {}
            Some(mut h) => {
                h["capacity"] = want["capacity"].clone();
                h["maximumVolumeSize"] = want["maximumVolumeSize"].clone();
                h["nodeTopology"] = want["nodeTopology"].clone();
                let r = client.put(&path).json(&h).send().await.map_err(|e| format!("PUT {path}: {e}"))?;
                if !r.status().is_success() {
                    return Err(format!("PUT {path}: {}", r.status()));
                }
            }
            None => {
                let r = client.post(&base).json(&want).send().await.map_err(|e| format!("POST {base}: {e}"))?;
                if !r.status().is_success() && r.status().as_u16() != 409 {
                    return Err(format!("POST {base}: {}", r.status()));
                }
            }
        }
    }
    let past = c.used_percent() >= policy.alert_percent;
    if past && !alerted {
        let message = format!(
            "this node's data slabs are {:.0}% written ({} free of {}): claims here may soon fail \
             their writes",
            c.used_percent(),
            q(c.free),
            q(c.total)
        );
        tracing::warn!("{message}");
        if let (Some(ev), false) = (events, api_url.is_empty()) {
            for pv in node_pvs(client, api_url, node).await {
                ev.object_event("v1", "PersistentVolume", &pv, "Warning", "SlabFilling", &message).await;
            }
        }
    }
    Ok(past)
}

/// This node's stormblock PVs: the built-in driver's, annotated with this
/// node.
async fn node_pvs(client: &reqwest::Client, api_url: &str, node: &str) -> Vec<Value> {
    let Ok(r) = client.get(format!("{api_url}/api/v1/persistentvolumes")).send().await else {
        return Vec::new();
    };
    let Ok(list) = r.json::<Value>().await else { return Vec::new() };
    list["items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|pv| {
            pv["spec"]["csi"]["driver"].as_str() == Some(crate::system_claims::DRIVER)
                && pv["metadata"]["annotations"]["storm.io/node"].as_str() == Some(node)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const GI: u64 = 1 << 30;
    const TI: u64 = 1 << 40;

    fn slab(role: &str, total: u64, free: u64) -> Value {
        json!({ "role": role, "total_bytes": total, "free_bytes": free })
    }
    fn vol(name: &str, role: &str, size: u64, sealed: bool) -> Value {
        json!({ "name": name, "role": role, "virtual_size_bytes": size, "sealed": sealed })
    }

    #[test]
    fn a_claim_is_charged_its_class_and_sources_are_not_charged() {
        // C2NR0Q2's shape: a 1.7 TB data slab, one 600Gi claim (1 TiB class).
        let slabs = [slab("data", 1700 * GI, 1650 * GI), slab("system", 100 * GI, 50 * GI)];
        let vols = [
            vol("pvc-default-big", "data", TI, false),
            vol("fastetcd-data", "data", 8 * GI, false),
            vol("pvc-ext4j-1048576m", "data", TI, false), // the 1 TiB blank
            vol("fastetcd-data.golden", "data", 8 * GI, true),
            vol("stormpump", "system", 20 * GI, false),
        ];
        let c = Capacity::of(&slabs, &vols, &Policy::default());
        assert_eq!((c.total, c.free), (1700 * GI, 1650 * GI), "only the data half");
        assert_eq!(c.committed, TI + 8 * GI, "the claim at its class, the service volume, nothing else");
        assert_eq!(c.reserve, 85 * GI);
        assert_eq!(c.available, 1700 * GI - TI - 8 * GI - 85 * GI);
        // A second 1 TiB class claim does not fit; a 256 GiB one does.
        assert_eq!(c.largest_class(), Some(("256G", 256 * GI)));
        let why = c.refusal("1T", TI, &Policy::default()).unwrap();
        assert!(why.contains("not enough room") && why.contains("1T class (1Ti)"), "{why}");
        assert!(c.refusal("256G", 256 * GI, &Policy::default()).is_none());
    }

    #[test]
    fn a_class_blank_still_formatting_is_not_charged() {
        // C2NR0Q2 (#209): 1800428Mi data slab, the 1T blank minting, ~13 GiB
        // of service volumes. Before, the raw template volume was charged
        // 1 TiB and the 1Ti claim that asked for the blank did not fit.
        const MI: u64 = 1 << 20;
        let slabs = [slab("data", 1_800_428 * MI, 1_784_859 * MI)];
        let vols = [
            vol("fstemplate-pvc-ext4j-1048576m-raw", "data", TI, false), // formatting
            vol("fstemplate-ext4-pvc-ext4j-262144m", "data", 256 * GI, true), // a sealed blank
            vol("fstemplate-pvc-ext4j-262144m-raw", "data", 256 * GI, false), // its formatted base
            vol("fastetcd-data", "data", 13_504 * MI, false),
        ];
        let c = Capacity::of(&slabs, &vols, &Policy::default());
        assert_eq!(c.committed, 13_504 * MI, "template volumes are sources, sealed or not");
        assert!(c.refusal("1T", TI, &Policy::default()).is_none(), "the 1Ti claim fits beside its blank");
        assert_eq!(c.largest_class(), Some(("1T", TI)));
    }

    #[test]
    fn physical_space_bounds_it_too_and_overcommit_is_a_setting() {
        // Committed little, but goldens wrote most of the slab.
        let slabs = [slab("data", 1000 * GI, 100 * GI)];
        let c = Capacity::of(&slabs, &[], &Policy { reserve_percent: 0.0, ..Policy::default() });
        assert_eq!(c.available, 100 * GI);
        // Ratio 2: twice the slab may be committed.
        let vols = [vol("pvc-a", "data", 1500 * GI, false)];
        let slabs = [slab("data", 1000 * GI, 1000 * GI)];
        let p = Policy { overcommit: 2.0, reserve_percent: 0.0, ..Policy::default() };
        assert_eq!(Capacity::of(&slabs, &vols, &p).available, 500 * GI);
        assert_eq!(Capacity::of(&slabs, &vols, &Policy::default()).available, 0, "ratio 1: already over");
    }

    #[test]
    fn the_published_object_is_what_the_scheduler_reads() {
        let c = Capacity { total: 2 * TI, free: 2 * TI, committed: 0, reserve: 0, available: 300 * GI };
        let o = object("node1", &c);
        assert_eq!(o["metadata"]["name"], "stormblock-node1");
        assert_eq!(o["metadata"]["namespace"], "kube-system");
        assert_eq!(o["storageClassName"], "stormblock");
        assert_eq!(o["nodeTopology"]["matchLabels"]["kubernetes.io/hostname"], "node1");
        assert_eq!(o["capacity"], "300Gi");
        assert_eq!(o["maximumVolumeSize"], "256Gi", "the largest class that fits");
        let none = Capacity { available: 512 * 1024, ..c };
        assert_eq!(object("n", &none)["maximumVolumeSize"], "0");
    }

    #[test]
    fn used_percent_is_of_the_data_slabs() {
        let c = Capacity { total: 100, free: 10, ..Default::default() };
        assert!((c.used_percent() - 90.0).abs() < 1e-9);
        assert_eq!(Capacity::default().used_percent(), 0.0);
    }
}
