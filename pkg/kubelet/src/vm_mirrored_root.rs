//! A VM disk on a stormstorage mirrored volume filled once from its golden
//! (#404, stormstorage#113 option B', stormstorage#116).
//!
//! A golden's disk is normally a copy-on-write clone on this node's engine:
//! it lives on one node and cannot be started anywhere else. A disk named in
//! the VMI's annotation [`REQUEST`] (`{"root": 2}`: the disk and its number
//! of copies, at least 2) is instead a stormstorage volume, RAID1 across
//! nodes, whose served volume starts as a copy of the golden:
//!
//! 1. **Create** (once, found by name after that): `POST /api/v1/volumes
//!    {name, size_bytes, raid: "mirror", replicas, from_golden, prefer_node,
//!    owner}`. The golden must be on a placed node, which becomes the head;
//!    this node is preferred, and its golden gives the size.
//! 2. **Wait**: until `seed.state` is `done`, the start waits and says how
//!    many bytes are left. A failed copy is retried by stormstorage after its
//!    cooldown; the start keeps waiting and says why.
//! 3. **Serve and attach**: `POST …/export/hosts {host_nqn}` names this
//!    node, then this node's engine attaches the served volume as ublk. When
//!    the head is another node, stormblock reaches it over NVMe/TCP and serves
//!    it here (stormblock#328), as for a moved disk.
//!
//! The volume is named [`volume_name`]: stormvm's name for the disk with
//! [`SUFFIX`]. stormstorage names each leg on its engine after the volume, so
//! without the suffix a leg on this node would answer to the name of the
//! disk's local clone. The engine's own sweep leaves these parts alone
//! ([`is_part`]); the volume is deleted through stormstorage once its owner is
//! gone and it serves no host ([`sweepable`]). A stop withdraws this node's
//! host.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::engine::EngineClient;

/// The request: `{"<disk>": <copies>}` on the VMI.
pub const REQUEST: &str = "storm.io/mirrored-disks";
/// What ends the stormstorage volume's name.
pub const SUFFIX: &str = ".mirrored";

/// The request on `vmi`: disk → copies. A request that is not such an object
/// is an error naming it.
pub fn requests(vmi: &Value) -> Result<BTreeMap<String, u32>, String> {
    let Some(text) = vmi["metadata"]["annotations"][REQUEST].as_str() else { return Ok(BTreeMap::new()) };
    let v: Value = serde_json::from_str(text).map_err(|e| format!("{REQUEST} is not JSON: {e}"))?;
    let o = v.as_object().ok_or_else(|| format!("{REQUEST} is not an object of disk → copies: {text}"))?;
    o.iter()
        .map(|(disk, n)| match n.as_u64() {
            Some(n) if (2..=u64::from(u32::MAX)).contains(&n) => Ok((disk.clone(), n as u32)),
            _ => Err(format!("{REQUEST}: disk {disk} asks for {n} copies; a mirrored disk has at least 2")),
        })
        .collect()
}

/// Why `requests` cannot be met for `spec`, if it cannot: each disk named is
/// one of the spec's, made from a golden.
pub fn refusal(requests: &BTreeMap<String, u32>, spec: &stormvm_spec::VmSpec) -> Option<String> {
    for disk in requests.keys() {
        match spec.disks.iter().find(|d| &d.name == disk) {
            None => return Some(format!("{REQUEST}: the machine has no disk {disk}")),
            Some(d) if !matches!(d.from, stormvm_spec::DiskSource::Golden(_)) => {
                return Some(format!(
                    "{REQUEST}: disk {disk} is not made from a golden; only a golden's disk is filled into a \
                     mirrored volume"
                ))
            }
            Some(_) => {}
        }
    }
    None
}

/// The stormstorage volume of `disk` of the machine `namespace/vm`.
pub fn name_of(namespace: &str, vm: &str, disk: &str) -> String {
    format!("{namespace}.{vm}-{disk}{SUFFIX}")
}

/// [`name_of`] for a spec: stormvm's own disk name with [`SUFFIX`].
pub fn volume_name(spec: &stormvm_spec::VmSpec, disk: &str) -> String {
    format!("{}{SUFFIX}", stormvm_node::start::volume_name(spec, disk))
}

/// Is this engine volume a part of a mirrored disk (a leg, the served
/// volume, the seed's clone)? stormstorage deletes those, never the engine
/// sweep.
pub fn is_part(engine_name: &str) -> bool {
    [SUFFIX, ".mirrored-mirror", ".mirrored-seed"].iter().any(|s| engine_name.ends_with(s))
}

/// This node's NVMe host NQN, as its engine connects to a peer with it:
/// `/etc/nvme/hostnqn`, else stormblock's own fallback
/// (`nqn.2026-09.lo.storm:host-<node>`).
pub fn host_nqn(node: &str) -> String {
    crate::stormpump_runtime::node_host_nqn().unwrap_or_else(|| format!("nqn.2026-09.lo.storm:host-{node}"))
}

/// Why a start does not go on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotYet {
    /// Something that changes by itself: the start waits and says this.
    Wait(String),
    /// Something a person must change.
    Fail(String),
}

/// What a volume's record says of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Seeded {
    /// The copy is not done: why the start waits.
    No(String),
    /// Done: the served volume's id on its engine, what this node attaches.
    Yes(String),
}

/// Where the seed of `name` is, from stormstorage's `GET` of it. A volume
/// with no seed was not made by this kubelet and is refused.
pub fn seeded(name: &str, v: &Value) -> Result<Seeded, String> {
    let seed = &v["seed"];
    if !seed.is_object() {
        return Err(format!(
            "stormstorage volume {name} was not made from a golden: it is not a mirrored disk this kubelet made"
        ));
    }
    let golden = seed["golden"].as_str().unwrap_or("?");
    let msg = seed["message"].as_str().unwrap_or("");
    Ok(match seed["state"].as_str().unwrap_or("") {
        "done" => Seeded::Yes(served_id(name, v)),
        "copying" => {
            let total = seed["bytes_total"].as_u64().unwrap_or(0);
            let left = seed["bytes_remaining"].as_u64().unwrap_or(0);
            Seeded::No(format!("copying golden {golden} into {name}: {left} of {total} bytes left"))
        }
        "failed" => Seeded::No(format!(
            "stormstorage's copy of golden {golden} into {name} failed ({msg}); it tries again after its cooldown"
        )),
        "pending" => Seeded::No(format!("waiting for stormstorage to start copying golden {golden} into {name}")),
        other => Seeded::No(format!("golden {golden} into {name}: seed {other}")),
    })
}

/// The served volume's id on its engine: `export.local_id`, else its name
/// (`<name>-mirror`), which an engine answers to as well.
pub fn served_id(name: &str, v: &Value) -> String {
    v["export"]["local_id"].as_str().filter(|s| !s.is_empty()).map(str::to_string).unwrap_or_else(|| format!("{name}-mirror"))
}

/// The create's body.
pub fn create_body(name: &str, golden: &str, size_bytes: u64, copies: u32, node: &str, owner: &Value) -> Value {
    let mut body = json!({
        "name": name,
        "size_bytes": size_bytes,
        "raid": "mirror",
        "replicas": copies,
        "from_golden": golden,
        "prefer_node": node,
    });
    if !owner.is_null() {
        body["owner"] = owner.clone();
    }
    body
}

/// May the sweep delete this stormstorage volume, its owner being gone? Only
/// a mirrored disk ([`SUFFIX`]) that serves no host: a machine stopping
/// somewhere still holds its host until its stop withdraws it.
pub fn sweepable(v: &Value) -> bool {
    let named = v["name"].as_str().is_some_and(|n| n.ends_with(SUFFIX));
    let hosts = v["export"]["hosts"].as_array().is_some_and(|h| !h.is_empty());
    named && !hosts
}

/// stormstorage's answer: its JSON, or `(status, body)`. `status` 0: no answer.
async fn answer(r: reqwest::Result<reqwest::Response>) -> Result<Value, (u16, String)> {
    let r = r.map_err(|e| (0, e.to_string()))?;
    let status = r.status().as_u16();
    let text = r.text().await.unwrap_or_default();
    if !(200..300).contains(&status) {
        return Err((status, text.trim().to_string()));
    }
    Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
}

/// The disk's volume as stormstorage has it, made when it has none.
/// `engine`/`storage` is this node's engine, which holds the golden.
#[allow(clippy::too_many_arguments)]
pub async fn find_or_create(
    ss: &EngineClient,
    engine: &EngineClient,
    storage: &str,
    name: &str,
    golden: &str,
    size: Option<&str>,
    copies: u32,
    node: &str,
    owner: &Value,
) -> Result<Value, NotYet> {
    let url = format!("{}/api/v1/volumes/{name}", ss.url());
    match answer(ss.get(&url).await).await {
        Ok(v) => return Ok(v),
        Err((404, _)) => {}
        Err((0, e)) => return Err(NotYet::Wait(format!("stormstorage did not answer for {name}: {e}"))),
        Err((s, e)) => return Err(NotYet::Wait(format!("stormstorage {s} for {name}: {e}"))),
    }
    let g = match answer(engine.get(&format!("{storage}/api/v1/volumes/{golden}")).await).await {
        Ok(g) => g,
        Err((404, _)) => return Err(NotYet::Wait(format!("waiting for golden {golden} on this node"))),
        Err((s, e)) => return Err(NotYet::Wait(format!("golden {golden}: the engine answered {s}: {e}"))),
    };
    let golden_bytes = g["virtual_size_bytes"].as_u64().or(g["size_bytes"].as_u64()).unwrap_or(0);
    let asked = match size {
        Some(s) => crate::storage::parse_quantity(s).ok_or_else(|| NotYet::Fail(format!("size {s:?} is not a quantity")))?,
        None => 0,
    };
    let size_bytes = golden_bytes.max(asked);
    if size_bytes == 0 {
        return Err(NotYet::Fail(format!("golden {golden} has no size, and the disk asks for none")));
    }
    let body = create_body(name, golden, size_bytes, copies, node, owner);
    match answer(ss.post(&format!("{}/api/v1/volumes", ss.url()), &body).await).await {
        Ok(v) => Ok(v),
        // No placed node holds the golden, or no room yet: may change.
        Err((409, e)) => Err(NotYet::Wait(format!("stormstorage cannot make {name} yet: {e}"))),
        Err((400, e)) => Err(NotYet::Fail(format!("stormstorage refused {name}: {e}"))),
        Err((0, e)) => Err(NotYet::Wait(format!("stormstorage did not answer the create of {name}: {e}"))),
        Err((s, e)) => Err(NotYet::Wait(format!("stormstorage {s} on the create of {name}: {e}"))),
    }
}

/// Serve `name` to `host_nqn` and read its record again (the served
/// volume's id is known once it is served per host).
pub async fn serve_to(ss: &EngineClient, name: &str, host_nqn: &str) -> Result<Value, NotYet> {
    let url = format!("{}/api/v1/volumes/{name}/export/hosts", ss.url());
    if let Err((s, e)) = answer(ss.post(&url, &json!({ "host_nqn": host_nqn })).await).await {
        let why = format!("stormstorage would not serve {name} to {host_nqn} ({s}): {e}");
        return Err(if s == 400 { NotYet::Fail(why) } else { NotYet::Wait(why) });
    }
    answer(ss.get(&format!("{}/api/v1/volumes/{name}", ss.url())).await)
        .await
        .map_err(|(s, e)| NotYet::Wait(format!("stormstorage {s} reading {name} after serving it: {e}")))
}

/// Stop serving `name` to `host_nqn`. `Ok` also when there is no such
/// volume: a disk that was not mirrored.
pub async fn withdraw(ss: &EngineClient, name: &str, host_nqn: &str) -> Result<(), String> {
    let url = format!("{}/api/v1/volumes/{name}/export/hosts/{}", ss.url(), urlencoding(host_nqn));
    match answer(ss.delete(&url).await).await {
        Ok(_) | Err((404, _)) => Ok(()),
        Err((s, e)) => Err(format!("stormstorage {s}: {e}")),
    }
}

/// Every stormstorage volume.
pub async fn list(ss: &EngineClient) -> Result<Vec<Value>, String> {
    let v = answer(ss.get(&format!("{}/api/v1/volumes", ss.url())).await).await.map_err(|(s, e)| format!("{s}: {e}"))?;
    Ok(v["volumes"].as_array().cloned().unwrap_or_default())
}

/// Delete `name` (stormstorage revokes its export, the assembly and the legs).
pub async fn delete(ss: &EngineClient, name: &str) -> Result<(), String> {
    match answer(ss.delete(&format!("{}/api/v1/volumes/{name}", ss.url())).await).await {
        Ok(_) | Err((404, _)) => Ok(()),
        Err((s, e)) => Err(format!("{s}: {e}")),
    }
}

/// A host NQN in a path: `:` and the rest of what an NQN holds are kept,
/// anything outside the unreserved set and `:` is percent-encoded.
fn urlencoding(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~:".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vmi(ann: &str) -> Value {
        json!({"metadata": {"annotations": {REQUEST: ann}}})
    }

    #[test]
    fn a_request_names_disks_and_at_least_two_copies() {
        assert!(requests(&json!({})).unwrap().is_empty());
        let r = requests(&vmi(r#"{"root": 2, "data": 3}"#)).unwrap();
        assert_eq!(r.get("root"), Some(&2));
        assert_eq!(r.get("data"), Some(&3));
        assert!(requests(&vmi(r#"{"root": 1}"#)).unwrap_err().contains("at least 2"));
        assert!(requests(&vmi(r#"{"root": "2"}"#)).is_err());
        assert!(requests(&vmi("root")).unwrap_err().contains("not JSON"));
        assert!(requests(&vmi("[1]")).unwrap_err().contains("not an object"));
    }

    #[test]
    fn only_a_goldens_disk_of_the_machine_is_mirrored() {
        let spec: stormvm_spec::VmSpec = stormvm_spec::kube::from_kube(&json!({
            "metadata": {"name": "web", "namespace": "default"},
            "spec": {"domain": {"devices": {"disks": [{"name": "root", "disk": {"bus": "virtio"}}, {"name": "data", "disk": {"bus": "virtio"}}]}},
                     "volumes": [{"name": "root", "containerDisk": {"image": "fedora-43"}},
                                 {"name": "data", "emptyDisk": {"capacity": "1Gi"}}]}
        }))
        .unwrap();
        let ask = |d: &str| BTreeMap::from([(d.to_string(), 2)]);
        assert_eq!(refusal(&ask("root"), &spec), None);
        assert!(refusal(&ask("data"), &spec).unwrap().contains("not made from a golden"));
        assert!(refusal(&ask("nope"), &spec).unwrap().contains("no disk nope"));
        assert_eq!(volume_name(&spec, "root"), "default.web-root.mirrored");
        assert_eq!(volume_name(&spec, "root"), name_of("default", "web", "root"));
    }

    #[test]
    fn the_parts_of_a_mirrored_disk_are_stormstorages() {
        assert!(is_part("default.web-root.mirrored"));
        assert!(is_part("default.web-root.mirrored-mirror"));
        assert!(is_part("default.web-root.mirrored-seed"));
        assert!(!is_part("default.web-root"));
        assert!(!is_part("pvc-1-mirror"));
    }

    #[test]
    fn the_seed_says_whether_to_wait_and_what_to_attach() {
        let n = "default.web-root.mirrored";
        assert!(seeded(n, &json!({"name": n})).unwrap_err().contains("not made from a golden"));
        let s = |seed: Value, export: Value| seeded(n, &json!({"seed": seed, "export": export})).unwrap();
        assert!(matches!(s(json!({"golden": "f", "state": "pending"}), Value::Null), Seeded::No(w) if w.contains("start copying")));
        assert_eq!(
            s(json!({"golden": "f", "state": "copying", "bytes_total": 100, "bytes_remaining": 40}), Value::Null),
            Seeded::No(format!("copying golden f into {n}: 40 of 100 bytes left"))
        );
        assert!(matches!(s(json!({"golden": "f", "state": "failed", "message": "blocks"}), Value::Null),
                         Seeded::No(w) if w.contains("(blocks)") && w.contains("cooldown")));
        assert_eq!(s(json!({"golden": "f", "state": "done"}), json!({"local_id": "u-9"})), Seeded::Yes("u-9".into()));
        assert_eq!(s(json!({"golden": "f", "state": "done"}), json!({})), Seeded::Yes(format!("{n}-mirror")));
    }

    #[test]
    fn the_create_asks_for_a_mirror_from_the_golden_here() {
        let owner = json!({"kind": "VirtualMachine", "namespace": "default", "name": "web", "uid": "u1"});
        let b = create_body("n.mirrored", "fedora-43", 10, 2, "node-a", &owner);
        assert_eq!(b, json!({"name": "n.mirrored", "size_bytes": 10, "raid": "mirror", "replicas": 2,
                             "from_golden": "fedora-43", "prefer_node": "node-a", "owner": owner}));
        assert!(create_body("n", "g", 1, 2, "a", &Value::Null).get("owner").is_none());
    }

    #[test]
    fn only_an_unserved_mirrored_disk_is_swept() {
        assert!(sweepable(&json!({"name": "a.b-root.mirrored", "export": {"hosts": []}})));
        assert!(sweepable(&json!({"name": "a.b-root.mirrored"})));
        assert!(!sweepable(&json!({"name": "a.b-root.mirrored", "export": {"hosts": [{"host_nqn": "nqn.x"}]}})));
        assert!(!sweepable(&json!({"name": "pvc-1"})));
    }

    #[test]
    fn an_nqn_goes_into_a_path_as_it_is() {
        assert_eq!(urlencoding("nqn.2026-09.lo.storm:host-a"), "nqn.2026-09.lo.storm:host-a");
        assert_eq!(urlencoding("a b/c"), "a%20b%2Fc");
    }
}
