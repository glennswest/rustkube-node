//! A pod's extra networks, the Multus way (#233; the owner's decision on
//! stormcos#249: the Multus/OpenShift standard as it is).
//!
//! - `k8s.v1.cni.cncf.io/networks` on the pod names NetworkAttachmentDefinitions
//!   (`k8s.cni.cncf.io/v1`): a comma list of `name`, `namespace/name` or
//!   `name@ifname`, or a JSON list of `{name, namespace, interface, ips, mac}`.
//!   Each is one more CNI ADD after the default network, on `net1`, `net2`, …
//!   (by position) unless the pod names the interface.
//! - `v1.multus-cni.io/default-network` replaces the default network with a
//!   NAD's config, on `eth0`.
//! - A NAD is looked for in the pod's namespace unless the entry names one.
//!   Its `spec.config` is the CNI config run as it is; the plugins it names
//!   come from the node's CNI bin directory.
//!
//! A NAD that does not exist, has no config, or cannot be read is a sandbox
//! that waits, naming it (`NetworkNotReady`, as a CNI that is not up yet).

use serde_json::Value;

use crate::cri::NetworkAttachment;

/// The pod annotation naming its extra networks.
pub const NETWORKS: &str = "k8s.v1.cni.cncf.io/networks";
/// The pod annotation replacing its default network.
pub const DEFAULT_NETWORK: &str = "v1.multus-cni.io/default-network";

/// One network a pod asks for, before it is resolved.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    pub namespace: Option<String>,
    pub name: String,
    pub interface: Option<String>,
    pub ips: Vec<String>,
    pub mac: Option<String>,
}

/// `namespace/name@ifname`, any part but the name optional.
fn parse_one(item: &str) -> Result<Selection, String> {
    let item = item.trim();
    let (rest, interface) = match item.split_once('@') {
        Some((r, i)) if !i.is_empty() => (r, Some(i.to_string())),
        Some(_) => return Err(format!("{item:?}: empty interface after @")),
        None => (item, None),
    };
    let (namespace, name) = match rest.split_once('/') {
        Some((ns, n)) => (Some(ns.to_string()), n.to_string()),
        None => (None, rest.to_string()),
    };
    if name.is_empty() || namespace.as_deref() == Some("") {
        return Err(format!("{item:?}: not a network name"));
    }
    Ok(Selection { namespace, name, interface, ..Default::default() })
}

/// The `networks` annotation's entries, in order.
pub fn parse(annotation: &str) -> Result<Vec<Selection>, String> {
    let a = annotation.trim();
    if a.is_empty() {
        return Ok(Vec::new());
    }
    if a.starts_with('[') {
        let list: Vec<Value> = serde_json::from_str(a).map_err(|e| format!("{NETWORKS}: not a JSON list: {e}"))?;
        return list
            .iter()
            .map(|e| {
                let name = e["name"].as_str().filter(|n| !n.is_empty()).ok_or_else(|| format!("{NETWORKS}: an entry with no name: {e}"))?;
                Ok(Selection {
                    namespace: e["namespace"].as_str().filter(|n| !n.is_empty()).map(str::to_string),
                    name: name.to_string(),
                    interface: e["interface"].as_str().filter(|n| !n.is_empty()).map(str::to_string),
                    ips: e["ips"].as_array().into_iter().flatten().filter_map(|i| i.as_str().map(str::to_string)).collect(),
                    mac: e["mac"].as_str().filter(|n| !n.is_empty()).map(str::to_string),
                })
            })
            .collect();
    }
    a.split(',').filter(|s| !s.trim().is_empty()).map(parse_one).collect()
}

/// Where a NAD is read from.
pub fn nad_path(namespace: &str, name: &str) -> String {
    format!("/apis/k8s.cni.cncf.io/v1/namespaces/{namespace}/network-attachment-definitions/{name}")
}

/// A selection resolved against its NAD (`nad` as the apiserver gave it).
pub fn attachment(sel: &Selection, pod_namespace: &str, ifname: String, nad: Option<&Value>) -> Result<NetworkAttachment, String> {
    let ns = sel.namespace.as_deref().unwrap_or(pod_namespace);
    let name = format!("{ns}/{}", sel.name);
    let nad = nad.ok_or_else(|| format!("NetworkAttachmentDefinition {name} not found"))?;
    let config = nad["spec"]["config"]
        .as_str()
        .filter(|c| !c.trim().is_empty())
        .ok_or_else(|| format!("NetworkAttachmentDefinition {name} has no spec.config"))?;
    Ok(NetworkAttachment { name, ifname, config: config.to_string(), ips: sel.ips.clone(), mac: sel.mac.clone() })
}

/// The interface a selection is put on: its own, else `net<position>`.
pub fn ifname(sel: &Selection, position: usize) -> String {
    sel.interface.clone().unwrap_or_else(|| format!("net{}", position + 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_networks_annotation_in_both_forms() {
        let l = parse("lan, other/storage@stor0 ,  vlan42").unwrap();
        assert_eq!(l.len(), 3);
        assert_eq!((l[0].namespace.clone(), l[0].name.as_str(), l[0].interface.clone()), (None, "lan", None));
        assert_eq!((l[1].namespace.as_deref(), l[1].name.as_str(), l[1].interface.as_deref()), (Some("other"), "storage", Some("stor0")));
        assert_eq!(ifname(&l[0], 0), "net1");
        assert_eq!(ifname(&l[1], 1), "stor0");
        assert_eq!(ifname(&l[2], 2), "net3", "by position, as Multus counts");

        let j = parse(r#"[{"name":"lan","interface":"lan0","ips":["10.1.0.5/24"],"mac":"02:00:00:00:00:05"},{"name":"b","namespace":"x"}]"#).unwrap();
        assert_eq!(j[0].ips, vec!["10.1.0.5/24".to_string()]);
        assert_eq!(j[0].mac.as_deref(), Some("02:00:00:00:00:05"));
        assert_eq!(j[1].namespace.as_deref(), Some("x"));
        assert!(parse("").unwrap().is_empty());
        for bad in ["/x", "ns/", "a@", r#"[{"interface":"x"}]"#, "[not json"] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_selection_resolves_to_its_nads_config_or_says_why_not() {
        let sel = parse("lan").unwrap().remove(0);
        let nad = json!({"spec": {"config": "{\"cniVersion\":\"1.0.0\",\"name\":\"lan\",\"type\":\"macvlan\"}"}});
        let a = attachment(&sel, "default", "net1".into(), Some(&nad)).unwrap();
        assert_eq!((a.name.as_str(), a.ifname.as_str()), ("default/lan", "net1"));
        assert!(a.config.contains("macvlan"));
        assert_eq!(attachment(&sel, "default", "net1".into(), None).unwrap_err(), "NetworkAttachmentDefinition default/lan not found");
        assert!(attachment(&sel, "default", "net1".into(), Some(&json!({"spec": {}}))).unwrap_err().contains("no spec.config"));
        assert_eq!(nad_path("default", "lan"), "/apis/k8s.cni.cncf.io/v1/namespaces/default/network-attachment-definitions/lan");
    }
}
