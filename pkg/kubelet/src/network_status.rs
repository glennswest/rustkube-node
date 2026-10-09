//! A pod's `k8s.v1.cni.cncf.io/network-status` annotation (#131).
//!
//! The shape Multus writes and consoles read (the Network Plumbing Working
//! Group's): one entry per interface the CNI put in the pod, `{name,
//! interface, ips, mac, mtu, default, dns, gateway}`. Built from the CNI ADD's
//! result, which said all of it and was kept nowhere: MTU, prefix, gateway,
//! routes and which network wired the pod were recorded only in the plugin's
//! stdout.
//!
//! Three additions the NPWG shape has no field for, ignored by readers that do
//! not know them: `addresses` (each address with its prefix, as the CNI gave
//! it), `routes` (`{dst, gw}`) and `plugins` (the chain's types, e.g.
//! `["cilium-cni"]`).

use serde_json::{json, Value};

/// The annotation's name.
pub const ANNOTATION: &str = "k8s.v1.cni.cncf.io/network-status";

/// The entries for `result`. `mtu_of(interface)` answers for an interface the
/// result does not give an MTU for (a CNI before 1.1, or one that leaves it
/// out); `None` leaves the field out. The first interface is the pod's
/// default network, as the cluster's CNI is.
pub fn entries(result: &cni::CniResult, mtu_of: &dyn Fn(&str) -> Option<u32>) -> Value {
    // Interfaces inside the pod: the result also lists host-side ones (a
    // veth's peer, a bridge) with no sandbox. A result with no interfaces at
    // all is one interface, `eth0`.
    let pod_side: Vec<(Option<usize>, String, String, Option<u32>)> = if result.interfaces.is_empty() {
        vec![(None, "eth0".to_string(), String::new(), None)]
    } else {
        result
            .interfaces
            .iter()
            .enumerate()
            .filter(|(_, i)| !i.sandbox.is_empty())
            .map(|(n, i)| (Some(n), i.name.clone(), i.mac.clone(), i.mtu))
            .collect()
    };
    let dns = {
        let d = &result.dns;
        let mut o = serde_json::Map::new();
        if !d.nameservers.is_empty() {
            o.insert("nameservers".into(), json!(d.nameservers));
        }
        if !d.domain.is_empty() {
            o.insert("domain".into(), json!(d.domain));
        }
        if !d.search.is_empty() {
            o.insert("search".into(), json!(d.search));
        }
        o
    };
    let entries: Vec<Value> = pod_side
        .iter()
        .enumerate()
        .map(|(n, (index, name, mac, mtu))| {
            // An address with no interface index belongs to the only (or
            // first) pod interface.
            let ips: Vec<&cni::cni_types::CniIpConfig> = result
                .ips
                .iter()
                .filter(|ip| match (ip.interface, index) {
                    (Some(i), Some(own)) => i == *own,
                    _ => n == 0,
                })
                .collect();
            let mut e = json!({
                "name": result.network,
                "interface": name,
                "ips": ips.iter().map(|ip| ip.address.split('/').next().unwrap_or("")).collect::<Vec<_>>(),
                "addresses": ips.iter().map(|ip| ip.address.as_str()).collect::<Vec<_>>(),
                "default": n == 0,
            });
            if !mac.is_empty() {
                e["mac"] = json!(mac);
            }
            if let Some(m) = mtu.or_else(|| mtu_of(name)) {
                e["mtu"] = json!(m);
            }
            let gateways: Vec<&str> = ips.iter().filter_map(|ip| ip.gateway.as_deref()).collect();
            if !gateways.is_empty() {
                e["gateway"] = json!(gateways);
            }
            if !dns.is_empty() {
                e["dns"] = Value::Object(dns.clone());
            }
            if n == 0 && !result.routes.is_empty() {
                e["routes"] = result
                    .routes
                    .iter()
                    .map(|r| {
                        let mut o = json!({"dst": r.dst});
                        if !r.gw.is_empty() {
                            o["gw"] = json!(r.gw);
                        }
                        o
                    })
                    .collect();
            }
            if !result.plugins.is_empty() {
                e["plugins"] = json!(result.plugins);
            }
            e
        })
        .collect();
    Value::Array(entries)
}

/// An extra network's entries (#233): as [`entries`], named for its
/// NetworkAttachmentDefinition (`namespace/name`), never the default, and on
/// the interface it was put on when the plugin named none.
pub fn attachment_entries(result: &cni::CniResult, name: &str, ifname: &str, mtu_of: &dyn Fn(&str) -> Option<u32>) -> Value {
    let mut v = entries(result, mtu_of);
    for (i, e) in v.as_array_mut().into_iter().flatten().enumerate() {
        e["name"] = json!(name);
        e["default"] = json!(false);
        if result.interfaces.is_empty() && i == 0 {
            e["interface"] = json!(ifname);
        }
    }
    v
}

/// Name the default network's entries for the NAD that replaced it (#233).
pub fn rename(status: &mut Value, name: &str) {
    for e in status.as_array_mut().into_iter().flatten() {
        if e["default"] == json!(true) {
            e["name"] = json!(name);
        }
    }
}

/// Add `more` entries after `status`'s: the default network first, as Multus
/// writes it.
pub fn append(status: &mut Value, more: Value) {
    if let (Some(list), Value::Array(extra)) = (status.as_array_mut(), more) {
        list.extend(extra);
    }
}

/// The MTU of `ifname` inside the network namespace at `netns` (a
/// `/proc/<pid>/ns/net`), read with `SIOCGIFMTU` from a thread that joins it:
/// `setns` changes only the calling thread, which ends right after.
pub fn mtu_in_netns(netns: &str, ifname: &str) -> Option<u32> {
    let (netns, ifname) = (netns.to_string(), ifname.to_string());
    std::thread::spawn(move || -> Option<u32> {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::MetadataExt;
        let file = std::fs::File::open(&netns).ok()?;
        // Already there (a host-network pod): no setns, which needs
        // CAP_SYS_ADMIN even to join the namespace one is in.
        let target = file.metadata().ok()?;
        let here = std::fs::metadata("/proc/thread-self/ns/net").ok()?;
        // SAFETY: setns affects only this thread, which ends right after.
        if (target.dev(), target.ino()) != (here.dev(), here.ino())
            && unsafe { libc::setns(file.as_raw_fd(), libc::CLONE_NEWNET) } != 0
        {
            return None;
        }
        // SAFETY: a plain datagram socket, closed below.
        let sock = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
        if sock < 0 {
            return None;
        }
        // SAFETY: zeroed ifreq is valid; the name is copied with room for NUL.
        let mut req: libc::ifreq = unsafe { std::mem::zeroed() };
        let bytes = ifname.as_bytes();
        if bytes.len() >= req.ifr_name.len() {
            unsafe { libc::close(sock) };
            return None;
        }
        for (d, s) in req.ifr_name.iter_mut().zip(bytes) {
            *d = *s as libc::c_char;
        }
        // SAFETY: SIOCGIFMTU fills ifr_ifru.ifru_mtu of the ifreq passed.
        let rc = unsafe { libc::ioctl(sock, libc::SIOCGIFMTU as _, &mut req) };
        let mtu = unsafe { req.ifr_ifru.ifru_mtu };
        unsafe { libc::close(sock) };
        (rc == 0 && mtu > 0).then_some(mtu as u32)
    })
    .join()
    .ok()
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result() -> cni::CniResult {
        serde_json::from_value(json!({
            "cniVersion": "1.1.0",
            "interfaces": [
                {"name": "lxc1234", "mac": "aa:aa:aa:aa:aa:aa"},
                {"name": "eth0", "mac": "0a:58:0a:00:00:05", "sandbox": "/proc/42/ns/net", "mtu": 1450}
            ],
            "ips": [{"address": "10.0.0.5/24", "gateway": "10.0.0.1", "interface": 1}],
            "routes": [{"dst": "0.0.0.0/0", "gw": "10.0.0.1"}],
            "dns": {"nameservers": ["10.96.0.10"], "search": ["default.svc.cluster.local"]}
        }))
        .map(|mut r: cni::CniResult| {
            r.network = "cilium".into();
            r.plugins = vec!["cilium-cni".into()];
            r
        })
        .unwrap()
    }

    /// #131: the pod's interface only, in the NPWG shape, with the additions.
    #[test]
    fn the_cni_result_becomes_multus_network_status() {
        let got = entries(&result(), &|_| None);
        assert_eq!(
            got,
            json!([{
                "name": "cilium", "interface": "eth0", "default": true,
                "ips": ["10.0.0.5"], "addresses": ["10.0.0.5/24"],
                "mac": "0a:58:0a:00:00:05", "mtu": 1450, "gateway": ["10.0.0.1"],
                "dns": {"nameservers": ["10.96.0.10"], "search": ["default.svc.cluster.local"]},
                "routes": [{"dst": "0.0.0.0/0", "gw": "10.0.0.1"}],
                "plugins": ["cilium-cni"]
            }])
        );
    }

    /// An MTU the plugin left out is read from the pod; a result with no
    /// interfaces is eth0 with every address.
    #[test]
    fn a_missing_mtu_is_asked_for_and_a_bare_result_is_eth0() {
        let mut r = result();
        r.interfaces[1].mtu = None;
        assert_eq!(entries(&r, &|i| (i == "eth0").then_some(1500))[0]["mtu"], 1500);
        assert!(entries(&r, &|_| None)[0].get("mtu").is_none());

        let bare: cni::CniResult = serde_json::from_value(json!({"ips": [{"address": "10.0.0.9/16"}]})).unwrap();
        let got = entries(&bare, &|_| None);
        assert_eq!(got[0]["interface"], "eth0");
        assert_eq!(got[0]["ips"], json!(["10.0.0.9"]));
        assert!(got[0].get("gateway").is_none());
    }

    /// This thread's own namespace has a loopback with an MTU.
    #[test]
    fn the_mtu_is_read_inside_a_namespace() {
        let mtu = mtu_in_netns("/proc/self/ns/net", "lo");
        assert!(mtu.is_some_and(|m| m >= 1500), "{mtu:?}");
        assert_eq!(mtu_in_netns("/proc/self/ns/net", "no-such-if"), None);
    }

    /// #233: the default network first, then each extra one, named for its
    /// NAD, on its own interface, never the default.
    #[test]
    fn extra_networks_follow_the_default() {
        let default: cni::CniResult = serde_json::from_value(serde_json::json!({
            "cniVersion": "1.0.0",
            "interfaces": [{"name": "eth0", "sandbox": "/proc/9/ns/net", "mac": "aa:aa:aa:aa:aa:aa"}],
            "ips": [{"address": "10.0.0.5/24", "interface": 0}]})).unwrap();
        let lan: cni::CniResult = serde_json::from_value(serde_json::json!({
            "cniVersion": "1.0.0", "ips": [{"address": "192.168.50.9/24"}]})).unwrap();
        let none = |_: &str| None;
        let mut status = entries(&default, &none);
        append(&mut status, attachment_entries(&lan, "default/lan", "net1", &none));
        let l = status.as_array().unwrap();
        assert_eq!(l.len(), 2);
        assert_eq!((l[0]["interface"].clone(), l[0]["default"].clone()), (json!("eth0"), json!(true)));
        assert_eq!(l[1]["name"], "default/lan");
        assert_eq!(l[1]["interface"], "net1");
        assert_eq!(l[1]["default"], false);
        assert_eq!(l[1]["ips"], json!(["192.168.50.9"]));
        rename(&mut status, "kube-system/vlan248");
        assert_eq!(status[0]["name"], "kube-system/vlan248");
        assert_eq!(status[1]["name"], "default/lan", "only the default is renamed");
    }
}
