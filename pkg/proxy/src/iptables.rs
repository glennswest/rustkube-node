//! iptables rule generation and application.
//!
//! `generate_rules` is a **pure** function: given the service model
//! (`&[ServiceInfo]`) it produces an `IptablesRules` value describing the
//! intended NAT-table state, using the classic kube-proxy chain layout
//! (`KUBE-SERVICES` / `KUBE-SVC-*` / `KUBE-SEP-*` / `KUBE-NODEPORTS` /
//! masquerade). It never touches the kernel, so it is fully unit-testable on
//! any platform (including macOS).
//!
//! Applying the rules is hidden behind the [`RuleApplier`] trait so the kernel
//! interaction (`iptables-restore`) is a thin, mockable seam. The real
//! Linux-only implementation lives in [`IptablesRestoreApplier`] (compiled with
//! `#[cfg(target_os = "linux")]`); a no-op applier is used elsewhere and a
//! collecting applier is available for tests.

use crate::service_map::{ServiceInfo, ServiceKey};

/// Custom iptables chain names (kube-proxy compatible).
pub const CHAIN_SERVICES: &str = "KUBE-SERVICES";
pub const CHAIN_NODEPORTS: &str = "KUBE-NODEPORTS";
pub const CHAIN_POSTROUTING: &str = "KUBE-POSTROUTING";
pub const CHAIN_MARK_MASQ: &str = "KUBE-MARK-MASQ";

/// The masquerade fwmark used to tag traffic that must be SNAT'd on egress.
const MASQ_MARK: &str = "0x4000/0x4000";

/// An iptables rule set describing the intended NAT table state.
///
/// This is a plain data value (chain declarations + append rules) so it can be
/// asserted against in unit tests without applying anything to the kernel.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IptablesRules {
    /// Chains to declare (`:CHAIN - [0:0]` in restore syntax). Emitted before
    /// any append rule so `iptables-restore` sees every chain up front.
    pub chains: Vec<String>,
    /// `-A CHAIN ...` append rules, in application order.
    pub nat_rules: Vec<String>,
}

impl IptablesRules {
    /// Render the rules as `iptables-restore` input for the `nat` table.
    pub fn to_restore_input(&self) -> String {
        let mut out = String::from("*nat\n");
        for chain in &self.chains {
            out.push_str(&format!(":{chain} - [0:0]\n"));
        }
        for rule in &self.nat_rules {
            out.push_str(rule);
            out.push('\n');
        }
        out.push_str("COMMIT\n");
        out
    }

    /// True if any DNAT rule is present (used by tests / diagnostics).
    pub fn has_dnat(&self) -> bool {
        self.nat_rules.iter().any(|r| r.contains("-j DNAT"))
    }
}

/// Deterministic per-service chain name, e.g. `KUBE-SVC-XXXXXXXXXXXXX`.
///
/// The hash is derived from the service identity (namespace/name/port/proto),
/// mirroring upstream kube-proxy's stable, collision-resistant chain naming.
pub fn service_chain_name(key: &ServiceKey) -> String {
    let seed = format!(
        "{}/{}:{}:{}",
        key.namespace, key.name, key.port, key.protocol
    );
    format!("KUBE-SVC-{}", short_hash(&seed))
}

/// Deterministic per-endpoint chain name, e.g. `KUBE-SEP-XXXXXXXXXXXXX`.
pub fn endpoint_chain_name(key: &ServiceKey, ep_ip: &str, ep_port: u16) -> String {
    let seed = format!(
        "{}/{}:{}:{}->{}:{}",
        key.namespace, key.name, key.port, key.protocol, ep_ip, ep_port
    );
    format!("KUBE-SEP-{}", short_hash(&seed))
}

/// FNV-1a 64-bit hash rendered as RFC-4648 base32 (uppercase, no padding).
fn short_hash(input: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in input.bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    base32(&hash.to_be_bytes())
}

/// Minimal RFC-4648 base32 encoder (uppercase alphabet, no padding).
fn base32(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut out = String::new();
    let mut buffer: u32 = 0;
    let mut bits: u32 = 0;
    for &b in bytes {
        buffer = (buffer << 8) | b as u32;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            let idx = ((buffer >> bits) & 0x1f) as usize;
            out.push(ALPHABET[idx] as char);
        }
    }
    if bits > 0 {
        let idx = ((buffer << (5 - bits)) & 0x1f) as usize;
        out.push(ALPHABET[idx] as char);
    }
    out
}

/// Generate the intended NAT rule set from the current service model.
///
/// Pure: no kernel interaction, deterministic given its input. Layout:
///
/// * `KUBE-SERVICES` — dispatch: a ClusterIP match per service port jumping to
///   its `KUBE-SVC-*` chain, plus a tail jump to `KUBE-NODEPORTS` for
///   locally-destined traffic.
/// * `KUBE-SVC-*` — per-service load balancing: one `-m statistic` jump per
///   ready endpoint's `KUBE-SEP-*` chain, with the last endpoint taking the
///   remainder unconditionally.
/// * `KUBE-SEP-*` — per-endpoint: mark-for-masquerade on hairpin traffic, then
///   `DNAT` to the endpoint IP:port.
/// * `KUBE-NODEPORTS` — NodePort dispatch to the service chain.
/// * `KUBE-MARK-MASQ` / `KUBE-POSTROUTING` — masquerade plumbing.
///
/// Services with no ready endpoints produce **no** DNAT (no service chain).
pub fn generate_rules(services: &[ServiceInfo]) -> IptablesRules {
    let mut chains = vec![
        CHAIN_SERVICES.to_string(),
        CHAIN_NODEPORTS.to_string(),
        CHAIN_POSTROUTING.to_string(),
        CHAIN_MARK_MASQ.to_string(),
    ];
    let mut nat_rules = Vec::new();

    // Masquerade plumbing (static).
    nat_rules.push(format!(
        "-A {CHAIN_MARK_MASQ} -j MARK --or-mark 0x4000"
    ));
    nat_rules.push(format!(
        "-A {CHAIN_POSTROUTING} -m mark ! --mark {MASQ_MARK} -j RETURN"
    ));
    nat_rules.push(format!(
        "-A {CHAIN_POSTROUTING} -j MARK --xor-mark 0x4000"
    ));
    nat_rules.push(format!(
        "-A {CHAIN_POSTROUTING} -m comment --comment \"kubernetes service traffic masquerade\" -j MASQUERADE"
    ));

    // Deterministic ordering so output is stable across runs (the service map
    // is backed by a DashMap, whose iteration order is not defined).
    let mut services: Vec<&ServiceInfo> = services.iter().collect();
    services.sort_by(|a, b| {
        (a.key.namespace.as_str(), a.key.name.as_str(), a.key.port)
            .cmp(&(b.key.namespace.as_str(), b.key.name.as_str(), b.key.port))
    });

    for svc in services {
        let cluster_ip = &svc.key.cluster_ip;
        let port = svc.key.port;
        let proto = svc.key.protocol.to_lowercase();

        let ready: Vec<_> = svc.endpoints.iter().filter(|e| e.ready).collect();

        // Headless / selectorless / no-ready-endpoint services: no DNAT.
        if cluster_ip.is_empty() || cluster_ip == "None" || ready.is_empty() {
            continue;
        }

        let svc_chain = service_chain_name(&svc.key);
        chains.push(svc_chain.clone());

        let svc_comment = format!("{}/{} cluster IP", svc.key.namespace, svc.key.name);

        // KUBE-SERVICES: ClusterIP match → service chain.
        nat_rules.push(format!(
            "-A {CHAIN_SERVICES} -d {cluster_ip}/32 -p {proto} -m {proto} --dport {port} \
             -m comment --comment \"{svc_comment}\" -j {svc_chain}"
        ));

        // NodePort: KUBE-NODEPORTS → service chain (+ masquerade the traffic).
        if let Some(node_port) = svc.key.node_port {
            let np_comment = format!("{}/{} nodeport", svc.key.namespace, svc.key.name);
            nat_rules.push(format!(
                "-A {CHAIN_NODEPORTS} -p {proto} -m {proto} --dport {node_port} \
                 -m comment --comment \"{np_comment}\" -j {CHAIN_MARK_MASQ}"
            ));
            nat_rules.push(format!(
                "-A {CHAIN_NODEPORTS} -p {proto} -m {proto} --dport {node_port} \
                 -m comment --comment \"{np_comment}\" -j {svc_chain}"
            ));
        }

        // Per-endpoint SEP chains: mark-masq on hairpin + DNAT to endpoint.
        let count = ready.len();
        let mut sep_chains = Vec::with_capacity(count);
        for ep in &ready {
            let sep_chain = endpoint_chain_name(&svc.key, &ep.ip, ep.port);
            chains.push(sep_chain.clone());
            nat_rules.push(format!(
                "-A {sep_chain} -s {}/32 -j {CHAIN_MARK_MASQ}",
                ep.ip
            ));
            nat_rules.push(format!(
                "-A {sep_chain} -p {proto} -m {proto} -j DNAT --to-destination {}:{}",
                ep.ip, ep.port
            ));
            sep_chains.push(sep_chain);
        }

        // KUBE-SVC chain: probabilistic load balancing across the SEP chains.
        for (i, sep_chain) in sep_chains.iter().enumerate() {
            let remaining = count - i;
            if remaining > 1 {
                let probability = 1.0 / remaining as f64;
                nat_rules.push(format!(
                    "-A {svc_chain} -m statistic --mode random --probability {probability:.5} \
                     -j {sep_chain}"
                ));
            } else {
                // Last endpoint takes all remaining traffic.
                nat_rules.push(format!("-A {svc_chain} -j {sep_chain}"));
            }
        }
    }

    // Dispatch NodePort traffic destined for a local address.
    nat_rules.push(format!(
        "-A {CHAIN_SERVICES} -m addrtype --dst-type LOCAL \
         -m comment --comment \"kubernetes service nodeports\" -j {CHAIN_NODEPORTS}"
    ));

    IptablesRules { chains, nat_rules }
}

/// A mockable seam for applying an [`IptablesRules`] set to the dataplane.
///
/// The real (Linux) implementation shells out to `iptables-restore`; tests and
/// non-Linux builds use collecting / no-op implementations so nothing touches
/// the kernel.
#[async_trait::async_trait]
pub trait RuleApplier: Send + Sync {
    async fn apply(&self, rules: &IptablesRules) -> anyhow::Result<()>;
}

/// The platform default applier: real `iptables-restore` on Linux, no-op
/// elsewhere (e.g. macOS development hosts).
pub fn default_applier() -> Box<dyn RuleApplier> {
    #[cfg(target_os = "linux")]
    {
        Box::new(IptablesRestoreApplier::default())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Box::new(NoopApplier)
    }
}

/// No-op applier: logs how many rules *would* be applied but does nothing.
/// Used on non-Linux platforms and wherever a real dataplane is unavailable.
pub struct NoopApplier;

#[async_trait::async_trait]
impl RuleApplier for NoopApplier {
    async fn apply(&self, rules: &IptablesRules) -> anyhow::Result<()> {
        tracing::info!(
            "iptables apply skipped (no-op applier): {} chains, {} rules",
            rules.chains.len(),
            rules.nat_rules.len()
        );
        Ok(())
    }
}

/// Real applier: pipes generated rules to `iptables-restore --noflush`.
/// Linux only — depends on the host iptables binaries.
#[cfg(target_os = "linux")]
#[derive(Default)]
pub struct IptablesRestoreApplier;

#[cfg(target_os = "linux")]
#[async_trait::async_trait]
impl RuleApplier for IptablesRestoreApplier {
    async fn apply(&self, rules: &IptablesRules) -> anyhow::Result<()> {
        use std::io::Write;
        use std::process::{Command, Stdio};

        let restore_input = rules.to_restore_input();
        tracing::debug!(
            "Applying {} iptables rules via iptables-restore",
            rules.nat_rules.len()
        );

        let output = Command::new("iptables-restore")
            .arg("--noflush")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                if let Some(ref mut stdin) = child.stdin {
                    stdin.write_all(restore_input.as_bytes())?;
                }
                child.wait_with_output()
            });

        match output {
            Ok(out) if out.status.success() => {
                tracing::info!("Applied {} iptables rules", rules.nat_rules.len());
                Ok(())
            }
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                tracing::error!("iptables-restore failed: {stderr}");
                anyhow::bail!("iptables-restore failed: {stderr}");
            }
            Err(e) => {
                tracing::error!("Failed to run iptables-restore: {e}");
                anyhow::bail!("Failed to run iptables-restore: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service_map::{Endpoint, ServiceInfo, ServiceKey};

    fn ep(ip: &str, port: u16, ready: bool) -> Endpoint {
        Endpoint {
            ip: ip.to_string(),
            port,
            ready,
        }
    }

    fn svc(key: ServiceKey, endpoints: Vec<Endpoint>) -> ServiceInfo {
        ServiceInfo {
            key,
            endpoints,
            session_affinity: false,
        }
    }

    fn clusterip_key(ns: &str, name: &str, ip: &str, port: u16) -> ServiceKey {
        ServiceKey {
            namespace: ns.to_string(),
            name: name.to_string(),
            cluster_ip: ip.to_string(),
            port,
            protocol: "TCP".to_string(),
            node_port: None,
        }
    }

    #[test]
    fn two_ready_endpoints_produce_svc_chain_with_two_sep_jumps_and_50pct() {
        let key = clusterip_key("default", "web", "10.0.0.10", 80);
        let rules = generate_rules(&[svc(
            key.clone(),
            vec![ep("10.244.0.1", 8080, true), ep("10.244.0.2", 8080, true)],
        )]);

        let svc_chain = service_chain_name(&key);

        // Two KUBE-SEP jumps live in the KUBE-SVC chain.
        let sep_jumps: Vec<&String> = rules
            .nat_rules
            .iter()
            .filter(|r| r.starts_with(&format!("-A {svc_chain}")) && r.contains("KUBE-SEP-"))
            .collect();
        assert_eq!(sep_jumps.len(), 2, "expected two SEP jumps in the SVC chain");

        // First jump load-balances ~50%.
        assert!(
            sep_jumps[0].contains("--probability 0.50000"),
            "first SEP jump should use ~50% statistic probability: {}",
            sep_jumps[0]
        );
        // Second jump is the unconditional remainder.
        assert!(
            !sep_jumps[1].contains("statistic"),
            "last SEP jump should be unconditional: {}",
            sep_jumps[1]
        );

        // Two DNAT rules, one per endpoint IP.
        assert!(rules
            .nat_rules
            .iter()
            .any(|r| r.contains("-j DNAT --to-destination 10.244.0.1:8080")));
        assert!(rules
            .nat_rules
            .iter()
            .any(|r| r.contains("-j DNAT --to-destination 10.244.0.2:8080")));
    }

    #[test]
    fn clusterip_dispatch_matches_service_ip_and_port() {
        let key = clusterip_key("default", "web", "10.0.0.10", 80);
        let svc_chain = service_chain_name(&key);
        let rules = generate_rules(&[svc(key, vec![ep("10.244.0.1", 8080, true)])]);

        let dispatch = rules
            .nat_rules
            .iter()
            .find(|r| r.starts_with(&format!("-A {CHAIN_SERVICES}")) && r.contains("10.0.0.10"))
            .expect("expected a KUBE-SERVICES ClusterIP dispatch rule");

        assert!(dispatch.contains("-d 10.0.0.10/32"), "rule: {dispatch}");
        assert!(dispatch.contains("--dport 80"), "rule: {dispatch}");
        assert!(dispatch.contains(&format!("-j {svc_chain}")), "rule: {dispatch}");
    }

    #[test]
    fn headless_or_no_endpoints_produce_no_dnat() {
        // Selectorless / no ready endpoints.
        let key = clusterip_key("default", "web", "10.0.0.10", 80);
        let rules = generate_rules(&[svc(key, vec![])]);
        assert!(!rules.has_dnat(), "no endpoints must yield no DNAT");

        // Not-ready endpoints only.
        let key2 = clusterip_key("default", "web2", "10.0.0.11", 80);
        let rules2 = generate_rules(&[svc(key2, vec![ep("10.244.0.9", 8080, false)])]);
        assert!(!rules2.has_dnat(), "unready endpoints must yield no DNAT");

        // Headless (ClusterIP None).
        let mut hkey = clusterip_key("default", "hdl", "None", 80);
        hkey.cluster_ip = "None".to_string();
        let rules3 = generate_rules(&[svc(hkey, vec![ep("10.244.0.5", 8080, true)])]);
        assert!(!rules3.has_dnat(), "headless service must yield no DNAT");
    }

    #[test]
    fn nodeport_adds_nodeports_rule() {
        let mut key = clusterip_key("default", "web", "10.0.0.10", 80);
        key.node_port = Some(30080);
        let svc_chain = service_chain_name(&key);
        let rules = generate_rules(&[svc(key, vec![ep("10.244.0.1", 8080, true)])]);

        let np_jump = rules
            .nat_rules
            .iter()
            .find(|r| {
                r.starts_with(&format!("-A {CHAIN_NODEPORTS}"))
                    && r.contains("--dport 30080")
                    && r.contains(&format!("-j {svc_chain}"))
            })
            .expect("expected a KUBE-NODEPORTS rule dispatching to the service chain");
        assert!(np_jump.contains("--dport 30080"));
    }

    #[test]
    fn no_nodeport_means_no_nodeport_dispatch() {
        let key = clusterip_key("default", "web", "10.0.0.10", 80);
        let rules = generate_rules(&[svc(key, vec![ep("10.244.0.1", 8080, true)])]);
        assert!(
            !rules
                .nat_rules
                .iter()
                .any(|r| r.starts_with(&format!("-A {CHAIN_NODEPORTS} -p"))),
            "ClusterIP-only service must not add a NodePort dispatch rule"
        );
    }

    #[test]
    fn masquerade_plumbing_is_present() {
        let rules = generate_rules(&[]);
        assert!(rules
            .nat_rules
            .iter()
            .any(|r| r.contains(CHAIN_POSTROUTING) && r.contains("MASQUERADE")));
        assert!(rules
            .nat_rules
            .iter()
            .any(|r| r.starts_with(&format!("-A {CHAIN_MARK_MASQ}")) && r.contains("MARK")));
    }

    #[test]
    fn output_is_deterministic() {
        let key = clusterip_key("default", "web", "10.0.0.10", 80);
        let s = svc(
            key,
            vec![ep("10.244.0.1", 8080, true), ep("10.244.0.2", 8080, true)],
        );
        let a = generate_rules(std::slice::from_ref(&s));
        let b = generate_rules(std::slice::from_ref(&s));
        assert_eq!(a, b, "rule generation must be deterministic");
    }

    #[test]
    fn restore_input_wraps_nat_table_and_declares_chains() {
        let key = clusterip_key("default", "web", "10.0.0.10", 80);
        let rules = generate_rules(&[svc(key, vec![ep("10.244.0.1", 8080, true)])]);
        let input = rules.to_restore_input();
        assert!(input.starts_with("*nat\n"));
        assert!(input.trim_end().ends_with("COMMIT"));
        assert!(input.contains(&format!(":{CHAIN_SERVICES} - [0:0]")));
    }

    #[tokio::test]
    async fn noop_applier_succeeds_without_touching_kernel() {
        let rules = generate_rules(&[]);
        let applier = NoopApplier;
        assert!(applier.apply(&rules).await.is_ok());
    }
}
