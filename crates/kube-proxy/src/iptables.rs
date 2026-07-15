//! iptables rule generator.
//!
//! Generates and applies iptables DNAT/SNAT rules for
//! ClusterIP and NodePort services.
//! Linux-only — uses Command to invoke iptables.

use crate::service_map::ServiceInfo;
use tracing::info;

/// Custom iptables chain names.
const CHAIN_SERVICES: &str = "RK-SERVICES";
const CHAIN_NODEPORTS: &str = "RK-NODEPORTS";
const CHAIN_POSTROUTING: &str = "RK-POSTROUTING";

/// An iptables rule set ready to be applied.
#[derive(Debug, Clone)]
pub struct IptablesRules {
    pub nat_rules: Vec<String>,
    pub filter_rules: Vec<String>,
}

/// Generate iptables rules from the current service map.
pub fn generate_rules(services: &[ServiceInfo]) -> IptablesRules {
    let mut nat_rules = Vec::new();

    // Create custom chains
    nat_rules.push(format!("-N {CHAIN_SERVICES} 2>/dev/null || true"));
    nat_rules.push(format!("-N {CHAIN_NODEPORTS} 2>/dev/null || true"));
    nat_rules.push(format!("-N {CHAIN_POSTROUTING} 2>/dev/null || true"));

    // Flush our chains
    nat_rules.push(format!("-F {CHAIN_SERVICES}"));
    nat_rules.push(format!("-F {CHAIN_NODEPORTS}"));
    nat_rules.push(format!("-F {CHAIN_POSTROUTING}"));

    // Jump from PREROUTING/OUTPUT to our service chain
    nat_rules.push(format!(
        "-A PREROUTING -j {CHAIN_SERVICES} -m comment --comment \"rustkube service proxy\""
    ));
    nat_rules.push(format!(
        "-A OUTPUT -j {CHAIN_SERVICES} -m comment --comment \"rustkube service proxy\""
    ));

    // Masquerade for traffic going to service backends
    nat_rules.push(format!(
        "-A POSTROUTING -j {CHAIN_POSTROUTING} -m comment --comment \"rustkube postrouting\""
    ));

    for svc in services {
        let cluster_ip = &svc.key.cluster_ip;
        let port = svc.key.port;
        let proto = svc.key.protocol.to_lowercase();
        let endpoints = &svc.endpoints;

        if endpoints.is_empty() {
            continue;
        }

        let svc_comment = format!(
            "{}/{} cluster IP",
            svc.key.namespace, svc.key.name
        );

        if endpoints.len() == 1 {
            // Single endpoint — simple DNAT
            let ep = &endpoints[0];
            nat_rules.push(format!(
                "-A {CHAIN_SERVICES} -d {cluster_ip}/32 -p {proto} --dport {port} \
                 -j DNAT --to-destination {}:{} \
                 -m comment --comment \"{svc_comment}\"",
                ep.ip, ep.port
            ));
        } else {
            // Multiple endpoints — probabilistic load balancing
            let chain_name = format!(
                "RK-SVC-{}-{}",
                svc.key.namespace.chars().take(4).collect::<String>().to_uppercase(),
                svc.key.name.chars().take(8).collect::<String>().to_uppercase()
            );

            nat_rules.push(format!("-N {chain_name} 2>/dev/null || true"));
            nat_rules.push(format!("-F {chain_name}"));

            // Jump to per-service chain
            nat_rules.push(format!(
                "-A {CHAIN_SERVICES} -d {cluster_ip}/32 -p {proto} --dport {port} \
                 -j {chain_name} -m comment --comment \"{svc_comment}\""
            ));

            // Add probability-based rules for each endpoint
            let ready_endpoints: Vec<_> = endpoints.iter().filter(|e| e.ready).collect();
            let count = ready_endpoints.len();

            for (i, ep) in ready_endpoints.iter().enumerate() {
                let remaining = count - i;
                if remaining > 1 {
                    let probability = 1.0 / remaining as f64;
                    nat_rules.push(format!(
                        "-A {chain_name} -p {proto} \
                         -m statistic --mode random --probability {probability:.5} \
                         -j DNAT --to-destination {}:{}",
                        ep.ip, ep.port
                    ));
                } else {
                    // Last endpoint gets all remaining traffic
                    nat_rules.push(format!(
                        "-A {chain_name} -p {proto} \
                         -j DNAT --to-destination {}:{}",
                        ep.ip, ep.port
                    ));
                }
            }
        }

        // NodePort rules
        if let Some(node_port) = svc.key.node_port {
            nat_rules.push(format!(
                "-A {CHAIN_NODEPORTS} -p {proto} --dport {node_port} \
                 -j DNAT --to-destination {cluster_ip}:{port} \
                 -m comment --comment \"{}/{} nodeport\"",
                svc.key.namespace, svc.key.name
            ));
        }
    }

    // Masquerade rule for pod-to-service hairpin traffic
    nat_rules.push(format!(
        "-A {CHAIN_POSTROUTING} -m mark --mark 0x4000/0x4000 -j MASQUERADE \
         -m comment --comment \"rustkube service traffic masquerade\""
    ));

    IptablesRules {
        nat_rules,
        filter_rules: vec![],
    }
}

/// Apply iptables rules via the iptables command. Linux only.
#[cfg(target_os = "linux")]
pub async fn apply_rules(rules: &IptablesRules) -> anyhow::Result<()> {
    use std::process::Command;

    // Build iptables-restore input
    let mut restore_input = String::new();
    restore_input.push_str("*nat\n");

    for rule in &rules.nat_rules {
        restore_input.push_str(rule);
        restore_input.push('\n');
    }

    restore_input.push_str("COMMIT\n");

    debug!("Applying {} iptables rules", rules.nat_rules.len());

    let output = Command::new("iptables-restore")
        .arg("--noflush")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                stdin.write_all(restore_input.as_bytes())?;
            }
            child.wait_with_output()
        });

    match output {
        Ok(out) if out.status.success() => {
            info!("Applied {} iptables rules", rules.nat_rules.len());
            Ok(())
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            error!("iptables-restore failed: {stderr}");
            anyhow::bail!("iptables-restore failed: {stderr}");
        }
        Err(e) => {
            error!("Failed to run iptables-restore: {e}");
            anyhow::bail!("Failed to run iptables-restore: {e}");
        }
    }
}

/// No-op on non-Linux platforms.
#[cfg(not(target_os = "linux"))]
pub async fn apply_rules(rules: &IptablesRules) -> anyhow::Result<()> {
    info!(
        "Skipping iptables apply on non-Linux (would apply {} rules)",
        rules.nat_rules.len()
    );
    Ok(())
}
