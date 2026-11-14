//! The `golden` suite (#344): the kubelet makes a sealed, de-identified
//! golden from a running VM when a `storm.io/v1alpha1 VirtualMachineGolden`
//! asks it to (vmcloud-image-operator#50, stormvm#17). vmimages writes those
//! requests for people; this suite writes them itself, in its own namespace,
//! the way vmimages does, and watches the kubelet carry them out.
//!
//! Two source VMIs are booted from the release's golden
//! (`RUSTKUBE_NODE_TEST_VM_GOLDEN`, default `fedora-44-x86_64`), on this
//! node, with no NIC:
//!
//! - one prints its `/etc/machine-id` on its console when cloud-init is done;
//! - one masks and stops its qemu-guest-agent, then says so on its console.
//!
//! Then, with one golden name `G` for the run (`rkn-golden-<run>`):
//!
//! - `golden-no-agent-fails`: a request for `G` from the agentless VM ends
//!   `Failed` (the copy's agent never answers, and an undeidentified golden
//!   is worse than none), and its message names nothing left behind.
//! - `golden-sealed`: a request for the same `G` from the other VM ends
//!   `Sealed`, with the volume's id and its sha256. The name being free
//!   again is itself the proof the failed attempt removed its copy: the
//!   kubelet refuses to make a golden over a volume of that name.
//! - `golden-clone-new-identity`: a VMI booted from `G` prints a machine-id,
//!   and it is not the source's: the copy was de-identified.
//!
//! The VMIs and requests are deleted after. **The sealed golden `G` is not**:
//! a test gets no stormblock admin token, and a storage user deletes only
//! what it made itself (stormcentral docs/test-standard.md); the kubelet
//! made it. `cleanup` reports it by name as a skip. So this suite runs on
//! request (`stormcentral test run rustkube-node golden`), in no gate.
//!
//! Needs `kvm` (`test/requires.toml`) and vmimages' CRD, which vmimages
//! installs at its start (a release with vmimages 60119d9 or later).

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::api::{s, short, Api};
use crate::env::Env;
use crate::k8s;
use crate::report::{Outcome, Report};

pub const API: &str = "storm.io/v1alpha1";
pub const KIND: &str = "VirtualMachineGolden";

/// The VMIs and requests, in the run's namespace.
pub const SOURCE: &str = "gold-src";
pub const NO_AGENT: &str = "gold-noagent";
pub const CLONE: &str = "gold-clone";
pub const ASK_FAIL: &str = "gold-fail";
pub const ASK_SEAL: &str = "gold-seal";

/// Printed by the guests. The shell makes the last part, so a log that
/// echoes the command line never carries the marker itself.
pub const ID_MARKER: &str = "RKN-GOLDEN-ID ";
pub const NO_AGENT_MARKER: &str = "RKN-GOLDEN-NOAGENT-2";

/// A boot and cloud-init on a pve test VM, with room.
const BOOT: Duration = Duration::from_secs(600);
/// A request: the snapshot, the copy, the sealer's boot (the kubelet gives
/// its agent 300 s, `vm_golden::BOOT_TIMEOUT`), and the seal.
const REQUEST: Duration = Duration::from_secs(900);
const MARGIN: Duration = Duration::from_secs(60);

pub fn vmis(env: &Env) -> String {
    format!("/apis/kubevirt.io/v1/namespaces/{}/virtualmachineinstances", env.namespace)
}

pub fn requests(env: &Env) -> String {
    format!("/apis/{API}/namespaces/{}/virtualmachinegoldens", env.namespace)
}

/// The golden the sources boot from.
pub fn source_golden() -> String {
    std::env::var("RUSTKUBE_NODE_TEST_VM_GOLDEN").ok().filter(|v| !v.is_empty()).unwrap_or_else(|| "fedora-44-x86_64".into())
}

/// The run's golden name: a DNS label, unique to the run.
pub fn golden_name(run_id: &str) -> String {
    let tail: String = run_id.to_ascii_lowercase().chars().filter(char::is_ascii_alphanumeric).take(16).collect();
    format!("rkn-golden-{}", if tail.is_empty() { "run".into() } else { tail })
}

/// Cloud-init that prints the machine-id once the guest is up.
pub fn says_its_id() -> String {
    format!("#cloud-config\nruncmd:\n  - [ sh, -c, \"sync; echo {}$(cat /etc/machine-id) > /dev/console\" ]\n", ID_MARKER)
}

/// Cloud-init that takes the guest agent away for good, then says so.
pub fn has_no_agent() -> String {
    let (head, _) = NO_AGENT_MARKER.rsplit_once('-').unwrap_or((NO_AGENT_MARKER, ""));
    format!(
        "#cloud-config\nruncmd:\n  - [ sh, -c, \"systemctl mask --now qemu-guest-agent; sync; echo {head}-$((1+1)) > /dev/console\" ]\n"
    )
}

/// A VMI on this node, root disk cloned from `root`, a seed of `user_data`,
/// and no NIC (the golden steps need none; the sealer has none either).
pub fn vmi(env: &Env, name: &str, root: &str, user_data: &str) -> Value {
    json!({
        "apiVersion": "kubevirt.io/v1",
        "kind": "VirtualMachineInstance",
        "metadata": { "name": name, "namespace": env.namespace, "labels": k8s::labels(env) },
        "spec": {
            "nodeSelector": { "kubernetes.io/hostname": env.node_name },
            "domain": {
                "cpu": { "cores": 2 },
                "resources": { "requests": { "memory": "2Gi" } },
                "devices": {
                    "disks": [
                        { "name": "root", "disk": { "bus": "virtio" } },
                        { "name": "seed", "disk": { "bus": "virtio" } }
                    ]
                }
            },
            "volumes": [
                { "name": "root", "dataVolume": { "name": root } },
                { "name": "seed", "cloudInitNoCloud": { "userData": user_data } }
            ]
        }
    })
}

/// The request vmimages would write: golden `golden` from VMI `vm`.
///
/// The CRD requires `spec.image` (the `CloudImage` that asked; d74a5827f5
/// was refused 422 without it). The kubelet never reads it, and vmimages
/// reads a request only from its `CloudImage`'s side, so naming one that
/// does not exist (the golden's own name) leaves the request to the kubelet.
pub fn request(env: &Env, name: &str, vm: &str, golden: &str) -> Value {
    json!({
        "apiVersion": API, "kind": KIND,
        "metadata": { "name": name, "namespace": env.namespace, "labels": k8s::labels(env) },
        "spec": { "vm": vm, "golden": golden, "image": golden },
    })
}

/// The last machine-id the console printed after [`ID_MARKER`].
pub fn machine_id(console: &str) -> Option<String> {
    console
        .rmatch_indices(ID_MARKER)
        .find_map(|(i, m)| {
            let id: String = console[i + m.len()..].chars().take_while(char::is_ascii_hexdigit).collect();
            (id.len() == 32).then_some(id)
        })
}

/// A request that ended `Failed` and removed what it made.
pub fn judge_failed(obj: &Value) -> Outcome {
    let (phase, msg) = (s(obj, "/status/phase"), s(obj, "/status/message"));
    if phase != "Failed" {
        return Outcome::Fail(format!("a guest with no agent ended {phase:?}, not Failed: {}", short(&obj["status"])));
    }
    if msg.contains("not removed") {
        return Outcome::Fail(format!("Failed, but left something behind: {msg}"));
    }
    Outcome::Pass(format!("Failed: {msg}"))
}

/// A request that ended `Sealed` with what vmimages adopts by.
pub fn judge_sealed(obj: &Value, golden: &str) -> Outcome {
    let st = &obj["status"];
    let phase = s(obj, "/status/phase");
    if phase != "Sealed" {
        return Outcome::Fail(format!("ended {phase:?}, not Sealed: {}", s(obj, "/status/message")));
    }
    let (id, sha) = (s(st, "/volumeId"), s(st, "/sha256"));
    if id.is_empty() {
        return Outcome::Fail(format!("Sealed with no volumeId: {}", short(st)));
    }
    if sha.len() != 64 || !sha.chars().all(|c| c.is_ascii_hexdigit()) {
        return Outcome::Fail(format!("Sealed with no sha256 (got {sha:?}): {}", short(st)));
    }
    let vol = s(st, "/volume");
    if !vol.is_empty() && vol != golden {
        return Outcome::Fail(format!("Sealed as volume {vol:?}, not {golden:?}"));
    }
    Outcome::Pass(format!(
        "Sealed on {}: volume {golden} ({id}), sha256 {sha}, {} {}, {}",
        s(st, "/node"),
        s(st, "/arch"),
        s(st, "/firmware"),
        s(st, "/provenance")
    ))
}

/// Whether the clone's identity is its own.
pub fn judge_identity(source: &str, clone: Option<&str>) -> Outcome {
    match clone {
        None => Outcome::Fail("the clone never printed a machine-id".into()),
        Some(c) if c == source => Outcome::Fail(format!("the clone kept the source's machine-id {c}: not de-identified")),
        Some(c) => Outcome::Pass(format!("machine-id {c}, the source's was {source}")),
    }
}

struct Suite {
    env: Arc<Env>,
    api: Api,
}

impl Suite {
    /// Boot VMI `name` and read its console until `enough`: what it said.
    async fn boot(&self, name: &str, root: &str, user_data: &str, enough: fn(&str) -> bool) -> Result<String, String> {
        self.api.create(&vmis(&self.env), &vmi(&self.env, name, root, user_data)).await?;
        let path = format!("{}/{name}", vmis(&self.env));
        let path = path.as_str();
        let within = self.env.budget(BOOT, MARGIN);
        let end = Instant::now() + within;
        k8s::until(within, || async move {
            let v = self.api.get(path).await?.ok_or_else(|| format!("VMI {name} is gone"))?;
            match s(&v, "/status/phase") {
                "Running" => Ok(()),
                "Failed" | "Succeeded" => Err(format!("VMI {name} ended {}", short(&v["status"]))),
                p => Err(format!("VMI {name} not Running after {} s: {p:?}", within.as_secs())),
            }
        })
        .await?;
        let mut last = String::new();
        while Instant::now() < end {
            match self.api.console(&self.env.namespace, name, Duration::from_secs(15), enough).await {
                Ok(seen) if enough(&seen) => return Ok(seen),
                Ok(seen) => last = seen,
                Err(e) => last = e,
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        let tail: String = last.chars().rev().take(300).collect::<Vec<_>>().into_iter().rev().collect();
        Err(format!("VMI {name}: its console did not say it was ready in {} s; last: {tail}", within.as_secs()))
    }

    /// Ask for a golden, and wait for the request to end.
    async fn ask(&self, name: &str, vm: &str, golden: &str) -> Result<Value, String> {
        self.api.create(&requests(&self.env), &request(&self.env, name, vm, golden)).await?;
        let path = format!("{}/{name}", requests(&self.env));
        let path = path.as_str();
        let within = self.env.budget(REQUEST, MARGIN);
        k8s::until(within, || async move {
            let v = self.api.get(path).await?.ok_or_else(|| format!("request {name} is gone"))?;
            match s(&v, "/status/phase") {
                "Sealed" | "Failed" => Ok(v),
                p => Err(format!("request {name} still {p:?} after {} s: {}", within.as_secs(), s(&v, "/status/message"))),
            }
        })
        .await
    }

    async fn cleanup(&self) -> Result<(), String> {
        for r in [ASK_FAIL, ASK_SEAL] {
            self.api.delete(&format!("{}/{r}", requests(&self.env))).await?;
        }
        for v in [SOURCE, NO_AGENT, CLONE] {
            self.api.delete(&format!("{}/{v}", vmis(&self.env))).await?;
        }
        for v in [SOURCE, NO_AGENT, CLONE] {
            k8s::gone(&self.api, &format!("{}/{v}", vmis(&self.env)), Duration::from_secs(120)).await?;
        }
        Ok(())
    }
}

pub async fn run(env: Arc<Env>, api: Api, r: &mut Report) {
    let suite = Suite { env: env.clone(), api: api.clone() };
    let golden = golden_name(&env.run_id);
    let root = source_golden();
    let t = Instant::now();

    // Nothing to ask without the CRD: a release older than vmimages 60119d9.
    match api.list(&requests(&env)).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            r.record("golden-crd", Outcome::Skip(format!("{API} {KIND} is not served here: vmimages 60119d9 or later installs it")), 0, None);
            return;
        }
        Err(e) => {
            r.record("golden-crd", Outcome::Infra(e), 0, None);
            return;
        }
    }

    let (says_id, no_agent) = (says_its_id(), has_no_agent());
    let (src, noagent) = tokio::join!(
        suite.boot(SOURCE, &root, &says_id, |c| machine_id(c).is_some()),
        suite.boot(NO_AGENT, &root, &no_agent, |c| c.contains(NO_AGENT_MARKER)),
    );
    let source_id = src.as_deref().ok().and_then(machine_id);
    r.record(
        "golden-sources-boot",
        match (&source_id, &noagent) {
            (Some(id), Ok(_)) => Outcome::Pass(format!("{SOURCE} (machine-id {id}) and {NO_AGENT} (agent masked) from {root}")),
            (_, Err(e)) => Outcome::Infra(e.clone()),
            (None, _) => Outcome::Infra(src.err().unwrap_or_else(|| format!("{SOURCE} printed no machine-id"))),
        },
        t.elapsed().as_millis(),
        None,
    );

    if noagent.is_ok() {
        let t = Instant::now();
        let o = match suite.ask(ASK_FAIL, NO_AGENT, &golden).await {
            Ok(v) => judge_failed(&v),
            Err(e) => Outcome::Fail(e),
        };
        r.record("golden-no-agent-fails", o, t.elapsed().as_millis(), None);
    }

    let mut sealed = false;
    if let Some(src_id) = &source_id {
        let t = Instant::now();
        let o = match suite.ask(ASK_SEAL, SOURCE, &golden).await {
            Ok(v) => judge_sealed(&v, &golden),
            Err(e) => Outcome::Fail(e),
        };
        sealed = matches!(o, Outcome::Pass(_));
        r.record("golden-sealed", o, t.elapsed().as_millis(), None);

        let t = Instant::now();
        let o = if !sealed {
            Outcome::Skip(format!("no golden {golden} was sealed to boot"))
        } else {
            match suite.boot(CLONE, &golden, &says_id, |c| machine_id(c).is_some()).await {
                Ok(c) => judge_identity(src_id, machine_id(&c).as_deref()),
                Err(e) => Outcome::Fail(e),
            }
        };
        r.record("golden-clone-new-identity", o, t.elapsed().as_millis(), None);
    }

    let t = Instant::now();
    let o = match suite.cleanup().await {
        Err(e) => Outcome::Infra(e),
        Ok(()) if sealed => Outcome::Skip(format!(
            "VMIs and requests deleted; golden {golden} stays on {}'s engine: the kubelet made it, and a test's storage user deletes only what it made",
            env.node_name
        )),
        Ok(()) => Outcome::Pass("VMIs and requests deleted".into()),
    };
    r.record("cleanup", o, t.elapsed().as_millis(), None);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_golden_name_is_a_dns_label_of_the_run() {
        assert_eq!(golden_name("Run_ab12-CD34"), "rkn-golden-runab12cd34");
        assert_eq!(golden_name(""), "rkn-golden-run");
        assert!(golden_name("0123456789abcdef0123456789").len() <= 63);
    }

    #[test]
    fn the_vmis_are_on_this_node_with_no_nic() {
        let env = k8s::tests::env();
        let v = vmi(&env, SOURCE, "fedora-44-x86_64", &says_its_id());
        assert_eq!(v["spec"]["nodeSelector"]["kubernetes.io/hostname"], json!(env.node_name));
        assert!(v["spec"]["domain"]["devices"]["interfaces"].is_null());
        assert_eq!(v["spec"]["volumes"][0]["dataVolume"]["name"], "fedora-44-x86_64");
        assert_eq!(v["metadata"]["labels"]["storm.io/test-run"], json!(env.run_id));
        let q = request(&env, ASK_SEAL, SOURCE, "rkn-golden-x");
        assert_eq!(q["spec"], json!({"vm": SOURCE, "golden": "rkn-golden-x", "image": "rkn-golden-x"}));
        assert_eq!(q["apiVersion"], API);
    }

    #[test]
    fn the_markers_are_not_in_the_commands() {
        assert!(!has_no_agent().contains(NO_AGENT_MARKER));
        assert!(has_no_agent().contains("RKN-GOLDEN-NOAGENT-$((1+1))"));
        // The command text carries the marker but no id after it.
        assert_eq!(machine_id(&says_its_id()), None);
    }

    #[test]
    fn the_machine_id_is_read_from_the_console() {
        let c = "boot\r\nRKN-GOLDEN-ID $(cat /etc/machine-id)\r\nRKN-GOLDEN-ID 0123456789abcdef0123456789abcdef\r\nlogin:";
        assert_eq!(machine_id(c).as_deref(), Some("0123456789abcdef0123456789abcdef"));
        assert_eq!(machine_id("RKN-GOLDEN-ID 0123"), None);
    }

    #[test]
    fn requests_are_judged() {
        let failed = json!({"status": {"phase": "Failed", "message": "the copy's agent did not answer in 300 s"}});
        assert!(matches!(judge_failed(&failed), Outcome::Pass(_)));
        let left = json!({"status": {"phase": "Failed", "message": "x; not removed: volume rkn-golden-a"}});
        assert!(matches!(judge_failed(&left), Outcome::Fail(_)));
        assert!(matches!(judge_failed(&json!({"status": {"phase": "Sealed"}})), Outcome::Fail(_)));

        let sha = "a".repeat(64);
        let ok = json!({"status": {"phase": "Sealed", "volumeId": "v-1", "volume": "g", "sha256": sha, "node": "n1"}});
        assert!(matches!(judge_sealed(&ok, "g"), Outcome::Pass(_)));
        let no_sha = json!({"status": {"phase": "Sealed", "volumeId": "v-1"}});
        assert!(matches!(judge_sealed(&no_sha, "g"), Outcome::Fail(_)));
        let other = json!({"status": {"phase": "Sealed", "volumeId": "v-1", "volume": "h", "sha256": sha}});
        assert!(matches!(judge_sealed(&other, "g"), Outcome::Fail(_)));

        assert!(matches!(judge_identity("a", Some("b")), Outcome::Pass(_)));
        assert!(matches!(judge_identity("a", Some("a")), Outcome::Fail(_)));
        assert!(matches!(judge_identity("a", None), Outcome::Fail(_)));
    }
}
