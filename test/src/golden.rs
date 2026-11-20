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
//! - one can't be sealed: it masks and stops its qemu-guest-agent, and masks
//!   systemd-debug-generator and `stormvm-seal.service`, so its copy has
//!   neither of the kubelet's two ways in (#399), then says so on its
//!   console.
//!
//! Then, with one golden name `G` for the run (`rkn-golden-<run>`):
//!
//! - `vmi-status-volumes` (#373): the source VMI's `status.volumes[]` names
//!   its root disk: role `root`, its golden, its engine volume, `owned`.
//! - `golden-no-agent-fails`: a request for `G` from that VM ends `Failed`
//!   (the copy's agent never answers and its seal unit never starts, and an
//!   undeidentified golden is worse than none), and its message names
//!   nothing left behind. Masking the agent alone is not enough since #399:
//!   the copy then seals itself by its unit, from PID 1 (run 7ffb659a57).
//! - `golden-sealed`: a request for the same `G` from the other VM ends
//!   `Sealed`, with the volume's id and its sha256. The name being free
//!   again is itself the proof the failed attempt removed its copy: the
//!   kubelet refuses to make a golden over a volume of that name.
//! - `golden-clone-new-identity`: a VMI booted from `G` prints a machine-id,
//!   and it is not the source's: the copy was de-identified.
//!
//! - `vmi-mirrored-root` (#404): a VMI whose root asks
//!   `storm.io/mirrored-disks: {"root": 2}` boots from a stormstorage
//!   mirrored volume filled from the golden: Running, its console prints a
//!   machine-id, and `status.volumes[]` names `mirrored.volume`
//!   (`<ns>.gold-mirror-root.mirrored`, 2 copies) and the served volume it
//!   attached. A skip where the node's kubelet has no stormstorage (its
//!   start says so); the volume is the VMI's and goes with it (the sweep).
//!
//! - `vmi-kernel-outside-refused` (#421): a VMI naming a kernel outside the
//!   kernel directories (`/data/stormcert/ca.crt`) never runs, and its status
//!   says why ("not a file under the kernel directories"): the hypervisor
//!   reads it in the node's view, so the kubelet refuses it.
//! - `vmi-direct-kernel` (#421, stormvm#111): a VMI booted straight into its
//!   golden's own kernel, from the `CloudImage`'s `status.boot` (vmimages
//!   reads it out of the golden), prints a machine-id, and its console never
//!   shows the firmware or the boot loader. A skip while vmimages publishes
//!   no kernel for the golden on this node.
//!
//! - `vmi-agent-answers` (#405): a third VMI (`gold-agent`) boots with an
//!   agent-propagated key. A privileged pod with the VMI's run directory
//!   (`/run/stormvm/<ns>/gold-agent`) as a hostPath asks its agent socket
//!   what the kubelet asks: `guest-ping`, `guest-info` (the agent's version
//!   and whether the commands the kubelet uses are enabled) and
//!   `guest-network-get-interfaces`. Passes when the agent answers the ping,
//!   and says the rest.
//! - `vmi-agent-keys` (#405, #92): that VMI's `AccessCredentialsSynchronized`
//!   is True within 180 s of its boot (its key set through the agent).
//!
//! - `vmi-pod-network` (#426, #88): a VMI on `pod: {}` (the bridge binding)
//!   prints its own IPv4 addresses on its console; a pod on the node then
//!   connects to the pod IP its status reports, port 22. Passes when it
//!   connects. Otherwise it says what the guest holds (did it get its lease?)
//!   and how the connect failed (`Host is unreachable`: nothing answers
//!   for the address), with the VMI's events.
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
pub const MIRRORED: &str = "gold-mirror";
pub const DIRECT: &str = "gold-direct";
pub const OUTSIDE: &str = "gold-outside";
pub const AGENT: &str = "gold-agent";
pub const PODNET: &str = "gold-podnet";
pub const PODNET_CLIENT: &str = "gold-podnet-client";
/// Printed with the guest's addresses; the shell makes the last part.
pub const PODNET_MARKER: &str = "RKN-PODNET-2";
pub const AGENT_PROBE: &str = "gold-agent-probe";
pub const AGENT_SECRET: &str = "gold-agent-key";
/// A well-formed ed25519 public key nobody holds the private half of.
pub const AGENT_KEY: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4f rkn-test@golden";
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
/// A mirrored root: stormstorage's copy of the golden onto two legs, then
/// the boot.
const MIRRORED_BOOT: Duration = Duration::from_secs(1200);

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
/// Keeps the copy's `stormvm-seal.service` from starting: the unit and its
/// `multi-user.target` drop-in come as SMBIOS credentials that
/// systemd-debug-generator writes out, so the generator is masked (a
/// `/dev/null` link in `/etc/systemd/system-generators`), and the unit too.
pub const NO_SEAL_UNIT: &str =
    "mkdir -p /etc/systemd/system-generators && ln -sf /dev/null /etc/systemd/system-generators/systemd-debug-generator && systemctl mask stormvm-seal.service";

pub fn has_no_agent() -> String {
    let (head, _) = NO_AGENT_MARKER.rsplit_once('-').unwrap_or((NO_AGENT_MARKER, ""));
    format!(
        "#cloud-config\nruncmd:\n  - [ sh, -c, \"systemctl mask --now qemu-guest-agent; {NO_SEAL_UNIT}; sync; echo {head}-$((1+1)) > /dev/console\" ]\n"
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
        return Outcome::Fail(format!("a guest with no agent and no seal unit ended {phase:?}, not Failed: {}", short(&obj["status"])));
    }
    if msg.contains("not removed") {
        return Outcome::Fail(format!("Failed, but left something behind: {msg}"));
    }
    // Failed for its missing agent, not for anything before it: run
    // 9f184d6390 passed here on a sealer that never started (an attach 404).
    if !msg.contains("agent") {
        return Outcome::Fail(format!("Failed, but not for the missing agent: {msg}"));
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
        self.boot_vmi(name, vmi(&self.env, name, root, user_data), BOOT, enough).await
    }

    /// [`Self::boot`] of a given VMI, within `budget`.
    async fn boot_vmi(&self, name: &str, obj: Value, budget: Duration, enough: fn(&str) -> bool) -> Result<String, String> {
        self.api.create(&vmis(&self.env), &obj).await?;
        let path = format!("{}/{name}", vmis(&self.env));
        let path = path.as_str();
        let within = self.env.budget(budget, MARGIN);
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

    /// `vmi-pod-network` (#426): `booted` is the VMI's console up to its
    /// addresses, or why it did not get there.
    async fn pod_network(&self, booted: Result<String, String>) -> Outcome {
        let path = format!("{}/{PODNET}", vmis(&self.env));
        let now = match self.api.get(&path).await {
            Ok(Some(v)) => v,
            Ok(None) => return Outcome::Fail(format!("VMI {PODNET} is gone")),
            Err(e) => return Outcome::Infra(e),
        };
        let held = booted.as_deref().ok().and_then(podnet_addresses);
        let events = k8s::events_for(&self.env, &self.api, "VirtualMachineInstance", PODNET).await.unwrap_or_default();
        let Some(ip) = now["status"]["interfaces"][0]["ipAddress"].as_str().filter(|i| !i.is_empty()).map(str::to_string) else {
            return Outcome::Fail(format!("the VMI reports no pod IP: {}", short(&now["status"])));
        };
        let target = format!("{ip}:22");
        let pods = k8s::pods(&self.env);
        if let Err(e) = self.api.create(&pods, &k8s::pod(&self.env, PODNET_CLIENT, &["tcp-connect", &target, "120"])).await {
            return Outcome::Infra(e);
        }
        let connect = k8s::pod_done(&self.env, &self.api, PODNET_CLIENT, Duration::from_secs(180)).await;
        judge_pod_network(&ip, booted.as_ref().err(), held.as_deref(), connect.map(|d| d.message), &events)
    }

    /// The VMI for #405: a key for root through the guest agent.
    async fn boot_agent_vm(&self, root: &str, says_id: &str) -> Result<String, String> {
        let secrets = format!("/api/v1/namespaces/{}/secrets", self.env.namespace);
        let secret = json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": { "name": AGENT_SECRET, "namespace": self.env.namespace, "labels": k8s::labels(&self.env) },
            "stringData": { "key": AGENT_KEY },
        });
        self.api.create(&secrets, &secret).await?;
        let mut obj = vmi(&self.env, AGENT, root, says_id);
        obj["spec"]["accessCredentials"] = agent_credentials();
        self.boot_vmi(AGENT, obj, BOOT, |c| machine_id(c).is_some()).await
    }

    /// `vmi-agent-answers` (#405).
    async fn agent_probe(&self) -> Outcome {
        let pods = k8s::pods(&self.env);
        if let Err(e) = self.api.create(&pods, &agent_probe_pod(&self.env)).await {
            return Outcome::Infra(e);
        }
        match k8s::pod_done(&self.env, &self.api, AGENT_PROBE, Duration::from_secs(120)).await {
            Ok(done) => judge_agent_probe(&done.message),
            Err(e) => Outcome::Infra(e),
        }
    }

    /// `vmi-agent-keys` (#405).
    async fn agent_keys(&self) -> Outcome {
        let path = format!("{}/{AGENT}", vmis(&self.env));
        let path = path.as_str();
        let within = Duration::from_secs(180);
        let got = k8s::until(within, || async move {
            let v = self.api.get(path).await?.ok_or_else(|| format!("VMI {AGENT} is gone"))?;
            match access_condition(&v) {
                Some((true, m)) => Ok(m),
                Some((false, m)) => Err(format!("AccessCredentialsSynchronized False after {} s: {m}", within.as_secs())),
                None => Err(format!("no AccessCredentialsSynchronized after {} s", within.as_secs())),
            }
        })
        .await;
        match got {
            Ok(m) => Outcome::Pass(format!("AccessCredentialsSynchronized True: {m}")),
            Err(e) => Outcome::Fail(e),
        }
    }

    /// `vmi-kernel-outside-refused` (#421).
    async fn kernel_outside(&self, root: &str) -> Outcome {
        let mut obj = vmi(&self.env, OUTSIDE, root, "#cloud-config\n");
        obj["metadata"]["annotations"] = json!({ KERNEL: "/data/stormcert/ca.crt" });
        if let Err(e) = self.api.create(&vmis(&self.env), &obj).await {
            return Outcome::Infra(e);
        }
        let path = format!("{}/{OUTSIDE}", vmis(&self.env));
        let path = path.as_str();
        let within = self.env.budget(Duration::from_secs(120), MARGIN);
        let seen = k8s::until(within, || async move {
            let v = self.api.get(path).await?.ok_or_else(|| format!("VMI {OUTSIDE} is gone"))?;
            judge_outside(&v).map(|_| v.clone()).map_err(|e| format!("{e} after {} s", within.as_secs()))
        })
        .await;
        match seen {
            Ok(v) => match judge_outside(&v) {
                Ok(why) => Outcome::Pass(why),
                Err(e) => Outcome::Fail(e),
            },
            Err(e) => Outcome::Fail(e),
        }
    }

    /// `vmi-direct-kernel` (#421).
    async fn direct_kernel(&self, root: &str, says_id: &str) -> Outcome {
        let images = match self.api.list(CLOUDIMAGES).await {
            Ok(Some(v)) => v,
            Ok(None) => return Outcome::Skip("vmimages' CloudImage is not served here".into()),
            Err(e) => return Outcome::Infra(e),
        };
        let boot = match boot_for(&images, root, &self.env.node_name) {
            Ok(b) => b,
            Err(why) => return Outcome::Skip(why),
        };
        let mut obj = vmi(&self.env, DIRECT, root, says_id);
        obj["metadata"]["annotations"] = boot.annotations();
        match self.boot_vmi(DIRECT, obj, BOOT, |c| machine_id(c).is_some()).await {
            Ok(console) => judge_direct(&console, &boot),
            Err(e) => {
                let now = self.api.get(&format!("{}/{DIRECT}", vmis(&self.env))).await.ok().flatten();
                Outcome::Fail(format!("{e}; status: {}", now.map(|v| short(&v["status"])).unwrap_or_default()))
            }
        }
    }

    /// `vmi-mirrored-root` (#404).
    async fn mirrored_root(&self, root: &str, says_id: &str) -> Outcome {
        let mut obj = vmi(&self.env, MIRRORED, root, says_id);
        obj["metadata"]["annotations"] = json!({ MIRROR_ANNOTATION: r#"{"root": 2}"# });
        let path = format!("{}/{MIRRORED}", vmis(&self.env));
        let booted = self.boot_vmi(MIRRORED, obj, MIRRORED_BOOT, |c| machine_id(c).is_some()).await;
        let now = match self.api.get(&path).await {
            Ok(Some(v)) => v,
            Ok(None) => return Outcome::Fail(format!("VMI {MIRRORED} is gone")),
            Err(e) => return Outcome::Infra(e),
        };
        match booted {
            Ok(console) => judge_mirrored(&now, &self.env.namespace, machine_id(&console).as_deref()),
            Err(e) if no_stormstorage(&now) => {
                Outcome::Skip(format!("{}'s kubelet has no stormstorage (--stormstorage): {e}", self.env.node_name))
            }
            Err(e) => Outcome::Fail(format!("{e}; status: {}", short(&now["status"]))),
        }
    }

    async fn cleanup(&self) -> Result<(), String> {
        for r in [ASK_FAIL, ASK_SEAL] {
            self.api.delete(&format!("{}/{r}", requests(&self.env))).await?;
        }
        for p in [AGENT_PROBE, PODNET_CLIENT] {
            self.api.delete(&format!("{}/{p}", k8s::pods(&self.env))).await?;
        }
        for v in [SOURCE, NO_AGENT, CLONE, MIRRORED, DIRECT, OUTSIDE, AGENT, PODNET] {
            self.api.delete(&format!("{}/{v}", vmis(&self.env))).await?;
        }
        for v in [SOURCE, NO_AGENT, CLONE, MIRRORED, DIRECT, OUTSIDE, AGENT, PODNET] {
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
    let (src, noagent, agent_vm, podnet) = tokio::join!(
        suite.boot(SOURCE, &root, &says_id, |c| machine_id(c).is_some()),
        suite.boot(NO_AGENT, &root, &no_agent, |c| c.contains(NO_AGENT_MARKER)),
        suite.boot_agent_vm(&root, &says_id),
        suite.boot_vmi(PODNET, podnet_vmi(&env, &root), BOOT, |c| c.contains(PODNET_MARKER)),
    );

    // A VMI on the pod network, reached from a pod (#426).
    let tp = Instant::now();
    let o = suite.pod_network(podnet).await;
    r.record("vmi-pod-network", o, tp.elapsed().as_millis(), None);

    // The guest agent as the node reaches it, and keys through it (#405).
    let ta = Instant::now();
    let o = match &agent_vm {
        Err(e) => Outcome::Infra(format!("VMI {AGENT} did not boot: {e}")),
        Ok(_) => suite.agent_probe().await,
    };
    r.record("vmi-agent-answers", o, ta.elapsed().as_millis(), None);
    let ta = Instant::now();
    let o = match &agent_vm {
        Err(_) => Outcome::Skip(format!("VMI {AGENT} did not boot")),
        Ok(_) => suite.agent_keys().await,
    };
    r.record("vmi-agent-keys", o, ta.elapsed().as_millis(), None);
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

    // The machine names its volumes (#373, stormvm#129): the source's
    // `status.volumes[]`, as the kubelet writes it from stormvm's list.
    if source_id.is_some() {
        let t = Instant::now();
        let o = match api.get(&format!("{}/{SOURCE}", vmis(&env))).await {
            Ok(Some(v)) => judge_status_volumes(&v, &root),
            Ok(None) => Outcome::Fail(format!("VMI {SOURCE} is gone")),
            Err(e) => Outcome::Infra(e),
        };
        r.record("vmi-status-volumes", o, t.elapsed().as_millis(), None);
    }

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

    // A kernel outside the kernel directories is refused (#421).
    let t = Instant::now();
    let o = suite.kernel_outside(&root).await;
    r.record("vmi-kernel-outside-refused", o, t.elapsed().as_millis(), None);

    // The golden's own kernel, booted directly (#421).
    let t = Instant::now();
    let o = suite.direct_kernel(&root, &says_id).await;
    r.record("vmi-direct-kernel", o, t.elapsed().as_millis(), None);

    // A root mirrored across nodes from the golden (#404).
    let t = Instant::now();
    let o = suite.mirrored_root(&root, &says_id).await;
    r.record("vmi-mirrored-root", o, t.elapsed().as_millis(), None);

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

/// A VMI's `status.volumes[]` (#373): its root disk is there with role
/// `root`, the golden it was cloned from, its engine volume and whether the
/// machine owns it.
pub fn judge_status_volumes(vmi: &Value, golden: &str) -> Outcome {
    let Some(vols) = vmi["status"]["volumes"].as_array() else {
        return Outcome::Fail(format!("no status.volumes; status keys: {:?}",
            vmi["status"].as_object().map(|o| o.keys().cloned().collect::<Vec<_>>()).unwrap_or_default()));
    };
    let Some(root) = vols.iter().find(|v| v["role"] == "root") else {
        return Outcome::Fail(format!("status.volumes has no role root: {}", Value::Array(vols.clone())));
    };
    let id = root["volumeID"].as_str().unwrap_or("");
    if id.is_empty() || root["golden"].as_str() != Some(golden) || !root["owned"].is_boolean() {
        return Outcome::Fail(format!("the root entry is not complete (volumeID, golden {golden}, owned): {root}"));
    }
    Outcome::Pass(format!("{} volume(s); root: {root}", vols.len()))
}

/// A VMI on this node on the pod network (the bridge binding, #426), whose
/// console says its IPv4 addresses once cloud-init has run.
pub fn podnet_vmi(env: &Env, root: &str) -> Value {
    let user_data = format!(
        "#cloud-config\nruncmd:\n  - [ sh, -c, \"sync; echo {}-$((1+1)) $(ip -4 -o addr show scope global | awk '{{print $4}}' | tr '\\\\n' ' ') > /dev/console\" ]\n",
        PODNET_MARKER.rsplit_once('-').map_or(PODNET_MARKER, |(h, _)| h)
    );
    let mut v = vmi(env, PODNET, root, &user_data);
    v["spec"]["domain"]["devices"]["interfaces"] = json!([{ "name": "default", "bridge": {} }]);
    v["spec"]["networks"] = json!([{ "name": "default", "pod": {} }]);
    v
}

/// What the guest said it holds, after [`PODNET_MARKER`]: `Some("")` for none.
pub fn podnet_addresses(console: &str) -> Option<String> {
    let i = console.rfind(PODNET_MARKER)?;
    Some(console[i + PODNET_MARKER.len()..].lines().next().unwrap_or("").trim().to_string())
}

/// `vmi-pod-network`.
pub fn judge_pod_network(
    ip: &str,
    boot_failed: Option<&String>,
    held: Option<&str>,
    connect: Result<String, String>,
    events: &[String],
) -> Outcome {
    let guest = match (boot_failed, held) {
        (_, Some("")) => "the guest says it holds no IPv4 address (no lease)".to_string(),
        (_, Some(h)) if h.split_whitespace().any(|a| a.split('/').next() == Some(ip)) => format!("the guest holds {h}"),
        (_, Some(h)) => format!("the guest holds {h}, not {ip}"),
        (Some(e), None) => format!("the guest never printed its addresses: {e}"),
        (None, None) => "the guest's addresses are unknown".to_string(),
    };
    let line: Value = connect.as_deref().ok().and_then(|m| serde_json::from_str(m).ok()).unwrap_or(Value::Null);
    let said = line["detail"].as_str().map(str::to_string).unwrap_or_else(|| match &connect {
        Ok(m) => m.clone(),
        Err(e) => e.clone(),
    });
    if line["ok"] == json!(true) {
        return Outcome::Pass(format!("a pod reached the VMI's pod IP: {said}; {guest}"));
    }
    let ev = if events.is_empty() { String::new() } else { format!("; VMI events: {}", events.join(" | ")) };
    Outcome::Fail(format!("no pod reaches the VMI's pod IP {ip}: {said}; {guest}{ev}"))
}

/// `accessCredentials` for root's key through the guest agent.
pub fn agent_credentials() -> Value {
    json!([{ "sshPublicKey": {
        "source": { "secret": { "secretName": AGENT_SECRET } },
        "propagationMethod": { "qemuGuestAgent": { "users": ["root"] } },
    } }])
}

/// The probe of [`AGENT`]'s agent socket: privileged (a node path), the
/// VMI's run directory as a hostPath.
pub fn agent_probe_pod(env: &Env) -> Value {
    let dir = format!("/run/stormvm/{}/{AGENT}", env.namespace);
    let mut p = k8s::pod(env, AGENT_PROBE, &["qga-probe", "/agent/agent.sock"]);
    let c = &mut p["spec"]["containers"][0];
    c["securityContext"] = json!({ "privileged": true, "runAsUser": 0 });
    c["volumeMounts"] = json!([{ "name": "run", "mountPath": "/agent" }]);
    p["spec"]["volumes"] = json!([{ "name": "run", "hostPath": { "path": dir, "type": "Directory" } }]);
    p
}

/// `vmi-agent-answers`, from the probe's termination message (its workload
/// line).
pub fn judge_agent_probe(message: &str) -> Outcome {
    let line: Value = match serde_json::from_str(message) {
        Ok(v) => v,
        Err(_) => return Outcome::Fail(format!("the probe said: {message}")),
    };
    let detail = line["detail"].as_str().unwrap_or_default();
    if line["ok"] != json!(true) {
        return Outcome::Fail(format!("the probe could not reach the agent socket: {detail}"));
    }
    let said: Value = serde_json::from_str(detail).unwrap_or(Value::Null);
    if said["ping"].get("ok").is_none() {
        return Outcome::Fail(format!("the agent did not answer guest-ping on the node's socket: {detail}"));
    }
    Outcome::Pass(format!("the agent answers on the node's socket: {detail}"))
}

/// A VMI's `AccessCredentialsSynchronized`: (True?, message).
pub fn access_condition(vmi: &Value) -> Option<(bool, String)> {
    let c = vmi["status"]["conditions"].as_array()?.iter().find(|c| c["type"] == "AccessCredentialsSynchronized")?;
    Some((c["status"] == "True", format!("{} {}", s(c, "/reason"), s(c, "/message")).trim().to_string()))
}

/// stormvm's direct-kernel annotations (`stormvm_spec::kube`).
pub const KERNEL: &str = "storm.io/kernel";
pub const INITRD: &str = "storm.io/initrd";
pub const CMDLINE: &str = "storm.io/cmdline";
/// vmimages' CloudImages: cluster-scoped.
pub const CLOUDIMAGES: &str = "/apis/storm.io/v1alpha1/cloudimages";

/// A golden's kernel, initramfs and command line, from `status.boot`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Boot {
    pub kernel: String,
    pub initrd: Option<String>,
    pub cmdline: Option<String>,
}

impl Boot {
    pub fn annotations(&self) -> Value {
        let mut a = json!({ KERNEL: self.kernel });
        if let Some(i) = &self.initrd {
            a[INITRD] = json!(i);
        }
        if let Some(c) = &self.cmdline {
            a[CMDLINE] = json!(c);
        }
        a
    }
}

/// The boot of the CloudImage whose golden is `golden`, when it is on
/// `node`; else why there is none (a skip).
pub fn boot_for(images: &[Value], golden: &str, node: &str) -> Result<Boot, String> {
    let Some(img) = images.iter().find(|i| i["status"]["golden"] == golden) else {
        return Err(format!("no CloudImage has golden {golden}"));
    };
    let b = &img["status"]["boot"];
    let Some(kernel) = b["kernel"].as_str().filter(|k| !k.is_empty()) else {
        let why = b["message"].as_str().unwrap_or("no status.boot.kernel yet");
        return Err(format!("CloudImage {}: {why}", s(img, "/metadata/name")));
    };
    if let Some(nodes) = b["nodes"].as_array() {
        if !nodes.iter().any(|n| n == node) {
            return Err(format!("the golden's kernel is on {nodes:?}, not on {node}"));
        }
    }
    Ok(Boot {
        kernel: kernel.to_string(),
        initrd: b["initrd"].as_str().map(str::to_string),
        cmdline: b["cmdline"].as_str().map(str::to_string),
    })
}

/// What a firmware boot prints on the serial console and a direct one does
/// not: OVMF's and grub's own lines.
pub const FIRMWARE_MARKERS: [&str; 4] = ["BdsDxe", "UEFI Interactive Shell", "GNU GRUB", "Booting `"];

/// `vmi-direct-kernel`: a machine-id, and no firmware or boot loader on the
/// console before it.
pub fn judge_direct(console: &str, boot: &Boot) -> Outcome {
    let Some(id) = machine_id(console) else { return Outcome::Fail("Running, but its console printed no machine-id".into()) };
    if let Some(m) = FIRMWARE_MARKERS.iter().find(|m| console.contains(*m)) {
        return Outcome::Fail(format!("the console shows {m:?}: it went through the firmware, not straight into {}", boot.kernel));
    }
    Outcome::Pass(format!("booted straight into {} (initrd {}), machine-id {id}, no firmware or boot loader on the console",
        boot.kernel, boot.initrd.as_deref().unwrap_or("-")))
}

/// `vmi-kernel-outside-refused`: never Running, and the status says why.
pub fn judge_outside(vmi: &Value) -> Result<String, String> {
    let phase = s(vmi, "/status/phase");
    if phase == "Running" || phase == "Succeeded" {
        return Err(format!("a kernel outside the kernel directories ran: {phase}"));
    }
    let text = vmi["status"].to_string();
    if text.contains("not a file under the kernel directories") {
        Ok(format!("{phase}: refused, its status says the kernel is not under the kernel directories"))
    } else {
        Err(format!("not refused yet ({phase}): {}", short(&vmi["status"])))
    }
}

/// The VMI annotation that asks for a mirrored disk (the kubelet's
/// `vm_mirrored_root::REQUEST`).
pub const MIRROR_ANNOTATION: &str = "storm.io/mirrored-disks";

/// Did the kubelet refuse the mirrored disk for want of stormstorage?
pub fn no_stormstorage(vmi: &Value) -> bool {
    vmi["status"].to_string().contains("no stormstorage")
}

/// `vmi-mirrored-root`: it booted (a machine-id), and its root's
/// `status.volumes[]` entry names the mirrored volume, 2 copies, and the
/// served volume it attached.
pub fn judge_mirrored(vmi: &Value, namespace: &str, id: Option<&str>) -> Outcome {
    let Some(id) = id else { return Outcome::Fail("Running, but its console printed no machine-id".into()) };
    let want = format!("{namespace}.{MIRRORED}-root.mirrored");
    let root = vmi["status"]["volumes"].as_array().and_then(|v| v.iter().find(|e| e["name"] == "root"));
    let Some(root) = root else {
        return Outcome::Fail(format!("no root in status.volumes: {}", short(&vmi["status"]["volumes"])));
    };
    if root["mirrored"]["volume"] != json!(want) || root["mirrored"]["copies"] != json!(2) {
        return Outcome::Fail(format!("root is not on {want} with 2 copies: {}", short(root)));
    }
    let Some(served) = root["volumeID"].as_str().filter(|v| !v.is_empty()) else {
        return Outcome::Fail(format!("root names no served volume: {}", short(root)));
    };
    Outcome::Pass(format!("booted (machine-id {id}) from {want}, 2 copies, served volume {served}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_volumes_are_judged() {
        let ok = serde_json::json!({"status": {"volumes": [
            {"golden": "fedora-44-x86_64", "name": "root", "owned": true, "role": "root", "source": "golden:fedora-44-x86_64", "volumeID": "u-1"},
            {"name": "seed", "owned": true, "role": "seed", "source": "seed"},
        ]}});
        assert!(matches!(judge_status_volumes(&ok, "fedora-44-x86_64"), Outcome::Pass(_)));
        assert!(matches!(judge_status_volumes(&ok, "other"), Outcome::Fail(_)));
        assert!(matches!(judge_status_volumes(&serde_json::json!({"status": {"phase": "Running"}}), "x"), Outcome::Fail(_)));
    }

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
        // Both ways in are shut: the agent, and the seal unit's generator (#399).
        assert!(has_no_agent().contains("systemctl mask --now qemu-guest-agent"));
        assert!(has_no_agent().contains("/etc/systemd/system-generators/systemd-debug-generator"));
        assert!(has_no_agent().contains("systemctl mask stormvm-seal.service"));
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
        let early = json!({"status": {"phase": "Failed", "message": "starting the sealer: attaching vol-3b05 for disk root: 404 Not Found"}});
        assert!(matches!(judge_failed(&early), Outcome::Fail(_)));
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

    #[test]
    fn the_goldens_boot_is_found_and_judged() {
        let imgs = vec![serde_json::json!({"metadata": {"name": "fedora-44"}, "status": {"golden": "g", "boot": {
            "kernel": "/data/vmimages/boot/g/vmlinuz", "initrd": "/data/vmimages/boot/g/initrd", "cmdline": "ro", "nodes": ["n1"]}}})];
        let b = boot_for(&imgs, "g", "n1").unwrap();
        assert_eq!(b.annotations(), serde_json::json!({KERNEL: "/data/vmimages/boot/g/vmlinuz", INITRD: "/data/vmimages/boot/g/initrd", CMDLINE: "ro"}));
        assert!(boot_for(&imgs, "g", "n2").unwrap_err().contains("not on n2"));
        assert!(boot_for(&imgs, "other", "n1").is_err());
        let none = vec![serde_json::json!({"metadata": {"name": "f"}, "status": {"golden": "g", "boot": {"message": "reading the kernel"}}})];
        assert!(boot_for(&none, "g", "n1").unwrap_err().contains("reading the kernel"));
        let id = format!("{ID_MARKER}{}", "a".repeat(32));
        assert!(matches!(judge_direct(&format!("[ 0.0] Linux\n{id}\n"), &b), Outcome::Pass(_)));
        assert!(matches!(judge_direct(&format!("BdsDxe: loading\n{id}\n"), &b), Outcome::Fail(_)));
        assert!(matches!(judge_direct("nothing", &b), Outcome::Fail(_)));
    }

    #[test]
    fn the_pod_network_is_judged() {
        let ok = |d: &str| Ok(serde_json::json!({"workload": "tcp-connect", "ok": true, "detail": d}).to_string());
        let no = |d: &str| Ok(serde_json::json!({"workload": "tcp-connect", "ok": false, "detail": d}).to_string());
        assert!(matches!(judge_pod_network("10.1.2.3", None, Some("10.1.2.3/16"), ok("connected"), &[]), Outcome::Pass(_)));
        match judge_pod_network("10.1.2.3", None, Some(""), no("Host is unreachable (os error 113)"), &["PodAddressNotTaken x".into()]) {
            Outcome::Fail(m) => assert!(m.contains("no lease") && m.contains("unreachable") && m.contains("PodAddressNotTaken"), "{m}"),
            o => panic!("{o:?}"),
        }
        match judge_pod_network("10.1.2.3", None, Some("10.1.2.3/16"), no("timed out"), &[]) {
            Outcome::Fail(m) => assert!(m.contains("the guest holds 10.1.2.3/16"), "{m}"),
            o => panic!("{o:?}"),
        }
        assert_eq!(podnet_addresses("x RKN-PODNET-2 10.1.2.3/16 \nmore"), Some("10.1.2.3/16".into()));
        assert_eq!(podnet_addresses("RKN-PODNET-2 \n"), Some(String::new()));
        assert_eq!(podnet_addresses("nothing"), None);
        let v = podnet_vmi(&crate::k8s::tests::env(), "g");
        assert_eq!(v["spec"]["networks"][0]["pod"], serde_json::json!({}));
        let ud = s(&v, "/spec/volumes/1/cloudInitNoCloud/userData");
        assert!(ud.contains("RKN-PODNET-$((1+1))") && !ud.contains(PODNET_MARKER), "{ud}");
    }

    #[test]
    fn the_agent_probe_and_keys_are_judged() {
        let line = |ok: bool, detail: &str| serde_json::json!({"workload": "qga-probe /agent/agent.sock", "ok": ok, "detail": detail}).to_string();
        let up = r#"{"ping":{"ok":{}},"version":"10.2.2","enabled":{"guest-ping":true},"interfaces":{"ok":2}}"#;
        assert!(matches!(judge_agent_probe(&line(true, up)), Outcome::Pass(_)));
        let down = r#"{"ping":{"error":"guest-sync-delimited: no answer: Resource temporarily unavailable"}}"#;
        assert!(matches!(judge_agent_probe(&line(true, down)), Outcome::Fail(_)));
        assert!(matches!(judge_agent_probe(&line(false, "connect: No such file")), Outcome::Fail(_)));
        assert!(matches!(judge_agent_probe("not json"), Outcome::Fail(_)));
        let v = |st: &str| serde_json::json!({"status": {"conditions": [{"type": "AccessCredentialsSynchronized", "status": st, "reason": "R", "message": "m"}]}});
        assert_eq!(access_condition(&v("True")), Some((true, "R m".into())));
        assert_eq!(access_condition(&v("False")), Some((false, "R m".into())));
        assert_eq!(access_condition(&serde_json::json!({"status": {}})), None);
        assert_eq!(agent_credentials()[0]["sshPublicKey"]["propagationMethod"]["qemuGuestAgent"]["users"][0], "root");
        let p = agent_probe_pod(&crate::k8s::tests::env());
        assert_eq!(p["spec"]["containers"][0]["securityContext"]["privileged"], true);
    }

    #[test]
    fn a_kernel_outside_is_judged() {
        let v = |phase: &str, msg: &str| serde_json::json!({"status": {"phase": phase, "conditions": [{"message": msg}]}});
        assert!(judge_outside(&v("Pending", "storm.io/kernel \"/x\" is not a file under the kernel directories (…)")).is_ok());
        assert!(judge_outside(&v("Running", "")).is_err());
        assert!(judge_outside(&v("Pending", "scheduling")).is_err());
    }

    #[test]
    fn a_mirrored_root_is_judged() {
        let vmi = |mirrored: Value, id: &str| {
            serde_json::json!({"status": {"volumes": [{"name": "root", "volumeID": id, "mirrored": mirrored}]}})
        };
        let want = serde_json::json!({"volume": "ns.gold-mirror-root.mirrored", "copies": 2});
        assert!(matches!(judge_mirrored(&vmi(want.clone(), "u-1"), "ns", Some("abc")), Outcome::Pass(_)));
        assert!(matches!(judge_mirrored(&vmi(want.clone(), "u-1"), "ns", None), Outcome::Fail(_)));
        assert!(matches!(judge_mirrored(&vmi(want.clone(), ""), "ns", Some("abc")), Outcome::Fail(_)));
        assert!(matches!(judge_mirrored(&vmi(Value::Null, "u-1"), "ns", Some("abc")), Outcome::Fail(_)));
        assert!(matches!(judge_mirrored(&vmi(want, "u-1"), "other", Some("abc")), Outcome::Fail(_)));
        assert!(no_stormstorage(&serde_json::json!({"status": {"conditions": [{"message": "x: this node has no stormstorage (--stormstorage)"}]}})));
        assert!(!no_stormstorage(&serde_json::json!({"status": {"phase": "Pending"}})));
    }
}
