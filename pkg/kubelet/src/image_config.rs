//! The image's own config, applied under the pod spec (#98).
//!
//! A CRI runtime runs what the image says unless the pod says otherwise:
//!
//! - `command` replaces the image's `Entrypoint`, `args` replaces its `Cmd`;
//!   `args` alone runs after the `Entrypoint` (CoreDNS gives only `-conf …`);
//!   neither runs `Entrypoint` + `Cmd`. A `command` alone drops `Cmd`.
//! - the image's `Env` is under the pod's `env`: a name the pod sets wins.
//! - `WorkingDir` and `User` apply when the pod leaves them unset
//!   (`workingDir`, `runAsUser`/`runAsGroup`).
//!
//! The stormpump runtime used to build argv from `command` + `args` alone,
//! so an args-only container exec'd its first argument, one with no `env`
//! had no image `PATH`, and every container ran as root.
//!
//! **Where the config comes from:** sbregistry records a pushed image's
//! config on its golden (`GET /v1/goldens/{name}` → `config`). The node's
//! boot goldens (pallets) have none yet, nor any image's `Volumes`
//! (stormblock-registry#58). A container whose image has no known config and
//! whose argv would be empty or begin with a flag is refused, naming the image,
//! rather than exec'ing the flag.

use std::collections::HashMap;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde::Deserialize;

/// The part of an OCI image config a runtime applies. The registry serves it
/// in the OCI spelling (capitalized keys).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ImageConfig {
    #[serde(rename = "Entrypoint", default)]
    pub entrypoint: Vec<String>,
    #[serde(rename = "Cmd", default)]
    pub cmd: Vec<String>,
    #[serde(rename = "Env", default)]
    pub env: Vec<String>,
    #[serde(rename = "WorkingDir", default)]
    pub working_dir: String,
    #[serde(rename = "User", default)]
    pub user: String,
    /// The image's declared volumes (#172): OCI spells it a map of path to
    /// `{}`. Each becomes a directory in the container's own root when no
    /// pod mount covers it ([`declared_volumes`]).
    #[serde(rename = "Volumes", default)]
    pub volumes: std::collections::BTreeMap<String, EmptyObject>,
}

/// The `{}` OCI uses as a set's value; anything inside it is ignored.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct EmptyObject {}

/// The image's declared volumes that need making (#172): absolute, normal
/// paths (no `..`), not covered by one of the pod's mounts (`mounts`, the
/// container paths): a mount at the path or above it already puts something
/// there.
pub fn declared_volumes(config: &ImageConfig, mounts: &[&str]) -> Vec<String> {
    config
        .volumes
        .keys()
        .map(|p| p.trim_end_matches('/').to_string())
        .filter(|p| p.starts_with('/') && p.len() > 1)
        .filter(|p| !p.split('/').any(|c| c == ".." || c == "."))
        .filter(|p| {
            !mounts.iter().any(|m| {
                let m = m.trim_end_matches('/');
                m.is_empty() || p == m || p.starts_with(&format!("{m}/"))
            })
        })
        .collect()
}

/// Make `path` (absolute, as the container sees it) a directory inside the
/// container root `root` (#172), as CRI-O's default `image_volumes = "mkdir"`
/// does. Each component is made if missing; **a symlink is never followed**:
/// the path comes from the image, and a link could point the walk out of the
/// root and onto the node. `Ok(false)`: stopped at a symlink or a
/// non-directory, nothing made past it.
pub fn make_in_root(root: &Path, path: &str) -> std::io::Result<bool> {
    let mut at = root.to_path_buf();
    for part in path.split('/').filter(|c| !c.is_empty()) {
        at.push(part);
        match std::fs::symlink_metadata(&at) {
            Ok(m) if m.file_type().is_symlink() || !m.is_dir() => return Ok(false),
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                use std::os::unix::fs::DirBuilderExt;
                match std::fs::DirBuilder::new().mode(0o755).create(&at) {
                    Ok(()) => {}
                    // Made meanwhile: look again on the next component.
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        if std::fs::symlink_metadata(&at)?.file_type().is_symlink() {
                            return Ok(false);
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

/// How long "no config for this image" is believed before the registry is
/// asked again. A known config is kept: an image root is one golden's clone.
pub const MISS_TTL: Duration = Duration::from_secs(300);

/// Image configs by the image's root (what `pull_image` returned): a pallet's
/// mount or a pulled clone's. Shared by the image service, which fills it,
/// and the runtime, which reads it at `create_container`.
#[derive(Debug, Default)]
pub struct ImageConfigs {
    /// root → (config, when asked, the image reference it was asked for).
    by_root: std::sync::Mutex<HashMap<String, (Option<ImageConfig>, Instant, String)>>,
    /// root → where the image came from (#130), from the registry's record.
    provenance: std::sync::Mutex<HashMap<String, Provenance>>,
}

impl ImageConfigs {
    /// The config of the image rooted at `root`, when one is known.
    pub fn get(&self, root: &str) -> Option<ImageConfig> {
        self.by_root.lock().unwrap_or_else(|e| e.into_inner()).get(root)?.0.clone()
    }

    /// The image reference a root was resolved from, for messages: the pod
    /// wrote `registry.k8s.io/coredns/coredns:v1.11.1`, not its mount.
    pub fn image_of(&self, root: &str) -> Option<String> {
        Some(self.by_root.lock().unwrap_or_else(|e| e.into_inner()).get(root)?.2.clone())
    }

    /// Whether `root` needs asking: never asked, or a miss older than [`MISS_TTL`].
    pub fn needs_lookup(&self, root: &str) -> bool {
        match self.by_root.lock().unwrap_or_else(|e| e.into_inner()).get(root) {
            None => true,
            Some((Some(_), _, _)) => false,
            Some((None, at, _)) => at.elapsed() >= MISS_TTL,
        }
    }

    pub fn put(&self, root: &str, image: &str, config: Option<ImageConfig>) {
        self.by_root
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(root.to_string(), (config, Instant::now(), image.to_string()));
    }

    /// Where the image rooted at `root` came from, when the registry said (#130).
    pub fn provenance(&self, root: &str) -> Option<Provenance> {
        self.provenance.lock().unwrap_or_else(|e| e.into_inner()).get(root).cloned()
    }

    pub fn put_provenance(&self, root: &str, p: Provenance) {
        self.provenance.lock().unwrap_or_else(|e| e.into_inner()).insert(root.to_string(), p);
    }
}

/// Where an image came from (#130): what a container status's `imageID` and
/// the pod's per-container annotations say.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Provenance {
    /// `sha256:<hex>`: the image's manifest digest (a pulled image), or the
    /// sealed golden's device digest from the release manifest (a pallet).
    pub digest: Option<String>,
    /// The golden it is cloned from, by name.
    pub golden: Option<String>,
    /// Build information: OCI's `created` and `org.opencontainers.image.*`
    /// labels (as `created`, `version`, `revision`, `source`, `title`,
    /// `vendor`), or a release's `provenance` and `version`. Empty: none known.
    pub build: serde_json::Map<String, serde_json::Value>,
}

/// A `sha256:` digest from what a record says: already prefixed, or 64 hex.
fn sha256(d: &str) -> Option<String> {
    let hex = d.strip_prefix("sha256:").unwrap_or(d);
    (hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit())).then(|| format!("sha256:{hex}"))
}

/// A registry golden record's provenance (#130): its manifest `digest`, its
/// template, and the OCI build info its `config` carries when the registry
/// keeps it (`created`, `Labels`; stormblock-registry#100).
pub fn provenance_of_record(record: &serde_json::Value) -> Provenance {
    let mut build = serde_json::Map::new();
    let config = &record["config"];
    if let Some(c) = config["created"].as_str().or(config["Created"].as_str()) {
        build.insert("created".into(), c.into());
    }
    let labels = config["Labels"].as_object().or(config["labels"].as_object());
    for (key, field) in [
        ("org.opencontainers.image.created", "created"),
        ("org.opencontainers.image.version", "version"),
        ("org.opencontainers.image.revision", "revision"),
        ("org.opencontainers.image.source", "source"),
        ("org.opencontainers.image.title", "title"),
        ("org.opencontainers.image.vendor", "vendor"),
    ] {
        if let Some(v) = labels.and_then(|l| l.get(key)).and_then(|v| v.as_str()) {
            build.insert(field.into(), v.into());
        }
    }
    Provenance {
        digest: record["digest"].as_str().and_then(sha256),
        golden: record["template_name"]
            .as_str()
            .or(record["name"].as_str())
            .filter(|n| !n.is_empty())
            .map(str::to_string),
        build,
    }
}

/// The release manifest a stormcos node carries (`assets[]`), as the kubelet
/// sees it.
pub const RELEASE_MANIFESTS: [&str; 2] =
    ["/etc/stormcos/release/manifest.json", "/hostroot/etc/stormcos/release/manifest.json"];

/// The provenance of the golden `name` in a release manifest's text (#130):
/// its device digest, its name, and its `provenance` / `version`.
pub fn release_golden_in(manifest: &str, name: &str) -> Option<Provenance> {
    let doc: serde_json::Value = serde_json::from_str(manifest).ok()?;
    let entry = doc["assets"]
        .as_array()?
        .iter()
        .find(|a| a["kind"] == "golden" && a["name"].as_str() == Some(name))?;
    let mut build = serde_json::Map::new();
    for field in ["provenance", "version"] {
        if let Some(v) = entry[field].as_str().filter(|v| !v.is_empty()) {
            build.insert(field.into(), v.into());
        }
    }
    Some(Provenance {
        digest: entry["digest"].as_str().and_then(sha256),
        golden: Some(name.to_string()),
        build,
    })
}

/// [`release_golden_in`] against the node's own manifest.
pub fn release_golden(name: &str) -> Option<Provenance> {
    RELEASE_MANIFESTS
        .iter()
        .find_map(|p| std::fs::read_to_string(p).ok())
        .and_then(|t| release_golden_in(&t, name))
}

/// A golden version a pod asked for by its image tag (#86).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoldenVersion {
    /// The component (`nextnfs`).
    pub component: String,
    /// The golden's volume name (`golden-nextnfs-<sha12>`).
    pub golden: String,
    /// How the registry knows it: `registry/<component>:<sha12>`, stormcentral's
    /// name for a component golden (`goldens::image_ref`).
    pub reference: String,
}

/// Is a 12-character lowercase hex string, as a golden's build id is.
fn is_sha12(s: &str) -> bool {
    s.len() == 12 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The golden version an image asks for, when it asks for one other than the
/// release's (#86, owner's choice A on the issue).
///
/// `image: nextnfs:<sha12>` (or `nextnfs:golden-nextnfs-<sha12>`) names the
/// golden `golden-nextnfs-<sha12>`. It is a version only for a **known
/// component**: one the release manifest lists as a golden, or one the node
/// carries as a pallet (`has_pallet`). The release's own version, an untagged
/// image, any other tag, and any digest run the pallet as before: upstream
/// manifests carry OCI tags and digests (Cilium is pinned `@sha256:`), and
/// those never named a golden. A digest selects nothing here: a node cannot
/// map a device digest to a version it does not hold.
pub fn golden_version(image: &str, manifest: &str, has_pallet: impl Fn(&str) -> bool) -> Option<GoldenVersion> {
    let last = image.rsplit('/').next().unwrap_or(image);
    let without_digest = last.split('@').next().unwrap_or(last);
    let (name, tag) = without_digest.split_once(':')?;
    if name.is_empty() {
        return None;
    }
    let prefix = format!("golden-{name}-");
    let sha12 = tag.strip_prefix(&prefix).unwrap_or(tag);
    if !is_sha12(sha12) {
        return None;
    }
    let release = release_golden_in(manifest, name);
    if release.is_none() && !has_pallet(name) {
        return None;
    }
    // The release's own version is the pallet.
    let release_sha12 = serde_json::from_str::<serde_json::Value>(manifest)
        .ok()
        .and_then(|doc| {
            doc["assets"].as_array()?.iter().find(|a| a["kind"] == "golden" && a["name"].as_str() == Some(name)).and_then(
                |a| a["provenance"].as_str().and_then(|p| p.strip_prefix(&prefix)).map(str::to_string),
            )
        });
    if release_sha12.as_deref() == Some(sha12) {
        return None;
    }
    Some(GoldenVersion {
        component: name.to_string(),
        golden: format!("{prefix}{sha12}"),
        reference: format!("registry/{name}:{sha12}"),
    })
}

/// A golden record's `config`, if it carries a non-empty one.
pub fn from_golden(record: &serde_json::Value) -> Option<ImageConfig> {
    let c: ImageConfig = serde_json::from_value(record.get("config")?.clone()).ok()?;
    (c != ImageConfig::default()).then_some(c)
}

/// What the pod spec says about a container, for [`compose`].
#[derive(Debug, Clone, Default)]
pub struct PodSide<'a> {
    pub image: &'a str,
    pub command: &'a [String],
    pub args: &'a [String],
    /// `NAME=value`, the pod's resolved env.
    pub env: &'a [String],
    pub working_dir: &'a str,
    pub run_as_user: Option<i64>,
    pub run_as_group: Option<i64>,
    pub run_as_non_root: bool,
}

/// What the container runs, as the engine takes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Composed {
    pub argv: Vec<String>,
    /// The image's env under the pod's, `NAME=value`; `HOME` from the user.
    pub env: Vec<String>,
    pub cwd: String,
    pub uid: u32,
    pub gid: u32,
}

const DEFAULT_PATH: &str = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// The pod spec over the image config, by CRI's rules (module doc). `root` is
/// the image's root, for user and group names (`/etc/passwd`, `/etc/group`).
pub fn compose(pod: &PodSide, image: Option<&ImageConfig>, root: Option<&Path>) -> Result<Composed, String> {
    let empty = ImageConfig::default();
    let img = image.unwrap_or(&empty);

    let mut argv: Vec<String> = if !pod.command.is_empty() {
        pod.command.to_vec()
    } else {
        img.entrypoint.clone()
    };
    if !pod.args.is_empty() {
        argv.extend(pod.args.iter().cloned());
    } else if pod.command.is_empty() {
        argv.extend(img.cmd.iter().cloned());
    }
    if image.is_none() {
        match argv.first() {
            None => {
                return Err(format!(
                    "image {} has no known config (no Entrypoint or Cmd) and the container gives \
                     no command: set `command` (stormblock-registry#58)",
                    pod.image
                ))
            }
            Some(a) if a.starts_with('-') => {
                return Err(format!(
                    "image {} has no known config, so its Entrypoint is unknown, and the \
                     container's args begin with the flag {a}: set `command` \
                     (stormblock-registry#58)",
                    pod.image
                ))
            }
            _ => {}
        }
    } else if argv.is_empty() {
        return Err(format!("image {} has no Entrypoint or Cmd and the container gives no command", pod.image));
    }

    // Who it runs as: the pod's numbers, else the image's User.
    let (mut uid, mut gid, mut home) = (0u32, 0u32, None);
    if !img.user.is_empty() {
        let (u, g, h) = resolve_user(&img.user, root).map_err(|e| format!("image {}: {e}", pod.image))?;
        (uid, gid, home) = (u, g, h);
    }
    if let Some(u) = pod.run_as_user {
        uid = u32::try_from(u).map_err(|_| format!("runAsUser {u} is not a uid"))?;
        // A uid the pod chose: its home comes from the image's database, and its
        // group from it too unless the pod also says (upstream's behaviour).
        let entry = root.and_then(|r| passwd_by_uid(r, uid));
        home = entry.as_ref().map(|e| e.2.clone());
        if pod.run_as_group.is_none() {
            gid = entry.map(|e| e.1).unwrap_or(0);
        }
    }
    if let Some(g) = pod.run_as_group {
        gid = u32::try_from(g).map_err(|_| format!("runAsGroup {g} is not a gid"))?;
    }
    if pod.run_as_non_root && uid == 0 {
        return Err(format!(
            "runAsNonRoot is set and the container would run as root (image {}{})",
            pod.image,
            if img.user.is_empty() { ", which sets no User" } else { "" }
        ));
    }

    // The image's env, then the pod's over it by name.
    let mut env: Vec<String> = Vec::new();
    let mut set = |e: &str| {
        let name = e.split('=').next().unwrap_or(e);
        let prefix = format!("{name}=");
        env.retain(|x| !x.starts_with(&prefix));
        env.push(e.to_string());
    };
    for e in img.env.iter().chain(pod.env.iter()) {
        set(e);
    }
    let has = |env: &[String], key: &str| env.iter().any(|e| e.starts_with(&format!("{key}=")));
    if !has(&env, "PATH") {
        env.push(DEFAULT_PATH.to_string());
    }
    if !has(&env, "HOME") {
        env.push(format!("HOME={}", home.unwrap_or_else(|| if uid == 0 { "/root".into() } else { "/".into() })));
    }

    let cwd = if !pod.working_dir.is_empty() {
        pod.working_dir.to_string()
    } else if !img.working_dir.is_empty() {
        img.working_dir.clone()
    } else {
        "/".to_string()
    };
    Ok(Composed { argv, env, cwd, uid, gid })
}

/// The image's `PATH`, for resolving a relative argv[0] the way the image
/// would: the composed env's, else the default.
pub fn path_of(env: &[String]) -> Vec<String> {
    let path = env
        .iter()
        .find_map(|e| e.strip_prefix("PATH="))
        .unwrap_or(&DEFAULT_PATH[5..]);
    path.split(':').filter(|d| d.starts_with('/')).map(String::from).collect()
}

/// An image's `User`: `uid`, `uid:gid`, `name`, `name:group` (names looked up
/// in the image's own `/etc/passwd` and `/etc/group`). Returns uid, gid and
/// the user's home when the database has one.
fn resolve_user(user: &str, root: Option<&Path>) -> Result<(u32, u32, Option<String>), String> {
    let (u, g) = match user.split_once(':') {
        Some((u, g)) => (u, Some(g)),
        None => (user, None),
    };
    let (uid, mut gid, home) = match u.parse::<u32>() {
        Ok(n) => {
            let e = root.and_then(|r| passwd_by_uid(r, n));
            (n, e.as_ref().map(|e| e.1).unwrap_or(0), e.map(|e| e.2))
        }
        Err(_) => {
            let e = root
                .and_then(|r| passwd_by_name(r, u))
                .ok_or_else(|| format!("User {user}: no user {u} in the image's /etc/passwd"))?;
            (e.0, e.1, Some(e.2))
        }
    };
    if let Some(g) = g {
        gid = match g.parse::<u32>() {
            Ok(n) => n,
            Err(_) => root
                .and_then(|r| group_by_name(r, g))
                .ok_or_else(|| format!("User {user}: no group {g} in the image's /etc/group"))?,
        };
    }
    Ok((uid, gid, home))
}

fn read(root: &Path, rel: &str) -> String {
    std::fs::read_to_string(root.join(rel)).unwrap_or_default()
}

/// `(uid, gid, home)` from `/etc/passwd` lines `name:x:uid:gid:gecos:home:shell`.
fn passwd_entries(root: &Path) -> Vec<(String, u32, u32, String)> {
    read(root, "etc/passwd")
        .lines()
        .filter_map(|l| {
            let f: Vec<&str> = l.split(':').collect();
            (f.len() >= 6).then(|| Some((f[0].to_string(), f[2].parse().ok()?, f[3].parse().ok()?, f[5].to_string())))?
        })
        .collect()
}

fn passwd_by_name(root: &Path, name: &str) -> Option<(u32, u32, String)> {
    passwd_entries(root).into_iter().find(|e| e.0 == name).map(|e| (e.1, e.2, e.3))
}

fn passwd_by_uid(root: &Path, uid: u32) -> Option<(u32, u32, String)> {
    passwd_entries(root).into_iter().find(|e| e.1 == uid).map(|e| (e.1, e.2, e.3))
}

fn group_by_name(root: &Path, name: &str) -> Option<u32> {
    read(root, "etc/group").lines().find_map(|l| {
        let f: Vec<&str> = l.split(':').collect();
        (f.len() >= 3 && f[0] == name).then(|| f[2].parse().ok())?
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn coredns() -> ImageConfig {
        ImageConfig {
            entrypoint: s(&["/coredns"]),
            env: s(&["PATH=/opt/bin:/bin", "LANG=C"]),
            working_dir: "/srv".into(),
            ..Default::default()
        }
    }

    #[test]
    fn command_and_args_replace_entrypoint_and_cmd_by_crs_rules() {
        let img = ImageConfig { entrypoint: s(&["/ep"]), cmd: s(&["c1", "c2"]), ..Default::default() };
        let run = |command: &[&str], args: &[&str]| {
            let (command, args) = (s(command), s(args));
            let pod = PodSide { image: "img", command: &command, args: &args, ..Default::default() };
            compose(&pod, Some(&img), None).unwrap().argv
        };
        assert_eq!(run(&[], &[]), s(&["/ep", "c1", "c2"]), "neither: Entrypoint + Cmd");
        assert_eq!(run(&[], &["-conf", "/etc/Corefile"]), s(&["/ep", "-conf", "/etc/Corefile"]), "args alone: after Entrypoint");
        assert_eq!(run(&["/bin/sh"], &[]), s(&["/bin/sh"]), "command alone: Cmd dropped");
        assert_eq!(run(&["/bin/sh"], &["-c", "x"]), s(&["/bin/sh", "-c", "x"]));
    }

    #[test]
    fn the_images_env_is_under_the_pods_and_its_workdir_applies_when_unset() {
        let env = s(&["LANG=en_US", "POD=1"]);
        let pod = PodSide { image: "coredns", env: &env, ..Default::default() };
        let c = compose(&pod, Some(&coredns()), None).unwrap();
        assert_eq!(c.env, s(&["PATH=/opt/bin:/bin", "LANG=en_US", "POD=1", "HOME=/root"]));
        assert_eq!(c.cwd, "/srv");
        assert_eq!(path_of(&c.env), s(&["/opt/bin", "/bin"]));
        let pod = PodSide { working_dir: "/data", ..pod };
        assert_eq!(compose(&pod, Some(&coredns()), None).unwrap().cwd, "/data");
        // No config: the defaults the runtime always gave.
        let command = s(&["/bin/true"]);
        let pod = PodSide { image: "busybox", command: &command, ..Default::default() };
        let c = compose(&pod, None, None).unwrap();
        assert_eq!(c.cwd, "/");
        assert!(c.env.contains(&DEFAULT_PATH.to_string()) && c.env.contains(&"HOME=/root".to_string()));
    }

    #[test]
    fn no_config_and_a_flag_or_nothing_to_run_names_the_image() {
        let args = s(&["-conf", "/etc/coredns/Corefile"]);
        let pod = PodSide { image: "registry.k8s.io/coredns/coredns:v1.11", args: &args, ..Default::default() };
        let e = compose(&pod, None, None).unwrap_err();
        assert!(e.contains("registry.k8s.io/coredns/coredns:v1.11") && e.contains("-conf"), "{e}");
        let pod = PodSide { image: "busybox", ..Default::default() };
        assert!(compose(&pod, None, None).unwrap_err().contains("busybox"));
        // Args that are a program still run, as before.
        let args = s(&["/bin/sleep", "1"]);
        let pod = PodSide { image: "busybox", args: &args, ..Default::default() };
        assert_eq!(compose(&pod, None, None).unwrap().argv, args);
    }

    fn image_root() -> PathBuf {
        let d = std::env::temp_dir().join(format!("imgcfg-{}", std::process::id()));
        std::fs::create_dir_all(d.join("etc")).unwrap();
        std::fs::write(
            d.join("etc/passwd"),
            "root:x:0:0:root:/root:/bin/sh\nnonroot:x:65532:65532:nonroot:/home/nonroot:/sbin/nologin\n",
        )
        .unwrap();
        std::fs::write(d.join("etc/group"), "root:x:0:\nnonroot:x:65532:\nstaff:x:50:\n").unwrap();
        d
    }

    #[test]
    fn the_images_user_applies_unless_the_pod_sets_one() {
        let root = image_root();
        let img = ImageConfig { user: "nonroot:nonroot".into(), ..coredns() };
        let pod = PodSide { image: "coredns", ..Default::default() };
        let c = compose(&pod, Some(&img), Some(&root)).unwrap();
        assert_eq!((c.uid, c.gid), (65532, 65532));
        assert!(c.env.contains(&"HOME=/home/nonroot".to_string()), "{:?}", c.env);

        let img2 = ImageConfig { user: "nonroot:staff".into(), ..coredns() };
        assert_eq!(compose(&pod, Some(&img2), Some(&root)).unwrap().gid, 50);
        let img3 = ImageConfig { user: "1000".into(), ..coredns() };
        let c = compose(&pod, Some(&img3), Some(&root)).unwrap();
        assert_eq!((c.uid, c.gid), (1000, 0));
        assert!(c.env.contains(&"HOME=/".to_string()));

        // The pod's numbers win; its group defaults from the user's entry.
        let pod = PodSide { run_as_user: Some(0), ..pod.clone() };
        let c = compose(&pod, Some(&img), Some(&root)).unwrap();
        assert_eq!((c.uid, c.gid), (0, 0));
        let pod = PodSide { run_as_user: Some(4000), run_as_group: Some(4001), ..pod };
        let c = compose(&pod, Some(&img), Some(&root)).unwrap();
        assert_eq!((c.uid, c.gid), (4000, 4001));

        let missing = ImageConfig { user: "ghost".into(), ..coredns() };
        let pod = PodSide { image: "coredns", ..Default::default() };
        assert!(compose(&pod, Some(&missing), Some(&root)).unwrap_err().contains("no user ghost"));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn run_as_non_root_refuses_a_root_container() {
        let pod = PodSide { image: "coredns", run_as_non_root: true, ..Default::default() };
        assert!(compose(&pod, Some(&coredns()), None).unwrap_err().contains("runAsNonRoot"));
        let pod = PodSide { run_as_user: Some(1000), ..pod };
        assert_eq!(compose(&pod, Some(&coredns()), None).unwrap().uid, 1000);
    }

    #[test]
    fn a_golden_record_carries_its_config_and_misses_are_asked_again_later() {
        let rec = serde_json::json!({"name": "coredns", "config": {"Entrypoint": ["/coredns"], "Env": ["A=1"]}});
        let c = from_golden(&rec).unwrap();
        assert_eq!(c.entrypoint, s(&["/coredns"]));
        assert!(from_golden(&serde_json::json!({"name": "x"})).is_none());
        assert!(from_golden(&serde_json::json!({"config": {}})).is_none());

        let cache = ImageConfigs::default();
        assert!(cache.needs_lookup("/pallets/busybox"));
        cache.put("/pallets/busybox", "busybox", None);
        assert!(!cache.needs_lookup("/pallets/busybox"), "a miss is believed for a while");
        assert!(cache.get("/pallets/busybox").is_none());
        cache.put("/run/stormpump/images/v1", "registry.k8s.io/coredns/coredns:v1.11.1", Some(c.clone()));
        assert_eq!(cache.get("/run/stormpump/images/v1"), Some(c));
        assert_eq!(cache.image_of("/pallets/busybox").as_deref(), Some("busybox"));
    }

    /// #130: a registry record's digest, golden and OCI build info.
    #[test]
    fn a_records_provenance() {
        let hex = "a".repeat(64);
        let r = serde_json::json!({"name": "busybox:1.36", "digest": format!("sha256:{hex}"),
            "template_name": "img-aaaaaaaaaaaa", "config": {"Entrypoint": ["sh"],
            "created": "2026-09-01T00:00:00Z",
            "Labels": {"org.opencontainers.image.version": "1.36", "org.opencontainers.image.revision": "abc",
                       "org.opencontainers.image.source": "https://x", "other": "ignored"}}});
        let p = provenance_of_record(&r);
        assert_eq!(p.digest.as_deref(), Some(format!("sha256:{hex}").as_str()));
        assert_eq!(p.golden.as_deref(), Some("img-aaaaaaaaaaaa"));
        assert_eq!(p.build["created"], "2026-09-01T00:00:00Z");
        assert_eq!(p.build["version"], "1.36");
        assert!(!p.build.contains_key("other"));
        // A record without them: no digest, no build.
        let bare = provenance_of_record(&serde_json::json!({"name": "x", "digest": "unknown"}));
        assert_eq!((bare.digest, bare.build.len()), (None, 0));
    }

    /// #130: a release manifest's golden entry.
    #[test]
    fn a_release_goldens_provenance() {
        let hex = "b".repeat(64);
        let m = format!(r#"{{"assets":[{{"kind":"binary","name":"stormlb","digest":"x"}},
            {{"kind":"golden","name":"stormlb","digest":"{hex}","provenance":"stormlb@b8ba1a7","version":"1.2.0"}},
            {{"kind":"golden","name":"cilium","digest":"unknown","provenance":"cilium/cilium@sha256:9d30"}}]}}"#);
        let p = release_golden_in(&m, "stormlb").unwrap();
        assert_eq!(p.digest, Some(format!("sha256:{hex}")));
        assert_eq!((p.build["provenance"].as_str(), p.build["version"].as_str()), (Some("stormlb@b8ba1a7"), Some("1.2.0")));
        let c = release_golden_in(&m, "cilium").unwrap();
        assert_eq!(c.digest, None);
        assert!(release_golden_in(&m, "absent").is_none());
    }

    /// #172: the record's `Volumes` are read; a pod mount at or above a path
    /// covers it; odd paths are dropped.
    #[test]
    fn declared_volumes_not_covered_by_a_mount() {
        let r = serde_json::json!({"config": {"Cmd": ["x"], "Volumes": {
            "/var/lib/postgresql/data": {}, "/cache/": {}, "/etc/conf": {}, "rel": {}, "/a/../b": {}}}});
        let c = from_golden(&r).unwrap();
        assert_eq!(c.volumes.len(), 5);
        let mut got = declared_volumes(&c, &["/etc", "/cache"]);
        got.sort();
        assert_eq!(got, vec!["/var/lib/postgresql/data".to_string()]);
        assert_eq!(declared_volumes(&c, &["/"]).len(), 0, "a mount at / covers all");
    }

    /// #172: made inside the root, existing content kept, never through a
    /// symlink (one pointing out of the root stops the walk).
    #[test]
    fn a_declared_volume_is_made_in_the_root_never_through_a_link() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let r = root.path();
        std::fs::create_dir_all(r.join("var/lib")).unwrap();
        std::fs::write(r.join("var/lib/keep"), "x").unwrap();
        assert!(make_in_root(r, "/var/lib/postgresql/data").unwrap());
        assert!(r.join("var/lib/postgresql/data").is_dir());
        assert!(r.join("var/lib/keep").is_file());
        std::os::unix::fs::symlink(outside.path(), r.join("escape")).unwrap();
        assert!(!make_in_root(r, "/escape/data").unwrap());
        assert!(!outside.path().join("data").exists(), "nothing made outside the root");
        std::fs::write(r.join("file"), "x").unwrap();
        assert!(!make_in_root(r, "/file/sub").unwrap());
    }

    /// #86: a tag naming a golden version other than the release's selects it;
    /// everything else stays the pallet.
    #[test]
    fn a_tag_selects_a_golden_version_and_nothing_else_does() {
        let m = r#"{"assets":[
            {"kind":"golden","name":"nextnfs","digest":"ab","provenance":"golden-nextnfs-1111aaaa2222"},
            {"kind":"golden","name":"cilium","digest":"unknown","provenance":"cilium/cilium@sha256:9d30"}]}"#;
        let none = |_: &str| false;
        let v = golden_version("nextnfs:3333bbbb4444", m, none).unwrap();
        assert_eq!(v.golden, "golden-nextnfs-3333bbbb4444");
        assert_eq!(v.reference, "registry/nextnfs:3333bbbb4444");
        assert_eq!(golden_version("registry/nextnfs:golden-nextnfs-3333bbbb4444", m, none), Some(v));
        // The release's own version, untagged, other tags, digests: the pallet.
        assert_eq!(golden_version("nextnfs:1111aaaa2222", m, none), None);
        assert_eq!(golden_version("nextnfs", m, none), None);
        assert_eq!(golden_version("nextnfs:latest", m, none), None);
        assert_eq!(golden_version("nextnfs@sha256:abcd", m, none), None);
        assert_eq!(golden_version("quay.io/cilium/cilium:v1.18.2@sha256:9d30", m, none), None);
        // A 12-hex tag on something that is no component of ours: not a version.
        assert_eq!(golden_version("ghcr.io/x/tool:3333bbbb4444", m, none), None);
        // A pallet the manifest does not list still has versions.
        assert!(golden_version("rocketsmbd:3333bbbb4444", m, |n| n == "rocketsmbd").is_some());
        assert_eq!(golden_version("nextnfs:3333BBBB4444", m, none), None, "build ids are lowercase");
    }
}
