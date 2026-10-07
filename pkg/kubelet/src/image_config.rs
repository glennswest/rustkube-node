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
    let has = |key: &str| env.iter().any(|e| e.starts_with(&format!("{key}=")));
    if !has("PATH") {
        env.push(DEFAULT_PATH.to_string());
    }
    if !has("HOME") {
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
}
