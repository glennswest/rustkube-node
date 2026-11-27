//! Real-time pods (#430, stormpump#252): how a pod asks for stormpump's
//! `Sched` and `Demand::Exclusive`, and what a node says it can give.
//!
//! A pod asks with three things together:
//! - `spec.runtimeClassName` naming a RuntimeClass whose handler is
//!   [`HANDLER_SOFT`] or [`HANDLER_HARD`];
//! - for each container that runs real time, the extended resource
//!   [`RESOURCE`] (`stormpump.io/isolated-cores`): whole physical cores,
//!   request equal to limit, as upstream's whole-CPU rule;
//! - the pod annotation [`ANNOTATION`]
//!   (`stormpump.io/realtime: "policy=fifo,priority=80,numa=0,same-cache=true,kind=p"`):
//!   the policy and priority, and the cores' shape. Only `priority` is
//!   required.
//!
//! Everything is checked before the pod's sandbox, and a mistake is a wait
//! that names the field: a real-time start must never fail after the fork.
//!
//! **Real time is never inherited.** A container without isolated cores runs
//! `SCHED_OTHER` on the shared pool, whatever the pod asked. A `SCHED_FIFO`
//! thread on shared cores is the hazard stormpump's `docs/realtime.md` names:
//! nothing below its priority, the engine included, preempts it. So:
//! - a real-time handler with no container holding isolated cores is refused;
//! - the annotation without a real-time handler is refused;
//! - neither: `SCHED_OTHER`, the shared pool, as before.
//!
//! **`realtime-hard` needs a `PREEMPT_RT` kernel** (`/sys/kernel/realtime` is
//! `1`). Without one it is refused, never run as soft.

use serde_json::Value;

/// The RuntimeClass handlers.
pub const HANDLER_SOFT: &str = "realtime-soft";
pub const HANDLER_HARD: &str = "realtime-hard";
/// The extended resource: whole physical cores from the isolated set.
pub const RESOURCE: &str = "stormpump.io/isolated-cores";
/// The pod annotation: policy, priority and shape.
pub const ANNOTATION: &str = "stormpump.io/realtime";
/// The Event reason a refusal carries.
pub const REFUSED: &str = "RealtimeRefused";

/// Node labels.
pub const LABEL_REALTIME: &str = "stormpump.io/realtime";
pub const LABEL_PREEMPT: &str = "kernel.stormpump.io/preempt";

pub fn is_realtime_handler(h: &str) -> bool {
    h == HANDLER_SOFT || h == HANDLER_HARD
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    Fifo,
    RoundRobin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cache {
    L2,
    L3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Performance,
    Efficiency,
}

/// What the annotation says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shape {
    pub policy: Policy,
    /// 1..=99.
    pub priority: u8,
    pub numa: Option<u16>,
    pub same_cache: Option<Cache>,
    pub kind: Option<Kind>,
}

/// One container's real time: its cores and how it is scheduled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Realtime {
    /// Whole physical cores from the isolated set.
    pub cores: u16,
    pub shape: Shape,
}

/// Parse [`ANNOTATION`]. An error names the field.
pub fn parse_annotation(text: &str) -> Result<Shape, String> {
    let mut policy = Policy::Fifo;
    let mut priority = None;
    let mut numa = None;
    let mut same_cache = None;
    let mut kind = None;
    for part in text.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (key, value) = part
            .split_once('=')
            .map(|(k, v)| (k.trim(), v.trim()))
            .ok_or_else(|| format!("{ANNOTATION}: {part:?} is not key=value"))?;
        let bad = |what: &str| format!("{ANNOTATION}: {key}={value:?}: {what}");
        match key {
            "policy" => {
                policy = match value.to_ascii_lowercase().as_str() {
                    "fifo" => Policy::Fifo,
                    "rr" | "roundrobin" | "round-robin" => Policy::RoundRobin,
                    _ => return Err(bad("policy is fifo or rr")),
                }
            }
            "priority" => {
                let p: u8 = value.parse().map_err(|_| bad("priority is a number, 1 to 99"))?;
                if !(1..=99).contains(&p) {
                    return Err(bad("priority is 1 to 99"));
                }
                priority = Some(p);
            }
            "numa" => numa = Some(value.parse::<u16>().map_err(|_| bad("numa is a NUMA node number"))?),
            "same-cache" => {
                same_cache = match value.to_ascii_lowercase().as_str() {
                    // One last-level cache: off a second chiplet.
                    "true" | "l3" => Some(Cache::L3),
                    "l2" => Some(Cache::L2),
                    "false" => None,
                    _ => return Err(bad("same-cache is true, false, l2 or l3")),
                }
            }
            "kind" => {
                kind = match value.to_ascii_lowercase().as_str() {
                    "p" | "performance" => Some(Kind::Performance),
                    "e" | "efficiency" => Some(Kind::Efficiency),
                    _ => return Err(bad("kind is p or e")),
                }
            }
            _ => return Err(format!("{ANNOTATION}: unknown key {key:?} (policy, priority, numa, same-cache, kind)")),
        }
    }
    let priority = priority.ok_or_else(|| format!("{ANNOTATION}: priority is required (1 to 99)"))?;
    Ok(Shape { policy, priority, numa, same_cache, kind })
}

/// A container's [`RESOURCE`]: 0 when it asks for none. Request and limit
/// must be equal whole numbers (a limit alone is the request, as upstream
/// defaults it).
pub fn isolated_cores(container: &Value) -> Result<u16, String> {
    let name = container["name"].as_str().unwrap_or("?");
    let read = |which: &str| -> Result<Option<u16>, String> {
        match &container["resources"][which][RESOURCE] {
            Value::Null => Ok(None),
            v => {
                let text = v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string());
                text.trim()
                    .parse::<u16>()
                    .map(Some)
                    .map_err(|_| format!("container {name}: {RESOURCE} {which} {text:?} is not a whole number of cores"))
            }
        }
    };
    let (req, lim) = (read("requests")?, read("limits")?);
    match (req, lim) {
        (None, None) => Ok(0),
        (Some(r), Some(l)) if r == l => Ok(r),
        (None, Some(l)) => Ok(l),
        (Some(r), None) => Err(format!(
            "container {name}: {RESOURCE} request {r} has no limit; an extended resource's limit must equal its request"
        )),
        (Some(r), Some(l)) => Err(format!("container {name}: {RESOURCE} request {r} is not its limit {l}")),
    }
}

/// Every container's real time for a pod whose RuntimeClass handler is
/// `handler`, by container name; checked whole, before anything starts. An
/// error is the refusal's message.
pub fn for_pod(pod: &Value, handler: &str) -> Result<Vec<(String, Realtime)>, String> {
    let annotation = pod["metadata"]["annotations"][ANNOTATION].as_str();
    let rt_handler = is_realtime_handler(handler);
    if annotation.is_some() && !rt_handler {
        return Err(format!(
            "the pod has {ANNOTATION} but its RuntimeClass handler is {:?}: real time needs runtimeClassName \
             with handler {HANDLER_SOFT} or {HANDLER_HARD}",
            handler
        ));
    }
    let mut cores = Vec::new();
    for key in ["initContainers", "containers"] {
        for c in pod["spec"][key].as_array().into_iter().flatten() {
            let n = isolated_cores(c)?;
            if n > 0 {
                cores.push((c["name"].as_str().unwrap_or("").to_string(), n));
            }
        }
    }
    if !rt_handler {
        // Isolated cores asked without real time: refused too, so a pod is
        // never placed on the isolated set by accident of a resource alone.
        if let Some((name, _)) = cores.first() {
            return Err(format!(
                "container {name} asks for {RESOURCE}, which only a pod with runtimeClassName \
                 {HANDLER_SOFT} or {HANDLER_HARD} is given"
            ));
        }
        return Ok(Vec::new());
    }
    let text = annotation.ok_or_else(|| {
        format!("RuntimeClass handler {handler} needs the pod annotation {ANNOTATION} with at least priority=<1..99>")
    })?;
    let shape = parse_annotation(text)?;
    if cores.is_empty() {
        return Err(format!(
            "RuntimeClass handler {handler}: no container asks for {RESOURCE}; real-time scheduling runs only on \
             isolated cores, never on the shared pool"
        ));
    }
    Ok(cores.into_iter().map(|(name, cores)| (name, Realtime { cores, shape })).collect())
}

/// What the node's kernel is, from `uname -v` and `/sys/kernel/realtime`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Preempt {
    Rt,
    Dynamic,
    Full,
    None,
}

impl Preempt {
    pub fn label(self) -> &'static str {
        match self {
            Preempt::Rt => "rt",
            Preempt::Dynamic => "dynamic",
            Preempt::Full => "full",
            Preempt::None => "none",
        }
    }

    /// From the kernel's version string and `/sys/kernel/realtime`'s text
    /// (`None`: the file is absent). `PREEMPT_RT` is the file saying `1`.
    pub fn of(version: &str, realtime: Option<&str>) -> Preempt {
        if realtime.map(str::trim) == Some("1") {
            return Preempt::Rt;
        }
        let words: Vec<&str> = version.split_whitespace().collect();
        if words.contains(&"PREEMPT_DYNAMIC") {
            Preempt::Dynamic
        } else if words.contains(&"PREEMPT") {
            Preempt::Full
        } else {
            Preempt::None
        }
    }

    pub fn read() -> Preempt {
        let version = std::fs::read_to_string("/proc/sys/kernel/version").unwrap_or_default();
        let rt = std::fs::read_to_string("/sys/kernel/realtime").ok();
        Preempt::of(&version, rt.as_deref())
    }
}

/// The `realtime-hard` admission (#430): refused without `PREEMPT_RT`.
pub fn hard_refusal(handler: &str, preempt: Preempt) -> Option<String> {
    (handler == HANDLER_HARD && preempt != Preempt::Rt).then(|| {
        format!(
            "RuntimeClass handler {HANDLER_HARD} needs a PREEMPT_RT kernel and this node's is {} \
             (/sys/kernel/realtime is not 1); it is not run as soft. Use {HANDLER_SOFT}, or a node \
             labelled {LABEL_REALTIME}=hard",
            preempt.label()
        )
    })
}

/// The node's isolated set: the list after `--isolate` on PID 1's command
/// line (stormpump's `--init --isolate L`), from `cmdline`'s NUL-separated
/// words. `--isolate=L` is read too.
pub fn isolate_arg(cmdline: &[u8]) -> Option<String> {
    let words: Vec<String> = cmdline
        .split(|b| *b == 0)
        .filter(|w| !w.is_empty())
        .map(|w| String::from_utf8_lossy(w).into_owned())
        .collect();
    for (i, w) in words.iter().enumerate() {
        if let Some(v) = w.strip_prefix("--isolate=") {
            return Some(v.to_string());
        }
        if w == "--isolate" {
            return words.get(i + 1).cloned();
        }
    }
    None
}

/// A kernel CPU list (`2-5,8,10-11`).
pub fn parse_cpu_list(list: &str) -> Result<Vec<u32>, String> {
    let mut out = Vec::new();
    for part in list.trim().split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (a, b) = match part.split_once('-') {
            Some((a, b)) => (a, b),
            None => (part, part),
        };
        let a: u32 = a.trim().parse().map_err(|_| format!("CPU list {list:?}: {part:?}"))?;
        let b: u32 = b.trim().parse().map_err(|_| format!("CPU list {list:?}: {part:?}"))?;
        if b < a {
            return Err(format!("CPU list {list:?}: {part:?} runs backwards"));
        }
        out.extend(a..=b);
    }
    Ok(out)
}

/// Whole physical cores among `cpus`: SMT siblings count once, by
/// (package, core) as `topology` gives them for each CPU.
pub fn physical_cores(cpus: &[u32], topology: impl Fn(u32) -> Option<(u32, u32)>) -> u16 {
    let mut seen = std::collections::BTreeSet::new();
    for &c in cpus {
        seen.insert(topology(c).unwrap_or((u32::MAX, c)));
    }
    seen.len().min(u16::MAX as usize) as u16
}

/// This node's isolated physical cores: 0 with no `--isolate`.
pub fn node_isolated_cores() -> u16 {
    let Some(list) = std::fs::read("/proc/1/cmdline").ok().and_then(|c| isolate_arg(&c)) else { return 0 };
    let cpus = match parse_cpu_list(&list) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("PID 1's --isolate is not a CPU list, no isolated cores reported: {e}");
            return 0;
        }
    };
    physical_cores(&cpus, |c| {
        let read = |f: &str| {
            std::fs::read_to_string(format!("/sys/devices/system/cpu/cpu{c}/topology/{f}"))
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok())
        };
        Some((read("physical_package_id")?, read("core_id")?))
    })
}

/// The node's [`LABEL_REALTIME`]: `hard` with isolated cores and
/// `PREEMPT_RT`, `soft` with isolated cores, `none` without.
pub fn node_label(isolated: u16, preempt: Preempt) -> &'static str {
    match (isolated, preempt) {
        (0, _) => "none",
        (_, Preempt::Rt) => "hard",
        _ => "soft",
    }
}

/// What a node can give real-time pods, for its Node object (#430).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeFacts {
    /// Whole physical cores in the isolated set.
    pub isolated: u16,
    pub preempt: Preempt,
}

impl NodeFacts {
    pub fn read() -> NodeFacts {
        NodeFacts { isolated: node_isolated_cores(), preempt: Preempt::read() }
    }

    /// The labels' merge patch: [`LABEL_REALTIME`] and [`LABEL_PREEMPT`].
    pub fn labels(&self) -> serde_json::Map<String, Value> {
        let mut m = serde_json::Map::new();
        m.insert(LABEL_REALTIME.into(), Value::String(node_label(self.isolated, self.preempt).into()));
        m.insert(LABEL_PREEMPT.into(), Value::String(self.preempt.label().into()));
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_annotation_is_parsed_and_its_mistakes_named() {
        let s = parse_annotation("policy=fifo,priority=80,numa=0,same-cache=true,kind=p").unwrap();
        assert_eq!(s, Shape { policy: Policy::Fifo, priority: 80, numa: Some(0), same_cache: Some(Cache::L3), kind: Some(Kind::Performance) });
        let s = parse_annotation(" priority = 5 , policy = rr ").unwrap();
        assert_eq!((s.policy, s.priority, s.numa, s.same_cache, s.kind), (Policy::RoundRobin, 5, None, None, None));
        assert_eq!(parse_annotation("priority=1,same-cache=l2").unwrap().same_cache, Some(Cache::L2));
        for (bad, field) in [
            ("policy=fifo", "priority is required"),
            ("priority=0", "priority=\"0\""),
            ("priority=100", "priority=\"100\""),
            ("priority=high", "priority=\"high\""),
            ("priority=80,policy=deadline", "policy=\"deadline\""),
            ("priority=80,numa=-1", "numa=\"-1\""),
            ("priority=80,kind=x", "kind=\"x\""),
            ("priority=80,same-cache=yes", "same-cache=\"yes\""),
            ("priority=80,cpus=2", "unknown key \"cpus\""),
            ("priority", "is not key=value"),
        ] {
            let e = parse_annotation(bad).unwrap_err();
            assert!(e.contains(field), "{bad}: {e}");
        }
    }

    fn container(name: &str, req: Option<Value>, lim: Option<Value>) -> Value {
        let mut c = json!({"name": name, "image": "i"});
        if let Some(r) = req {
            c["resources"]["requests"][RESOURCE] = r;
        }
        if let Some(l) = lim {
            c["resources"]["limits"][RESOURCE] = l;
        }
        c
    }

    #[test]
    fn isolated_cores_are_whole_and_their_request_is_their_limit() {
        assert_eq!(isolated_cores(&container("a", None, None)), Ok(0));
        assert_eq!(isolated_cores(&container("a", Some(json!("2")), Some(json!("2")))), Ok(2));
        assert_eq!(isolated_cores(&container("a", None, Some(json!(3)))), Ok(3), "a limit alone is the request");
        assert!(isolated_cores(&container("a", Some(json!("2")), Some(json!("3")))).unwrap_err().contains("not its limit 3"));
        assert!(isolated_cores(&container("a", Some(json!("2")), None)).unwrap_err().contains("has no limit"));
        assert!(isolated_cores(&container("a", Some(json!("1500m")), Some(json!("1500m")))).unwrap_err().contains("whole number"));
    }

    fn rt_pod(annotation: Option<&str>, containers: Vec<Value>) -> Value {
        let mut p = json!({"metadata": {"name": "rt", "annotations": {}}, "spec": {"containers": containers}});
        if let Some(a) = annotation {
            p["metadata"]["annotations"][ANNOTATION] = json!(a);
        }
        p
    }

    #[test]
    fn real_time_is_given_only_as_asked_and_never_inherited() {
        let two = || container("loop", Some(json!("2")), Some(json!("2")));
        let shared = || container("side", None, None);
        // Asked properly: the isolated container is real time, its neighbour not.
        let rt = for_pod(&rt_pod(Some("priority=80"), vec![two(), shared()]), HANDLER_SOFT).unwrap();
        assert_eq!(rt.len(), 1);
        assert_eq!((rt[0].0.as_str(), rt[0].1.cores, rt[0].1.shape.priority), ("loop", 2, 80));
        // Neither: nothing, as before.
        assert_eq!(for_pod(&rt_pod(None, vec![shared()]), ""), Ok(vec![]));
        // The annotation without the handler.
        assert!(for_pod(&rt_pod(Some("priority=80"), vec![two()]), "").unwrap_err().contains("needs runtimeClassName"));
        // Isolated cores without the handler.
        assert!(for_pod(&rt_pod(None, vec![two()]), "").unwrap_err().contains("only a pod with runtimeClassName"));
        // The handler without the annotation, or without isolated cores.
        assert!(for_pod(&rt_pod(None, vec![two()]), HANDLER_SOFT).unwrap_err().contains("priority=<1..99>"));
        assert!(for_pod(&rt_pod(Some("priority=80"), vec![shared()]), HANDLER_HARD).unwrap_err().contains("never on the shared pool"));
        // A bad field is named.
        assert!(for_pod(&rt_pod(Some("priority=80,kind=q"), vec![two()]), HANDLER_SOFT).unwrap_err().contains("kind=\"q\""));
    }

    #[test]
    fn hard_is_refused_without_preempt_rt_and_soft_is_not() {
        let fedora = "#1 SMP PREEMPT_DYNAMIC Fri Oct 31 00:00:00 UTC 2026";
        assert_eq!(Preempt::of(fedora, None), Preempt::Dynamic);
        assert_eq!(Preempt::of("#1 SMP PREEMPT_RT Fri", Some("1\n")), Preempt::Rt);
        assert_eq!(Preempt::of("#1 SMP PREEMPT Fri", Some("0")), Preempt::Full);
        assert_eq!(Preempt::of("#1 SMP Fri", None), Preempt::None);
        let why = hard_refusal(HANDLER_HARD, Preempt::Dynamic).unwrap();
        assert!(why.contains("PREEMPT_RT") && why.contains("dynamic") && why.contains("not run as soft"), "{why}");
        assert_eq!(hard_refusal(HANDLER_HARD, Preempt::Rt), None);
        assert_eq!(hard_refusal(HANDLER_SOFT, Preempt::Dynamic), None);
    }

    #[test]
    fn the_isolated_set_is_counted_in_physical_cores() {
        let cmdline = b"/stormpump\0--init\0--isolate\x002-5,8\0--infra\x000-1\0";
        assert_eq!(isolate_arg(cmdline).as_deref(), Some("2-5,8"));
        assert_eq!(isolate_arg(b"/stormpump\0--isolate=4-7\0").as_deref(), Some("4-7"));
        assert_eq!(isolate_arg(b"/stormpump\0--init\0"), None);
        let cpus = parse_cpu_list("2-5,8").unwrap();
        assert_eq!(cpus, vec![2, 3, 4, 5, 8]);
        assert!(parse_cpu_list("5-2").is_err());
        // SMT siblings are CPUs 2n and 2n+1: 2-3 are core 1, 4-5 core 2, 8 core 4.
        let topo = |c: u32| Some((0, c / 2));
        assert_eq!(physical_cores(&cpus, topo), 3);
        assert_eq!(node_label(0, Preempt::Rt), "none");
        assert_eq!(node_label(3, Preempt::Dynamic), "soft");
        assert_eq!(node_label(3, Preempt::Rt), "hard");
        let l = NodeFacts { isolated: 3, preempt: Preempt::Dynamic }.labels();
        assert_eq!((l[LABEL_REALTIME].as_str(), l[LABEL_PREEMPT].as_str()), (Some("soft"), Some("dynamic")));
    }
}
