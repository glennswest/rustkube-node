//! `/logs/`: the node's log files and its services' logs, which `oc adm
//! node-logs` reads through the apiserver's `nodes/{name}/proxy` (#198,
//! rustkube#108).
//!
//! `oc adm node-logs <node>` asks `GET /logs/journal`, and with `-u <unit>`
//! adds `unit=<unit>&query=<unit>` (the second is upstream's NodeLogQuery
//! name). Its `--since`, `--until`, `--boot`, `--tail`, `--grep`,
//! `--case-sensitive` and `-o` become `since`, `until`, `boot`,
//! `tail`/`tailLines`, `grep`/`pattern`, `case-sensitive` and `output`.
//! `--path=<p>` asks `GET /logs/<p>`. oc prints the body as it comes, except
//! that a body starting `<pre>` or `<!doctype html>` is read as a directory
//! listing and each `href` is printed, one per line.
//!
//! **The journal.** stormcos has no journald. A node service's output is
//! stormd's, on the service's log volume ([`crate::node_logs`]), or, for a
//! service stormd does not run (stormblock, the registry), PID 1's files for
//! each of its runs. So the journal is every node service's log, merged in
//! time order:
//!
//! - a stormd service: every process's finished runs, rotations and live file,
//!   each line's own RFC 3339 time;
//! - a service PID 1 runs directly: its runs listed in `assets.json`, oldest
//!   first. Those lines carry no time of their own, so each is placed, and
//!   filtered, at the start of its run.
//!
//! A unit names a service (`fastetcd`, `rustkube-node`), or, when no service
//! has that name, every stormd process that has it (`kubelet` is
//! rustkube-node's kubelet process). A trailing `.service` is dropped. A unit
//! that names neither is a 404 that lists the services.
//!
//! **The files.** Upstream serves the host's `/var/log`. Here `/logs/` lists
//! the host's `/var/log` (seen at `/hostroot/var/log`) and every node service's
//! stormd log directory by service name. When the two have an entry of the
//! same name, the host's is served. A path never leaves the directory it
//! starts in: `..` is refused, and a symlink that resolves outside it is
//! not served.

use crate::node_logs::{self, Record};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::io::BufRead;
use std::path::{Path, PathBuf};

/// Upstream's bound on `tailLines` (NodeLogQuery), and oc's `--tail` help.
pub const MAX_TAIL: usize = 100_000;

/// Where this node's logs are, as the kubelet sees them.
#[derive(Debug, Clone)]
pub struct Node {
    /// The host's root (`/hostroot`).
    pub host_root: PathBuf,
    /// PID 1's asset table.
    pub assets_json: PathBuf,
    /// PID 1's per-run output files.
    pub runs_dir: PathBuf,
    /// The node's name: the journal's host field.
    pub name: String,
}

/// Where one service's log comes from.
#[derive(Debug, Clone)]
pub enum Source {
    /// Its stormd log directory.
    Stormd(PathBuf),
    /// PID 1's record of its runs.
    Engine(Record),
}

impl Node {
    /// The host's `/var/log`: under the host root when there is one, else
    /// the kubelet's own (a kubelet run on the host itself).
    pub fn var_log(&self) -> PathBuf {
        if self.host_root.is_dir() {
            self.host_root.join("var/log")
        } else {
            PathBuf::from("/var/log")
        }
    }

    /// Every node service, by name: each boot.d spec with a stormd log
    /// volume, and each asset in PID 1's table. A service whose stormd
    /// directory has no log yet is read from PID 1's record, when it has one.
    pub fn services(&self) -> BTreeMap<String, Source> {
        let mut out: BTreeMap<String, Source> = node_logs::log_dirs(&self.host_root)
            .into_iter()
            .map(|(name, dir)| (name, Source::Stormd(dir)))
            .collect();
        let records = std::fs::read_to_string(&self.assets_json).map(|t| node_logs::records(&t)).unwrap_or_default();
        for (name, record) in records {
            let empty = match out.get(&name) {
                None => true,
                Some(Source::Stormd(dir)) => node_logs::all_files(dir).is_empty(),
                Some(Source::Engine(_)) => false,
            };
            if empty {
                out.insert(name, Source::Engine(record));
            }
        }
        out
    }
}

/// One journal line.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub ts: DateTime<Utc>,
    pub service: String,
    /// The stormd process that wrote it; `None` for a service PID 1 runs.
    pub process: Option<String>,
    /// stormd's stream (`stdout`, `stderr`, `syslog`, `ingest`).
    pub stream: Option<String>,
    /// stormd's severity (`error`, `warn`, `info`, …).
    pub severity: Option<String>,
    pub message: String,
}

/// What a chain reads next.
#[derive(Debug)]
enum Piece {
    /// A stormd log file: each line carries its time.
    Stormd(PathBuf),
    /// One of PID 1's run files: each line is placed at its run's start.
    Run(PathBuf, DateTime<Utc>),
}

/// One process's (or one engine service's) lines, oldest first, read a line
/// at a time so the journal never holds a whole log.
#[derive(Debug)]
pub struct Chain {
    service: String,
    process: Option<String>,
    pieces: VecDeque<Piece>,
    #[allow(clippy::type_complexity)]
    open: Option<(std::io::BufReader<std::fs::File>, Option<DateTime<Utc>>, DateTime<Utc>)>,
    last: Option<DateTime<Utc>>,
}

impl Chain {
    fn new(service: &str, process: Option<&str>, pieces: Vec<Piece>) -> Chain {
        Chain {
            service: service.to_string(),
            process: process.map(str::to_string),
            pieces: pieces.into(),
            open: None,
            last: None,
        }
    }
}

impl Iterator for Chain {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        loop {
            if self.open.is_none() {
                let (path, fixed) = match self.pieces.pop_front()? {
                    Piece::Stormd(p) => (p, None),
                    Piece::Run(p, at) => (p, Some(at)),
                };
                let Ok(f) = std::fs::File::open(&path) else { continue };
                // A line with no time before any line with one: the file's
                // own time is the nearest there is.
                let mtime = f.metadata().and_then(|m| m.modified()).map(DateTime::<Utc>::from).unwrap_or_default();
                self.open = Some((std::io::BufReader::new(f), fixed, mtime));
            }
            let (reader, fixed, mtime) = self.open.as_mut()?;
            let mut buf = Vec::new();
            match reader.read_until(b'\n', &mut buf) {
                Ok(0) | Err(_) => {
                    self.open = None;
                    continue;
                }
                Ok(_) => {}
            }
            let line = String::from_utf8_lossy(&buf);
            let line = line.trim_end_matches(['\n', '\r']);
            if let Some(at) = *fixed {
                return Some(Entry {
                    ts: at,
                    service: self.service.clone(),
                    process: self.process.clone(),
                    stream: None,
                    severity: None,
                    message: line.to_string(),
                });
            }
            let (ts, stream, severity, message) = match stormd_line(line) {
                Some((ts, stream, sev, msg)) => (ts, Some(stream.to_string()), Some(sev.to_string()), msg.to_string()),
                None => (self.last.unwrap_or(*mtime), None, None, line.to_string()),
            };
            self.last = Some(ts);
            return Some(Entry {
                ts,
                service: self.service.clone(),
                process: self.process.clone(),
                stream,
                severity,
                message,
            });
        }
    }
}

/// A stormd log line: `<rfc3339> <stream> <severity> <message>`.
fn stormd_line(line: &str) -> Option<(DateTime<Utc>, &str, &str, &str)> {
    let mut it = line.splitn(4, ' ');
    let ts = DateTime::parse_from_rfc3339(it.next()?).ok()?.with_timezone(&Utc);
    let (stream, sev) = (it.next()?, it.next()?);
    Some((ts, stream, sev, it.next().unwrap_or("")))
}

/// Several chains merged in time order; a tie goes to the earlier chain.
struct Merge {
    chains: Vec<Chain>,
    heads: Vec<Option<Entry>>,
    heap: BinaryHeap<Reverse<(DateTime<Utc>, usize)>>,
}

impl Merge {
    fn new(mut chains: Vec<Chain>) -> Merge {
        let mut heap = BinaryHeap::new();
        let heads: Vec<Option<Entry>> = chains.iter_mut().map(Iterator::next).collect();
        for (i, h) in heads.iter().enumerate() {
            if let Some(e) = h {
                heap.push(Reverse((e.ts, i)));
            }
        }
        Merge { chains, heads, heap }
    }
}

impl Iterator for Merge {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        let Reverse((_, i)) = self.heap.pop()?;
        let out = self.heads[i].take();
        self.heads[i] = self.chains[i].next();
        if let Some(e) = &self.heads[i] {
            self.heap.push(Reverse((e.ts, i)));
        }
        out
    }
}

/// How each line is written: journalctl's `--output` modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Output {
    Short,
    ShortPrecise,
    ShortIso,
    ShortIsoPrecise,
    ShortUnix,
    Cat,
    Json,
}

impl Output {
    const NAMES: [(&'static str, Output); 7] = [
        ("short", Output::Short),
        ("short-precise", Output::ShortPrecise),
        ("short-iso", Output::ShortIso),
        ("short-iso-precise", Output::ShortIsoPrecise),
        ("short-unix", Output::ShortUnix),
        ("cat", Output::Cat),
        ("json", Output::Json),
    ];

    fn parse(s: &str) -> Result<Output, String> {
        Self::NAMES.iter().find(|(n, _)| *n == s).map(|(_, o)| *o).ok_or_else(|| {
            let names: Vec<&str> = Self::NAMES.iter().map(|(n, _)| *n).collect();
            format!("output={s}: one of {}\n", names.join(", "))
        })
    }

    /// One entry as a line, newline included.
    pub fn format(self, e: &Entry, host: &str) -> String {
        let ident = match &e.process {
            Some(p) => format!("{}[{p}]", e.service),
            None => e.service.clone(),
        };
        let micros = e.ts.timestamp_subsec_micros();
        let stamp = match self {
            Output::Cat => return format!("{}\n", e.message),
            Output::Json => {
                let mut v = serde_json::json!({
                    "__REALTIME_TIMESTAMP": (e.ts.timestamp() * 1_000_000 + i64::from(micros)).to_string(),
                    "_HOSTNAME": host,
                    "SYSLOG_IDENTIFIER": e.service,
                    "_SYSTEMD_UNIT": e.service,
                    "MESSAGE": e.message,
                });
                if let Some(p) = &e.process {
                    v["_COMM"] = p.clone().into();
                }
                if let Some(p) = e.severity.as_deref().and_then(priority) {
                    v["PRIORITY"] = p.to_string().into();
                }
                if let Some(s) = &e.stream {
                    v["STORMD_STREAM"] = s.clone().into();
                }
                return format!("{v}\n");
            }
            Output::Short => e.ts.format("%b %d %H:%M:%S").to_string(),
            Output::ShortPrecise => format!("{}.{micros:06}", e.ts.format("%b %d %H:%M:%S")),
            Output::ShortIso => e.ts.format("%Y-%m-%dT%H:%M:%S%:z").to_string(),
            Output::ShortIsoPrecise => format!("{}.{micros:06}{}", e.ts.format("%Y-%m-%dT%H:%M:%S"), e.ts.format("%:z")),
            Output::ShortUnix => format!("{}.{micros:06}", e.ts.timestamp()),
        };
        format!("{stamp} {host} {ident}: {}\n", e.message)
    }
}

/// syslog's priority for a stormd severity.
fn priority(severity: &str) -> Option<u8> {
    Some(match severity {
        "emerg" => 0,
        "alert" => 1,
        "crit" => 2,
        "error" => 3,
        "warn" => 4,
        "notice" => 5,
        "info" => 6,
        "debug" => 7,
        _ => return None,
    })
}

/// A journal request, read from its query string.
#[derive(Debug)]
pub struct Query {
    /// Units asked for, `.service` dropped; empty is every service.
    pub units: Vec<String>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub pattern: Option<regex::Regex>,
    pub tail: Option<usize>,
    pub output: Output,
}

impl Query {
    /// Parse the pairs oc and upstream's NodeLogQuery send. When both
    /// spellings of one option come (oc sends `tail` and `tailLines`, `grep`
    /// and `pattern`), the first named here is taken. `boot_time` is asked
    /// only for `boot=0`.
    pub fn parse(
        pairs: &[(String, String)],
        now: DateTime<Utc>,
        boot_time: impl FnOnce() -> Option<DateTime<Utc>>,
    ) -> Result<Query, String> {
        fn value<'a>(pairs: &'a [(String, String)], keys: &[&str]) -> Option<&'a str> {
            keys.iter().find_map(|k| pairs.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str()))
        }
        let first = |keys: &[&str]| value(pairs, keys);
        let mut units: Vec<String> = Vec::new();
        for (k, v) in pairs {
            if k == "unit" || k == "query" {
                let u = v.trim().strip_suffix(".service").unwrap_or(v.trim()).to_string();
                if !u.is_empty() && !units.contains(&u) {
                    units.push(u);
                }
            }
        }
        let mut since = match first(&["since", "sinceTime"]) {
            Some(s) => Some(parse_time(s, now).map_err(|e| format!("since={s}: {e}\n"))?),
            None => None,
        };
        let until = match first(&["until", "untilTime"]) {
            Some(s) => Some(parse_time(s, now).map_err(|e| format!("until={s}: {e}\n"))?),
            None => None,
        };
        if let Some(b) = first(&["boot"]) {
            match b.trim().parse::<i64>() {
                Ok(0) => {
                    let booted = boot_time().ok_or("boot=0: the host's boot time (btime in /proc/stat) cannot be read\n")?;
                    since = Some(since.map_or(booted, |s| s.max(booted)));
                }
                Ok(n) => {
                    return Err(format!(
                        "boot={n}: data from the specified boot is not available: stormd's logs record no boots, so only the current one (0) is known\n"
                    ))
                }
                Err(_) => return Err(format!("boot={b}: a boot offset, 0 for the current boot\n")),
            }
        }
        let tail = match first(&["tail", "tailLines"]) {
            Some(t) => match t.trim().parse::<usize>() {
                Ok(n) if n <= MAX_TAIL => Some(n),
                _ => return Err(format!("tail={t}: a number of lines from 0 to {MAX_TAIL}\n")),
            },
            None => None,
        };
        let pattern = match first(&["grep", "pattern"]) {
            Some(p) if !p.is_empty() => {
                // journalctl's smart case when it is not said: insensitive
                // unless the pattern has a capital.
                let sensitive = match first(&["case-sensitive"]) {
                    None => p.chars().any(char::is_uppercase),
                    Some(v) => parse_bool(v).ok_or_else(|| format!("case-sensitive={v}: true or false\n"))?,
                };
                Some(
                    regex::RegexBuilder::new(p)
                        .case_insensitive(!sensitive)
                        .build()
                        .map_err(|e| format!("grep={p}: {e}\n"))?,
                )
            }
            _ => None,
        };
        let output = match first(&["output"]) {
            Some(o) if !o.is_empty() => Output::parse(o)?,
            // Upstream's NodeLogQuery runs journalctl with short-precise.
            _ => Output::ShortPrecise,
        };
        Ok(Query { units, since, until, pattern, tail, output })
    }

    fn keeps(&self, e: &Entry) -> bool {
        self.since.is_none_or(|s| e.ts >= s)
            && self.until.is_none_or(|u| e.ts <= u)
            && self.pattern.as_ref().is_none_or(|p| p.is_match(&e.message))
    }
}

/// Go's `strconv.ParseBool`.
fn parse_bool(v: &str) -> Option<bool> {
    match v {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Some(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Some(false),
        _ => None,
    }
}

/// When the host booted: `btime` in `/proc/stat` (the kernel's, whatever the
/// kubelet's mount namespace).
pub fn boot_time() -> Option<DateTime<Utc>> {
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    let secs: i64 = stat.lines().find_map(|l| l.strip_prefix("btime "))?.trim().parse().ok()?;
    Utc.timestamp_opt(secs, 0).single()
}

/// A time as journalctl's `--since`/`--until` take it, in UTC: RFC 3339;
/// `YYYY-MM-DD[ HH:MM[:SS[.frac]]]` (or with a `T`), a trailing ` UTC` or `Z`
/// allowed; `HH:MM[:SS]` today; `now`, `today`, `yesterday`, `tomorrow`; and
/// a span from now: `-1h`, `+30min`, `2 days ago`.
pub fn parse_time(s: &str, now: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
    let s = s.trim();
    let midnight = |d: NaiveDate| Utc.from_utc_datetime(&d.and_time(NaiveTime::MIN));
    let today = NaiveDate::from_ymd_opt(now.year(), now.month(), now.day()).ok_or("no date today")?;
    match s {
        "now" => return Ok(now),
        "today" => return Ok(midnight(today)),
        "yesterday" => return Ok(midnight(today.pred_opt().ok_or("no yesterday")?)),
        "tomorrow" => return Ok(midnight(today.succ_opt().ok_or("no tomorrow")?)),
        _ => {}
    }
    if let Some(span) = s.strip_prefix('-') {
        return Ok(now - parse_span(span)?);
    }
    if let Some(span) = s.strip_prefix('+') {
        return Ok(now + parse_span(span)?);
    }
    if let Some(span) = s.strip_suffix(" ago") {
        return Ok(now - parse_span(span)?);
    }
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Ok(t.with_timezone(&Utc));
    }
    let bare = s.strip_suffix(" UTC").or_else(|| s.strip_suffix('Z')).unwrap_or(s);
    for f in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"] {
        if let Ok(t) = NaiveDateTime::parse_from_str(bare, f) {
            return Ok(Utc.from_utc_datetime(&t));
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(bare, "%Y-%m-%d") {
        return Ok(midnight(d));
    }
    for f in ["%H:%M:%S", "%H:%M"] {
        if let Ok(t) = NaiveTime::parse_from_str(bare, f) {
            return Ok(Utc.from_utc_datetime(&today.and_time(t)));
        }
    }
    Err("not a time: RFC 3339, YYYY-MM-DD [HH:MM[:SS]], now, today, yesterday, tomorrow, or a span such as -1h or \"2 days ago\"".into())
}

/// A span as systemd writes one: `1h30min`, `2 days`, `90` (seconds).
fn parse_span(s: &str) -> Result<chrono::Duration, String> {
    let bad = || format!("{s:?} is not a span such as 1h, 30min or 2 days");
    let mut total = 0f64;
    let mut rest = s.trim();
    if rest.is_empty() {
        return Err(bad());
    }
    while !rest.is_empty() {
        let n_end = rest.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(rest.len());
        let n: f64 = rest[..n_end].parse().map_err(|_| bad())?;
        rest = rest[n_end..].trim_start();
        let u_end = rest.find(|c: char| !c.is_ascii_alphabetic()).unwrap_or(rest.len());
        let secs = match &rest[..u_end] {
            "" | "s" | "sec" | "secs" | "second" | "seconds" => 1.0,
            "us" | "usec" => 1e-6,
            "ms" | "msec" => 1e-3,
            "m" | "min" | "mins" | "minute" | "minutes" => 60.0,
            "h" | "hr" | "hour" | "hours" => 3600.0,
            "d" | "day" | "days" => 86_400.0,
            "w" | "week" | "weeks" => 604_800.0,
            "M" | "month" | "months" => 2_629_800.0,
            "y" | "year" | "years" => 31_557_600.0,
            _ => return Err(bad()),
        };
        total += n * secs;
        rest = rest[u_end..].trim_start();
    }
    chrono::Duration::try_milliseconds((total * 1000.0) as i64).ok_or_else(bad)
}

/// The chains a journal request reads: every service, or the units asked
/// for. A unit that names neither a service nor a stormd process is an
/// error naming the services there are.
pub fn chains(node: &Node, services: &BTreeMap<String, Source>, units: &[String]) -> Result<Vec<Chain>, String> {
    let procs: BTreeMap<&str, Vec<(String, Vec<PathBuf>)>> = services
        .iter()
        .map(|(name, src)| {
            let files = match src {
                Source::Stormd(dir) => node_logs::all_files(dir),
                Source::Engine(_) => Vec::new(),
            };
            (name.as_str(), files)
        })
        .collect();
    let missing: Vec<&String> = units
        .iter()
        .filter(|u| !services.contains_key(*u) && !procs.values().flatten().any(|(p, _)| p == *u))
        .collect();
    if !missing.is_empty() {
        let names: Vec<&str> = services.keys().map(String::as_str).collect();
        return Err(format!(
            "no node service or stormd process {} on {}; its services: {}\n",
            missing.iter().map(|u| u.as_str()).collect::<Vec<_>>().join(", "),
            node.name,
            if names.is_empty() { "none".to_string() } else { names.join(", ") }
        ));
    }
    let mut out = Vec::new();
    for (name, src) in services {
        let whole = units.is_empty() || units.contains(name);
        match src {
            Source::Stormd(_) => {
                for (proc_name, files) in &procs[name.as_str()] {
                    // A unit that is a service's name means that service, not
                    // processes of the same name elsewhere.
                    let by_proc = units.iter().any(|u| u == proc_name && !services.contains_key(u));
                    if whole || by_proc {
                        out.push(Chain::new(name, Some(proc_name), files.iter().cloned().map(Piece::Stormd).collect()));
                    }
                }
            }
            Source::Engine(record) if whole => {
                let pieces = record
                    .runs
                    .iter()
                    .filter_map(|run| Some((run, Utc.timestamp_opt(run.started_at as i64, 0).single()?)))
                    .flat_map(|(run, at)| {
                        node_logs::run_files(&node.runs_dir, run).into_iter().map(move |f| Piece::Run(f, at))
                    })
                    .collect();
                out.push(Chain::new(name, None, pieces));
            }
            Source::Engine(_) => {}
        }
    }
    Ok(out)
}

/// Write the journal through `send`, in chunks; `send` answers false when
/// the reader has gone, and writing stops.
pub fn write_journal(chains: Vec<Chain>, q: &Query, host: &str, mut send: impl FnMut(Vec<u8>) -> bool) {
    const CHUNK: usize = 64 * 1024;
    let mut tail: Option<(usize, VecDeque<String>)> = q.tail.map(|n| (n, VecDeque::new()));
    let mut buf = String::new();
    let mut any = false;
    for e in Merge::new(chains).filter(|e| q.keeps(e)) {
        any = true;
        let line = q.output.format(&e, host);
        match &mut tail {
            Some((0, _)) => {}
            Some((n, lines)) => {
                if lines.len() == *n {
                    lines.pop_front();
                }
                lines.push_back(line);
            }
            None => {
                buf.push_str(&line);
                if buf.len() >= CHUNK && !send(std::mem::take(&mut buf).into_bytes()) {
                    return;
                }
            }
        }
    }
    if let Some((_, lines)) = tail {
        buf.extend(lines);
    }
    if !any && !matches!(q.output, Output::Cat | Output::Json) {
        buf.push_str("-- No entries --\n");
    }
    if !buf.is_empty() {
        send(buf.into_bytes());
    }
}

/// What a `/logs/<path>` names.
#[derive(Debug, PartialEq)]
pub enum Found {
    /// A directory's entries: name and whether it is a directory, by name.
    Dir(Vec<(String, bool)>),
    File(PathBuf),
}

/// Why a path is not served: the status and a line saying why.
#[derive(Debug, PartialEq)]
pub struct Refused(pub u16, pub String);

/// Resolve a path under `/logs/` (already percent-decoded, no leading `/`).
pub fn resolve(node: &Node, path: &str) -> Result<Found, Refused> {
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty() && *p != ".").collect();
    if parts.contains(&"..") {
        return Err(Refused(400, format!("{path}: a path under /logs/ may not contain ..\n")));
    }
    let var_log = node.var_log();
    let services = node_logs::log_dirs(&node.host_root);
    let Some((first, rest)) = parts.split_first() else {
        let mut entries = list(&var_log).unwrap_or_default();
        for (name, dir) in &services {
            if dir.is_dir() && !entries.iter().any(|(n, _)| n == name) {
                entries.push((name.clone(), true));
            }
        }
        entries.sort();
        return Ok(Found::Dir(entries));
    };
    let base = if var_log.join(first).symlink_metadata().is_ok() {
        var_log.join(first)
    } else if let Some(dir) = services.get(*first) {
        dir.clone()
    } else {
        return Err(Refused(404, format!("{path}: not in the host's /var/log nor a node service's log directory\n")));
    };
    let target = rest.iter().fold(base.clone(), |p, part| p.join(part));
    let not_found = |e: std::io::Error| Refused(404, format!("{path}: {e}\n"));
    // The directory the path starts in: for a host entry, the host's
    // /var/log itself, so a symlink in it may not lead out of it.
    let root = if base.starts_with(&var_log) { var_log.clone() } else { base.clone() };
    let root = root.canonicalize().map_err(not_found)?;
    let real = target.canonicalize().map_err(not_found)?;
    if !real.starts_with(&root) {
        return Err(Refused(403, format!("{path}: leads outside its log directory\n")));
    }
    if real.is_dir() {
        return list(&real).map(Found::Dir).map_err(not_found);
    }
    Ok(Found::File(real))
}

/// A directory's entries, by name.
fn list(dir: &Path) -> std::io::Result<Vec<(String, bool)>> {
    let mut out: Vec<(String, bool)> = std::fs::read_dir(dir)?
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_str()?.to_string();
            // Followed, as a listing shows where a link leads.
            let is_dir = std::fs::metadata(e.path()).is_ok_and(|m| m.is_dir());
            Some((name, is_dir))
        })
        .collect();
    out.sort();
    Ok(out)
}

/// A listing as Go's `http.FileServer` writes one, which is what oc reads:
/// it prints each `href`.
pub fn listing_html(entries: &[(String, bool)]) -> String {
    let mut out = String::from("<!doctype html>\n<meta name=\"viewport\" content=\"width=device-width\">\n<pre>\n");
    for (name, is_dir) in entries {
        let name = if *is_dir { format!("{name}/") } else { name.clone() };
        out.push_str(&format!("<a href=\"{}\">{}</a>\n", href(&name), html_escape(&name)));
    }
    out.push_str("</pre>\n");
    out
}

/// A name as a relative URL path: everything but unreserved characters and
/// `/` percent-encoded.
fn href(name: &str) -> String {
    let mut out = String::new();
    for b in name.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~/".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&#34;").replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// A node with two stormd services and one PID 1 runs directly.
    fn node() -> (tempfile::TempDir, Node) {
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        let bd = r.join("etc/stormpump/boot.d");
        std::fs::create_dir_all(&bd).unwrap();
        std::fs::write(
            bd.join("30-kube"),
            "volume felogs /logs/fastetcd\nspec fastetcd\n  mount felogs /var/log/stormd\n\
             volume nl /logs/rustkube-node\nspec rustkube-node\n  mount nl /var/log/stormd\n",
        )
        .unwrap();
        let fe = r.join("logs/fastetcd");
        std::fs::create_dir_all(&fe).unwrap();
        std::fs::write(
            fe.join("etcd.20260101T000000.failed.log"),
            "2026-11-22T09:00:00.000Z stderr error etcd died\n",
        )
        .unwrap();
        std::fs::write(fe.join("etcd.log"), "2026-11-22T10:00:00.000Z stdout info etcd up\n").unwrap();
        let rk = r.join("logs/rustkube-node");
        std::fs::create_dir_all(&rk).unwrap();
        std::fs::write(
            rk.join("kubelet.log"),
            "2026-11-22T10:00:01.500Z stdout info kubelet started\n2026-11-22T10:00:03.000Z stderr warn Node not ready\n",
        )
        .unwrap();
        std::fs::write(rk.join("kube-proxy.log"), "2026-11-22T10:00:02.000Z stdout info proxy up\n").unwrap();
        let runs = r.join("run/logs");
        std::fs::create_dir_all(&runs).unwrap();
        std::fs::write(runs.join("w7.log"), "engine ready\nserving\n").unwrap();
        // 2026-11-22T10:00:01Z
        let started = t("2026-11-22T10:00:01Z").timestamp();
        let assets = r.join("run/assets.json");
        std::fs::write(
            &assets,
            format!(r#"{{"assets":[{{"name":"stormblock","running":true,"runs":[{{"started_at":{started},"log":"w7.log"}}]}},{{"name":"fastetcd","running":true}}]}}"#),
        )
        .unwrap();
        let var_log = r.join("var/log");
        std::fs::create_dir_all(var_log.join("pods")).unwrap();
        std::fs::write(var_log.join("boot.log"), "booted\n").unwrap();
        let node = Node { host_root: r.into(), assets_json: assets, runs_dir: runs, name: "n1".into() };
        (root, node)
    }

    fn journal(node: &Node, pairs: &[(&str, &str)]) -> Result<String, String> {
        let pairs: Vec<(String, String)> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let q = Query::parse(&pairs, t("2026-11-22T12:00:00Z"), || Some(t("2026-11-22T08:00:00Z")))?;
        let chains = chains(node, &node.services(), &q.units)?;
        let mut out = Vec::new();
        write_journal(chains, &q, &node.name, |b| {
            out.extend(b);
            true
        });
        Ok(String::from_utf8(out).unwrap())
    }

    #[test]
    fn the_journal_is_every_service_merged_in_time_order() {
        let (_root, node) = node();
        assert_eq!(
            journal(&node, &[]).unwrap(),
            "Nov 22 09:00:00.000000 n1 fastetcd[etcd]: etcd died\n\
             Nov 22 10:00:00.000000 n1 fastetcd[etcd]: etcd up\n\
             Nov 22 10:00:01.000000 n1 stormblock: engine ready\n\
             Nov 22 10:00:01.000000 n1 stormblock: serving\n\
             Nov 22 10:00:01.500000 n1 rustkube-node[kubelet]: kubelet started\n\
             Nov 22 10:00:02.000000 n1 rustkube-node[kube-proxy]: proxy up\n\
             Nov 22 10:00:03.000000 n1 rustkube-node[kubelet]: Node not ready\n"
        );
    }

    #[test]
    fn a_unit_is_a_service_or_a_stormd_process() {
        let (_root, node) = node();
        // oc sends unit= and query= with the same value.
        let svc = journal(&node, &[("unit", "rustkube-node"), ("query", "rustkube-node")]).unwrap();
        assert_eq!(svc.lines().count(), 3, "{svc}");
        let kubelet = journal(&node, &[("unit", "kubelet.service")]).unwrap();
        assert_eq!(
            kubelet,
            "Nov 22 10:00:01.500000 n1 rustkube-node[kubelet]: kubelet started\n\
             Nov 22 10:00:03.000000 n1 rustkube-node[kubelet]: Node not ready\n"
        );
        let engine = journal(&node, &[("unit", "stormblock"), ("output", "cat")]).unwrap();
        assert_eq!(engine, "engine ready\nserving\n");
        let err = journal(&node, &[("unit", "sshd")]).unwrap_err();
        assert!(err.contains("no node service or stormd process sshd on n1"), "{err}");
        assert!(err.contains("fastetcd, rustkube-node, stormblock"), "{err}");
    }

    #[test]
    fn since_until_grep_tail_and_boot_filter() {
        let (_root, node) = node();
        let w = journal(&node, &[("since", "2026-11-22 10:00:01"), ("until", "2026-11-22T10:00:02Z"), ("output", "cat")]).unwrap();
        assert_eq!(w, "engine ready\nserving\nkubelet started\nproxy up\n");
        // oc's --grep defaults to case-sensitive=true.
        let g = journal(&node, &[("grep", "node"), ("pattern", "node"), ("case-sensitive", "true"), ("output", "cat")]).unwrap();
        assert_eq!(g, "");
        let g = journal(&node, &[("grep", "node"), ("case-sensitive", "false"), ("output", "cat")]).unwrap();
        assert_eq!(g, "Node not ready\n");
        // Smart case when not said.
        assert_eq!(journal(&node, &[("grep", "UP"), ("output", "cat")]).unwrap(), "");
        let tl = journal(&node, &[("tail", "2"), ("tailLines", "2"), ("output", "cat")]).unwrap();
        assert_eq!(tl, "proxy up\nNode not ready\n");
        // Booted at 08:00 in this test: everything is this boot's.
        assert_eq!(journal(&node, &[("boot", "0"), ("output", "cat")]).unwrap().lines().count(), 7);
        let since_boot = journal(&node, &[("boot", "0"), ("since", "-2h"), ("output", "cat")]).unwrap();
        assert_eq!(since_boot.lines().count(), 6, "since 10:00 wins over the boot at 08:00");
        assert!(journal(&node, &[("boot", "-1")]).unwrap_err().contains("not available"));
        assert_eq!(journal(&node, &[("since", "2027-01-01")]).unwrap(), "-- No entries --\n");
    }

    #[test]
    fn bad_options_are_refused_with_why() {
        let (_root, node) = node();
        for (k, v, want) in [
            ("since", "whenever", "not a time"),
            ("tail", "100001", "0 to 100000"),
            ("output", "verbose", "one of short, short-precise"),
            ("grep", "(", "grep=("),
            ("case-sensitive", "maybe", ""),
            ("boot", "x", "a boot offset"),
        ] {
            let mut pairs = vec![(k, v)];
            if k == "case-sensitive" {
                pairs.push(("grep", "a"));
            }
            let err = journal(&node, &pairs).unwrap_err();
            assert!(err.contains(want), "{k}={v}: {err}");
        }
    }

    #[test]
    fn outputs_are_journalctls() {
        let e = Entry {
            ts: t("2026-11-02T10:00:01.5Z"),
            service: "rustkube-node".into(),
            process: Some("kubelet".into()),
            stream: Some("stderr".into()),
            severity: Some("warn".into()),
            message: "Node not ready".into(),
        };
        assert_eq!(Output::Short.format(&e, "n1"), "Nov 02 10:00:01 n1 rustkube-node[kubelet]: Node not ready\n");
        assert_eq!(Output::ShortIso.format(&e, "n1"), "2026-11-02T10:00:01+00:00 n1 rustkube-node[kubelet]: Node not ready\n");
        assert_eq!(
            Output::ShortIsoPrecise.format(&e, "n1"),
            "2026-11-02T10:00:01.500000+00:00 n1 rustkube-node[kubelet]: Node not ready\n"
        );
        assert_eq!(Output::ShortUnix.format(&e, "n1"), "1793613601.500000 n1 rustkube-node[kubelet]: Node not ready\n");
        let j: serde_json::Value = serde_json::from_str(Output::Json.format(&e, "n1").trim()).unwrap();
        assert_eq!(j["__REALTIME_TIMESTAMP"], "1793613601500000");
        assert_eq!(j["PRIORITY"], "4");
        assert_eq!(j["_SYSTEMD_UNIT"], "rustkube-node");
        assert_eq!(j["_COMM"], "kubelet");
        assert_eq!(j["MESSAGE"], "Node not ready");
    }

    #[test]
    fn times_parse_as_journalctl_takes_them() {
        let now = t("2026-11-22T12:30:00Z");
        for (s, want) in [
            ("now", "2026-11-22T12:30:00Z"),
            ("today", "2026-11-22T00:00:00Z"),
            ("yesterday", "2026-11-21T00:00:00Z"),
            ("tomorrow", "2026-11-23T00:00:00Z"),
            ("-1h", "2026-11-22T11:30:00Z"),
            ("-1h30min", "2026-11-22T11:00:00Z"),
            ("+30min", "2026-11-22T13:00:00Z"),
            ("2 days ago", "2026-11-20T12:30:00Z"),
            ("2026-11-01", "2026-11-01T00:00:00Z"),
            ("2026-11-01 08:15", "2026-11-01T08:15:00Z"),
            ("2026-11-01 08:15:30 UTC", "2026-11-01T08:15:30Z"),
            ("2026-11-01T08:15:30.25Z", "2026-11-01T08:15:30.25Z"),
            ("2026-11-01T09:15:30+01:00", "2026-11-01T08:15:30Z"),
            ("08:00", "2026-11-22T08:00:00Z"),
        ] {
            assert_eq!(parse_time(s, now), Ok(t(want)), "{s}");
        }
        assert!(parse_time("-1fortnight", now).is_err());
        assert!(parse_time("", now).is_err());
    }

    #[test]
    fn files_are_the_hosts_var_log_and_each_services_directory() {
        let (_root, node) = node();
        assert_eq!(
            resolve(&node, ""),
            Ok(Found::Dir(vec![
                ("boot.log".into(), false),
                ("fastetcd".into(), true),
                ("pods".into(), true),
                ("rustkube-node".into(), true),
            ]))
        );
        assert_eq!(
            resolve(&node, "boot.log"),
            Ok(Found::File(node.host_root.join("var/log/boot.log").canonicalize().unwrap()))
        );
        assert_eq!(
            resolve(&node, "rustkube-node/"),
            Ok(Found::Dir(vec![("kube-proxy.log".into(), false), ("kubelet.log".into(), false)]))
        );
        assert_eq!(
            resolve(&node, "fastetcd/etcd.log"),
            Ok(Found::File(node.host_root.join("logs/fastetcd/etcd.log").canonicalize().unwrap()))
        );
        assert_eq!(resolve(&node, "nothing").unwrap_err().0, 404);
        assert_eq!(resolve(&node, "fastetcd/../rustkube-node/kubelet.log").unwrap_err().0, 400);
    }

    #[test]
    fn a_symlink_may_not_lead_out_of_its_directory() {
        let (_root, node) = node();
        let var_log = node.host_root.join("var/log");
        std::fs::write(node.host_root.join("secret"), "x").unwrap();
        std::os::unix::fs::symlink(node.host_root.join("secret"), var_log.join("leak")).unwrap();
        assert_eq!(resolve(&node, "leak").unwrap_err().0, 403);
        // Within /var/log is fine.
        std::os::unix::fs::symlink("boot.log", var_log.join("boot.current")).unwrap();
        assert!(matches!(resolve(&node, "boot.current"), Ok(Found::File(_))));
        // A service directory may not reach the rest of the host either.
        std::os::unix::fs::symlink(node.host_root.join("secret"), node.host_root.join("logs/fastetcd/x.log")).unwrap();
        assert_eq!(resolve(&node, "fastetcd/x.log").unwrap_err().0, 403);
    }

    #[test]
    fn a_listing_is_go_file_servers() {
        let html = listing_html(&[("a b&.log".into(), false), ("pods".into(), true)]);
        assert_eq!(
            html,
            "<!doctype html>\n<meta name=\"viewport\" content=\"width=device-width\">\n<pre>\n\
             <a href=\"a%20b%26.log\">a b&amp;.log</a>\n<a href=\"pods/\">pods/</a>\n</pre>\n"
        );
    }
}
