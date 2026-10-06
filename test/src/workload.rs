//! The workload pods' side. The suites run this same image in pods that hold
//! a claim, with one of these argument lists (so no other image is needed):
//!
//! - `fill <path> <seed> <bytes>`: write a seeded pattern, fsync, read it
//!   back and compare.
//! - `verify <path> <seed> <bytes>`: read the pattern back and compare.
//! - `size-at-least <path> <bytes>`: the filesystem (a directory) or device
//!   (anything else) is at least that large.
//! - `sized <path> <seed> <bytes> <lo> <hi>`: `fill`, then the filesystem's
//!   size (what `df` reports) is more than `lo` and at most `hi`: the claim
//!   got the size class it rounded to, not the one below or above.
//!
//! And for the pod cases (#61), which need no claim:
//!
//! - `echo <text>`: print it, exit 0.
//! - `exit <code>`: exit with that code.
//! - `sleep <secs>`: sleep; SIGTERM ends it with 0 (PID 1 in a container
//!   ignores a signal it has no handler for, which would turn every delete
//!   into a wait for the grace period).
//! - `fail-once <dir>`: exit 1 the first time (leaving a marker in `dir`, an
//!   emptyDir that outlives the container), 0 after.
//! - `write-file <path> <text>`, `expect-file <path> <text>`: write, or check
//!   the file holds exactly `text`.
//! - `expect-env <name> <value>`: the variable is set to exactly `value`.
//!
//! A directory `path` means a filesystem volume: the data goes in
//! `<path>/rustkube-node-test.bin`. Anything else is a raw block device,
//! written from offset 0. Exit 0 on success, 1 on a mismatch or error; one
//! JSON line on stdout either way, also written as the termination message.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const CHUNK: usize = 1 << 20;

/// Whether `args` (without the program name) is a workload invocation.
pub fn is_workload(args: &[String]) -> bool {
    matches!(
        args.first().map(String::as_str),
        Some(
            "fill" | "verify" | "size-at-least" | "sized" | "echo" | "exit" | "sleep" | "fail-once"
                | "write-file" | "expect-file" | "expect-env"
        )
    )
}

/// Run a workload invocation; returns the exit code.
pub fn main(args: &[String]) -> i32 {
    if args[0] == "exit" {
        let code = args.get(1).and_then(|c| c.parse().ok()).unwrap_or(1);
        println!("exiting {code} as asked");
        return code;
    }
    if args[0] == "sleep" {
        let secs = args.get(1).and_then(|c| c.parse().ok()).unwrap_or(3600);
        // SAFETY: the handler only calls _exit, which is async-signal-safe.
        unsafe { libc::signal(libc::SIGTERM, on_term as *const () as libc::sighandler_t) };
        println!("sleeping {secs} s");
        std::thread::sleep(std::time::Duration::from_secs(secs));
        return 0;
    }
    let result = run(args);
    let (code, line) = match &result {
        Ok(d) => (0, format!("{{\"workload\": {}, \"ok\": true, \"detail\": {}}}", crate::report::json(&args.join(" ")), crate::report::json(d))),
        Err(e) => (1, format!("{{\"workload\": {}, \"ok\": false, \"detail\": {}}}", crate::report::json(&args.join(" ")), crate::report::json(e))),
    };
    println!("{line}");
    let _ = std::fs::write("/dev/termination-log", &line);
    code
}

extern "C" fn on_term(_: libc::c_int) {
    // SAFETY: _exit is async-signal-safe.
    unsafe { libc::_exit(0) }
}

fn run(args: &[String]) -> Result<String, String> {
    let arg = |i: usize, what: &str| args.get(i).cloned().ok_or_else(|| format!("missing {what}"));
    let num = |i: usize, what: &str| arg(i, what)?.parse::<u64>().map_err(|e| format!("{what}: {e}"));
    match args[0].as_str() {
        "fill" => {
            let (p, seed, n) = (target(&arg(1, "path")?), num(2, "seed")?, num(3, "bytes")?);
            fill(&p, seed, n)?;
            verify(&p, seed, n)?;
            Ok(format!("wrote and read back {n} bytes at {}", p.display()))
        }
        "verify" => {
            let (p, seed, n) = (target(&arg(1, "path")?), num(2, "seed")?, num(3, "bytes")?);
            verify(&p, seed, n)?;
            Ok(format!("read back {n} bytes at {}", p.display()))
        }
        "size-at-least" => {
            let (p, want) = (PathBuf::from(arg(1, "path")?), num(2, "bytes")?);
            let have = size(&p)?;
            if have >= want {
                Ok(format!("{} is {have} bytes (>= {want})", p.display()))
            } else {
                Err(format!("{} is {have} bytes, wanted >= {want}", p.display()))
            }
        }
        "sized" => {
            let (dir, seed, n) = (PathBuf::from(arg(1, "path")?), num(2, "seed")?, num(3, "bytes")?);
            let (lo, hi) = (num(4, "lo")?, num(5, "hi")?);
            let p = target(&arg(1, "path")?);
            fill(&p, seed, n)?;
            verify(&p, seed, n)?;
            let have = size(&dir)?;
            if have > lo && have <= hi {
                Ok(format!("wrote and read back {n} bytes; {} is {have} bytes, in ({lo}, {hi}]", dir.display()))
            } else {
                Err(format!("{} is {have} bytes, not in ({lo}, {hi}]", dir.display()))
            }
        }
        "echo" => Ok(args[1..].join(" ")),
        "fail-once" => {
            let marker = PathBuf::from(arg(1, "dir")?).join("rustkube-node-test.ran");
            if marker.exists() {
                Ok(format!("second run: {} is there", marker.display()))
            } else {
                std::fs::write(&marker, b"1").map_err(|e| format!("write {}: {e}", marker.display()))?;
                Err("first run: failing on purpose".into())
            }
        }
        "write-file" => {
            let (p, text) = (arg(1, "path")?, arg(2, "text")?);
            std::fs::write(&p, &text).map_err(|e| format!("write {p}: {e}"))?;
            Ok(format!("wrote {p}"))
        }
        "expect-file" => {
            let (p, text) = (arg(1, "path")?, arg(2, "text")?);
            let have = std::fs::read_to_string(&p).map_err(|e| format!("read {p}: {e}"))?;
            if have == text { Ok(format!("{p} holds {text:?}")) } else { Err(format!("{p} holds {have:?}, not {text:?}")) }
        }
        "expect-env" => {
            let (k, v) = (arg(1, "name")?, arg(2, "value")?);
            match std::env::var(&k) {
                Ok(have) if have == v => Ok(format!("{k}={v}")),
                Ok(have) => Err(format!("{k}={have:?}, not {v:?}")),
                Err(_) => Err(format!("{k} is not set")),
            }
        }
        other => Err(format!("unknown workload {other:?}")),
    }
}

fn target(p: &str) -> PathBuf {
    let p = PathBuf::from(p);
    if p.is_dir() { p.join("rustkube-node-test.bin") } else { p }
}

/// The pattern: a xorshift64 stream from `seed`, so every claim's data is
/// distinct and a stale or swapped volume never reads back as correct.
pub fn pattern(seed: u64, offset: u64, buf: &mut [u8]) {
    // Each 8-byte word is a function of (seed, word index), so any chunk can
    // be produced without the ones before it.
    for (i, b) in buf.iter_mut().enumerate() {
        let pos = offset + i as u64;
        let word = mix(seed ^ (pos / 8).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        *b = (word >> ((pos % 8) * 8)) as u8;
    }
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

fn fill(p: &Path, seed: u64, n: u64) -> Result<(), String> {
    let mut f = OpenOptions::new().create(true).write(true).truncate(false).open(p).map_err(|e| format!("open {}: {e}", p.display()))?;
    let mut buf = vec![0u8; CHUNK];
    let mut off = 0u64;
    while off < n {
        let len = CHUNK.min((n - off) as usize);
        pattern(seed, off, &mut buf[..len]);
        f.write_all(&buf[..len]).map_err(|e| format!("write {} at {off}: {e}", p.display()))?;
        off += len as u64;
    }
    f.sync_all().map_err(|e| format!("fsync {}: {e}", p.display()))
}

fn verify(p: &Path, seed: u64, n: u64) -> Result<(), String> {
    let mut f = File::open(p).map_err(|e| format!("open {}: {e}", p.display()))?;
    let (mut got, mut want) = (vec![0u8; CHUNK], vec![0u8; CHUNK]);
    let mut off = 0u64;
    while off < n {
        let len = CHUNK.min((n - off) as usize);
        f.read_exact(&mut got[..len]).map_err(|e| format!("read {} at {off}: {e}", p.display()))?;
        pattern(seed, off, &mut want[..len]);
        if let Some(i) = (0..len).find(|&i| got[i] != want[i]) {
            return Err(format!("{} differs at byte {}", p.display(), off + i as u64));
        }
        off += len as u64;
    }
    Ok(())
}

fn size(p: &Path) -> Result<u64, String> {
    if p.is_dir() {
        let c = std::ffi::CString::new(p.as_os_str().to_string_lossy().as_bytes()).map_err(|e| e.to_string())?;
        let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: `c` is a valid NUL-terminated path, `st` a valid out-pointer.
        if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
            return Err(format!("statvfs {}: {}", p.display(), std::io::Error::last_os_error()));
        }
        Ok(st.f_blocks as u64 * st.f_frsize as u64)
    } else {
        let mut f = File::open(p).map_err(|e| format!("open {}: {e}", p.display()))?;
        f.seek(SeekFrom::End(0)).map_err(|e| format!("seek {}: {e}", p.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("rknt-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn pattern_is_position_addressable() {
        let mut whole = vec![0u8; 100];
        pattern(7, 0, &mut whole);
        let mut part = vec![0u8; 37];
        pattern(7, 13, &mut part);
        assert_eq!(&whole[13..50], &part[..]);
        let mut other = vec![0u8; 100];
        pattern(8, 0, &mut other);
        assert_ne!(whole, other);
    }

    #[test]
    fn fill_then_verify_in_a_directory() {
        let d = tmp("dir");
        let a = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let dir = d.to_string_lossy().to_string();
        assert_eq!(main(&a(&["fill", &dir, "3", &(CHUNK as u64 * 2 + 5).to_string()])), 0);
        assert_eq!(main(&a(&["verify", &dir, "3", &(CHUNK as u64 * 2 + 5).to_string()])), 0);
        assert_eq!(main(&a(&["verify", &dir, "4", "100"])), 1);
        assert_eq!(main(&a(&["size-at-least", &dir, "1"])), 0);
        assert_eq!(main(&a(&["size-at-least", &dir, &u64::MAX.to_string()])), 1);
        std::fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn a_plain_file_stands_in_for_a_device() {
        let d = tmp("dev");
        let f = d.join("dev");
        std::fs::write(&f, vec![0u8; 4096]).unwrap();
        let f = f.to_string_lossy().to_string();
        let a = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(main(&a(&["fill", &f, "9", "4096"])), 0);
        assert_eq!(main(&a(&["size-at-least", &f, "4096"])), 0);
        assert_eq!(main(&a(&["size-at-least", &f, "4097"])), 1);
        std::fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn sized_checks_the_range() {
        let d = tmp("sized");
        let a = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let dir = d.to_string_lossy().to_string();
        let have = size(&d).unwrap();
        assert_eq!(main(&a(&["sized", &dir, "5", "4096", "0", &have.to_string()])), 0);
        // Exclusive below, inclusive above.
        assert_eq!(main(&a(&["sized", &dir, "5", "4096", &have.to_string(), &u64::MAX.to_string()])), 1);
        assert_eq!(main(&a(&["sized", &dir, "5", "4096", "0", &(have - 1).to_string()])), 1);
        std::fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn pod_case_modes() {
        let d = tmp("modes");
        let a = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let dir = d.to_string_lossy().to_string();
        assert_eq!(main(&a(&["echo", "hello", "there"])), 0);
        assert_eq!(main(&a(&["exit", "3"])), 3);
        assert_eq!(main(&a(&["sleep", "0"])), 0);
        assert_eq!(main(&a(&["fail-once", &dir])), 1);
        assert_eq!(main(&a(&["fail-once", &dir])), 0);
        let f = d.join("f").to_string_lossy().to_string();
        assert_eq!(main(&a(&["expect-file", &f, "x"])), 1);
        assert_eq!(main(&a(&["write-file", &f, "x"])), 0);
        assert_eq!(main(&a(&["expect-file", &f, "x"])), 0);
        assert_eq!(main(&a(&["expect-file", &f, "y"])), 1);
        assert_eq!(main(&a(&["expect-env", "PATH", &std::env::var("PATH").unwrap()])), 0);
        assert_eq!(main(&a(&["expect-env", "RKNT_UNSET_VAR", "x"])), 1);
        std::fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn workload_dispatch() {
        assert!(is_workload(&["fill".into()]));
        assert!(is_workload(&["sized".into()]));
        assert!(!is_workload(&[]));
        assert!(!is_workload(&["short".into()]));
    }
}
