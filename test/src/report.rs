//! Results as the test standard wants them: one JSON object per test on
//! stdout, a summary line last, the same lines under `/results`, and an exit
//! code of 0 (all passed), 1 (a test failed) or 2 (could not run).

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::time::Instant;

/// How one test came out.
pub enum Outcome {
    Pass(String),
    Fail(String),
    /// Not applicable here; never counted as a pass.
    Skip(String),
    /// The test could not run (the apiserver refused, a variable the runner
    /// must set is missing). Reported as a failure, exits 2.
    Infra(String),
}

pub struct Report {
    pass: u32,
    fail: u32,
    skip: u32,
    infra: u32,
    file: Option<File>,
    /// Every test's name and status, in order.
    pub outcomes: Vec<(String, &'static str)>,
}

impl Default for Report {
    fn default() -> Self {
        Self::new()
    }
}

impl Report {
    pub fn new() -> Report {
        // `/results` is the runner's volume; without it (a hand run) the
        // lines still go to stdout.
        let file = OpenOptions::new().create(true).append(true).open("/results/results.jsonl").ok();
        Report { pass: 0, fail: 0, skip: 0, infra: 0, file, outcomes: Vec::new() }
    }

    /// Run one test, timed, and record it. Returns whether it passed.
    pub async fn run(&mut self, name: &str, f: impl Future<Output = Outcome>) -> bool {
        let t = Instant::now();
        let outcome = f.await;
        let ms = t.elapsed().as_millis();
        self.record(name, outcome, ms, None)
    }

    /// Record a result; `extra` is a JSON object's members, added verbatim.
    pub fn record(&mut self, name: &str, outcome: Outcome, ms: u128, extra: Option<&str>) -> bool {
        let (status, detail) = match outcome {
            Outcome::Pass(d) => {
                self.pass += 1;
                ("pass", d)
            }
            Outcome::Fail(d) => {
                self.fail += 1;
                ("fail", d)
            }
            Outcome::Skip(d) => {
                self.skip += 1;
                ("skip", d)
            }
            Outcome::Infra(d) => {
                self.infra += 1;
                ("fail", format!("could not run: {d}"))
            }
        };
        let extra = extra.map(|e| format!(", {e}")).unwrap_or_default();
        self.line(&format!(
            "{{\"test\": {}, \"status\": \"{status}\", \"ms\": {ms}, \"detail\": {}{extra}}}",
            json(name),
            json(&detail)
        ));
        self.outcomes.push((name.to_string(), status));
        status == "pass"
    }

    fn line(&mut self, l: &str) {
        println!("{l}");
        if let Some(f) = &mut self.file {
            let _ = writeln!(f, "{l}");
        }
    }

    /// Print the summary and return the exit code. A real failure outranks
    /// an infrastructure one: it is the more useful thing to be told.
    pub fn finish(&mut self) -> i32 {
        let fail = self.fail + self.infra;
        let s = format!(
            "{{\"summary\": {{\"pass\": {}, \"fail\": {fail}, \"skip\": {}}}}}",
            self.pass, self.skip
        );
        self.line(&s);
        match (self.fail, self.infra) {
            (0, 0) => 0,
            (0, _) => 2,
            _ => 1,
        }
    }
}

/// A JSON string literal.
pub fn json(s: &str) -> String {
    serde_json::Value::String(s.to_string()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_strings_escape() {
        assert_eq!(json("a\"b\\c\nd"), r#""a\"b\\c\nd""#);
        assert_eq!(json("em — dash"), "\"em — dash\"");
    }

    #[test]
    fn exit_codes_follow_the_standard() {
        let mut r = Report { pass: 0, fail: 0, skip: 0, infra: 0, file: None, outcomes: vec![] };
        r.record("a", Outcome::Pass(String::new()), 0, None);
        r.record("b", Outcome::Skip(String::new()), 0, None);
        assert_eq!(r.finish(), 0);
        r.record("c", Outcome::Infra(String::new()), 0, None);
        assert_eq!(r.finish(), 2);
        r.record("d", Outcome::Fail(String::new()), 0, None);
        assert_eq!(r.finish(), 1);
        assert_eq!(r.outcomes[2], ("c".to_string(), "fail"));
    }
}
