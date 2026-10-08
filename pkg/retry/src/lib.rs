//! One retry helper for every call that leaves the process (#211).
//!
//! The kubelet, kube-proxy and the test container all talk to services that
//! can be briefly away: the apiserver restarting, the node's stormblock engine
//! starting after the kubelet, the registry busy. A call that meets one of
//! those once used to fail its whole pass (or a whole test) on a single
//! timeout. Every call site now goes through here instead of an ad-hoc loop,
//! so the rules are the same everywhere:
//!
//! 1. **Bounded retry with backoff and jitter** for a transient failure
//!    ([`Class::Infra`]): a timeout, a connection refused or reset, HTTP 408,
//!    429, 500, 502, 503 or 504. The delay starts at [`Policy::first_delay`],
//!    doubles to [`Policy::max_delay`], and is jittered (half fixed, half
//!    random) so a fleet of nodes does not retry in step. A `Retry-After`
//!    (seconds) is honoured up to [`MAX_RETRY_AFTER`]. No new attempt starts
//!    once [`Policy::deadline`] has passed since the first, and there are at
//!    most [`Policy::attempts`].
//! 2. **No retry on a real answer** ([`Class::Real`]): any other 4xx or 5xx,
//!    a request that could not be built, a body that does not decode. Those go
//!    back to the caller at once.
//! 3. **Idempotency.** GET, HEAD, OPTIONS, PUT, DELETE and PATCH are repeated
//!    freely: the apiserver's PUT carries a resourceVersion, so a repeat after
//!    an applied write is a 409 (a real answer), not a second write. A POST is
//!    repeated only when it provably never reached the server (the connect
//!    failed) or the server said it did nothing (429), because a POST may have
//!    been applied before the answer was lost. A caller whose POST is safe to
//!    repeat (a create with a fixed name answers 409 the second time; a
//!    TokenReview changes nothing) says so with [`send_repeatable`].
//! 4. **Attempts are logged**: "succeeded on attempt 3 after 4.1 s" (info) or
//!    "gave up after 5 attempts / 61.0 s: <last error>" (warn), with the
//!    failure's class, so a flaky dependency shows in the log instead of being
//!    hidden by the retry.
//! 5. **Failures are classified**: [`class_of_error`] and [`class_of_status`]
//!    tell a caller (and a test runner) whether what came back is the
//!    infrastructure being away or a real refusal.
//!
//! A call that must not be retried here (a liveness probe, whose threshold is
//! its retry; a long-lived watch, which has its own reconnect loop) does not
//! use this crate and says why where it is made. `docs/retries.md` lists every
//! call site.

use std::fmt::Display;
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::time::{Duration, Instant};

use reqwest::{Method, RequestBuilder, Response, StatusCode};
use tracing::{debug, info, warn};

/// The longest `Retry-After` honoured; a longer one is cut to this.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);

/// How hard to retry one kind of dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// What is being called, for log lines ("apiserver", "stormblock").
    pub name: &'static str,
    /// The most attempts, the first included. 1 = never retried.
    pub attempts: u32,
    /// The delay after the first failure, before jitter.
    pub first_delay: Duration,
    /// The delay never grows past this (before jitter).
    pub max_delay: Duration,
    /// No attempt starts later than this after the first one. Each attempt is
    /// still bounded by its own request timeout.
    pub deadline: Duration,
}

impl Policy {
    /// The apiserver. Most calls are made from a reconcile pass that runs
    /// again on the next event, so this rides out a restart or a leader
    /// change (seconds) without holding a worker for long.
    pub const API: Policy = Policy {
        name: "apiserver",
        attempts: 4,
        first_delay: Duration::from_millis(250),
        max_delay: Duration::from_secs(4),
        deadline: Duration::from_secs(60),
    };

    /// The node's stormblock engine, on loopback: it answers in milliseconds
    /// when up, and is away only while it (re)starts.
    pub const ENGINE: Policy = Policy {
        name: "stormblock",
        attempts: 5,
        first_delay: Duration::from_millis(250),
        max_delay: Duration::from_secs(5),
        deadline: Duration::from_secs(90),
    };

    /// The node's registry (sbregistry), which answers 503 while it fetches
    /// a golden from the cluster.
    pub const REGISTRY: Policy = Policy {
        name: "registry",
        attempts: 4,
        first_delay: Duration::from_millis(500),
        max_delay: Duration::from_secs(5),
        deadline: Duration::from_secs(60),
    };

    /// Another node service read for information only (stormdrive's
    /// placement): a short budget, the next pass asks again.
    pub const PEER: Policy = Policy {
        name: "peer",
        attempts: 3,
        first_delay: Duration::from_millis(500),
        max_delay: Duration::from_secs(4),
        deadline: Duration::from_secs(20),
    };

    /// A gRPC plugin on a local socket (a CSI driver, the CRI runtime): away
    /// only while it restarts.
    pub const LOCAL: Policy = Policy {
        name: "local-rpc",
        attempts: 4,
        first_delay: Duration::from_millis(200),
        max_delay: Duration::from_secs(2),
        deadline: Duration::from_secs(20),
    };

    /// The same policy under another name, for log lines.
    pub const fn named(self, name: &'static str) -> Policy {
        Policy { name, ..self }
    }

    /// The delay after failed attempt `n` (1-based), before jitter:
    /// `first_delay × 2^(n−1)`, capped at `max_delay`.
    pub fn backoff(&self, n: u32) -> Duration {
        let shift = n.saturating_sub(1).min(20);
        self.first_delay.saturating_mul(1u32 << shift).min(self.max_delay)
    }

    /// [`Self::backoff`] with equal jitter: half of it fixed, half random.
    pub fn delay(&self, n: u32) -> Duration {
        jitter(self.backoff(n))
    }
}

/// `d/2 + random(0..=d/2)`.
fn jitter(d: Duration) -> Duration {
    let half = d / 2;
    let nanos = half.as_nanos() as u64;
    if nanos == 0 {
        return d;
    }
    // A fresh RandomState is randomly keyed: enough randomness for jitter
    // without a dependency.
    let r = std::collections::hash_map::RandomState::new().build_hasher().finish();
    half + Duration::from_nanos(r % (nanos + 1))
}

/// Whether a failure is the infrastructure being away (retried) or a real
/// answer (returned at once).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// Timeout, unreachable, reset, overloaded: worth asking again.
    Infra,
    /// The other side answered, and the answer is no.
    Real,
}

impl Class {
    pub fn as_str(self) -> &'static str {
        match self {
            Class::Infra => "infrastructure",
            Class::Real => "refused",
        }
    }
}

impl Display for Class {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The class of an HTTP status, or `None` for a success (1xx–3xx).
pub fn class_of_status(s: StatusCode) -> Option<Class> {
    if !(s.is_client_error() || s.is_server_error()) {
        return None;
    }
    Some(match s.as_u16() {
        408 | 429 | 500 | 502 | 503 | 504 => Class::Infra,
        _ => Class::Real,
    })
}

/// The class of a transport error.
pub fn class_of_error(e: &reqwest::Error) -> Class {
    if let Some(s) = e.status() {
        return class_of_status(s).unwrap_or(Class::Real);
    }
    if e.is_timeout() || e.is_connect() || e.is_request() || e.is_body() {
        Class::Infra
    } else {
        // Builder, redirect, decode: repeating the same call gets the same.
        Class::Real
    }
}

/// Whether a method may be repeated after a request that may have been
/// applied.
pub fn repeatable(m: &Method) -> bool {
    matches!(*m, Method::GET | Method::HEAD | Method::OPTIONS | Method::PUT | Method::DELETE | Method::PATCH)
}

/// A response's `Retry-After`, in seconds form, capped at [`MAX_RETRY_AFTER`].
pub fn retry_after(r: &Response) -> Option<Duration> {
    let v = r.headers().get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    let secs: u64 = v.trim().parse().ok()?;
    Some(Duration::from_secs(secs).min(MAX_RETRY_AFTER))
}

/// Send `req` under `policy`. Retries a transient failure as the crate's
/// rules say (a POST only when it never left, or on 429). Returns what the
/// last attempt got: a response (any status, the caller reads it as before)
/// or the transport error.
pub async fn send(req: RequestBuilder, policy: Policy) -> reqwest::Result<Response> {
    send_inner(req, policy, false).await
}

/// [`send`] for a POST the caller knows is safe to repeat: a create with a
/// fixed name (the repeat is a 409 the caller already reads as "exists"), a
/// review or token request that changes nothing.
pub async fn send_repeatable(req: RequestBuilder, policy: Policy) -> reqwest::Result<Response> {
    send_inner(req, policy, true).await
}

/// [`send`] and [`send_repeatable`] as methods, so a call site reads
/// `client.get(url).send_retrying(Policy::API).await` where it read
/// `.send().await`.
pub trait RetryExt {
    fn send_retrying(self, policy: Policy) -> impl Future<Output = reqwest::Result<Response>> + Send;
    fn send_repeatable(self, policy: Policy) -> impl Future<Output = reqwest::Result<Response>> + Send;
}

impl RetryExt for RequestBuilder {
    fn send_retrying(self, policy: Policy) -> impl Future<Output = reqwest::Result<Response>> + Send {
        send_inner(self, policy, false)
    }
    fn send_repeatable(self, policy: Policy) -> impl Future<Output = reqwest::Result<Response>> + Send {
        send_inner(self, policy, true)
    }
}

async fn send_inner(req: RequestBuilder, policy: Policy, repeatable_post: bool) -> reqwest::Result<Response> {
    // The method and URL, for the rules and the log; a body that cannot be
    // cloned (a stream) cannot be sent twice.
    let Some((method, what)) = req.try_clone().and_then(|r| r.build().ok()).map(|r| {
        let url = r.url();
        (r.method().clone(), format!("{} {}{}", r.method(), url.host_str().unwrap_or_default(), url.path()))
    }) else {
        return req.send().await;
    };
    let repeat = repeatable_post || repeatable(&method);
    let started = Instant::now();
    let mut attempt = 0u32;
    let template = req;
    loop {
        attempt += 1;
        let this = match template.try_clone() {
            Some(c) => c,
            None => return template.send().await,
        };
        let result = this.send().await;
        let (retry_ok, wait, last): (bool, Option<Duration>, String) = match &result {
            Ok(r) => match class_of_status(r.status()) {
                None | Some(Class::Real) => {
                    if attempt > 1 {
                        let how = if r.status().is_success() { "succeeded" } else { "answered" };
                        info!(
                            target: "retry",
                            call = %what, policy = policy.name,
                            "{what}: {how} ({}) on attempt {attempt} after {:.1} s",
                            r.status(), started.elapsed().as_secs_f64()
                        );
                    }
                    return result;
                }
                Some(Class::Infra) => {
                    let s = r.status();
                    (repeat || s == StatusCode::TOO_MANY_REQUESTS, retry_after(r), format!("HTTP {s}"))
                }
            },
            Err(e) => match class_of_error(e) {
                Class::Real => {
                    if attempt > 1 {
                        warn!(target: "retry", call = %what, policy = policy.name,
                              "{what}: refused on attempt {attempt} after {:.1} s: {e}", started.elapsed().as_secs_f64());
                    }
                    return result;
                }
                Class::Infra => (repeat || e.is_connect(), None, e.to_string()),
            },
        };
        if !retry_ok {
            warn!(
                target: "retry", call = %what, policy = policy.name, class = "infrastructure",
                "{what}: not retried ({last}): a {method} may have been applied before the answer was lost; \
                 its caller checks and asks again on its next pass"
            );
            return result;
        }
        let delay = wait.unwrap_or_else(|| policy.delay(attempt));
        if attempt >= policy.attempts || started.elapsed() + delay > policy.deadline {
            warn!(
                target: "retry", call = %what, policy = policy.name, class = "infrastructure",
                "{what}: gave up after {attempt} attempts / {:.1} s: {last}", started.elapsed().as_secs_f64()
            );
            return result;
        }
        debug!(target: "retry", call = %what, policy = policy.name, "{what}: attempt {attempt} failed ({last}); retrying in {delay:?}");
        drop(result);
        tokio::time::sleep(delay).await;
    }
}

/// Run `op` under `policy` for anything that is not a plain reqwest call
/// (gRPC to a CSI driver, another crate's client). `class` says which errors
/// are worth another attempt; `op` must be safe to repeat (the caller decides,
/// and says so where it calls this). Attempts are logged as for [`send`].
pub async fn with_backoff<T, E, F, Fut>(policy: Policy, what: &str, class: impl Fn(&E) -> Class, mut op: F) -> Result<T, E>
where
    E: Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let started = Instant::now();
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match op().await {
            Ok(v) => {
                if attempt > 1 {
                    info!(target: "retry", call = %what, policy = policy.name,
                          "{what}: succeeded on attempt {attempt} after {:.1} s", started.elapsed().as_secs_f64());
                }
                return Ok(v);
            }
            Err(e) => {
                if class(&e) == Class::Real {
                    if attempt > 1 {
                        warn!(target: "retry", call = %what, policy = policy.name,
                              "{what}: refused on attempt {attempt} after {:.1} s: {e}", started.elapsed().as_secs_f64());
                    }
                    return Err(e);
                }
                let delay = policy.delay(attempt);
                if attempt >= policy.attempts || started.elapsed() + delay > policy.deadline {
                    warn!(target: "retry", call = %what, policy = policy.name, class = "infrastructure",
                          "{what}: gave up after {attempt} attempts / {:.1} s: {e}", started.elapsed().as_secs_f64());
                    return Err(e);
                }
                debug!(target: "retry", call = %what, policy = policy.name, "{what}: attempt {attempt} failed ({e}); retrying in {delay:?}");
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// [`with_backoff`] for a blocking call (another crate's synchronous client,
/// already run off the async threads). Sleeps the calling thread between
/// attempts.
pub fn blocking<T, E, F>(policy: Policy, what: &str, class: impl Fn(&E) -> Class, mut op: F) -> Result<T, E>
where
    E: Display,
    F: FnMut() -> Result<T, E>,
{
    let started = Instant::now();
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match op() {
            Ok(v) => {
                if attempt > 1 {
                    info!(target: "retry", call = %what, policy = policy.name,
                          "{what}: succeeded on attempt {attempt} after {:.1} s", started.elapsed().as_secs_f64());
                }
                return Ok(v);
            }
            Err(e) => {
                if class(&e) == Class::Real {
                    return Err(e);
                }
                let delay = policy.delay(attempt);
                if attempt >= policy.attempts || started.elapsed() + delay > policy.deadline {
                    warn!(target: "retry", call = %what, policy = policy.name, class = "infrastructure",
                          "{what}: gave up after {attempt} attempts / {:.1} s: {e}", started.elapsed().as_secs_f64());
                    return Err(e);
                }
                debug!(target: "retry", call = %what, policy = policy.name, "{what}: attempt {attempt} failed ({e}); retrying in {delay:?}");
                std::thread::sleep(delay);
            }
        }
    }
}

/// The class of an error known only by its text: another crate's client that
/// hands back a string. Infrastructure when it reads as a transport failure
/// or a transient HTTP status; otherwise a real answer.
pub fn class_of_message(m: &str) -> Class {
    let m = m.to_ascii_lowercase();
    const INFRA: [&str; 14] = [
        "timed out", "timeout", "connection refused", "connection reset", "broken pipe",
        "error sending request", "error trying to connect", "unexpected eof", "dns error",
        " 408", " 429", " 502", " 503", " 504",
    ];
    if INFRA.iter().any(|w| m.contains(w)) || m.starts_with("408") || m.starts_with("429") || (m.starts_with("50") && !m.starts_with("501")) {
        Class::Infra
    } else {
        Class::Real
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A policy fast enough for tests.
    const FAST: Policy = Policy {
        name: "test",
        attempts: 5,
        first_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(40),
        deadline: Duration::from_secs(10),
    };

    /// One scripted answer: status, extra header lines, how long to sit first.
    #[derive(Clone)]
    struct Answer(u16, &'static str, Duration);

    /// An HTTP server that answers the `n`th request with `script[n]` (the
    /// last one repeats) and counts requests.
    async fn fake(script: Vec<Answer>) -> (String, Arc<AtomicUsize>) {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        let seen = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l.accept().await else { return };
                let n = count.fetch_add(1, Ordering::SeqCst);
                let a = script[n.min(script.len() - 1)].clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let mut got = Vec::new();
                    // Read the head (and a small body) before answering.
                    while !got.windows(4).any(|w| w == b"\r\n\r\n") {
                        match s.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(k) => got.extend_from_slice(&buf[..k]),
                        }
                    }
                    tokio::time::sleep(a.2).await;
                    let r = format!("HTTP/1.1 {} X\r\ncontent-length: 2\r\nconnection: close\r\n{}\r\nok", a.0, a.1);
                    let _ = s.write_all(r.as_bytes()).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        (url, seen)
    }

    fn client() -> reqwest::Client {
        reqwest::Client::builder().timeout(Duration::from_millis(300)).build().unwrap()
    }

    const OK: Answer = Answer(200, "", Duration::ZERO);
    const UNAVAILABLE: Answer = Answer(503, "", Duration::ZERO);

    #[test]
    fn backoff_doubles_to_the_cap_and_jitter_stays_within_it() {
        assert_eq!(FAST.backoff(1), Duration::from_millis(10));
        assert_eq!(FAST.backoff(2), Duration::from_millis(20));
        assert_eq!(FAST.backoff(3), Duration::from_millis(40));
        assert_eq!(FAST.backoff(9), Duration::from_millis(40));
        assert_eq!(Policy::API.backoff(100), Policy::API.max_delay);
        for n in 1..6 {
            let d = FAST.delay(n);
            assert!(d >= FAST.backoff(n) / 2 && d <= FAST.backoff(n), "{d:?}");
        }
    }

    #[test]
    fn statuses_and_methods_are_classified() {
        for s in [408, 429, 500, 502, 503, 504] {
            assert_eq!(class_of_status(StatusCode::from_u16(s).unwrap()), Some(Class::Infra), "{s}");
        }
        for s in [400, 401, 403, 404, 409, 422, 501] {
            assert_eq!(class_of_status(StatusCode::from_u16(s).unwrap()), Some(Class::Real), "{s}");
        }
        assert_eq!(class_of_status(StatusCode::OK), None);
        assert!(repeatable(&Method::GET) && repeatable(&Method::PUT) && repeatable(&Method::PATCH));
        assert!(!repeatable(&Method::POST));
    }

    /// Every policy rides out N transient failures then succeeds, within its
    /// attempts.
    #[tokio::test]
    async fn each_policy_survives_failures_then_succeeds() {
        for p in [Policy::API, Policy::ENGINE, Policy::REGISTRY, Policy::PEER, Policy::LOCAL] {
            // Same shape, test-sized delays.
            let p = Policy { first_delay: Duration::from_millis(5), max_delay: Duration::from_millis(20), ..p };
            let fails = (p.attempts - 1) as usize;
            let mut script = vec![UNAVAILABLE; fails];
            script.push(OK);
            let (url, seen) = fake(script).await;
            let r = send(client().get(format!("{url}/x")), p).await.unwrap();
            assert_eq!(r.status(), 200, "{}", p.name);
            assert_eq!(seen.load(Ordering::SeqCst), fails + 1, "{}", p.name);
        }
    }

    #[tokio::test]
    async fn a_real_answer_is_not_retried() {
        let (url, seen) = fake(vec![Answer(404, "", Duration::ZERO)]).await;
        let r = send(client().get(format!("{url}/x")), FAST).await.unwrap();
        assert_eq!(r.status(), 404);
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn gives_up_after_the_attempts_with_the_last_answer() {
        let (url, seen) = fake(vec![UNAVAILABLE]).await;
        let r = send(client().delete(format!("{url}/x")), FAST).await.unwrap();
        assert_eq!(r.status(), 503);
        assert_eq!(seen.load(Ordering::SeqCst), FAST.attempts as usize);
    }

    #[tokio::test]
    async fn the_deadline_caps_the_attempts() {
        let (url, seen) = fake(vec![UNAVAILABLE]).await;
        let p = Policy { attempts: 100, first_delay: Duration::from_millis(50), max_delay: Duration::from_millis(50), deadline: Duration::from_millis(120), ..FAST };
        let started = Instant::now();
        let r = send(client().get(format!("{url}/x")), p).await.unwrap();
        assert_eq!(r.status(), 503);
        assert!(seen.load(Ordering::SeqCst) < 6, "{}", seen.load(Ordering::SeqCst));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn a_timeout_is_retried_for_a_get() {
        let (url, seen) = fake(vec![Answer(200, "", Duration::from_secs(2)), OK]).await;
        let r = send(client().get(format!("{url}/x")), FAST).await.unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(seen.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_post_that_may_have_landed_is_not_repeated() {
        // A 503 after a POST: maybe applied, so not sent again.
        let (url, seen) = fake(vec![UNAVAILABLE, OK]).await;
        let r = send(client().post(format!("{url}/x")).body("{}"), FAST).await.unwrap();
        assert_eq!(r.status(), 503);
        assert_eq!(seen.load(Ordering::SeqCst), 1);
        // A 429 said it did nothing: sent again.
        let (url, seen) = fake(vec![Answer(429, "", Duration::ZERO), OK]).await;
        let r = send(client().post(format!("{url}/x")).body("{}"), FAST).await.unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(seen.load(Ordering::SeqCst), 2);
        // A caller that knows the POST is safe to repeat.
        let (url, seen) = fake(vec![UNAVAILABLE, OK]).await;
        let r = send_repeatable(client().post(format!("{url}/x")).body("{}"), FAST).await.unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(seen.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_post_whose_connect_failed_is_repeated() {
        // A port with nothing on it until the second attempt.
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        let seen = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&seen);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(15)).await;
            let l = tokio::net::TcpListener::bind(addr).await.unwrap();
            let (mut s, _) = l.accept().await.unwrap();
            count.fetch_add(1, Ordering::SeqCst);
            let mut buf = vec![0u8; 4096];
            let _ = s.read(&mut buf).await;
            let _ = s.write_all(b"HTTP/1.1 201 X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").await;
        });
        let p = Policy { attempts: 20, first_delay: Duration::from_millis(10), max_delay: Duration::from_millis(10), ..FAST };
        let r = send(client().post(format!("http://{addr}/x")).body("{}"), p).await.unwrap();
        assert_eq!(r.status(), 201);
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_after_is_honoured() {
        let (url, seen) = fake(vec![Answer(503, "retry-after: 1\r\n", Duration::ZERO), OK]).await;
        let started = Instant::now();
        let r = send(client().get(format!("{url}/x")), FAST).await.unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(seen.load(Ordering::SeqCst), 2);
        assert!(started.elapsed() >= Duration::from_millis(950), "{:?}", started.elapsed());
    }

    #[tokio::test]
    async fn with_backoff_retries_infra_and_returns_real_at_once() {
        let calls = AtomicUsize::new(0);
        let r: Result<u32, String> = with_backoff(FAST, "op", |_| Class::Infra, || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move { if n < 3 { Err(format!("down {n}")) } else { Ok(7) } }
        })
        .await;
        assert_eq!(r, Ok(7));
        assert_eq!(calls.load(Ordering::SeqCst), 4);

        let calls = AtomicUsize::new(0);
        let r: Result<u32, String> = with_backoff(FAST, "op", |_| Class::Real, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err("no".to_string()) }
        })
        .await;
        assert_eq!(r, Err("no".into()));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let calls = AtomicUsize::new(0);
        let r: Result<u32, String> = with_backoff(FAST, "op", |_| Class::Infra, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err("down".to_string()) }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), FAST.attempts as usize);
    }

    #[test]
    fn blocking_retries_infra_then_succeeds() {
        let mut n = 0;
        let r: Result<u32, String> = blocking(FAST, "op", |e: &String| class_of_message(e), || {
            n += 1;
            if n < 3 { Err("error sending request: connection refused".into()) } else { Ok(1) }
        });
        assert_eq!(r, Ok(1));
        assert_eq!(n, 3);
        let mut n = 0;
        let r: Result<u32, String> = blocking(FAST, "op", |e: &String| class_of_message(e), || {
            n += 1;
            Err("409 Conflict: volume exists".into())
        });
        assert!(r.is_err());
        assert_eq!(n, 1);
    }

    #[test]
    fn messages_are_classified() {
        for m in ["operation timed out", "Connection refused (os error 111)", "HTTP 503 Service Unavailable", "502 Bad Gateway"] {
            assert_eq!(class_of_message(m), Class::Infra, "{m}");
        }
        for m in ["404 Not Found: no such group", "volume is not sealed", "501 Not Implemented", "HTTP 400"] {
            assert_eq!(class_of_message(m), Class::Real, "{m}");
        }
    }
}
