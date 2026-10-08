//! Apiserver client for kube-proxy (rustkube-node#145).
//!
//! A stormcos apiserver serves TLS from the node CA on :6443 and wants a
//! bearer token, so the client trusts a CA file as its root and sends
//! `Authorization: Bearer` read from a token file. Both default to the
//! in-cluster ServiceAccount paths when those exist, which is what a
//! DaemonSet pod gets. The token file is read again on every request:
//! projected ServiceAccount tokens are rotated in place.
//!
//! A list is only a list when the apiserver says so: a non-2xx answer, or a
//! body with no `items`, is an error, never an empty set (an empty set would
//! remove every Service's rules).

use retry::RetryExt;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;
use tracing::warn;

/// The in-cluster ServiceAccount CA.
pub const SA_CA_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
/// The in-cluster ServiceAccount token.
pub const SA_TOKEN_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";

/// Where the client's credentials come from.
#[derive(Debug, Clone, Default)]
pub struct ApiAuth {
    /// PEM CA trusted as the apiserver's root.
    pub ca_file: Option<PathBuf>,
    /// File holding the bearer token (re-read on every request).
    pub token_file: Option<PathBuf>,
}

impl ApiAuth {
    /// Resolve explicit paths, else the in-cluster defaults that exist.
    /// An explicit path must exist; a missing default is simply not used
    /// (a plaintext dev apiserver needs neither).
    pub fn resolve(ca_file: Option<PathBuf>, token_file: Option<PathBuf>) -> anyhow::Result<Self> {
        Self::resolve_with(ca_file, token_file, Path::new(SA_CA_FILE), Path::new(SA_TOKEN_FILE))
    }

    fn resolve_with(
        ca_file: Option<PathBuf>,
        token_file: Option<PathBuf>,
        default_ca: &Path,
        default_token: &Path,
    ) -> anyhow::Result<Self> {
        let pick = |given: Option<PathBuf>, default: &Path, what: &str| -> anyhow::Result<Option<PathBuf>> {
            match given {
                Some(p) if p.exists() => Ok(Some(p)),
                Some(p) => anyhow::bail!("{what} {} does not exist", p.display()),
                None if default.exists() => Ok(Some(default.to_path_buf())),
                None => Ok(None),
            }
        };
        Ok(Self {
            ca_file: pick(ca_file, default_ca, "CA file")?,
            token_file: pick(token_file, default_token, "token file")?,
        })
    }
}

/// An apiserver client with a fixed base URL.
pub struct ApiClient {
    base: String,
    http: reqwest::Client,
    token_file: Option<PathBuf>,
    /// The last token read; kept when a later read fails (mid-rotation).
    token: Mutex<Option<String>>,
}

impl ApiClient {
    pub fn new(base: &str, auth: &ApiAuth) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30));
        if let Some(ca) = &auth.ca_file {
            let pem = std::fs::read(ca)
                .map_err(|e| anyhow::anyhow!("reading CA file {}: {e}", ca.display()))?;
            let cert = reqwest::Certificate::from_pem(&pem)
                .map_err(|e| anyhow::anyhow!("CA file {} not usable: {e}", ca.display()))?;
            builder = builder.add_root_certificate(cert);
        }
        let http = builder
            .build()
            .map_err(|e| anyhow::anyhow!("building the apiserver client: {e}"))?;
        let client = Self {
            base: base.trim_end_matches('/').to_string(),
            http,
            token_file: auth.token_file.clone(),
            token: Mutex::new(None),
        };
        if let Some(f) = &client.token_file {
            // A token file that cannot be read at start is a configuration
            // error, not something to discover as a 401 every five seconds.
            let t = read_token(f)?;
            *client.token.lock().unwrap() = Some(t);
        }
        Ok(client)
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    /// The current token: the file as it is now, else the last one read.
    fn current_token(&self) -> Option<String> {
        let f = self.token_file.as_ref()?;
        let mut last = self.token.lock().unwrap();
        match read_token(f) {
            Ok(t) => *last = Some(t),
            Err(e) => warn!("{e}; using the last token read"),
        }
        last.clone()
    }

    /// GET a list (`/api/v1/services`, …) and return its `items`.
    pub async fn list(&self, path: &str) -> anyhow::Result<Vec<Value>> {
        let url = format!("{}{path}", self.base);
        let mut req = self.http.get(&url).header("Accept", "application/json");
        if let Some(t) = self.current_token() {
            req = req.bearer_auth(t);
        }
        let resp = req
            .send_retrying(retry::Policy::API)
            .await
            .map_err(|e| anyhow::anyhow!("GET {url}: {e}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {url}: {status}: {}", body.trim());
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("GET {url}: not JSON: {e}"))?;
        match body.get("items") {
            Some(Value::Array(items)) => Ok(items.clone()),
            // `"items": null` is how an empty list can be encoded.
            Some(Value::Null) => Ok(Vec::new()),
            _ => anyhow::bail!("GET {url}: answer has no items: {body}"),
        }
    }
}

fn read_token(f: &Path) -> anyhow::Result<String> {
    let t = std::fs::read_to_string(f)
        .map_err(|e| anyhow::anyhow!("reading token file {}: {e}", f.display()))?;
    let t = t.trim().to_string();
    if t.is_empty() {
        anyhow::bail!("token file {} is empty", f.display());
    }
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    /// One-request HTTP server: answers `status`/`body`, returns the request head.
    fn serve_once(status: &str, body: &str) -> (String, std::thread::JoinHandle<String>) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        let (status, body) = (status.to_string(), body.to_string());
        let h = std::thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            let mut head = String::new();
            let mut r = BufReader::new(s.try_clone().unwrap());
            loop {
                let mut line = String::new();
                r.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                head.push_str(&line);
            }
            write!(
                s,
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
            head
        });
        (url, h)
    }

    #[test]
    fn explicit_missing_file_is_an_error_default_missing_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let none = dir.path().join("absent");
        let a = ApiAuth::resolve_with(None, None, &none, &none).unwrap();
        assert!(a.ca_file.is_none() && a.token_file.is_none());
        assert!(ApiAuth::resolve_with(Some(none.clone()), None, &none, &none).is_err());
        assert!(ApiAuth::resolve_with(None, Some(none.clone()), &none, &none).is_err());
    }

    #[test]
    fn present_defaults_are_used() {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path().join("ca.crt");
        let tok = dir.path().join("token");
        std::fs::write(&ca, "x").unwrap();
        std::fs::write(&tok, "t").unwrap();
        let a = ApiAuth::resolve_with(None, None, &ca, &tok).unwrap();
        assert_eq!(a.ca_file.as_deref(), Some(ca.as_path()));
        assert_eq!(a.token_file.as_deref(), Some(tok.as_path()));
    }

    #[test]
    fn an_empty_token_file_fails_at_start() {
        let dir = tempfile::tempdir().unwrap();
        let tok = dir.path().join("token");
        std::fs::write(&tok, "\n").unwrap();
        let auth = ApiAuth { ca_file: None, token_file: Some(tok) };
        assert!(ApiClient::new("https://127.0.0.1:6443", &auth).is_err());
    }

    #[tokio::test]
    async fn sends_the_current_token_and_returns_items() {
        let dir = tempfile::tempdir().unwrap();
        let tok = dir.path().join("token");
        std::fs::write(&tok, "first\n").unwrap();
        let (url, h) = serve_once("200 OK", r#"{"kind":"ServiceList","items":[{"a":1}]}"#);
        let c = ApiClient::new(&url, &ApiAuth { ca_file: None, token_file: Some(tok.clone()) }).unwrap();
        // Rotated after start: the request carries the new one.
        std::fs::write(&tok, "second\n").unwrap();
        let items = c.list("/api/v1/services").await.unwrap();
        assert_eq!(items.len(), 1);
        let head = h.join().unwrap().to_ascii_lowercase();
        assert!(head.contains("authorization: bearer second"), "{head}");
    }

    #[tokio::test]
    async fn a_refusal_is_an_error_not_an_empty_list() {
        let (url, h) = serve_once(
            "401 Unauthorized",
            r#"{"kind":"Status","status":"Failure","reason":"Unauthorized","code":401}"#,
        );
        let c = ApiClient::new(&url, &ApiAuth::default()).unwrap();
        let e = c.list("/api/v1/services").await.unwrap_err().to_string();
        assert!(e.contains("401"), "{e}");
        h.join().unwrap();
    }

    #[tokio::test]
    async fn a_body_without_items_is_an_error() {
        let (url, h) = serve_once("200 OK", r#"{"kind":"Status"}"#);
        let c = ApiClient::new(&url, &ApiAuth::default()).unwrap();
        assert!(c.list("/api/v1/services").await.is_err());
        h.join().unwrap();
    }
}
