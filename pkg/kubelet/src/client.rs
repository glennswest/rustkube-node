//! Authenticated API-server HTTP client.
//!
//! Builds a `reqwest::Client` that (optionally) trusts a cluster CA for HTTPS
//! and carries a bearer token on every request, so the kubelet can talk to a
//! TLS + RBAC apiserver as a real identity rather than plain-HTTP anonymous
//! (rustkube-node#11). With no CA/token it degrades to the previous plain
//! client, preserving the dev/plaintext path.

use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};

/// Build an apiserver client. `ca_pem` (PEM bytes) is added as a trusted root
/// for HTTPS; `token` is sent as `Authorization: Bearer <token>` on every call.
pub fn build_authed_client(ca_pem: Option<&[u8]>, token: Option<&str>) -> reqwest::Client {
    let mut builder = reqwest::Client::builder();

    if let Some(pem) = ca_pem {
        match reqwest::Certificate::from_pem(pem) {
            Ok(cert) => builder = builder.add_root_certificate(cert),
            Err(e) => tracing::warn!("apiserver CA cert not usable: {e}"),
        }
    }

    if let Some(tok) = token.filter(|t| !t.is_empty()) {
        if let Ok(mut val) = HeaderValue::from_str(&format!("Bearer {tok}")) {
            val.set_sensitive(true);
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, val);
            builder = builder.default_headers(headers);
        }
    }

    // Never fall back to a default client here. `unwrap_or_default()` would
    // silently discard the root CA and bearer token and hand back a bare
    // client, which then fails TLS against the cluster CA with an opaque
    // "error sending request" — or worse, talks to an apiserver unauthenticated.
    // A kubelet that cannot build its authenticated client must not continue.
    match builder.build() {
        Ok(client) => client,
        Err(e) => panic!(
            "failed to build the apiserver client (CA supplied: {}, token supplied: {}): {e}",
            ca_pem.is_some(),
            token.is_some_and(|t| !t.is_empty())
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_without_ca_or_token() {
        // The plain/dev path must still yield a working client.
        let _ = build_authed_client(None, None);
    }

    #[test]
    fn builds_with_token() {
        let _ = build_authed_client(None, Some("abc.def.ghi"));
    }
}
