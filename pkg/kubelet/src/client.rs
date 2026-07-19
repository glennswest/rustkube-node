//! Authenticated API-server HTTP client.
//!
//! Builds a `reqwest::Client` that (optionally) trusts a cluster CA for HTTPS
//! and carries a bearer token on every request, so the kubelet can talk to a
//! TLS + RBAC apiserver as a real identity rather than plain-HTTP anonymous
//! (rustkube-node#11). With no CA/token it degrades to the previous plain
//! client, preserving the dev/plaintext path.

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION};

/// Build an apiserver client. `ca_pem` (PEM bytes) is added as a trusted root
/// for HTTPS; `token` is sent as `Authorization: Bearer <token>` on every call.
///
/// Every request carries `Accept: application/json`. The kubelet's client is a
/// hand-rolled JSON client that cannot decode `application/vnd.kubernetes.protobuf`;
/// now that the apiserver can emit protobuf (rustkube#32), pinning Accept keeps
/// content negotiation on JSON rather than relying on the server's default
/// (rustkube-node#17).
///
/// Returns an error instead of silently degrading: if a CA was supplied but is
/// unusable, or the client fails to build, the caller must not proceed with a
/// client that would fail every HTTPS request with an opaque transport error
/// (rustkube-node#16 — the old `build().unwrap_or_default()` dropped the CA and
/// token on any builder failure, so the node never registered).
pub fn build_authed_client(
    ca_pem: Option<&[u8]>,
    token: Option<&str>,
) -> anyhow::Result<reqwest::Client> {
    // Bound every request: without a timeout a single unresponsive apiserver
    // call (e.g. a TokenRequest that never returns) wedges the kubelet's sync
    // loop indefinitely. Connect + overall timeouts keep the loop live.
    let mut builder = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30));

    if let Some(pem) = ca_pem {
        // A supplied-but-unusable CA is fatal: continuing without it would
        // leave the client unable to verify a privately-signed apiserver, which
        // surfaces only later as a confusing "error sending request".
        let cert = reqwest::Certificate::from_pem(pem)
            .map_err(|e| anyhow::anyhow!("apiserver CA cert not usable: {e}"))?;
        builder = builder.add_root_certificate(cert);
    }

    // Default headers on every request: always ask for JSON; add the bearer
    // token when configured.
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    if let Some(tok) = token.filter(|t| !t.is_empty()) {
        let mut val = HeaderValue::from_str(&format!("Bearer {tok}"))
            .map_err(|e| anyhow::anyhow!("bearer token is not a valid header value: {e}"))?;
        val.set_sensitive(true);
        headers.insert(AUTHORIZATION, val);
    }
    builder = builder.default_headers(headers);

    builder
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build apiserver HTTP client: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_without_ca_or_token() {
        // The plain/dev path must still yield a working client.
        assert!(build_authed_client(None, None).is_ok());
    }

    #[test]
    fn builds_with_token() {
        assert!(build_authed_client(None, Some("abc.def.ghi")).is_ok());
    }

    #[test]
    fn unusable_ca_is_an_error_not_a_silent_drop() {
        // rustkube-node#16: a supplied-but-broken CA must surface as an error,
        // never a client that silently omits the root and fails every request.
        // reqwest validates the cert lazily, so this fails at build() rather
        // than at from_pem() — either way it must be an error, not a fallback.
        let err = build_authed_client(
            Some(b"-----BEGIN CERTIFICATE-----\nnope\n-----END CERTIFICATE-----\n"),
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("CA cert not usable") || err.contains("failed to build"),
            "got: {err}"
        );
    }
}
