//! Authenticated API-server HTTP client.
//!
//! Builds a `reqwest::Client` that (optionally) trusts a cluster CA for HTTPS
//! and carries a bearer token on every request, so the kubelet can talk to a
//! TLS + RBAC apiserver as a real identity rather than plain-HTTP anonymous
//! (rustkube-node#11). With no CA/token it degrades to the previous plain
//! client, preserving the dev/plaintext path.

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION};

/// How the kubelet authenticates to (and trusts) the apiserver. All fields are
/// optional so the plain/dev path (`ClientAuth::default()`) still yields a
/// working plaintext client.
#[derive(Default)]
pub struct ClientAuth<'a> {
    /// Cluster CA (PEM) trusted as a root for HTTPS.
    pub ca_pem: Option<&'a [u8]>,
    /// Bearer token sent as `Authorization: Bearer <token>`.
    pub token: Option<&'a str>,
    /// Client certificate chain (PEM) for mutual-TLS client auth (node identity
    /// `system:node:<name>`). Requires `client_key_pem`.
    pub client_cert_pem: Option<&'a [u8]>,
    /// Private key (PEM) for `client_cert_pem`.
    pub client_key_pem: Option<&'a [u8]>,
    /// Skip server-cert verification (dev only).
    pub insecure_skip_tls_verify: bool,
}

/// Build an apiserver client from a [`ClientAuth`]. Adds the cluster CA as a
/// trusted root, presents a client certificate for mutual-TLS auth, and/or
/// sends a bearer token — matching the auth options `kube-controller-manager`
/// and `kube-scheduler` already accept (rustkube-node#19).
///
/// Every request carries `Accept: application/json`. The kubelet's client is a
/// hand-rolled JSON client that cannot decode `application/vnd.kubernetes.protobuf`;
/// now that the apiserver can emit protobuf (rustkube#32), pinning Accept keeps
/// content negotiation on JSON rather than relying on the server's default
/// (rustkube-node#17).
///
/// Returns an error instead of silently degrading: if a CA/cert is supplied but
/// unusable, or the client fails to build, the caller must not proceed with a
/// client that would fail every HTTPS request with an opaque transport error
/// (rustkube-node#16 — the old `build().unwrap_or_default()` dropped the CA and
/// token on any builder failure, so the node never registered).
pub fn build_authed_client(auth: &ClientAuth) -> anyhow::Result<reqwest::Client> {
    // Bound every request: without a timeout a single unresponsive apiserver
    // call (e.g. a TokenRequest that never returns) wedges the kubelet's sync
    // loop indefinitely. Connect + overall timeouts keep the loop live.
    let mut builder = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30));

    if auth.insecure_skip_tls_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }

    if let Some(pem) = auth.ca_pem {
        // A supplied-but-unusable CA is fatal: continuing without it would
        // leave the client unable to verify a privately-signed apiserver, which
        // surfaces only later as a confusing "error sending request".
        let cert = reqwest::Certificate::from_pem(pem)
            .map_err(|e| anyhow::anyhow!("apiserver CA cert not usable: {e}"))?;
        builder = builder.add_root_certificate(cert);
    }

    // Client certificate (mutual TLS). reqwest wants the cert chain and key in
    // one PEM bundle; require both halves so we fail loudly rather than sending
    // an anonymous handshake the apiserver will reject.
    match (auth.client_cert_pem, auth.client_key_pem) {
        (Some(cert), Some(key)) => {
            let mut bundle = Vec::with_capacity(cert.len() + key.len() + 1);
            bundle.extend_from_slice(cert);
            if !cert.ends_with(b"\n") {
                bundle.push(b'\n');
            }
            bundle.extend_from_slice(key);
            let identity = reqwest::Identity::from_pem(&bundle)
                .map_err(|e| anyhow::anyhow!("client certificate/key not usable: {e}"))?;
            builder = builder.identity(identity);
        }
        (Some(_), None) | (None, Some(_)) => {
            anyhow::bail!(
                "client-cert auth needs both a certificate and a key \
                 (--client-certificate and --client-key)"
            );
        }
        (None, None) => {}
    }

    // Default headers on every request: always ask for JSON; add the bearer
    // token when configured.
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    if let Some(tok) = auth.token.filter(|t| !t.is_empty()) {
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
        assert!(build_authed_client(&ClientAuth::default()).is_ok());
    }

    #[test]
    fn builds_with_token() {
        assert!(build_authed_client(&ClientAuth {
            token: Some("abc.def.ghi"),
            ..Default::default()
        })
        .is_ok());
    }

    #[test]
    fn unusable_ca_is_an_error_not_a_silent_drop() {
        // rustkube-node#16: a supplied-but-broken CA must surface as an error,
        // never a client that silently omits the root and fails every request.
        // reqwest validates the cert lazily, so this fails at build() rather
        // than at from_pem() — either way it must be an error, not a fallback.
        let err = build_authed_client(&ClientAuth {
            ca_pem: Some(b"-----BEGIN CERTIFICATE-----\nnope\n-----END CERTIFICATE-----\n"),
            ..Default::default()
        })
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("CA cert not usable") || err.contains("failed to build"),
            "got: {err}"
        );
    }

    #[test]
    fn client_cert_without_key_is_an_error() {
        // A half-configured client cert must fail loudly, not send an anonymous
        // handshake the apiserver rejects (rustkube-node#19).
        let err = build_authed_client(&ClientAuth {
            client_cert_pem: Some(b"-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n"),
            ..Default::default()
        })
        .unwrap_err()
        .to_string();
        assert!(err.contains("both a certificate and a key"), "got: {err}");
    }
}
