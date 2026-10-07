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
    build_authed_client_reloadable(auth).map(|(client, _)| client)
}

/// The kubelet's client certificate, replaceable while every clone of the
/// client keeps working (#77). stormcert renews `system:node:<node>` in place
/// at 80% of its year; the client read it once, so a kubelet that ran a year
/// presented the old one until it expired and lost the apiserver.
///
/// rustls asks this for the pair on every handshake, and every clone of the
/// `reqwest::Client` shares it: a new connection presents the current pair.
#[derive(Debug)]
pub struct ReloadingClientCert {
    current: std::sync::RwLock<(std::sync::Arc<rustls::sign::CertifiedKey>, Vec<u8>, Vec<u8>)>,
}

impl ReloadingClientCert {
    /// A pair as rustls presents it, checked: the key parses and matches the
    /// certificate.
    fn certified(cert_pem: &[u8], key_pem: &[u8]) -> anyhow::Result<std::sync::Arc<rustls::sign::CertifiedKey>> {
        use rustls::pki_types::pem::PemObject;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem)
            .collect::<Result<_, _>>()
            .map_err(|e| anyhow::anyhow!("client certificate: {e:?}"))?;
        anyhow::ensure!(!certs.is_empty(), "client certificate: no certificate in the PEM");
        let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(|e| anyhow::anyhow!("client key: {e:?}"))?;
        let signer = rustls::crypto::ring::sign::any_supported_type(&key)
            .map_err(|e| anyhow::anyhow!("client key: {e}"))?;
        let ck = rustls::sign::CertifiedKey::new(certs, signer);
        ck.keys_match().map_err(|e| anyhow::anyhow!("client key does not match its certificate: {e}"))?;
        Ok(std::sync::Arc::new(ck))
    }

    pub fn new(cert_pem: &[u8], key_pem: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            current: std::sync::RwLock::new((Self::certified(cert_pem, key_pem)?, cert_pem.to_vec(), key_pem.to_vec())),
        })
    }

    /// Present this pair from the next handshake on. `Ok(false)`: the same
    /// bytes as now. An error (unparsable, the key not the certificate's, a
    /// half-written pair) leaves the current pair in place.
    pub fn replace(&self, cert_pem: &[u8], key_pem: &[u8]) -> anyhow::Result<bool> {
        {
            let cur = self.current.read().unwrap_or_else(|e| e.into_inner());
            if cur.1 == cert_pem && cur.2 == key_pem {
                return Ok(false);
            }
        }
        let ck = Self::certified(cert_pem, key_pem)?;
        *self.current.write().unwrap_or_else(|e| e.into_inner()) = (ck, cert_pem.to_vec(), key_pem.to_vec());
        Ok(true)
    }

    /// The pair presented now (tests).
    pub fn current(&self) -> std::sync::Arc<rustls::sign::CertifiedKey> {
        self.current.read().unwrap_or_else(|e| e.into_inner()).0.clone()
    }
}

impl rustls::client::ResolvesClientCert for ReloadingClientCert {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[rustls::SignatureScheme],
    ) -> Option<std::sync::Arc<rustls::sign::CertifiedKey>> {
        Some(self.current())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// [`build_authed_client`], and the client certificate's resolver when the
/// pair can be reloaded (#77): a client pair with the cluster CA and
/// verification on. Otherwise (no pair, no CA, `--insecure-skip-tls-verify`)
/// the identity is fixed for the life of the process, as it was.
pub fn build_authed_client_reloadable(
    auth: &ClientAuth,
) -> anyhow::Result<(reqwest::Client, Option<std::sync::Arc<ReloadingClientCert>>)> {
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
    let mut reloadable = None;
    match (auth.client_cert_pem, auth.client_key_pem) {
        (Some(cert), Some(key)) if auth.ca_pem.is_some() && !auth.insecure_skip_tls_verify => {
            // The CA is the whole root store: the apiserver is the cluster's,
            // signed by its CA, and nothing else is spoken to with this client.
            use rustls::pki_types::pem::PemObject;
            let mut roots = rustls::RootCertStore::empty();
            for c in rustls::pki_types::CertificateDer::pem_slice_iter(auth.ca_pem.unwrap_or_default()) {
                let c = c.map_err(|e| anyhow::anyhow!("apiserver CA cert not usable: {e:?}"))?;
                roots.add(c).map_err(|e| anyhow::anyhow!("apiserver CA cert not usable: {e}"))?;
            }
            anyhow::ensure!(!roots.is_empty(), "apiserver CA cert not usable: no certificate in the PEM");
            let resolver = std::sync::Arc::new(
                ReloadingClientCert::new(cert, key).map_err(|e| anyhow::anyhow!("client certificate/key not usable: {e}"))?,
            );
            let tls = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|e| anyhow::anyhow!("TLS config: {e}"))?
                .with_root_certificates(roots)
                .with_client_cert_resolver(resolver.clone());
            builder = builder.use_preconfigured_tls(tls);
            reloadable = Some(resolver);
        }
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

    let client = builder
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build apiserver HTTP client: {e}"))?;
    Ok((client, reloadable))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair() -> (Vec<u8>, Vec<u8>) {
        let k = rcgen::generate_simple_self_signed(vec!["system:node:n1".to_string()]).unwrap();
        (k.cert.pem().into_bytes(), k.key_pair.serialize_pem().into_bytes())
    }

    /// #77: a renewed pair replaces the presented one; the same bytes do
    /// nothing; a key that is not the certificate's is refused and the
    /// current pair stays.
    #[test]
    fn a_renewed_client_pair_is_presented_and_a_mismatched_one_is_refused() {
        let (ca, _) = pair();
        let (a_cert, a_key) = pair();
        let (client, resolver) = build_authed_client_reloadable(&ClientAuth {
            ca_pem: Some(&ca),
            client_cert_pem: Some(&a_cert),
            client_key_pem: Some(&a_key),
            ..Default::default()
        })
        .unwrap();
        drop(client);
        let r = resolver.expect("reloadable with a CA and a pair");
        let first = r.current();
        assert!(!r.replace(&a_cert, &a_key).unwrap(), "unchanged");
        let (b_cert, b_key) = pair();
        assert!(r.replace(&b_cert, &b_key).unwrap());
        assert!(!std::sync::Arc::ptr_eq(&first, &r.current()), "the renewed pair is presented");
        let now = r.current();
        let err = r.replace(&b_cert, &a_key).unwrap_err().to_string();
        assert!(err.contains("does not match"), "{err}");
        assert!(std::sync::Arc::ptr_eq(&now, &r.current()), "the current pair stays");
        assert!(r.replace(&b_cert, b"-----BEGIN PRIVATE KEY-----\nhalf").is_err());
    }

    #[test]
    fn without_a_ca_or_with_insecure_the_identity_is_fixed() {
        let (cert, key) = pair();
        let (_, r) = build_authed_client_reloadable(&ClientAuth {
            client_cert_pem: Some(&cert),
            client_key_pem: Some(&key),
            ..Default::default()
        })
        .unwrap();
        assert!(r.is_none());
        let (ca, _) = pair();
        let (_, r) = build_authed_client_reloadable(&ClientAuth {
            ca_pem: Some(&ca),
            client_cert_pem: Some(&cert),
            client_key_pem: Some(&key),
            insecure_skip_tls_verify: true,
            ..Default::default()
        })
        .unwrap();
        assert!(r.is_none());
    }

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
