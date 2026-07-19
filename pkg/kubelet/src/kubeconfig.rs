//! Minimal kubeconfig loader.
//!
//! Resolves the current context of a standard kubeconfig into the pieces the
//! kubelet needs to join a secure apiserver: server URL, cluster CA, and the
//! user's client certificate/key or bearer token (rustkube-node#19). Supports
//! both inline `*-data` (base64) fields and on-disk file references.

use anyhow::{anyhow, Context, Result};
use base64::Engine;
use serde::Deserialize;
use std::path::Path;

/// The resolved connection parameters from a kubeconfig's current context.
#[derive(Debug, Default)]
pub struct Kubeconfig {
    pub server: Option<String>,
    pub ca_pem: Option<Vec<u8>>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
    pub token: Option<String>,
    pub insecure_skip_tls_verify: bool,
}

#[derive(Deserialize)]
struct Raw {
    #[serde(rename = "current-context")]
    current_context: Option<String>,
    #[serde(default)]
    contexts: Vec<Named<RawContext>>,
    #[serde(default)]
    clusters: Vec<Named<RawCluster>>,
    #[serde(default)]
    users: Vec<Named<RawUser>>,
}

#[derive(Deserialize)]
struct Named<T> {
    name: String,
    #[serde(flatten)]
    inner: T,
}

#[derive(Deserialize)]
struct RawContext {
    context: ContextRef,
}

#[derive(Deserialize)]
struct ContextRef {
    cluster: String,
    user: String,
}

#[derive(Deserialize)]
struct RawCluster {
    cluster: ClusterFields,
}

#[derive(Deserialize, Default)]
struct ClusterFields {
    server: Option<String>,
    #[serde(rename = "certificate-authority")]
    certificate_authority: Option<String>,
    #[serde(rename = "certificate-authority-data")]
    certificate_authority_data: Option<String>,
    #[serde(rename = "insecure-skip-tls-verify", default)]
    insecure_skip_tls_verify: bool,
}

#[derive(Deserialize)]
struct RawUser {
    user: UserFields,
}

#[derive(Deserialize, Default)]
struct UserFields {
    #[serde(rename = "client-certificate")]
    client_certificate: Option<String>,
    #[serde(rename = "client-certificate-data")]
    client_certificate_data: Option<String>,
    #[serde(rename = "client-key")]
    client_key: Option<String>,
    #[serde(rename = "client-key-data")]
    client_key_data: Option<String>,
    token: Option<String>,
    #[serde(rename = "tokenFile")]
    token_file: Option<String>,
}

/// Load and resolve a kubeconfig file's current context.
pub fn load(path: &str) -> Result<Kubeconfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading kubeconfig {path}"))?;
    let raw: Raw = serde_yaml::from_str(&text)
        .with_context(|| format!("parsing kubeconfig {path}"))?;

    let ctx_name = raw
        .current_context
        .clone()
        .ok_or_else(|| anyhow!("kubeconfig {path}: no current-context"))?;
    let ctx = raw
        .contexts
        .iter()
        .find(|c| c.name == ctx_name)
        .ok_or_else(|| anyhow!("kubeconfig {path}: current-context '{ctx_name}' not found"))?;
    let cluster = raw
        .clusters
        .iter()
        .find(|c| c.name == ctx.inner.context.cluster)
        .map(|c| &c.inner.cluster)
        .ok_or_else(|| anyhow!("kubeconfig {path}: cluster '{}' not found", ctx.inner.context.cluster))?;
    let user = raw
        .users
        .iter()
        .find(|u| u.name == ctx.inner.context.user)
        .map(|u| &u.inner.user);

    // Certificate/key paths in a kubeconfig are relative to the kubeconfig dir.
    let base = Path::new(path).parent();

    let ca_pem = resolve_pem(
        cluster.certificate_authority_data.as_deref(),
        cluster.certificate_authority.as_deref(),
        base,
    )
    .context("cluster certificate-authority")?;

    let (client_cert_pem, client_key_pem, token) = match user {
        Some(u) => {
            let cert = resolve_pem(
                u.client_certificate_data.as_deref(),
                u.client_certificate.as_deref(),
                base,
            )
            .context("user client-certificate")?;
            let key = resolve_pem(u.client_key_data.as_deref(), u.client_key.as_deref(), base)
                .context("user client-key")?;
            let token = match (&u.token, &u.token_file) {
                (Some(t), _) => Some(t.clone()),
                (None, Some(f)) => Some(
                    std::fs::read_to_string(resolve_path(f, base))
                        .with_context(|| format!("reading tokenFile {f}"))?
                        .trim()
                        .to_string(),
                ),
                (None, None) => None,
            };
            (cert, key, token)
        }
        None => (None, None, None),
    };

    Ok(Kubeconfig {
        server: cluster.server.clone(),
        ca_pem,
        client_cert_pem,
        client_key_pem,
        token,
        insecure_skip_tls_verify: cluster.insecure_skip_tls_verify,
    })
}

/// Prefer inline base64 `*-data`, else read the referenced file.
fn resolve_pem(data_b64: Option<&str>, path: Option<&str>, base: Option<&Path>) -> Result<Option<Vec<u8>>> {
    if let Some(b64) = data_b64.filter(|s| !s.is_empty()) {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64.trim())
            .context("base64 decode")?;
        return Ok(Some(bytes));
    }
    if let Some(p) = path.filter(|s| !s.is_empty()) {
        let bytes = std::fs::read(resolve_path(p, base)).with_context(|| format!("reading {p}"))?;
        return Ok(Some(bytes));
    }
    Ok(None)
}

/// Resolve a possibly-relative path against the kubeconfig's directory.
fn resolve_path(p: &str, base: Option<&Path>) -> std::path::PathBuf {
    let path = Path::new(p);
    match base {
        Some(dir) if path.is_relative() => dir.join(path),
        _ => path.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    #[test]
    fn resolves_current_context_with_inline_data() {
        let ca = base64::engine::general_purpose::STANDARD.encode(b"CA-PEM");
        let cert = base64::engine::general_purpose::STANDARD.encode(b"CERT-PEM");
        let key = base64::engine::general_purpose::STANDARD.encode(b"KEY-PEM");
        let yaml = format!(
            r#"
apiVersion: v1
kind: Config
current-context: node@cluster
contexts:
- name: node@cluster
  context: {{cluster: cluster, user: node}}
clusters:
- name: cluster
  cluster: {{server: "https://api:6443", certificate-authority-data: "{ca}"}}
users:
- name: node
  user: {{client-certificate-data: "{cert}", client-key-data: "{key}"}}
"#
        );
        let dir = std::env::temp_dir().join(format!("kctest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("kubeconfig");
        std::fs::write(&path, yaml).unwrap();

        let kc = load(path.to_str().unwrap()).unwrap();
        assert_eq!(kc.server.as_deref(), Some("https://api:6443"));
        assert_eq!(kc.ca_pem.as_deref(), Some(&b"CA-PEM"[..]));
        assert_eq!(kc.client_cert_pem.as_deref(), Some(&b"CERT-PEM"[..]));
        assert_eq!(kc.client_key_pem.as_deref(), Some(&b"KEY-PEM"[..]));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
