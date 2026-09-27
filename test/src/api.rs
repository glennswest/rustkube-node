//! A small Kubernetes REST client: the ServiceAccount's bearer and CA, JSON
//! in and out. Only verbs rustkube serves for every kind are used (GET, LIST,
//! POST, PUT, DELETE); spec changes are read-modify-PUT.

use std::time::Duration;

use serde_json::Value;

use crate::env::Env;

#[derive(Clone)]
pub struct Api {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
}

impl Api {
    pub fn new(env: &Env) -> Result<Api, String> {
        let mut b = reqwest::Client::builder().timeout(Duration::from_secs(30));
        if let Some(ca) = &env.ca {
            let cert = reqwest::Certificate::from_pem(ca).map_err(|e| format!("ServiceAccount ca.crt: {e}"))?;
            b = b.add_root_certificate(cert);
        }
        let http = b.build().map_err(|e| format!("http client: {e}"))?;
        Ok(Api { http, base: env.api.trim_end_matches('/').to_string(), token: env.token.clone() })
    }

    async fn send(&self, method: reqwest::Method, path: &str, body: Option<&Value>) -> Result<(u16, Value), String> {
        let mut req = self.http.request(method.clone(), format!("{}{path}", self.base));
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().await.map_err(|e| format!("{method} {path}: {e}"))?;
        let code = resp.status().as_u16();
        let text = resp.text().await.map_err(|e| format!("{method} {path}: body: {e}"))?;
        let v = serde_json::from_str(&text).unwrap_or(Value::String(text));
        Ok((code, v))
    }

    fn fail(what: &str, path: &str, code: u16, v: &Value) -> String {
        let msg = v.get("message").and_then(Value::as_str).map(str::to_string).unwrap_or_else(|| short(v));
        format!("{what} {path}: HTTP {code}: {msg}")
    }

    /// GET one object; `None` when it does not exist.
    pub async fn get(&self, path: &str) -> Result<Option<Value>, String> {
        match self.send(reqwest::Method::GET, path, None).await? {
            (200, v) => Ok(Some(v)),
            (404, _) => Ok(None),
            (c, v) => Err(Self::fail("GET", path, c, &v)),
        }
    }

    /// LIST a collection's items; `None` when the API does not serve it.
    pub async fn list(&self, path: &str) -> Result<Option<Vec<Value>>, String> {
        match self.get(path).await? {
            None => Ok(None),
            Some(v) => Ok(Some(v.get("items").and_then(Value::as_array).cloned().unwrap_or_default())),
        }
    }

    pub async fn create(&self, path: &str, body: &Value) -> Result<Value, String> {
        match self.send(reqwest::Method::POST, path, Some(body)).await? {
            (200..=202, v) => Ok(v),
            (c, v) => Err(Self::fail("POST", path, c, &v)),
        }
    }

    pub async fn put(&self, path: &str, body: &Value) -> Result<Value, String> {
        match self.send(reqwest::Method::PUT, path, Some(body)).await? {
            (200..=201, v) => Ok(v),
            (c, v) => Err(Self::fail("PUT", path, c, &v)),
        }
    }

    /// DELETE; a missing object is already deleted.
    pub async fn delete(&self, path: &str) -> Result<(), String> {
        match self.send(reqwest::Method::DELETE, path, None).await? {
            (200..=202 | 404, _) => Ok(()),
            (c, v) => Err(Self::fail("DELETE", path, c, &v)),
        }
    }
}

/// A value for an error message, cut short.
pub fn short(v: &Value) -> String {
    let s = match v {
        Value::String(s) => s.clone(),
        v => v.to_string(),
    };
    if s.chars().count() > 200 { format!("{}…", s.chars().take(200).collect::<String>()) } else { s }
}

/// A string field by JSON pointer, or "".
pub fn s<'a>(v: &'a Value, ptr: &str) -> &'a str {
    v.pointer(ptr).and_then(Value::as_str).unwrap_or("")
}

/// A Kubernetes quantity in bytes (`1Gi`, `500M`, `1073741824`, `1.5Gi`).
pub fn quantity_bytes(q: &str) -> Option<u64> {
    let q = q.trim();
    let split = q.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(q.len());
    let (num, suf) = q.split_at(split);
    let n: f64 = num.parse().ok()?;
    let mul: f64 = match suf {
        "" => 1.0,
        "Ki" => 1024.0,
        "Mi" => 1024f64.powi(2),
        "Gi" => 1024f64.powi(3),
        "Ti" => 1024f64.powi(4),
        "Pi" => 1024f64.powi(5),
        "k" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        "T" => 1e12,
        "P" => 1e15,
        _ => return None,
    };
    Some((n * mul) as u64)
}

/// Seconds since the epoch of an RFC 3339 UTC timestamp as the apiserver
/// writes them (`2026-09-26T07:18:29Z`, `…29.123456Z`).
pub fn rfc3339_secs(t: &str) -> Option<i64> {
    let t = t.strip_suffix('Z')?;
    let (date, time) = t.split_once('T')?;
    let mut d = date.split('-').map(|x| x.parse::<i64>().ok());
    let (y, m, dd) = (d.next()??, d.next()??, d.next()??);
    let time = time.split('.').next()?;
    let mut h = time.split(':').map(|x| x.parse::<i64>().ok());
    let (hh, mm, ss) = (h.next()??, h.next()??, h.next()??);
    // Howard Hinnant's days_from_civil.
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + dd - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some(days * 86400 + hh * 3600 + mm * 60 + ss)
}

pub fn now_secs() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

/// The `p`th percentile (0–100) of `xs`, nearest-rank.
pub fn percentile(xs: &[u128], p: u32) -> u128 {
    if xs.is_empty() {
        return 0;
    }
    let mut v = xs.to_vec();
    v.sort_unstable();
    let rank = ((p as usize * v.len()).div_ceil(100)).clamp(1, v.len());
    v[rank - 1]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantities() {
        assert_eq!(quantity_bytes("1Gi"), Some(1 << 30));
        assert_eq!(quantity_bytes("1.5Gi"), Some(3 << 29));
        assert_eq!(quantity_bytes("42"), Some(42));
        assert_eq!(quantity_bytes("2G"), Some(2_000_000_000));
        assert_eq!(quantity_bytes("1Xi"), None);
        assert_eq!(quantity_bytes(""), None);
    }

    #[test]
    fn timestamps() {
        assert_eq!(rfc3339_secs("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(rfc3339_secs("2026-09-26T07:18:29Z"), Some(1_790_407_109));
        assert_eq!(rfc3339_secs("2026-09-26T07:18:29.123456Z"), Some(1_790_407_109));
        assert_eq!(rfc3339_secs("2000-03-01T00:00:00Z"), Some(951_868_800));
        assert_eq!(rfc3339_secs("garbage"), None);
    }

    #[test]
    fn percentiles() {
        assert_eq!(percentile(&[], 50), 0);
        assert_eq!(percentile(&[5], 95), 5);
        assert_eq!(percentile(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 50), 5);
        assert_eq!(percentile(&[10, 1, 9, 2, 8, 3, 7, 4, 6, 5], 95), 10);
    }
}
