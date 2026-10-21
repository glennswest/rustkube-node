//! `kubectl exec` and `kubectl attach` (#56): upstream's remotecommand
//! protocols, served on a process the runtime runs ([`crate::cri::ExecSession`]).
//!
//! Two transports carry the same five streams (stdin, stdout, stderr, error,
//! resize):
//!
//! - **SPDY/3.1** (`spdy.rs`), protocol `v4.channel.k8s.io` (or v3, v2,
//!   `channel.k8s.io`): the client opens one stream per channel, named by its
//!   `streamType` header. v3 adds the resize stream; v4 writes the result on
//!   the error stream as a `metav1.Status` (the exit code in its causes) where
//!   the older ones write only an error's text.
//! - **WebSocket**, protocol `v5.channel.k8s.io` or `v4.channel.k8s.io`
//!   (KEP-4006, what kubectl ≥ 1.30 tries first): binary messages whose first
//!   byte is the channel (0 stdin, 1 stdout, 2 stderr, 3 error, 4 resize); v5
//!   adds 255, "the client closed channel N", which is how its stdin ends.
//!
//! The query is the kubelet's (the apiserver translates kubectl's): `command`
//! (repeated), `input`, `output`, `error`, `tty`.

use axum::body::Bytes;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

/// What the request asked for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Options {
    pub command: Vec<String>,
    pub io: crate::exec::ExecIo,
}

/// Parse the kubelet's query (`command=sh&command=-c&input=1&output=1&tty=1`).
/// kubectl's own names (`stdin`, `stdout`, `stderr`) are taken too.
pub fn options(query: Option<&str>) -> Options {
    let mut o = Options::default();
    for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let v = decode(v);
        let on = matches!(v.as_str(), "1" | "true" | "True" | "TRUE");
        match k {
            "command" => o.command.push(v),
            "input" | "stdin" => o.io.stdin = on,
            "output" | "stdout" => o.io.stdout = on,
            "error" | "stderr" => o.io.stderr = on,
            "tty" => o.io.tty = on,
            _ => {}
        }
    }
    // A terminal merges stderr into stdout: no stderr stream (upstream).
    if o.io.tty {
        o.io.stderr = false;
    }
    o
}

/// Percent-decoding of a query value (`+` is a space).
fn decode(v: &str) -> String {
    let b = v.as_bytes();
    let hex = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' => match (b.get(i + 1).copied().and_then(hex), b.get(i + 2).copied().and_then(hex)) {
                (Some(h), Some(l)) => {
                    out.push(h << 4 | l);
                    i += 2;
                }
                _ => out.push(b'%'),
            },
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The SPDY subprotocols, best first.
pub const SPDY_PROTOCOLS: &[&str] =
    &["v4.channel.k8s.io", "v3.channel.k8s.io", "v2.channel.k8s.io", "channel.k8s.io"];
/// The WebSocket subprotocols, best first.
pub const WEBSOCKET_PROTOCOLS: &[&str] = &["v5.channel.k8s.io", "v4.channel.k8s.io"];

/// The best of `offered` this side speaks, from `ours` (best first).
pub fn negotiate<'a>(ours: &[&'a str], offered: &[String]) -> Option<&'a str> {
    ours.iter().find(|p| offered.iter().any(|o| o == *p)).copied()
}

/// The version a protocol name carries (`channel.k8s.io` is 1).
pub fn version(protocol: &str) -> u8 {
    protocol.strip_prefix('v').and_then(|r| r.split('.').next()).and_then(|n| n.parse().ok()).unwrap_or(1)
}

/// The v4+ result on the error stream: Success, or the exit code as upstream
/// words it (`NonZeroExitCode`, cause `ExitCode`), which kubectl turns into
/// its own exit status.
pub fn status(command: &[String], result: &Result<i32, String>) -> Value {
    match result {
        Ok(0) => json!({"metadata": {}, "status": "Success"}),
        Ok(code) => json!({
            "metadata": {},
            "status": "Failure",
            "message": format!(
                "command terminated with non-zero exit code: error executing command {command:?}, exit code {code}"
            ),
            "reason": "NonZeroExitCode",
            "details": {"causes": [{"reason": "ExitCode", "message": code.to_string()}]},
        }),
        Err(why) => json!({"metadata": {}, "status": "Failure", "message": why, "reason": "InternalError"}),
    }
}

/// Resize messages (`{"Width":80,"Height":24}`), one after another on a stream
/// that may split them anywhere.
#[derive(Default)]
pub struct Resizes(Vec<u8>);

impl Resizes {
    /// Add bytes; every complete size so far, in order.
    pub fn push(&mut self, data: &[u8]) -> Vec<(u16, u16)> {
        self.0.extend_from_slice(data);
        let mut sizes = Vec::new();
        let mut used = 0;
        let mut it = serde_json::Deserializer::from_slice(&self.0).into_iter::<Value>();
        loop {
            let v = match it.next() {
                Some(Ok(v)) => v,
                // Not JSON at all: nothing after it can be read either.
                Some(Err(e)) if !e.is_eof() => {
                    used = self.0.len();
                    break;
                }
                _ => break,
            };
            used = it.byte_offset();
            let dim = |k: &str| v[k].as_u64().and_then(|n| u16::try_from(n).ok());
            if let (Some(w), Some(h)) = (dim("Width"), dim("Height")) {
                sizes.push((w, h));
            }
        }
        self.0.drain(..used);
        sizes
    }
}

/// One outbound channel, whatever carries it.
#[async_trait::async_trait]
pub trait Out: Send + Sync {
    async fn write(&self, data: &[u8]) -> Result<(), ()>;
    async fn finish(&self);
}

#[async_trait::async_trait]
impl Out for crate::spdy::StreamWriter {
    async fn write(&self, data: &[u8]) -> Result<(), ()> {
        crate::spdy::StreamWriter::write(self, data).await.map_err(|_| ())
    }
    async fn finish(&self) {
        crate::spdy::StreamWriter::finish(self).await
    }
}

/// A WebSocket channel: its byte, on the session's one message sender.
pub struct WsChannel {
    pub channel: u8,
    pub tx: mpsc::Sender<Bytes>,
}

#[async_trait::async_trait]
impl Out for WsChannel {
    async fn write(&self, data: &[u8]) -> Result<(), ()> {
        for chunk in data.chunks(32 * 1024) {
            let mut m = Vec::with_capacity(chunk.len() + 1);
            m.push(self.channel);
            m.extend_from_slice(chunk);
            self.tx.send(Bytes::from(m)).await.map_err(|_| ())?;
        }
        Ok(())
    }
    async fn finish(&self) {}
}

/// The client's side of a session, as channels.
pub struct Wire {
    pub stdin: Option<mpsc::Receiver<Bytes>>,
    pub resize: Option<mpsc::Receiver<Bytes>>,
    pub stdout: Option<Box<dyn Out>>,
    pub stderr: Option<Box<dyn Out>>,
    pub error: Option<Box<dyn Out>>,
    /// Ends when the client has gone (the session's connection closed).
    pub gone: tokio::sync::oneshot::Receiver<()>,
}

/// How long output is still copied after the process ended: whatever it
/// wrote last is in the pipe, and a child it left behind may hold it open.
const DRAIN_AFTER_EXIT: std::time::Duration = std::time::Duration::from_secs(2);

/// Run a session to its end: stdin and resizes in, stdout and stderr out,
/// then the result on the error channel (`v4`: a Status; older: the error's
/// text only). The client going away stops the process.
pub async fn run(
    session: crate::cri::ExecSession,
    mut wire: Wire,
    command: Vec<String>,
    protocol_version: u8,
) -> Result<i32, String> {
    let crate::cri::ExecSession { mut stdio, exit, kill } = session;

    // stdin: the client's bytes, then EOF when it closes the channel.
    if let (Some(mut from), Some(mut to)) = (wire.stdin.take(), stdio.stdin.take()) {
        tokio::spawn(async move {
            while let Some(b) = from.recv().await {
                if to.write_all(&b).await.is_err() || to.flush().await.is_err() {
                    return;
                }
            }
            let _ = to.shutdown().await;
        });
    }
    if let (Some(mut from), Some(pty)) = (wire.resize.take(), stdio.pty.take()) {
        tokio::spawn(async move {
            let mut sizes = Resizes::default();
            while let Some(b) = from.recv().await {
                for (w, h) in sizes.push(&b) {
                    if let Err(e) = pty.resize(w, h) {
                        tracing::debug!("exec: resize to {w}x{h}: {e}");
                    }
                }
            }
        });
    }
    let copy = |r: Option<crate::exec::Reader>, w: Option<Box<dyn Out>>| async move {
        let (Some(mut r), Some(w)) = (r, w) else { return };
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            match r.read(&mut buf).await {
                // A pty's master reads EIO once the last slave is closed: the end.
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if w.write(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
        w.finish().await;
    };
    let mut output = tokio::spawn({
        let (o, e) = (stdio.stdout.take(), stdio.stderr.take());
        let (wo, we) = (wire.stdout.take(), wire.stderr.take());
        async move {
            tokio::join!(copy(o, wo), copy(e, we));
        }
    });

    let result = tokio::select! {
        code = exit => code.map_err(|_| "the process's exit was lost (the engine connection went)".to_string()),
        _ = &mut wire.gone => {
            kill();
            output.abort();
            return Err("the client went away".into());
        }
    };
    let _ = tokio::time::timeout(DRAIN_AFTER_EXIT, &mut output).await;
    output.abort();

    if let Some(err) = wire.error.take() {
        if protocol_version >= 4 {
            let _ = err.write(status(&command, &result).to_string().as_bytes()).await;
        } else if let Ok(code) = &result {
            if *code != 0 {
                let _ = err.write(format!("command terminated with non-zero exit code: {code}").as_bytes()).await;
            }
        } else if let Err(why) = &result {
            let _ = err.write(why.as_bytes()).await;
        }
        err.finish().await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_query_is_read_as_the_kubelets_and_kubectls() {
        let o = options(Some("command=sh&command=-c&command=echo%20hi+there&input=1&output=1&error=1&tty=0"));
        assert_eq!(o.command, ["sh", "-c", "echo hi there"]);
        assert_eq!(o.io, crate::exec::ExecIo { stdin: true, stdout: true, stderr: true, tty: false });
        let o = options(Some("command=bash&stdin=true&stdout=true&stderr=true&tty=true"));
        assert!(o.io.tty && o.io.stdin && o.io.stdout);
        assert!(!o.io.stderr, "a terminal has no stderr stream");
        assert_eq!(options(None), Options::default());
    }

    #[test]
    fn the_best_protocol_both_speak_is_chosen() {
        let offered = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(negotiate(SPDY_PROTOCOLS, &offered(&["v2.channel.k8s.io", "v4.channel.k8s.io"])), Some("v4.channel.k8s.io"));
        assert_eq!(negotiate(WEBSOCKET_PROTOCOLS, &offered(&["v4.channel.k8s.io"])), Some("v4.channel.k8s.io"));
        assert_eq!(negotiate(WEBSOCKET_PROTOCOLS, &offered(&["base64.channel.k8s.io"])), None);
        assert_eq!((version("v5.channel.k8s.io"), version("channel.k8s.io")), (5, 1));
    }

    #[test]
    fn a_non_zero_exit_is_kubectls_exit_code() {
        let cmd = vec!["sh".to_string(), "-c".into(), "exit 3".into()];
        assert_eq!(status(&cmd, &Ok(0))["status"], "Success");
        let s = status(&cmd, &Ok(3));
        assert_eq!(s["reason"], "NonZeroExitCode");
        assert_eq!(s["details"]["causes"][0], json!({"reason": "ExitCode", "message": "3"}));
    }

    #[test]
    fn resizes_split_anywhere_are_read_whole_and_in_order() {
        let mut r = Resizes::default();
        assert_eq!(r.push(br#"{"Width":80,"Hei"#), []);
        assert_eq!(r.push(br#"ght":24}{"Width":120,"Height":40}"#), [(80, 24), (120, 40)]);
        assert_eq!(r.push(b""), []);
    }
}
