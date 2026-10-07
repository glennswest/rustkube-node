//! `kubectl port-forward` (#56): the kubelet's half of `portforward.k8s.io`.
//!
//! Over a SPDY session ([`crate::spdy`]) the client opens two streams per
//! connection it forwards, both carrying `port` and the same `requestID`: an
//! `error` stream (closed by the client at once; this side writes a message
//! there if the forward fails) and a `data` stream (the bytes). This side
//! connects to that port *inside the pod's network namespace* and splices.
//!
//! "Inside the pod" is `localhost` there: what upstream's runtimes dial, so a
//! server bound to 127.0.0.1 in the pod is reachable, which is the usual reason
//! for port-forwarding at all. The kubelet joins the namespace on a thread of
//! its own (`setns` changes only the calling thread, as the probes do it,
//! `health.rs`); the socket keeps the namespace it was made in. A host-network
//! pod's namespace is the node's, and the kubelet is already in it.

use std::collections::HashMap;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::spdy::{Stream, StreamWriter};

/// The pod a session forwards into.
#[derive(Debug, Clone)]
pub struct Target {
    /// `namespace/name`, for messages.
    pub pod: String,
    pub uid: String,
    /// The sandbox's network namespace; `None` is the node's (hostNetwork).
    pub netns: Option<String>,
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Pair the session's streams by request and forward each pair.
pub async fn serve(mut streams: tokio::sync::mpsc::Receiver<Stream>, target: Target) {
    let mut pending: HashMap<String, (Option<Stream>, Option<Stream>)> = HashMap::new();
    while let Some(s) = streams.recv().await {
        let kind = s.headers.get("streamtype").unwrap_or("").to_ascii_lowercase();
        let request = s.headers.get("requestid").unwrap_or("").to_string();
        let entry = pending.entry(request.clone()).or_default();
        match kind.as_str() {
            "error" => entry.0 = Some(s),
            "data" => entry.1 = Some(s),
            other => {
                tracing::debug!(pod = %target.pod, "port-forward: stream of unknown type {other:?}");
                s.out.reset(crate::spdy::RST_REFUSED).await;
                continue;
            }
        }
        if let (Some(_), Some(_)) = entry {
            let (err, data) = pending.remove(&request).unwrap();
            tokio::spawn(forward(err.unwrap(), data.unwrap(), target.clone()));
        }
    }
}

async fn forward(err: Stream, data: Stream, target: Target) {
    let port = data.headers.get("port").and_then(|p| p.parse::<u16>().ok());
    let Some(port) = port.filter(|p| *p > 0) else {
        let msg = format!("invalid port {:?}", data.headers.get("port").unwrap_or(""));
        fail(&err.out, &data.out, &msg).await;
        return;
    };
    let conn = match connect(target.netns.clone(), port).await {
        Ok(c) => c,
        Err(e) => {
            let msg = format!(
                "error forwarding port {port} to pod {}, uid {}: {e}",
                target.pod, target.uid
            );
            fail(&err.out, &data.out, &msg).await;
            return;
        }
    };
    tracing::info!(pod = %target.pod, port, "port-forward: connected");
    splice(conn, data).await;
    err.out.finish().await;
}

async fn fail(err: &StreamWriter, data: &StreamWriter, msg: &str) {
    tracing::info!("port-forward: {msg}");
    let _ = err.write(msg.as_bytes()).await;
    err.finish().await;
    data.finish().await;
}

/// The data stream and the pod's connection, both ways. The client's FIN
/// half-closes the connection; the connection's end FINs the stream.
async fn splice(conn: TcpStream, data: Stream) {
    let Stream { data: mut inbound, out, .. } = data;
    let (mut r, mut w) = conn.into_split();
    let up = async move {
        while let Some(b) = inbound.recv().await {
            if w.write_all(&b).await.is_err() {
                return;
            }
        }
        let _ = w.shutdown().await;
    };
    let down = async {
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            match r.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if out.write(&buf[..n]).await.is_err() {
                        return;
                    }
                }
            }
        }
        out.finish().await;
    };
    tokio::join!(up, down);
}

/// Connect to `localhost:port` in `netns` (the node's when `None`): IPv4,
/// then IPv6.
pub async fn connect(netns: Option<String>, port: u16) -> Result<TcpStream, String> {
    let Some(netns) = netns else {
        return match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(("127.0.0.1", port))).await {
            Ok(Ok(c)) => Ok(c),
            _ => tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(("::1", port)))
                .await
                .map_err(|_| format!("connect to localhost:{port}: timed out"))?
                .map_err(|e| format!("connect to localhost:{port}: {e}")),
        };
    };
    connect_in(netns, port).await
}

#[cfg(target_os = "linux")]
async fn connect_in(netns: String, port: u16) -> Result<TcpStream, String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        use std::os::fd::AsRawFd;
        let res = (|| -> Result<std::net::TcpStream, String> {
            let file = std::fs::File::open(&netns).map_err(|e| format!("open netns {netns}: {e}"))?;
            // SAFETY: setns affects only this thread, which ends right after.
            if unsafe { libc::setns(file.as_raw_fd(), libc::CLONE_NEWNET) } != 0 {
                return Err(format!("setns({netns}): {}", std::io::Error::last_os_error()));
            }
            let v4 = std::net::SocketAddr::from(([127, 0, 0, 1], port));
            let v6 = std::net::SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, port));
            let c = std::net::TcpStream::connect_timeout(&v4, CONNECT_TIMEOUT)
                .or_else(|_| std::net::TcpStream::connect_timeout(&v6, CONNECT_TIMEOUT))
                .map_err(|e| format!("connect to localhost:{port} in the pod: {e}"))?;
            c.set_nonblocking(true).map_err(|e| format!("socket: {e}"))?;
            Ok(c)
        })();
        let _ = tx.send(res);
    });
    let c = rx.await.map_err(|_| "connect thread ended".to_string())??;
    TcpStream::from_std(c).map_err(|e| format!("socket: {e}"))
}

#[cfg(not(target_os = "linux"))]
async fn connect_in(_netns: String, port: u16) -> Result<TcpStream, String> {
    Err(format!("port {port}: a pod's network namespace needs Linux"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spdy::client::{read_frame, Client, Frame};

    async fn echo_server() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut c, _)) = l.accept().await {
                tokio::spawn(async move {
                    let (mut r, mut w) = c.split();
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                    let _ = w.shutdown().await;
                });
            }
        });
        port
    }

    fn target() -> Target {
        Target { pod: "default/web".into(), uid: "u-1".into(), netns: None }
    }

    /// Frames until a DATA frame on `id`: SYN_REPLYs and the rest are skipped.
    async fn data_on<R: tokio::io::AsyncRead + Unpin>(r: &mut R, id: u32) -> (u8, Vec<u8>) {
        loop {
            match read_frame(r).await.expect("the session ended") {
                Frame::Data(i, f, b) if i == id => return (f, b),
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn a_request_pair_is_forwarded_to_the_port_and_back() {
        let port = echo_server().await.to_string();
        let (mut cli, srv) = tokio::io::duplex(1 << 16);
        tokio::spawn(serve(crate::spdy::serve(srv), target()));
        let mut c = Client::new();
        let h = |t: &'static str| [("streamType", t), ("port", port.as_str()), ("requestID", "0")];
        cli.write_all(&c.syn_stream(1, &h("error"), true)).await.unwrap();
        cli.write_all(&c.syn_stream(3, &h("data"), false)).await.unwrap();
        cli.write_all(&Client::data(3, b"ping over spdy", false)).await.unwrap();
        assert_eq!(data_on(&mut cli, 3).await, (0, b"ping over spdy".to_vec()));
        // The client's FIN half-closes the connection; the echo server's end
        // comes back as a FIN, and the error stream closes empty.
        cli.write_all(&Client::data(3, b"", true)).await.unwrap();
        let mut fins = 0;
        while fins < 2 {
            match read_frame(&mut cli).await.unwrap() {
                Frame::Data(3, 1, b) | Frame::Data(1, 1, b) => {
                    assert!(b.is_empty());
                    fins += 1;
                }
                Frame::Data(1, _, b) => panic!("an error was written: {}", String::from_utf8_lossy(&b)),
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn a_port_nothing_listens_on_is_an_error_on_the_error_stream() {
        let free = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port().to_string()
        };
        let (mut cli, srv) = tokio::io::duplex(1 << 16);
        tokio::spawn(serve(crate::spdy::serve(srv), target()));
        let mut c = Client::new();
        let h = |t: &'static str| [("streamtype", t), ("port", free.as_str()), ("requestid", "7")];
        cli.write_all(&c.syn_stream(1, &h("data"), false)).await.unwrap();
        cli.write_all(&c.syn_stream(3, &h("error"), true)).await.unwrap();
        let (_, msg) = data_on(&mut cli, 3).await;
        let msg = String::from_utf8(msg).unwrap();
        assert!(
            msg.starts_with(&format!("error forwarding port {free} to pod default/web, uid u-1:")),
            "{msg}"
        );
    }
}
