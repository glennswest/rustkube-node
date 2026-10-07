//! A SPDY/3.1 server session, for the kubelet's streaming endpoints (#56).
//!
//! `kubectl port-forward` (and `exec`/`attach` from an older `kubectl`) opens
//! an HTTP upgrade to `SPDY/3.1` and then multiplexes streams over the one
//! connection: each stream is a SYN_STREAM with headers, DATA frames, and a
//! FIN. rustkube's apiserver splices that connection to this server unchanged
//! (rustkube `streaming.rs`), so the kubelet is the SPDY server.
//!
//! What the client (moby/spdystream, inside client-go) needs, and so what this
//! implements:
//! - every SYN_STREAM it opens answered with a SYN_REPLY (it waits for one);
//! - DATA in both directions, FIN to half-close, RST_STREAM to abandon;
//! - PING echoed; GOAWAY ends the session.
//!
//! spdystream implements no flow control (it has no WINDOW_UPDATE handling
//! and ignores frame types it does not handle), so none is done here either:
//! SETTINGS and WINDOW_UPDATE are read and ignored, and none are sent.
//!
//! Header blocks are zlib streams, one per direction for the life of the
//! connection, compressed against the SPDY/3 dictionary
//! ([`spdy_dictionary.bin`], verbatim from moby/spdystream). The client's are
//! inflated with that dictionary preloaded into the inflater's window; this
//! side's are written as stored (uncompressed) deflate blocks with no
//! dictionary flag, which a zlib reader given a dictionary accepts as is.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// `bytes::Bytes`, by the re-export the server already depends on.
use axum::body::Bytes;
use miniz_oxide::inflate::core::{decompress, inflate_flags, DecompressorOxide};
use miniz_oxide::inflate::TINFLStatus;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

/// The SPDY/3 header dictionary (the spec's, as moby/spdystream carries it).
pub const DICTIONARY: &[u8] = include_bytes!("spdy_dictionary.bin");

const VERSION: u16 = 3;
const SYN_STREAM: u16 = 1;
const SYN_REPLY: u16 = 2;
const RST_STREAM: u16 = 3;
const SETTINGS: u16 = 4;
const PING: u16 = 6;
const GOAWAY: u16 = 7;
const HEADERS: u16 = 8;
const WINDOW_UPDATE: u16 = 9;
const FLAG_FIN: u8 = 0x01;

/// RST_STREAM status codes this side sends.
pub const RST_CANCEL: u32 = 5;
pub const RST_REFUSED: u32 = 3;

/// The largest frame accepted: SPDY's length field is 24 bits.
const MAX_FRAME: usize = (1 << 24) - 1;
/// Outbound DATA is cut into frames no larger than this.
const DATA_CHUNK: usize = 32 * 1024;

/// A stream's headers, as the client sent them. Names compare without case.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Headers(pub Vec<(String, String)>);

impl Headers {
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
}

/// One stream the client opened.
pub struct Stream {
    pub id: u32,
    pub headers: Headers,
    /// What the client sends on it; closed at its FIN, a reset, or the end
    /// of the session.
    pub data: mpsc::Receiver<Bytes>,
    pub out: StreamWriter,
}

/// This side's half of a stream.
#[derive(Clone)]
pub struct StreamWriter {
    id: u32,
    tx: mpsc::Sender<Out>,
    reset: Arc<AtomicBool>,
}

/// The session's writer is gone, or the client reset the stream.
#[derive(Debug, PartialEq, Eq)]
pub struct Closed;

impl StreamWriter {
    /// Send `data` on the stream, in frames of at most 32 KiB.
    pub async fn write(&self, data: &[u8]) -> Result<(), Closed> {
        for chunk in data.chunks(DATA_CHUNK) {
            if self.reset.load(Ordering::Acquire) {
                return Err(Closed);
            }
            self.tx
                .send(Out::Data(self.id, Bytes::copy_from_slice(chunk), false))
                .await
                .map_err(|_| Closed)?;
        }
        Ok(())
    }

    /// Half-close: this side sends nothing more (an empty DATA with FIN).
    pub async fn finish(&self) {
        let _ = self.tx.send(Out::Data(self.id, Bytes::new(), true)).await;
    }

    /// Abandon the stream.
    pub async fn reset(&self, status: u32) {
        self.reset.store(true, Ordering::Release);
        let _ = self.tx.send(Out::Rst(self.id, status)).await;
    }

    pub fn is_reset(&self) -> bool {
        self.reset.load(Ordering::Acquire)
    }
}

/// What the writer task sends.
#[derive(Debug)]
enum Out {
    Reply(u32),
    Data(u32, Bytes, bool),
    Rst(u32, u32),
    Ping(u32),
}

/// Serve a SPDY/3.1 session on `io` (already upgraded). Each stream the client
/// opens is answered and delivered on the returned channel, which ends when
/// the session does.
pub fn serve<IO>(io: IO) -> mpsc::Receiver<Stream>
where
    IO: AsyncRead + AsyncWrite + Send + 'static,
{
    let (read, write) = tokio::io::split(io);
    let (out_tx, out_rx) = mpsc::channel::<Out>(256);
    let (streams_tx, streams_rx) = mpsc::channel::<Stream>(64);
    tokio::spawn(write_loop(write, out_rx));
    tokio::spawn(async move {
        if let Err(e) = read_loop(read, out_tx, streams_tx).await {
            tracing::debug!("spdy session ended: {e}");
        }
    });
    streams_rx
}

struct Inbound {
    data: mpsc::Sender<Bytes>,
    reset: Arc<AtomicBool>,
}

async fn read_loop<R: AsyncRead + Unpin>(
    mut r: R,
    out: mpsc::Sender<Out>,
    deliver: mpsc::Sender<Stream>,
) -> Result<(), String> {
    let mut inflater = HeaderInflater::new();
    let mut streams: HashMap<u32, Inbound> = HashMap::new();
    loop {
        let mut head = [0u8; 8];
        match r.read_exact(&mut head).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(format!("read: {e}")),
        }
        let flags = head[4];
        let len = u32::from_be_bytes([0, head[5], head[6], head[7]]) as usize;
        if len > MAX_FRAME {
            return Err(format!("frame of {len} bytes"));
        }
        let mut body = vec![0u8; len];
        r.read_exact(&mut body).await.map_err(|e| format!("read: {e}"))?;

        if head[0] & 0x80 == 0 {
            // DATA.
            let id = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) & 0x7fff_ffff;
            if let Some(s) = streams.get(&id) {
                if !body.is_empty() && s.data.send(Bytes::from(body)).await.is_err() {
                    // Nobody reads it any more: say so once.
                    let _ = out.send(Out::Rst(id, RST_CANCEL)).await;
                    streams.remove(&id);
                    continue;
                }
                if flags & FLAG_FIN != 0 {
                    streams.remove(&id);
                }
            }
            continue;
        }

        let version = u16::from_be_bytes([head[0] & 0x7f, head[1]]);
        if version != VERSION {
            return Err(format!("SPDY version {version}"));
        }
        let kind = u16::from_be_bytes([head[2], head[3]]);
        let word = |at: usize| -> Result<u32, String> {
            body.get(at..at + 4)
                .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
                .ok_or_else(|| format!("short control frame {kind}"))
        };
        match kind {
            SYN_STREAM => {
                let id = word(0)? & 0x7fff_ffff;
                let block = body.get(10..).ok_or("short SYN_STREAM")?;
                let headers = parse_block(&inflater.inflate(block)?)?;
                let reset = Arc::new(AtomicBool::new(false));
                let (data_tx, data_rx) = mpsc::channel(64);
                if flags & FLAG_FIN == 0 {
                    streams.insert(id, Inbound { data: data_tx, reset: reset.clone() });
                }
                out.send(Out::Reply(id)).await.map_err(|_| "writer gone")?;
                let stream = Stream {
                    id,
                    headers,
                    data: data_rx,
                    out: StreamWriter { id, tx: out.clone(), reset },
                };
                if deliver.send(stream).await.is_err() {
                    let _ = out.send(Out::Rst(id, RST_REFUSED)).await;
                }
            }
            RST_STREAM => {
                if let Some(s) = streams.remove(&(word(0)? & 0x7fff_ffff)) {
                    s.reset.store(true, Ordering::Release);
                }
            }
            HEADERS => {
                // Read (the zlib context must advance) and otherwise ignored;
                // a FIN on it half-closes like a DATA FIN.
                let id = word(0)? & 0x7fff_ffff;
                if let Some(block) = body.get(4..) {
                    inflater.inflate(block)?;
                }
                if flags & FLAG_FIN != 0 {
                    streams.remove(&id);
                }
            }
            PING => {
                let id = word(0)?;
                // Ours are even, theirs odd: only theirs are echoed.
                if id % 2 == 1 {
                    out.send(Out::Ping(id)).await.map_err(|_| "writer gone")?;
                }
            }
            GOAWAY => return Ok(()),
            SYN_REPLY | SETTINGS | WINDOW_UPDATE => {}
            _ => {}
        }
    }
}

async fn write_loop<W: AsyncWrite + Unpin>(mut w: W, mut rx: mpsc::Receiver<Out>) {
    let mut deflater = HeaderDeflater::default();
    while let Some(o) = rx.recv().await {
        let frame = match o {
            Out::Reply(id) => {
                let mut body = id.to_be_bytes().to_vec();
                body.extend(deflater.block(&encode_block(&Headers::default())));
                control(SYN_REPLY, 0, &body)
            }
            Out::Data(id, data, fin) => {
                let mut f = Vec::with_capacity(8 + data.len());
                f.extend((id & 0x7fff_ffff).to_be_bytes());
                f.push(if fin { FLAG_FIN } else { 0 });
                f.extend(&(data.len() as u32).to_be_bytes()[1..]);
                f.extend(&data);
                f
            }
            Out::Rst(id, status) => {
                let mut body = id.to_be_bytes().to_vec();
                body.extend(status.to_be_bytes());
                control(RST_STREAM, 0, &body)
            }
            Out::Ping(id) => control(PING, 0, &id.to_be_bytes()),
        };
        if w.write_all(&frame).await.is_err() || w.flush().await.is_err() {
            break;
        }
    }
    let _ = w.shutdown().await;
}

fn control(kind: u16, flags: u8, body: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(8 + body.len());
    f.extend((0x8000u16 | VERSION).to_be_bytes());
    f.extend(kind.to_be_bytes());
    f.push(flags);
    f.extend(&(body.len() as u32).to_be_bytes()[1..]);
    f.extend(body);
    f
}

/// A name/value block: a count, then each name and value with its length.
pub fn encode_block(h: &Headers) -> Vec<u8> {
    let mut b = (h.0.len() as u32).to_be_bytes().to_vec();
    for (k, v) in &h.0 {
        b.extend((k.len() as u32).to_be_bytes());
        b.extend(k.to_ascii_lowercase().as_bytes());
        b.extend((v.len() as u32).to_be_bytes());
        b.extend(v.as_bytes());
    }
    b
}

pub fn parse_block(b: &[u8]) -> Result<Headers, String> {
    let mut at = 0usize;
    let mut take = |n: usize| -> Result<&[u8], String> {
        let s = b.get(at..at + n).ok_or("short header block")?;
        at += n;
        Ok(s)
    };
    let word = |s: &[u8]| u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as usize;
    let count = word(take(4)?);
    let mut out = Vec::with_capacity(count.min(64));
    for _ in 0..count {
        let n = word(take(4)?);
        let name = String::from_utf8_lossy(take(n)?).into_owned();
        let n = word(take(4)?);
        // Several values of one name are joined with NUL; the first is
        // enough for anything this server reads.
        let value = String::from_utf8_lossy(take(n)?).split('\0').next().unwrap_or("").to_string();
        out.push((name, value));
    }
    Ok(Headers(out))
}

/// This side's header blocks: one zlib stream for the connection, written as
/// stored blocks. A zlib reader given the dictionary accepts a stream that
/// does not declare one.
#[derive(Default)]
pub struct HeaderDeflater {
    started: bool,
}

impl HeaderDeflater {
    pub fn block(&mut self, raw: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(raw.len() + 16);
        if !self.started {
            // CMF 0x78 (deflate, 32 KiB window), FLG 0x01: no dictionary,
            // check bits making 0x7801 a multiple of 31.
            out.extend([0x78, 0x01]);
            self.started = true;
        }
        for chunk in raw.chunks(0xffff) {
            let n = chunk.len() as u16;
            // BFINAL 0, BTYPE 00 (stored); byte-aligned, so one zero byte.
            out.push(0x00);
            out.extend(n.to_le_bytes());
            out.extend((!n).to_le_bytes());
            out.extend(chunk);
        }
        out
    }
}

/// The client's header blocks: one zlib stream for the connection, inflated
/// with the SPDY dictionary as the window's start.
pub struct HeaderInflater {
    state: Box<DecompressorOxide>,
    window: Vec<u8>,
    pos: usize,
    /// The 2-byte zlib header (and a 4-byte DICTID after it when FDICT is
    /// set) not yet read: the inflater is given raw deflate.
    header_left: usize,
    header: Vec<u8>,
}

/// The inflater's circular window: a power of two, at least the 32 KiB
/// deflate may refer back.
const WINDOW: usize = 64 * 1024;

impl HeaderInflater {
    pub fn new() -> Self {
        let mut window = vec![0u8; WINDOW];
        window[..DICTIONARY.len()].copy_from_slice(DICTIONARY);
        HeaderInflater {
            state: Box::default(),
            window,
            pos: DICTIONARY.len(),
            header_left: 2,
            header: Vec::new(),
        }
    }

    /// The next block of the stream, decompressed.
    pub fn inflate(&mut self, mut input: &[u8]) -> Result<Vec<u8>, String> {
        while self.header_left > 0 && !input.is_empty() {
            self.header.push(input[0]);
            input = &input[1..];
            self.header_left -= 1;
            if self.header.len() == 2 {
                let (cmf, flg) = (self.header[0], self.header[1]);
                if cmf & 0x0f != 8 || (u16::from(cmf) << 8 | u16::from(flg)) % 31 != 0 {
                    return Err("header block is not a zlib stream".into());
                }
                if flg & 0x20 != 0 {
                    self.header_left = 4; // DICTID: the SPDY dictionary's.
                }
            }
        }
        let mut out = Vec::new();
        let flags = inflate_flags::TINFL_FLAG_HAS_MORE_INPUT;
        loop {
            let (status, read, written) =
                decompress(&mut self.state, input, &mut self.window, self.pos, flags);
            out.extend_from_slice(&self.window[self.pos..self.pos + written]);
            self.pos = (self.pos + written) & (WINDOW - 1);
            input = &input[read..];
            match status {
                TINFLStatus::HasMoreOutput => continue,
                TINFLStatus::NeedsMoreInput | TINFLStatus::Done => {
                    if input.is_empty() {
                        return Ok(out);
                    }
                    if read == 0 && written == 0 {
                        return Err("header block: inflate made no progress".into());
                    }
                }
                s => return Err(format!("header block: inflate failed ({s:?})")),
            }
        }
    }
}

impl Default for HeaderInflater {
    fn default() -> Self {
        Self::new()
    }
}

/// A client's side of a session, for tests here and in the endpoints.
#[cfg(test)]
pub mod client {
    use super::*;

    /// Frames as a client writes them: its own header stream (stored blocks,
    /// like this side's) and stream ids.
    pub struct Client {
        pub deflater: HeaderDeflater,
        pub inflater: HeaderInflater,
    }

    impl Client {
        pub fn new() -> Self {
            Client { deflater: HeaderDeflater::default(), inflater: HeaderInflater::new() }
        }

        pub fn syn_stream(&mut self, id: u32, headers: &[(&str, &str)], fin: bool) -> Vec<u8> {
            let h = Headers(headers.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect());
            let mut body = id.to_be_bytes().to_vec();
            body.extend([0, 0, 0, 0, 0, 0]); // associated stream, priority, slot
            body.extend(self.deflater.block(&encode_block(&h)));
            control(SYN_STREAM, if fin { FLAG_FIN } else { 0 }, &body)
        }

        pub fn data(id: u32, data: &[u8], fin: bool) -> Vec<u8> {
            let mut f = id.to_be_bytes().to_vec();
            f.push(if fin { FLAG_FIN } else { 0 });
            f.extend(&(data.len() as u32).to_be_bytes()[1..]);
            f.extend(data);
            f
        }

        pub fn ping(id: u32) -> Vec<u8> {
            control(PING, 0, &id.to_be_bytes())
        }
    }

    /// One frame read back: control (kind, flags, body) or data (id, flags, body).
    #[derive(Debug, PartialEq, Eq)]
    pub enum Frame {
        Control(u16, u8, Vec<u8>),
        Data(u32, u8, Vec<u8>),
    }

    pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Option<Frame> {
        let mut head = [0u8; 8];
        r.read_exact(&mut head).await.ok()?;
        let len = u32::from_be_bytes([0, head[5], head[6], head[7]]) as usize;
        let mut body = vec![0u8; len];
        r.read_exact(&mut body).await.ok()?;
        Some(if head[0] & 0x80 != 0 {
            Frame::Control(u16::from_be_bytes([head[2], head[3]]), head[4], body)
        } else {
            Frame::Data(u32::from_be_bytes([head[0], head[1], head[2], head[3]]), head[4], body)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::client::{read_frame, Client, Frame};
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    /// Two header blocks as a client compresses them: zlib with the SPDY/3
    /// dictionary (FDICT), one context across frames, sync-flushed. Made by
    /// Python's zlib (`compressobj(zdict=…)`), not by this module.
    #[test]
    fn a_clients_dictionary_compressed_header_blocks_are_read_across_frames() {
        let f1 = hex("78f9e3c6a7c202253a703a2b294a4dcc851625aca9a0808614cb45e01c6061606100a4398b20d198094ade8c0600000000ffff");
        let f2 = hex("c2aa19540e2512d60b000000ffff");
        let mut inf = HeaderInflater::new();
        let h1 = parse_block(&inf.inflate(&f1).unwrap()).unwrap();
        assert_eq!(h1.get("streamtype"), Some("error"));
        assert_eq!(h1.get("Port"), Some("8080"));
        assert_eq!(h1.get("requestid"), Some("0"));
        let h2 = parse_block(&inf.inflate(&f2).unwrap()).unwrap();
        assert_eq!(h2.get("streamType"), Some("data"), "the second frame refers back into the first");
    }

    #[test]
    fn this_sides_header_blocks_read_back_and_are_a_zlib_stream() {
        let mut def = HeaderDeflater::default();
        let mut inf = HeaderInflater::new();
        let h = Headers(vec![("a".into(), "1".into()), ("b".into(), "xyz".into())]);
        let first = def.block(&encode_block(&h));
        assert_eq!(&first[..2], &[0x78, 0x01]);
        assert_eq!(parse_block(&inf.inflate(&first).unwrap()).unwrap(), h);
        let empty = def.block(&encode_block(&Headers::default()));
        assert_eq!(parse_block(&inf.inflate(&empty).unwrap()).unwrap(), Headers::default());
        let big = Headers(vec![("x".into(), "v".repeat(70_000))]);
        assert_eq!(parse_block(&inf.inflate(&def.block(&encode_block(&big))).unwrap()).unwrap(), big);
    }

    #[test]
    fn the_dictionary_is_spdy3s() {
        // The DICTID a client's zlib header carries for it.
        let id = hex("78f9e3c6a7c2");
        let (a, b) = DICTIONARY.iter().fold((1u32, 0u32), |(a, b), &x| {
            let a = (a + x as u32) % 65521;
            (a, (b + a) % 65521)
        });
        assert_eq!((b << 16 | a).to_be_bytes(), id[2..6]);
    }

    #[tokio::test]
    async fn a_session_answers_streams_echoes_pings_and_carries_data_both_ways() {
        let (mut cli, srv) = tokio::io::duplex(1 << 16);
        let mut streams = serve(srv);
        let mut c = Client::new();

        cli.write_all(&c.syn_stream(1, &[("streamtype", "data"), ("port", "80")], false)).await.unwrap();
        let mut s = streams.recv().await.unwrap();
        assert_eq!((s.id, s.headers.get("port")), (1, Some("80")));
        match read_frame(&mut cli).await.unwrap() {
            Frame::Control(k, _, body) => {
                assert_eq!(k, SYN_REPLY);
                assert_eq!(&body[..4], &1u32.to_be_bytes());
                assert_eq!(parse_block(&c.inflater.inflate(&body[4..]).unwrap()).unwrap(), Headers::default());
            }
            f => panic!("{f:?}"),
        }

        cli.write_all(&Client::ping(7)).await.unwrap();
        assert_eq!(read_frame(&mut cli).await.unwrap(), Frame::Control(PING, 0, 7u32.to_be_bytes().to_vec()));

        cli.write_all(&Client::data(1, b"hello", true)).await.unwrap();
        assert_eq!(s.data.recv().await.unwrap(), Bytes::from_static(b"hello"));
        assert!(s.data.recv().await.is_none(), "closed at the client's FIN");

        s.out.write(b"back").await.unwrap();
        s.out.finish().await;
        assert_eq!(read_frame(&mut cli).await.unwrap(), Frame::Data(1, 0, b"back".to_vec()));
        assert_eq!(read_frame(&mut cli).await.unwrap(), Frame::Data(1, FLAG_FIN, vec![]));

        // A stream opened already half-closed (an error stream) has no data.
        cli.write_all(&c.syn_stream(3, &[("streamtype", "error")], true)).await.unwrap();
        let mut e = streams.recv().await.unwrap();
        assert!(e.data.recv().await.is_none());

        // A stream the client resets refuses further writes. The PING after
        // the RST is echoed only once the RST has been read.
        cli.write_all(&c.syn_stream(5, &[("streamtype", "data")], false)).await.unwrap();
        let r = streams.recv().await.unwrap();
        assert!(matches!(read_frame(&mut cli).await.unwrap(), Frame::Control(SYN_REPLY, _, _)));
        let mut body = 5u32.to_be_bytes().to_vec();
        body.extend(5u32.to_be_bytes());
        cli.write_all(&control(RST_STREAM, 0, &body)).await.unwrap();
        cli.write_all(&Client::ping(9)).await.unwrap();
        assert_eq!(read_frame(&mut cli).await.unwrap(), Frame::Control(PING, 0, 9u32.to_be_bytes().to_vec()));
        assert!(r.out.is_reset());
        assert_eq!(r.out.write(b"x").await, Err(Closed));

        drop(cli);
        assert!(streams.recv().await.is_none(), "the session ends with the connection");
    }
}
