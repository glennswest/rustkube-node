//! A process run inside a running container (#56, stormpump#103): the stdio
//! the kubelet holds for `kubectl exec`, `kubectl attach` and exec probes.
//!
//! stormpump's exec is a `SPAWN` with the running workload's handle in
//! `inline_a`: the spec's argv runs in that workload's namespaces, root and
//! cgroup. No stream data passes through the ring. The process's 0, 1 and 2
//! are descriptors the kubelet deposited on the engine's socket, named by the
//! spec's `fds` slots: a pipe per stream it asked for, or the slave of one pty
//! on all three for a terminal (the engine makes a deposited terminal on fd 0
//! the controlling terminal of a new session). The kubelet keeps the other
//! ends, here.

use std::io;
use std::os::fd::{FromRawFd, OwnedFd};
use std::pin::Pin;

use tokio::io::{AsyncRead, AsyncWrite};

/// Which streams the caller wants, as kubectl's `stdin`/`stdout`/`stderr`/`tty`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecIo {
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    /// One terminal for all three: stderr is merged into stdout, as upstream.
    pub tty: bool,
}

pub type Reader = Pin<Box<dyn AsyncRead + Send>>;
pub type Writer = Pin<Box<dyn AsyncWrite + Send>>;

/// The kubelet's ends.
#[derive(Default)]
pub struct Stdio {
    pub stdin: Option<Writer>,
    pub stdout: Option<Reader>,
    pub stderr: Option<Reader>,
    /// The terminal's master, for a resize. `None` without a tty.
    pub pty: Option<Pty>,
}

/// The process's ends: each descriptor and the fd it becomes.
pub struct ChildEnds(pub Vec<(i32, OwnedFd)>);

/// A pty master, kept for its window size.
pub struct Pty(OwnedFd);

impl Pty {
    /// `TIOCSWINSZ`: what `kubectl exec -t` sends on its resize stream.
    pub fn resize(&self, width: u16, height: u16) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let ws = libc::winsize { ws_row: height, ws_col: width, ws_xpixel: 0, ws_ypixel: 0 };
            // SAFETY: a valid descriptor and a winsize that outlives the call.
            if unsafe { libc::ioctl(self.0.as_raw_fd(), libc::TIOCSWINSZ, &ws) } != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (width, height);
            Err(io::Error::from(io::ErrorKind::Unsupported))
        }
    }
}

/// Make the descriptors for `io`.
pub fn make(io: &ExecIo) -> io::Result<(Stdio, ChildEnds)> {
    let mut ours = Stdio::default();
    let mut theirs = Vec::new();
    if io.tty {
        let (master, slave) = open_pty()?;
        // Reads and writes on the master go through the blocking pool: a pty
        // is not a pipe, and tokio's pipe types refuse it.
        let read = std::fs::File::from(master.try_clone()?);
        let write = std::fs::File::from(master.try_clone()?);
        if io.stdout || io.stderr {
            ours.stdout = Some(Box::pin(tokio::fs::File::from_std(read)));
        }
        if io.stdin {
            ours.stdin = Some(Box::pin(tokio::fs::File::from_std(write)));
        }
        ours.pty = Some(Pty(master));
        for fd in 0..3 {
            theirs.push((fd, slave.try_clone()?));
        }
        return Ok((ours, ChildEnds(theirs)));
    }
    if io.stdin {
        let (r, w) = pipe()?;
        ours.stdin = Some(Box::pin(tokio::net::unix::pipe::Sender::from_owned_fd(w)?));
        theirs.push((0, r));
    }
    if io.stdout {
        let (r, w) = pipe()?;
        ours.stdout = Some(Box::pin(tokio::net::unix::pipe::Receiver::from_owned_fd(r)?));
        theirs.push((1, w));
    }
    if io.stderr {
        let (r, w) = pipe()?;
        ours.stderr = Some(Box::pin(tokio::net::unix::pipe::Receiver::from_owned_fd(r)?));
        theirs.push((2, w));
    }
    Ok((ours, ChildEnds(theirs)))
}

/// A close-on-exec pipe: (read, write).
pub fn pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0i32; 2];
    // SAFETY: fds has room for the two descriptors pipe2 writes.
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: pipe2 succeeded, so both are open descriptors we now own.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

/// A new pty: (master, slave), both close-on-exec here (the engine dup2s the
/// slave onto the process's 0–2).
fn open_pty() -> io::Result<(OwnedFd, OwnedFd)> {
    // SAFETY: plain libc calls on descriptors this function owns; every
    // failure closes what was opened (OwnedFd) before returning.
    unsafe {
        let m = libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC);
        if m < 0 {
            return Err(io::Error::last_os_error());
        }
        let master = OwnedFd::from_raw_fd(m);
        if libc::grantpt(m) != 0 || libc::unlockpt(m) != 0 {
            return Err(io::Error::last_os_error());
        }
        let mut name = [0 as libc::c_char; 128];
        if libc::ptsname_r(m, name.as_mut_ptr(), name.len()) != 0 {
            return Err(io::Error::last_os_error());
        }
        let s = libc::open(name.as_ptr(), libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC);
        if s < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((master, OwnedFd::from_raw_fd(s)))
    }
}

/// A wait status as Kubernetes reports it: the exit code, or 128 + the signal.
pub fn exit_code(status: u32) -> i32 {
    let sig = (status & 0x7f) as i32;
    if sig != 0 { 128 + sig } else { ((status >> 8) & 0xff) as i32 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn pipes_carry_each_stream_it_asked_for_and_none_it_did_not() {
        let io = ExecIo { stdin: true, stdout: true, stderr: false, tty: false };
        let (mut ours, theirs) = make(&io).unwrap();
        assert!(ours.stderr.is_none() && ours.pty.is_none());
        let fds: Vec<i32> = theirs.0.iter().map(|(fd, _)| *fd).collect();
        assert_eq!(fds, [0, 1]);
        let mut ends = theirs.0.into_iter();
        let mut child_in = std::fs::File::from(ends.next().unwrap().1);
        let mut child_out = std::fs::File::from(ends.next().unwrap().1);

        ours.stdin.as_mut().unwrap().write_all(b"ping").await.unwrap();
        ours.stdin = None; // EOF for the child
        let mut got = String::new();
        child_in.read_to_string(&mut got).unwrap();
        assert_eq!(got, "ping");

        child_out.write_all(b"pong").unwrap();
        drop(child_out);
        let mut out = Vec::new();
        ours.stdout.as_mut().unwrap().read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"pong");
    }

    #[tokio::test]
    async fn a_tty_is_one_slave_on_all_three_and_resizes() {
        let io = ExecIo { stdin: true, stdout: true, stderr: true, tty: true };
        let (mut ours, theirs) = make(&io).unwrap();
        assert_eq!(theirs.0.iter().map(|(fd, _)| *fd).collect::<Vec<_>>(), [0, 1, 2]);
        assert!(ours.stderr.is_none(), "a terminal merges stderr into stdout");
        ours.pty.as_ref().unwrap().resize(120, 40).unwrap();
        // What the slave reads back as its size.
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
            assert_eq!(unsafe { libc::ioctl(theirs.0[0].1.as_raw_fd(), libc::TIOCGWINSZ, &mut ws) }, 0);
            assert_eq!((ws.ws_col, ws.ws_row), (120, 40));
        }
        // Output written on the slave reads on the master.
        let mut slave = std::fs::File::from(theirs.0.into_iter().nth(1).unwrap().1);
        slave.write_all(b"hi\n").unwrap();
        let mut buf = [0u8; 16];
        let n = ours.stdout.as_mut().unwrap().read(&mut buf).await.unwrap();
        assert!(buf[..n].starts_with(b"hi"), "{:?}", &buf[..n]);
    }

    #[test]
    fn wait_statuses_read_as_kubernetes_exit_codes() {
        assert_eq!(exit_code(0), 0);
        assert_eq!(exit_code(3 << 8), 3);
        assert_eq!(exit_code(9), 137);
    }
}
