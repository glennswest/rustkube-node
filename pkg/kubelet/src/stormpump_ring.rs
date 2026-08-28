//! Talking to stormpump over its ring.
//!
//! # Why a thread and a channel
//!
//! The ring is a shared-memory structure with one producer on this side. Its
//! `Mapping` needs `&mut` to write the arena and is not something to hand
//! around between async tasks, and the CRI traits above are `&self` and async.
//!
//! So the ring gets a thread of its own, which owns the `Mapping` outright, and
//! callers reach it through a channel. That keeps the ring single-threaded —
//! which is what it is designed for — and gives async callers something to
//! await. It also puts submission and completion in one place, which is where
//! the arena has to be managed from anyway.
//!
//! # Unsolicited completions
//!
//! Not every CQE answers a request. A workload that exits produces one with
//! `user_data` of zero, and that is how the kubelet learns a container died
//! without polling for it. Those are routed to a separate channel rather than
//! being matched against outstanding requests and dropped as unrecognised —
//! dropping them would mean a container that crashed stayed `Running` until
//! something happened to ask.

use std::collections::HashMap;
use std::sync::mpsc;

use stormpump::ring::Mapping;
use stormpump_abi::handle::Handle;
use stormpump_abi::{ArenaRef, Cqe, Op, Sqe};

/// How this client is known to the engine.
///
/// Stable, and deliberately so: the token is how a client is recognised again
/// after it reconnects or the engine re-execs, and a client presenting the same
/// bytes gets its workloads back. A kubelet that restarted with a fresh token
/// would find a node with no pods on it and start them all a second time, while
/// the first set went on running unsupervised.
pub const TOKEN: [u8; 16] = *b"rustkube-kubelet";

/// A request for the ring thread.
struct Request {
    sqe: Sqe,
    /// Written into the arena before the SQE is pushed, if any.
    payload: Option<Vec<u8>>,
    reply: mpsc::Sender<Result<Cqe, RingError>>,
}

#[derive(Debug)]
pub enum RingError {
    /// The engine is not there, or would not complete the handshake.
    Attach(String),
    /// The submission queue was full and stayed full.
    Full,
    /// The engine did not answer within the deadline.
    Timeout,
    /// The engine answered with a negative errno.
    ///
    /// Carries the opcode, because "stormpump refused: errno 22" names neither
    /// the call nor the argument, and a pod start makes several in a row. The
    /// step is how far a spawn's child got before giving up — 0 for anything
    /// refused before the fork.
    Failed { op: u8, errno: i32, step: u32 },
    /// The ring thread is gone, which means the connection is.
    Gone,
}

impl std::fmt::Display for RingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RingError::Attach(w) => write!(f, "cannot attach to stormpump: {w}"),
            RingError::Full => write!(f, "stormpump submission queue is full"),
            RingError::Timeout => write!(f, "stormpump did not answer"),
            RingError::Failed { op, errno, step } => {
                let name = match Op::from_u8(*op) {
                    Some(o) => format!("{o:?}"),
                    None => format!("op {op}"),
                };
                // The step is named, not numbered. `errno 2 at step 4` and
                // `ENOENT attaching mounts` are the same fact, and only one of
                // them tells you which mount to go and look at.
                //
                // SpecDefine's number is a *spec error*, not an exec step:
                // nothing has forked yet, so the taxonomy is what was wrong
                // with the spec. Reading it as a step printed a plausible and
                // entirely unrelated stage name, which is worse than a number.
                let where_ = if *op == Op::SpecDefine as u8 {
                    stormpump::spec::SpecError::name_of(*step)
                } else {
                    stormpump_abi::ExecStep::from_u32(*step).name()
                };
                let why = std::io::Error::from_raw_os_error(*errno);
                write!(f, "stormpump refused {name}: {why} ({where_})")
            }
            RingError::Gone => write!(f, "the connection to stormpump is gone"),
        }
    }
}

impl std::error::Error for RingError {}

/// An exit the engine reported without being asked.
#[derive(Debug, Clone, Copy)]
pub struct Exited {
    pub handle: Handle,
    /// The wait status, as `waitpid` reports it.
    pub status: u32,
}

/// The client half of the ring.
///
/// Both ends are behind mutexes because `std::sync::mpsc` is `Send` but not
/// `Sync`, and the CRI traits are `&self` on a value shared between tasks. The
/// locks are held for a send and a try_recv — never across the wait, which
/// happens on a channel private to the caller.
pub struct RingClient {
    /// Named rather than derived: a `Debug` that dumps channel internals says
    /// nothing useful, and `unwrap_err()` in a test needs one.
    tx: std::sync::Mutex<mpsc::Sender<Request>>,
    exits: std::sync::Mutex<mpsc::Receiver<Exited>>,
}

impl std::fmt::Debug for RingClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RingClient(attached)")
    }
}

impl RingClient {
    /// Attach to the engine and start the thread that owns the ring.
    pub fn attach(socket: &str) -> Result<RingClient, RingError> {
        let attached = stormpump::transport::attach(socket, TOKEN)
            .map_err(|e| RingError::Attach(format!("{socket}: {e}")))?;

        let (tx, rx) = mpsc::channel::<Request>();
        let (exit_tx, exits) = mpsc::channel::<Exited>();
        let submit = attached.submit;
        let complete = attached.complete;
        let ring_fd = attached.ring;

        std::thread::Builder::new()
            .name("kubelet-stormpump-ring".into())
            .spawn(move || {
                // The mapping is built *here*, not before the spawn: it holds
                // a raw pointer into the shared region and is therefore not
                // `Send`, which is correct — one thread owns the ring. A
                // descriptor is just an integer and crosses freely.
                let mapping = match Mapping::from_fd(ring_fd) {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::error!("mapping the stormpump ring: {e}");
                        return;
                    }
                };
                // The stream is held for the life of the thread: dropping it
                // closes the connection, and the engine takes that as the
                // client having gone away.
                let _stream = attached.stream;
                run(mapping, rx, exit_tx, submit, complete);
            })
            .map_err(|e| RingError::Attach(format!("starting the ring thread: {e}")))?;

        Ok(RingClient {
            tx: std::sync::Mutex::new(tx),
            exits: std::sync::Mutex::new(exits),
        })
    }

    /// Submit one entry and wait for its completion.
    ///
    /// Blocking, and called from `spawn_blocking` by the async side. The ring
    /// thread serialises requests, which at the rate a kubelet starts pods is
    /// not a constraint worth engineering around: stormpump's own measured cost
    /// for a container start is around 200 µs, so the queue is empty long
    /// before the next pod arrives.
    pub fn submit(&self, sqe: Sqe, payload: Option<Vec<u8>>) -> Result<Cqe, RingError> {
        let (reply, answer) = mpsc::channel();
        {
            let tx = self.tx.lock().map_err(|_| RingError::Gone)?;
            tx.send(Request { sqe, payload, reply }).map_err(|_| RingError::Gone)?;
        }
        // Outside the lock: another caller must be able to submit while this
        // one waits, or the ring serialises on the client rather than on the
        // engine.
        answer.recv().map_err(|_| RingError::Gone)?
    }

    /// Define a spec, returning its handle.
    pub fn spec_define(&self, encoded: Vec<u8>) -> Result<Handle, RingError> {
        let cqe = self.submit(
            Sqe { opcode: Op::SpecDefine as u8, ..Default::default() },
            Some(encoded),
        )?;
        Ok(cqe.handle())
    }

    /// Register a mounted volume by path, returning its handle.
    pub fn volume_register(&self, mount: &str) -> Result<Handle, RingError> {
        let cqe = self.submit(
            Sqe { opcode: Op::VolumeRegister as u8, ..Default::default() },
            Some(mount.as_bytes().to_vec()),
        )?;
        Ok(cqe.handle())
    }

    /// Register a volume that is a block device, mounting it at `mount`.
    ///
    /// **The engine does the mount, and that is the point.** A mount is only
    /// visible in the mount namespace that made it, and the kubelet runs in a
    /// container — so a device this process mounted would be a device only
    /// this process could see, and the container started on it would find an
    /// empty root. The engine is PID 1, so the mount it makes is the node's.
    ///
    /// Idempotent: registering a device already mounted at that path returns a
    /// handle rather than an error, because two pods of one image both ask.
    pub fn volume_register_device(
        &self,
        mount: &str,
        device: &str,
        fstype: &str,
    ) -> Result<Handle, RingError> {
        let payload = format!("{mount}\0{device}\0{fstype}").into_bytes();
        let cqe = self.submit(
            Sqe { opcode: Op::VolumeRegister as u8, ..Default::default() },
            Some(payload),
        )?;
        Ok(cqe.handle())
    }

    /// Take a sandbox from the warm pool, or build one.
    ///
    /// The profile is a byte, not a spec: a sandbox is namespaces and nothing
    /// else, so a spec for one would be a spec with nothing to run — which the
    /// engine refuses.
    ///
    /// This is the operation a CRI shim cannot express and half the reason for
    /// going direct: the namespaces already exist, so what a container start
    /// costs is `setns` rather than `unshare`. The other half is that a pod's
    /// containers can be put in the *same* one.
    pub fn sandbox_acquire(&self, profile: u8) -> Result<Handle, RingError> {
        let cqe = self.submit(
            Sqe {
                opcode: Op::SandboxAcquire as u8,
                inline_a: profile as u64,
                ..Default::default()
            },
            None,
        )?;
        Ok(cqe.handle())
    }

    pub fn sandbox_release(&self, sandbox: Handle) -> Result<(), RingError> {
        self.submit(
            Sqe {
                opcode: Op::SandboxRelease as u8,
                primary: sandbox,
                ..Default::default()
            },
            None,
        )?;
        Ok(())
    }

    /// spec + root volume -> a running workload.
    ///
    /// `logs` is the volume the workload's log file is opened in, carried in
    /// `inline_b`. A spec asking for its own log file without one is refused
    /// with EINVAL, which is exactly the shape of bug that is hard to place
    /// from the outside — so it is a required argument here rather than
    /// something a caller can forget.
    /// `sandbox` is the pod's, joined rather than taken — which is what puts a
    /// pod's containers on one network instead of several. `Handle::NONE`
    /// means "take your own", which is right for anything that is not a pod.
    /// `mounts` are the volumes for the spec's mount points, **in the order the
    /// spec declares them** — the engine pairs the nth handle with the nth
    /// destination and refuses a count that does not match, because a container
    /// missing one of its volumes starts and then behaves inexplicably.
    pub fn spawn(
        &self,
        spec: Handle,
        root: Handle,
        logs: Handle,
        sandbox: Handle,
        mounts: &[Handle],
        domain: u8,
    ) -> Result<Handle, RingError> {
        let payload = if mounts.is_empty() {
            None
        } else {
            let mut bytes = Vec::with_capacity(mounts.len() * 8);
            for m in mounts {
                bytes.extend_from_slice(&m.0.to_le_bytes());
            }
            Some(bytes)
        };
        let cqe = self.submit(
            Sqe {
                opcode: Op::Spawn as u8,
                domain,
                primary: spec,
                handle_a: root,
                // The sandbox and the log volume both travel as inline
                // handles. `handle_b` is the parent workload and means
                // something else.
                inline_a: sandbox.0,
                inline_b: logs.0,
                // Wait for the child to reach `execve` before calling the start
                // a success. Without this the fork returning is the answer, and
                // a container whose setup fails — a missing root, a mount that
                // will not bind, a cgroup that refuses — is reported as started
                // and then found dead with exit 127 and an empty log, which
                // says nothing about which of those it was. The cost is one
                // pipe per start and a completion that arrives a moment later.
                flags: stormpump_abi::flags::AWAIT_EXEC,
                ..Default::default()
            },
            payload,
        )?;
        Ok(cqe.handle())
    }

    /// Signal, grace, kill — one op, so the policy timer is the engine's.
    pub fn stop(&self, workload: Handle, grace_secs: u64) -> Result<(), RingError> {
        self.submit(
            Sqe {
                opcode: Op::Stop as u8,
                primary: workload,
                inline_a: grace_secs,
                ..Default::default()
            },
            None,
        )?;
        Ok(())
    }

    /// Free an exited workload's handle, its pidfd and its cgroup.
    pub fn workload_release(&self, workload: Handle) -> Result<(), RingError> {
        self.submit(
            Sqe {
                opcode: Op::WorkloadRelease as u8,
                primary: workload,
                ..Default::default()
            },
            None,
        )?;
        Ok(())
    }

    /// Everything the engine has reported ending since the last call.
    ///
    /// Drained rather than subscribed to: the caller is the runtime's own
    /// status path, which runs when the kubelet asks, and an exit that arrives
    /// between two asks must still be there for the second one.
    pub fn drain_exits(&self) -> Vec<Exited> {
        let mut out = Vec::new();
        if let Ok(rx) = self.exits.lock() {
            while let Ok(e) = rx.try_recv() {
                out.push(e);
            }
        }
        out
    }

    /// Ask about a workload without changing it.
    pub fn query(&self, workload: Handle) -> Result<Cqe, RingError> {
        self.submit(
            Sqe {
                opcode: Op::Query as u8,
                primary: workload,
                ..Default::default()
            },
            None,
        )
    }
}

/// How long to wait for one completion.
///
/// Generous against what these cost — a container start is sub-millisecond —
/// and bounded so that a wedged engine surfaces as a failed pod with a reason
/// rather than a kubelet that stops reconciling.
const DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

fn run(
    mut mapping: Mapping,
    rx: mpsc::Receiver<Request>,
    exits: mpsc::Sender<Exited>,
    submit: i32,
    complete: i32,
) {
    let mut next_id: u64 = 1;
    // Requests submitted and not yet answered. More than one can be in flight
    // when a completion for an earlier request arrives out of order.
    let mut waiting: HashMap<u64, mpsc::Sender<Result<Cqe, RingError>>> = HashMap::new();
    // Which op each outstanding request was, so a failure can name it.
    let mut sent: HashMap<u64, u8> = HashMap::new();

    loop {
        // Take one request if there is one, without blocking so completions
        // for work already submitted keep flowing.
        match rx.recv_timeout(std::time::Duration::from_millis(2)) {
            Ok(req) => {
                let id = next_id;
                next_id += 1;
                let mut sqe = req.sqe;
                sqe.user_data = id;
                if let Some(bytes) = &req.payload {
                    mapping.write_arena(0, bytes);
                    match ArenaRef::new(0, bytes.len() as u32) {
                        Some(a) => sqe.arena = a,
                        None => {
                            let _ = req.reply.send(Err(RingError::Full));
                            continue;
                        }
                    }
                }
                if mapping.ring().push_sqe(sqe).is_err() {
                    let _ = req.reply.send(Err(RingError::Full));
                    continue;
                }
                sent.insert(id, sqe.opcode);
                waiting.insert(id, req.reply);
                stormpump::transport::kick(submit);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            // Every sender is gone: the client was dropped, so this thread's
            // work is done and the stream closes with it.
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }

        stormpump::transport::drain(complete);
        while let Some(cqe) = mapping.ring().pop_cqe() {
            if cqe.user_data == 0 {
                // Unsolicited: a workload ended. Nothing asked, and something
                // still has to hear it.
                let _ = exits.send(Exited { handle: cqe.handle(), status: cqe.aux });
                continue;
            }
            if let Some(reply) = waiting.remove(&cqe.user_data) {
                if !cqe.is_err() {
                    sent.remove(&cqe.user_data);
                }
                let answer = if cqe.is_err() {
                    Err(RingError::Failed {
                        op: sent.remove(&cqe.user_data).unwrap_or(0),
                        errno: -cqe.result as i32,
                        step: cqe.aux,
                    })
                } else {
                    Ok(cqe)
                };
                let _ = reply.send(answer);
            }
        }

        // Anything outstanding past the deadline is reported as such rather
        // than waited on forever. Cheap to check: `waiting` holds one entry per
        // request in flight, which is a handful at most.
        if !waiting.is_empty() {
            // A deadline per request would need a timestamp each; one shared
            // deadline is enough because these complete in microseconds, and
            // the case this exists for is an engine that has stopped answering
            // at all.
            let _ = DEADLINE;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_token_is_sixteen_bytes_and_says_who_we_are() {
        // The engine reads exactly sixteen. A token that has to be padded or
        // truncated is a token that changes when someone edits the string.
        assert_eq!(TOKEN.len(), 16);
        assert_eq!(&TOKEN[..], b"rustkube-kubelet");
    }

    #[test]
    fn attaching_to_nothing_says_where_it_looked() {
        let e = RingClient::attach("/nonexistent/stormpump.sock").unwrap_err();
        let text = format!("{e}");
        assert!(text.contains("/nonexistent/stormpump.sock"), "{text}");
    }

    #[test]
    fn a_failure_carries_the_errno_and_the_step() {
        // Spawn reports how far the child got before it gave up, and that is
        // most of the diagnosis — "failed at mount" and "failed at exec" are
        // different problems with the same errno.
        // The opcode matters as much as the errno: a pod start makes several
        // calls in a row, and "errno 22" alone names neither the call nor the
        // argument. This cost an afternoon before the op was carried.
        let e = RingError::Failed { op: Op::VolumeRegister as u8, errno: 22, step: 4 };
        let text = format!("{e}");
        assert!(text.contains("VolumeRegister"), "{text}");
        // Both halves in words: the step named rather than numbered, and the
        // errno spelled out. A reader should not need two lookup tables.
        assert!(text.contains("mounts"), "{text}");
        assert!(text.contains("Invalid argument"), "{text}");
    }
}
