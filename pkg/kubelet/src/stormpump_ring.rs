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

/// `DEPOSIT_WITHDRAW` (stormpump#63), by number: the locked stormpump-abi
/// predates `Op::DepositWithdraw`, and the lock cannot move until stormvm#65.
pub const OP_DEPOSIT_WITHDRAW: u8 = 9;

/// The engine's answer for a handle that was already released.
pub const ESTALE: i32 = 116;
/// The engine's answer for an op it does not know, or a malformed argument.
pub const EINVAL: i32 = 22;

/// A request for the ring thread.
struct Request {
    sqe: Sqe,
    /// Written into the arena before the SQE is pushed, if any.
    payload: Option<Vec<u8>>,
    /// Copy the request's arena region back after it completes: the engine
    /// answered in it (`QUERY`'s stats block).
    read_back: bool,
    reply: mpsc::Sender<Result<Reply, RingError>>,
    /// When the caller stops waiting (#99): [`DEADLINE`] from when it asked,
    /// whether the request is still queued, waiting for the arena, or in
    /// flight.
    expires: std::time::Instant,
}

/// A completion, and the arena region the engine answered in when the request
/// asked for it back.
struct Reply {
    cqe: Cqe,
    arena: Vec<u8>,
}

/// A descriptor to hand the engine, under a name.
///
/// A separate message rather than part of `Request` because it does not go
/// through the ring at all: a descriptor cannot travel through shared memory,
/// so it goes over the attach socket with `SCM_RIGHTS` — and that socket is
/// owned by the ring thread, which is the only reason this is a message
/// instead of a method call.
struct Deposit {
    name: String,
    fd: std::os::fd::OwnedFd,
    reply: mpsc::Sender<Result<(), RingError>>,
}

#[derive(Debug)]
pub enum RingError {
    /// A failure that has already been described, with detail the caller had
    /// and this crate does not — which mount, which path. Carried rather than
    /// re-derived, because the context exists at exactly one place.
    Detail(String),
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
                    // A mount failure packs the mount's index in the high
                    // sixteen bits; the step itself is the low half.
                    stormpump_abi::ExecStep::from_u32(*step & 0xFFFF).name()
                };
                let why = std::io::Error::from_raw_os_error(*errno);
                // The raw step too, when it is one nothing recognises.
                //
                // "setup" is the name for *unknown*, and an unknown step read
                // as a sentence is worse than a number: it looks like a stage
                // that exists. Printing the value turns "which step is setup"
                // into a lookup rather than a guess — this cost a boot.
                // SpecDefine never reaches a *step*: it fails while reading
                // the spec, and what it puts in that field is a SpecError
                // code. Decoding it as a step therefore printed
                // "unrecognised step 14" for what is "argv is not absolute" —
                // a number where a sentence was already available, and a
                // lookup in the wrong table.
                if name == "SpecDefine" {
                    return write!(
                        f,
                        "stormpump refused SpecDefine: {why} ({})",
                        stormpump::spec::SpecError::name_of(*step)
                    );
                }
                match stormpump_abi::ExecStep::from_u32(*step & 0xFFFF) {
                    // Step 0 is "the child never reported one" — which is a
                    // fact about where the failure happened, not a stage. It
                    // read as "setup", a plausible-looking name for a phase
                    // that does not exist, and cost several boots.
                    stormpump_abi::ExecStep::Unknown if *step == 0 => write!(
                        f,
                        "stormpump refused {name}: {why} (no step reported — the failure \
                         was before the child could describe it)"
                    ),
                    stormpump_abi::ExecStep::Unknown => write!(
                        f,
                        "stormpump refused {name}: {why} (unrecognised step {step})"
                    ),
                    _ => write!(f, "stormpump refused {name}: {why} ({where_})"),
                }
            }
            RingError::Detail(msg) => write!(f, "{msg}"),
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
    /// Descriptors to hand over. Its own channel because a descriptor does not
    /// travel through the ring — `SCM_RIGHTS` on the attach socket is the only
    /// way, and that socket belongs to the ring thread.
    deposits: std::sync::Mutex<mpsc::Sender<Deposit>>,
    exits: std::sync::Mutex<mpsc::Receiver<Exited>>,
    exit_changes: tokio::sync::watch::Sender<u64>,
}

impl std::fmt::Debug for RingClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RingClient(attached)")
    }
}

/// The mount a failure refers to, if it names one.
///
/// stormpump packs the index plus one into the high sixteen bits of the step
/// word, so zero means "no mount named" — which is what every non-mount step
/// reports.
pub fn failed_mount_index(step: u32) -> Option<usize> {
    match (step >> 16) as usize {
        0 => None,
        n => Some(n - 1),
    }
}

impl RingClient {
    /// Attach to the engine and start the thread that owns the ring.
    pub fn attach(socket: &str) -> Result<RingClient, RingError> {
        let attached = stormpump::transport::attach(socket, TOKEN)
            .map_err(|e| RingError::Attach(format!("{socket}: {e}")))?;

        let (tx, rx) = mpsc::channel::<Request>();
        let (dep_tx, dep_rx) = mpsc::channel::<Deposit>();
        let (exit_tx, exits) = mpsc::channel::<Exited>();
        let (exit_changes, _) = tokio::sync::watch::channel(0_u64);
        let notify_exits = exit_changes.clone();
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
                let stream = attached.stream;
                run(
                    mapping,
                    rx,
                    dep_rx,
                    &stream,
                    exit_tx,
                    notify_exits,
                    submit,
                    complete,
                );
            })
            .map_err(|e| RingError::Attach(format!("starting the ring thread: {e}")))?;

        Ok(RingClient {
            tx: std::sync::Mutex::new(tx),
            deposits: std::sync::Mutex::new(dep_tx),
            exits: std::sync::Mutex::new(exits),
            exit_changes,
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
        self.request(sqe, payload, false).map(|r| r.cqe)
    }

    fn request(
        &self,
        sqe: Sqe,
        payload: Option<Vec<u8>>,
        read_back: bool,
    ) -> Result<Reply, RingError> {
        let (reply, answer) = mpsc::channel();
        {
            let tx = self.tx.lock().map_err(|_| RingError::Gone)?;
            tx.send(Request {
                sqe,
                payload,
                read_back,
                reply,
                expires: std::time::Instant::now() + DEADLINE,
            })
            .map_err(|_| RingError::Gone)?;
        }
        // Outside the lock: another caller must be able to submit while this
        // one waits, or the ring serialises on the client rather than on the
        // engine.
        //
        // The ring thread answers by the deadline (#99). Waiting twice as
        // long here is only a guard against that thread being stuck itself;
        // a request it dequeues after its deadline is never submitted.
        match answer.recv_timeout(DEADLINE * 2) {
            Ok(answer) => answer,
            Err(mpsc::RecvTimeoutError::Timeout) => Err(RingError::Timeout),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(RingError::Gone),
        }
    }

    /// Hand the engine a descriptor, under a name a spec can claim.
    ///
    /// **This is how a workload gets something it could not open itself** — a
    /// tap, a vhost descriptor, a socket the client already holds. Making a
    /// tap needs CAP_NET_ADMIN; a hypervisor that could make its own would
    /// hold that capability for the life of the guest, so the node makes it
    /// and passes the descriptor instead.
    ///
    /// The name is the whole binding: the spec says "the tap for eth0 arrives
    /// on fd 9", and this says which descriptor that name means.
    pub fn deposit_fd(&self, name: &str, fd: std::os::fd::OwnedFd) -> Result<(), RingError> {
        let (reply, answer) = mpsc::channel();
        {
            let tx = self.deposits.lock().map_err(|_| RingError::Gone)?;
            tx.send(Deposit {
                name: name.to_string(),
                fd,
                reply,
            })
            .map_err(|_| RingError::Gone)?;
        }
        match answer.recv_timeout(DEADLINE * 2) {
            Ok(answer) => answer,
            Err(mpsc::RecvTimeoutError::Timeout) => Err(RingError::Timeout),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(RingError::Gone),
        }
    }

    /// Close a descriptor this client deposited that no spawn consumed
    /// (stormpump#63). `true` when one was closed, `false` when none was held:
    /// never deposited, consumed by a spawn, or already withdrawn. Safe to send
    /// again, and it cannot reach a running workload, whose child holds its own
    /// copy.
    ///
    /// **Why it matters**: a tap exists while anything holds its descriptor. A
    /// VM start that deposited a tap and then failed left the engine holding
    /// it for the life of the connection, and the next start under the same
    /// name met EBUSY at TUNSETIFF.
    ///
    /// Sent after the deposit on the same thread, so the ring thread has put
    /// the descriptor on the socket before the SQE, and the engine collects
    /// waiting deposits before it dispatches a withdraw. An engine older than
    /// the op answers EINVAL, the same as a bad name.
    pub fn deposit_withdraw(&self, name: &str) -> Result<bool, RingError> {
        let cqe = self.submit(
            Sqe {
                opcode: OP_DEPOSIT_WITHDRAW,
                ..Default::default()
            },
            Some(name.as_bytes().to_vec()),
        )?;
        Ok(cqe.aux == 1)
    }

    /// Release a defined spec. ESTALE: it was released already.
    pub fn spec_release(&self, spec: Handle) -> Result<(), RingError> {
        self.submit(
            Sqe {
                opcode: Op::SpecRelease as u8,
                primary: spec,
                ..Default::default()
            },
            None,
        )?;
        Ok(())
    }

    /// Define a spec, returning its handle.
    pub fn spec_define(&self, encoded: Vec<u8>) -> Result<Handle, RingError> {
        let cqe = self.submit(
            Sqe {
                opcode: Op::SpecDefine as u8,
                ..Default::default()
            },
            Some(encoded),
        )?;
        Ok(cqe.handle())
    }

    /// Register a mounted volume by path, returning its handle.
    pub fn volume_register(&self, mount: &str) -> Result<Handle, RingError> {
        let cqe = self.submit(
            Sqe {
                opcode: Op::VolumeRegister as u8,
                ..Default::default()
            },
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
            Sqe {
                opcode: Op::VolumeRegister as u8,
                ..Default::default()
            },
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
    /// Acquire a sandbox, and learn the pid holding its namespaces.
    ///
    /// The pid is what makes the namespaces nameable from outside this ring:
    /// a CNI plugin is handed `/proc/<pid>/ns/net`, never a handle. Zero means
    /// the engine did not report one, which a caller must treat as "no path" —
    /// `/proc/0/ns/net` is not a thing, and passing it to a plugin would fail
    /// somewhere far from here.
    pub fn sandbox_acquire(&self, profile: u8) -> Result<(Handle, u32), RingError> {
        let cqe = self.submit(
            Sqe {
                opcode: Op::SandboxAcquire as u8,
                inline_a: profile as u64,
                ..Default::default()
            },
            None,
        )?;
        Ok((cqe.handle(), cqe.aux))
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

    /// Release a registered volume. The last release of a device mount
    /// unmounts it in the engine; a mount still in use answers EBUSY.
    pub fn volume_release(&self, volume: Handle) -> Result<(), RingError> {
        self.submit(
            Sqe {
                opcode: Op::VolumeRelease as u8,
                primary: volume,
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
    /// Notifications never drain the exit data needed by runtime status.
    pub fn subscribe_exits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.exit_changes.subscribe()
    }

    pub fn drain_exits(&self) -> Vec<Exited> {
        let mut out = Vec::new();
        if let Ok(rx) = self.exits.lock() {
            while let Ok(e) = rx.try_recv() {
                out.push(e);
            }
        }
        out
    }

    /// What a workload has consumed, from its cgroup (#57).
    ///
    /// `QUERY` with room for the stats block. `Ok(None)` when the engine wrote
    /// none: a workload with no cgroup has nothing to report, and that is not a
    /// workload that used nothing.
    pub fn query_stats(
        &self,
        workload: Handle,
    ) -> Result<Option<stormpump_abi::query::Stats>, RingError> {
        let r = self.request(
            Sqe {
                opcode: Op::Query as u8,
                primary: workload,
                ..Default::default()
            },
            Some(vec![0u8; stormpump_abi::query::STATS_END]),
            true,
        )?;
        Ok(stormpump_abi::query::stats(r.cqe.flags, &r.arena))
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

/// The bytes of `region` in `arena`, clipped to what exists: an engine that
/// named a region past the end gets an empty reply, not a panic.
fn arena_region(arena: &[u8], region: ArenaRef) -> Vec<u8> {
    let start = region.offset() as usize;
    let end = start.saturating_add(region.len() as usize);
    arena
        .get(start..end)
        .map(<[u8]>::to_vec)
        .unwrap_or_default()
}

/// How long to wait for one completion.
///
/// Generous against what these cost — a container start is sub-millisecond —
/// and bounded so that a wedged engine surfaces as a failed pod with a reason
/// rather than a kubelet that stops reconciling.
const DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// A request in flight: which op, who is waiting, and until when.
struct Live {
    op: u8,
    reply: mpsc::Sender<Result<Reply, RingError>>,
    expires: std::time::Instant,
}

/// What a completion answers.
enum Answer {
    /// A caller still waiting.
    Waiting(u8, mpsc::Sender<Result<Reply, RingError>>),
    /// A request whose caller gave up at its deadline, or one of this
    /// thread's own undo requests: nobody is told (#99).
    Abandoned(u8),
    /// Nothing this thread sent.
    Unknown,
}

/// Requests in flight, and the ones given up on (#99).
///
/// **A deadline does not un-send a request.** Once an SQE is on the ring the
/// engine may act on it at any moment, and its completion still arrives. So
/// a request past its deadline is answered `Timeout` and moved to
/// `abandoned`, not forgotten: its completion must still free the arena, and
/// a late success must still be undone, because nothing else owns what it
/// made.
#[derive(Default)]
struct Outstanding {
    live: HashMap<u64, Live>,
    abandoned: HashMap<u64, u8>,
    /// Workloads a late spawn started: stopped at once, released on exit.
    orphans: std::collections::HashSet<u64>,
}

impl Outstanding {
    fn insert(&mut self, id: u64, op: u8, reply: mpsc::Sender<Result<Reply, RingError>>, expires: std::time::Instant) {
        self.live.insert(id, Live { op, reply, expires });
    }

    /// One of this thread's own requests, whose answer nobody wants.
    fn internal(&mut self, id: u64, op: u8) {
        self.abandoned.insert(id, op);
    }

    fn complete(&mut self, id: u64) -> Answer {
        if let Some(l) = self.live.remove(&id) {
            return Answer::Waiting(l.op, l.reply);
        }
        match self.abandoned.remove(&id) {
            Some(op) => Answer::Abandoned(op),
            None => Answer::Unknown,
        }
    }

    /// Answer `Timeout` to every request past its deadline, and keep them as
    /// abandoned.
    fn expire(&mut self, now: std::time::Instant) {
        let late: Vec<u64> = self.live.iter().filter(|(_, l)| l.expires <= now).map(|(id, _)| *id).collect();
        for id in late {
            if let Some(l) = self.live.remove(&id) {
                let name = Op::from_u8(l.op).map_or_else(|| format!("op {}", l.op), |o| format!("{o:?}"));
                tracing::warn!("stormpump did not answer {name} within {DEADLINE:?}; given up, its completion will be undone");
                let _ = l.reply.send(Err(RingError::Timeout));
                self.abandoned.insert(id, l.op);
            }
        }
    }
}

/// The request that undoes a late success of `op` (#99): the caller was told
/// it failed, so nothing holds what it made. A spawn's workload is stopped
/// here and released when its exit arrives.
fn undo_late(op: u8, cqe: &Cqe) -> Option<Sqe> {
    if cqe.is_err() {
        return None;
    }
    let (opcode, inline_a) = match Op::from_u8(op)? {
        Op::Spawn => (Op::Stop, 0),
        Op::VolumeRegister => (Op::VolumeRelease, 0),
        Op::SpecDefine => (Op::SpecRelease, 0),
        Op::SandboxAcquire => (Op::SandboxRelease, 0),
        _ => return None,
    };
    Some(Sqe {
        opcode: opcode as u8,
        primary: cqe.handle(),
        inline_a,
        ..Default::default()
    })
}

fn run(
    mut mapping: Mapping,
    rx: mpsc::Receiver<Request>,
    deposits: mpsc::Receiver<Deposit>,
    stream: &std::os::unix::net::UnixStream,
    exits: mpsc::Sender<Exited>,
    exit_changes: tokio::sync::watch::Sender<u64>,
    submit: i32,
    complete: i32,
) {
    let mut next_id: u64 = 1;
    // Requests submitted and not yet answered. More than one can be in flight
    // when a completion for an earlier request arrives out of order.
    let mut outstanding = Outstanding::default();
    // Undo requests not yet on the ring (it was full when they were made).
    let mut undo: std::collections::VecDeque<Sqe> = std::collections::VecDeque::new();
    // **One request owns the arena at a time.** Every payload is written at
    // offset 0, and a reply (`QUERY`'s stats) is written back into the same
    // region, so a second request taking the arena before the first completed
    // would overwrite what the engine has yet to read, or what it answered.
    // The id of the request that has it, whether its region is to be copied
    // back, and a request that is waiting for it.
    let mut arena_holder: Option<(u64, bool)> = None;
    let mut parked: Option<Request> = None;

    loop {
        // Take one request if there is one, without blocking so completions
        // for work already submitted keep flowing.
        // Descriptors first, and out of band: they do not go through the ring
        // at all, and a spawn that names one must not be submitted before the
        // engine has it.
        while let Ok(dep) = deposits.try_recv() {
            let result = stormpump::transport::deposit_fd(
                stream,
                &dep.name,
                std::os::fd::AsRawFd::as_raw_fd(&dep.fd),
            )
            .map_err(|e| RingError::Detail(format!("depositing {}: {e}", dep.name)));
            // The descriptor is dropped here, after the send: the engine has
            // its own copy from SCM_RIGHTS, and holding ours would leak one
            // per NIC per VM.
            drop(dep.fd);
            let _ = dep.reply.send(result);
        }

        // Undo requests first: each is a resource nobody owns.
        while let Some(sqe) = undo.pop_front() {
            let id = next_id;
            next_id += 1;
            let sqe = Sqe { user_data: id, ..sqe };
            if mapping.ring().push_sqe(sqe).is_err() {
                undo.push_front(Sqe { user_data: 0, ..sqe });
                break;
            }
            outstanding.internal(id, sqe.opcode);
            stormpump::transport::kick(submit);
        }

        let next = match parked.take() {
            Some(req) => Ok(req),
            None => rx.recv_timeout(std::time::Duration::from_millis(2)),
        };
        // A request whose caller has stopped waiting is never submitted: it
        // would be a side effect nobody owns (#99).
        let next = match next {
            Ok(req) if req.expires <= std::time::Instant::now() => {
                let _ = req.reply.send(Err(RingError::Timeout));
                continue;
            }
            other => other,
        };
        match next {
            Ok(req) if req.payload.is_some() && arena_holder.is_some() => {
                // Wait for the arena. The completion that frees it is drained
                // below; the pause keeps this from spinning meanwhile.
                parked = Some(req);
                std::thread::sleep(std::time::Duration::from_micros(200));
            }
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
                if req.payload.is_some() {
                    arena_holder = Some((id, req.read_back));
                }
                outstanding.insert(id, sqe.opcode, req.reply, req.expires);
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
                // An orphan's exit: release it; nobody else knows it (#99).
                if outstanding.orphans.remove(&cqe.handle().0) {
                    undo.push_back(Sqe {
                        opcode: Op::WorkloadRelease as u8,
                        primary: cqe.handle(),
                        ..Default::default()
                    });
                }
                // Unsolicited: a workload ended. Nothing asked, and something
                // still has to hear it.
                let _ = exits.send(Exited {
                    handle: cqe.handle(),
                    status: cqe.aux,
                });
                exit_changes.send_modify(|revision| *revision = revision.wrapping_add(1));
                continue;
            }
            // The arena is free again, once what the engine answered in it
            // has been copied out — whoever is (or is no longer) waiting.
            let mut arena = Vec::new();
            if let Some((holder, read_back)) = arena_holder {
                if holder == cqe.user_data {
                    if read_back && !cqe.is_err() {
                        arena = arena_region(mapping.arena(), cqe.arena);
                    }
                    arena_holder = None;
                }
            }
            match outstanding.complete(cqe.user_data) {
                Answer::Waiting(op, reply) => {
                    let answer = if cqe.is_err() {
                        Err(RingError::Failed { op, errno: -cqe.result as i32, step: cqe.aux })
                    } else {
                        Ok(Reply { cqe, arena })
                    };
                    let _ = reply.send(answer);
                }
                Answer::Abandoned(op) => {
                    if let Some(sqe) = undo_late(op, &cqe) {
                        tracing::warn!(op, handle = cqe.handle().0, "a request given up on completed late; undoing it");
                        if op == Op::Spawn as u8 {
                            outstanding.orphans.insert(cqe.handle().0);
                        }
                        undo.push_back(sqe);
                    }
                }
                Answer::Unknown => {}
            }
        }

        // Anything past its deadline is answered `Timeout` rather than waited
        // on forever (#99), and a request parked for the arena too.
        let now = std::time::Instant::now();
        outstanding.expire(now);
        if let Some(req) = parked.take() {
            if req.expires <= now {
                let _ = req.reply.send(Err(RingError::Timeout));
            } else {
                parked = Some(req);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A request past its deadline is answered `Timeout` and kept, so its
    /// late completion is recognised; one that answers in time is not (#99).
    #[test]
    fn a_request_past_its_deadline_times_out_and_is_kept() {
        let mut o = Outstanding::default();
        let now = std::time::Instant::now();
        let (late_tx, late_rx) = mpsc::channel();
        let (ok_tx, ok_rx) = mpsc::channel();
        o.insert(1, Op::Spawn as u8, late_tx, now);
        o.insert(2, Op::Query as u8, ok_tx, now + DEADLINE);
        o.expire(now);
        assert!(matches!(late_rx.try_recv(), Ok(Err(RingError::Timeout))));
        assert!(ok_rx.try_recv().is_err(), "still inside its deadline");
        assert!(matches!(o.complete(1), Answer::Abandoned(op) if op == Op::Spawn as u8));
        assert!(matches!(o.complete(2), Answer::Waiting(..)));
        assert!(matches!(o.complete(3), Answer::Unknown));
        o.internal(4, Op::Stop as u8);
        assert!(matches!(o.complete(4), Answer::Abandoned(_)));
    }

    /// What a late success made is undone; a late failure made nothing (#99).
    #[test]
    fn a_late_success_is_undone() {
        let ok = Cqe::ok(1, Handle(42));
        let undo = |op: Op| undo_late(op as u8, &ok).map(|s| (s.opcode, s.primary));
        assert_eq!(undo(Op::Spawn), Some((Op::Stop as u8, Handle(42))));
        assert_eq!(undo(Op::VolumeRegister), Some((Op::VolumeRelease as u8, Handle(42))));
        assert_eq!(undo(Op::SpecDefine), Some((Op::SpecRelease as u8, Handle(42))));
        assert_eq!(undo(Op::SandboxAcquire), Some((Op::SandboxRelease as u8, Handle(42))));
        assert_eq!(undo(Op::Query), None);
        assert_eq!(undo(Op::Stop), None);
        assert!(undo_late(Op::Spawn as u8, &Cqe::err(1, 22)).is_none());
    }

    /// A reply is read from the region the request offered, and a region the
    /// arena does not have is an empty reply rather than a panic.
    #[test]
    fn a_reply_is_read_from_its_own_region() {
        let arena: Vec<u8> = (0..64u8).collect();
        let r = ArenaRef::new(8, 4).unwrap();
        assert_eq!(arena_region(&arena, r), vec![8, 9, 10, 11]);
        let past = ArenaRef::new(60, 16).unwrap();
        assert!(arena_region(&arena, past).is_empty());
    }

    /// The stats block `QUERY` writes decodes from a region laid out the way
    /// the engine lays it out.
    #[test]
    fn the_stats_block_decodes_from_a_read_back_region() {
        use stormpump_abi::query;
        let st = query::Stats {
            cpu_usage_usec: 7,
            memory_current: 9,
            ..Default::default()
        };
        let mut region = vec![0u8; query::STATS_END];
        region[query::REPLY_LEN..query::STATS_END].copy_from_slice(&st.to_bytes());
        assert_eq!(query::stats(query::WROTE_STATS, &region), Some(st));
        // No flag, no stats: an unwritten block is not a workload that used nothing.
        assert_eq!(query::stats(0, &region), None);
    }

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
        let e = RingError::Failed {
            op: Op::VolumeRegister as u8,
            errno: 22,
            step: 4,
        };
        let text = format!("{e}");
        assert!(text.contains("VolumeRegister"), "{text}");
        // Both halves in words: the step named rather than numbered, and the
        // errno spelled out. A reader should not need two lookup tables.
        assert!(text.contains("mounts"), "{text}");
        assert!(text.contains("Invalid argument"), "{text}");
    }

    /// A mount failure names which mount. Zero keeps meaning "no index", so a
    /// failure from any other step still decodes as it always did.
    #[test]
    fn a_mount_failure_carries_its_index() {
        let pack = |step: u32, idx: usize| (step & 0xFFFF) | (((idx as u32) + 1) << 16);
        let mounts = stormpump_abi::ExecStep::Mounts as u32;

        assert_eq!(failed_mount_index(pack(mounts, 0)), Some(0));
        assert_eq!(failed_mount_index(pack(mounts, 12)), Some(12));
        // The step survives the packing.
        assert_eq!(pack(mounts, 12) & 0xFFFF, mounts);
        // A plain step names no mount.
        assert_eq!(failed_mount_index(mounts), None);
        assert_eq!(failed_mount_index(0), None);
    }
}
