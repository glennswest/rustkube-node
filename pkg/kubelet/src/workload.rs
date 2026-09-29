//! One event-driven executor and resource-admission table for Pods and VMIs.
use apimachinery::workqueue::WorkQueue;
use futures::{stream::FuturesUnordered, StreamExt};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Kind {
    Pod,
    VirtualMachine,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Key {
    pub kind: Kind,
    pub namespace: String,
    pub name: String,
    pub uid: String,
}
impl Key {
    pub fn of(kind: Kind, object: &Value) -> anyhow::Result<Self> {
        let metadata = &object["metadata"];
        let name = metadata["name"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow::anyhow!("workload has no name"))?;
        let uid = metadata["uid"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow::anyhow!("workload has no UID"))?;
        Ok(Self {
            kind,
            namespace: metadata["namespace"].as_str().unwrap_or("default").into(),
            name: name.into(),
            uid: uid.into(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Resource {
    Name(Kind, String, String),
    Claim(String, String),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Access {
    SharedFilesystem,
    Exclusive,
}

#[derive(Default)]
struct Admissions {
    holders: HashMap<Resource, HashMap<Key, Access>>,
    owned: HashMap<Key, HashSet<Resource>>,
    waiting: HashMap<Resource, HashSet<Key>>,
}

/// Reservations describe live resources, not mutex guards. They outlive a
/// reconcile and are released only after runtime/volume cleanup is confirmed.
pub struct Reservations {
    state: Mutex<Admissions>,
    ready: Arc<WorkQueue<Key>>,
}
impl Reservations {
    pub fn new(ready: Arc<WorkQueue<Key>>) -> Self {
        Self {
            state: Mutex::new(Admissions::default()),
            ready,
        }
    }

    /// Atomically reserve the workload name and all claims. VM raw disks and
    /// RWOP claims require Exclusive; ordinary Pod filesystem sharing is allowed.
    pub fn acquire(&self, key: &Key, claims: &[(Resource, Access)]) -> bool {
        let name = (
            Resource::Name(key.kind, key.namespace.clone(), key.name.clone()),
            Access::Exclusive,
        );
        let mut state = self.state.lock().unwrap();
        let requested = std::iter::once(&name)
            .chain(claims.iter())
            .collect::<Vec<_>>();
        let blocked = requested
            .iter()
            .filter(|(resource, access)| {
                state.holders.get(resource).is_some_and(|holders| {
                    holders.iter().any(|(owner, mode)| {
                        owner != key && (*access == Access::Exclusive || *mode == Access::Exclusive)
                    })
                })
            })
            .map(|(resource, _)| resource.clone())
            .collect::<Vec<_>>();
        if !blocked.is_empty() {
            for resource in blocked {
                state
                    .waiting
                    .entry(resource)
                    .or_default()
                    .insert(key.clone());
            }
            return false;
        }
        for (resource, access) in requested {
            state
                .holders
                .entry(resource.clone())
                .or_default()
                .insert(key.clone(), *access);
            state
                .owned
                .entry(key.clone())
                .or_default()
                .insert(resource.clone());
        }
        true
    }

    pub fn release(&self, key: &Key) {
        let mut wake = HashSet::new();
        let mut state = self.state.lock().unwrap();
        for resource in state.owned.remove(key).unwrap_or_default() {
            if let Some(holders) = state.holders.get_mut(&resource) {
                holders.remove(key);
                if holders.is_empty() {
                    state.holders.remove(&resource);
                }
            }
            wake.extend(state.waiting.remove(&resource).unwrap_or_default());
        }
        for waiters in state.waiting.values_mut() {
            waiters.remove(key);
        }
        drop(state);
        for key in wake {
            self.ready.add(key);
        }
    }
}

pub enum Next {
    AwaitEvent,
    After(Duration),
}

#[async_trait::async_trait]
pub trait Adapter: Send + Sync {
    /// None is an observed deletion, not an unavailable desired-state source.
    async fn reconcile(&self, key: &Key, desired: Option<Value>) -> anyhow::Result<Next>;
}

/// Producers publish desired state before enqueueing. All runtime kinds share
/// the same bounded pool; WorkQueue guarantees one active pass per UID.
pub struct Executor {
    pub ready: Arc<WorkQueue<Key>>,
    desired: Mutex<HashMap<Key, Value>>,
    pub reservations: Reservations,
}
impl Executor {
    pub fn new() -> Arc<Self> {
        let ready = WorkQueue::new();
        Arc::new(Self {
            reservations: Reservations::new(ready.clone()),
            ready,
            desired: Mutex::new(HashMap::new()),
        })
    }

    pub fn replace(&self, kind: Kind, objects: &[Value]) -> anyhow::Result<()> {
        // Validate the complete snapshot before allowing absence to mean deletion.
        let incoming = objects
            .iter()
            .map(|v| Ok((Key::of(kind, v)?, v.clone())))
            .collect::<anyhow::Result<HashMap<_, _>>>()?;
        let mut desired = self.desired.lock().unwrap();
        let removed = desired
            .keys()
            .filter(|key| key.kind == kind && !incoming.contains_key(*key))
            .cloned()
            .collect::<Vec<_>>();
        let mut changed = removed.clone();
        for key in removed {
            desired.remove(&key);
        }
        for (key, object) in incoming {
            if desired.get(&key) != Some(&object) {
                desired.insert(key.clone(), object);
                changed.push(key);
            }
        }
        drop(desired);
        for key in changed {
            self.ready.add(key);
        }
        Ok(())
    }

    pub async fn run(&self, adapter: &dyn Adapter, concurrency: usize) {
        let mut active = FuturesUnordered::new();
        let mut failures = HashMap::<Key, u32>::new();
        loop {
            tokio::select! {
                work = self.ready.next(), if active.len() < concurrency.max(1) => {
                    self.ready.cancel_deadline(work.key());
                    let desired = self.desired.lock().unwrap().get(work.key()).cloned();
                    active.push(async move {
                        let result = adapter.reconcile(work.key(),desired).await;
                        (work,result)
                    });
                }
                Some((work,result)) = active.next(), if !active.is_empty() => {
                    match result {
                        Ok(next) => {
                            failures.remove(work.key());
                            if let Next::After(delay) = next { self.ready.add_at(work.key().clone(),tokio::time::Instant::now()+delay); }
                        }
                        Err(error) => {
                            let attempts = failures.entry(work.key().clone()).or_default();
                            *attempts = attempts.saturating_add(1);
                            tracing::warn!(?error, key=?work.key(), "workload reconcile failed");
                            self.ready.add_at(work.key().clone(),tokio::time::Instant::now()+Duration::from_millis((100_u64 << (*attempts).min(8)).min(30_000)));
                        }
                    }
                    drop(work);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(kind: Kind, uid: &str) -> Key {
        Key {
            kind,
            namespace: "ns".into(),
            name: uid.into(),
            uid: uid.into(),
        }
    }
    struct SlowVm {
        completed: tokio::sync::mpsc::UnboundedSender<Kind>,
    }
    #[async_trait::async_trait]
    impl Adapter for SlowVm {
        async fn reconcile(&self, key: &Key, _desired: Option<Value>) -> anyhow::Result<Next> {
            if key.kind == Kind::VirtualMachine {
                std::future::pending::<()>().await;
            }
            self.completed.send(key.kind).unwrap();
            Ok(Next::AwaitEvent)
        }
    }
    #[tokio::test]
    async fn one_pool_runs_a_pod_while_a_vm_waits() {
        let executor = Executor::new();
        executor.ready.add(key(Kind::VirtualMachine, "slow"));
        executor.ready.add(key(Kind::Pod, "fast"));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runner = executor.clone();
        let task = tokio::spawn(async move { runner.run(&SlowVm { completed: tx }, 2).await });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap(),
            Some(Kind::Pod)
        );
        task.abort();
        let _ = task.await;
    }
    #[tokio::test]
    async fn pod_and_vm_share_claim_admission_and_release_wakes_waiter() {
        let ready = WorkQueue::new();
        let reservations = Reservations::new(ready.clone());
        let pod = key(Kind::Pod, "pod");
        let vm = key(Kind::VirtualMachine, "vm");
        let claim = Resource::Claim("ns".into(), "data".into());
        assert!(reservations.acquire(&pod, &[(claim.clone(), Access::SharedFilesystem)]));
        assert!(!reservations.acquire(&vm, &[(claim.clone(), Access::Exclusive)]));
        reservations.release(&pod);
        let work = tokio::time::timeout(Duration::from_secs(1), ready.next())
            .await
            .unwrap();
        assert_eq!(work.key(), &vm);
        assert!(reservations.acquire(&vm, &[(claim, Access::Exclusive)]));
    }
    #[test]
    fn failed_admission_reserves_no_partial_claims() {
        let ready = WorkQueue::new();
        let reservations = Reservations::new(ready);
        let a = Resource::Claim("ns".into(), "a".into());
        let b = Resource::Claim("ns".into(), "b".into());
        assert!(reservations.acquire(&key(Kind::Pod, "holder"), &[(b.clone(), Access::Exclusive)]));
        assert!(!reservations.acquire(
            &key(Kind::VirtualMachine, "blocked"),
            &[(a.clone(), Access::Exclusive), (b, Access::Exclusive)]
        ));
        assert!(reservations.acquire(&key(Kind::Pod, "unrelated"), &[(a, Access::Exclusive)]));
    }
}
