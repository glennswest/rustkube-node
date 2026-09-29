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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Dependency {
    Claim(String, String),
    Image(String),
    Driver(String),
}

pub fn dependencies(key: &Key, object: &Value) -> HashSet<Dependency> {
    let mut out = HashSet::new();
    for volume in object["spec"]["volumes"].as_array().into_iter().flatten() {
        if let Some(name) = volume["persistentVolumeClaim"]["claimName"].as_str() {
            out.insert(Dependency::Claim(key.namespace.clone(), name.into()));
        }
        if !volume["ephemeral"].is_null() {
            if let Some(name) = volume["name"].as_str() {
                out.insert(Dependency::Claim(key.namespace.clone(), format!("{}-{name}", key.name)));
            }
        }
        if let Some(driver) = volume["csi"]["driver"].as_str() {
            out.insert(Dependency::Driver(driver.into()));
        }
        for field in ["containerDisk", "dataVolume"] {
            if let Some(image) = volume[field][if field == "dataVolume" { "name" } else { "image" }].as_str() {
                out.insert(Dependency::Image(image.into()));
            }
        }
    }
    for field in ["containers", "initContainers"] {
        for container in object["spec"][field].as_array().into_iter().flatten() {
            if let Some(image) = container["image"].as_str() {
                out.insert(Dependency::Image(image.into()));
            }
        }
    }
    out
}

#[derive(Default)]
struct Desired {
    sources: HashMap<String, HashMap<Key, Value>>,
    objects: HashMap<Key, Value>,
    dependents: HashMap<Dependency, HashSet<Key>>,
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
    operations: HashMap<Resource, Arc<tokio::sync::Mutex<()>>>,
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
                .entry(key.clone())
                .and_modify(|old| { if *access == Access::Exclusive { *old = *access; } })
                .or_insert(*access);
            state
                .owned
                .entry(key.clone())
                .or_default()
                .insert(resource.clone());
        }
        true
    }

    /// Shared filesystem holders may coexist, but stage/unstage/clone mutations
    /// for one claim must not run simultaneously. Never wait holding a subset.
    pub fn try_operations(&self, claims: &[(Resource, Access)]) -> Option<Vec<tokio::sync::OwnedMutexGuard<()>>> {
        let mut state = self.state.lock().unwrap();
        let mut guards = Vec::new();
        let mut seen = HashSet::new();
        for (resource, _) in claims {
            if seen.insert(resource.clone()) {
                let lock = state.operations.entry(resource.clone()).or_default().clone();
                guards.push(lock.try_lock_owned().ok()?);
            }
        }
        Some(guards)
    }

    pub fn claims(&self,key:&Key) -> Vec<(Resource,Access)> {
        let state=self.state.lock().unwrap();
        state.owned.get(key).into_iter().flatten().filter_map(|resource| {
            if matches!(resource,Resource::Claim(..)) {
                state.holders.get(resource)?.get(key).map(|mode|(resource.clone(),*mode))
            } else {None}
        }).collect()
    }

    pub fn holder(&self, resource: &Resource) -> Option<Key> {
        self.state.lock().unwrap().holders.get(resource)?.keys().next().cloned()
    }

    /// Recovery records are facts, even if a previous kubelet admitted
    /// conflicting users. Record all holders; never erase one to make a new
    /// admission pass. Unknown claims must be handled by the startup barrier.
    pub fn seed(&self, key: &Key, claims: &[(Resource, Access)]) {
        let mut state = self.state.lock().unwrap();
        for (resource, access) in std::iter::once((Resource::Name(key.kind,
            key.namespace.clone(),key.name.clone()), Access::Exclusive)).chain(claims.iter().cloned()) {
            state.holders.entry(resource.clone()).or_default().entry(key.clone())
                .and_modify(|old| { if access == Access::Exclusive { *old = access; } }).or_insert(access);
            state.owned.entry(key.clone()).or_default().insert(resource);
        }
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
    desired: Mutex<Desired>,
    pub reservations: Arc<Reservations>,
}
impl Executor {
    pub fn new() -> Arc<Self> {
        let ready = WorkQueue::new();
        Arc::new(Self {
            reservations: Arc::new(Reservations::new(ready.clone())),
            ready,
            desired: Mutex::new(Desired::default()),
        })
    }

    pub fn replace(&self, kind: Kind, objects: &[Value]) -> anyhow::Result<()> {
        self.replace_source(&format!("{kind:?}"), kind, objects)
    }

    /// Each authoritative source has its own deletion boundary. In particular,
    /// reading static manifests cannot delete API Pods during an API outage.
    pub fn replace_source(&self, source: &str, kind: Kind, objects: &[Value]) -> anyhow::Result<()> {
        let mut incoming = HashMap::new();
        for object in objects {
            let key = Key::of(kind, object)?;
            anyhow::ensure!(incoming.insert(key, object.clone()).is_none(), "duplicate workload UID");
        }
        let mut state = self.desired.lock().unwrap();
        state.sources.insert(source.into(), incoming);
        let objects: HashMap<_, _> = state.sources.values().flat_map(|s| s.iter())
            .map(|(k,v)| (k.clone(),v.clone())).collect();
        let mut changed: HashSet<_> = state.objects.keys().filter(|k| !objects.contains_key(*k)).cloned().collect();
        for (key, object) in &objects {
            // Status and resourceVersion echoes are not desired-state changes.
            if state.objects.get(key).map(intent) != Some(intent(object)) {
                changed.insert(key.clone());
            }
        }
        state.dependents.clear();
        for (key, object) in &objects {
            for dependency in dependencies(key, object) {
                state.dependents.entry(dependency).or_default().insert(key.clone());
            }
        }
        state.objects = objects;
        drop(state);
        for key in changed { self.ready.add(key); }
        Ok(())
    }

    pub fn wake_dependency(&self, dependency: &Dependency) {
        let keys = self.desired.lock().unwrap().dependents.get(dependency).cloned().unwrap_or_default();
        for key in keys { self.ready.add(key); }
    }

    pub fn wake_kind(&self, kind: Kind) {
        let keys: Vec<_> = self.desired.lock().unwrap().objects.keys()
            .filter(|k| k.kind == kind).cloned().collect();
        for key in keys { self.ready.add(key); }
    }

    pub fn objects(&self, kind: Kind) -> Vec<Value> {
        self.desired.lock().unwrap().objects.iter().filter(|(k,_)| k.kind == kind)
            .map(|(_,v)| v.clone()).collect()
    }

    pub async fn run(&self, adapter: Arc<dyn Adapter>, concurrency: usize) {
        let mut active = FuturesUnordered::new();
        let mut failures = HashMap::<Key, u32>::new();
        loop {
            tokio::select! {
                work = self.ready.next(), if active.len() < concurrency.max(1) => {
                    self.ready.cancel_deadline(work.key());
                    let desired = self.desired.lock().unwrap().objects.get(work.key()).cloned();
                    let adapter=adapter.clone();
                    // Dropping the supervisor must not cancel an in-flight
                    // runtime RPC after the engine accepted its side effect.
                    // An intent change dirties this UID and runs cleanup next.
                    active.push(tokio::spawn(async move {
                        let result = adapter.reconcile(work.key(),desired).await;
                        (work,result)
                    }));
                }
                Some(completed) = active.next(), if !active.is_empty() => {
                    let (work,result)=match completed {
                        Ok(done)=>done,
                        Err(error)=>{ tracing::error!(%error,"workload task panicked"); continue; }
                    };
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

/// Ignore observed status while retaining every input that can change work.
fn intent(object: &Value) -> Value {
    serde_json::json!({"spec":object["spec"], "metadata": {
        "uid":object["metadata"]["uid"], "name":object["metadata"]["name"],
        "namespace":object["metadata"]["namespace"],
        "deletionTimestamp":object["metadata"]["deletionTimestamp"],
        "annotations":object["metadata"]["annotations"],
        "labels":object["metadata"]["labels"],
        "ownerReferences":object["metadata"]["ownerReferences"]}})
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
    fn object(uid: &str, claim: &str) -> Value {
        serde_json::json!({"metadata":{"name":uid,"uid":uid,"namespace":"ns"},
            "spec":{"volumes":[{"name":"disk","persistentVolumeClaim":{"claimName":claim}}]}})
    }

    #[tokio::test]
    async fn source_replacement_never_deletes_another_sources_work() {
        let e = Executor::new();
        e.replace_source("api", Kind::Pod, &[object("api", "a")]).unwrap();
        drop(e.ready.next().await);
        e.replace_source("static", Kind::Pod, &[object("static", "b")]).unwrap();
        drop(e.ready.next().await);
        e.replace_source("static", Kind::Pod, &[]).unwrap();
        let work = e.ready.next().await;
        assert_eq!(work.key().uid,"static");
        assert_eq!(e.objects(Kind::Pod), vec![object("api","a")]);
        assert!(e.replace_source("api", Kind::Pod, &[Value::Null]).is_err());
        assert_eq!(e.objects(Kind::Pod).len(),1);
    }

    #[tokio::test]
    async fn dependency_index_only_wakes_users_and_drops_removed_users() {
        let e = Executor::new();
        e.replace(Kind::Pod, &[object("a","data"), object("b","other")]).unwrap();
        drop(e.ready.next().await); drop(e.ready.next().await);
        e.wake_dependency(&Dependency::Claim("ns".into(),"data".into()));
        let work=e.ready.next().await;
        assert_eq!(work.key().uid,"a"); drop(work);
        let mut changed=object("a","new");
        changed["status"]=serde_json::json!({"phase":"Running"});
        e.replace(Kind::Pod, &[changed.clone()]).unwrap();
        drop(e.ready.next().await); drop(e.ready.next().await);
        e.wake_dependency(&Dependency::Claim("ns".into(),"data".into()));
        assert!(tokio::time::timeout(Duration::from_millis(20),e.ready.next()).await.is_err());
        changed["metadata"]["resourceVersion"]=Value::String("2".into());
        changed["status"]=serde_json::json!({"phase":"Pending"});
        e.replace(Kind::Pod, &[changed]).unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(20),e.ready.next()).await.is_err());
    }

    #[test]
    fn recovery_keeps_all_holders_and_never_downgrades_exclusive_access() {
        let e=Executor::new();
        let resource=Resource::Claim("ns".into(),"data".into());
        let a=key(Kind::Pod,"a"); let b=key(Kind::VirtualMachine,"b");
        e.reservations.seed(&a,&[(resource.clone(),Access::Exclusive)]);
        assert!(e.reservations.acquire(&a,&[(resource.clone(),Access::SharedFilesystem)]));
        assert!(!e.reservations.acquire(&b,&[(resource.clone(),Access::SharedFilesystem)]));
        e.reservations.seed(&b,&[(resource.clone(),Access::Exclusive)]);
        e.reservations.release(&a);
        assert!(!e.reservations.acquire(&key(Kind::Pod,"c"),&[(resource,Access::SharedFilesystem)]));
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
        let task = tokio::spawn(async move { runner.run(Arc::new(SlowVm { completed: tx }), 2).await });
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
