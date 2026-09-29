//! One event-driven executor and resource-admission table for Pods and VMIs.
use apimachinery::workqueue::WorkQueue;
use futures::{stream::FuturesUnordered, StreamExt, FutureExt};
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

/// Joins volume/attachment notifications back to their claim users. Snapshots
/// are committed together: an unavailable collection cannot masquerade as deletion.
#[derive(Default)]
pub struct VolumeIndex {
    observed: HashMap<(String, String), Value>,
    users: HashMap<(String, String), HashSet<Dependency>>,
}
impl VolumeIndex {
    pub fn update(&mut self, claims: &[Value], volumes: &[Value], attachments: &[Value]) -> HashSet<Dependency> {
        let mut observed = HashMap::new();
        let mut users: HashMap<(String,String),HashSet<Dependency>> = HashMap::new();
        for claim in claims {
            let ns = claim["metadata"]["namespace"].as_str().unwrap_or("default");
            let Some(name) = claim["metadata"]["name"].as_str() else {continue};
            let dependency = Dependency::Claim(ns.into(),name.into());
            let key = ("claim".into(),format!("{ns}/{name}"));
            observed.insert(key.clone(),claim.clone());
            users.entry(key).or_default().insert(dependency.clone());
            if let Some(volume) = claim["spec"]["volumeName"].as_str() {
                users.entry(("volume".into(),volume.into())).or_default().insert(dependency);
            }
        }
        for volume in volumes {
            let Some(name) = volume["metadata"]["name"].as_str() else {continue};
            let key = ("volume".into(),name.into());
            observed.insert(key.clone(),volume.clone());
            if let (Some(ns),Some(claim)) = (volume["spec"]["claimRef"]["namespace"].as_str(),volume["spec"]["claimRef"]["name"].as_str()) {
                users.entry(key).or_default().insert(Dependency::Claim(ns.into(),claim.into()));
            }
        }
        for attachment in attachments {
            let (Some(name),Some(volume)) = (attachment["metadata"]["name"].as_str(),attachment["spec"]["source"]["persistentVolumeName"].as_str()) else {continue};
            let key = ("attachment".into(),name.into());
            observed.insert(key.clone(),attachment.clone());
            users.insert(key, users.get(&("volume".into(),volume.into())).cloned().unwrap_or_default());
        }
        let mut changed = HashSet::new();
        for key in observed.keys().chain(self.observed.keys()) {
            if observed.get(key) != self.observed.get(key) || users.get(key) != self.users.get(key) {
                changed.extend(users.get(key).into_iter().flatten().cloned());
                changed.extend(self.users.get(key).into_iter().flatten().cloned());
            }
        }
        self.observed = observed;
        self.users = users;
        changed
    }
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
/// A transient destructive operation participates in the same admission table.
pub struct ClaimGuard {
    reservations: Arc<Reservations>,
    key: Key,
}
impl Drop for ClaimGuard {
    fn drop(&mut self) { self.reservations.release(&self.key); }
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

    pub fn reclaim(self: &Arc<Self>,namespace:&str,claim:&str)->Option<ClaimGuard> {
        let key=Key{kind:Kind::Pod,namespace:namespace.into(),name:format!("reclaim-{claim}"),uid:uuid::Uuid::new_v4().to_string()};
        if !self.acquire(&key,&[(Resource::Claim(namespace.into(),claim.into()),Access::Exclusive)]) {
            // A request-scoped waiter has no runtime work to enqueue.
            self.release(&key);
            return None;
        }
        Some(ClaimGuard{reservations:self.clone(),key})
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
                        let result = std::panic::AssertUnwindSafe(adapter.reconcile(work.key(),desired))
                            .catch_unwind().await.unwrap_or_else(|_|Err(anyhow::anyhow!("workload adapter panicked")));
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

    struct BarrierAdapter {
        started: tokio::sync::mpsc::UnboundedSender<bool>,
        release: Arc<tokio::sync::Notify>,
    }
    #[async_trait::async_trait]
    impl Adapter for BarrierAdapter {
        async fn reconcile(&self,_:&Key,desired:Option<Value>)->anyhow::Result<Next> {
            self.started.send(desired.is_some()).unwrap();
            if desired.is_some() {self.release.notified().await;}
            Ok(Next::AwaitEvent)
        }
    }

    #[tokio::test]
    async fn deletion_during_side_effect_runs_cleanup_after_completion() {
        let e=Executor::new(); let runner=e.clone();
        let (tx,mut rx)=tokio::sync::mpsc::unbounded_channel();
        let release=Arc::new(tokio::sync::Notify::new());
        let a=Arc::new(BarrierAdapter{started:tx,release:release.clone()});
        e.replace(Kind::Pod,&[object("x","data")]).unwrap();
        let task=tokio::spawn(async move{runner.run(a,2).await});
        assert_eq!(rx.recv().await,Some(true));
        e.replace(Kind::Pod,&[]).unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(20),rx.recv()).await.is_err(),"one active operation per UID");
        release.notify_one();
        assert_eq!(tokio::time::timeout(Duration::from_secs(1),rx.recv()).await.unwrap(),Some(false));
        task.abort();
    }

    #[test]
    fn same_name_replacement_and_claim_mutations_wait_for_cleanup() {
        let e=Executor::new(); let old=key(Kind::Pod,"old");
        let mut new=key(Kind::Pod,"new");new.name=old.name.clone();
        assert!(e.reservations.acquire(&old,&[]));
        assert!(!e.reservations.acquire(&new,&[]));
        e.reservations.release(&old);
        assert!(e.reservations.acquire(&new,&[]));
        let claim=(Resource::Claim("ns".into(),"data".into()),Access::SharedFilesystem);
        let held=e.reservations.try_operations(&[claim.clone()]).unwrap();
        assert!(e.reservations.try_operations(&[claim.clone()]).is_none());
        drop(held);
        assert!(e.reservations.try_operations(&[claim]).is_some());
    }

    #[test]
    fn reclaim_excludes_start_until_its_request_finishes() {
        let e=Executor::new();let resource=Resource::Claim("ns".into(),"data".into());
        let guard=e.reservations.reclaim("ns","data").unwrap();
        assert!(!e.reservations.acquire(&key(Kind::Pod,"p"),&[(resource.clone(),Access::SharedFilesystem)]));
        assert!(e.reservations.reclaim("ns","data").is_none());
        drop(guard);
        assert!(e.reservations.acquire(&key(Kind::Pod,"p"),&[(resource,Access::SharedFilesystem)]));
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
    #[test]
    fn attachment_and_volume_changes_only_wake_claim_dependents() {
        let mut index = VolumeIndex::default();
        let claims = vec![json!({"metadata":{"namespace":"ns","name":"data"},"spec":{"volumeName":"pv"}}),
            json!({"metadata":{"namespace":"ns","name":"other"},"spec":{"volumeName":"other-pv"}})];
        let mut volumes = vec![json!({"metadata":{"name":"pv"}})];
        let mut attachments = vec![json!({"metadata":{"name":"attach"},"spec":{"source":{"persistentVolumeName":"pv"}},"status":{"attached":false}})];
        index.update(&claims,&volumes,&attachments);
        assert!(index.update(&claims,&volumes,&attachments).is_empty());
        let expected = HashSet::from([Dependency::Claim("ns".into(),"data".into())]);
        attachments[0]["status"]["attached"] = json!(true);
        assert_eq!(index.update(&claims,&volumes,&attachments),expected);
        volumes[0]["spec"] = json!({"csi":{"volumeHandle":"new-handle"}});
        assert_eq!(index.update(&claims,&volumes,&attachments),expected);
        assert_eq!(index.update(&claims,&volumes,&[]),expected);
    }

}
