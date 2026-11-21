//! Per-request timing for one git push: stage wall times and blob store call counts.
//!
//! `scope` makes a [`PushStats`] current for a future. [`CountedBlobs`] and [`timed`] write to
//! it. Outside a scope they do nothing.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use scion_object::ObjectId;

use crate::{BlobStore, Result};

tokio::task_local! {
    static CURRENT: Arc<PushStats>;
}

/// Call count of one blob store operation and the time spent waiting in it.
#[derive(Default)]
struct Calls {
    n: AtomicU64,
    nanos: AtomicU64,
}

impl Calls {
    fn add(&self, d: Duration) {
        self.n.fetch_add(1, Relaxed);
        self.nanos
            .fetch_add(u64::try_from(d.as_nanos()).unwrap_or(u64::MAX), Relaxed);
    }
}

/// What one push did: stages in the order they first ran, and blob store calls.
#[derive(Default)]
pub struct PushStats {
    stages: Mutex<Vec<(&'static str, Duration)>>,
    get: Calls,
    head: Calls,
    put: Calls,
}

impl PushStats {
    /// Adds `d` to stage `name`. A stage that runs many times (each window) is summed.
    fn stage(&self, name: &'static str, d: Duration) {
        let mut s = self.stages.lock().unwrap_or_else(|e| e.into_inner());
        match s.iter_mut().find(|(n, _)| *n == name) {
            Some((_, t)) => *t += d,
            None => s.push((name, d)),
        }
    }

    /// `(get, head, put)` call counts. `put` counts `put_many` as one call.
    pub fn counts(&self) -> [u64; 3] {
        [&self.get, &self.head, &self.put].map(|c| c.n.load(Relaxed))
    }

    /// The fields of the `push timing` line. Times are in milliseconds.
    pub fn summary(&self) -> String {
        let mut line = String::new();
        for (n, d) in self.stages.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            line += &format!("{n}_ms={} ", d.as_millis());
        }
        let wait: u64 = [&self.get, &self.head, &self.put]
            .iter()
            .map(|c| c.nanos.load(Relaxed))
            .sum();
        let [g, h, p] = self.counts();
        // ponytail: the BlobStore trait has no list; add a `list` count if it grows one
        line + &format!(
            "blob_get={g} blob_head={h} blob_put={p} blob_wait_ms={}",
            wait / 1_000_000
        )
    }
}

/// Runs `f` with `stats` as the current stats of the tasks it awaits.
pub async fn scope<F: Future>(stats: Arc<PushStats>, f: F) -> F::Output {
    CURRENT.scope(stats, f).await
}

/// Awaits `f` and adds its wall time to stage `name` of the current push.
pub async fn timed<F: Future>(name: &'static str, f: F) -> F::Output {
    let t = Instant::now();
    let out = f.await;
    let _ = CURRENT.try_with(|s| s.stage(name, t.elapsed()));
    out
}

async fn count<F: Future>(pick: fn(&PushStats) -> &Calls, f: F) -> F::Output {
    let t = Instant::now();
    let out = f.await;
    let d = t.elapsed();
    let _ = CURRENT.try_with(|s| pick(s).add(d));
    out
}

/// A [`BlobStore`] that counts its calls into the current [`PushStats`]. Put it directly on the
/// backend, under any cache, so that it counts only the calls that reach the backend.
pub struct CountedBlobs(pub Arc<dyn BlobStore>);

#[async_trait]
impl BlobStore for CountedBlobs {
    async fn put(&self, id: ObjectId, data: Bytes) -> Result<()> {
        count(|s| &s.put, self.0.put(id, data)).await
    }

    async fn put_many(&self, objects: &[(ObjectId, Bytes)]) -> Result<()> {
        count(|s| &s.put, self.0.put_many(objects)).await
    }

    async fn get(&self, id: &ObjectId) -> Result<Option<Bytes>> {
        count(|s| &s.get, self.0.get(id)).await
    }

    async fn exists(&self, id: &ObjectId) -> Result<bool> {
        count(|s| &s.head, self.0.exists(id)).await
    }

    async fn delete(&self, id: &ObjectId) -> Result<()> {
        self.0.delete(id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemStore;

    #[tokio::test]
    async fn counts_calls_and_stages_inside_a_scope_only() {
        let blobs = CountedBlobs(Arc::new(MemStore::new()));
        let id = ObjectId::of_blob(b"x");
        let stats = Arc::new(PushStats::default());
        scope(stats.clone(), async {
            blobs.put(id, Bytes::from_static(b"x")).await.expect("put");
            blobs.get(&id).await.expect("get");
            blobs.get(&id).await.expect("get");
            blobs.exists(&id).await.expect("exists");
            timed("a", async {}).await;
            timed("a", async {}).await;
            timed("b", async {}).await;
        })
        .await;
        blobs.get(&id).await.expect("outside a scope");
        assert_eq!(stats.counts(), [2, 1, 1]);
        let s = stats.summary();
        assert!(s.starts_with("a_ms=") && s.contains(" b_ms="), "{s}");
        assert!(s.contains("blob_get=2 blob_head=1 blob_put=1"), "{s}");
    }
}
