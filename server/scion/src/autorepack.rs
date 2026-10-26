//! Repacking on its own (`scion server`): now and then the server checks how much was written to
//! the object store since the last repack, and repacks when it is a lot (after a large import), or
//! when a day (`--repack-every`) has passed and anything was written. One node repacks at a time.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use scion_store::MetaStore;
use scion_store_blob::ObjectBlobStore;

use crate::repack::{RepackOptions, repack};

/// Below this, a scheduled repack is skipped: there is nothing worth rewriting everything for.
const MIN_NEW_BYTES: u64 = 1 << 20;
/// The lease that keeps nodes from repacking at once, and how long it lasts unless renewed.
const LEASE_SCOPE: &str = "admin/repack";
const LEASE_TTL: Duration = Duration::from_secs(600);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Schedule {
    /// How often to look.
    pub check: Duration,
    /// Repack when this much was written since the last repack; 0: never for this reason.
    pub after_bytes: u64,
    /// Repack when this long has passed since the last one and anything was written; zero: never.
    pub every: Duration,
}

impl Schedule {
    pub fn enabled(&self) -> bool {
        self.after_bytes > 0 || !self.every.is_zero()
    }

    /// Why a repack is due, given when the last one was and what was written since.
    pub fn due(&self, last: Option<SystemTime>, written: u64, now: SystemTime) -> Option<String> {
        if self.after_bytes > 0 && written >= self.after_bytes {
            return Some(format!("{} written since the last repack", mib(written)));
        }
        let age = last.map(|t| now.duration_since(t).unwrap_or_default());
        if !self.every.is_zero() && written >= MIN_NEW_BYTES && age.is_none_or(|a| a >= self.every) {
            return Some(match age {
                Some(a) => format!("{} since the last repack, {} written", hours(a), mib(written)),
                None => format!("never repacked, {} written", mib(written)),
            });
        }
        None
    }
}

fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / f64::from(1 << 20))
}

fn hours(d: Duration) -> String {
    format!("{:.1}h", d.as_secs_f64() / 3600.0)
}

/// Runs the schedule until the process exits.
pub fn spawn(meta: Arc<dyn MetaStore>, objects: Arc<ObjectBlobStore>, schedule: Schedule) {
    let holder = format!("repack-{}-{}", std::process::id(), rand_suffix());
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(schedule.check);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick fires at once; let the server settle first
        tick.tick().await;
        loop {
            tick.tick().await;
            if let Err(e) = check(&meta, &objects, &schedule, &holder).await {
                tracing::warn!(error = %e, "automatic repack failed; trying again at the next check");
            }
        }
    });
}

fn rand_suffix() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()))
}

/// One look: repacks when due and no other node is repacking. Whether it repacked.
pub async fn check(
    meta: &Arc<dyn MetaStore>,
    objects: &ObjectBlobStore,
    schedule: &Schedule,
    holder: &str,
) -> anyhow::Result<bool> {
    let last = objects.last_repack().await?;
    let written = objects.written_since(last).await?;
    let Some(reason) = schedule.due(last, written, SystemTime::now()) else {
        return Ok(false);
    };
    let ttl = i64::try_from(LEASE_TTL.as_millis()).unwrap_or(i64::MAX);
    let lease = meta.acquire_lease(LEASE_SCOPE, holder, ttl, now_ms()).await?;
    if lease.holder != holder {
        tracing::debug!(holder = %lease.holder, "another node is repacking");
        return Ok(false);
    }
    tracing::info!(%reason, "repacking");
    let started = std::time::Instant::now();
    let opts = RepackOptions {
        repos: Vec::new(),
        prune_after: Duration::from_secs(3600),
        dry_run: false,
    };
    // Renew the lease while the repack runs
    let renew = async {
        loop {
            tokio::time::sleep(LEASE_TTL / 3).await;
            if let Err(e) = meta.acquire_lease(LEASE_SCOPE, holder, ttl, now_ms()).await {
                tracing::warn!(error = %e, "renewing the repack lease");
            }
        }
    };
    let result = tokio::select! {
        r = repack(meta, objects, &opts) => r,
        () = renew => unreachable!("renewing never ends"),
    };
    let _ = meta.release_lease(LEASE_SCOPE, holder).await;
    let report = result?;
    for line in report.lines(false) {
        tracing::info!(elapsed_s = started.elapsed().as_secs(), "{line}");
    }
    Ok(true)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use scion_object::ObjectId;
    use scion_store::{BlobStore, MemStore};

    #[tokio::test]
    async fn spawned_schedule_repacks_on_its_own() {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta: Arc<dyn MetaStore> = Arc::new(MemStore::new());
        let objects = Arc::new(
            ObjectBlobStore::local(dir.path())
                .expect("open")
                .with_packs(meta.clone()),
        );
        objects
            .put(ObjectId::of_blob(b"x"), Bytes::from_static(b"x"))
            .await
            .expect("put");
        let s = Schedule {
            check: Duration::from_millis(50),
            after_bytes: 1,
            every: Duration::ZERO,
        };
        spawn(meta, objects.clone(), s);
        for _ in 0..100 {
            if objects.last_repack().await.expect("marker").is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("no repack");
    }

    #[tokio::test]
    async fn repacks_when_due_once_and_not_while_another_node_does() {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta: Arc<dyn MetaStore> = Arc::new(MemStore::new());
        let objects = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(meta.clone());
        let put = |data: &'static [u8]| objects.put(ObjectId::of_blob(data), Bytes::from_static(data));
        put(b"one").await.expect("put");
        let s = Schedule {
            check: H,
            after_bytes: 1,
            every: Duration::ZERO,
        };
        assert!(check(&meta, &objects, &s, "me").await.expect("check"), "never repacked");
        assert!(objects.last_repack().await.expect("marker").is_some());
        assert!(!check(&meta, &objects, &s, "me").await.expect("check"), "nothing new");
        // The lease was released: another node may take it, and then this one waits
        tokio::time::sleep(Duration::from_millis(1100)).await;
        put(b"two").await.expect("put");
        meta.acquire_lease(LEASE_SCOPE, "other", 60_000, now_ms())
            .await
            .expect("lease");
        assert!(
            !check(&meta, &objects, &s, "me").await.expect("check"),
            "the other node repacks"
        );
        meta.release_lease(LEASE_SCOPE, "other").await.expect("release");
        assert!(check(&meta, &objects, &s, "me").await.expect("check"));
    }

    const H: Duration = Duration::from_secs(3600);

    #[test]
    fn due_after_a_lot_written_or_a_day_with_anything() {
        let s = Schedule {
            check: H,
            after_bytes: 256 << 20,
            every: 24 * H,
        };
        let now = SystemTime::now();
        assert!(s.enabled());
        assert_eq!(
            s.due(Some(now - H), 300 << 20, now).as_deref(),
            Some("300.0 MiB written since the last repack")
        );
        assert_eq!(s.due(Some(now - H), 100 << 20, now), None);
        assert_eq!(
            s.due(Some(now - 25 * H), 2 << 20, now).as_deref(),
            Some("25.0h since the last repack, 2.0 MiB written")
        );
        assert_eq!(s.due(Some(now - 25 * H), 1000, now), None, "too little to bother");
        assert_eq!(
            s.due(None, 2 << 20, now).as_deref(),
            Some("never repacked, 2.0 MiB written")
        );
        let off = Schedule {
            after_bytes: 0,
            every: Duration::ZERO,
            ..s
        };
        assert!(!off.enabled());
        assert_eq!(off.due(None, u64::MAX, now), None);
    }
}
