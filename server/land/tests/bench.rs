//! 合入吞吐基准（M3 验收：单 ref 每秒 200 次以上合入）。手动运行：
//!
//! ```sh
//! SCION_BENCH_META=postgres SCION_BENCH_PG_URL=postgres://… \
//!   cargo test --release -p scion-land --test bench -- --ignored --nocapture
//! ```
//!
//! `SCION_BENCH_LANDS`（默认 2000）个客户端提交各改一个不同的文件，基于同一个旧提交，
//! 由 `SCION_BENCH_CLIENTS`（默认 200）个并发任务合入同一个 ref。对象在内存中，元数据可选
//! mem / sqlite / postgres（PostgreSQL 需要一个空数据库）。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use scion_land::{ChangesetWriter, Coordinator, CoordinatorConfig, LandRequest};
use scion_object::{Changeset, EntryMode, FileChange, ObjectId, Signature};
use scion_store::{BlobStore, IndexedObject, MemStore, MetaStore};

const REPO: &str = "bench";

fn env_or<T: std::str::FromStr>(k: &str, default: T) -> T {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

async fn meta_store(dir: &std::path::Path) -> (String, Arc<dyn MetaStore>) {
    let backend = std::env::var("SCION_BENCH_META").unwrap_or_else(|_| "mem".into());
    let meta: Arc<dyn MetaStore> = match backend.as_str() {
        "sqlite" => {
            let url = format!("sqlite://{}", dir.join("meta.db").display());
            Arc::new(scion_store_sql::SqliteMetaStore::connect(&url).await.expect("sqlite"))
        }
        "postgres" => {
            let url = std::env::var("SCION_BENCH_PG_URL").expect("SCION_BENCH_PG_URL");
            Arc::new(scion_store_sql::PgMetaStore::connect(&url).await.expect("postgres"))
        }
        _ => Arc::new(MemStore::new()),
    };
    (backend, meta)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark; run manually with --release"]
async fn land_throughput_on_one_ref() {
    let lands: usize = env_or("SCION_BENCH_LANDS", 2000);
    let clients: usize = env_or("SCION_BENCH_CLIENTS", 200);
    let dir = tempfile::tempdir().expect("tempdir");
    let (backend, raw) = meta_store(dir.path()).await;
    // 与服务端一致：提交记录经过进程内缓存
    let meta: Arc<dyn MetaStore> = Arc::new(scion_store::CachedMeta::new(raw, 1_000_000));
    let blobs = Arc::new(MemStore::new());
    meta.create_repo(REPO).await.expect("repo");
    let writer = Arc::new(ChangesetWriter::new(meta.clone(), blobs.clone()));
    let sig = Signature {
        name: "bench".into(),
        email: "bench@example.com".into(),
        time_ms: 0,
        tz_offset_minutes: 0,
    };
    let base = Changeset::new(
        vec![],
        sig.clone(),
        sig.clone(),
        "base",
        [0; 16],
        vec![],
        BTreeMap::new(),
    )
    .expect("base");
    let base = writer.create(REPO, &base).await.expect("base").id;
    let coordinator = Coordinator::new(writer.clone(), CoordinatorConfig::default());
    coordinator
        .land(LandRequest {
            repo: REPO,
            reference: "main",
            tip: base,
            fast_forward_only: false,
            request_id: None,
            actor: "",
        })
        .await
        .expect("base lands");

    eprintln!("preparing {lands} changesets (meta: {backend}) ...");
    let mut tips = Vec::with_capacity(lands);
    for i in 0..lands {
        let data = format!("file {i}\n");
        let id = ObjectId::of_blob(data.as_bytes());
        blobs.put(id, Bytes::from(data.clone())).await.expect("blob");
        meta.index_objects(
            REPO,
            &[IndexedObject {
                id,
                size: data.len() as u64,
            }],
        )
        .await
        .expect("index");
        let mut change_id = [0u8; 16];
        change_id[..8].copy_from_slice(&(i as u64 + 1).to_be_bytes());
        let cs = Changeset::new(
            vec![base],
            sig.clone(),
            sig.clone(),
            format!("change {i}"),
            change_id,
            vec![FileChange::put(
                format!("dir{}/f{i}", i % 64),
                EntryMode::File,
                id,
                data.len() as u64,
            )],
            BTreeMap::new(),
        )
        .expect("cs");
        tips.push(writer.create(REPO, &cs).await.expect("create").id);
    }

    let tips = Arc::new(tips);
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let started = Instant::now();
    let tasks: Vec<_> = (0..clients)
        .map(|_| {
            let (c, tips, next) = (coordinator.clone(), tips.clone(), next.clone());
            tokio::spawn(async move {
                let mut latencies = Vec::new();
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(tip) = tips.get(i) else { break };
                    let id = format!("req-{i}");
                    let t = Instant::now();
                    c.land(LandRequest {
                        repo: REPO,
                        reference: "main",
                        tip: *tip,
                        fast_forward_only: false,
                        request_id: Some(&id),
                        actor: "",
                    })
                    .await
                    .expect("land");
                    latencies.push(t.elapsed());
                }
                latencies
            })
        })
        .collect();
    let mut latencies: Vec<Duration> = Vec::with_capacity(lands);
    for t in tasks {
        latencies.extend(t.await.expect("join"));
    }
    let elapsed = started.elapsed();
    latencies.sort();
    let pct = |p: f64| latencies[((latencies.len() as f64 * p) as usize).min(latencies.len() - 1)];
    let head = meta.get_ref(REPO, "main").await.expect("get").expect("ref");
    eprintln!(
        "{lands} lands, {clients} clients, meta {backend}: {:.0} lands/s, {} batches (avg {:.1}/batch), \
         latency p50 {:.1} ms p99 {:.1} ms max {:.1} ms",
        lands as f64 / elapsed.as_secs_f64(),
        head.version - 1,
        lands as f64 / (head.version - 1) as f64,
        pct(0.5).as_secs_f64() * 1e3,
        pct(0.99).as_secs_f64() * 1e3,
        latencies.last().copied().unwrap_or_default().as_secs_f64() * 1e3,
    );
}
