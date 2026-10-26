//! 合入的线性一致性检查（服务端设计文档 §12：并发 land、租约抢占、leader 被杀）。
//!
//! 每个客户端合入一个带唯一 change id 的提交，遇到不确定的错误（超时、节点被杀、竞争）时用同一个
//! request id 重试，直到成功或得到确定的失败（冲突）。结束后按最终的第一父提交链检查：
//!
//! 1. 不重复：每个 change id 在链上至多出现一次；
//! 2. 不丢失：每个确认成功的 change 都在链上；
//! 3. 不凭空出现：确定失败的 change 不在链上；
//! 4. 实时顺序：A 确认早于 B 发起时，A 在链上位于 B 之前，且 A 的版本小于 B 的版本。

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use scion_land::{ChangesetWriter, ClusterConfig, Coordinator, CoordinatorConfig, LandError, LandOutcome, LandRequest};
use scion_object::{Changeset, EntryMode, FileChange, ObjectId, Signature};
use scion_store::{BlobStore, IndexedObject, MemStore, MetaStore};

const REPO: &str = "lin";

/// 一次（可能重试多次的）合入操作的结果。
#[derive(Debug, Clone)]
enum Outcome {
    Acked { version: u64 },
    Failed,
}

#[derive(Debug, Clone)]
struct Op {
    change_id: [u8; 16],
    invoked: Instant,
    completed: Instant,
    outcome: Outcome,
}

struct Fixture {
    meta: Arc<dyn MetaStore>,
    blobs: Arc<MemStore>,
    writer: Arc<ChangesetWriter>,
    base: ObjectId,
}

fn sig() -> Signature {
    Signature {
        name: "lin".into(),
        email: "lin@example.com".into(),
        time_ms: 0,
        tz_offset_minutes: 0,
    }
}

impl Fixture {
    async fn new(meta: Arc<dyn MetaStore>) -> Self {
        let blobs = Arc::new(MemStore::new());
        meta.create_repo(REPO).await.expect("repo");
        let writer = Arc::new(ChangesetWriter::new(meta.clone(), blobs.clone()));
        let mut fx = Fixture {
            meta,
            blobs,
            writer,
            base: ObjectId::of_blob(b""),
        };
        fx.base = fx.commit(None, "hot", "0\n", [0xff; 16]).await;
        Coordinator::new(fx.writer.clone(), CoordinatorConfig::default())
            .land(request(fx.base, None))
            .await
            .expect("base");
        fx
    }

    async fn commit(&self, parent: Option<ObjectId>, path: &str, content: &str, change_id: [u8; 16]) -> ObjectId {
        let id = ObjectId::of_blob(content.as_bytes());
        self.blobs.put(id, Bytes::from(content.to_owned())).await.expect("blob");
        self.meta
            .index_objects(
                REPO,
                &[IndexedObject {
                    id,
                    size: content.len() as u64,
                }],
            )
            .await
            .expect("index");
        let cs = Changeset::new(
            parent.into_iter().collect(),
            sig(),
            sig(),
            format!("{path} {change_id:?}"),
            change_id,
            vec![FileChange::put(path, EntryMode::File, id, content.len() as u64)],
            BTreeMap::new(),
        )
        .expect("cs");
        self.writer.create(REPO, &cs).await.expect("create").id
    }

    /// 第 i 个客户端的提交：大多改自己的文件；每 `conflict_every` 个改共享文件（彼此冲突）。
    async fn client_commits(&self, n: usize, conflict_every: usize) -> Vec<([u8; 16], ObjectId)> {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let mut change_id = [0u8; 16];
            change_id[..8].copy_from_slice(&(i as u64 + 1).to_be_bytes());
            let (path, content) = if conflict_every > 0 && i % conflict_every == 0 {
                ("hot".to_owned(), format!("v{i}\n"))
            } else {
                (format!("own/{i}"), format!("{i}\n"))
            };
            out.push((
                change_id,
                self.commit(Some(self.base), &path, &content, change_id).await,
            ));
        }
        out
    }

    /// 最终第一父提交链上的 change id（从旧到新），以及当前 ref。
    async fn chain(&self) -> (Vec<[u8; 16]>, u64) {
        let head = self.meta.get_ref(REPO, "main").await.expect("get").expect("ref");
        let mut out = Vec::new();
        let mut cur = Some(head.target);
        while let Some(id) = cur {
            let cs = scion_query::load_changeset(self.blobs.as_ref(), id)
                .await
                .expect("load");
            out.push(cs.change_id);
            cur = cs.parents.first().copied();
        }
        out.reverse();
        (out, head.version)
    }
}

fn request(tip: ObjectId, request_id: Option<&str>) -> LandRequest<'_> {
    LandRequest {
        repo: REPO,
        reference: "main",
        tip,
        fast_forward_only: false,
        request_id,
        actor: "",
    }
}

/// 确定的失败：重试不会改变结果。
fn definite(e: &LandError) -> bool {
    matches!(e, LandError::Conflict { .. })
}

fn check(history: &[Op], chain: &[[u8; 16]]) {
    let mut pos: HashMap<[u8; 16], usize> = HashMap::new();
    for (i, c) in chain.iter().enumerate() {
        assert!(pos.insert(*c, i).is_none(), "change {c:?} landed twice");
    }
    let acked: Vec<&Op> = history
        .iter()
        .filter(|o| matches!(o.outcome, Outcome::Acked { .. }))
        .collect();
    for o in history {
        match o.outcome {
            Outcome::Acked { .. } => assert!(pos.contains_key(&o.change_id), "acked change {:?} lost", o.change_id),
            Outcome::Failed => assert!(
                !pos.contains_key(&o.change_id),
                "failed change {:?} is in the history",
                o.change_id
            ),
        }
    }
    for a in &acked {
        for b in &acked {
            if a.completed < b.invoked {
                let (Outcome::Acked { version: va }, Outcome::Acked { version: vb }) = (&a.outcome, &b.outcome) else {
                    unreachable!()
                };
                assert!(pos[&a.change_id] < pos[&b.change_id], "real-time order violated");
                assert!(va < vb, "version order violated: {va} then {vb}");
            }
        }
    }
}

/// 单节点：大量并发客户端，其中一部分彼此冲突。
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_lands_on_one_node_are_linearizable() {
    let fx = Fixture::new(Arc::new(MemStore::new())).await;
    let commits = fx.client_commits(300, 10).await;
    let c = Coordinator::new(
        fx.writer.clone(),
        CoordinatorConfig {
            max_batch: 16,
            ..CoordinatorConfig::default()
        },
    );
    let tasks: Vec<_> = commits
        .into_iter()
        .enumerate()
        .map(|(i, (change_id, tip))| {
            let c = c.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_micros((i as u64 * 7919) % 3000)).await;
                let invoked = Instant::now();
                let id = format!("op-{i}");
                let outcome = match c.land(request(tip, Some(&id))).await {
                    Ok(o) => Outcome::Acked {
                        version: o.state.version,
                    },
                    Err(e) if definite(&e) => Outcome::Failed,
                    Err(e) => panic!("unexpected error {e:?}"),
                };
                Op {
                    change_id,
                    invoked,
                    completed: Instant::now(),
                    outcome,
                }
            })
        })
        .collect();
    let mut history = Vec::new();
    for t in tasks {
        history.push(t.await.expect("join"));
    }
    let (chain, _) = fx.chain().await;
    check(&history, &chain);
    let failed = history.iter().filter(|o| matches!(o.outcome, Outcome::Failed)).count();
    // 30 个改共享文件的请求中只有一个能成功
    assert_eq!(failed, 29, "conflicting changes on the shared file");
    assert_eq!(chain.len(), 1 + 300 - 29);
}

/// 两个集群节点：客户端随机挑节点，遇到 NotLeader 换到 leader 重试（相当于转发）。
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lands_through_two_clustered_nodes_are_linearizable() {
    let fx = Fixture::new(Arc::new(MemStore::new())).await;
    let commits = fx.client_commits(200, 0).await;
    let nodes: Arc<HashMap<String, Coordinator>> = Arc::new(
        ["http://a", "http://b"]
            .into_iter()
            .map(|n| {
                let c = Coordinator::clustered(fx.writer.clone(), CoordinatorConfig::default(), ClusterConfig::new(n));
                (n.to_owned(), c)
            })
            .collect(),
    );
    let tasks: Vec<_> = commits
        .into_iter()
        .enumerate()
        .map(|(i, (change_id, tip))| {
            let nodes = nodes.clone();
            tokio::spawn(async move {
                let invoked = Instant::now();
                let id = format!("op-{i}");
                let mut node = if i % 2 == 0 { "http://a" } else { "http://b" }.to_owned();
                let outcome = loop {
                    match nodes[&node].land(request(tip, Some(&id))).await {
                        Ok(o) => {
                            break Outcome::Acked {
                                version: o.state.version,
                            };
                        }
                        Err(LandError::NotLeader { leader }) => node = leader,
                        Err(e) => panic!("unexpected error {e:?}"),
                    }
                };
                Op {
                    change_id,
                    invoked,
                    completed: Instant::now(),
                    outcome,
                }
            })
        })
        .collect();
    let mut history = Vec::new();
    for t in tasks {
        history.push(t.await.expect("join"));
    }
    let (chain, _) = fx.chain().await;
    check(&history, &chain);
    assert_eq!(chain.len(), 201);
}

/// leader 在合入进行中被杀（运行时被强行关闭，租约不释放）：客户端用同一个 request id 重试，
/// 租约到期后另一个节点接管。不重复、不丢失、顺序一致。
#[test]
fn killing_the_leader_mid_run_keeps_lands_exactly_once() {
    let main_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    let dir = tempfile::tempdir().expect("tempdir");
    let url = format!("sqlite://{}", dir.path().join("meta.db").display());
    let (fx, commits) = main_rt.block_on(async {
        let meta: Arc<dyn MetaStore> = Arc::new(scion_store_sql::SqliteMetaStore::connect(&url).await.expect("sqlite"));
        let fx = Fixture::new(meta).await;
        let commits = fx.client_commits(150, 0).await;
        (fx, commits)
    });
    let ttl = Duration::from_millis(400);
    let cluster = |n: &str| ClusterConfig {
        node: n.into(),
        lease_ttl: ttl,
        renew_every: ttl / 4,
    };
    let leader_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let leader = leader_rt.handle().clone();
    let a = Coordinator::clustered(fx.writer.clone(), CoordinatorConfig::default(), cluster("http://a"));
    let b = Coordinator::clustered(fx.writer.clone(), CoordinatorConfig::default(), cluster("http://b"));
    // a 先成为 leader
    leader
        .block_on(a.land(request(fx.base, None)))
        .expect("a takes the lease");
    let a_alive = Arc::new(AtomicBool::new(true));
    let attempts = Arc::new(AtomicU64::new(0));

    let history = main_rt.block_on(async {
        let tasks: Vec<_> = commits
            .into_iter()
            .enumerate()
            .map(|(i, (change_id, tip))| {
                let (a, b, leader, a_alive, attempts) =
                    (a.clone(), b.clone(), leader.clone(), a_alive.clone(), attempts.clone());
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis((i as u64 * 37) % 600)).await;
                    let invoked = Instant::now();
                    let id = format!("op-{i}");
                    let mut use_a = true;
                    let outcome = loop {
                        attempts.fetch_add(1, Ordering::Relaxed);
                        let result: Result<LandOutcome, String> = if use_a && a_alive.load(Ordering::SeqCst) {
                            // a 的任务运行在 leader 运行时里，被杀时请求以 JoinError 结束（结果不确定）
                            let (a, id) = (a.clone(), id.clone());
                            match leader.spawn(async move { a.land(request(tip, Some(&id))).await }).await {
                                Ok(r) => r.map_err(|e| format!("{e:?}")),
                                Err(e) => Err(format!("leader killed: {e}")),
                            }
                        } else {
                            b.land(request(tip, Some(&id))).await.map_err(|e| format!("{e:?}"))
                        };
                        match result {
                            Ok(o) => {
                                break Outcome::Acked {
                                    version: o.state.version,
                                };
                            }
                            Err(e) if e.contains("http://a") => use_a = true,
                            Err(e) if e.contains("http://b") => use_a = false,
                            Err(_) => {
                                // 不确定：换个节点，用同一个 request id 重试
                                use_a = !use_a;
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                        }
                    };
                    Op {
                        change_id,
                        invoked,
                        completed: Instant::now(),
                        outcome,
                    }
                })
            })
            .collect();
        // 运行一段时间后杀掉 leader
        tokio::time::sleep(Duration::from_millis(250)).await;
        a_alive.store(false, Ordering::SeqCst);
        leader_rt.shutdown_background();
        let mut history = Vec::new();
        for t in tasks {
            history.push(t.await.expect("join"));
        }
        history
    });
    let (chain, _) = main_rt.block_on(fx.chain());
    check(&history, &chain);
    assert_eq!(chain.len(), 151, "base + every change exactly once");
    assert!(attempts.load(Ordering::Relaxed) > 150, "the kill should force retries");
}
