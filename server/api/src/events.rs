//! ref 变化的推送（docs/guide/events.md）。
//!
//! 事件在元数据库里与 ref 的修改同一事务写入（`ref_events`），这里按 seq 顺序读取并推送：
//! - webhook：POST JSON，`X-Scion-Signature-256` 为请求体的 HMAC-SHA256（与 GitHub 相同的格式）；
//! - NATS JetStream：subject 为 `<前缀>.ref.created|updated|deleted`，`Nats-Msg-Id` 用于服务端去重。
//!
//! 每个推送目标在集群中由持有租约的一个节点负责（`events/<目标>`），投递进度保存在元数据库
//! （`events.cursor.<目标>`），按 seq 顺序、至少送达一次：失败时退避重试，不跳过事件。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use scion_object::{Changeset, FileOp, ObjectId};
use scion_store::{BlobStore, MetaStore, RefEvent};
use serde::Serialize;
use sha2::Sha256;
use tokio::sync::Mutex;

/// 一个事件最多列出的提交数。
pub const MAX_COMMITS: usize = 100;
/// 一个提交最多列出的改动路径数。
pub const MAX_CHANGES: usize = 1000;
/// 推送租约的有效期。
const LEASE_TTL: Duration = Duration::from_secs(15);
/// 重试的最长间隔。
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// 推送配置。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventsConfig {
    pub webhook: Option<WebhookConfig>,
    pub nats: Option<NatsConfig>,
    /// 事件在元数据库中保留的时间（拉取方式 `ListRefEvents` 也受它限制）。
    pub retention: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookConfig {
    pub url: String,
    /// HMAC-SHA256 签名密钥；不设置时不签名。
    pub secret: Option<String>,
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsConfig {
    pub url: String,
    /// subject 前缀，默认 `scion`。
    pub subject_prefix: String,
}

/// 事件类型：`ref.created`、`ref.updated` 或 `ref.deleted`。
pub fn event_kind(ev: &RefEvent) -> &'static str {
    match (ev.old_target, ev.new_target) {
        (None, _) => "ref.created",
        (_, None) => "ref.deleted",
        _ => "ref.updated",
    }
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Payload {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub seq: u64,
    pub repo: String,
    #[serde(rename = "ref")]
    pub reference: String,
    pub old: Option<String>,
    pub new: Option<String>,
    /// `old` / `new` 对应的 git 兼容 sha（40 位十六进制）。
    pub old_git_sha: Option<String>,
    pub new_git_sha: Option<String>,
    pub version: u64,
    pub time_ms: i64,
    /// 本次变化引入的提交（从新到旧）；删除时为空。
    pub commits: Vec<CommitInfo>,
    /// 提交数超过上限，只列出了最新的 [`MAX_COMMITS`] 个。
    pub commits_truncated: bool,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct CommitInfo {
    pub id: String,
    /// git 兼容 sha（40 位十六进制）；算不出来时省略。
    pub git_sha: Option<String>,
    pub parents: Vec<String>,
    pub author: Person,
    pub message: String,
    /// 相对第一个父提交的改动。
    pub changes: Vec<PathChange>,
    pub changes_truncated: bool,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Person {
    pub name: String,
    pub email: String,
    pub time_ms: i64,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct PathChange {
    pub path: String,
    /// `put` 或 `delete`。
    pub op: &'static str,
    /// 文件大小（`put` 时）。
    pub size: Option<u64>,
}

/// 提交的 git sha。算不出来（例如文件内容已被清除）时记录警告并返回 `None`，不阻塞推送或检查。
pub(crate) async fn git_sha(meta: &dyn MetaStore, blobs: &dyn BlobStore, repo: &str, id: ObjectId) -> Option<String> {
    match scion_query::derive_git_sha(meta, blobs, repo, id).await {
        Ok(sha) => Some(hex::encode(sha)),
        Err(e) => {
            tracing::warn!(repo, %id, error = %e, "cannot derive git sha");
            None
        }
    }
}

pub(crate) fn commit_info(id: ObjectId, git_sha: Option<String>, cs: &Changeset) -> CommitInfo {
    let changes = cs
        .delta
        .iter()
        .take(MAX_CHANGES)
        .map(|c| match &c.op {
            FileOp::Put { size, .. } => PathChange {
                path: c.path.clone(),
                op: "put",
                size: Some(*size),
            },
            FileOp::Delete => PathChange {
                path: c.path.clone(),
                op: "delete",
                size: None,
            },
        })
        .collect();
    CommitInfo {
        id: id.to_string(),
        git_sha,
        parents: cs.parents.iter().map(ToString::to_string).collect(),
        author: Person {
            name: cs.author.name.clone(),
            email: cs.author.email.clone(),
            time_ms: cs.author.time_ms,
        },
        message: cs.message.clone(),
        changes,
        changes_truncated: cs.delta.len() > MAX_CHANGES,
    }
}

/// 组装推送的内容：事件本身，加上本次变化引入的提交与路径级改动。
pub async fn payload(meta: &dyn MetaStore, blobs: &dyn BlobStore, ev: &RefEvent) -> Result<Payload, String> {
    let (commits, truncated) = match ev.new_target {
        Some(new) => {
            let (recs, truncated) = scion_query::new_commits(meta, &ev.repo, ev.old_target, new, MAX_COMMITS)
                .await
                .map_err(|e| e.to_string())?;
            let mut out = Vec::with_capacity(recs.len());
            for r in recs {
                let cs = scion_query::load_changeset(blobs, r.id)
                    .await
                    .map_err(|e| e.to_string())?;
                let sha = git_sha(meta, blobs, &ev.repo, r.id).await;
                out.push(commit_info(r.id, sha, &cs));
            }
            (out, truncated)
        }
        None => (Vec::new(), false),
    };
    let old_git_sha = match ev.old_target {
        Some(id) => git_sha(meta, blobs, &ev.repo, id).await,
        None => None,
    };
    let new_git_sha = match ev.new_target {
        Some(id) => git_sha(meta, blobs, &ev.repo, id).await,
        None => None,
    };
    Ok(Payload {
        kind: event_kind(ev),
        seq: ev.seq,
        repo: ev.repo.clone(),
        reference: ev.name.clone(),
        old: ev.old_target.map(|t| t.to_string()),
        new: ev.new_target.map(|t| t.to_string()),
        old_git_sha,
        new_git_sha,
        version: ev.version,
        time_ms: ev.created_at_ms,
        commits,
        commits_truncated: truncated,
    })
}

/// 推送目标。
#[async_trait]
pub trait Sink: Send + Sync {
    /// 指标和租约中使用的名字。
    fn name(&self) -> &'static str;
    async fn deliver(&self, kind: &str, seq: u64, body: &[u8]) -> Result<(), String>;
}

/// `sha256=<hex>`：请求体的 HMAC-SHA256。
pub fn signature(secret: &[u8], body: &[u8]) -> String {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret).expect("HMAC accepts keys of any length");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

pub struct WebhookSink {
    client: reqwest::Client,
    url: String,
    secret: Option<Vec<u8>>,
}

impl WebhookSink {
    pub fn new(cfg: &WebhookConfig) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(cfg.timeout)
            .build()
            .map_err(|e| format!("webhook client: {e}"))?;
        Ok(Self {
            client,
            url: cfg.url.clone(),
            secret: cfg.secret.as_ref().map(|s| s.as_bytes().to_vec()),
        })
    }
}

#[async_trait]
impl Sink for WebhookSink {
    fn name(&self) -> &'static str {
        "webhook"
    }

    async fn deliver(&self, kind: &str, seq: u64, body: &[u8]) -> Result<(), String> {
        let mut req = self
            .client
            .post(&self.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("X-Scion-Event", kind)
            .header("X-Scion-Delivery", seq.to_string())
            .body(body.to_vec());
        if let Some(secret) = &self.secret {
            req = req.header("X-Scion-Signature-256", signature(secret, body));
        }
        let resp = req.send().await.map_err(|e| format!("POST {}: {e}", self.url))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(format!("POST {}: HTTP {}", self.url, resp.status()))
        }
    }
}

pub struct NatsSink {
    url: String,
    prefix: String,
    js: Mutex<Option<async_nats::jetstream::Context>>,
}

impl NatsSink {
    pub fn new(cfg: &NatsConfig) -> Self {
        Self {
            url: cfg.url.clone(),
            prefix: cfg.subject_prefix.clone(),
            js: Mutex::new(None),
        }
    }

    /// `<前缀>.ref.updated` 等。
    pub fn subject(&self, kind: &str) -> String {
        format!("{}.{kind}", self.prefix)
    }
}

#[async_trait]
impl Sink for NatsSink {
    fn name(&self) -> &'static str {
        "nats"
    }

    async fn deliver(&self, kind: &str, seq: u64, body: &[u8]) -> Result<(), String> {
        let mut js = self.js.lock().await;
        if js.is_none() {
            let client = async_nats::connect(&self.url)
                .await
                .map_err(|e| format!("connect {}: {e}", self.url))?;
            *js = Some(async_nats::jetstream::new(client));
        }
        let Some(ctx) = js.as_ref() else {
            return Err("not connected".into());
        };
        let publish = async_nats::jetstream::message::PublishMessage::build()
            .payload(body.to_vec().into())
            .message_id(format!("scion-{seq}"));
        let ack = ctx
            .send_publish(self.subject(kind), publish)
            .await
            .map_err(|e| format!("publish: {e}"))?;
        ack.await.map_err(|e| format!("publish ack: {e}"))?;
        Ok(())
    }
}

/// 投递进度在元数据库中的键。
fn cursor_key(sink: &str) -> String {
    format!("events.cursor.{sink}")
}

fn now_ms() -> i64 {
    crate::service::repos::now_ms()
}

/// 一个推送目标的投递循环。
pub struct Dispatcher {
    pub meta: Arc<dyn MetaStore>,
    pub blobs: Arc<dyn BlobStore>,
    pub sink: Arc<dyn Sink>,
    /// 本节点在租约中的名字。
    pub holder: String,
    /// 没有新事件时的轮询间隔。
    pub poll: Duration,
}

impl Dispatcher {
    /// 第一次配置这个推送目标时，把投递进度设为当前位置（不重放历史事件）。服务端在开始监听之前调用，
    /// 之后的变化都会推送。
    pub async fn init_cursor(&self) -> Result<(), String> {
        let key = cursor_key(self.sink.name());
        if self.meta.get_setting(&key).await.map_err(|e| e.to_string())?.is_none() {
            let (_, last) = self.meta.ref_event_bounds().await.map_err(|e| e.to_string())?;
            self.save_cursor(last).await?;
            tracing::info!(
                sink = self.sink.name(),
                seq = last,
                "event delivery starts at the current position"
            );
        }
        Ok(())
    }

    /// 持有租约时投递一批事件，返回投递的条数；没有租约时返回 `Ok(None)`。
    pub async fn step(&self, batch: usize) -> Result<Option<usize>, String> {
        let name = self.sink.name();
        let scope = format!("events/{name}");
        let ttl = i64::try_from(LEASE_TTL.as_millis()).unwrap_or(i64::MAX);
        let lease = self
            .meta
            .acquire_lease(&scope, &self.holder, ttl, now_ms())
            .await
            .map_err(|e| e.to_string())?;
        if lease.holder != self.holder {
            return Ok(None);
        }
        let (oldest, last) = self.meta.ref_event_bounds().await.map_err(|e| e.to_string())?;
        let cursor = match self
            .meta
            .get_setting(&cursor_key(name))
            .await
            .map_err(|e| e.to_string())?
        {
            Some(raw) => String::from_utf8_lossy(&raw)
                .parse::<u64>()
                .map_err(|e| format!("bad {name} cursor: {e}"))?,
            None => {
                // 新配置的推送目标从现在开始，不重放历史事件
                self.save_cursor(last).await?;
                tracing::info!(sink = name, seq = last, "event delivery starts at the current position");
                last
            }
        };
        if oldest.is_some_and(|lo| lo > cursor.saturating_add(1)) {
            tracing::error!(
                sink = name,
                cursor,
                oldest,
                "events were pruned before delivery; some changes were skipped"
            );
        }
        scion_obs::metrics().event_lag(name, last.saturating_sub(cursor));
        let events = self
            .meta
            .list_ref_events(cursor, batch, None)
            .await
            .map_err(|e| e.to_string())?;
        let mut delivered = 0;
        for ev in &events {
            let body = match payload(self.meta.as_ref(), self.blobs.as_ref(), ev).await {
                Ok(p) => serde_json::to_vec(&p).map_err(|e| e.to_string())?,
                Err(e) => {
                    // 历史已不可读（例如仓库已清除）：只推送事件本身
                    tracing::warn!(sink = name, seq = ev.seq, error = %e, "event without commit details");
                    serde_json::to_vec(&Payload {
                        kind: event_kind(ev),
                        seq: ev.seq,
                        repo: ev.repo.clone(),
                        reference: ev.name.clone(),
                        old: ev.old_target.map(|t| t.to_string()),
                        new: ev.new_target.map(|t| t.to_string()),
                        old_git_sha: None,
                        new_git_sha: None,
                        version: ev.version,
                        time_ms: ev.created_at_ms,
                        commits: Vec::new(),
                        commits_truncated: true,
                    })
                    .map_err(|e| e.to_string())?
                }
            };
            if let Err(e) = self.sink.deliver(event_kind(ev), ev.seq, &body).await {
                scion_obs::metrics().event_failed(name);
                return Err(e);
            }
            scion_obs::metrics().event_delivered(name);
            self.save_cursor(ev.seq).await?;
            delivered += 1;
        }
        scion_obs::metrics().event_lag(name, last.saturating_sub(events.last().map_or(cursor, |e| e.seq)));
        Ok(Some(delivered))
    }

    async fn save_cursor(&self, seq: u64) -> Result<(), String> {
        self.meta
            .put_setting(&cursor_key(self.sink.name()), seq.to_string().as_bytes())
            .await
            .map_err(|e| e.to_string())
    }

    /// 一直运行：有事件时连续投递，失败时退避重试。
    pub async fn run(self) {
        let mut backoff = Duration::from_secs(1);
        loop {
            match self.step(100).await {
                Ok(Some(n)) if n > 0 => {
                    backoff = Duration::from_secs(1);
                    continue;
                }
                Ok(_) => {
                    backoff = Duration::from_secs(1);
                    tokio::time::sleep(self.poll).await;
                }
                Err(e) => {
                    tracing::warn!(sink = self.sink.name(), error = %e, retry_in = ?backoff, "event delivery failed");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
    }
}

/// 按配置创建推送目标。
pub fn sinks(cfg: &EventsConfig) -> Result<Vec<Arc<dyn Sink>>, String> {
    let mut out: Vec<Arc<dyn Sink>> = Vec::new();
    if let Some(w) = &cfg.webhook {
        out.push(Arc::new(WebhookSink::new(w)?));
    }
    if let Some(n) = &cfg.nats {
        out.push(Arc::new(NatsSink::new(n)));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_store::{MemStore, RefExpect};
    use std::sync::Mutex as StdMutex;

    /// 测试用推送目标：记录收到的内容；`fail` 次数内返回失败。
    #[derive(Default)]
    struct Recorder {
        got: StdMutex<Vec<(String, u64, serde_json::Value)>>,
        fail: StdMutex<usize>,
    }

    #[async_trait]
    impl Sink for Recorder {
        fn name(&self) -> &'static str {
            "test"
        }
        async fn deliver(&self, kind: &str, seq: u64, body: &[u8]) -> Result<(), String> {
            let mut fail = self.fail.lock().expect("lock");
            if *fail > 0 {
                *fail -= 1;
                return Err("down".into());
            }
            let v = serde_json::from_slice(body).expect("json");
            self.got.lock().expect("lock").push((kind.to_owned(), seq, v));
            Ok(())
        }
    }

    async fn commit(store: &MemStore, parents: Vec<ObjectId>, path: &str, message: &str) -> ObjectId {
        use scion_object::{EntryMode, FileChange, Signature};
        let who = Signature {
            name: "Ada".into(),
            email: "ada@example.com".into(),
            time_ms: 5,
            tz_offset_minutes: 0,
        };
        let blob = ObjectId::of_blob(path.as_bytes());
        let cs = Changeset::new(
            parents.clone(),
            who.clone(),
            who,
            message,
            [path.len() as u8; 16],
            vec![
                FileChange::put(path, EntryMode::File, blob, 3),
                FileChange::delete("old.txt"),
            ],
            Default::default(),
        )
        .expect("changeset");
        let bytes = cs.encode();
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, &bytes);
        store.put(id, bytes::Bytes::from(bytes)).await.expect("put");
        let generation = parents.len() as u64 + 1;
        let rec = scion_store::ChangesetRecord {
            id,
            root_tree: ObjectId::hash(scion_object::ObjectKind::Tree, path.as_bytes()),
            parents,
            generation,
            fp_depth: generation,
            fp_jump: None,
        };
        store.put_changeset("r", &rec).await.expect("record");
        id
    }

    fn dispatcher(store: Arc<MemStore>, sink: Arc<Recorder>, holder: &str) -> Dispatcher {
        Dispatcher {
            meta: store.clone(),
            blobs: store,
            sink,
            holder: holder.into(),
            poll: Duration::from_millis(10),
        }
    }

    #[tokio::test]
    async fn delivers_in_order_with_commit_details_and_retries() {
        let store = Arc::new(MemStore::new());
        store.create_repo("r").await.expect("repo");
        // 推送目标第一次启动之前的事件不重放
        let before = commit(&store, vec![], "before.txt", "before").await;
        store
            .cas_ref("r", "old", RefExpect::Absent, before)
            .await
            .expect("old ref");
        let sink = Arc::new(Recorder::default());
        let d = dispatcher(store.clone(), sink.clone(), "node-a");
        d.init_cursor().await.expect("init");
        d.init_cursor().await.expect("init is idempotent");
        assert_eq!(d.step(10).await, Ok(Some(0)));

        let a = commit(&store, vec![], "a.txt", "first").await;
        let b = commit(&store, vec![a], "b.txt", "second").await;
        store.cas_ref("r", "main", RefExpect::Absent, a).await.expect("create");
        store
            .cas_ref("r", "main", RefExpect::Version(1), b)
            .await
            .expect("advance");
        store.delete_ref("r", "main", None).await.expect("delete");

        // 第一次投递失败：进度不变，下一次重试同一个事件
        *sink.fail.lock().expect("lock") = 1;
        assert_eq!(d.step(10).await, Err("down".into()));
        assert_eq!(d.step(10).await, Ok(Some(3)));
        let got = sink.got.lock().expect("lock").clone();
        let kinds: Vec<&str> = got.iter().map(|(k, _, _)| k.as_str()).collect();
        assert_eq!(kinds, ["ref.created", "ref.updated", "ref.deleted"]);
        assert!(got.windows(2).all(|w| w[1].1 == w[0].1 + 1));
        let created = &got[0].2;
        assert_eq!(created["repo"], "r");
        assert_eq!(created["ref"], "main");
        assert!(created["old"].is_null());
        assert_eq!(created["commits"].as_array().map(Vec::len), Some(1));
        let updated = &got[1].2;
        assert_eq!(updated["old"], a.to_string());
        assert_eq!(updated["commits"][0]["id"], b.to_string());
        assert_eq!(updated["commits"][0]["message"], "second");
        assert_eq!(updated["commits"][0]["author"]["email"], "ada@example.com");
        assert_eq!(updated["commits"][0]["changes"][0]["path"], "b.txt");
        assert_eq!(updated["commits"][0]["changes"][0]["size"], 3);
        assert_eq!(updated["commits"][0]["changes"][1]["op"], "delete");
        // 这里的提交没有真实的目录树：算不出 git sha 时省略，事件照常推送
        assert!(updated["commits"][0]["git_sha"].is_null());
        assert!(updated["old_git_sha"].is_null() && updated["new_git_sha"].is_null());
        assert!(got[2].2["new"].is_null());
        assert_eq!(got[2].2["commits"].as_array().map(Vec::len), Some(0));
        assert_eq!(d.step(10).await, Ok(Some(0)), "nothing left");

        // 只有持有租约的节点投递
        let other = dispatcher(store.clone(), sink.clone(), "node-b");
        assert_eq!(other.step(10).await, Ok(None));
    }

    #[tokio::test]
    async fn history_that_cannot_be_read_still_produces_events() {
        let store = Arc::new(MemStore::new());
        store.create_repo("r").await.expect("repo");
        let sink = Arc::new(Recorder::default());
        let d = dispatcher(store.clone(), sink.clone(), "n");
        d.step(1).await.expect("start");
        let ghost = ObjectId::hash(scion_object::ObjectKind::Changeset, b"ghost");
        store.cas_ref("r", "main", RefExpect::Absent, ghost).await.expect("ref");
        assert_eq!(d.step(10).await, Ok(Some(1)));
        let got = sink.got.lock().expect("lock").clone();
        assert_eq!(got[0].2["commits_truncated"], true);
        // 进度损坏
        store.put_setting("events.cursor.test", b"x").await.expect("put");
        assert!(d.step(10).await.expect_err("bad cursor").contains("bad test cursor"));
    }

    #[tokio::test]
    async fn run_loop_retries_and_keeps_going() {
        let store = Arc::new(MemStore::new());
        store.create_repo("r").await.expect("repo");
        let sink = Arc::new(Recorder::default());
        let d = dispatcher(store.clone(), sink.clone(), "n");
        d.step(1).await.expect("start");
        let a = commit(&store, vec![], "a.txt", "first").await;
        store.cas_ref("r", "main", RefExpect::Absent, a).await.expect("ref");
        *sink.fail.lock().expect("lock") = 1;
        let task = tokio::spawn(d.run());
        for _ in 0..200 {
            if !sink.got.lock().expect("lock").is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        task.abort();
        assert_eq!(sink.got.lock().expect("lock").len(), 1, "delivered after a retry");
    }

    #[test]
    fn signatures_kinds_and_subjects() {
        // RFC 4231 测试用例 2
        assert_eq!(
            signature(b"Jefe", b"what do ya want for nothing?"),
            "sha256=5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        let ev = |old: Option<u8>, new: Option<u8>| RefEvent {
            seq: 1,
            repo: "r".into(),
            name: "main".into(),
            old_target: old.map(|n| ObjectId::hash(scion_object::ObjectKind::Changeset, &[n])),
            new_target: new.map(|n| ObjectId::hash(scion_object::ObjectKind::Changeset, &[n])),
            version: 1,
            created_at_ms: 1,
        };
        assert_eq!(event_kind(&ev(None, Some(1))), "ref.created");
        assert_eq!(event_kind(&ev(Some(1), Some(2))), "ref.updated");
        assert_eq!(event_kind(&ev(Some(1), None)), "ref.deleted");
        let nats = NatsSink::new(&NatsConfig {
            url: "nats://127.0.0.1:1".into(),
            subject_prefix: "scion".into(),
        });
        assert_eq!(nats.subject("ref.updated"), "scion.ref.updated");
        assert_eq!(nats.name(), "nats");
        let cfg = EventsConfig {
            webhook: Some(WebhookConfig {
                url: "http://127.0.0.1:1/hook".into(),
                secret: None,
                timeout: Duration::from_secs(1),
            }),
            nats: Some(NatsConfig {
                url: "nats://127.0.0.1:1".into(),
                subject_prefix: "scion".into(),
            }),
            retention: Duration::from_secs(1),
        };
        let names: Vec<&str> = sinks(&cfg).expect("sinks").iter().map(|s| s.name()).collect();
        assert_eq!(names, ["webhook", "nats"]);
    }

    #[tokio::test]
    async fn unreachable_targets_fail() {
        let nats = NatsSink::new(&NatsConfig {
            url: "nats://127.0.0.1:1".into(),
            subject_prefix: "scion".into(),
        });
        assert!(nats.deliver("ref.updated", 1, b"{}").await.is_err());
        let hook = WebhookSink::new(&WebhookConfig {
            url: "http://127.0.0.1:1/hook".into(),
            secret: Some("s".into()),
            timeout: Duration::from_secs(1),
        })
        .expect("client");
        assert!(hook.deliver("ref.updated", 1, b"{}").await.is_err());
    }

    /// 本地 HTTP 服务：记录请求头与请求体，按顺序返回给定的状态码。
    async fn hook_server(statuses: Vec<u16>) -> (String, Arc<StdMutex<Vec<(http::HeaderMap, Vec<u8>)>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}/hook", listener.local_addr().expect("addr"));
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            for status in statuses {
                let Ok((mut s, _)) = listener.accept().await else {
                    return;
                };
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                // 读到请求头结束，再按 content-length 读请求体
                let (head, body) = loop {
                    let n = s.read(&mut chunk).await.expect("read");
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..i]).into_owned();
                        let len = head
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                            })
                            .unwrap_or(0);
                        while buf.len() < i + 4 + len {
                            let n = s.read(&mut chunk).await.expect("read body");
                            buf.extend_from_slice(&chunk[..n]);
                        }
                        break (head, buf[i + 4..i + 4 + len].to_vec());
                    }
                };
                let mut headers = http::HeaderMap::new();
                for l in head.lines().skip(1) {
                    if let Some((k, v)) = l.split_once(':')
                        && let (Ok(k), Ok(v)) = (
                            http::HeaderName::from_bytes(k.trim().as_bytes()),
                            http::HeaderValue::from_str(v.trim()),
                        )
                    {
                        headers.insert(k, v);
                    }
                }
                log.lock().expect("lock").push((headers, body));
                let resp = format!("HTTP/1.1 {status} X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
                let _ = s.write_all(resp.as_bytes()).await;
            }
        });
        (url, seen)
    }

    #[tokio::test]
    async fn webhook_posts_signed_json() {
        let (url, seen) = hook_server(vec![500, 204]).await;
        let hook = WebhookSink::new(&WebhookConfig {
            url,
            secret: Some("s3cret".into()),
            timeout: Duration::from_secs(5),
        })
        .expect("client");
        assert!(
            hook.deliver("ref.updated", 7, b"{\"a\":1}")
                .await
                .expect_err("500")
                .contains("HTTP 500")
        );
        hook.deliver("ref.updated", 7, b"{\"a\":1}").await.expect("204");
        let seen = seen.lock().expect("lock");
        let (h, body) = &seen[1];
        assert_eq!(body, b"{\"a\":1}");
        assert_eq!(h["x-scion-event"], "ref.updated");
        assert_eq!(h["x-scion-delivery"], "7");
        assert_eq!(
            h["x-scion-signature-256"].to_str().expect("ascii"),
            signature(b"s3cret", body)
        );
        assert_eq!(h["content-type"], "application/json");
    }
}
