//! 缓存节点的读路径：本地命中 → 合并并发未命中（single-flight）→ 按 rendezvous 分片交给负责的
//! 节点，或自己回源。
//!
//! **按仓库的可见性**：与 L3 一致，对象只对索引中有它的仓库可见。字节按对象 id 缓存（跨仓库去重），
//! 但应答前确认 id 在本次请求的仓库里可见：回源成功（L3 已按仓库校验）的 (仓库, id) 记入可见集；
//! 本地命中而可见性未知的，先向上游批量 `HasObjects` 确认，不可见时返回与 L3 相同的 OBJECT_MISSING。
//! single-flight 按 (仓库, id) 合并，一个仓库的回源结果不会交给另一个仓库的请求。
//!
//! **redaction**：按仓库保存 redact 过的对象，最多每 [`REDACTION_TTL`] 用调用者的令牌向上游增量同步一次
//! （`ListRedactedObjects`）。redact 过的对象返回与 L3 相同的 REDACTED，字节从本地缓存删除。同步失败时拒绝读取。
//!
//! 一个 id 只由负责它的节点回源，整个区域对同一个对象只回源一次。负责的节点不可达时自己回源
//! （区域内绕路，但绝不让客户端绕过 L2 直连源站）。转发来的请求（[`CACHE_FORWARDED_HEADER`]）不再
//! 转发，避免环路。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::future::join_all;
use scion_flight::{Claim, Group};
use scion_object::ObjectId;
use tonic::Status;

use crate::ring::Ring;
use crate::store::ObjectCache;
use crate::upstream::{RedactionPage, RequestCtx, Upstream, UpstreamPool, is_upstream_failure};

#[cfg(doc)]
use crate::upstream::CACHE_FORWARDED_HEADER;

/// 命中率等计数。
#[derive(Debug, Default)]
pub struct Stats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    /// 交给负责节点的对象数。
    pub peer_objects: AtomicU64,
    /// 自己回源的对象数。
    pub upstream_objects: AtomicU64,
    /// 回源请求数（一次请求可含多个对象）。
    pub upstream_requests: AtomicU64,
    /// 为确认可见性而向上游查询的对象数。
    pub visibility_checks: AtomicU64,
    /// 向上游确认读权限的次数。
    pub access_checks: AtomicU64,
}

/// [`Stats`] 的一个快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub peer_objects: u64,
    pub upstream_objects: u64,
    pub upstream_requests: u64,
    pub visibility_checks: u64,
    pub access_checks: u64,
}

impl StatsSnapshot {
    /// 命中率（0..=1）；没有请求时为 1。
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            1.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            peer_objects: self.peer_objects.load(Ordering::Relaxed),
            upstream_objects: self.upstream_objects.load(Ordering::Relaxed),
            upstream_requests: self.upstream_requests.load(Ordering::Relaxed),
            visibility_checks: self.visibility_checks.load(Ordering::Relaxed),
            access_checks: self.access_checks.load(Ordering::Relaxed),
        }
    }
}

type Outcome = Result<Bytes, Status>;
type FlightKey = (Arc<str>, ObjectId);

/// 两次向上游同步 redaction 的最长间隔，即 redact 之后缓存层最多还返回该对象这么久。
pub const REDACTION_TTL: std::time::Duration = std::time::Duration::from_secs(10);

/// 按仓库名保存的状态（可见集、redaction）的最长保留时间。
pub const REPO_STATE_TTL: std::time::Duration = std::time::Duration::from_secs(3600);

/// 一次同步最多读取的页数（每页最多 1000 条）；剩下的下次请求继续。
const MAX_REDACTION_PAGES: usize = 100;

/// 一个仓库的 redaction 状态。
#[derive(Default)]
struct RepoRedactions {
    after_seq: u64,
    synced_at: Option<std::time::Instant>,
    /// 对象 → redaction 标识。
    redacted: HashMap<ObjectId, String>,
}

/// 可见集默认条目数上限（每条约百字节）。
pub const DEFAULT_VISIBLE_ENTRIES: u64 = 10_000_000;

/// 错误中最多列出的 id 个数（与 L3 相同）。
const MAX_IDS_IN_ERROR: usize = 32;

/// 与 L3 相同的「没有权限」错误。
fn permission_denied(repo: &str) -> Status {
    let details = tonic_types::ErrorDetails::with_error_info(
        scion_proto::reason_name(scion_proto::v1::ErrorReason::PermissionDenied),
        scion_proto::ERROR_DOMAIN,
        HashMap::from([
            ("repo".to_string(), repo.to_string()),
            ("action".to_string(), "read".to_string()),
        ]),
    );
    <Status as tonic_types::StatusExt>::with_error_details(
        tonic::Code::PermissionDenied,
        format!("permission denied: read on {repo}"),
        details,
    )
}

/// redaction 同步失败时拒绝读取的错误。令牌被拒、仓库不存在等业务错误原样返回，其它情况为 UNAVAILABLE。
fn redaction_sync_failed(repo: &str, cause: &Status) -> Status {
    match cause.code() {
        tonic::Code::Unauthenticated
        | tonic::Code::PermissionDenied
        | tonic::Code::NotFound
        | tonic::Code::InvalidArgument => cause.clone(),
        _ => Status::unavailable(format!("cannot verify redactions of {repo}: {}", cause.message())),
    }
}

/// 与 L3 相同的「对象已被 redact」错误：NOT_FOUND / REDACTED，metadata 带 `ids`、`redaction_ids` 与 `count`。
pub fn redacted(objects: &[(ObjectId, String)]) -> Status {
    let shown = &objects[..objects.len().min(MAX_IDS_IN_ERROR)];
    let ids: Vec<String> = shown.iter().map(|(id, _)| id.to_string()).collect();
    let rids: Vec<&str> = shown.iter().map(|(_, r)| r.as_str()).collect();
    let details = tonic_types::ErrorDetails::with_error_info(
        scion_proto::reason_name(scion_proto::v1::ErrorReason::Redacted),
        scion_proto::ERROR_DOMAIN,
        HashMap::from([
            ("ids".to_string(), ids.join(",")),
            ("redaction_ids".to_string(), rids.join(",")),
            ("count".to_string(), objects.len().to_string()),
        ]),
    );
    <Status as tonic_types::StatusExt>::with_error_details(
        tonic::Code::NotFound,
        format!("{} object(s) are redacted", objects.len()),
        details,
    )
}

/// 与 L3 相同的「对象不存在」错误：NOT_FOUND / OBJECT_MISSING，metadata 带 `ids` 与 `count`。
pub fn object_missing(ids: &[ObjectId]) -> Status {
    let listed: Vec<String> = ids.iter().take(MAX_IDS_IN_ERROR).map(ToString::to_string).collect();
    let details = tonic_types::ErrorDetails::with_error_info(
        scion_proto::reason_name(scion_proto::v1::ErrorReason::ObjectMissing),
        scion_proto::ERROR_DOMAIN,
        HashMap::from([
            ("ids".to_string(), listed.join(",")),
            ("count".to_string(), ids.len().to_string()),
        ]),
    );
    <Status as tonic_types::StatusExt>::with_error_details(
        tonic::Code::NotFound,
        format!("{} object(s) not found", ids.len()),
        details,
    )
}

pub struct CacheNode {
    store: ObjectCache,
    flight: Group<FlightKey, Outcome>,
    /// 已确认可见的 (仓库, id)。对象一旦进入仓库索引就不会离开；redaction 单独处理（见 `redactions`）。
    visible: moka::sync::Cache<FlightKey, ()>,
    /// 各仓库的 redaction 状态；锁同时让同一仓库的并发同步只做一次。
    redactions: moka::sync::Cache<Arc<str>, Arc<tokio::sync::Mutex<RepoRedactions>>>,
    redaction_ttl: std::time::Duration,
    ring: Ring,
    /// 本节点在 ring 中的地址；不在 ring 中（单节点）时为 `None`。
    me: Option<String>,
    /// 其它缓存节点（请求带转发标记）。
    peers: HashMap<String, Arc<dyn Upstream>>,
    upstream: UpstreamPool,
    stats: Stats,
}

/// 一组要一起取的对象：交给某个节点，或回源。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Route {
    Peer(String),
    Upstream,
}

impl CacheNode {
    pub fn new(
        store: ObjectCache,
        ring: Ring,
        me: Option<String>,
        peers: HashMap<String, Arc<dyn Upstream>>,
        upstream: UpstreamPool,
    ) -> Self {
        Self {
            store,
            flight: Group::new(),
            // 按仓库名保存的状态最多保留 REPO_STATE_TTL：服务端删除的仓库名至少 2 小时后才能重新使用，
            // 重新创建的同名仓库不会沿用旧仓库的可见集和 redaction
            visible: moka::sync::Cache::builder()
                .max_capacity(DEFAULT_VISIBLE_ENTRIES)
                .time_to_live(REPO_STATE_TTL)
                .build(),
            redactions: moka::sync::Cache::builder()
                .max_capacity(1_000_000)
                .time_to_live(REPO_STATE_TTL)
                .build(),
            redaction_ttl: REDACTION_TTL,
            ring,
            me,
            peers,
            upstream,
            stats: Stats::default(),
        }
    }

    /// 修改 redaction 同步间隔（测试用）。
    pub fn with_redaction_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.redaction_ttl = ttl;
        self
    }

    pub fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }

    pub fn store(&self) -> &ObjectCache {
        &self.store
    }

    fn route(&self, id: &ObjectId, forwarded: bool) -> Route {
        if forwarded {
            return Route::Upstream;
        }
        match self.ring.owner(id) {
            Some(owner) if Some(owner) != self.me.as_deref() && self.peers.contains_key(owner) => {
                Route::Peer(owner.to_owned())
            }
            _ => Route::Upstream,
        }
    }

    /// 取一批对象（顺序与 `ids` 相同）。任何一个对象取不到时整个请求失败（与 L3 的语义一致）。
    pub async fn get_many(
        &self,
        repo: &str,
        ids: &[ObjectId],
        ctx: &RequestCtx,
        forwarded: bool,
    ) -> Result<Vec<Bytes>, Status> {
        let mut unique: Vec<ObjectId> = ids.to_vec();
        unique.sort_unstable_by_key(|id| id.to_bytes());
        unique.dedup();
        let repo_key: Arc<str> = Arc::from(repo);
        self.check_access(&repo_key, ctx).await?;
        self.check_redactions(&repo_key, &unique, ctx).await?;
        let found = self.resolve(&repo_key, &unique, ctx, forwarded).await;
        let out: Vec<Bytes> = ids
            .iter()
            .map(|id| match found.get(id) {
                Some(Ok(b)) => Ok(b.clone()),
                Some(Err(e)) => Err(e.clone()),
                None => Err(Status::internal(format!("object {id} was not resolved"))),
            })
            .collect::<Result<_, _>>()?;
        self.check_visible(&repo_key, &unique, ctx).await?;
        Ok(out)
    }

    /// 确认调用者能读取整个仓库（按 id 读对象无法按路径限制，与 L3 的要求相同）。缓存不解析令牌，
    /// 每次读取都用调用者自己的令牌向上游确认，结果不缓存：令牌被吊销或策略改变后，下一次读取就被拒绝，
    /// 命中缓存也一样。缓存的只有对象字节。
    async fn check_access(&self, repo: &Arc<str>, ctx: &RequestCtx) -> Result<(), Status> {
        self.stats.access_checks.fetch_add(1, Ordering::Relaxed);
        if !self.upstream.can_read(repo, ctx).await? {
            return Err(if ctx.authorization.is_none() {
                Status::unauthenticated("authentication required")
            } else {
                permission_denied(repo)
            });
        }
        Ok(())
    }

    /// `ids` 中有 redact 过的对象时返回 REDACTED。状态过期时先向上游同步。
    async fn check_redactions(&self, repo: &Arc<str>, ids: &[ObjectId], ctx: &RequestCtx) -> Result<(), Status> {
        let state = self.redactions.get_with(repo.clone(), Default::default);
        let mut st = state.lock().await;
        if st.synced_at.is_none_or(|t| t.elapsed() >= self.redaction_ttl) {
            self.sync_redactions(repo, &mut st, ctx).await?;
        }
        let hits: Vec<(ObjectId, String)> = ids
            .iter()
            .filter_map(|id| st.redacted.get(id).map(|r| (*id, r.clone())))
            .collect();
        if hits.is_empty() { Ok(()) } else { Err(redacted(&hits)) }
    }

    /// 向上游增量同步一个仓库的 redaction。同步失败时拒绝这次读取（已拉到的变化保留，下一个请求接着同步）：
    /// 否则上游不可用或令牌被拒期间，redact 过的字节会一直从缓存返回，`REDACTION_TTL` 的上限不成立。
    async fn sync_redactions(&self, repo: &Arc<str>, st: &mut RepoRedactions, ctx: &RequestCtx) -> Result<(), Status> {
        for _ in 0..MAX_REDACTION_PAGES {
            let page: RedactionPage = match self.upstream.redactions(repo, st.after_seq, ctx).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(%repo, error = %e, "syncing redactions failed; refusing the read");
                    return Err(redaction_sync_failed(repo, &e));
                }
            };
            for c in page.changes {
                if c.active {
                    // 字节跨仓库共享：从本地删除，其它仓库的读取会重新回源（对象被清除后回源也读不到）
                    self.store.remove(&c.id);
                    self.visible.invalidate(&(repo.clone(), c.id));
                    st.redacted.insert(c.id, c.redaction_id);
                } else {
                    st.redacted.remove(&c.id);
                }
            }
            st.after_seq = page.next_seq;
            if !page.more {
                st.synced_at = Some(std::time::Instant::now());
                return Ok(());
            }
        }
        Err(Status::unavailable(format!(
            "redactions of {repo} are still syncing; retry the read"
        )))
    }

    /// 确认 `ids` 都在 `repo` 中可见：可见集里没有的向上游批量查询。
    async fn check_visible(&self, repo: &Arc<str>, ids: &[ObjectId], ctx: &RequestCtx) -> Result<(), Status> {
        let unknown: Vec<ObjectId> = ids
            .iter()
            .filter(|id| !self.visible.contains_key(&(repo.clone(), **id)))
            .copied()
            .collect();
        if unknown.is_empty() {
            return Ok(());
        }
        self.stats
            .visibility_checks
            .fetch_add(unknown.len() as u64, Ordering::Relaxed);
        let missing = self.upstream.missing(repo, &unknown, ctx).await?;
        if !missing.is_empty() {
            return Err(object_missing(&missing));
        }
        for id in unknown {
            self.visible.insert((repo.clone(), id), ());
        }
        Ok(())
    }

    /// 解析一组不重复的 id。
    async fn resolve(
        &self,
        repo: &Arc<str>,
        ids: &[ObjectId],
        ctx: &RequestCtx,
        forwarded: bool,
    ) -> HashMap<ObjectId, Outcome> {
        let mut out = HashMap::with_capacity(ids.len());
        let cached = join_all(ids.iter().map(|id| self.store.get(id))).await;
        let mut leaders = Vec::new();
        let mut followers = Vec::new();
        for (id, hit) in ids.iter().zip(cached) {
            match hit {
                Some(b) => {
                    self.stats.hits.fetch_add(1, Ordering::Relaxed);
                    out.insert(*id, Ok(b));
                }
                None => {
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                    match self.flight.claim((repo.clone(), *id)) {
                        Claim::Leader(l) => leaders.push(l),
                        Claim::Follower(f) => followers.push((*id, f)),
                    }
                }
            }
        }

        // 成为 leader 后再查一次缓存：上一个 leader 可能刚好在我们查缓存和领取之间完成
        let rechecked = join_all(leaders.iter().map(|l| self.store.get(&l.key().1))).await;
        let mut pending = Vec::with_capacity(leaders.len());
        for (l, hit) in leaders.into_iter().zip(rechecked) {
            match hit {
                Some(b) => {
                    out.insert(l.key().1, Ok(b.clone()));
                    l.complete(Ok(b));
                }
                None => pending.push(l),
            }
        }
        let leaders = pending;

        // 自己负责回源的 id，按去向分组，各组并发
        let mut groups: HashMap<Route, Vec<scion_flight::Leader<FlightKey, Outcome>>> = HashMap::new();
        for l in leaders {
            groups.entry(self.route(&l.key().1, forwarded)).or_default().push(l);
        }
        let fetched = join_all(groups.into_iter().map(|(route, group)| async move {
            let ids: Vec<ObjectId> = group.iter().map(|l| l.key().1).collect();
            let results = self.fetch_group(&route, repo, &ids, ctx).await;
            let mut done = Vec::with_capacity(group.len());
            for (l, r) in group.into_iter().zip(results) {
                let id = l.key().1;
                if let Ok(b) = &r {
                    self.store.insert(id, b.clone());
                    // 上游（或负责的节点）按这个仓库校验过：可见。先记下再交付，等待者据此免查
                    self.visible.insert((repo.clone(), id), ());
                }
                l.complete(r.clone());
                done.push((id, r));
            }
            done
        }))
        .await;
        out.extend(fetched.into_iter().flatten());

        // 别人正在取的 id：等结果；对方被取消时重新解析
        let mut retry = Vec::new();
        for (id, f) in followers {
            match f.wait().await {
                Some(r) => {
                    out.insert(id, r);
                }
                None => retry.push(id),
            }
        }
        if !retry.is_empty() {
            out.extend(Box::pin(self.resolve(repo, &retry, ctx, forwarded)).await);
        }
        out
    }

    /// 取一组对象，返回每个 id 的结果（顺序与 `ids` 相同）。
    async fn fetch_group(&self, route: &Route, repo: &str, ids: &[ObjectId], ctx: &RequestCtx) -> Vec<Outcome> {
        let batch = match route {
            Route::Peer(p) => {
                self.stats.peer_objects.fetch_add(ids.len() as u64, Ordering::Relaxed);
                let peer = self.peers.get(p).cloned();
                match peer {
                    Some(peer) => match peer.fetch(repo, ids, ctx).await {
                        // 负责的节点不可达：自己回源
                        Err(e) if is_upstream_failure(&e) => {
                            tracing::warn!(peer = %p, error = %e, "cache peer unavailable; fetching from upstream");
                            self.fetch_upstream(repo, ids, ctx).await
                        }
                        r => r,
                    },
                    None => self.fetch_upstream(repo, ids, ctx).await,
                }
            }
            Route::Upstream => self.fetch_upstream(repo, ids, ctx).await,
        };
        match batch {
            Ok(objs) => objs.into_iter().map(Ok).collect(),
            Err(e) if ids.len() == 1 => vec![Err(e)],
            // 整批失败：可能只是其中某个对象的问题（例如不存在），逐个重试，其它对象的等待者不受连累
            Err(e) if !is_upstream_failure(&e) => {
                join_all(ids.iter().map(|id| async move {
                    self.fetch_upstream(repo, std::slice::from_ref(id), ctx)
                        .await
                        .and_then(|mut v| v.pop().ok_or_else(|| Status::internal("empty upstream response")))
                }))
                .await
            }
            Err(e) => vec![Err(e); ids.len()],
        }
    }

    async fn fetch_upstream(&self, repo: &str, ids: &[ObjectId], ctx: &RequestCtx) -> Result<Vec<Bytes>, Status> {
        self.stats.upstream_requests.fetch_add(1, Ordering::Relaxed);
        self.stats
            .upstream_objects
            .fetch_add(ids.len() as u64, Ordering::Relaxed);
        self.upstream.fetch(repo, ids, ctx).await
    }

    /// 关闭本地存储（刷盘）。
    pub async fn close(&self) {
        self.store.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::StorageConfig;
    use crate::upstream::PoolConfig;
    use crate::upstream::tests::Fake;
    use std::time::Duration;
    use tonic::Code;

    const A: &[u8] = b"alpha";
    const B: &[u8] = b"beta";
    const C: &[u8] = b"gamma";

    async fn store() -> ObjectCache {
        ObjectCache::open(&StorageConfig {
            memory_bytes: 1 << 20,
            ..StorageConfig::default()
        })
        .await
        .expect("cache")
    }

    fn pool(f: &Arc<Fake>) -> UpstreamPool {
        UpstreamPool::new(
            vec![f.clone() as Arc<dyn Upstream>],
            PoolConfig {
                hedge: false,
                ..PoolConfig::default()
            },
        )
    }

    fn id(b: &[u8]) -> ObjectId {
        ObjectId::of_blob(b)
    }

    async fn single(f: &Arc<Fake>) -> CacheNode {
        CacheNode::new(store().await, Ring::new(Vec::new()), None, HashMap::new(), pool(f))
    }

    #[tokio::test]
    async fn redacted_objects_are_refused_and_evicted() {
        use std::sync::atomic::Ordering;
        let up = Arc::new(Fake::new("l3", &[A, B, C]));
        let n = single(&up).await.with_redaction_ttl(Duration::ZERO);
        let ctx = RequestCtx::default();
        n.get_many("r", &[id(A), id(B)], &ctx, false).await.expect("ok");
        assert!(n.store().get(&id(A)).await.is_some());

        // 三条变化分两页同步
        up.redact(A, true);
        up.redact(C, true);
        up.redact(B, true);
        let err = n
            .get_many("r", &[id(A), id(B)], &ctx, false)
            .await
            .expect_err("redacted");
        assert_eq!(err.code(), Code::NotFound);
        let info = tonic_types::StatusExt::get_error_details(&err)
            .error_info()
            .cloned()
            .expect("info");
        assert_eq!(info.reason, "REDACTED");
        assert_eq!(info.metadata["count"], "2");
        assert_eq!(info.metadata["redaction_ids"], "rd1,rd1");
        assert!(n.store().get(&id(A)).await.is_none(), "evicted");
        assert_eq!(
            up.redaction_calls.load(Ordering::SeqCst),
            3,
            "initial sync, then two pages"
        );

        // 撤销：恢复读取（重新回源）
        up.redact(A, false);
        up.redact(B, false);
        let got = n.get_many("r", &[id(A), id(B)], &ctx, false).await.expect("restored");
        assert_eq!(got, [Bytes::from_static(A), Bytes::from_static(B)]);

        // 同步失败：拒绝读取，不交出本地副本；令牌被拒原样返回
        *up.redaction_error.lock().expect("lock") = Some(Status::unavailable("down"));
        let err = n.get_many("r", &[id(A)], &ctx, false).await.expect_err("refused");
        assert_eq!(err.code(), Code::Unavailable);
        assert!(err.message().contains("cannot verify redactions of r"), "{err:?}");
        *up.redaction_error.lock().expect("lock") = Some(Status::permission_denied("token rejected"));
        assert_eq!(
            n.get_many("r", &[id(A)], &ctx, false).await.map_err(|e| e.code()),
            Err(Code::PermissionDenied)
        );

        // 恢复后照常读取，已知的 redaction 仍然生效
        *up.redaction_error.lock().expect("lock") = None;
        n.get_many("r", &[id(A)], &ctx, false).await.expect("served again");
        assert_eq!(
            n.get_many("r", &[id(C)], &ctx, false).await.map_err(|e| e.code()),
            Err(Code::NotFound),
            "known redaction kept"
        );
    }

    #[tokio::test]
    async fn reads_wait_until_a_long_redaction_backlog_is_synced() {
        let up = Arc::new(Fake::new("l3", &[A, B]));
        // 每页 2 条：超过 MAX_REDACTION_PAGES 页的积压一次同步不完
        for _ in 0..MAX_REDACTION_PAGES {
            up.redact(B, true);
            up.redact(B, false);
        }
        up.redact(B, true);
        let n = single(&up).await;
        let ctx = RequestCtx::default();
        let err = n.get_many("r", &[id(A)], &ctx, false).await.expect_err("still syncing");
        assert_eq!(err.code(), Code::Unavailable);
        // 下一个请求接着同步完剩下的
        n.get_many("r", &[id(A)], &ctx, false).await.expect("synced");
        assert_eq!(
            n.get_many("r", &[id(B)], &ctx, false).await.map_err(|e| e.code()),
            Err(Code::NotFound)
        );
    }

    #[tokio::test]
    async fn redactions_sync_at_most_once_per_ttl() {
        use std::sync::atomic::Ordering;
        let up = Arc::new(Fake::new("l3", &[A]));
        let n = single(&up).await;
        let ctx = RequestCtx::default();
        for _ in 0..3 {
            n.get_many("r", &[id(A)], &ctx, false).await.expect("ok");
        }
        assert_eq!(up.redaction_calls.load(Ordering::SeqCst), 1);
        n.get_many("other", &[id(A)], &ctx, false).await.expect("ok");
        assert_eq!(up.redaction_calls.load(Ordering::SeqCst), 2, "per repository");
        assert_eq!(redacted(&[]).code(), Code::NotFound);
    }

    #[tokio::test]
    async fn misses_are_fetched_once_and_then_hit() {
        let up = Arc::new(Fake::new("l3", &[A, B]));
        let n = single(&up).await;
        let ctx = RequestCtx::default();
        let got = n.get_many("r", &[id(A), id(B), id(A)], &ctx, false).await.expect("ok");
        assert_eq!(
            got,
            [Bytes::from_static(A), Bytes::from_static(B), Bytes::from_static(A)]
        );
        assert_eq!(up.calls(), 1, "one batched upstream request");
        n.get_many("r", &[id(B), id(A)], &ctx, false).await.expect("hit");
        assert_eq!(up.calls(), 1);
        let s = n.stats();
        assert_eq!(
            (s.hits, s.misses, s.upstream_objects, s.upstream_requests),
            (2, 2, 2, 1)
        );
        assert!((s.hit_ratio() - 0.5).abs() < f64::EPSILON);
        assert!((StatsSnapshot::default().hit_ratio() - 1.0).abs() < f64::EPSILON);
        assert_eq!(n.store().get(&id(A)).await, Some(Bytes::from_static(A)));
        n.close().await;
    }

    #[tokio::test]
    async fn concurrent_misses_coalesce() {
        let up = Arc::new(Fake::new("l3", &[A]).delay(Duration::from_millis(30)));
        let n = Arc::new(single(&up).await);
        let tasks: Vec<_> = (0..20)
            .map(|_| {
                let n = n.clone();
                tokio::spawn(async move { n.get_many("r", &[id(A)], &RequestCtx::default(), false).await })
            })
            .collect();
        for t in tasks {
            assert_eq!(t.await.expect("join").expect("ok"), [Bytes::from_static(A)]);
        }
        assert_eq!(up.calls(), 1);
    }

    #[tokio::test]
    async fn missing_object_fails_only_its_own_requests() {
        let up = Arc::new(Fake::new("l3", &[A]));
        let n = single(&up).await;
        let ctx = RequestCtx::default();
        let err = n
            .get_many("r", &[id(A), id(C)], &ctx, false)
            .await
            .expect_err("C missing");
        assert_eq!(err.code(), Code::FailedPrecondition);
        // A 在逐个重试时取到并缓存了
        assert_eq!(n.store().get(&id(A)).await, Some(Bytes::from_static(A)));
        // 上游故障：整组同样的错误
        let down = Arc::new(Fake::new("down", &[A]).script(vec![Err(Status::unavailable("down"))]));
        let n = single(&down).await;
        let err = n.get_many("r", &[id(A), id(B)], &ctx, false).await.expect_err("down");
        assert_eq!(err.code(), Code::Unavailable);
    }

    #[tokio::test]
    async fn abandoned_fetch_is_retried_by_waiters() {
        let up = Arc::new(Fake::new("l3", &[A]));
        let n = single(&up).await;
        // 模拟另一个请求领取后被取消
        let Claim::Leader(l) = n.flight.claim((Arc::from("r"), id(A))) else {
            panic!("leader");
        };
        let ctx = RequestCtx::default();
        let want = [id(A)];
        let (got, ()) = tokio::join!(n.get_many("r", &want, &ctx, false), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(l);
        });
        assert_eq!(got.expect("retried"), [Bytes::from_static(A)]);
    }

    /// 评审指出的问题：缓存命中与 single-flight 不能跨仓库交出对象（与 L3 的按仓库可见性一致）。
    #[tokio::test]
    async fn objects_are_only_served_to_repos_that_can_see_them() {
        let up = Arc::new(Fake::new("l3", &[A, B]).visible_in("a", &[A]).visible_in("b", &[A, B]));
        let n = single(&up).await;
        let ctx = RequestCtx::default();
        // b 取到 B，字节进入缓存
        n.get_many("b", &[id(B)], &ctx, false).await.expect("b sees B");
        // a 看不到 B：命中缓存也不交出，返回与 L3 相同的 OBJECT_MISSING
        let err = n
            .get_many("a", &[id(B)], &ctx, false)
            .await
            .expect_err("a cannot see B");
        assert_eq!(err.code(), Code::NotFound);
        let info = tonic_types::StatusExt::get_error_details(&err)
            .error_info()
            .cloned()
            .expect("info");
        assert_eq!(
            (info.reason.as_str(), info.metadata["count"].as_str()),
            ("OBJECT_MISSING", "1")
        );
        assert_eq!(info.metadata["ids"], id(B).to_string());
        // a 能看到 A：b 取过的字节直接复用，只确认一次可见性
        n.get_many("b", &[id(A)], &ctx, false).await.expect("b sees A");
        let before = up.calls();
        n.get_many("a", &[id(A)], &ctx, false).await.expect("a sees A");
        n.get_many("a", &[id(A)], &ctx, false).await.expect("again");
        assert_eq!(up.calls(), before, "bytes not fetched again");
        assert_eq!(
            up.missing_calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "one check per (repo, id)"
        );
        assert_eq!(n.stats().visibility_checks, 2);
        // 回源成功的对象不需要再确认
        let fresh = single(&up).await;
        fresh.get_many("b", &[id(A), id(B)], &ctx, false).await.expect("ok");
        assert_eq!(fresh.stats().visibility_checks, 0);
        // 确认可见性时上游故障：错误原样返回
        // 依次是：确认读权限、回源、确认可见性（失败）
        let down = Arc::new(Fake::new("down", &[A]).script(vec![Ok(()), Ok(()), Err(Status::unavailable("down"))]));
        let n = single(&down).await;
        n.get_many("x", &[id(A)], &ctx, false).await.expect("fetched");
        n.visible.invalidate_all();
        let err = n.get_many("x", &[id(A)], &ctx, false).await.expect_err("check fails");
        assert_eq!(err.code(), Code::Unavailable);
    }

    /// 命中缓存前先确认调用者能读这个仓库（每次读取都向上游确认）。
    #[tokio::test]
    async fn reads_require_repo_access() {
        let mut f = Fake::new("l3", &[A]);
        f.unreadable = vec!["secret".into()];
        let up = Arc::new(f);
        let n = single(&up).await;
        let anon = RequestCtx::default();
        n.get_many("open", &[id(A)], &anon, false).await.expect("readable");
        n.get_many("open", &[id(A)], &anon, false).await.expect("again");
        assert_eq!(n.stats().access_checks, 2, "checked on every read, also on a hit");
        let err = n
            .get_many("secret", &[id(A)], &anon, false)
            .await
            .expect_err("anonymous denied");
        assert_eq!(err.code(), Code::Unauthenticated);
        let alice = RequestCtx {
            authorization: Some(tonic::metadata::MetadataValue::from_static("Bearer alice")),
            ..RequestCtx::default()
        };
        let err = n.get_many("secret", &[id(A)], &alice, false).await.expect_err("denied");
        assert_eq!(err.code(), Code::PermissionDenied);
        let info = tonic_types::StatusExt::get_error_details(&err)
            .error_info()
            .cloned()
            .expect("info");
        assert_eq!(info.reason, "PERMISSION_DENIED");
        // 不同令牌分别确认
        n.get_many("open", &[id(A)], &alice, false).await.expect("ok");
        assert_eq!(n.stats().access_checks, 5);
        let invalid = RequestCtx {
            authorization: Some(tonic::metadata::MetadataValue::from_static("Bearer invalid")),
            ..RequestCtx::default()
        };
        assert_eq!(
            n.get_many("open", &[id(A)], &invalid, false)
                .await
                .map_err(|e| e.code())
                .expect_err("bad token"),
            Code::Unauthenticated
        );
    }

    /// 并发的不同仓库各自回源：a 的失败不会交给 b。
    #[tokio::test]
    async fn single_flight_is_per_repo() {
        let up = Arc::new(
            Fake::new("l3", &[B])
                .visible_in("a", &[])
                .visible_in("b", &[B])
                .delay(Duration::from_millis(30)),
        );
        let n = Arc::new(single(&up).await);
        let (na, nb) = (n.clone(), n.clone());
        let a = tokio::spawn(async move { na.get_many("a", &[id(B)], &RequestCtx::default(), false).await });
        let b = tokio::spawn(async move { nb.get_many("b", &[id(B)], &RequestCtx::default(), false).await });
        assert!(a.await.expect("join").is_err(), "a cannot see B");
        assert_eq!(b.await.expect("join").expect("b sees B"), [Bytes::from_static(B)]);
        assert_eq!(up.calls(), 2, "one fetch per repo");
    }

    #[tokio::test]
    async fn objects_owned_by_peers_are_fetched_from_them() {
        let up = Arc::new(Fake::new("l3", &[A, B, C]));
        let peer = Arc::new(Fake::new("peer", &[A, B, C]));
        let ring = Ring::new(["me".to_string(), "peer".into()]);
        let ids: Vec<ObjectId> = [A, B, C].iter().map(|b| id(b)).collect();
        let (mine, theirs): (Vec<&ObjectId>, Vec<&ObjectId>) = ids.iter().partition(|i| ring.owner(i) == Some("me"));
        let mut peers: HashMap<String, Arc<dyn Upstream>> = HashMap::new();
        peers.insert("peer".into(), peer.clone());
        let n = CacheNode::new(store().await, ring.clone(), Some("me".into()), peers, pool(&up));
        n.get_many("r", &ids, &RequestCtx::default(), false).await.expect("ok");
        let s = n.stats();
        assert_eq!(s.peer_objects as usize, theirs.len());
        assert_eq!(s.upstream_objects as usize, mine.len());
        assert_eq!(peer.calls(), u32::from(!theirs.is_empty()));

        // 转发来的请求不再转发
        let n2 = CacheNode::new(
            store().await,
            ring.clone(),
            Some("me".into()),
            HashMap::from([("peer".to_string(), peer.clone() as Arc<dyn Upstream>)]),
            pool(&up),
        );
        n2.get_many("r", &ids, &RequestCtx::default(), true).await.expect("ok");
        assert_eq!(n2.stats().peer_objects, 0);

        // 负责的节点不可达：自己回源
        let dead = Arc::new(Fake::new("peer", &[]).script(vec![Err(Status::unavailable("gone"))]));
        let n3 = CacheNode::new(
            store().await,
            ring.clone(),
            Some("me".into()),
            HashMap::from([("peer".to_string(), dead as Arc<dyn Upstream>)]),
            pool(&up),
        );
        n3.get_many("r", &ids, &RequestCtx::default(), false)
            .await
            .expect("fallback");
        assert_eq!(n3.stats().upstream_objects, 3);

        // ring 中有、但没有连接的节点：回源
        let n4 = CacheNode::new(store().await, ring, Some("me".into()), HashMap::new(), pool(&up));
        n4.get_many("r", &ids, &RequestCtx::default(), false).await.expect("ok");
        assert_eq!(n4.stats().peer_objects, 0);
    }
}
