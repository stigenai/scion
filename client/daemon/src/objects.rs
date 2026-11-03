//! daemon 的 `ObjectService`：本地命中就地应答，未命中按调用者身份回源（经服务端推荐的区域缓存），
//! 同一仓库对同一对象的并发未命中只回源一次。
//!
//! 与服务端一致，对象只对索引中有它的仓库可见：本地缓存按 id 去重，但应答前确认 id 在调用者的仓库里
//! 可见。回源成功的 (服务端, 仓库, id) 记入可见集；命中而可见性未知的，先向服务端 `HasObjects` 确认。
//!
//! 读权限：每次 `GetObjects`（命中也一样）都用调用者自己的令牌向服务端 `CheckAccess`，不缓存结果；
//! 服务端不可达时拒绝读取。
//!
//! redaction：按 (服务端, 仓库) 保存 redact 过的对象，最多每 [`REDACTION_TTL`] 用调用者的身份增量同步一次。
//! redact 过的对象返回与服务端相同的 REDACTED，本地副本删除。同步失败（包括离线）时拒绝读取。

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use scion_flight::{Claim, Group};
use scion_object::ObjectId;
use scion_proto::v1::object_service_client::ObjectServiceClient;
use scion_proto::v1::object_service_server::ObjectService;
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetObjectsResponse, HasObjectsRequest, HasObjectsResponse, ListRedactedObjectsRequest,
    ListRedactedObjectsResponse,
};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::Daemon;
use crate::upstream::{auth_of, forward, upstream_of};

pub type GetObjectsStream = Pin<Box<dyn Stream<Item = Result<GetObjectsResponse, Status>> + Send>>;

/// (服务端, 仓库, 对象)。
pub type Key = (Arc<str>, Arc<str>, ObjectId);

/// 并发未命中的合并：同一仓库的同一对象只回源一次。
pub type Flight = Group<Key, Result<Bytes, Status>>;

/// 已确认可见的 (服务端, 仓库, 对象)。
pub type Visible = moka::sync::Cache<Key, ()>;

/// 两次同步 redaction 的默认最长间隔。
pub const REDACTION_TTL: std::time::Duration = std::time::Duration::from_secs(10);

/// 一次同步最多读取的页数；剩下的下次请求继续。
const MAX_REDACTION_PAGES: usize = 100;

/// 一个 (服务端, 仓库) 的 redaction 状态。
#[derive(Default)]
pub struct RepoRedactions {
    after_seq: u64,
    synced_at: Option<std::time::Instant>,
    redacted: HashMap<ObjectId, String>,
}

/// 各 (服务端, 仓库) 的 redaction 状态；锁同时让同一仓库的并发同步只做一次。
pub type Redactions = moka::sync::Cache<(Arc<str>, Arc<str>), Arc<tokio::sync::Mutex<RepoRedactions>>>;

/// 按仓库名保存的状态（可见集、redaction）的最长保留时间。服务端删除的仓库名至少 2 小时后才能重新使用，
/// 所以重新创建的同名仓库不会沿用旧仓库的状态。
pub const REPO_STATE_TTL: std::time::Duration = std::time::Duration::from_secs(3600);

pub fn new_visible_cache() -> Visible {
    moka::sync::Cache::builder()
        .max_capacity(1_000_000)
        .time_to_live(REPO_STATE_TTL)
        .build()
}

pub fn new_redaction_cache() -> Redactions {
    moka::sync::Cache::builder()
        .max_capacity(100_000)
        .time_to_live(REPO_STATE_TTL)
        .build()
}

/// 与服务端相同的「对象不存在」错误。
fn object_missing(ids: &[ObjectId]) -> Status {
    let listed: Vec<String> = ids.iter().take(32).map(ToString::to_string).collect();
    scion_client::Error::Rpc {
        code: tonic::Code::NotFound,
        reason: Some(scion_proto::v1::ErrorReason::ObjectMissing),
        message: format!("{} object(s) not found", ids.len()),
        metadata: HashMap::from([
            ("ids".to_string(), listed.join(",")),
            ("count".to_string(), ids.len().to_string()),
        ]),
    }
    .to_status()
}

/// 与服务端相同的「对象已被 redact」错误。
fn redacted(objects: &[(ObjectId, String)]) -> Status {
    let shown = &objects[..objects.len().min(32)];
    scion_client::Error::Rpc {
        code: tonic::Code::NotFound,
        reason: Some(scion_proto::v1::ErrorReason::Redacted),
        message: format!("{} object(s) are redacted", objects.len()),
        metadata: HashMap::from([
            (
                "ids".to_string(),
                shown.iter().map(|(id, _)| id.to_string()).collect::<Vec<_>>().join(","),
            ),
            (
                "redaction_ids".to_string(),
                shown.iter().map(|(_, r)| r.as_str()).collect::<Vec<_>>().join(","),
            ),
            ("count".to_string(), objects.len().to_string()),
        ]),
    }
    .to_status()
}

/// redaction 同步失败时拒绝读取的错误。令牌被拒、仓库不存在等业务错误原样返回，其它情况（包括离线）为 UNAVAILABLE。
fn redaction_sync_failed(repo: &str, cause: Status) -> Status {
    match cause.code() {
        tonic::Code::Unauthenticated
        | tonic::Code::PermissionDenied
        | tonic::Code::NotFound
        | tonic::Code::InvalidArgument => cause,
        _ => Status::unavailable(format!("cannot verify redactions of {repo}: {}", cause.message())),
    }
}

/// 把一个对象从 `start` 开始切成若干分块。`start == len` 时返回一个空的结尾块。
fn chunk_object(id: &ObjectId, data: Bytes, start: usize, chunk: usize) -> Vec<GetObjectsResponse> {
    let total = data.len();
    let id_bytes = Bytes::copy_from_slice(&id.to_bytes());
    let mut out = Vec::new();
    let mut off = start;
    loop {
        let end = (off + chunk).min(total);
        out.push(GetObjectsResponse {
            id: id_bytes.clone(),
            offset: off as u64,
            data: data.slice(off..end),
            encoding: Encoding::Identity.into(),
            last: end == total,
            total_size: total as u64,
        });
        if end == total {
            return out;
        }
        off = end;
    }
}

pub struct DaemonObjectSvc {
    daemon: Arc<Daemon>,
}

impl DaemonObjectSvc {
    pub fn new(daemon: Arc<Daemon>) -> Self {
        Self { daemon }
    }
}

impl Daemon {
    /// 取一组不重复的对象：本地 → 合并的回源。
    pub(crate) async fn objects(
        &self,
        client: &scion_client::Client,
        upstream: &Arc<str>,
        repo: &Arc<str>,
        ids: &[ObjectId],
    ) -> HashMap<ObjectId, Result<Bytes, Status>> {
        let mut out = HashMap::with_capacity(ids.len());
        let mut leaders = Vec::new();
        let mut followers = Vec::new();
        for id in ids {
            if let Some(b) = self.cas.get(id) {
                self.stats.hit();
                out.insert(*id, Ok(b));
                continue;
            }
            self.stats.miss();
            match self.flight.claim((upstream.clone(), repo.clone(), *id)) {
                Claim::Leader(l) => {
                    // 上一个 leader 可能刚刚写入
                    match self.cas.get(id) {
                        Some(b) => {
                            out.insert(*id, Ok(b.clone()));
                            l.complete(Ok(b));
                        }
                        None => leaders.push(l),
                    }
                }
                Claim::Follower(f) => followers.push((*id, f)),
            }
        }
        if !leaders.is_empty() {
            let want: Vec<ObjectId> = leaders.iter().map(|l| l.key().2).collect();
            let results: Vec<Result<Bytes, Status>> = match client.get_objects(&want).await {
                Ok(v) => v.into_iter().map(Ok).collect(),
                Err(e) if want.len() == 1 => vec![Err(e.to_status())],
                // 整批失败可能只是其中一个对象的问题：逐个重试，不连累其它对象
                Err(_) => {
                    let mut v = Vec::with_capacity(want.len());
                    for id in &want {
                        v.push(client.get_object(id).await.map_err(|e| e.to_status()));
                    }
                    v
                }
            };
            let mut fetched = Vec::new();
            for (l, r) in leaders.into_iter().zip(results) {
                let id = l.key().2;
                if let Ok(b) = &r {
                    self.cas.remember(id, b);
                    // 服务端按这个仓库校验过：可见。先记下再交付，等待者据此免查
                    self.visible.insert(l.key().clone(), ());
                    fetched.push((id, b.clone()));
                }
                l.complete(r.clone());
                out.insert(id, r);
            }
            // 落盘不阻塞应答：对象已在内存层，随后的读取照样命中
            if !fetched.is_empty() {
                let cas = self.cas.clone();
                tokio::task::spawn_blocking(move || {
                    for (id, b) in fetched {
                        if let Err(e) = cas.persist(id, &b) {
                            tracing::warn!(%id, error = %e, "writing the object cache failed");
                        }
                    }
                });
            }
        }
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
            out.extend(Box::pin(self.objects(client, upstream, repo, &retry)).await);
        }
        out
    }

    /// 确认调用者（`authorization`）能读取整个仓库：本地命中也不例外。每次读取都用调用者的令牌向服务端
    /// `CheckAccess`，结果不缓存：令牌被吊销或过期、策略改变后，下一次读取就被拒绝（ent ADR 0004）。
    /// 服务端不可达时拒绝读取（离线不放行私有对象）；没有 AuthService 的旧服务端视为允许。
    pub(crate) async fn check_access(
        &self,
        raw: &tonic::transport::Channel,
        repo: &Arc<str>,
        authorization: Option<&tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    ) -> Result<(), Status> {
        self.stats.access_check();
        let mut req = tonic::Request::new(scion_proto::v1::CheckAccessRequest {
            repo: repo.to_string(),
            action: scion_proto::v1::Action::Read.into(),
            r#ref: String::new(),
            path: String::new(),
        });
        if let Some(a) = authorization {
            req.metadata_mut().insert("authorization", a.clone());
        }
        let allowed = match scion_proto::v1::auth_service_client::AuthServiceClient::new(raw.clone())
            .check_access(req)
            .await
        {
            Ok(r) => r.into_inner().allowed,
            Err(s) if s.code() == tonic::Code::Unimplemented => true,
            Err(s) => {
                tracing::debug!(%repo, code = ?s.code(), "checking read access failed; refusing the read");
                return Err(s);
            }
        };
        if !allowed {
            return Err(if authorization.is_none() {
                Status::unauthenticated("authentication required")
            } else {
                scion_client::Error::Rpc {
                    code: tonic::Code::PermissionDenied,
                    reason: Some(scion_proto::v1::ErrorReason::PermissionDenied),
                    message: format!("permission denied: read on {repo}"),
                    metadata: HashMap::from([
                        ("repo".to_string(), repo.to_string()),
                        ("action".to_string(), "read".to_string()),
                    ]),
                }
                .to_status()
            });
        }
        Ok(())
    }

    /// `ids` 中有 redact 过的对象时返回 REDACTED。状态过期时先用调用者的身份向服务端同步。
    ///
    /// 同步失败（包括离线）时拒绝这次读取：否则 redact 之后本地副本会一直返回，`REDACTION_TTL`
    /// 的上限不成立。只有不支持 redaction 的旧服务端（`Unimplemented`）按没有 redaction 处理。
    pub(crate) async fn check_redactions(
        &self,
        client: &scion_client::Client,
        upstream: &Arc<str>,
        repo: &Arc<str>,
        ids: &[ObjectId],
    ) -> Result<(), Status> {
        let state = self
            .redactions
            .get_with((upstream.clone(), repo.clone()), Default::default);
        let mut st = state.lock().await;
        if st.synced_at.is_none_or(|t| t.elapsed() >= self.redaction_ttl) {
            let mut synced = false;
            for _ in 0..MAX_REDACTION_PAGES {
                let page = match client.list_redacted_objects(st.after_seq).await {
                    Ok(p) => p,
                    // 不支持 redaction 的旧服务端
                    Err(scion_client::Error::Rpc {
                        code: tonic::Code::Unimplemented,
                        ..
                    }) => {
                        st.synced_at = Some(std::time::Instant::now());
                        synced = true;
                        break;
                    }
                    Err(e) => {
                        tracing::debug!(%repo, error = %e, "syncing redactions failed; refusing the read");
                        return Err(redaction_sync_failed(repo, e.to_status()));
                    }
                };
                for o in page.objects {
                    let key = (upstream.clone(), repo.clone(), o.id);
                    if o.active {
                        self.cas.remove(&o.id);
                        self.visible.invalidate(&key);
                        st.redacted.insert(o.id, o.redaction_id);
                    } else {
                        st.redacted.remove(&o.id);
                    }
                }
                st.after_seq = page.next_seq;
                if !page.more {
                    st.synced_at = Some(std::time::Instant::now());
                    synced = true;
                    break;
                }
            }
            if !synced {
                return Err(Status::unavailable(format!(
                    "redactions of {repo} are still syncing; retry the read"
                )));
            }
        }
        let hits: Vec<(ObjectId, String)> = ids
            .iter()
            .filter_map(|id| st.redacted.get(id).map(|r| (*id, r.clone())))
            .collect();
        if hits.is_empty() { Ok(()) } else { Err(redacted(&hits)) }
    }

    /// 确认 `ids` 都在调用者的仓库中可见（`client` 带着仓库与调用者身份）。
    pub(crate) async fn check_visible(
        &self,
        client: &scion_client::Client,
        upstream: &Arc<str>,
        repo: &Arc<str>,
        ids: &[ObjectId],
    ) -> Result<(), Status> {
        let key = |id: ObjectId| (upstream.clone(), repo.clone(), id);
        let unknown: Vec<ObjectId> = ids
            .iter()
            .filter(|id| !self.visible.contains_key(&key(**id)))
            .copied()
            .collect();
        if unknown.is_empty() {
            return Ok(());
        }
        let missing = client.missing_objects(&unknown).await.map_err(|e| e.to_status())?;
        if !missing.is_empty() {
            return Err(object_missing(&missing));
        }
        for id in unknown {
            self.visible.insert(key(id), ());
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl ObjectService for DaemonObjectSvc {
    type GetObjectsStream = GetObjectsStream;

    async fn get_objects(&self, request: Request<GetObjectsRequest>) -> Result<Response<GetObjectsStream>, Status> {
        self.daemon.touch();
        let url = upstream_of(&request)?;
        let auth = auth_of(&request);
        let authorization = request.metadata().get("authorization").cloned();
        let req = request.into_inner();
        let up = self.daemon.upstreams.get(&url)?;
        let client = up.client().await?.with_repo(req.repo.clone()).with_auth(auth);
        let limits = client.server_info().limits.clone();
        if req.wants.len() > limits.max_batch_objects as usize {
            return Err(Status::invalid_argument(format!(
                "{} objects requested; at most {} per request (max_batch_objects)",
                req.wants.len(),
                limits.max_batch_objects
            )));
        }
        let ids = req
            .wants
            .iter()
            .map(|w| {
                ObjectId::from_bytes(&w.id).map_err(|e| Status::invalid_argument(format!("invalid object id: {e}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut unique = ids.clone();
        unique.sort_unstable_by_key(|id| id.to_bytes());
        unique.dedup();
        let (upstream, repo): (Arc<str>, Arc<str>) = (Arc::from(url.as_str()), Arc::from(req.repo.as_str()));
        self.daemon.check_access(&up.raw, &repo, authorization.as_ref()).await?;
        self.daemon.check_redactions(&client, &upstream, &repo, &unique).await?;
        let found = self.daemon.objects(&client, &upstream, &repo, &unique).await;
        let chunk = usize::try_from(limits.max_chunk_bytes).unwrap_or(usize::MAX).max(1);
        let mut parts = Vec::new();
        for (id, want) in ids.iter().zip(&req.wants) {
            let data = match found.get(id) {
                Some(Ok(b)) => b.clone(),
                Some(Err(e)) => return Err(e.clone()),
                None => return Err(Status::internal(format!("object {id} was not resolved"))),
            };
            if want.offset > data.len() as u64 {
                return Err(Status::out_of_range(format!(
                    "offset {} beyond size {} of {id}",
                    want.offset,
                    data.len()
                )));
            }
            parts.extend(chunk_object(id, data, want.offset as usize, chunk).into_iter().map(Ok));
        }
        self.daemon.check_visible(&client, &upstream, &repo, &unique).await?;
        Ok(Response::new(Box::pin(tokio_stream::iter(parts))))
    }

    async fn has_objects(&self, request: Request<HasObjectsRequest>) -> Result<Response<HasObjectsResponse>, Status> {
        self.daemon.touch();
        let up = self.daemon.upstreams.get(&upstream_of(&request)?)?;
        let fwd = forward(&request, request.get_ref().clone());
        let resp = ObjectServiceClient::new(up.raw.clone()).has_objects(fwd).await?;
        Ok(Response::new(resp.into_inner()))
    }

    async fn list_redacted_objects(
        &self,
        request: Request<ListRedactedObjectsRequest>,
    ) -> Result<Response<ListRedactedObjectsResponse>, Status> {
        self.daemon.touch();
        let up = self.daemon.upstreams.get(&upstream_of(&request)?)?;
        let fwd = forward(&request, request.get_ref().clone());
        let resp = ObjectServiceClient::new(up.raw.clone())
            .list_redacted_objects(fwd)
            .await?;
        Ok(Response::new(resp.into_inner()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_cover_the_object_from_offset() {
        let id = ObjectId::of_blob(b"0123456789");
        let parts = chunk_object(&id, Bytes::from_static(b"0123456789"), 2, 4);
        let got: Vec<_> = parts.iter().map(|p| (p.offset, p.data.len(), p.last)).collect();
        assert_eq!(got, [(2, 4, false), (6, 4, true)]);
        let end = chunk_object(&id, Bytes::from_static(b"0123456789"), 10, 4);
        assert_eq!((end.len(), end[0].last, end[0].data.len()), (1, true, 0));
    }
}
