//! 回源：从上游（L3 服务端，或负责某个 id 的另一个缓存节点）批量取对象。
//!
//! [`UpstreamPool`] 管理多个上游副本：
//! - **熔断**：每个上游一个 [`Breaker`]，连续失败的上游在冷却期内被跳过；
//! - **故障转移**：连接层面的失败（不可用、超时、数据损坏）换下一个上游重试；业务错误（对象不存在、
//!   无权限等）直接返回，换上游也不会不同；
//! - **对冲**：请求超过最近 p95 延迟仍未返回时，向另一个上游再发一份，取先成功的结果；
//! - **并发上限**：同时在途的回源请求数有上限，保护上游。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use scion_object::ObjectId;
use scion_proto::v1::auth_service_client::AuthServiceClient;
use scion_proto::v1::object_service_client::ObjectServiceClient;
use scion_proto::v1::{
    Action, CheckAccessRequest, Encoding, GetObjectsRequest, HasObjectsRequest, ListRedactedObjectsRequest, ObjectWant,
};
use tokio::sync::Semaphore;
use tonic::Code;
use tonic::Status;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::Channel;

use crate::breaker::Breaker;
use crate::latency::Latency;

/// 缓存节点之间转发的请求带上的元数据：收到的节点不再转发，直接回源。
pub const CACHE_FORWARDED_HEADER: &str = "x-scion-cache-forwarded";

/// 随请求透传给上游的信息。
#[derive(Debug, Clone, Default)]
pub struct RequestCtx {
    /// 调用者的 `authorization`，原样转给上游（缓存不持有凭证）。
    pub authorization: Option<MetadataValue<Ascii>>,
    /// 调用者的 `traceparent`：本节点没有启用追踪时原样转发，trace 不会断开。
    pub traceparent: Option<MetadataValue<Ascii>>,
    /// 调用者的 deadline：回源沿用剩余的时间。
    pub deadline: Option<scion_obs::deadline::Deadline>,
}

impl RequestCtx {
    /// 给发往上游的请求带上调用者的身份、trace 上下文与剩余的 deadline。
    pub fn apply<T>(&self, req: &mut tonic::Request<T>) {
        if let Some(a) = &self.authorization {
            req.metadata_mut().insert("authorization", a.clone());
        }
        scion_obs::trace::inject_or(req.metadata_mut(), self.traceparent.as_ref());
        if let Some(d) = self.deadline {
            req.set_timeout(d.remaining());
        }
    }
}

/// redaction 的一次变化（`ListRedactedObjects`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionChange {
    pub id: ObjectId,
    pub redaction_id: String,
    /// false 表示已撤销。
    pub active: bool,
}

/// `ListRedactedObjects` 的一页。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedactionPage {
    pub changes: Vec<RedactionChange>,
    /// 下一次请求的 `after_seq`。
    pub next_seq: u64,
    pub more: bool,
}

#[tonic::async_trait]
pub trait Upstream: Send + Sync {
    /// 取一批对象的完整内容，顺序与 `ids` 相同，并校验内容哈希。
    async fn fetch(&self, repo: &str, ids: &[ObjectId], ctx: &RequestCtx) -> Result<Vec<Bytes>, Status>;

    /// `ids` 中在 `repo` 里不可见的对象（`HasObjects`）。
    async fn missing(&self, repo: &str, ids: &[ObjectId], ctx: &RequestCtx) -> Result<Vec<ObjectId>, Status>;

    /// 调用者（`ctx` 中的令牌）能否读取整个仓库（`AuthService.CheckAccess`）。上游不支持时视为允许。
    async fn can_read(&self, repo: &str, ctx: &RequestCtx) -> Result<bool, Status>;

    /// `repo` 中 `after_seq` 之后的 redaction 变化（用调用者的令牌）。上游不支持（Unimplemented）时返回错误，调用方拒绝读取。
    async fn redactions(&self, repo: &str, after_seq: u64, ctx: &RequestCtx) -> Result<RedactionPage, Status>;

    /// 日志中使用的名字（地址）。
    fn name(&self) -> &str;
}

/// 通过 gRPC `GetObjects` 回源。
pub struct GrpcUpstream {
    name: String,
    channel: Channel,
    max_batch: usize,
    /// 是否标记为缓存节点之间的转发。
    forwarded: bool,
}

impl GrpcUpstream {
    pub fn new(name: impl Into<String>, channel: Channel, max_batch: usize) -> Self {
        Self {
            name: name.into(),
            channel,
            max_batch: max_batch.max(1),
            forwarded: false,
        }
    }

    /// 发给另一个缓存节点：带上 [`CACHE_FORWARDED_HEADER`]。
    pub fn forwarded(mut self) -> Self {
        self.forwarded = true;
        self
    }

    async fn fetch_batch(&self, repo: &str, ids: &[ObjectId], ctx: &RequestCtx) -> Result<Vec<Bytes>, Status> {
        let mut req = tonic::Request::new(GetObjectsRequest {
            repo: repo.to_owned(),
            wants: ids
                .iter()
                .map(|id| ObjectWant {
                    id: Bytes::copy_from_slice(&id.to_bytes()),
                    offset: 0,
                })
                .collect(),
            accept_encodings: vec![Encoding::Identity as i32],
        });
        ctx.apply(&mut req);
        if self.forwarded {
            req.metadata_mut()
                .insert(CACHE_FORWARDED_HEADER, MetadataValue::from_static("1"));
        }
        let mut stream = ObjectServiceClient::new(self.channel.clone())
            .max_decoding_message_size(64 << 20)
            .get_objects(req)
            .await?
            .into_inner();
        let mut out = Vec::with_capacity(ids.len());
        let mut buf = BytesMut::new();
        while let Some(part) = stream.message().await? {
            let Some(id) = ids.get(out.len()) else {
                return Err(Status::internal(format!(
                    "{} sent more objects than requested",
                    self.name
                )));
            };
            if part.id.as_ref() != id.to_bytes() || part.offset != buf.len() as u64 {
                return Err(Status::internal(format!(
                    "{} sent an unexpected chunk while reading {id}",
                    self.name
                )));
            }
            buf.extend_from_slice(&part.data);
            if part.last {
                let data = std::mem::take(&mut buf).freeze();
                if data.len() as u64 != part.total_size || !id.verify(&data) {
                    return Err(Status::data_loss(format!("{} returned corrupt object {id}", self.name)));
                }
                out.push(data);
            }
        }
        if out.len() != ids.len() {
            return Err(Status::unavailable(format!(
                "{} ended the stream after {} of {} objects",
                self.name,
                out.len(),
                ids.len()
            )));
        }
        Ok(out)
    }
}

#[tonic::async_trait]
impl Upstream for GrpcUpstream {
    async fn fetch(&self, repo: &str, ids: &[ObjectId], ctx: &RequestCtx) -> Result<Vec<Bytes>, Status> {
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(self.max_batch) {
            out.extend(self.fetch_batch(repo, chunk, ctx).await?);
        }
        Ok(out)
    }

    async fn missing(&self, repo: &str, ids: &[ObjectId], ctx: &RequestCtx) -> Result<Vec<ObjectId>, Status> {
        let mut out = Vec::new();
        for chunk in ids.chunks(self.max_batch) {
            let mut req = tonic::Request::new(HasObjectsRequest {
                repo: repo.to_owned(),
                ids: chunk.iter().map(|id| Bytes::copy_from_slice(&id.to_bytes())).collect(),
            });
            ctx.apply(&mut req);
            let resp = ObjectServiceClient::new(self.channel.clone())
                .has_objects(req)
                .await?
                .into_inner();
            for raw in resp.missing_ids {
                out.push(
                    ObjectId::from_bytes(&raw)
                        .map_err(|e| Status::internal(format!("{} returned a bad id: {e}", self.name)))?,
                );
            }
        }
        Ok(out)
    }

    async fn can_read(&self, repo: &str, ctx: &RequestCtx) -> Result<bool, Status> {
        let mut req = tonic::Request::new(CheckAccessRequest {
            repo: repo.to_owned(),
            action: Action::Read.into(),
            r#ref: String::new(),
            path: String::new(),
        });
        ctx.apply(&mut req);
        match AuthServiceClient::new(self.channel.clone()).check_access(req).await {
            Ok(r) => Ok(r.into_inner().allowed),
            // 没有 AuthService 的上游无法确认调用者有权读取：拒绝（fail closed）。
            Err(s) if s.code() == Code::Unimplemented => {
                tracing::warn!(upstream = %self.name, "upstream has no AuthService; cache denies the read");
                Ok(false)
            }
            Err(s) => Err(s),
        }
    }

    async fn redactions(&self, repo: &str, after_seq: u64, ctx: &RequestCtx) -> Result<RedactionPage, Status> {
        let mut req = tonic::Request::new(ListRedactedObjectsRequest {
            repo: repo.to_owned(),
            after_seq,
            page_size: 0,
        });
        ctx.apply(&mut req);
        match ObjectServiceClient::new(self.channel.clone())
            .list_redacted_objects(req)
            .await
        {
            Ok(r) => {
                let r = r.into_inner();
                let changes = r
                    .objects
                    .into_iter()
                    .map(|o| {
                        Ok(RedactionChange {
                            id: ObjectId::from_bytes(&o.id)
                                .map_err(|e| Status::internal(format!("upstream sent a bad object id: {e}")))?,
                            redaction_id: o.redaction_id,
                            active: o.active,
                        })
                    })
                    .collect::<Result<_, Status>>()?;
                Ok(RedactionPage {
                    changes,
                    next_seq: r.next_seq,
                    more: r.more,
                })
            }
            // Unimplemented 也是错误：无法确认 redaction 时调用方拒绝读取
            Err(s) => Err(s),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// 是否是上游本身的故障（值得换一个上游重试、计入熔断）。业务错误换上游也一样，直接返回。
pub fn is_upstream_failure(s: &Status) -> bool {
    matches!(
        s.code(),
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Unknown
            | Code::Internal
            | Code::DataLoss
            | Code::Cancelled
            | Code::ResourceExhausted
            | Code::Unimplemented
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolConfig {
    /// 同时在途的回源请求上限。
    pub max_in_flight: usize,
    /// 是否对冲。
    pub hedge: bool,
    /// 对冲阈值的上下限（阈值本身取最近的 p95）。
    pub hedge_min: Duration,
    pub hedge_max: Duration,
    /// 熔断：连续失败多少次打开、冷却多久。
    pub breaker_threshold: u32,
    pub breaker_cooldown: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 512,
            hedge: true,
            hedge_min: Duration::from_millis(5),
            hedge_max: Duration::from_secs(2),
            breaker_threshold: 5,
            breaker_cooldown: Duration::from_secs(5),
        }
    }
}

struct Member {
    up: Arc<dyn Upstream>,
    breaker: Breaker,
}

/// 一组等价的上游副本。
pub struct UpstreamPool {
    members: Vec<Member>,
    next: AtomicUsize,
    permits: Semaphore,
    latency: Latency,
    config: PoolConfig,
}

type Attempt<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<Bytes>, Status>> + Send + 'a>>;

impl UpstreamPool {
    pub fn new(upstreams: Vec<Arc<dyn Upstream>>, config: PoolConfig) -> Self {
        Self {
            members: upstreams
                .into_iter()
                .map(|up| Member {
                    up,
                    breaker: Breaker::new(config.breaker_threshold, config.breaker_cooldown),
                })
                .collect(),
            next: AtomicUsize::new(0),
            permits: Semaphore::new(config.max_in_flight.max(1)),
            latency: Latency::default(),
            config,
        }
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// 本次请求依次尝试的上游：从轮询位置开始，跳过熔断中的。
    fn order(&self) -> Vec<usize> {
        let n = self.members.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        (0..n).map(|i| (start + i) % n).collect()
    }

    fn attempt<'a>(&'a self, i: usize, repo: &'a str, ids: &'a [ObjectId], ctx: &'a RequestCtx) -> Attempt<'a> {
        Box::pin(async move {
            let m = &self.members[i];
            let started = Instant::now();
            let r = m.up.fetch(repo, ids, ctx).await;
            match &r {
                Ok(_) => {
                    self.latency.record(started.elapsed());
                    m.breaker.success();
                }
                Err(s) if is_upstream_failure(s) => {
                    tracing::warn!(upstream = m.up.name(), code = ?s.code(), error = %s.message(), "upstream fetch failed");
                    m.breaker.failure();
                }
                // 业务错误说明上游是健康的
                Err(_) => m.breaker.success(),
            }
            r
        })
    }

    /// 取下一个可用的上游。
    fn take_next(&self, order: &mut std::vec::IntoIter<usize>) -> Option<usize> {
        order.find(|i| self.members[*i].breaker.allow())
    }

    /// 查询 `ids` 中在 `repo` 里不可见的对象：依次尝试可用的上游，上游故障时换下一个（不对冲）。
    pub async fn missing(&self, repo: &str, ids: &[ObjectId], ctx: &RequestCtx) -> Result<Vec<ObjectId>, Status> {
        let mut order = self.order().into_iter();
        let mut last_err = None;
        while let Some(i) = self.take_next(&mut order) {
            let m = &self.members[i];
            match m.up.missing(repo, ids, ctx).await {
                Ok(v) => {
                    m.breaker.success();
                    return Ok(v);
                }
                Err(e) if is_upstream_failure(&e) => {
                    m.breaker.failure();
                    last_err = Some(e);
                }
                Err(e) => {
                    m.breaker.success();
                    return Err(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| Status::unavailable("all upstreams are unavailable")))
    }

    /// 调用者能否读取整个仓库：依次尝试可用的上游，上游故障时换下一个。
    pub async fn can_read(&self, repo: &str, ctx: &RequestCtx) -> Result<bool, Status> {
        let mut order = self.order().into_iter();
        let mut last_err = None;
        while let Some(i) = self.take_next(&mut order) {
            let m = &self.members[i];
            match m.up.can_read(repo, ctx).await {
                Ok(v) => {
                    m.breaker.success();
                    return Ok(v);
                }
                Err(e) if is_upstream_failure(&e) => {
                    m.breaker.failure();
                    last_err = Some(e);
                }
                Err(e) => {
                    m.breaker.success();
                    return Err(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| Status::unavailable("all upstreams are unavailable")))
    }

    /// `repo` 中 `after_seq` 之后的 redaction 变化：依次尝试可用的上游，上游故障时换下一个。
    pub async fn redactions(&self, repo: &str, after_seq: u64, ctx: &RequestCtx) -> Result<RedactionPage, Status> {
        let mut order = self.order().into_iter();
        let mut last_err = None;
        while let Some(i) = self.take_next(&mut order) {
            let m = &self.members[i];
            match m.up.redactions(repo, after_seq, ctx).await {
                Ok(v) => {
                    m.breaker.success();
                    return Ok(v);
                }
                Err(e) if is_upstream_failure(&e) => {
                    m.breaker.failure();
                    last_err = Some(e);
                }
                Err(e) => {
                    m.breaker.success();
                    return Err(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| Status::unavailable("all upstreams are unavailable")))
    }

    /// 回源一批对象。
    pub async fn fetch(&self, repo: &str, ids: &[ObjectId], ctx: &RequestCtx) -> Result<Vec<Bytes>, Status> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| Status::unavailable("upstream pool closed"))?;
        let mut order = self.order().into_iter();
        let Some(first) = self.take_next(&mut order) else {
            return Err(Status::unavailable("all upstreams are unavailable"));
        };
        let mut running = FuturesUnordered::new();
        running.push(self.attempt(first, repo, ids, ctx));
        let mut hedged = !self.config.hedge;
        let timer = tokio::time::sleep(self.latency.hedge_after(self.config.hedge_min, self.config.hedge_max));
        tokio::pin!(timer);
        let mut last_err = None;
        loop {
            tokio::select! {
                Some(r) = running.next() => match r {
                    Ok(v) => return Ok(v),
                    Err(e) if !is_upstream_failure(&e) => return Err(e),
                    Err(e) => {
                        last_err = Some(e);
                        // 没有其它在途的请求时，换下一个上游
                        if running.is_empty() {
                            match self.take_next(&mut order) {
                                Some(i) => running.push(self.attempt(i, repo, ids, ctx)),
                                None => break,
                            }
                        }
                    }
                },
                // 超过 p95 仍未返回：对冲，再发一份给另一个上游
                () = &mut timer, if !hedged => {
                    hedged = true;
                    if let Some(i) = self.take_next(&mut order) {
                        tracing::debug!(upstream = self.members[i].up.name(), "hedging slow upstream fetch");
                        running.push(self.attempt(i, repo, ids, ctx));
                    }
                }
                else => break,
            }
        }
        Err(last_err.unwrap_or_else(|| Status::unavailable("all upstreams are unavailable")))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn request_ctx_forwards_identity_trace_and_deadline() {
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let ctx = RequestCtx {
            authorization: Some(MetadataValue::from_static("Bearer t")),
            traceparent: Some(MetadataValue::from_static(tp)),
            deadline: Some(scion_obs::deadline::Deadline(
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            )),
        };
        let mut req = tonic::Request::new(());
        ctx.apply(&mut req);
        let md = req.metadata();
        assert_eq!(
            md.get("authorization").map(|v| v.to_str().expect("ascii")),
            Some("Bearer t")
        );
        assert_eq!(md.get("traceparent").map(|v| v.to_str().expect("ascii")), Some(tp));
        let timeout = md.get("grpc-timeout").expect("timeout").to_str().expect("ascii");
        let left = scion_obs::deadline::parse_grpc_timeout(timeout).expect("parse");
        assert!(left <= std::time::Duration::from_secs(5) && left > std::time::Duration::from_secs(4));
        let mut bare = tonic::Request::new(());
        RequestCtx::default().apply(&mut bare);
        assert!(bare.metadata().is_empty());
    }

    /// 测试用上游：按脚本返回结果，可设置延迟。
    pub(crate) struct Fake {
        pub name: String,
        pub delay: Duration,
        /// 每次调用依次取一个结果；用完后重复最后一个。
        pub script: Mutex<Vec<Result<(), Status>>>,
        pub calls: AtomicU32,
        pub data: std::collections::HashMap<ObjectId, Bytes>,
        /// 每个仓库可见的对象；`None` 表示所有仓库都能看到全部对象。
        pub repos: Option<std::collections::HashMap<String, Vec<ObjectId>>>,
        pub missing_calls: AtomicU32,
        /// 不允许读取的仓库。
        pub unreadable: Vec<String>,
        pub access_calls: AtomicU32,
        /// redaction 变化（下标 + 1 即 seq）。
        pub redaction_log: Mutex<Vec<RedactionChange>>,
        pub redaction_calls: AtomicU32,
        /// 设置时 `redactions` 返回这个错误。
        pub redaction_error: Mutex<Option<Status>>,
    }

    impl Fake {
        pub(crate) fn new(name: &str, objects: &[&'static [u8]]) -> Self {
            Self {
                name: name.into(),
                delay: Duration::ZERO,
                script: Mutex::new(vec![Ok(())]),
                calls: AtomicU32::new(0),
                repos: None,
                missing_calls: AtomicU32::new(0),
                unreadable: Vec::new(),
                access_calls: AtomicU32::new(0),
                redaction_log: Mutex::new(Vec::new()),
                redaction_calls: AtomicU32::new(0),
                redaction_error: Mutex::new(None),
                data: objects
                    .iter()
                    .map(|o| (ObjectId::of_blob(o), Bytes::from_static(o)))
                    .collect(),
            }
        }

        pub(crate) fn delay(mut self, d: Duration) -> Self {
            self.delay = d;
            self
        }

        pub(crate) fn script(self, s: Vec<Result<(), Status>>) -> Self {
            *self.script.lock().expect("lock") = s;
            self
        }

        pub(crate) fn calls(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }

        /// 只让 `repo` 看到 `objects`（可多次调用）。
        pub(crate) fn visible_in(mut self, repo: &str, objects: &[&[u8]]) -> Self {
            self.repos
                .get_or_insert_with(Default::default)
                .insert(repo.into(), objects.iter().map(|o| ObjectId::of_blob(o)).collect());
            self
        }

        /// 记一次 redaction 变化。
        pub(crate) fn redact(&self, object: &[u8], active: bool) {
            self.redaction_log.lock().expect("lock").push(RedactionChange {
                id: ObjectId::of_blob(object),
                redaction_id: "rd1".into(),
                active,
            });
        }

        fn sees(&self, repo: &str, id: &ObjectId) -> bool {
            self.data.contains_key(id)
                && self
                    .repos
                    .as_ref()
                    .is_none_or(|r| r.get(repo).is_some_and(|v| v.contains(id)))
        }
    }

    #[tonic::async_trait]
    impl Upstream for Fake {
        async fn can_read(&self, repo: &str, ctx: &RequestCtx) -> Result<bool, Status> {
            self.access_calls.fetch_add(1, Ordering::SeqCst);
            let step = {
                let mut s = self.script.lock().expect("lock");
                if s.len() > 1 { s.remove(0) } else { s[0].clone() }
            };
            step?;
            if ctx
                .authorization
                .as_ref()
                .is_some_and(|a| a.as_bytes() == b"Bearer invalid")
            {
                return Err(Status::unauthenticated("invalid token"));
            }
            Ok(!self.unreadable.iter().any(|r| r == repo))
        }

        async fn missing(&self, repo: &str, ids: &[ObjectId], _ctx: &RequestCtx) -> Result<Vec<ObjectId>, Status> {
            self.missing_calls.fetch_add(1, Ordering::SeqCst);
            let step = self.script.lock().expect("lock").last().cloned().unwrap_or(Ok(()));
            step?;
            Ok(ids.iter().filter(|id| !self.sees(repo, id)).copied().collect())
        }

        async fn fetch(&self, repo: &str, ids: &[ObjectId], _ctx: &RequestCtx) -> Result<Vec<Bytes>, Status> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let step = {
                let mut s = self.script.lock().expect("lock");
                if s.len() > 1 { s.remove(0) } else { s[0].clone() }
            };
            tokio::time::sleep(self.delay).await;
            step?;
            ids.iter()
                .map(|id| {
                    if self.sees(repo, id) {
                        Ok(self.data[id].clone())
                    } else {
                        Err(Status::failed_precondition(format!("missing {id}")))
                    }
                })
                .collect()
        }

        async fn redactions(&self, _repo: &str, after_seq: u64, _ctx: &RequestCtx) -> Result<RedactionPage, Status> {
            self.redaction_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(e) = self.redaction_error.lock().expect("lock").clone() {
                return Err(e);
            }
            // 每页最多 2 条，覆盖分页
            let log = self.redaction_log.lock().expect("lock");
            let start = usize::try_from(after_seq).unwrap_or(usize::MAX).min(log.len());
            let end = (start + 2).min(log.len());
            Ok(RedactionPage {
                changes: log[start..end].to_vec(),
                next_seq: end as u64,
                more: end < log.len(),
            })
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    fn pool(members: Vec<Arc<Fake>>, config: PoolConfig) -> UpstreamPool {
        UpstreamPool::new(members.into_iter().map(|m| m as Arc<dyn Upstream>).collect(), config)
    }

    fn no_hedge() -> PoolConfig {
        PoolConfig {
            hedge: false,
            ..PoolConfig::default()
        }
    }

    const OBJ: &[u8] = b"object";

    fn id() -> ObjectId {
        ObjectId::of_blob(OBJ)
    }

    #[tokio::test]
    async fn fetches_and_round_robins() {
        let (a, b) = (Arc::new(Fake::new("a", &[OBJ])), Arc::new(Fake::new("b", &[OBJ])));
        let p = pool(vec![a.clone(), b.clone()], no_hedge());
        assert_eq!((p.len(), p.is_empty()), (2, false));
        for _ in 0..4 {
            let got = p.fetch("r", &[id()], &RequestCtx::default()).await.expect("ok");
            assert_eq!(got, vec![Bytes::from_static(OBJ)]);
        }
        assert_eq!((a.calls(), b.calls()), (2, 2));
    }

    #[tokio::test]
    async fn business_errors_return_without_failover() {
        let (a, b) = (Arc::new(Fake::new("a", &[])), Arc::new(Fake::new("b", &[OBJ])));
        let p = pool(vec![a.clone(), b.clone()], no_hedge());
        let err = p
            .fetch("r", &[id()], &RequestCtx::default())
            .await
            .expect_err("missing");
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert_eq!(a.calls() + b.calls(), 1);
    }

    #[tokio::test]
    async fn failures_fail_over_and_trip_the_breaker() {
        let down = Arc::new(Fake::new("down", &[OBJ]).script(vec![Err(Status::unavailable("down"))]));
        let up = Arc::new(Fake::new("up", &[OBJ]));
        let p = pool(
            vec![down.clone(), up.clone()],
            PoolConfig {
                breaker_threshold: 2,
                breaker_cooldown: Duration::from_secs(60),
                ..no_hedge()
            },
        );
        for _ in 0..6 {
            p.fetch("r", &[id()], &RequestCtx::default())
                .await
                .expect("served by up");
        }
        assert_eq!(down.calls(), 2, "breaker opened after two failures");
        assert_eq!(up.calls(), 6);
        // 全部上游都坏了：返回最后的错误
        let p = pool(vec![down.clone()], no_hedge());
        assert_eq!(
            p.fetch("r", &[id()], &RequestCtx::default())
                .await
                .map_err(|e| e.code()),
            Err(Code::Unavailable)
        );
        let empty = pool(vec![], no_hedge());
        assert!(empty.is_empty());
        assert_eq!(
            empty
                .fetch("r", &[id()], &RequestCtx::default())
                .await
                .map_err(|e| e.code()),
            Err(Code::Unavailable)
        );
    }

    #[tokio::test]
    async fn slow_primary_is_hedged() {
        let slow = Arc::new(Fake::new("slow", &[OBJ]).delay(Duration::from_secs(5)));
        let fast = Arc::new(Fake::new("fast", &[OBJ]).delay(Duration::from_millis(1)));
        let cfg = PoolConfig {
            hedge_min: Duration::from_millis(10),
            hedge_max: Duration::from_millis(20),
            ..PoolConfig::default()
        };
        let p = pool(vec![slow.clone(), fast.clone()], cfg);
        let started = Instant::now();
        for _ in 0..2 {
            p.fetch("r", &[id()], &RequestCtx::default()).await.expect("ok");
        }
        assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
        assert_eq!(slow.calls(), 1, "slow was primary once");
        // 只有一个上游时对冲没有对象可发，等主请求完成
        let only = pool(
            vec![Arc::new(Fake::new("only", &[OBJ]).delay(Duration::from_millis(30)))],
            cfg,
        );
        only.fetch("r", &[id()], &RequestCtx::default()).await.expect("ok");
    }

    #[tokio::test]
    async fn hedge_failure_waits_for_primary() {
        let slow = Arc::new(Fake::new("slow", &[OBJ]).delay(Duration::from_millis(60)));
        let broken = Arc::new(Fake::new("broken", &[OBJ]).script(vec![Err(Status::internal("boom"))]));
        let cfg = PoolConfig {
            hedge_min: Duration::from_millis(5),
            hedge_max: Duration::from_millis(5),
            breaker_threshold: 100,
            ..PoolConfig::default()
        };
        let p = pool(vec![slow.clone(), broken.clone()], cfg);
        // 第一次 slow 是主请求，对冲给 broken 失败，最终等到 slow 的结果
        p.fetch("r", &[id()], &RequestCtx::default())
            .await
            .expect("primary wins");
        assert_eq!((slow.calls(), broken.calls()), (1, 1));
    }

    #[tokio::test]
    async fn missing_fails_over_and_passes_business_errors() {
        let down = Arc::new(Fake::new("down", &[OBJ]).script(vec![Err(Status::unavailable("down"))]));
        let up = Arc::new(Fake::new("up", &[OBJ]).visible_in("r", &[OBJ]));
        let p = pool(vec![down.clone(), up.clone()], no_hedge());
        let other = ObjectId::of_blob(b"other");
        for _ in 0..2 {
            assert_eq!(
                p.missing("r", &[id(), other], &RequestCtx::default())
                    .await
                    .expect("ok"),
                [other]
            );
        }
        assert_eq!(
            p.missing("x", &[id()], &RequestCtx::default()).await.expect("ok"),
            [id()]
        );
        let gone = Arc::new(Fake::new("gone", &[]).script(vec![Err(Status::not_found("repo"))]));
        let p = pool(vec![gone], no_hedge());
        assert_eq!(
            p.missing("r", &[id()], &RequestCtx::default())
                .await
                .map_err(|e| e.code()),
            Err(Code::NotFound)
        );
        let p = pool(vec![down], no_hedge());
        assert_eq!(
            p.missing("r", &[id()], &RequestCtx::default())
                .await
                .map_err(|e| e.code()),
            Err(Code::Unavailable)
        );
    }

    #[tokio::test]
    async fn can_read_fails_over_and_passes_business_errors() {
        let down = Arc::new(Fake::new("down", &[]).script(vec![Err(Status::unavailable("down"))]));
        let mut f = Fake::new("up", &[]);
        f.unreadable = vec!["secret".into()];
        let up = Arc::new(f);
        let p = pool(vec![down.clone(), up], no_hedge());
        for _ in 0..2 {
            assert!(p.can_read("open", &RequestCtx::default()).await.expect("ok"));
        }
        assert!(!p.can_read("secret", &RequestCtx::default()).await.expect("ok"));
        let invalid = RequestCtx {
            authorization: Some(MetadataValue::from_static("Bearer invalid")),
            ..RequestCtx::default()
        };
        assert_eq!(
            p.can_read("open", &invalid).await.map_err(|e| e.code()),
            Err(Code::Unauthenticated)
        );
        let only_down = pool(vec![down], no_hedge());
        assert_eq!(
            only_down
                .can_read("x", &RequestCtx::default())
                .await
                .map_err(|e| e.code()),
            Err(Code::Unavailable)
        );
    }

    #[test]
    fn upstream_failure_classification() {
        for c in [
            Code::Unavailable,
            Code::DeadlineExceeded,
            Code::DataLoss,
            Code::Internal,
        ] {
            assert!(is_upstream_failure(&Status::new(c, "")), "{c:?}");
        }
        for c in [
            Code::NotFound,
            Code::FailedPrecondition,
            Code::PermissionDenied,
            Code::InvalidArgument,
        ] {
            assert!(!is_upstream_failure(&Status::new(c, "")), "{c:?}");
        }
    }
}
