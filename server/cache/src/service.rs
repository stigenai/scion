//! 缓存节点对外提供的 gRPC 服务：`ObjectService`（与 L3 的消息定义完全相同）和 `ServerInfoService`
//! （角色为 CACHE）。

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use scion_object::ObjectId;
use scion_obs::streams::StreamLimits;
use scion_proto::v1::object_service_client::ObjectServiceClient;
use scion_proto::v1::object_service_server::ObjectService;
use scion_proto::v1::server_info_service_server::ServerInfoService;
use scion_proto::v1::{
    self, Encoding, GetObjectsRequest, GetObjectsResponse, GetServerInfoRequest, GetServerInfoResponse,
    HasObjectsRequest, HasObjectsResponse, ListRedactedObjectsRequest, ListRedactedObjectsResponse,
};
use tokio_stream::Stream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::node::CacheNode;
use crate::upstream::{CACHE_FORWARDED_HEADER, RequestCtx};

pub type GetObjectsStream = Pin<Box<dyn Stream<Item = Result<GetObjectsResponse, Status>> + Send>>;

/// 把一个对象从 `start` 开始切成若干分块。`start == len` 时返回一个空的结尾块。
fn chunk_object(id: &ObjectId, data: Bytes, start: usize, chunk: usize) -> Vec<GetObjectsResponse> {
    let total = data.len();
    let id_bytes = Bytes::copy_from_slice(&id.to_bytes());
    let mut out = Vec::with_capacity((total - start).div_ceil(chunk).max(1));
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

fn parse_id(raw: &[u8]) -> Result<ObjectId, Status> {
    ObjectId::from_bytes(raw).map_err(|e| Status::invalid_argument(format!("invalid object id: {e}")))
}

fn ctx_of<T>(req: &Request<T>) -> (RequestCtx, bool) {
    let ctx = RequestCtx {
        authorization: req.metadata().get("authorization").cloned(),
        traceparent: req.metadata().get("traceparent").cloned(),
        deadline: req.extensions().get::<scion_obs::deadline::Deadline>().copied(),
    };
    (ctx, req.metadata().contains_key(CACHE_FORWARDED_HEADER))
}

/// The stream budget key of the caller of `request`. The cache has no auth layer and does not know
/// the subject, so the key is a hash of the bearer token, else the peer address. The bearer is
/// not in the key: the refusal message shows the key to the caller.
fn caller_key<T>(request: &Request<T>) -> String {
    match bearer_token(request) {
        Some(t) => format!("tok:{}", &blake3::hash(t.as_bytes()).to_hex()[..32]),
        None => match request.remote_addr() {
            Some(a) => format!("ip:{}", a.ip()),
            None => "anonymous".to_owned(),
        },
    }
}

/// The bearer token as L3 reads it (`bearer_value` in server/api/src/auth.rs): the scheme matches
/// in any case and the token is trimmed. scion-cache cannot depend on scion-api (xtask layer
/// rule), so this is a copy: keep the two the same. A header that is not a Bearer one is
/// anonymous at L3, so it has no token here. An empty token also gives none (L3 gives `Some("")`).
fn bearer_token<T>(request: &Request<T>) -> Option<&str> {
    let (scheme, token) = request
        .metadata()
        .get("authorization")?
        .to_str()
        .ok()?
        .split_once(' ')?;
    let token = token.trim();
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
}

/// The same refusal as L3: RESOURCE_EXHAUSTED with reason RATE_LIMITED, retry after 1s.
fn too_many_streams(key: &str) -> Status {
    let details = tonic_types::ErrorDetails::with_error_info(
        scion_proto::reason_name(v1::ErrorReason::RateLimited),
        scion_proto::ERROR_DOMAIN,
        HashMap::from([("retry_after_ms".to_string(), "1000".to_string())]),
    );
    <Status as tonic_types::StatusExt>::with_error_details(
        tonic::Code::ResourceExhausted,
        format!("too many open streams for {key}"),
        details,
    )
}

pub struct CacheObjectSvc {
    node: Arc<CacheNode>,
    limits: v1::Limits,
    /// HasObjects 不缓存（写路径用），直接转给上游。
    upstreams: Vec<Channel>,
    next: AtomicUsize,
    streams: StreamLimits,
}

impl CacheObjectSvc {
    pub fn new(node: Arc<CacheNode>, limits: v1::Limits, upstreams: Vec<Channel>) -> Self {
        Self {
            node,
            limits,
            upstreams,
            next: AtomicUsize::new(0),
            streams: StreamLimits::default(),
        }
    }

    /// Sets the lifetime and the per-caller budget of GetObjects streams. The default sets none.
    pub fn with_stream_limits(mut self, streams: StreamLimits) -> Self {
        self.streams = streams;
        self
    }

    fn check_batch(&self, n: usize) -> Result<(), Status> {
        let max = self.limits.max_batch_objects as usize;
        if max > 0 && n > max {
            return Err(Status::invalid_argument(format!(
                "{n} objects requested; at most {max} per request (max_batch_objects)"
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl ObjectService for CacheObjectSvc {
    type GetObjectsStream = GetObjectsStream;

    async fn get_objects(&self, request: Request<GetObjectsRequest>) -> Result<Response<GetObjectsStream>, Status> {
        let (ctx, forwarded) = ctx_of(&request);
        let key = caller_key(&request);
        let req = request.into_inner();
        self.check_batch(req.wants.len())?;
        if !req.accept_encodings.is_empty() && !req.accept_encodings.contains(&(Encoding::Identity as i32)) {
            return Err(Status::failed_precondition(
                "none of accept_encodings is supported; IDENTITY is always supported",
            ));
        }
        let ids = req
            .wants
            .iter()
            .map(|w| parse_id(&w.id))
            .collect::<Result<Vec<_>, _>>()?;
        let objects = self.node.get_many(&req.repo, &ids, &ctx, forwarded).await?;
        for ((id, data), want) in ids.iter().zip(&objects).zip(&req.wants) {
            if want.offset > data.len() as u64 {
                return Err(Status::out_of_range(format!(
                    "offset {} beyond size {} of {id}",
                    want.offset,
                    data.len()
                )));
            }
        }
        // After the L3 check and the offset check: a refused call takes no slot.
        let ticket = self.streams.open_with(&key, too_many_streams)?;
        let chunk = usize::try_from(self.limits.max_chunk_bytes)
            .unwrap_or(usize::MAX)
            .max(1);
        let parts: Vec<_> = ids
            .iter()
            .zip(objects)
            .zip(&req.wants)
            .flat_map(|((id, data), want)| chunk_object(id, data, want.offset as usize, chunk))
            .map(Ok)
            .collect();
        Ok(Response::new(Box::pin(
            self.streams.bound(ticket, tokio_stream::iter(parts)),
        )))
    }

    async fn has_objects(&self, request: Request<HasObjectsRequest>) -> Result<Response<HasObjectsResponse>, Status> {
        let (channel, fwd) = self.forward(request)?;
        let resp = ObjectServiceClient::new(channel).has_objects(fwd).await?;
        Ok(Response::new(resp.into_inner()))
    }

    /// 不缓存，直接转给上游（客户端经由缓存同步 redaction 时使用）。
    async fn list_redacted_objects(
        &self,
        request: Request<ListRedactedObjectsRequest>,
    ) -> Result<Response<ListRedactedObjectsResponse>, Status> {
        let (channel, fwd) = self.forward(request)?;
        let resp = ObjectServiceClient::new(channel).list_redacted_objects(fwd).await?;
        Ok(Response::new(resp.into_inner()))
    }
}

impl CacheObjectSvc {
    /// 选一个上游，把请求连同调用者的 `authorization`、trace 上下文与 deadline 转过去。
    fn forward<T>(&self, request: Request<T>) -> Result<(Channel, Request<T>), Status> {
        if self.upstreams.is_empty() {
            return Err(Status::unavailable("no upstream configured"));
        }
        let (ctx, _) = ctx_of(&request);
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.upstreams.len();
        let mut fwd = Request::new(request.into_inner());
        ctx.apply(&mut fwd);
        Ok((self.upstreams[i].clone(), fwd))
    }
}

pub struct CacheInfoSvc {
    info: v1::ServerInfo,
}

impl CacheInfoSvc {
    /// 由上游的 ServerInfo 派生：限额相同，角色为 CACHE，只声明对象读取相关的能力。
    pub fn from_upstream(upstream: &v1::ServerInfo) -> Self {
        let keep = [
            scion_proto::features::OBJECTS_GET,
            scion_proto::features::OBJECTS_HAS,
            scion_proto::features::REDACTION,
        ];
        let info = v1::ServerInfo {
            protocol_version: scion_proto::PROTOCOL_VERSION.to_string(),
            features: upstream
                .features
                .iter()
                .filter(|f| keep.contains(&f.as_str()))
                .cloned()
                .collect(),
            limits: upstream.limits,
            read_endpoints: Vec::new(),
            hash_algo: upstream.hash_algo,
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            role: v1::ServerRole::Cache.into(),
        };
        Self { info }
    }

    pub fn info(&self) -> &v1::ServerInfo {
        &self.info
    }
}

#[tonic::async_trait]
impl ServerInfoService for CacheInfoSvc {
    async fn get_server_info(
        &self,
        _request: Request<GetServerInfoRequest>,
    ) -> Result<Response<GetServerInfoResponse>, Status> {
        Ok(Response::new(GetServerInfoResponse {
            info: Some(self.info.clone()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring::Ring;
    use crate::store::{ObjectCache, StorageConfig};
    use crate::upstream::tests::Fake;
    use crate::upstream::{PoolConfig, Upstream, UpstreamPool};
    use scion_proto::v1::ObjectWant;
    use std::collections::HashMap;
    use tokio_stream::StreamExt;
    use tonic::Code;

    const BIG: &[u8] = b"0123456789";

    async fn svc(limits: v1::Limits) -> CacheObjectSvc {
        let store = ObjectCache::open(&StorageConfig::default()).await.expect("cache");
        let up = Arc::new(Fake::new("l3", &[BIG]));
        let pool = UpstreamPool::new(vec![up as Arc<dyn Upstream>], PoolConfig::default());
        let node = Arc::new(CacheNode::new(store, Ring::new(Vec::new()), None, HashMap::new(), pool));
        CacheObjectSvc::new(node, limits, Vec::new())
    }

    fn limits(batch: u32, chunk: u64) -> v1::Limits {
        v1::Limits {
            max_batch_objects: batch,
            max_chunk_bytes: chunk,
            ..Default::default()
        }
    }

    fn want(data: &[u8], offset: u64) -> ObjectWant {
        ObjectWant {
            id: Bytes::copy_from_slice(&ObjectId::of_blob(data).to_bytes()),
            offset,
        }
    }

    async fn get(s: &CacheObjectSvc, req: GetObjectsRequest) -> Result<Vec<GetObjectsResponse>, Status> {
        s.get_objects(Request::new(req)).await?.into_inner().collect().await
    }

    #[tokio::test]
    async fn get_objects_chunks_and_resumes() {
        let s = svc(limits(10, 4)).await;
        let parts = get(
            &s,
            GetObjectsRequest {
                repo: "r".into(),
                wants: vec![want(BIG, 0), want(BIG, 8)],
                accept_encodings: vec![],
            },
        )
        .await
        .expect("ok");
        let offsets: Vec<_> = parts.iter().map(|p| (p.offset, p.last)).collect();
        assert_eq!(offsets, [(0, false), (4, false), (8, true), (8, true)]);
        let whole: Vec<u8> = parts[..3].iter().flat_map(|p| p.data.to_vec()).collect();
        assert_eq!(whole, BIG);
        // 从末尾续传：一个空的结尾块
        let end = get(
            &s,
            GetObjectsRequest {
                repo: "r".into(),
                wants: vec![want(BIG, 10)],
                accept_encodings: vec![Encoding::Identity as i32],
            },
        )
        .await
        .expect("ok");
        assert_eq!((end.len(), end[0].data.len(), end[0].last), (1, 0, true));
    }

    #[tokio::test]
    async fn get_objects_rejections() {
        let s = svc(limits(1, 4)).await;
        let code = |r: Result<Vec<GetObjectsResponse>, Status>| r.err().map(|e| e.code());
        let req = |wants, enc| GetObjectsRequest {
            repo: "r".into(),
            wants,
            accept_encodings: enc,
        };
        assert_eq!(
            code(get(&s, req(vec![want(BIG, 0), want(BIG, 0)], vec![])).await),
            Some(Code::InvalidArgument),
            "batch limit"
        );
        assert_eq!(
            code(get(&s, req(vec![want(BIG, 0)], vec![Encoding::Zstd as i32])).await),
            Some(Code::FailedPrecondition)
        );
        let bad = ObjectWant {
            id: Bytes::from_static(b"x"),
            offset: 0,
        };
        assert_eq!(code(get(&s, req(vec![bad], vec![])).await), Some(Code::InvalidArgument));
        assert_eq!(
            code(get(&s, req(vec![want(BIG, 11)], vec![])).await),
            Some(Code::OutOfRange)
        );
        assert_eq!(
            code(get(&s, req(vec![want(b"missing", 0)], vec![])).await),
            Some(Code::FailedPrecondition),
            "upstream error passed through"
        );
        let unlimited = svc(limits(0, 0)).await;
        assert!(
            get(&unlimited, req(vec![want(BIG, 0), want(BIG, 0)], vec![]))
                .await
                .is_ok()
        );
        let none = unlimited
            .has_objects(Request::new(HasObjectsRequest::default()))
            .await
            .map(|_| ());
        assert_eq!(none.map_err(|e| e.code()), Err(Code::Unavailable));
    }

    #[test]
    fn a_header_that_is_not_a_bearer_one_has_no_token() {
        let token = |h: &str| {
            let mut r = Request::new(());
            r.metadata_mut().insert("authorization", h.parse().expect("header"));
            bearer_token(&r).map(str::to_owned)
        };
        assert_eq!(token("Bearer  t "), Some("t".into()));
        assert_eq!(token("BEARER t"), Some("t".into()));
        assert_eq!(token("Basic x"), None);
        assert_eq!(token("junk"), None);
        assert_eq!(token("Bearer "), None);
        assert_eq!(token("Bearer    "), None);
        assert_eq!(bearer_token(&Request::new(())), None);
    }

    #[test]
    fn info_keeps_object_features_and_limits() {
        let up = v1::ServerInfo {
            protocol_version: "1.0".into(),
            features: vec![
                scion_proto::features::OBJECTS_GET.into(),
                scion_proto::features::LAND_DIRECT.into(),
                scion_proto::features::OBJECTS_HAS.into(),
            ],
            limits: Some(limits(7, 9)),
            hash_algo: v1::HashAlgo::Blake3256.into(),
            role: v1::ServerRole::Server.into(),
            ..Default::default()
        };
        let i = CacheInfoSvc::from_upstream(&up);
        assert_eq!(i.info().features, ["objects.get", "objects.has"]);
        assert_eq!(i.info().role, v1::ServerRole::Cache as i32);
        assert_eq!(i.info().limits.map(|l| l.max_batch_objects), Some(7));
    }
}
