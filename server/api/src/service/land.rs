//! LandService：把 ref 移到指定提交，落后时在服务端 rebase（实现在 `scion-land`）。
//!
//! 集群模式下每个 ref 由持有租约的节点（leader）处理：本节点不是 leader 时把请求原样转给它，
//! 转发的请求带上 [`FORWARDED_HEADER`]，收到的节点不会再转发（避免环路），而是返回可重试的错误。
//!
//! 这个头是普通的 gRPC metadata，调用方也能带上。值为集群共享密钥（`--cluster-secret`）时才认定是
//! 本集群节点转发的（[`trusted_forward`]），此时按仓库和按租户的限流已在第一个节点计过，不再计；
//! 没有配置密钥时一律照常计数（集群内转发的合入会被计两次）。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use scion_land::{Coordinator, LandError, LandOutcome, LandRequest as Request_, Landed};
use scion_object::ObjectId;
use scion_proto::v1::land_service_client::LandServiceClient;
use scion_proto::v1::land_service_server::LandService;
use scion_proto::v1::{
    GetLandStatusRequest, GetLandStatusResponse, LandRequest, LandResponse, LandState, LandedChangeset,
};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use crate::auth::{authorize, authorize_any, principal};
use crate::limit::RateLimiter;
use crate::service::support::{check_repo, parse_id};
use crate::status;
use crate::watch::RefHub;
use scion_auth::{Action, Guard, Request as AuthRequest};
use scion_store::RefState;

/// 节点间转发的请求带上的元数据。
pub const FORWARDED_HEADER: &str = "x-scion-forwarded";

/// 请求是否由本集群的节点转发：[`FORWARDED_HEADER`] 的值等于集群共享密钥（常数时间比较）。
/// 没有配置密钥时总是 false。
pub fn trusted_forward(value: Option<&[u8]>, secret: Option<&str>) -> bool {
    match (value, secret) {
        (Some(v), Some(s)) if !s.is_empty() && v.len() == s.len() => {
            v.iter().zip(s.as_bytes()).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
        }
        _ => false,
    }
}

pub struct LandSvc {
    coordinator: Coordinator,
    /// 到其它节点的连接（按地址缓存）。
    peers: Mutex<HashMap<String, Channel>>,
    limiter: Option<Arc<RateLimiter>>,
    hub: Option<Arc<RefHub>>,
    guard: Arc<Guard>,
    /// 集群共享密钥：转发时作为 [`FORWARDED_HEADER`] 的值，收到时据此认定是本集群转发的。
    cluster_secret: Option<Arc<str>>,
    /// 合入前检查（外部 webhook）。
    pre_land: Option<Arc<crate::prehook::PreLand>>,
}

impl LandSvc {
    pub fn new(coordinator: Coordinator) -> Self {
        Self {
            coordinator,
            peers: Mutex::new(HashMap::new()),
            limiter: None,
            hub: None,
            guard: Guard::disabled(),
            cluster_secret: None,
            pre_land: None,
        }
    }

    /// 合入之前调用外部检查；本集群转发来的请求已在第一个节点检查过。
    pub fn with_pre_land(mut self, hook: Option<Arc<crate::prehook::PreLand>>) -> Self {
        self.pre_land = hook;
        self
    }

    pub fn with_cluster_secret(mut self, secret: Option<Arc<str>>) -> Self {
        self.cluster_secret = secret.filter(|s| !s.is_empty());
        self
    }

    pub fn with_guard(mut self, guard: Arc<Guard>) -> Self {
        self.guard = guard;
        self
    }

    /// 合入成功后通知 WatchRefs 的订阅者。
    pub fn with_ref_hub(mut self, hub: Arc<RefHub>) -> Self {
        self.hub = Some(hub);
        self
    }

    async fn notify(&self, repo: &str, reference: &str, state: RefState) {
        if let Some(h) = &self.hub {
            h.publish(repo, reference, state).await;
        }
    }

    /// 按仓库限制合入速率。本集群转发来的请求（[`trusted_forward`]）已在第一个节点计过数，不再检查。
    pub fn with_rate_limit(mut self, limiter: Arc<RateLimiter>) -> Self {
        self.limiter = Some(limiter);
        self
    }

    fn peer(&self, addr: &str) -> Result<Channel, Status> {
        let mut peers = self.peers.lock().map_err(|_| Status::internal("peer cache poisoned"))?;
        if let Some(c) = peers.get(addr) {
            return Ok(c.clone());
        }
        let channel = Endpoint::from_shared(addr.to_owned())
            .map_err(|e| Status::internal(format!("invalid peer address {addr:?}: {e}")))?
            .connect_timeout(std::time::Duration::from_secs(2))
            .connect_lazy();
        peers.insert(addr.to_owned(), channel.clone());
        Ok(channel)
    }

    /// 把请求转给 leader，原样返回它的结果。
    async fn forward(
        &self,
        leader: &str,
        req: LandRequest,
        auth: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
        deadline: Option<scion_obs::deadline::Deadline>,
    ) -> Result<Response<LandResponse>, Status> {
        let mut fwd = Request::new(req);
        let marker = self
            .cluster_secret
            .as_deref()
            .and_then(|s| tonic::metadata::MetadataValue::try_from(s).ok())
            .unwrap_or_else(|| tonic::metadata::MetadataValue::from_static("1"));
        fwd.metadata_mut().insert(FORWARDED_HEADER, marker);
        if let Some(a) = auth {
            fwd.metadata_mut().insert("authorization", a);
        }
        // leader 接续同一个 trace，并沿用本请求剩余的 deadline
        scion_obs::trace::inject(fwd.metadata_mut());
        if let Some(d) = deadline {
            fwd.set_timeout(d.remaining());
        }
        tracing::debug!(leader, "forwarding land to the ref's leader");
        let resp = LandServiceClient::new(self.peer(leader)?).land(fwd).await?;
        Ok(Response::new(resp.into_inner()))
    }

    #[cfg(test)]
    pub(crate) async fn land(
        &self,
        repo: &str,
        reference: &str,
        tip: ObjectId,
        fast_forward_only: bool,
    ) -> Result<LandOutcome, Status> {
        self.land_with_id(repo, reference, tip, fast_forward_only, None, "")
            .await
            .map_err(|e| to_status(reference, e))
    }

    async fn land_with_id(
        &self,
        repo: &str,
        reference: &str,
        tip: ObjectId,
        fast_forward_only: bool,
        request_id: Option<&str>,
        actor: &str,
    ) -> Result<LandOutcome, LandError> {
        let out = self
            .coordinator
            .land(Request_ {
                repo,
                reference,
                tip,
                fast_forward_only,
                request_id,
                actor,
            })
            .await?;
        self.notify(repo, reference, out.state).await;
        Ok(out)
    }
}

fn to_status(reference: &str, e: LandError) -> Status {
    match e {
        LandError::NotFastForward { tip } => status::not_fast_forward(reference, &tip),
        e => status::from_land(e),
    }
}

fn id_bytes(id: &ObjectId) -> Bytes {
    Bytes::copy_from_slice(&id.to_bytes())
}

fn to_proto(list: &[Landed]) -> Vec<LandedChangeset> {
    list.iter()
        .map(|l| LandedChangeset {
            changeset_id: id_bytes(&l.original),
            landed_id: id_bytes(&l.landed),
            change_id: Bytes::copy_from_slice(&l.change_id),
        })
        .collect()
}

fn response(out: &LandOutcome) -> LandResponse {
    LandResponse {
        tip: id_bytes(&out.state.target),
        ref_version: out.state.version,
        landed: to_proto(&out.landed),
        previously_landed: to_proto(&out.previously_landed),
    }
}

#[tonic::async_trait]
impl LandService for LandSvc {
    async fn land(&self, request: Request<LandRequest>) -> Result<Response<LandResponse>, Status> {
        // 带了转发头就不再转发（避免环路）；只有密钥正确时才跳过限流
        let forwarded = request.metadata().contains_key(FORWARDED_HEADER);
        let trusted = trusted_forward(
            request.metadata().get(FORWARDED_HEADER).map(|v| v.as_bytes()),
            self.cluster_secret.as_deref(),
        );
        let auth = request.metadata().get("authorization").cloned();
        let deadline = request.extensions().get::<scion_obs::deadline::Deadline>().copied();
        let p = principal(&request);
        let req = request.into_inner();
        authorize(
            &self.guard,
            &p,
            AuthRequest::repo(&req.repo, Action::Land).with_ref(&req.r#ref),
        )
        .await?;
        let cs_id = parse_id(&req.changeset_id)?;
        // A forecast: any node can make it, and nothing is written
        if req.dry_run {
            let out = self
                .coordinator
                .check(&req.repo, &req.r#ref, cs_id, req.fast_forward_only)
                .await
                .map_err(|e| to_status(&req.r#ref, e))?;
            return Ok(Response::new(response(&out)));
        }
        if !trusted {
            RateLimiter::admit(self.limiter.as_deref(), &req.repo)?;
            if let Some(hook) = &self.pre_land {
                hook.check(&p, &req.repo, &req.r#ref, cs_id).await?;
            }
        }
        let request_id = Some(req.request_id.as_str()).filter(|id| !id.is_empty());
        let out = match self
            .land_with_id(
                &req.repo,
                &req.r#ref,
                cs_id,
                req.fast_forward_only,
                request_id,
                &p.subject,
            )
            .await
        {
            Ok(out) => out,
            // 别的节点是 leader：转给它（转发来的请求不再转发，由客户端重试）
            Err(LandError::NotLeader { leader }) if !forwarded => {
                let resp = self.forward(&leader, req.clone(), auth, deadline).await?;
                if let Ok(target) = ObjectId::from_bytes(&resp.get_ref().tip) {
                    let state = RefState {
                        target,
                        version: resp.get_ref().ref_version,
                    };
                    self.notify(&req.repo, &req.r#ref, state).await;
                }
                return Ok(resp);
            }
            Err(e) => return Err(to_status(&req.r#ref, e)),
        };
        let rebased = out.landed.iter().filter(|l| l.original != l.landed).count();
        tracing::info!(repo = %req.repo, r#ref = %req.r#ref, tip = %out.state.target, version = out.state.version,
            landed = out.landed.len(), rebased, previously_landed = out.previously_landed.len(),
            request_id = %req.request_id, "landed");
        Ok(Response::new(response(&out)))
    }

    async fn get_land_status(
        &self,
        request: Request<GetLandStatusRequest>,
    ) -> Result<Response<GetLandStatusResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        if req.request_id.is_empty() {
            return Err(Status::invalid_argument("request_id is required"));
        }
        authorize_any(&self.guard, &p, &req.repo, Action::Read).await?;
        check_repo(self.coordinator.writer().meta().as_ref(), &req.repo).await?;
        let found = self
            .coordinator
            .status(&req.repo, &req.request_id)
            .await
            .map_err(status::from_land)?;
        Ok(Response::new(match found {
            Some(out) => GetLandStatusResponse {
                state: LandState::Landed as i32,
                result: Some(response(&out)),
            },
            None => GetLandStatusResponse {
                state: LandState::Unknown as i32,
                result: None,
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::write::WriteSvc;
    use crate::service::write::spec_to_changeset;
    use crate::service::write::tests::{REPO, put, spec, svc, upload};
    use tonic::Code;
    use tonic_types::StatusExt;

    fn land_svc(w: &WriteSvc) -> LandSvc {
        LandSvc::new(Coordinator::new(w.writer().clone(), Default::default()))
    }

    fn reason(s: &Status) -> (Code, String, std::collections::HashMap<String, String>) {
        let d = s.get_error_details();
        let info = d
            .error_info()
            .map(|i| (i.reason.clone(), i.metadata.clone()))
            .unwrap_or_default();
        (s.code(), info.0, info.1)
    }

    async fn commit(w: &WriteSvc, parents: &[ObjectId], files: &[(&str, &[u8])], msg: &str) -> ObjectId {
        let mut delta = Vec::new();
        for (path, data) in files {
            upload(w, data).await;
            delta.push(put(path, data));
        }
        let mut s = spec(parents, delta);
        s.message = msg.into();
        // 每个提交一个独立的 change id（同一 ref 上同一 change 只合入一次）
        s.change_id = Bytes::copy_from_slice(&ObjectId::of_blob(msg.as_bytes()).to_bytes()[2..18]);
        w.create(REPO, &spec_to_changeset(s, 10).expect("s"))
            .await
            .expect("create")
            .id
    }

    #[tokio::test]
    async fn grpc_land_reports_landed_changesets() {
        let (_store, w) = svc().await;
        let land = land_svc(&w);
        let a = commit(&w, &[], &[("f", b"1")], "a").await;
        let side = commit(&w, &[a], &[("g", b"1")], "side").await;
        let b = commit(&w, &[a], &[("f", b"2")], "b").await;
        let call = |id: ObjectId| LandRequest {
            repo: REPO.into(),
            request_id: String::new(),
            r#ref: "main".into(),
            changeset_id: id_bytes(&id),
            fast_forward_only: false,
            dry_run: false,
        };
        let r = LandService::land(&land, Request::new(call(a)))
            .await
            .expect("create")
            .into_inner();
        assert_eq!((r.ref_version, r.landed.len()), (1, 1));
        assert_eq!(r.landed[0].changeset_id, r.landed[0].landed_id);
        let r = LandService::land(&land, Request::new(call(b)))
            .await
            .expect("ff")
            .into_inner();
        assert_eq!((r.tip.clone(), r.ref_version), (id_bytes(&b), 2));
        // 落后的提交：服务端 rebase，返回新旧 id
        let r = LandService::land(&land, Request::new(call(side)))
            .await
            .expect("rebase")
            .into_inner();
        assert_eq!(r.ref_version, 3);
        assert_eq!(r.landed[0].changeset_id, id_bytes(&side));
        assert_ne!(r.landed[0].landed_id, id_bytes(&side));
        assert_eq!(r.landed[0].landed_id, r.tip);
        assert_eq!(r.landed[0].change_id.len(), 16);
        // 已在历史中：返回当前状态，不再合入
        let r = LandService::land(&land, Request::new(call(b)))
            .await
            .expect("noop")
            .into_inner();
        assert_eq!((r.ref_version, r.landed.len()), (3, 0));
        // 带 request id：重试返回第一次的结果；GetLandStatus 可以查到
        let d = commit(&w, &[a], &[("h", b"1")], "d").await;
        let with_id = LandRequest {
            request_id: "req-d".into(),
            ..call(d)
        };
        let first = LandService::land(&land, Request::new(with_id.clone()))
            .await
            .expect("d")
            .into_inner();
        let retry = LandService::land(&land, Request::new(with_id.clone()))
            .await
            .expect("retry")
            .into_inner();
        assert_eq!(retry, first);
        let status_of = |id: &str| GetLandStatusRequest {
            repo: REPO.into(),
            request_id: id.into(),
        };
        let st = land
            .get_land_status(Request::new(status_of("req-d")))
            .await
            .expect("status")
            .into_inner();
        assert_eq!((st.state, st.result), (LandState::Landed as i32, Some(first)));
        let st = land
            .get_land_status(Request::new(status_of("nope")))
            .await
            .expect("status")
            .into_inner();
        assert_eq!((st.state, st.result), (LandState::Unknown as i32, None));
        let code = |r: Result<Response<GetLandStatusResponse>, Status>| r.err().map(|s| s.code());
        assert_eq!(
            code(land.get_land_status(Request::new(status_of(""))).await),
            Some(Code::InvalidArgument)
        );
        let other_repo = GetLandStatusRequest {
            repo: "missing".into(),
            request_id: "x".into(),
        };
        assert_eq!(
            code(land.get_land_status(Request::new(other_repo)).await),
            Some(Code::NotFound)
        );
        // 同一个 id 用于不同的请求
        let reused = LandRequest {
            request_id: "req-d".into(),
            ..call(a)
        };
        assert_eq!(
            LandService::land(&land, Request::new(reused))
                .await
                .err()
                .map(|s| s.code()),
            Some(Code::InvalidArgument)
        );

        let mut bad = call(a);
        bad.changeset_id = Bytes::from_static(b"x");
        assert_eq!(
            LandService::land(&land, Request::new(bad))
                .await
                .err()
                .map(|s| s.code()),
            Some(Code::InvalidArgument)
        );
    }

    #[tokio::test]
    async fn conflicts_and_fast_forward_only_are_reported_with_details() {
        let (_store, w) = svc().await;
        let land = land_svc(&w);
        let a = commit(&w, &[], &[("f", b"1\n")], "a").await;
        let b = commit(&w, &[a], &[("f", b"theirs\n")], "b").await;
        let c = commit(&w, &[a], &[("f", b"ours\n")], "c").await;
        land.land(REPO, "main", b, false).await.expect("b");

        let (code, why, md) = reason(&land.land(REPO, "main", c, false).await.expect_err("conflict"));
        assert_eq!((code, why.as_str()), (Code::Aborted, "MERGE_CONFLICT"));
        assert_eq!(md["changeset"], c.to_string());
        assert_eq!(
            (md["count"].as_str(), md["path.0"].as_str(), md["kind.0"].as_str()),
            ("1", "f", "content")
        );
        assert_eq!(md["base.0"], ObjectId::of_blob(b"1\n").to_string());
        assert_eq!(md["ours.0"], ObjectId::of_blob(b"ours\n").to_string());
        assert_eq!(md["theirs.0"], ObjectId::of_blob(b"theirs\n").to_string());

        let d = commit(&w, &[a], &[("other", b"x")], "d").await;
        let (code, why, md) = reason(&land.land(REPO, "main", d, true).await.expect_err("ff only"));
        assert_eq!((code, why.as_str()), (Code::Aborted, "NOT_FAST_FORWARD"));
        assert_eq!((md["ref"].as_str(), md["tip"].clone()), ("main", b.to_string()));
    }

    #[test]
    fn forwarded_requests_are_trusted_only_with_the_cluster_secret() {
        assert!(trusted_forward(Some(b"s3cret"), Some("s3cret")));
        assert!(!trusted_forward(Some(b"s3creT"), Some("s3cret")));
        assert!(!trusted_forward(Some(b"s3cre"), Some("s3cret")));
        assert!(!trusted_forward(Some(b"1"), None), "no secret: never trusted");
        assert!(!trusted_forward(Some(b""), Some("")));
        assert!(!trusted_forward(None, Some("s3cret")));
    }

    #[tokio::test]
    async fn lands_are_rate_limited_per_repo_except_forwarded() {
        let (_store, w) = svc().await;
        let land = land_svc(&w)
            .with_rate_limit(Arc::new(RateLimiter::new(1, 1)))
            .with_cluster_secret(Some(Arc::from("s3cret")));
        let a = commit(&w, &[], &[], "a").await;
        let b = commit(&w, &[a], &[("f", b"1")], "b").await;
        let call = |id: ObjectId| LandRequest {
            repo: REPO.into(),
            request_id: String::new(),
            r#ref: "main".into(),
            changeset_id: id_bytes(&id),
            fast_forward_only: false,
            dry_run: false,
        };
        LandService::land(&land, Request::new(call(a))).await.expect("first");
        let err = LandService::land(&land, Request::new(call(b)))
            .await
            .expect_err("limited");
        assert_eq!(err.code(), Code::ResourceExhausted);
        // 调用方自带的转发头不能绕过限流
        let mut spoofed = Request::new(call(b));
        spoofed
            .metadata_mut()
            .insert(FORWARDED_HEADER, tonic::metadata::MetadataValue::from_static("1"));
        let err = LandService::land(&land, spoofed).await.expect_err("spoofed header");
        assert_eq!(err.code(), Code::ResourceExhausted);
        let mut fwd = Request::new(call(b));
        fwd.metadata_mut()
            .insert(FORWARDED_HEADER, tonic::metadata::MetadataValue::from_static("s3cret"));
        LandService::land(&land, fwd)
            .await
            .expect("forwarded requests were counted on the first node");
    }

    #[tokio::test]
    async fn land_rejects_bad_input() {
        let (_store, w) = svc().await;
        let land = land_svc(&w);
        let a = commit(&w, &[], &[], "a").await;
        let code = |r: Result<LandOutcome, Status>| r.err().map(|s| s.code());
        let ghost = ObjectId::hash(scion_object::ObjectKind::Changeset, b"ghost");
        assert_eq!(
            code(land.land(REPO, "main", ghost, false).await),
            Some(Code::FailedPrecondition)
        );
        assert_eq!(
            code(land.land(REPO, "bad//ref", a, false).await),
            Some(Code::InvalidArgument)
        );
        assert_eq!(code(land.land("nope", "main", a, false).await), Some(Code::NotFound));
        // 无共同历史
        land.land(REPO, "main", a, false).await.expect("a");
        let other = commit(&w, &[], &[("x", b"x")], "other").await;
        assert_eq!(
            code(land.land(REPO, "main", other, false).await),
            Some(Code::FailedPrecondition)
        );
    }
}
