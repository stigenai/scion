//! daemon 的 `RefService`：全部转发给服务端（ref 永远不缓存）。`ResolveRef` 同时登记预取：
//! 工作副本解析过的 ref 之后每次前进，daemon 都在后台把变化的目录树和小文件取进本地缓存。

use std::pin::Pin;
use std::sync::Arc;

use scion_proto::v1::ref_service_client::RefServiceClient;
use scion_proto::v1::ref_service_server::RefService;
use scion_proto::v1::{
    CreateRefRequest, CreateRefResponse, DeleteRefRequest, DeleteRefResponse, ListRefLogRequest, ListRefLogResponse,
    ListRefsRequest, ListRefsResponse, ResolveRefRequest, ResolveRefResponse, UpdateRefRequest, UpdateRefResponse,
    WatchRefsRequest, WatchRefsResponse,
};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::Daemon;
use crate::upstream::{auth_of, forward, upstream_of};

pub type WatchRefsStream = Pin<Box<dyn Stream<Item = Result<WatchRefsResponse, Status>> + Send>>;

pub struct DaemonRefSvc {
    daemon: Arc<Daemon>,
}

impl DaemonRefSvc {
    pub fn new(daemon: Arc<Daemon>) -> Self {
        Self { daemon }
    }
}

#[tonic::async_trait]
impl RefService for DaemonRefSvc {
    type WatchRefsStream = WatchRefsStream;

    async fn resolve_ref(&self, request: Request<ResolveRefRequest>) -> Result<Response<ResolveRefResponse>, Status> {
        self.daemon.touch();
        let up = self.daemon.upstreams.get(&upstream_of(&request)?)?;
        let fwd = forward(&request, request.get_ref().clone());
        let resp = RefServiceClient::new(up.raw.clone()).resolve_ref(fwd).await?;
        let req = request.get_ref();
        self.daemon
            .prefetch
            .watch(&self.daemon, up, &req.repo, &req.r#ref, auth_of(&request));
        Ok(Response::new(resp.into_inner()))
    }

    async fn list_refs(&self, request: Request<ListRefsRequest>) -> Result<Response<ListRefsResponse>, Status> {
        self.daemon.touch();
        let up = self.daemon.upstreams.get(&upstream_of(&request)?)?;
        let fwd = forward(&request, request.get_ref().clone());
        let resp = RefServiceClient::new(up.raw.clone()).list_refs(fwd).await?;
        Ok(Response::new(resp.into_inner()))
    }

    async fn list_ref_log(&self, request: Request<ListRefLogRequest>) -> Result<Response<ListRefLogResponse>, Status> {
        self.daemon.touch();
        let up = self.daemon.upstreams.get(&upstream_of(&request)?)?;
        let fwd = forward(&request, request.get_ref().clone());
        let resp = RefServiceClient::new(up.raw.clone()).list_ref_log(fwd).await?;
        Ok(Response::new(resp.into_inner()))
    }

    async fn create_ref(&self, request: Request<CreateRefRequest>) -> Result<Response<CreateRefResponse>, Status> {
        self.daemon.touch();
        let up = self.daemon.upstreams.get(&upstream_of(&request)?)?;
        let fwd = forward(&request, request.get_ref().clone());
        let resp = RefServiceClient::new(up.raw.clone()).create_ref(fwd).await?;
        Ok(Response::new(resp.into_inner()))
    }

    async fn update_ref(&self, request: Request<UpdateRefRequest>) -> Result<Response<UpdateRefResponse>, Status> {
        self.daemon.touch();
        let up = self.daemon.upstreams.get(&upstream_of(&request)?)?;
        let fwd = forward(&request, request.get_ref().clone());
        let resp = RefServiceClient::new(up.raw.clone()).update_ref(fwd).await?;
        Ok(Response::new(resp.into_inner()))
    }

    async fn delete_ref(&self, request: Request<DeleteRefRequest>) -> Result<Response<DeleteRefResponse>, Status> {
        self.daemon.touch();
        let up = self.daemon.upstreams.get(&upstream_of(&request)?)?;
        let fwd = forward(&request, request.get_ref().clone());
        let resp = RefServiceClient::new(up.raw.clone()).delete_ref(fwd).await?;
        Ok(Response::new(resp.into_inner()))
    }

    async fn watch_refs(&self, request: Request<WatchRefsRequest>) -> Result<Response<WatchRefsStream>, Status> {
        self.daemon.touch();
        let up = self.daemon.upstreams.get(&upstream_of(&request)?)?;
        let fwd = forward(&request, request.get_ref().clone());
        let stream = RefServiceClient::new(up.raw.clone())
            .watch_refs(fwd)
            .await?
            .into_inner();
        Ok(Response::new(Box::pin(stream)))
    }
}
