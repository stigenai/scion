//! An L3 upstream that answers Unimplemented to ListRedactedObjects must not make the cache serve
//! bytes: the cache cannot verify redactions, so it fails closed.

use std::collections::HashMap;
use std::sync::Arc;

use scion_cache::{
    CacheNode, GrpcUpstream, ObjectCache, PoolConfig, RequestCtx, Ring, StorageConfig, Upstream, UpstreamPool,
};
use scion_object::ObjectId;
use scion_proto::v1::auth_service_server::{AuthService, AuthServiceServer};
use scion_proto::v1::object_service_server::{ObjectService, ObjectServiceServer};
use scion_proto::v1::{
    CheckAccessRequest, CheckAccessResponse, ExchangeTokenRequest, ExchangeTokenResponse, GetObjectsRequest,
    GetObjectsResponse, HasObjectsRequest, HasObjectsResponse, ListRedactedObjectsRequest, ListRedactedObjectsResponse,
    PollDeviceLoginRequest, PollDeviceLoginResponse, StartDeviceLoginRequest, StartDeviceLoginResponse, WhoAmIRequest,
    WhoAmIResponse,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};
use tonic::{Code, Request, Response, Status};

const BLOB: &[u8] = b"secret";

/// Allows every read. `redactions` decides whether ListRedactedObjects is served (empty page) or Unimplemented.
#[derive(Clone)]
struct Stub {
    redactions: bool,
}

#[tonic::async_trait]
impl AuthService for Stub {
    async fn who_am_i(&self, _: Request<WhoAmIRequest>) -> Result<Response<WhoAmIResponse>, Status> {
        Err(Status::unimplemented("who_am_i"))
    }
    async fn exchange_token(
        &self,
        _: Request<ExchangeTokenRequest>,
    ) -> Result<Response<ExchangeTokenResponse>, Status> {
        Err(Status::unimplemented("exchange_token"))
    }
    async fn start_device_login(
        &self,
        _: Request<StartDeviceLoginRequest>,
    ) -> Result<Response<StartDeviceLoginResponse>, Status> {
        Err(Status::unimplemented("start_device_login"))
    }
    async fn poll_device_login(
        &self,
        _: Request<PollDeviceLoginRequest>,
    ) -> Result<Response<PollDeviceLoginResponse>, Status> {
        Err(Status::unimplemented("poll_device_login"))
    }
    async fn check_access(&self, _: Request<CheckAccessRequest>) -> Result<Response<CheckAccessResponse>, Status> {
        Ok(Response::new(CheckAccessResponse { allowed: true }))
    }
}

#[tonic::async_trait]
impl ObjectService for Stub {
    type GetObjectsStream = tokio_stream::Empty<Result<GetObjectsResponse, Status>>;

    async fn get_objects(&self, _: Request<GetObjectsRequest>) -> Result<Response<Self::GetObjectsStream>, Status> {
        Err(Status::unimplemented("get_objects"))
    }
    async fn has_objects(&self, _: Request<HasObjectsRequest>) -> Result<Response<HasObjectsResponse>, Status> {
        Ok(Response::new(HasObjectsResponse {
            missing_ids: Vec::new(),
        }))
    }
    async fn list_redacted_objects(
        &self,
        _: Request<ListRedactedObjectsRequest>,
    ) -> Result<Response<ListRedactedObjectsResponse>, Status> {
        if self.redactions {
            Ok(Response::new(ListRedactedObjectsResponse::default()))
        } else {
            Err(Status::unimplemented("list_redacted_objects"))
        }
    }
}

async fn node_with(redactions: bool) -> (CacheNode, ObjectId) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let stub = Stub { redactions };
    tokio::spawn(async move {
        Server::builder()
            .add_service(AuthServiceServer::new(stub.clone()))
            .add_service(ObjectServiceServer::new(stub))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    let channel = Endpoint::from_shared(url.clone()).expect("url").connect_lazy();
    let up = Arc::new(GrpcUpstream::new(url, channel, 100));
    let store = ObjectCache::open(&StorageConfig {
        memory_bytes: 1 << 20,
        ..StorageConfig::default()
    })
    .await
    .expect("cache");
    let id = ObjectId::of_blob(BLOB);
    assert!(store.insert(id, BLOB.into()));
    let pool = UpstreamPool::new(vec![up as Arc<dyn Upstream>], PoolConfig::default());
    (
        CacheNode::new(store, Ring::new(Vec::new()), None, HashMap::new(), pool),
        id,
    )
}

fn bearer() -> RequestCtx {
    RequestCtx {
        authorization: Some("Bearer t".parse().expect("header")),
        ..RequestCtx::default()
    }
}

#[tokio::test]
async fn unimplemented_redaction_feed_refuses_the_read() {
    let (node, id) = node_with(false).await;
    let err = node.get_many("r", &[id], &bearer(), false).await.expect_err("refused");
    assert_eq!(err.code(), Code::Unavailable);
    assert!(err.message().contains("cannot verify redactions of r"), "{err:?}");
}

#[tokio::test]
async fn unimplemented_redaction_feed_refuses_the_forwarded_read() {
    let (node, id) = node_with(false).await;
    let err = node.get_many("r", &[id], &bearer(), true).await.expect_err("refused");
    assert_eq!(err.code(), Code::Unavailable);
    assert!(err.message().contains("cannot verify redactions of r"), "{err:?}");
}

#[tokio::test]
async fn served_empty_redaction_feed_allows_the_read() {
    let (node, id) = node_with(true).await;
    let got = node.get_many("r", &[id], &bearer(), false).await.expect("ok");
    assert_eq!(got[0].as_ref(), BLOB);
}
