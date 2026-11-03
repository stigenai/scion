//! A server that answers Unimplemented to ListRedactedObjects must not make the daemon serve
//! objects: the daemon cannot verify redactions, so it fails closed.
#![cfg(unix)]

use std::time::Duration;

use scion_client::{Client, Error, ObjectId};
use scion_daemon::{DaemonConfig, PrefetchOptions};
use scion_proto::v1::auth_service_server::{AuthService, AuthServiceServer};
use scion_proto::v1::object_service_server::{ObjectService, ObjectServiceServer};
use scion_proto::v1::server_info_service_server::{ServerInfoService, ServerInfoServiceServer};
use scion_proto::v1::{
    CheckAccessRequest, CheckAccessResponse, ExchangeTokenRequest, ExchangeTokenResponse, GetObjectsRequest,
    GetObjectsResponse, GetServerInfoRequest, GetServerInfoResponse, HasObjectsRequest, HasObjectsResponse,
    ListRedactedObjectsRequest, ListRedactedObjectsResponse, PollDeviceLoginRequest, PollDeviceLoginResponse,
    StartDeviceLoginRequest, StartDeviceLoginResponse, WhoAmIRequest, WhoAmIResponse,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// Allows every read and serves no object. ListRedactedObjects is Unimplemented.
#[derive(Clone)]
struct Stub;

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
impl ServerInfoService for Stub {
    async fn get_server_info(
        &self,
        _: Request<GetServerInfoRequest>,
    ) -> Result<Response<GetServerInfoResponse>, Status> {
        let info = scion_proto::v1::ServerInfo {
            protocol_version: scion_proto::PROTOCOL_VERSION.into(),
            hash_algo: scion_proto::v1::HashAlgo::Blake3256.into(),
            limits: Some(scion_proto::v1::Limits {
                max_batch_objects: 1000,
                max_chunk_bytes: 1 << 20,
                ..Default::default()
            }),
            ..Default::default()
        };
        Ok(Response::new(GetServerInfoResponse { info: Some(info) }))
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
        Err(Status::unimplemented("list_redacted_objects"))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unimplemented_redaction_feed_refuses_the_read() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let server = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        Server::builder()
            .add_service(AuthServiceServer::new(Stub))
            .add_service(ObjectServiceServer::new(Stub))
            .add_service(ServerInfoServiceServer::new(Stub))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = DaemonConfig {
        socket: dir.path().join("run/daemon.sock"),
        cache_dir: dir.path().join("objects"),
        disk_bytes: 64 << 20,
        memory_bytes: 8 << 20,
        idle_timeout: None,
        prefetch: PrefetchOptions::default(),
        redaction_ttl: Duration::ZERO,
    };
    let socket = cfg.socket.clone();
    let (stop, rx) = oneshot::channel::<()>();
    let daemon = tokio::spawn(scion_daemon::run(cfg, async {
        let _ = rx.await;
    }));
    for _ in 0..200 {
        if tokio::net::UnixStream::connect(&socket).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let c = Client::builder()
        .endpoint(&server)
        .repo("mono")
        .daemon_socket(&socket)
        .build()
        .await
        .expect("connect via daemon");

    let err = c.get_object(&ObjectId::of_blob(b"secret")).await.expect_err("refused");
    match &err {
        Error::Rpc { code, message, .. } => {
            assert_eq!(*code, tonic::Code::Unavailable, "{err:?}");
            assert!(message.contains("cannot verify redactions of mono"), "{err:?}");
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let _ = stop.send(());
    daemon.await.expect("join").expect("clean exit");
}
