//! An L3 upstream without AuthService must not make the cache serve bytes: the cache fails closed.

use std::collections::HashMap;
use std::sync::Arc;

use scion_cache::{
    CacheNode, GrpcUpstream, ObjectCache, PoolConfig, RequestCtx, Ring, StorageConfig, Upstream, UpstreamPool,
};
use scion_object::ObjectId;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::transport::{Endpoint, Server};

const BLOB: &[u8] = b"secret";

/// A server with only the health service: any other service answers Unimplemented.
async fn bare_upstream() -> (Arc<GrpcUpstream>, CacheNode, ObjectId) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (_reporter, health) = tonic_health::server::health_reporter();
    tokio::spawn(async move {
        Server::builder()
            .add_service(health)
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
    let pool = UpstreamPool::new(vec![up.clone() as Arc<dyn Upstream>], PoolConfig::default());
    let node = CacheNode::new(store, Ring::new(Vec::new()), None, HashMap::new(), pool);
    (up, node, id)
}

fn bearer() -> RequestCtx {
    RequestCtx {
        authorization: Some("Bearer t".parse().expect("header")),
        ..RequestCtx::default()
    }
}

#[tokio::test]
async fn no_token_gets_unauthenticated() {
    let (_, node, id) = bare_upstream().await;
    let err = node
        .get_many("r", &[id], &RequestCtx::default(), false)
        .await
        .expect_err("denied");
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn token_gets_permission_denied() {
    let (_, node, id) = bare_upstream().await;
    let err = node.get_many("r", &[id], &bearer(), false).await.expect_err("denied");
    assert_eq!(err.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn forwarded_read_gets_permission_denied() {
    let (_, node, id) = bare_upstream().await;
    let err = node.get_many("r", &[id], &bearer(), true).await.expect_err("denied");
    assert_eq!(err.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn can_read_is_false_not_an_error() {
    let (up, _, _) = bare_upstream().await;
    assert!(!up.can_read("r", &bearer()).await.expect("ok"));
}
