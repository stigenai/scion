//! An upstream without AuthService must not make the daemon serve its local cache: `check_access` fails closed.

use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::transport::{Channel, Endpoint, Server};

use crate::{Cas, Daemon, PrefetchOptions, REDACTION_TTL};

/// A server with only the health service: any other service answers Unimplemented.
async fn bare_upstream() -> Channel {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (_reporter, health) = tonic_health::server::health_reporter();
    tokio::spawn(async move {
        Server::builder()
            .add_service(health)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    Endpoint::from_shared(url).expect("url").connect_lazy()
}

async fn check(authorization: Option<&str>) -> Result<(), tonic::Status> {
    let dir = tempdir().expect("tempdir");
    let daemon = Daemon::new(
        Cas::open(dir.path(), 1 << 20, 1 << 20).expect("cas"),
        PrefetchOptions::default(),
        REDACTION_TTL,
    );
    let auth = authorization.map(|a| a.parse().expect("header"));
    daemon.check_access(&bare_upstream().await, &"r".into(), auth.as_ref()).await
}

#[tokio::test]
async fn token_gets_permission_denied() {
    let err = check(Some("Bearer zq")).await.expect_err("denied");
    assert_eq!(err.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn no_token_gets_unauthenticated() {
    let err = check(None).await.expect_err("denied");
    assert_eq!(err.code(), Code::Unauthenticated);
}
