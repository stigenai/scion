//! The connection lifetime, end to end: the server sends GOAWAY at the lifetime, a call in flight
//! finishes, and the hard close stays as the backstop (see `stream_limits.rs`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use h2::Reason;
use h2::client::SendRequest;
use scion_api::scion_auth::{AuthConfig, GuardConfig};
use scion_api::{App, ServerConfig, serve};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;

struct Server {
    url: String,
    stop: tokio::sync::oneshot::Sender<()>,
    done: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

async fn start(lifetime: Option<Duration>) -> Server {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let auth = GuardConfig {
        required: true,
        auth: AuthConfig::default(),
        signing_key_pem: Some(scion_api::scion_auth::token::generate_signing_key()),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            auth,
            warm_record_cache: false,
            max_stream_lifetime: lifetime,
            max_request_timeout: Some(Duration::from_secs(1)),
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store,
    };
    let (stop, rx) = tokio::sync::oneshot::channel::<()>();
    let done = tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    Server { url, stop, done }
}

type Conn = tokio::task::JoinHandle<Result<(), h2::Error>>;

async fn connect(url: &str) -> (SendRequest<bytes::Bytes>, Conn) {
    let tcp = tokio::net::TcpStream::connect(url.trim_start_matches("http://"))
        .await
        .expect("tcp");
    let (send, conn) = h2::client::handshake(tcp).await.expect("h2");
    (send, tokio::spawn(conn))
}

fn health(url: &str) -> http::Request<()> {
    http::Request::post(format!("{url}/grpc.health.v1.Health/Check"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(())
        .expect("request")
}

const EMPTY_FRAME: &[u8] = &[0, 0, 0, 0, 0];

/// A whole Health/Check on a connection; returns the HTTP status and the `grpc-status` trailer.
async fn check(send: SendRequest<bytes::Bytes>, url: &str) -> (u16, String) {
    let mut send = send.ready().await.expect("ready");
    let (response, mut body) = send.send_request(health(url), false).expect("send");
    body.send_data(bytes::Bytes::from_static(EMPTY_FRAME), true)
        .expect("data");
    finish(response).await
}

async fn finish(response: h2::client::ResponseFuture) -> (u16, String) {
    let head = response.await.expect("response");
    let status = head.status().as_u16();
    let mut body = head.into_body();
    while let Some(chunk) = body.data().await {
        chunk.expect("chunk");
    }
    let trailers = body.trailers().await.expect("trailers").expect("trailer block");
    (status, trailers["grpc-status"].to_str().expect("text").to_string())
}

fn counter(reason: &str) -> u64 {
    scion_obs::metrics()
        .encode()
        .lines()
        .find_map(|l| l.strip_prefix(&format!("scion_rejected_requests_total{{reason=\"{reason}\"}} ")))
        .map_or(0, |v| v.trim().parse().expect("counter"))
}

/// Lifetime 1s, hard age 2s. At 0.5s a call works. At 1.3s the client holds a GOAWAY: a new call
/// is refused on the client with NO_ERROR (a retry-safe signal, not a cut), and the connection
/// ends cleanly before the hard age.
#[tokio::test(flavor = "multi_thread")]
async fn a_healthy_connection_gets_goaway_at_the_lifetime() {
    let s = start(Some(Duration::from_secs(1))).await;
    let forced = counter("connection_age");
    let t0 = Instant::now();
    let (mut send, conn) = connect(&s.url).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        check(send.clone(), &s.url).await,
        (200, "0".into()),
        "a call works before the lifetime"
    );
    tokio::time::sleep(Duration::from_millis(800)).await;
    let err = send
        .send_request(health(&s.url), true)
        .expect_err("refused after the GOAWAY");
    assert!(err.is_go_away(), "{err}");
    assert_eq!(err.reason(), Some(Reason::NO_ERROR), "{err}");
    assert!(!err.is_io(), "{err}");
    let ended = tokio::time::timeout(Duration::from_secs(2), conn)
        .await
        .expect("the connection ends");
    assert!(ended.expect("join").is_ok(), "a graceful close");
    assert!(
        t0.elapsed() < Duration::from_secs(2),
        "before the hard age: {:?}",
        t0.elapsed()
    );
    assert_eq!(
        counter("connection_age"),
        forced,
        "a graceful close is not a forced close"
    );
}

/// A call that starts before the lifetime and sends its body after the GOAWAY finishes.
#[tokio::test(flavor = "multi_thread")]
async fn a_call_in_flight_at_the_lifetime_finishes() {
    let s = start(Some(Duration::from_secs(1))).await;
    let (mut send, _conn) = connect(&s.url).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let (response, mut body) = send.send_request(health(&s.url), false).expect("send");
    tokio::time::sleep(Duration::from_millis(500)).await;
    body.send_data(bytes::Bytes::from_static(EMPTY_FRAME), true)
        .expect("data after the GOAWAY");
    assert_eq!(finish(response).await, (200, "0".into()));
}

/// With no lifetime the server keeps the connection open.
#[tokio::test(flavor = "multi_thread")]
async fn no_lifetime_sends_no_goaway() {
    let s = start(None).await;
    let (send, _conn) = connect(&s.url).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(check(send, &s.url).await, (200, "0".into()));
}

/// The server stops in time when a connection has had its GOAWAY already.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_after_a_goaway_returns() {
    let s = start(Some(Duration::from_secs(1))).await;
    let (_send, _conn) = connect(&s.url).await;
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let _ = s.stop.send(());
    let result = tokio::time::timeout(Duration::from_secs(5), s.done)
        .await
        .expect("serve returns");
    assert!(result.expect("join").is_ok());
}
