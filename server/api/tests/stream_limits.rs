//! Limits on server streams, end to end: the lifetime of a WatchRefs stream and the resume of
//! `scn watch`, the per-caller stream budget, and a compressed gRPC request.

use std::sync::Arc;
use std::time::Duration;

use scion_api::scion_auth::{AuthConfig, Guard, GuardConfig};
use scion_api::{App, ServerConfig, serve};
use scion_client::{Auth, Client};
use scion_proto::v1::object_service_client::ObjectServiceClient;
use scion_proto::v1::query_service_client::QueryServiceClient;
use scion_proto::v1::ref_service_client::RefServiceClient;
use scion_proto::v1::{
    ArchiveRequest, DiffRequest, GetObjectsRequest, GrepRequest, ReadFileRequest, WatchRefsRequest, WatchRefsResponse,
};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Streaming};
use tonic_types::StatusExt;

const POLICY: &str = r#"
[[rule]]
subjects = ["user:alice", "user:bob"]
repos = ["mono"]
actions = ["read"]

[[rule]]
subjects = ["user:writer"]
repos = ["mono"]
actions = ["read", "land"]
"#;

struct Server {
    url: String,
    guard: Arc<Guard>,
    _stop: tokio::sync::oneshot::Sender<()>,
}

async fn start(lifetime: Option<Duration>, streams: Option<usize>) -> Server {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let auth = GuardConfig {
        required: true,
        auth: AuthConfig::default(),
        signing_key_pem: Some(scion_api::scion_auth::token::generate_signing_key()),
    };
    let guard = Guard::build(auth.clone(), store.clone(), Arc::new(|_: &str| None)).expect("guard");
    guard.set_policy(POLICY).await.expect("policy");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            auth,
            warm_record_cache: false,
            ref_poll_interval: Duration::from_millis(50),
            max_stream_lifetime: lifetime,
            max_streams_per_tenant: streams,
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store,
    };
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    Server { url, guard, _stop: tx }
}

async fn token(s: &Server, subject: &str) -> String {
    s.guard
        .create_token("pat", subject, vec![], "test", None)
        .await
        .expect("token")
        .0
}

fn with_token<T>(msg: T, token: &str) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).expect("header"),
    );
    r
}

async fn client(s: &Server, token: &str) -> Client {
    Client::builder()
        .endpoint(&s.url)
        .repo("mono")
        .auth(Auth::Bearer(token.into()))
        .build()
        .await
        .expect("connect")
}

async fn land_change(writer: &Client, n: usize) {
    let tip = writer.resolve_ref("main").await.expect("resolve").map(|t| t.changeset);
    let cs = writer
        .changeset(tip)
        .put("f.txt", format!("v{n}"))
        .create()
        .await
        .expect("create");
    writer.land("main", &cs.id).await.expect("land");
}

fn counter(reason: &str) -> u64 {
    scion_obs::metrics()
        .encode()
        .lines()
        .find_map(|l| l.strip_prefix(&format!("scion_rejected_requests_total{{reason=\"{reason}\"}} ")))
        .map_or(0, |v| v.trim().parse().expect("counter"))
}

async fn watch(channel: &Channel, token: &str, repo: &str) -> Result<Streaming<WatchRefsResponse>, tonic::Status> {
    let req = WatchRefsRequest {
        repo: repo.into(),
        refs: vec!["main".into()],
        known_versions: Default::default(),
    };
    let mut refs = RefServiceClient::new(channel.clone());
    let mut stream = refs.watch_refs(with_token(req, token)).await?.into_inner();
    stream.message().await?.expect("snapshot");
    Ok(stream)
}

/// A stream ends at its lifetime with UNAVAILABLE, and the SDK resumes by version with no lost
/// event: five lands over three seconds reach `next()` once each and in order.
#[tokio::test(flavor = "multi_thread")]
async fn watch_resumes_by_version_after_the_lifetime() {
    let s = start(Some(Duration::from_secs(1)), None).await;
    let writer = client(&s, &token(&s, "writer").await).await;
    land_change(&writer, 1).await;
    let before = counter("stream_lifetime");

    let mut w = client(&s, &token(&s, "alice").await)
        .await
        .watch_refs(&["main"])
        .await
        .expect("watch");
    let first = w.next().await.expect("snapshot");
    assert_eq!((first.version, first.initial), (1, true));

    let lander = tokio::spawn(async move {
        for n in 2..=6 {
            tokio::time::sleep(Duration::from_millis(600)).await;
            land_change(&writer, n).await;
        }
    });
    let mut seen = Vec::new();
    for _ in 2..=6 {
        let c = tokio::time::timeout(Duration::from_secs(10), w.next())
            .await
            .expect("an event within 10 s")
            .expect("event");
        seen.push(c.version);
    }
    lander.await.expect("lander");
    assert_eq!(seen, vec![2, 3, 4, 5, 6]);
    assert_eq!(w.known_versions()["main"], 6);
    assert!(
        counter("stream_lifetime") > before,
        "the stream ended at its lifetime at least once"
    );
}

/// The raw stream ends with UNAVAILABLE, not with a clean end.
#[tokio::test(flavor = "multi_thread")]
async fn the_stream_ends_with_unavailable() {
    let s = start(Some(Duration::from_secs(1)), None).await;
    let alice = token(&s, "alice").await;
    let channel = Channel::from_shared(s.url.clone())
        .expect("url")
        .connect()
        .await
        .expect("connect");
    let mut stream = watch(&channel, &alice, "mono").await.expect("watch");
    let end = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("ends within 5 s")
        .expect_err("a status, not a clean end");
    assert_eq!(end.code(), Code::Unavailable);
    assert!(end.message().contains("stream lifetime of 1s reached"), "{end}");
    assert!(end.get_error_details().error_info().is_none(), "no ErrorInfo reason");
}

/// The N+1th stream from one caller is refused; another caller still opens one, a closed stream
/// frees a slot, and ReadFile and GetObjects use the same budget.
#[tokio::test(flavor = "multi_thread")]
async fn each_caller_has_its_own_stream_budget() {
    let s = start(None, Some(2)).await;
    let writer = client(&s, &token(&s, "writer").await).await;
    land_change(&writer, 1).await;
    let (alice, bob) = (token(&s, "alice").await, token(&s, "bob").await);
    let channel = Channel::from_shared(s.url.clone())
        .expect("url")
        .connect()
        .await
        .expect("connect");
    let before = counter("tenant_streams");

    let a1 = watch(&channel, &alice, "mono").await.expect("alice 1");
    let _a2 = watch(&channel, &alice, "mono").await.expect("alice 2");
    let refused = watch(&channel, &alice, "mono").await.expect_err("alice 3 is refused");
    assert_eq!(refused.code(), Code::ResourceExhausted);
    let info = refused.get_error_details().error_info().cloned().expect("info");
    assert_eq!(info.reason, "RATE_LIMITED");
    let _b1 = watch(&channel, &bob, "mono").await.expect("bob still opens a stream");

    let read = |token: &str| {
        let mut q = QueryServiceClient::new(channel.clone());
        let req = with_token(
            ReadFileRequest {
                repo: "mono".into(),
                revision: "main".into(),
                path: "f.txt".into(),
                offset: 0,
            },
            token,
        );
        async move { q.read_file(req).await.map(|r| r.into_inner()) }
    };
    let get = |token: &str| {
        let mut o = ObjectServiceClient::new(channel.clone());
        let req = with_token(
            GetObjectsRequest {
                repo: "mono".into(),
                wants: vec![],
                accept_encodings: vec![],
            },
            token,
        );
        async move { o.get_objects(req).await.map(|r| r.into_inner()) }
    };
    assert_eq!(
        read(&alice).await.expect_err("read_file").code(),
        Code::ResourceExhausted
    );
    assert_eq!(
        get(&alice).await.expect_err("get_objects").code(),
        Code::ResourceExhausted
    );
    let mut q = QueryServiceClient::new(channel.clone());
    let diff = q
        .diff(with_token(
            DiffRequest {
                repo: "mono".into(),
                ..Default::default()
            },
            &alice,
        ))
        .await
        .expect_err("diff");
    let grep = q
        .grep(with_token(
            GrepRequest {
                repo: "mono".into(),
                pattern: "v".into(),
                ..Default::default()
            },
            &alice,
        ))
        .await
        .expect_err("grep");
    let archive = q
        .archive(with_token(
            ArchiveRequest {
                repo: "mono".into(),
                ..Default::default()
            },
            &alice,
        ))
        .await
        .expect_err("archive");
    for (rpc, err) in [("diff", diff), ("grep", grep), ("archive", archive)] {
        assert_eq!(err.code(), Code::ResourceExhausted, "{rpc}: {err}");
    }
    assert!(counter("tenant_streams") >= before + 6, "each refusal counts");

    drop(a1);
    let mut freed = false;
    for _ in 0..50 {
        if read(&alice).await.is_ok() {
            freed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(freed, "a closed stream frees its slot");
}

/// A refused call takes no slot: an unknown caller and a missing repository fail on their own
/// error, never with ResourceExhausted, however often they try.
#[tokio::test(flavor = "multi_thread")]
async fn refused_calls_take_no_slot() {
    let s = start(None, Some(1)).await;
    let channel = Channel::from_shared(s.url.clone())
        .expect("url")
        .connect()
        .await
        .expect("connect");
    let dave = token(&s, "dave").await;
    let alice = token(&s, "alice").await;
    for _ in 0..4 {
        let denied = watch(&channel, &dave, "mono").await.expect_err("no read access");
        assert_eq!(denied.code(), Code::PermissionDenied);
        let missing = watch(&channel, &alice, "nope").await.expect_err("no such repo");
        assert_ne!(missing.code(), Code::ResourceExhausted, "{missing}");
    }
    watch(&channel, &alice, "mono")
        .await
        .expect("alice's only slot is still free");
}

/// The server inflates no gRPC request: a compressed request gets UNIMPLEMENTED.
#[tokio::test(flavor = "multi_thread")]
async fn compressed_requests_are_refused() {
    let s = start(None, None).await;
    let alice = token(&s, "alice").await;
    let channel = Channel::from_shared(s.url.clone())
        .expect("url")
        .connect()
        .await
        .expect("connect");
    let mut refs = RefServiceClient::new(channel).send_compressed(CompressionEncoding::Gzip);
    let req = WatchRefsRequest {
        repo: "mono".into(),
        refs: vec![],
        known_versions: Default::default(),
    };
    let err = refs.watch_refs(with_token(req, &alice)).await.expect_err("refused");
    assert_eq!(err.code(), Code::Unimplemented, "{err}");
}

/// A client that keeps its HTTP/2 window at zero and never reads holds a stream past its
/// lifetime, because hyper does not poll the body while a chunk waits for send capacity. The
/// connection age limit is the backstop: the connection closes and the slot comes back.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_stops_reading_loses_its_stream_at_the_lifetime() {
    let s = start(Some(Duration::from_secs(1)), Some(1)).await;
    let alice = token(&s, "alice").await;
    let tcp = tokio::net::TcpStream::connect(s.url.trim_start_matches("http://"))
        .await
        .expect("tcp");
    let (mut send, conn) = h2::client::Builder::new()
        .initial_window_size(0)
        .initial_connection_window_size(0)
        .handshake::<_, bytes::Bytes>(tcp)
        .await
        .expect("h2");
    tokio::spawn(conn);
    let request = http::Request::post(format!("{}/scion.v1.RefService/WatchRefs", s.url))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("authorization", format!("Bearer {alice}"))
        .body(())
        .expect("request");
    let (response, mut body) = send.send_request(request, false).expect("send");
    // repo = "mono", refs = ["main"], in one gRPC frame
    let msg = b"\x0a\x04mono\x12\x04main";
    let mut frame = vec![0u8, 0, 0, 0, msg.len() as u8];
    frame.extend_from_slice(msg);
    body.send_data(bytes::Bytes::from(frame), true).expect("data");
    // The response head arrives; its body is never read, and the window stays at zero.
    let _held = response.await.expect("response head");

    let fresh = Channel::from_shared(s.url.clone())
        .expect("url")
        .connect()
        .await
        .expect("connect");
    let refused = watch(&fresh, &alice, "mono").await.expect_err("the slot is held");
    assert_eq!(refused.code(), Code::ResourceExhausted);
    let mut freed = false;
    for _ in 0..80 {
        if watch(&fresh, &alice, "mono").await.is_ok() {
            freed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(freed, "the stalled stream is gone a few seconds after its lifetime");
}
