//! Stream limits of the cache node: the lifetime of a GetObjects stream, the per-caller stream
//! budget, the budget key, and the connection age backstop for a client that stops reading.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use scion_api::scion_auth::{Guard, GuardConfig};
use scion_api::{App, ServerConfig};
use scion_cache::{CacheConfig, StorageConfig};
use scion_client::{Auth, Client, ObjectId};
use scion_proto::v1::object_service_client::ObjectServiceClient;
use scion_proto::v1::{GetObjectsRequest, GetObjectsResponse, ObjectWant};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Streaming};

const REPO: &str = "mono";
const POLICY: &str = "[[rule]]\nsubjects=[\"group:eng\"]\nrepos=[\"mono\"]\nactions=[\"read\",\"land\"]\n";
/// 2 MiB in 64 KiB chunks: more than one HTTP/2 window, so a stream stays open until it is read.
const BLOB_LEN: usize = 2 << 20;
const CHUNK: u64 = 64 << 10;
const CHUNKS: usize = BLOB_LEN / CHUNK as usize;

/// The rejection counters are global to the process: tests that read them run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Rig {
    url: String,
    blob: ObjectId,
    eng: Vec<String>,
    outsider: String,
    _stop: Vec<oneshot::Sender<()>>,
}

fn blob_bytes() -> Vec<u8> {
    (0..BLOB_LEN).map(|i| (i * 31 % 251) as u8).collect()
}

async fn start(lifetime: Option<Duration>, streams: Option<usize>) -> Rig {
    let s = Arc::new(MemStore::new());
    s.create_repo(REPO).await.expect("repo");
    let auth = GuardConfig {
        required: true,
        ..GuardConfig::default()
    };
    let guard = Guard::build(auth.clone(), s.clone(), Arc::new(|_: &str| None)).expect("guard");
    guard.set_policy(POLICY).await.expect("policy");
    let mut eng = Vec::new();
    for who in ["a", "b", "writer"] {
        eng.push(
            guard
                .create_token("pat", who, vec!["eng".into()], "", None)
                .await
                .expect("token")
                .0,
        );
    }
    let outsider = guard
        .create_token("pat", "outsider", vec![], "", None)
        .await
        .expect("token")
        .0;
    let l3 = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let l3_url = format!("http://{}", l3.local_addr().expect("addr"));
    let mut config = ServerConfig {
        auth,
        ..ServerConfig::default()
    };
    config.limits.max_chunk_bytes = CHUNK;
    let app = App {
        config: Arc::new(config),
        meta: s.clone(),
        blobs: s.clone(),
    };
    let (stop_l3, rx) = oneshot::channel::<()>();
    tokio::spawn(scion_api::serve(l3, app, async {
        let _ = rx.await;
    }));
    let writer = Client::builder()
        .endpoint(&l3_url)
        .repo(REPO)
        .auth(Auth::Bearer(eng[2].clone()))
        .build()
        .await
        .expect("connect");
    let cs = writer
        .changeset(None)
        .put("big.bin", blob_bytes())
        .create()
        .await
        .expect("create");
    writer.land("main", &cs.id).await.expect("land");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let config = CacheConfig {
        upstreams: vec![l3_url],
        storage: StorageConfig {
            memory_bytes: 64 << 20,
            ..StorageConfig::default()
        },
        stats_interval: None,
        max_stream_lifetime: lifetime,
        max_streams_per_tenant: streams,
        ..CacheConfig::default()
    };
    let (node, info, channels) = scion_cache::build(&config).await.expect("build");
    let (stop_cache, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        scion_cache::serve_node(listener, node, info, channels, &config, async {
            let _ = rx.await;
        })
        .await
        .expect("serve")
    });
    Rig {
        url,
        blob: ObjectId::of_blob(&blob_bytes()),
        eng,
        outsider,
        _stop: vec![stop_l3, stop_cache],
    }
}

fn counter(reason: &str) -> u64 {
    scion_obs::metrics()
        .encode()
        .lines()
        .find_map(|l| l.strip_prefix(&format!("scion_rejected_requests_total{{reason=\"{reason}\"}} ")))
        .map_or(0, |v| v.trim().parse().expect("counter"))
}

fn want(id: &ObjectId, offset: u64) -> GetObjectsRequest {
    GetObjectsRequest {
        repo: REPO.into(),
        wants: vec![ObjectWant {
            id: Bytes::copy_from_slice(&id.to_bytes()),
            offset,
        }],
        accept_encodings: vec![],
    }
}

fn with_token<T>(msg: T, token: &str) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).expect("header"),
    );
    r
}

async fn channel(r: &Rig) -> Channel {
    // Fixed small windows turn off the adaptive window, so an unread stream stays open on the server.
    Channel::from_shared(r.url.clone())
        .expect("url")
        .initial_stream_window_size(65535)
        .initial_connection_window_size(65535)
        .connect()
        .await
        .expect("connect")
}

/// Opens a GetObjects stream and reads its first chunk.
async fn open(
    ch: &Channel,
    token: &str,
    req: GetObjectsRequest,
) -> Result<Streaming<GetObjectsResponse>, tonic::Status> {
    let mut s = ObjectServiceClient::new(ch.clone())
        .get_objects(with_token(req, token))
        .await?
        .into_inner();
    s.message().await?.expect("first chunk");
    Ok(s)
}

/// Reads the rest of a stream and returns the number of chunks.
async fn drain(mut s: Streaming<GetObjectsResponse>) -> Result<usize, tonic::Status> {
    let mut n = 0;
    while s.message().await?.is_some() {
        n += 1;
    }
    Ok(n)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cache_stream_ends_with_unavailable_at_its_lifetime() {
    let _g = SERIAL.lock().await;
    let r = start(Some(Duration::from_secs(2)), None).await;
    let before = counter("stream_lifetime");
    let slow = open(&channel(&r).await, &r.eng[0], want(&r.blob, 0))
        .await
        .expect("open");
    // A fast reader ends cleanly inside the lifetime.
    let fast = open(&channel(&r).await, &r.eng[0], want(&r.blob, 0))
        .await
        .expect("open");
    assert_eq!(drain(fast).await.expect("clean end"), CHUNKS - 1);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let err = drain(slow).await.expect_err("not a clean end");
    assert_eq!(err.code(), Code::Unavailable, "{err}");
    assert_eq!(err.message(), "stream lifetime of 2s reached; reconnect");
    assert!(counter("stream_lifetime") > before);
    for none in [None, Some(Duration::ZERO)] {
        let r = start(none, None).await;
        let slow = open(&channel(&r).await, &r.eng[0], want(&r.blob, 0))
            .await
            .expect("open");
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert_eq!(drain(slow).await.expect("clean end"), CHUNKS - 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn each_token_has_its_own_stream_budget() {
    let _g = SERIAL.lock().await;
    let r = start(None, Some(1)).await;
    let before = counter("tenant_streams");
    let a = open(&channel(&r).await, &r.eng[0], want(&r.blob, 0))
        .await
        .expect("a's slot");
    let refused = open(&channel(&r).await, &r.eng[0], want(&r.blob, 0))
        .await
        .expect_err("a's second");
    assert_eq!(refused.code(), Code::ResourceExhausted, "{refused}");
    assert!(counter("tenant_streams") > before);
    let _b = open(&channel(&r).await, &r.eng[1], want(&r.blob, 0))
        .await
        .expect("b has its own budget");
    drop(a);
    let mut again = None;
    for _ in 0..50 {
        if let Ok(s) = open(&channel(&r).await, &r.eng[0], want(&r.blob, 0)).await {
            again = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(again.is_some(), "a's slot is free after its stream drops");

    let r = start(None, None).await;
    let mut held = Vec::new();
    for _ in 0..20 {
        held.push(
            open(&channel(&r).await, &r.eng[0], want(&r.blob, 0))
                .await
                .expect("no budget"),
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn one_token_written_two_ways_has_one_budget() {
    let _g = SERIAL.lock().await;
    let r = start(None, Some(1)).await;
    let ch = channel(&r).await;
    let _held = open(&ch, &r.eng[0], want(&r.blob, 0)).await.expect("slot");
    for header in [
        format!("bearer {}", r.eng[0]),
        format!("BEARER {}", r.eng[0]),
        format!("Bearer   {}", r.eng[0]),
    ] {
        let mut req = tonic::Request::new(want(&r.blob, 0));
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(header.clone()).expect("header"),
        );
        let err = ObjectServiceClient::new(ch.clone())
            .get_objects(req)
            .await
            .expect_err(&header);
        assert_eq!(err.code(), Code::ResourceExhausted, "{header}: {err}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn refused_reads_take_no_slot() {
    let _g = SERIAL.lock().await;
    let r = start(None, Some(1)).await;
    for _ in 0..3 {
        let e = open(&channel(&r).await, &r.outsider, want(&r.blob, 0))
            .await
            .expect_err("no read access");
        assert!(
            matches!(e.code(), Code::PermissionDenied | Code::Unauthenticated),
            "{e}"
        );
        let e = open(&channel(&r).await, "no-such-token", want(&r.blob, 0))
            .await
            .expect_err("bad token");
        assert_eq!(e.code(), Code::Unauthenticated, "{e}");
        let mut bad = want(&r.blob, 0);
        bad.wants[0].id = Bytes::from_static(b"x");
        let e = open(&channel(&r).await, &r.eng[0], bad).await.expect_err("bad id");
        assert_eq!(e.code(), Code::InvalidArgument, "{e}");
        let e = open(&channel(&r).await, &r.eng[0], want(&r.blob, BLOB_LEN as u64 + 1))
            .await
            .expect_err("offset");
        assert_eq!(e.code(), Code::OutOfRange, "{e}");
    }
    open(&channel(&r).await, &r.eng[0], want(&r.blob, 0))
        .await
        .expect("the only slot is free");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_budget_key_never_holds_the_bearer() {
    let _g = SERIAL.lock().await;
    let r = start(None, Some(1)).await;
    let _a = open(&channel(&r).await, &r.eng[0], want(&r.blob, 0))
        .await
        .expect("slot");
    let e = open(&channel(&r).await, &r.eng[0], want(&r.blob, 0))
        .await
        .expect_err("refused");
    assert!(e.message().contains("tok:"), "{e}");
    assert!(
        !e.message().contains(&r.eng[0]),
        "the bearer is not in the message: {e}"
    );
    // The same rule holds for a request that another cache forwards.
    let mut fwd = with_token(want(&r.blob, 0), &r.eng[0]);
    fwd.metadata_mut()
        .insert(scion_cache::CACHE_FORWARDED_HEADER, MetadataValue::from_static("1"));
    let e = ObjectServiceClient::new(channel(&r).await)
        .get_objects(fwd)
        .await
        .expect_err("same key, same budget");
    assert_eq!(e.code(), Code::ResourceExhausted, "{e}");
}

/// A client with a zero HTTP/2 window never reads. The lifetime timer cannot run, so the
/// connection age limit closes the connection: lifetime 2s plus a 2s grace.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_stops_reading_loses_its_connection() {
    let _g = SERIAL.lock().await;
    for (lifetime, closes) in [(Some(Duration::from_secs(2)), true), (None, false)] {
        let r = start(lifetime, None).await;
        let forced = counter("connection_age");
        let tcp = tokio::net::TcpStream::connect(r.url.trim_start_matches("http://"))
            .await
            .expect("tcp");
        let (mut send, conn) = h2::client::Builder::new()
            .initial_window_size(0)
            .initial_connection_window_size(0)
            .handshake::<_, Bytes>(tcp)
            .await
            .expect("h2");
        let mut conn = tokio::spawn(conn);
        let request = http::Request::post(format!("{}/scion.v1.ObjectService/GetObjects", r.url))
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("authorization", format!("Bearer {}", r.eng[0]))
            .body(())
            .expect("request");
        let (response, mut body) = send.send_request(request, false).expect("send");
        let msg = want(&r.blob, 0).encode_to_vec();
        let mut frame = vec![0u8, 0, 0, 0, msg.len() as u8];
        frame.extend_from_slice(&msg);
        body.send_data(Bytes::from(frame), true).expect("data");
        let _held = response.await.expect("response head");
        let outcome = tokio::time::timeout(Duration::from_secs(8), &mut conn).await;
        assert_eq!(outcome.is_ok(), closes, "connection closed within 8s: {outcome:?}");
        if closes {
            assert!(counter("connection_age") > forced, "the forced close counts");
        }
    }
}
