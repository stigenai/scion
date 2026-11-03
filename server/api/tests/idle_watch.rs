//! An idle WatchRefs stream sees no ref change. It must still end when its caller loses the credential
//! or read access. The server checks the caller every `stream_recheck_interval` (200 ms here).

use std::sync::Arc;
use std::time::Duration;

use scion_api::scion_auth::{AuthConfig, Guard, GuardConfig};
use scion_api::{App, ServerConfig, serve};
use scion_client::{Auth, Client};
use scion_proto::v1::admin_service_client::AdminServiceClient;
use scion_proto::v1::ref_service_client::RefServiceClient;
use scion_proto::v1::{RevokeTokenRequest, SetPolicyRequest, WatchRefsRequest, WatchRefsResponse};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;
use tonic::metadata::MetadataValue;
use tonic::{Code, Streaming};

const INTERVAL: Duration = Duration::from_millis(200);

const POLICY: &str = r#"
[[rule]]
subjects = ["group:eng"]
repos = ["mono"]
actions = ["land"]

[[rule]]
subjects = ["user:root"]
repos = ["*"]
actions = ["admin"]
"#;

struct Server {
    url: String,
    guard: Arc<Guard>,
    _stop: tokio::sync::oneshot::Sender<()>,
}

async fn start() -> Server {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let auth = GuardConfig {
        required: true,
        auth: AuthConfig::parse("[token]\nissuer = \"https://scion.test\"\n").expect("auth config"),
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
            stream_recheck_interval: INTERVAL,
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

fn with_token<T>(msg: T, token: &str) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).expect("header"),
    );
    r
}

async fn pat(s: &Server, subject: &str, ttl: Option<Duration>) -> (String, String) {
    let (t, rec) = s
        .guard
        .create_token("pat", subject, vec!["eng".into()], "test", ttl)
        .await
        .expect("token");
    (t, rec.id)
}

async fn open_watch(s: &Server, token: &str) -> Streaming<WatchRefsResponse> {
    let mut refs = RefServiceClient::connect(s.url.clone()).await.expect("connect");
    let mut stream = refs
        .watch_refs(with_token(
            WatchRefsRequest {
                repo: "mono".into(),
                refs: vec!["main".into()],
                known_versions: Default::default(),
            },
            token,
        ))
        .await
        .expect("watch")
        .into_inner();
    assert!(stream.message().await.expect("snapshot").expect("msg").snapshot);
    stream
}

/// The idle stream must end with `want` inside `within`, without any ref change.
async fn expect_end(stream: &mut Streaming<WatchRefsResponse>, want: Code, within: Duration) {
    match tokio::time::timeout(within, stream.message())
        .await
        .expect("the idle stream ended in time")
    {
        Err(st) => assert_eq!(st.code(), want, "{st}"),
        Ok(m) => panic!("stream delivered {m:?} after the credential was lost"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_stream_ends_after_pat_revocation() {
    let s = start().await;
    let (watcher, id) = pat(&s, "alice", None).await;
    let mut stream = open_watch(&s, &watcher).await;
    let (root, _) = s
        .guard
        .create_token("pat", "root", vec![], "test", None)
        .await
        .map(|(t, r)| (t, r.id))
        .expect("root");
    AdminServiceClient::connect(s.url.clone())
        .await
        .expect("admin")
        .revoke_token(with_token(RevokeTokenRequest { id }, &root))
        .await
        .expect("revoke");
    expect_end(&mut stream, Code::Unauthenticated, Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_stream_ends_after_pat_expiry() {
    let s = start().await;
    let (watcher, _) = pat(&s, "alice", Some(Duration::from_secs(2))).await;
    let mut stream = open_watch(&s, &watcher).await;
    expect_end(&mut stream, Code::Unauthenticated, Duration::from_millis(2800)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_stream_ends_after_read_access_removed() {
    let s = start().await;
    let (watcher, _) = pat(&s, "alice", None).await;
    let mut stream = open_watch(&s, &watcher).await;
    let (root, _) = s
        .guard
        .create_token("pat", "root", vec![], "test", None)
        .await
        .map(|(t, r)| (t, r.id))
        .expect("root");
    AdminServiceClient::connect(s.url.clone())
        .await
        .expect("admin")
        .set_policy(with_token(
            SetPolicyRequest {
                policy: "[[rule]]\nsubjects = [\"group:eng\"]\nrepos = [\"other\"]\nactions = [\"read\"]\n\n\
                         [[rule]]\nsubjects = [\"user:root\"]\nrepos = [\"*\"]\nactions = [\"admin\"]\n"
                    .into(),
            },
            &root,
        ))
        .await
        .expect("set policy");
    expect_end(&mut stream, Code::PermissionDenied, Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_idle_stream_stays_silent_and_still_gets_events() {
    let s = start().await;
    let (watcher, _) = pat(&s, "alice", None).await;
    let mut stream = open_watch(&s, &watcher).await;
    // Five rechecks pass: no message and no error
    let idle = tokio::time::timeout(INTERVAL * 5 + Duration::from_millis(100), stream.message()).await;
    assert!(idle.is_err(), "a valid idle stream stays silent: {idle:?}");
    let (writer, _) = pat(&s, "bob", None).await;
    let w = Client::builder()
        .endpoint(&s.url)
        .repo("mono")
        .auth(Auth::Bearer(writer))
        .build()
        .await
        .expect("connect");
    let cs = w.changeset(None).put("f.txt", "v1").create().await.expect("create");
    w.land("main", &cs.id).await.expect("land");
    let ev = tokio::time::timeout(Duration::from_secs(3), stream.message())
        .await
        .expect("timely")
        .expect("ok")
        .expect("event");
    assert_eq!(ev.events[0].version, 1);
}
