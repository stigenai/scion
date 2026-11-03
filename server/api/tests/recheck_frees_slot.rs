//! A WatchRefs stream that the credential recheck ends frees its slot in the stream budget.

use std::sync::Arc;
use std::time::Duration;

use scion_api::scion_auth::{AuthConfig, Guard, GuardConfig};
use scion_api::{App, ServerConfig, serve};
use scion_proto::v1::ref_service_client::RefServiceClient;
use scion_proto::v1::{WatchRefsRequest, WatchRefsResponse};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Streaming};
use tonic_types::StatusExt;

const POLICY: &str = r#"
[[rule]]
subjects = ["user:alice"]
repos = ["mono"]
actions = ["read"]
"#;

async fn start() -> (String, Arc<Guard>, tokio::sync::oneshot::Sender<()>) {
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
            stream_recheck_interval: Duration::from_millis(200),
            max_stream_lifetime: None,
            max_streams_per_tenant: Some(1),
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store,
    };
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    (url, guard, tx)
}

async fn watch(channel: &Channel, token: &str) -> Result<Streaming<WatchRefsResponse>, tonic::Status> {
    let mut req = tonic::Request::new(WatchRefsRequest {
        repo: "mono".into(),
        refs: vec!["main".into()],
        known_versions: Default::default(),
    });
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).expect("header"),
    );
    let mut stream = RefServiceClient::new(channel.clone())
        .watch_refs(req)
        .await?
        .into_inner();
    stream.message().await?.expect("snapshot");
    Ok(stream)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_watch_ended_by_the_recheck_frees_its_slot() {
    let (url, guard, _stop) = start().await;
    let (t1, rec1) = guard
        .create_token("pat", "alice", vec![], "first", None)
        .await
        .expect("t1");
    let (t2, _) = guard
        .create_token("pat", "alice", vec![], "second", None)
        .await
        .expect("t2");
    let channel = Channel::from_shared(url)
        .expect("url")
        .connect()
        .await
        .expect("connect");

    let mut first = watch(&channel, &t1).await.expect("first watch");
    let held = watch(&channel, &t2).await.expect_err("the slot is held");
    assert_eq!(held.code(), Code::ResourceExhausted);
    assert_eq!(
        held.get_error_details().error_info().expect("info").reason,
        "RATE_LIMITED"
    );

    assert!(guard.revoke_token(&rec1.id).await.expect("revoke"));
    let ended = tokio::time::timeout(Duration::from_secs(2), first.message())
        .await
        .expect("the recheck ends the stream within 2 s")
        .expect_err("the stream ends with an error, not a message or a clean end");
    assert_eq!(ended.code(), Code::Unauthenticated);

    let mut second = None;
    for _ in 0..20 {
        match watch(&channel, &t2).await {
            Ok(s) => {
                second = Some(s);
                break;
            }
            Err(e) => assert_eq!(e.code(), Code::ResourceExhausted, "{e}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _second = second.expect("the ended stream frees its slot");

    let third = watch(&channel, &t2).await.expect_err("the budget stays 1");
    assert_eq!(third.code(), Code::ResourceExhausted);
    let revoked = watch(&channel, &t1).await.expect_err("the revoked token is refused");
    assert_eq!(revoked.code(), Code::Unauthenticated);
    drop(first);
}
