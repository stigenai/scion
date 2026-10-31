//! A cache hit must still check the caller's token with the L3 server: a revoked PAT must not read cached objects.

use std::sync::Arc;
use std::time::Duration;

use scion_api::scion_auth::{Guard, GuardConfig};
use scion_api::{App, ServerConfig};
use scion_cache::{CacheConfig, StorageConfig};
use scion_client::{Auth, Client, Error, ObjectId};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

const REPO: &str = "mono";
const POLICY: &str = "[[rule]]\nsubjects=[\"group:eng\"]\nrepos=[\"mono\"]\nactions=[\"land\"]\n\n\
                      [[rule]]\nsubjects=[\"user:contractor\"]\nrepos=[\"mono\"]\nactions=[\"read\"]\npaths=[\"src/ui\"]\n";

async fn connect(url: &str, token: Option<&str>) -> Client {
    let mut b = Client::builder().endpoint(url).repo(REPO);
    if let Some(t) = token {
        b = b.auth(Auth::Bearer(t.into()));
    }
    b.build().await.expect("connect")
}

fn code(e: &Error) -> Option<tonic::Code> {
    match e {
        Error::Rpc { code, .. } => Some(*code),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_rechecks_revoked_token_on_hit() {
    let s = Arc::new(MemStore::new());
    s.create_repo(REPO).await.expect("repo");
    let auth = GuardConfig {
        required: true,
        ..GuardConfig::default()
    };
    let guard = Guard::build(auth.clone(), s.clone(), Arc::new(|_: &str| None)).expect("guard");
    guard.set_policy(POLICY).await.expect("policy");
    let (eng, _) = guard
        .create_token("pat", "bob", vec!["eng".into()], "", None)
        .await
        .expect("eng");
    let (second, _) = guard
        .create_token("pat", "alice", vec!["eng".into()], "", None)
        .await
        .expect("second");
    let (victim, victim_rec) = guard
        .create_token("pat", "carol", vec!["eng".into()], "", None)
        .await
        .expect("victim");
    let (ct, _) = guard
        .create_token("pat", "contractor", vec![], "", None)
        .await
        .expect("ct");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let up_url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            auth,
            ref_poll_interval: Duration::from_millis(50),
            ..ServerConfig::default()
        }),
        meta: s.clone(),
        blobs: s.clone(),
    };
    let (_stop_l3, rx) = oneshot::channel::<()>();
    tokio::spawn(scion_api::serve(listener, app, async {
        let _ = rx.await;
    }));

    let writer = connect(&up_url, Some(&eng)).await;
    let cs = writer
        .changeset(None)
        .put("src/ui/a.rs", "ui")
        .create()
        .await
        .expect("create");
    writer.land("main", &cs.id).await.expect("land");

    let cache_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let cache_url = format!("http://{}", cache_listener.local_addr().expect("addr"));
    let config = CacheConfig {
        upstreams: vec![up_url],
        storage: StorageConfig {
            memory_bytes: 64 << 20,
            ..StorageConfig::default()
        },
        stats_interval: None,
        ..CacheConfig::default()
    };
    let (node, info, channels) = scion_cache::build(&config).await.expect("build");
    let (_stop_cache, rx) = oneshot::channel::<()>();
    let n = node.clone();
    tokio::spawn(async move {
        scion_cache::serve_node(cache_listener, n, info, channels, &config, async {
            let _ = rx.await;
        })
        .await
        .expect("serve")
    });

    let blob = ObjectId::of_blob(b"ui");
    let via_victim = connect(&cache_url, Some(&victim)).await;
    assert_eq!(via_victim.get_object(&blob).await.expect("miss").as_ref(), b"ui");
    // A real hit under the victim's token, before the revoke.
    let hits = node.stats().hits;
    assert_eq!(via_victim.get_object(&blob).await.expect("hit").as_ref(), b"ui");
    assert!(node.stats().hits > hits, "second read is a cache hit");

    assert!(guard.revoke_token(&victim_rec.id).await.expect("revoke"));
    let e = via_victim.get_object(&blob).await.expect_err("revoked token on a hit");
    assert_eq!(code(&e), Some(tonic::Code::Unauthenticated), "{e}");

    // Controls: a second valid token is still served from the cache; the other rows do not change.
    let hits = node.stats().hits;
    let via_second = connect(&cache_url, Some(&second)).await;
    assert_eq!(via_second.get_object(&blob).await.expect("second").as_ref(), b"ui");
    assert!(node.stats().hits > hits, "valid second token is a hit");
    let e = connect(&cache_url, Some(&ct))
        .await
        .get_object(&blob)
        .await
        .expect_err("path-scoped reader");
    assert_eq!(e.reason(), Some(scion_proto::v1::ErrorReason::PermissionDenied));
    let e = connect(&cache_url, None)
        .await
        .get_object(&blob)
        .await
        .expect_err("anonymous");
    assert_eq!(code(&e), Some(tonic::Code::Unauthenticated));
}
