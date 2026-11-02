//! A cache hit must refuse an exchanged JWT whose source token was revoked or has expired.

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
const POLICY: &str = "[[rule]]\nsubjects=[\"group:eng\"]\nrepos=[\"mono\"]\nactions=[\"land\"]\n";

async fn connect(url: &str, token: &str) -> Client {
    Client::builder()
        .endpoint(url)
        .repo(REPO)
        .auth(Auth::Bearer(token.into()))
        .build()
        .await
        .expect("connect")
}

fn unauthenticated(e: &Error) -> bool {
    matches!(e, Error::Rpc { code, .. } if *code == tonic::Code::Unauthenticated)
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_hit_refuses_exchanged_jwt_after_source_revoke_or_expiry() {
    let s = Arc::new(MemStore::new());
    s.create_repo(REPO).await.expect("repo");
    let auth = GuardConfig {
        required: true,
        signing_key_pem: Some(scion_api::scion_auth::token::generate_signing_key()),
        ..GuardConfig::default()
    };
    let guard = Guard::build(auth.clone(), s.clone(), Arc::new(|_: &str| None)).expect("guard");
    guard.set_policy(POLICY).await.expect("policy");
    let (eng, _) = guard
        .create_token("pat", "bob", vec!["eng".into()], "", None)
        .await
        .expect("eng");

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
    let writer = connect(&up_url, &eng).await;
    let cs = writer.changeset(None).put("a.txt", "hello").create().await.expect("create");
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

    let blob = ObjectId::of_blob(b"hello");
    let exchange = |token: String| {
        let guard = guard.clone();
        async move { guard.exchange(&token).await.expect("exchange").0 }
    };

    let (p_revoke, rec_revoke) = guard
        .create_token("pat", "alice", vec!["eng".into()], "", None)
        .await
        .expect("pat");
    let (p_expire, _) = guard
        .create_token("pat", "carol", vec!["eng".into()], "", Some(Duration::from_secs(2)))
        .await
        .expect("short pat");
    let (p_ok, _) = guard
        .create_token("pat", "dave", vec!["eng".into()], "", None)
        .await
        .expect("valid pat");
    let (j_revoke, j_expire, j_ok) = (
        exchange(p_revoke).await,
        exchange(p_expire).await,
        exchange(p_ok).await,
    );

    // Warm the cache, then prove each JWT reads a real hit.
    for j in [&j_revoke, &j_expire, &j_ok] {
        let c = connect(&cache_url, j).await;
        assert_eq!(c.get_object(&blob).await.expect("miss or hit").as_ref(), b"hello");
        let hits = node.stats().hits;
        assert_eq!(c.get_object(&blob).await.expect("hit").as_ref(), b"hello");
        assert!(node.stats().hits > hits, "second read is a cache hit");
    }

    assert!(guard.revoke_token(&rec_revoke.id).await.expect("revoke"));
    tokio::time::sleep(Duration::from_millis(2500)).await;

    let e = connect(&cache_url, &j_revoke)
        .await
        .get_object(&blob)
        .await
        .expect_err("revoked source");
    assert!(unauthenticated(&e), "{e}");
    let e = connect(&cache_url, &j_expire)
        .await
        .get_object(&blob)
        .await
        .expect_err("expired source");
    assert!(unauthenticated(&e), "{e}");

    let hits = node.stats().hits;
    let ok = connect(&cache_url, &j_ok).await;
    assert_eq!(ok.get_object(&blob).await.expect("valid").as_ref(), b"hello");
    assert!(node.stats().hits > hits, "valid JWT is still a hit");
}
