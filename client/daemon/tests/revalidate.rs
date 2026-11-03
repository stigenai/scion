//! The daemon checks read access on every GetObjects (ent-fdw, ADR 0003): a revoked or expired PAT and a
//! policy change take effect on the next read, a hit included; offline reads of private objects are refused.
//! The fixture is a real auth-required L3 and a real daemon on a Unix socket.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use scion_api::scion_auth::{Guard, GuardConfig};
use scion_api::{App, ServerConfig};
use scion_client::{Auth, Client, Error, ObjectId};
use scion_daemon::{Cas, Daemon, PrefetchOptions};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::Code;

const SECRET: &[u8] = b"private bytes of mono";

const POLICY: &str = "[[rule]]\nsubjects=[\"user:root\"]\nrepos=[\"mono\"]\nactions=[\"land\"]\n\n\
[[rule]]\nsubjects=[\"user:root\"]\nrepos=[\"*\"]\nactions=[\"admin\"]\n\n\
[[rule]]\nsubjects=[\"user:bob\",\"user:carol\"]\nrepos=[\"mono\"]\nactions=[\"read\"]\n";

struct Fx {
    guard: Arc<Guard>,
    server: String,
    stop_l3: Option<oneshot::Sender<()>>,
    l3: Option<tokio::task::JoinHandle<()>>,
    daemon: Arc<Daemon>,
    socket: PathBuf,
    dir: tempfile::TempDir,
    id: ObjectId,
}

impl Fx {
    async fn new() -> Self {
        let store = Arc::new(MemStore::new());
        store.create_repo("mono").await.expect("repo");
        let auth = GuardConfig {
            required: true,
            ..GuardConfig::default()
        };
        let guard = Guard::build(auth.clone(), store.clone(), Arc::new(|_: &str| None)).expect("guard");
        guard.set_policy(POLICY).await.expect("policy");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let server = format!("http://{}", listener.local_addr().expect("addr"));
        let app = App {
            config: Arc::new(ServerConfig {
                auth,
                ..ServerConfig::default()
            }),
            meta: store.clone(),
            blobs: store,
        };
        let (tx, rx) = oneshot::channel::<()>();
        let l3 = tokio::spawn(async move {
            let _ = scion_api::serve(listener, app, async {
                let _ = rx.await;
            })
            .await;
        });
        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("daemon.sock");
        let cas = Cas::open(&dir.path().join("objects"), 64 << 20, 8 << 20).expect("cas");
        let daemon = Daemon::new(cas, PrefetchOptions::default(), scion_daemon::REDACTION_TTL);
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind socket");
        let routes = scion_daemon::routes(daemon.clone());
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_routes(routes)
                .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener)),
        );
        let fx = Self {
            guard,
            server,
            stop_l3: Some(tx),
            l3: Some(l3),
            daemon,
            socket,
            dir,
            id: ObjectId::of_blob(SECRET),
        };
        let (root, _) = fx
            .guard
            .create_token("pat", "root", vec![], "", None)
            .await
            .expect("root");
        let cs = fx
            .client(Some(&root), false)
            .await
            .changeset(None)
            .put("secret.txt", SECRET.to_vec())
            .create()
            .await
            .expect("create");
        fx.client(Some(&root), false)
            .await
            .land("main", &cs.id)
            .await
            .expect("land");
        fx
    }

    async fn token(&self, user: &str, ttl: Option<Duration>) -> (String, String) {
        let (secret, rec) = self
            .guard
            .create_token("pat", user, vec![], "", ttl)
            .await
            .expect("token");
        (secret, rec.id)
    }

    async fn client(&self, token: Option<&str>, via_daemon: bool) -> Client {
        let mut b = Client::builder().endpoint(&self.server).repo("mono");
        if let Some(t) = token {
            b = b.auth(Auth::Bearer(t.to_string()));
        }
        if via_daemon {
            b = b.daemon_socket(&self.socket);
        }
        b.build().await.expect("connect")
    }

    async fn read(&self, token: Option<&str>) -> Result<(), Code> {
        self.read_via(&self.client(token, true).await).await
    }

    async fn read_via(&self, c: &Client) -> Result<(), Code> {
        match c.get_object(&self.id).await {
            Ok(b) => {
                assert_eq!(b.as_ref(), SECRET, "a successful read returns the exact bytes");
                Ok(())
            }
            Err(Error::Rpc { code, .. }) => Err(code),
            Err(e) => panic!("{e:?}"),
        }
    }

    async fn stop_l3(&mut self) {
        let _ = self.stop_l3.take().expect("l3 running").send(());
        let _ = self.l3.take().expect("l3 running").await;
    }

    fn cached_files(&self) -> usize {
        fn walk(p: &Path) -> usize {
            std::fs::read_dir(p)
                .map(|rd| {
                    rd.flatten()
                        .map(|e| if e.path().is_dir() { walk(&e.path()) } else { 1 })
                        .sum()
                })
                .unwrap_or(0)
        }
        walk(&self.dir.path().join("objects"))
    }
}

/// A valid reader that keeps the repository redaction state fresh, as in the native probe.
fn keep_fresh(fx: &Fx, token: String) -> tokio::task::JoinHandle<()> {
    let (server, socket) = (fx.server.clone(), fx.socket.clone());
    let id = fx.id;
    tokio::spawn(async move {
        let c = Client::builder()
            .endpoint(server)
            .repo("mono")
            .auth(Auth::Bearer(token))
            .daemon_socket(socket)
            .build()
            .await
            .expect("carol");
        loop {
            c.get_object(&id).await.expect("carol keeps reading");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
}

/// AC1: a valid reader gets exact bytes from the local cache.
#[tokio::test(flavor = "multi_thread")]
async fn valid_reader_hits_the_local_cache() {
    let fx = Fx::new().await;
    let (bob, _) = fx.token("bob", None).await;
    fx.read(Some(&bob)).await.expect("warm read");
    let (hits, misses) = (fx.daemon.stats().hits(), fx.daemon.stats().misses());
    fx.read(Some(&bob)).await.expect("second read");
    assert_eq!(fx.daemon.stats().hits(), hits + 1);
    assert_eq!(fx.daemon.stats().misses(), misses, "no upstream GetObjects on a hit");
}

/// AC2: an expired PAT gets no cached bytes. A valid reader still does.
#[tokio::test(flavor = "multi_thread")]
async fn expired_pat_is_refused_on_a_warm_hit() {
    let fx = Fx::new().await;
    let (bob, _) = fx.token("bob", Some(Duration::from_secs(2))).await;
    let (carol, _) = fx.token("carol", None).await;
    fx.read(Some(&bob)).await.expect("warm read");
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(fx.read(Some(&bob)).await, Err(Code::Unauthenticated));
    fx.read(Some(&carol)).await.expect("valid reader");
}

/// AC3: a revoked PAT is refused on the next read, with a valid reader keeping the redaction state fresh.
#[tokio::test(flavor = "multi_thread")]
async fn revoked_pat_is_refused_at_once() {
    let fx = Fx::new().await;
    let (bob, bob_id) = fx.token("bob", None).await;
    let (carol, _) = fx.token("carol", None).await;
    fx.read(Some(&bob)).await.expect("warm read");
    let reader = keep_fresh(&fx, carol.clone());
    assert!(fx.guard.revoke_token(&bob_id).await.expect("revoke"));
    assert_eq!(fx.read(Some(&bob)).await, Err(Code::Unauthenticated), "at 0 s");
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert_eq!(fx.read(Some(&bob)).await, Err(Code::Unauthenticated), "after 11 s");
    fx.read(Some(&carol)).await.expect("valid reader");
    assert!(!reader.is_finished(), "the valid reader never failed");
    reader.abort();
}

/// AC4: a policy change that removes read takes effect on the next read.
#[tokio::test(flavor = "multi_thread")]
async fn policy_change_is_refused_at_once() {
    let fx = Fx::new().await;
    let (bob, _) = fx.token("bob", None).await;
    let (carol, _) = fx.token("carol", None).await;
    fx.read(Some(&bob)).await.expect("warm read");
    // The server owns its own guard: change the policy through the admin RPC.
    let (root, _) = fx.token("root", None).await;
    let mut req = tonic::Request::new(scion_proto::v1::SetPolicyRequest {
        policy: POLICY.replace("\"user:bob\",", ""),
    });
    req.metadata_mut()
        .insert("authorization", format!("Bearer {root}").parse().expect("header"));
    scion_proto::v1::admin_service_client::AdminServiceClient::connect(fx.server.clone())
        .await
        .expect("admin")
        .set_policy(req)
        .await
        .expect("set policy");
    assert_eq!(fx.read(Some(&bob)).await, Err(Code::PermissionDenied));
    fx.read(Some(&carol)).await.expect("valid reader");
}

/// AC5: an unrelated PAT and an anonymous caller get nothing from a warm cache.
#[tokio::test(flavor = "multi_thread")]
async fn outsiders_get_nothing_from_a_warm_cache() {
    let fx = Fx::new().await;
    let (bob, _) = fx.token("bob", None).await;
    let (eve, _) = fx.token("eve", None).await;
    fx.read(Some(&bob)).await.expect("warm read");
    assert_eq!(fx.read(Some(&eve)).await, Err(Code::PermissionDenied));
    assert_eq!(fx.read(None).await, Err(Code::Unauthenticated));
    fx.read(Some(&bob)).await.expect("valid reader is never refused");
}

/// AC6: offline is fail-closed: no private bytes, now or later. The cached file stays on disk.
#[tokio::test(flavor = "multi_thread")]
async fn offline_reads_are_refused() {
    let mut fx = Fx::new().await;
    let (bob, _) = fx.token("bob", None).await;
    // The client connects (GetServerInfo) before the L3 stops.
    let c = fx.client(Some(&bob), true).await;
    fx.read_via(&c).await.expect("warm read");
    assert!(fx.cached_files() > 0);
    fx.stop_l3().await;
    assert_eq!(fx.read_via(&c).await, Err(Code::Unavailable), "at 0 s");
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert_eq!(fx.read_via(&c).await, Err(Code::Unavailable), "after 11 s");
    assert!(fx.cached_files() > 0, "the cache keeps the file");
}

/// AC7: one CheckAccess for each GetObjects, a hit included, and none for HasObjects.
#[tokio::test(flavor = "multi_thread")]
async fn one_access_check_for_each_get_objects() {
    let fx = Fx::new().await;
    let (bob, _) = fx.token("bob", None).await;
    let c = fx.client(Some(&bob), true).await;
    let checks = || fx.daemon.stats().access_checks();
    let base = checks();
    c.get_object(&fx.id).await.expect("miss");
    assert_eq!(checks(), base + 1);
    c.get_object(&fx.id).await.expect("hit");
    assert_eq!(checks(), base + 2);
    c.get_object(&fx.id).await.expect("hit again");
    assert_eq!(checks(), base + 3);
    c.missing_objects(&[fx.id]).await.expect("has");
    assert_eq!(checks(), base + 3, "HasObjects makes no access check");
}
