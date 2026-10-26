//! daemon 端到端：SDK 经 Unix socket 连接 daemon，写入与合入透明转发、对象读取命中本地缓存、
//! 错误原样返回、ResolveRef 登记的预取、本地 GetServerInfo、重复启动与空闲退出。
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use scion_api::{App, ServerConfig};
use scion_client::{Client, ObjectId, Role};
use scion_daemon::{DaemonConfig, DaemonError, PrefetchOptions};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

async fn l3() -> (String, oneshot::Sender<()>) {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    store.create_repo("other").await.expect("other repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            ref_poll_interval: Duration::from_millis(50),
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store,
    };
    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(scion_api::serve(listener, app, async {
        let _ = rx.await;
    }));
    (url, tx)
}

fn config(dir: &Path) -> DaemonConfig {
    DaemonConfig {
        socket: dir.join("run/daemon.sock"),
        cache_dir: dir.join("objects"),
        disk_bytes: 64 << 20,
        memory_bytes: 8 << 20,
        idle_timeout: None,
        prefetch: PrefetchOptions::default(),
        redaction_ttl: scion_daemon::REDACTION_TTL,
    }
}

struct Running {
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), DaemonError>>,
}

impl Running {
    async fn stop(mut self) {
        if let Some(s) = self.stop.take() {
            let _ = s.send(());
        }
        self.task.await.expect("join").expect("clean exit");
    }
}

async fn start(cfg: DaemonConfig) -> Running {
    let socket = cfg.socket.clone();
    let (tx, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(scion_daemon::run(cfg, async {
        let _ = rx.await;
    }));
    for _ in 0..200 {
        if tokio::net::UnixStream::connect(&socket).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Running { stop: Some(tx), task }
}

async fn via(socket: &Path, server: &str) -> Client {
    Client::builder()
        .endpoint(server)
        .repo("mono")
        .daemon_socket(socket)
        .build()
        .await
        .expect("connect via daemon")
}

async fn eventually(mut f: impl FnMut() -> bool) -> bool {
    for _ in 0..300 {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// 直接读 daemon 中的对象文件数（不经 daemon 进程，确认确实缓存到了磁盘）。
fn cached_files(dir: &Path) -> usize {
    fn walk(p: &Path) -> usize {
        std::fs::read_dir(p)
            .map(|rd| {
                rd.flatten()
                    .map(|e| if e.path().is_dir() { walk(&e.path()) } else { 1 })
                    .sum()
            })
            .unwrap_or(0)
    }
    walk(&dir.join("objects").join("objects"))
}

#[tokio::test(flavor = "multi_thread")]
async fn sdk_works_through_the_daemon() {
    let (server, _stop) = l3().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = config(dir.path());
    let socket = cfg.socket.clone();
    let daemon = start(cfg).await;

    let c = via(&socket, &server).await;
    assert!(c.via_daemon());
    // 能力协商、写入、合入都透明转发给服务端
    assert_eq!(c.server_info().role, Role::Server);
    let a = c
        .changeset(None)
        .put("src/a.rs", "fn a() {}")
        .put("big.bin", vec![9u8; 3 << 20])
        .message("a")
        .create()
        .await
        .expect("create through daemon");
    c.land("main", &a.id).await.expect("land through daemon");
    let tip = c.resolve_ref("main").await.expect("resolve").expect("main");
    assert_eq!(tip.changeset, a.id);
    assert_eq!(c.list_refs().await.expect("refs").len(), 1);
    let (log, next) = c.ref_log("main", None, 0).await.expect("ref log");
    assert_eq!((log.len(), log[0].changeset, next), (1, a.id, None));
    assert_eq!(c.log("main", None, 10, None).await.expect("log").entries.len(), 1);
    // 分支与仓库管理同样转发（RepoService 走透明代理）
    assert_eq!(c.create_ref("dev", &a.id).await.expect("create ref"), (a.id, 1));
    assert_eq!(c.update_ref("dev", &a.id, Some(1)).await.expect("update ref").1, 2);
    c.delete_ref("dev", Some(2)).await.expect("delete ref");
    c.create_repo("made-via-daemon", scion_client::NewRepo::default())
        .await
        .expect("create repo through the proxy");

    // 对象读取：第一次回源并写入本地缓存，之后即使服务端停了也能读到
    let blob = ObjectId::of_blob(b"fn a() {}");
    let big = ObjectId::of_blob(&vec![9u8; 3 << 20]);
    let got = c.get_objects(&[blob, big, blob]).await.expect("objects");
    assert_eq!(got[0].as_ref(), b"fn a() {}");
    assert_eq!(got[1].len(), 3 << 20, "multi-chunk object reassembled");
    assert_eq!(cached_files(dir.path()), 2);
    // 错误原样返回（保留 reason）
    let err = c.get_object(&ObjectId::of_blob(b"nope")).await.expect_err("missing");
    assert_eq!(err.reason(), Some(scion_proto::v1::ErrorReason::ObjectMissing));
    let missing = c
        .missing_objects(&[blob, ObjectId::of_blob(b"nope")])
        .await
        .expect("has");
    assert_eq!(missing, [ObjectId::of_blob(b"nope")]);
    // WatchRefs 也经 daemon 转发
    let mut w = c.watch_refs(&["main"]).await.expect("watch");
    assert_eq!(w.next().await.expect("snapshot").changeset, a.id);
    drop(w);

    // 服务端停掉：对象仍从本地读到
    drop(_stop);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        c.get_object(&blob).await.expect("served locally").as_ref(),
        b"fn a() {}"
    );
    daemon.stop().await;
    assert!(!socket.exists(), "socket removed on exit");
    assert!(!scion_daemon::pid_file(&socket).exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn resolved_refs_are_prefetched_when_they_advance() {
    let (server, _stop) = l3().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = config(dir.path());
    let socket = cfg.socket.clone();
    let daemon = start(cfg).await;
    let direct = Client::builder()
        .endpoint(&server)
        .repo("mono")
        .build()
        .await
        .expect("direct");
    let a = direct.changeset(None).put("src/a.rs", "a").create().await.expect("a");
    direct.land("main", &a.id).await.expect("land a");

    // 工作副本经 daemon 解析 main：登记预取
    let c = via(&socket, &server).await;
    c.resolve_ref("main").await.expect("resolve");
    // 检出时经 daemon 读过对象：读权限已确认，之后服务端不可达也能离线读本地缓存
    c.get_object(&a.root_tree).await.expect("checkout read");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = cached_files(dir.path());

    // 别人合入：daemon 预取新的目录树和小文件
    let b = direct
        .changeset(Some(a.id))
        .put("src/b.rs", "fn b() {}")
        .message("b")
        .create()
        .await
        .expect("b");
    direct.land("main", &b.id).await.expect("land b");
    let d = dir.path().to_path_buf();
    assert!(
        eventually(|| cached_files(&d) >= before + 3).await,
        "root, src and b.rs prefetched ({} files)",
        cached_files(&d)
    );
    // 之后读取直接命中，不需要服务端
    drop(_stop);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(c.get_object(&b.root_tree).await.is_ok());
    assert_eq!(
        c.get_object(&ObjectId::of_blob(b"fn b() {}"))
            .await
            .expect("blob")
            .as_ref(),
        b"fn b() {}"
    );
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn local_info_duplicate_start_and_idle_exit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cfg = config(dir.path());
    let socket: PathBuf = cfg.socket.clone();
    let daemon = start(cfg.clone()).await;

    // 不带服务端地址的 GetServerInfo：daemon 自己应答
    let channel =
        tonic::transport::Endpoint::from_static("http://daemon")
            .connect_with_connector(tower::service_fn(move |_| {
                let s = socket.clone();
                async move {
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(tokio::net::UnixStream::connect(s).await?))
                }
            }))
            .await
            .expect("uds");
    let info = scion_proto::v1::server_info_service_client::ServerInfoServiceClient::new(channel.clone())
        .get_server_info(scion_proto::v1::GetServerInfoRequest::default())
        .await
        .expect("info")
        .into_inner()
        .info
        .expect("info");
    assert_eq!(info.role, scion_proto::v1::ServerRole::Daemon as i32);
    assert_eq!(info.server_version, scion_daemon::DAEMON_VERSION);
    // 其它不带服务端地址的请求被拒绝
    let err = scion_proto::v1::ref_service_client::RefServiceClient::new(channel.clone())
        .list_refs(scion_proto::v1::ListRefsRequest { repo: "mono".into() })
        .await
        .expect_err("no upstream");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    let err = scion_proto::v1::query_service_client::QueryServiceClient::new(channel)
        .stat(scion_proto::v1::StatRequest::default())
        .await
        .expect_err("no upstream");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    // 已有 daemon 在跑：第二个拒绝启动
    let second = scion_daemon::run(cfg.clone(), std::future::pending()).await;
    assert!(matches!(second, Err(DaemonError::AlreadyRunning(_))), "{second:?}");
    daemon.stop().await;

    // 残留的 socket 文件（上次异常退出）：清理后正常启动；空闲超时后自动退出
    std::fs::create_dir_all(cfg.socket.parent().expect("parent")).expect("dir");
    std::fs::write(&cfg.socket, b"").expect("stale");
    cfg.idle_timeout = Some(Duration::from_millis(50));
    tokio::time::timeout(Duration::from_secs(10), scion_daemon::run(cfg, std::future::pending()))
        .await
        .expect("idle exit")
        .expect("clean");
}

/// 与服务端一致：本地缓存里有的对象，只交给索引中有它的仓库。
#[tokio::test(flavor = "multi_thread")]
async fn cached_objects_are_not_visible_across_repositories() {
    let (server, _stop) = l3().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = config(dir.path());
    let socket = cfg.socket.clone();
    let daemon = start(cfg).await;
    let mono = via(&socket, &server).await;
    let cs = mono
        .changeset(None)
        .put("secret.txt", "only in mono")
        .create()
        .await
        .expect("create");
    let id = ObjectId::of_blob(b"only in mono");
    mono.get_object(&id).await.expect("visible in mono");
    assert!(mono.get_object(&cs.root_tree).await.is_ok());

    let other = mono.with_repo("other");
    let direct = Client::builder()
        .endpoint(&server)
        .repo("other")
        .build()
        .await
        .expect("direct");
    let via_daemon = other.get_object(&id).await.expect_err("not visible in other");
    let from_server = direct.get_object(&id).await.expect_err("server agrees");
    assert_eq!(via_daemon.reason(), from_server.reason());
    assert_eq!(via_daemon.reason(), Some(scion_proto::v1::ErrorReason::ObjectMissing));
    // 并发读同一对象的两个仓库各自得到自己的结果
    let (a, b) = tokio::join!(other.get_object(&id), mono.get_object(&id));
    assert!(a.is_err() && b.is_ok());

    // 对象进入 other 的索引后可以读到
    direct
        .changeset(None)
        .put("copy.txt", "only in mono")
        .create()
        .await
        .expect("upload");
    assert_eq!(
        other.get_object(&id).await.expect("now visible").as_ref(),
        b"only in mono"
    );
    daemon.stop().await;
}

/// 服务端要求认证：本地缓存里有的对象，也只交给有读权限的调用者；无权限的拒绝不受离线宽限影响。
#[tokio::test(flavor = "multi_thread")]
async fn cached_objects_require_read_access() {
    use scion_api::scion_auth::{Guard, GuardConfig};
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let auth = GuardConfig {
        required: true,
        ..GuardConfig::default()
    };
    let guard = Guard::build(auth.clone(), store.clone(), Arc::new(|_: &str| None)).expect("guard");
    guard
        .set_policy("[[rule]]\nsubjects=[\"user:bob\"]\nrepos=[\"mono\"]\nactions=[\"land\"]\n")
        .await
        .expect("policy");
    let (bob, _) = guard.create_token("pat", "bob", vec![], "", None).await.expect("bob");
    let (eve, _) = guard.create_token("pat", "eve", vec![], "", None).await.expect("eve");
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
    let (_stop, rx) = oneshot::channel::<()>();
    tokio::spawn(scion_api::serve(listener, app, async {
        let _ = rx.await;
    }));
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = config(dir.path());
    let socket = cfg.socket.clone();
    let daemon = start(cfg).await;
    let as_user = |token: String| {
        let (server, socket) = (server.clone(), socket.clone());
        async move {
            Client::builder()
                .endpoint(server)
                .repo("mono")
                .auth(scion_client::Auth::Bearer(token))
                .daemon_socket(socket)
                .build()
                .await
                .expect("connect")
        }
    };
    let b = as_user(bob).await;
    let cs = b
        .changeset(None)
        .put("secret.txt", "s3cret")
        .create()
        .await
        .expect("create");
    let id = ObjectId::of_blob(b"s3cret");
    b.get_object(&id).await.expect("bob reads");
    let e = as_user(eve).await.get_object(&id).await.expect_err("eve has no access");
    assert_eq!(e.reason(), Some(scion_proto::v1::ErrorReason::PermissionDenied));
    assert!(b.get_object(&cs.root_tree).await.is_ok());
    daemon.stop().await;
}

/// redaction：daemon 用调用者的身份同步，redact 过的对象返回 REDACTED 并删除本地副本；撤销后恢复。
#[tokio::test(flavor = "multi_thread")]
async fn redacted_objects_are_refused_by_the_daemon() {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let server = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store.clone(),
    };
    let (stop_l3, rx) = oneshot::channel::<()>();
    let l3 = tokio::spawn(scion_api::serve(listener, app, async {
        let _ = rx.await;
    }));
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = DaemonConfig {
        redaction_ttl: Duration::ZERO,
        ..config(dir.path())
    };
    let socket = cfg.socket.clone();
    let daemon = start(cfg).await;
    let c = via(&socket, &server).await;
    let a = c
        .changeset(None)
        .put("secret.txt", "password=hunter2")
        .put("a.txt", "a")
        .create()
        .await
        .expect("create");
    c.land("main", &a.id).await.expect("land");
    let secret = ObjectId::of_blob(b"password=hunter2");
    assert_eq!(
        c.get_object(&secret).await.expect("fetch").as_ref(),
        b"password=hunter2"
    );
    assert!(eventually(|| cached_files(dir.path()) >= 1).await);
    let before = cached_files(dir.path());

    let red = scion_api::redaction::Redactor::new(store.clone(), store.clone());
    let (rid, _) = red
        .redact("mono", &[secret], &[], "leak", "root", false, 1)
        .await
        .expect("redact");
    let err = c.get_object(&secret).await.expect_err("redacted");
    assert!(err.is_redacted(), "{err:?}");
    assert_eq!(cached_files(dir.path()), before - 1, "local copy removed");
    assert_eq!(
        c.get_object(&ObjectId::of_blob(b"a")).await.expect("other").as_ref(),
        b"a"
    );
    // 经由 daemon 转给服务端
    assert_eq!(c.list_redacted_objects(0).await.expect("feed").objects.len(), 1);

    red.unredact(&rid).await.expect("unredact");
    assert_eq!(
        c.get_object(&secret).await.expect("restored").as_ref(),
        b"password=hunter2"
    );

    // 服务端不可达：无法确认 redaction，本地有副本也拒绝读取
    let _ = stop_l3.send(());
    let _ = l3.await;
    let err = c.get_object(&ObjectId::of_blob(b"a")).await.expect_err("unverified");
    assert!(
        matches!(
            &err,
            scion_client::Error::Rpc {
                code: tonic::Code::Unavailable,
                ..
            }
        ),
        "{err:?}"
    );
    drop(c);
    daemon.stop().await;
}
