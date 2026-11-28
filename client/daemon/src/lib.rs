//! `scn daemon`：节点 L1 缓存（客户端设计文档 §7）。
//!
//! 监听本机的 Unix socket。SDK 经 daemon 连接时，每个请求都带着真正的服务端地址
//! （[`scion_client::UPSTREAM_HEADER`]），daemon：
//! - `ObjectService.GetObjects`：本地缓存（磁盘 + 内存）命中就地应答，未命中按调用者身份回源（经服务端
//!   推荐的区域缓存），同一对象的并发未命中只回源一次；
//! - `RefService`：转发（ref 永远不缓存），`ResolveRef` 同时登记 WatchRefs 预取；
//! - 其它全部 RPC：在 HTTP/2 层原样转发，daemon 不需要理解它们。
//!
//! daemon 不持有凭证：每个请求带着调用者自己的 token，原样转给服务端。每次 `GetObjects` 先用该 token 向服务端
//! `CheckAccess`，本地命中也一样，不缓存结果，离线时拒绝读取（ent ADR 0004）。可见性与服务端一致：本地缓存
//! 按对象 id 去重，但只把对象交给索引中有它的仓库（见 `objects.rs`）。

mod cas;
#[cfg(test)]
mod no_auth_service_tests;
mod objects;
mod prefetch;
mod refs;
mod upstream;

use std::convert::Infallible;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use scion_client::UPSTREAM_HEADER;
use scion_proto::v1::object_service_server::ObjectServiceServer;
use scion_proto::v1::ref_service_server::RefServiceServer;
use scion_proto::v1::server_info_service_server::{ServerInfoService, ServerInfoServiceServer};
use scion_proto::v1::{self, GetServerInfoRequest, GetServerInfoResponse};
use tower::{Service, ServiceExt};

pub use cas::Cas;
pub use objects::{DaemonObjectSvc, REDACTION_TTL};
pub use prefetch::{PrefetchOptions, Prefetcher};
pub use refs::DaemonRefSvc;
pub use upstream::{Upstream, Upstreams};

/// 命中统计。
#[derive(Debug, Default)]
pub struct Stats {
    hits: AtomicU64,
    misses: AtomicU64,
    access_checks: AtomicU64,
}

impl Stats {
    fn hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }
    fn miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn access_check(&self) {
        self.access_checks.fetch_add(1, Ordering::Relaxed);
    }
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
    /// CheckAccess requests sent to the upstream.
    pub fn access_checks(&self) -> u64 {
        self.access_checks.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfig {
    pub socket: PathBuf,
    pub cache_dir: PathBuf,
    /// 磁盘缓存上限。
    pub disk_bytes: u64,
    /// 内存缓存大小。
    pub memory_bytes: u64,
    /// 没有请求多久后退出；`None` 一直运行。
    pub idle_timeout: Option<Duration>,
    pub prefetch: PrefetchOptions,
    /// 两次向服务端同步 redaction 的最长间隔（redact 之后 daemon 最多还返回该对象这么久）。
    pub redaction_ttl: Duration,
}

impl DaemonConfig {
    /// The default `(socket, cache_dir)` from `env`. Only an absolute root counts: a relative one
    /// would mean the current directory. A path is `None` when no root qualifies.
    ///
    /// Linux: cache `$XDG_CACHE_HOME/scion/objects`, else `$HOME/.cache/scion/objects`;
    /// socket `$XDG_RUNTIME_DIR/scion/daemon.sock`, else `daemon.sock` in the cache base.
    /// macOS: both under `$HOME/Library/Caches/scion`.
    pub fn default_locations(env: impl Fn(&str) -> Option<String>, macos: bool) -> (Option<PathBuf>, Option<PathBuf>) {
        let root = |k: &str| env(k).map(PathBuf::from).filter(|p| p.is_absolute());
        let (runtime, cache) = if macos {
            let d = root("HOME").map(|h| h.join("Library/Caches/scion"));
            (d.clone(), d)
        } else {
            let cache = root("XDG_CACHE_HOME")
                .or_else(|| root("HOME").map(|h| h.join(".cache")))
                .map(|d| d.join("scion"));
            let runtime = root("XDG_RUNTIME_DIR")
                .map(|d| d.join("scion"))
                .or_else(|| cache.clone());
            (runtime, cache)
        };
        (runtime.map(|d| d.join("daemon.sock")), cache.map(|d| d.join("objects")))
    }

    /// The default limits at the given paths.
    pub fn at(socket: PathBuf, cache_dir: PathBuf) -> Self {
        Self {
            socket,
            cache_dir,
            disk_bytes: 20 << 30,
            memory_bytes: 256 << 20,
            idle_timeout: Some(Duration::from_secs(30 * 60)),
            prefetch: PrefetchOptions::default(),
            redaction_ttl: objects::REDACTION_TTL,
        }
    }
}

/// daemon 的全部状态。
pub struct Daemon {
    pub(crate) cas: Arc<Cas>,
    pub(crate) upstreams: Upstreams,
    pub(crate) flight: objects::Flight,
    pub(crate) visible: objects::Visible,
    pub(crate) redactions: objects::Redactions,
    pub(crate) redaction_ttl: Duration,
    pub(crate) prefetch: Prefetcher,
    pub(crate) stats: Stats,
    /// 最近一次请求的时间（Unix 秒）。
    last_activity: AtomicU64,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl Daemon {
    pub fn new(cas: Arc<Cas>, prefetch: PrefetchOptions, redaction_ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            cas,
            upstreams: Upstreams::default(),
            flight: objects::Flight::new(),
            visible: objects::new_visible_cache(),
            redactions: objects::new_redaction_cache(),
            redaction_ttl,
            prefetch: Prefetcher::new(prefetch),
            stats: Stats::default(),
            last_activity: AtomicU64::new(now_secs()),
        })
    }

    pub(crate) fn touch(&self) {
        self.last_activity.store(now_secs(), Ordering::Relaxed);
    }

    /// 距最近一次请求的时间。
    pub fn idle_for(&self) -> Duration {
        Duration::from_secs(now_secs().saturating_sub(self.last_activity.load(Ordering::Relaxed)))
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    pub fn cas(&self) -> &Cas {
        &self.cas
    }

    pub fn prefetcher(&self) -> &Prefetcher {
        &self.prefetch
    }
}

/// 不带服务端地址的 `GetServerInfo`：daemon 自己应答（`scn daemon status` 用）。
struct LocalInfo;

/// daemon 应答 `GetServerInfo` 时的版本字符串。
pub const DAEMON_VERSION: &str = concat!("scn-daemon ", env!("CARGO_PKG_VERSION"));

#[tonic::async_trait]
impl ServerInfoService for LocalInfo {
    async fn get_server_info(
        &self,
        _request: tonic::Request<GetServerInfoRequest>,
    ) -> Result<tonic::Response<GetServerInfoResponse>, tonic::Status> {
        Ok(tonic::Response::new(GetServerInfoResponse {
            info: Some(v1::ServerInfo {
                protocol_version: scion_proto::PROTOCOL_VERSION.into(),
                features: vec![scion_proto::features::OBJECTS_GET.into()],
                limits: Some(v1::Limits {
                    max_batch_objects: 1000,
                    max_chunk_bytes: 1 << 20,
                    ..Default::default()
                }),
                hash_algo: v1::HashAlgo::Blake3256.into(),
                server_version: DAEMON_VERSION.into(),
                role: v1::ServerRole::Daemon.into(),
                read_endpoints: Vec::new(),
            }),
        }))
    }
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// 其余请求：带服务端地址的原样转发；不带的只有 `GetServerInfo` 由 daemon 自己应答。
#[derive(Clone)]
struct Proxy {
    daemon: Arc<Daemon>,
    local: ServerInfoServiceServer<LocalInfo>,
}

impl Service<http::Request<axum::body::Body>> for Proxy {
    type Response = http::Response<axum::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<axum::body::Body>) -> Self::Future {
        let this = self.clone();
        Box::pin(async move { Ok(this.forward(req).await) })
    }
}

fn status_response(s: tonic::Status) -> http::Response<axum::body::Body> {
    s.into_http::<axum::body::Body>()
}

impl Proxy {
    async fn forward(self, req: http::Request<axum::body::Body>) -> http::Response<axum::body::Body> {
        self.daemon.touch();
        let upstream = req
            .headers()
            .get(UPSTREAM_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let Some(url) = upstream else {
            if req.uri().path() == "/scion.v1.ServerInfoService/GetServerInfo" {
                let mut local = self.local.clone();
                return match local.call(req.map(tonic::body::Body::new)).await {
                    Ok(r) => r.map(axum::body::Body::new),
                    Err(e) => match e {},
                };
            }
            return status_response(tonic::Status::failed_precondition(format!(
                "missing {UPSTREAM_HEADER}; connect with the Scion SDK"
            )));
        };
        let up = match self.daemon.upstreams.get(&url) {
            Ok(u) => u,
            Err(s) => return status_response(s),
        };
        let (mut parts, body) = req.into_parts();
        parts.headers.remove(UPSTREAM_HEADER);
        let req = http::Request::from_parts(parts, tonic::body::Body::new(body));
        let mut ch = up.raw.clone();
        match ch.ready().await {
            Ok(ch) => match ch.call(req).await {
                Ok(resp) => resp.map(axum::body::Body::new),
                Err(e) => status_response(tonic::Status::unavailable(format!("forwarding to {url}: {e}"))),
            },
            Err(e) => status_response(tonic::Status::unavailable(format!("connecting to {url}: {e}"))),
        }
    }
}

/// 组装 daemon 的 gRPC 路由。
pub fn routes(daemon: Arc<Daemon>) -> tonic::service::Routes {
    let proxy = Proxy {
        daemon: daemon.clone(),
        local: ServerInfoServiceServer::new(LocalInfo),
    };
    let router = tonic::service::Routes::new(
        ObjectServiceServer::new(DaemonObjectSvc::new(daemon.clone())).max_encoding_message_size(64 << 20),
    )
    .add_service(RefServiceServer::new(DaemonRefSvc::new(daemon)))
    .into_axum_router()
    .fallback_service(proxy);
    tonic::service::Routes::from(router)
}

#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    #[error("another daemon is already listening on {0}")]
    AlreadyRunning(PathBuf),
    #[error("{0}: {1}")]
    Io(String, std::io::Error),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

/// pid 文件：socket 旁边的 `daemon.pid`。
pub fn pid_file(socket: &Path) -> PathBuf {
    socket.with_extension("pid")
}

/// 运行 daemon，直到 `shutdown` 完成或空闲超时。
#[cfg(unix)]
pub async fn run(config: DaemonConfig, shutdown: impl Future<Output = ()> + Send) -> Result<(), DaemonError> {
    use std::os::unix::fs::PermissionsExt;

    let io = |what: &str, p: &Path| {
        let label = format!("{what} {}", p.display());
        move |e| DaemonError::Io(label, e)
    };
    if let Some(dir) = config.socket.parent() {
        std::fs::create_dir_all(dir).map_err(io("create", dir))?;
    }
    if config.socket.exists() {
        // 还有 daemon 在应答：不抢；否则是上次异常退出留下的 socket
        if tokio::net::UnixStream::connect(&config.socket).await.is_ok() {
            return Err(DaemonError::AlreadyRunning(config.socket.clone()));
        }
        std::fs::remove_file(&config.socket).map_err(io("remove stale socket", &config.socket))?;
    }
    let cas = Cas::open(&config.cache_dir, config.disk_bytes, config.memory_bytes)
        .map_err(io("open cache", &config.cache_dir))?;
    let listener = tokio::net::UnixListener::bind(&config.socket).map_err(io("listen on", &config.socket))?;
    // 只有本用户能连接
    std::fs::set_permissions(&config.socket, std::fs::Permissions::from_mode(0o600))
        .map_err(io("chmod", &config.socket))?;
    let pid = pid_file(&config.socket);
    std::fs::write(&pid, std::process::id().to_string()).map_err(io("write", &pid))?;

    let daemon = Daemon::new(cas, config.prefetch, config.redaction_ttl);
    tracing::info!(socket = %config.socket.display(), cache = %config.cache_dir.display(), "scn daemon listening");
    let idle = idle_watch(daemon.clone(), config.idle_timeout);
    let result = tonic::transport::Server::builder()
        .add_routes(routes(daemon.clone()))
        .serve_with_incoming_shutdown(tokio_stream::wrappers::UnixListenerStream::new(listener), async {
            tokio::select! {
                () = shutdown => {},
                () = idle => tracing::info!("idle; exiting"),
            }
        })
        .await;
    daemon.prefetch.stop();
    let _ = std::fs::remove_file(&config.socket);
    let _ = std::fs::remove_file(&pid);
    result.map_err(DaemonError::from)
}

/// 空闲超时后完成；同时定期取消空闲的预取订阅。
async fn idle_watch(daemon: Arc<Daemon>, timeout: Option<Duration>) {
    let check = timeout.map_or(Duration::from_secs(60), |t| {
        (t / 4).clamp(Duration::from_millis(10), Duration::from_secs(60))
    });
    loop {
        tokio::time::sleep(check).await;
        daemon.prefetch.reap();
        if let Some(t) = timeout
            && daemon.idle_for() >= t
        {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_path_matrix() {
        let env = |vars: Vec<(&'static str, &'static str)>| {
            move |k: &str| vars.iter().find(|(n, _)| *n == k).map(|(_, v)| (*v).to_string())
        };
        let p = |s: &str| Some(PathBuf::from(s));
        let loc = |vars: Vec<(&'static str, &'static str)>, mac| DaemonConfig::default_locations(env(vars), mac);
        // macOS ignores XDG and needs an absolute HOME
        let mac = loc(
            vec![
                ("HOME", "/Users/a"),
                ("XDG_CACHE_HOME", "/c"),
                ("XDG_RUNTIME_DIR", "/r"),
            ],
            true,
        );
        assert_eq!(
            mac,
            (
                p("/Users/a/Library/Caches/scion/daemon.sock"),
                p("/Users/a/Library/Caches/scion/objects")
            )
        );
        for h in ["", ".", "rel"] {
            assert_eq!(loc(vec![("HOME", h), ("XDG_CACHE_HOME", "/c")], true), (None, None));
        }
        assert_eq!(loc(vec![], true), (None, None));
        // Linux: both valid
        let both = loc(
            vec![("HOME", "/h"), ("XDG_RUNTIME_DIR", "/run/1"), ("XDG_CACHE_HOME", "/c")],
            false,
        );
        assert_eq!(both, (p("/run/1/scion/daemon.sock"), p("/c/scion/objects")));
        // HOME only: the socket goes to the cache base
        let home = loc(vec![("HOME", "/h")], false);
        assert_eq!(home, (p("/h/.cache/scion/daemon.sock"), p("/h/.cache/scion/objects")));
        // each invalid class at each root, with the other root valid and invalid
        for bad in ["", ".", "rel"] {
            assert_eq!(
                loc(
                    vec![("HOME", "/h"), ("XDG_CACHE_HOME", bad), ("XDG_RUNTIME_DIR", bad)],
                    false
                ),
                home
            );
            assert_eq!(
                loc(vec![("HOME", bad), ("XDG_CACHE_HOME", "/c")], false),
                (p("/c/scion/daemon.sock"), p("/c/scion/objects"))
            );
            assert_eq!(
                loc(vec![("HOME", bad), ("XDG_RUNTIME_DIR", "/r")], false),
                (p("/r/scion/daemon.sock"), None)
            );
            assert_eq!(
                loc(
                    vec![("HOME", bad), ("XDG_CACHE_HOME", bad), ("XDG_RUNTIME_DIR", bad)],
                    false
                ),
                (None, None)
            );
        }
        assert_eq!(loc(vec![], false), (None, None));
        assert_eq!(
            pid_file(Path::new("/h/.cache/scion/daemon.sock")),
            PathBuf::from("/h/.cache/scion/daemon.pid")
        );
        assert!(DaemonConfig::at("/s".into(), "/c".into()).idle_timeout.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn idle_watch_fires_after_timeout() {
        let dir = tempfile::tempdir().expect("tempdir");
        let d = Daemon::new(
            Cas::open(dir.path(), 1 << 20, 0).expect("cas"),
            PrefetchOptions::default(),
            objects::REDACTION_TTL,
        );
        d.last_activity.store(0, Ordering::Relaxed);
        tokio::time::timeout(
            Duration::from_secs(5),
            idle_watch(d.clone(), Some(Duration::from_secs(1))),
        )
        .await
        .expect("fires");
        assert!(d.idle_for() > Duration::from_secs(1));
        d.touch();
        assert!(d.idle_for() < Duration::from_secs(5));
        // 不设超时：一直运行
        assert!(
            tokio::time::timeout(Duration::from_secs(120), idle_watch(d.clone(), None))
                .await
                .is_err()
        );
        assert_eq!((d.stats().hits(), d.stats().misses()), (0, 0));
        assert!(d.prefetcher().is_empty());
        assert_eq!(d.cas().used(), 0);
    }
}
