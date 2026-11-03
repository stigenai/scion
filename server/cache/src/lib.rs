//! `scion cache`：区域对象缓存（L2，服务端设计文档 §7.2）。
//!
//! 对外提供与 L3 完全相同的 `ObjectService`。对象按内容寻址、不可变，缓存永不失效；ref 从不缓存。
//! 本层没有持久状态：节点挂掉只损失命中率。
//!
//! - 存储：foyer 混合缓存（内存 + 可选 NVMe 目录），[`ObjectCache`]；
//! - 分片：多个缓存节点按 rendezvous 哈希分担对象，未命中先交给负责的节点，整个区域对同一对象只
//!   回源一次，[`Ring`]、[`CacheNode`]；
//! - 合并回源：同一对象的并发未命中只回源一次（single-flight），一批未命中合成一次请求；
//! - 对冲与熔断：多个上游副本，慢请求对冲、故障上游熔断，[`UpstreamPool`]；
//! - 预热：订阅上游的 WatchRefs，ref 前进时预取变化的目录树，[`warm`]。

mod breaker;
mod latency;
mod node;
mod ring;
mod service;
mod store;
mod upstream;
pub mod warm;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use scion_proto::v1::object_service_server::ObjectServiceServer;
use scion_proto::v1::server_info_service_client::ServerInfoServiceClient;
use scion_proto::v1::server_info_service_server::ServerInfoServiceServer;
use scion_proto::v1::{self, GetServerInfoRequest};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint};

pub use breaker::Breaker;
pub use latency::Latency;
pub use node::{CacheNode, DEFAULT_VISIBLE_ENTRIES, REDACTION_TTL, Stats, StatsSnapshot, object_missing};
pub use ring::Ring;
pub use service::{CacheInfoSvc, CacheObjectSvc};
pub use store::{CacheKey, DiskConfig, ObjectCache, StorageConfig};
pub use upstream::{
    CACHE_FORWARDED_HEADER, GrpcUpstream, PoolConfig, RequestCtx, Upstream, UpstreamPool, is_upstream_failure,
};
pub use warm::{WarmOptions, WarmTarget};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheConfig {
    /// 上游 L3 服务端地址（同一集群的多个节点，互为副本）。
    pub upstreams: Vec<String>,
    /// 本节点供其它缓存节点访问的地址；与 `peers` 一起启用分片。
    pub advertise_url: Option<String>,
    /// 区域内全部缓存节点的地址（可以包含本节点）。为空时单节点运行。
    pub peers: Vec<String>,
    pub storage: StorageConfig,
    pub pool: PoolConfig,
    /// 预热的仓库与 ref。
    pub warm: Vec<WarmTarget>,
    pub warm_options: WarmOptions,
    /// 统计日志的间隔；`None` 不输出。
    pub stats_interval: Option<Duration>,
    /// 两次向上游同步 redaction 的最长间隔（redact 之后缓存最多还返回该对象这么久）。
    pub redaction_ttl: Duration,
    /// How long a GetObjects stream stays open. It then ends with UNAVAILABLE and the client
    /// reconnects. `None` or zero sets no limit.
    pub max_stream_lifetime: Option<Duration>,
    /// How many streams one caller (by hash of the authorization header) keeps open. `None` sets no limit.
    pub max_streams_per_tenant: Option<usize>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            upstreams: Vec::new(),
            advertise_url: None,
            peers: Vec::new(),
            storage: StorageConfig::default(),
            pool: PoolConfig::default(),
            warm: Vec::new(),
            warm_options: WarmOptions::default(),
            stats_interval: Some(Duration::from_secs(60)),
            redaction_ttl: node::REDACTION_TTL,
            max_stream_lifetime: Some(Duration::from_secs(3600)),
            max_streams_per_tenant: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("cannot reach any upstream: {0}")]
    Upstream(String),
    #[error("cache storage: {0}")]
    Storage(String),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

fn channel(url: &str) -> Result<Channel, CacheError> {
    Ok(Endpoint::from_shared(url.to_owned())
        .map_err(|e| CacheError::Config(format!("invalid address {url:?}: {e}")))?
        .connect_timeout(Duration::from_secs(2))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_while_idle(true)
        .tcp_nodelay(true)
        .connect_lazy())
}

/// 从任一上游取 ServerInfo（限额等）。全部不可达时报错。
async fn upstream_info(channels: &[(String, Channel)]) -> Result<v1::ServerInfo, CacheError> {
    let mut last = String::from("no upstream configured");
    for (url, ch) in channels {
        let req = GetServerInfoRequest {
            client_name: "scion-cache".into(),
            client_version: env!("CARGO_PKG_VERSION").into(),
            protocol_version: scion_proto::PROTOCOL_VERSION.into(),
        };
        match ServerInfoServiceClient::new(ch.clone()).get_server_info(req).await {
            Ok(r) => {
                if let Some(info) = r.into_inner().info {
                    return Ok(info);
                }
                last = format!("{url}: empty server info");
            }
            Err(e) => last = format!("{url}: {}", e.message()),
        }
    }
    Err(CacheError::Upstream(last))
}

/// 按配置组装节点（连接上游与其它缓存节点、打开本地存储）。返回节点、上游 ServerInfo 与上游连接。
pub async fn build(config: &CacheConfig) -> Result<(Arc<CacheNode>, v1::ServerInfo, Vec<Channel>), CacheError> {
    if config.upstreams.is_empty() {
        return Err(CacheError::Config("at least one upstream is required".into()));
    }
    let channels = config
        .upstreams
        .iter()
        .map(|u| Ok((u.trim_end_matches('/').to_owned(), channel(u)?)))
        .collect::<Result<Vec<_>, CacheError>>()?;
    let info = upstream_info(&channels).await?;
    let max_batch = info.limits.map(|l| l.max_batch_objects as usize).unwrap_or(1000).max(1);
    let upstreams: Vec<Arc<dyn Upstream>> = channels
        .iter()
        .map(|(url, ch)| Arc::new(GrpcUpstream::new(url.clone(), ch.clone(), max_batch)) as Arc<dyn Upstream>)
        .collect();
    let me = config
        .advertise_url
        .as_ref()
        .map(|u| u.trim_end_matches('/').to_owned());
    let mut nodes: Vec<String> = config
        .peers
        .iter()
        .map(|p| p.trim_end_matches('/').to_owned())
        .collect();
    if !nodes.is_empty() {
        match &me {
            Some(m) => nodes.push(m.clone()),
            None => return Err(CacheError::Config("--peer requires --advertise-url".into())),
        }
    }
    let ring = Ring::new(nodes);
    let mut peers: HashMap<String, Arc<dyn Upstream>> = HashMap::new();
    for n in ring.nodes() {
        if Some(n) != me.as_ref() {
            peers.insert(
                n.clone(),
                Arc::new(GrpcUpstream::new(n.clone(), channel(n)?, max_batch).forwarded()),
            );
        }
    }
    let store = ObjectCache::open(&config.storage).await.map_err(CacheError::Storage)?;
    let node = Arc::new(
        CacheNode::new(store, ring, me, peers, UpstreamPool::new(upstreams, config.pool))
            .with_redaction_ttl(config.redaction_ttl),
    );
    Ok((node, info, channels.into_iter().map(|(_, c)| c).collect()))
}

async fn log_stats(node: Arc<CacheNode>, every: Duration) {
    let mut tick = tokio::time::interval(every);
    tick.tick().await;
    loop {
        tick.tick().await;
        let s = node.stats();
        tracing::info!(
            hits = s.hits,
            misses = s.misses,
            hit_ratio = format!("{:.4}", s.hit_ratio()),
            peer_objects = s.peer_objects,
            upstream_objects = s.upstream_objects,
            upstream_requests = s.upstream_requests,
            "cache stats"
        );
    }
}

/// 在给定监听器上运行缓存节点，直到 `shutdown` 完成。
pub async fn serve(
    listener: TcpListener,
    config: CacheConfig,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), CacheError> {
    let (node, info, channels) = build(&config).await?;
    serve_node(listener, node, info, channels, &config, shutdown).await
}

/// 把节点的统计注册为 Prometheus 指标（抓取时读取）。返回值被释放后不再输出。
pub fn register_stats(node: &Arc<CacheNode>) -> Vec<Arc<scion_obs::metrics::SampleFn>> {
    use scion_obs::{Sample, SampleKind, metrics};
    let sampled = |f: fn(&StatsSnapshot) -> Vec<Sample>| -> Arc<scion_obs::metrics::SampleFn> {
        let weak = Arc::downgrade(node);
        Arc::new(move || weak.upgrade().map(|n| f(&n.stats())).unwrap_or_default())
    };
    let sources = [
        (
            "scion_l2_lookups",
            "Object lookups in the regional cache, by result",
            sampled(|s| {
                vec![
                    Sample::new(&[("result", "hit")], s.hits as f64),
                    Sample::new(&[("result", "miss")], s.misses as f64),
                ]
            }),
        ),
        (
            "scion_l2_fetched_objects",
            "Objects the regional cache fetched, by source",
            sampled(|s| {
                vec![
                    Sample::new(&[("from", "peer")], s.peer_objects as f64),
                    Sample::new(&[("from", "upstream")], s.upstream_objects as f64),
                ]
            }),
        ),
        (
            "scion_l2_upstream_requests",
            "Requests the regional cache sent to the servers",
            sampled(|s| vec![Sample::new(&[], s.upstream_requests as f64)]),
        ),
        (
            "scion_l2_checks",
            "Visibility and access checks sent to the servers",
            sampled(|s| {
                vec![
                    Sample::new(&[("kind", "visibility")], s.visibility_checks as f64),
                    Sample::new(&[("kind", "access")], s.access_checks as f64),
                ]
            }),
        ),
    ];
    sources
        .into_iter()
        .map(|(name, help, f)| {
            metrics().register_sampled(name, help, SampleKind::Counter, Arc::downgrade(&f));
            f
        })
        .collect()
}

/// 用已组装好的节点提供服务（测试与基准测试可以先拿到节点再启动）。
pub async fn serve_node(
    listener: TcpListener,
    node: Arc<CacheNode>,
    info: v1::ServerInfo,
    channels: Vec<Channel>,
    config: &CacheConfig,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), CacheError> {
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mut tasks = Vec::new();
    if let Some(ch) = channels.first() {
        for t in &config.warm {
            tasks.push(tokio::spawn(warm::follow(
                node.clone(),
                ch.clone(),
                t.clone(),
                config.warm_options.clone(),
                stop_rx.clone(),
            )));
        }
    }
    if let Some(every) = config.stats_interval {
        tasks.push(tokio::spawn(log_stats(node.clone(), every)));
    }
    let (health, health_svc) = tonic_health::server::health_reporter();
    health.set_serving::<ObjectServiceServer<CacheObjectSvc>>().await;
    let limits = info.limits.unwrap_or_default();
    let max_msg = usize::try_from(limits.max_chunk_bytes)
        .unwrap_or(usize::MAX)
        .saturating_add(64 * 1024)
        .max(4 << 20);
    let streams = scion_obs::streams::StreamLimits::new(config.max_stream_lifetime, config.max_streams_per_tenant);
    let objects =
        ObjectServiceServer::new(CacheObjectSvc::new(node.clone(), limits, channels).with_stream_limits(streams))
            .max_encoding_message_size(max_msg);
    let infos = ServerInfoServiceServer::new(CacheInfoSvc::from_upstream(&info));
    // 抓取 /metrics 时读取本节点的统计；serve_node 返回后不再输出
    let _stats = register_stats(&node);
    // A client that stops reading outlives the timer in `StreamLimits::bound`, because hyper does
    // not poll a body that waits for HTTP/2 capacity. `Aged` closes the connection at the lifetime
    // plus a grace. See `scion_obs::age`.
    let hard_age = config
        .max_stream_lifetime
        .filter(|l| !l.is_zero())
        .map(|l| scion_obs::age::hard_age(l, None));
    let incoming = tokio_stream::StreamExt::map(TcpListenerStream::new(listener), move |io| {
        io.map(|io| scion_obs::age::Aged::new(io, hard_age))
    });
    let result = tonic::transport::Server::builder()
        .layer(scion_obs::trace::TraceLayer)
        .layer(scion_obs::grpc::MetricsLayer::new(
            scion_proto::METHODS
                .iter()
                .chain(scion_obs::grpc::HEALTH_METHODS)
                .copied(),
        ))
        // 不设上限，只把客户端的 deadline 交给回源请求
        .layer(scion_obs::deadline::DeadlineLayer::new(None))
        .add_service(health_svc)
        .add_service(infos)
        .add_service(objects)
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await;
    let _ = stop_tx.send(true);
    for t in tasks {
        t.abort();
    }
    node.close().await;
    result.map_err(CacheError::from)
}
