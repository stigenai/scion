//! Scion 服务端的 gRPC 服务实现与启动入口。

pub mod auth;
pub mod config;
pub mod events;
pub mod git;
pub mod limit;
pub mod prehook;
pub mod redaction;
mod service;
pub mod status;
pub mod watch;

use std::future::Future;
use std::sync::Arc;

use scion_proto::v1::admin_service_server::AdminServiceServer;
use scion_proto::v1::auth_service_server::AuthServiceServer;
use scion_proto::v1::event_service_server::EventServiceServer;
use scion_proto::v1::land_service_server::LandServiceServer;
use scion_proto::v1::object_service_server::ObjectServiceServer;
use scion_proto::v1::org_service_server::OrgServiceServer;
use scion_proto::v1::query_service_server::QueryServiceServer;
use scion_proto::v1::ref_service_server::RefServiceServer;
use scion_proto::v1::repo_service_server::RepoServiceServer;
use scion_proto::v1::server_info_service_server::ServerInfoServiceServer;
use scion_proto::v1::write_service_server::WriteServiceServer;
use scion_store::{BlobStore, MetaStore};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

pub use config::{Limits, ReadEndpoint, ServerConfig, WriteRate};
pub use scion_auth;
pub use scion_proto::PROTOCOL_VERSION;
pub use service::{
    AdminSvc, AuthSvc, EventSvc, FORWARDED_HEADER, LandSvc, ObjectSvc, OrgSvc, QuerySvc, RefSvc, RepoSvc,
    ServerInfoSvc, WriteSvc, purge_deleted_repos, trusted_forward,
};

/// 服务端依赖的全部状态。
#[derive(Clone)]
pub struct App {
    pub config: Arc<ServerConfig>,
    pub meta: Arc<dyn MetaStore>,
    pub blobs: Arc<dyn BlobStore>,
}

/// 预热全部仓库的提交记录缓存。失败只记录日志：缓存未命中时照常按需读取。
/// 定期删除过期的幂等记录。多个节点同时执行是安全的（删除是幂等的）。
async fn janitor(
    meta: Arc<dyn scion_store::MetaStore>,
    redactor: Arc<redaction::Redactor>,
    ttl: std::time::Duration,
    interval: std::time::Duration,
    repo_grace: std::time::Duration,
    event_retention: std::time::Duration,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        if let Err(e) = expire_once(meta.as_ref(), ttl).await {
            tracing::warn!(error = %e, "expiring request results failed");
        }
        // 到期的 redaction 清除：删除幂等，集群中多个节点同时执行也没有问题
        let now = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX);
        if let Err(e) = redactor.run_purges(now).await {
            tracing::warn!(error = %e.message(), "purging redacted objects failed");
        }
        // 宽限期已过的已删除仓库
        if let Err(e) = purge_deleted_repos(meta.as_ref(), repo_grace, now).await {
            tracing::warn!(error = %e, "purging deleted repositories failed");
        }
        let keep_ms = i64::try_from(event_retention.as_millis()).unwrap_or(i64::MAX);
        if let Err(e) = meta.prune_ref_events(now.saturating_sub(keep_ms)).await {
            tracing::warn!(error = %e, "pruning ref events failed");
        }
    }
}

async fn expire_once(meta: &dyn scion_store::MetaStore, ttl: std::time::Duration) -> scion_store::Result<u64> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let before = now.saturating_sub(ttl);
    let n = meta
        .expire_idempotency(i64::try_from(before.as_millis()).unwrap_or(i64::MAX))
        .await?;
    if n > 0 {
        tracing::info!(expired = n, "expired request results");
    }
    Ok(n)
}

/// 按配置构建认证与鉴权。配置无效时（启动入口应已提前校验）拒绝一切，而不是放行。
fn build_guard(config: &scion_auth::GuardConfig, meta: Arc<dyn MetaStore>) -> Arc<scion_auth::Guard> {
    if !config.required && *config == scion_auth::GuardConfig::default() {
        return scion_auth::Guard::disabled();
    }
    match scion_auth::Guard::build(config.clone(), meta, scion_auth::remote::env_lookup()) {
        Ok(g) => g,
        Err(e) => {
            tracing::error!(error = %e, "invalid auth configuration; denying all requests");
            scion_auth::Guard::deny_all()
        }
    }
}

/// 策略还没加载成功时的重试间隔。
const POLICY_RETRY: std::time::Duration = std::time::Duration::from_secs(1);

/// 定期重新加载访问控制策略（其它节点的修改）。还没加载成功时（此期间拒绝一切请求）每秒重试。
async fn reload_policy(guard: Arc<scion_auth::Guard>, every: std::time::Duration) {
    let every = every.max(std::time::Duration::from_millis(10));
    loop {
        let wait = if guard.policy_loaded() {
            every
        } else {
            every.min(POLICY_RETRY)
        };
        tokio::time::sleep(wait).await;
        match guard.reload_policy().await {
            Ok(_) => {}
            Err(e) if guard.policy_loaded() => {
                tracing::warn!(error = %e, "reloading the access policy failed; keeping the previous one");
            }
            Err(e) => tracing::error!(error = %e, "loading the access policy failed; denying all requests"),
        }
    }
}

async fn warm(cache: Arc<scion_store::CachedMeta>) {
    let repos = match scion_store::MetaStore::list_repos(cache.as_ref()).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "record cache warm-up skipped");
            return;
        }
    };
    let started = std::time::Instant::now();
    let mut total = 0;
    for repo in repos {
        match cache.warm(&repo, 4096).await {
            Ok(n) => total += n,
            Err(e) => tracing::warn!(%repo, error = %e, "record cache warm-up failed"),
        }
    }
    tracing::info!(
        records = total,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "record cache warmed"
    );
}

/// WriteService 请求的大小上限：不小于上传分块，且能容纳 `max_changes_per_changeset` 个平均 256 字节的
/// 改动条目，最多 256 MiB。
fn write_message_limit(limits: &Limits, chunk_msg: usize) -> usize {
    let changes = usize::try_from(limits.max_changes_per_changeset).unwrap_or(usize::MAX);
    changes.saturating_mul(256).clamp(chunk_msg, 256 << 20)
}

/// 在给定监听器上提供全部 gRPC 服务（含标准健康检查），直到 `shutdown` 完成。
pub async fn serve(
    listener: TcpListener,
    app: App,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error> {
    let (health, health_svc) = tonic_health::server::health_reporter();
    health.set_serving::<ServerInfoServiceServer<ServerInfoSvc>>().await;
    health.set_serving::<ObjectServiceServer<ObjectSvc>>().await;
    health.set_serving::<WriteServiceServer<WriteSvc>>().await;
    health.set_serving::<LandServiceServer<LandSvc>>().await;
    health.set_serving::<RefServiceServer<RefSvc>>().await;
    health.set_serving::<QueryServiceServer<QuerySvc>>().await;
    health.set_serving::<AuthServiceServer<AuthSvc>>().await;
    health.set_serving::<AdminServiceServer<AdminSvc>>().await;
    health.set_serving::<RepoServiceServer<RepoSvc>>().await;
    health.set_serving::<OrgServiceServer<OrgSvc>>().await;
    health.set_serving::<EventServiceServer<EventSvc>>().await;

    // 所有服务共享一份提交记录缓存；启动后在后台预热，不阻塞监听
    let cached = Arc::new(scion_store::CachedMeta::new(app.meta, app.config.record_cache_capacity));
    if app.config.warm_record_cache {
        tokio::spawn(warm(cached.clone()));
    }
    // 对象读取：小对象缓存、合并并发读、限制后端并发（§7.3）
    let blobs: Arc<dyn BlobStore> = Arc::new(scion_store::CachedBlobs::new(app.blobs, app.config.blob_cache));
    let app = App {
        meta: cached,
        blobs,
        ..app
    };
    let redactor = Arc::new(redaction::Redactor::new(app.meta.clone(), app.blobs.clone()));
    tokio::spawn(janitor(
        app.meta.clone(),
        redactor.clone(),
        app.config.idempotency_ttl,
        app.config.janitor_interval,
        app.config.repo_delete_grace,
        app.config.events.retention,
    ));
    // 事件推送：每个目标在集群中由持有租约的一个节点负责
    let holder = app
        .config
        .cluster
        .as_ref()
        .map_or_else(|| format!("local-{}", std::process::id()), |c| c.node.clone());
    match events::sinks(&app.config.events) {
        Ok(sinks) => {
            for sink in sinks {
                tracing::info!(sink = sink.name(), "delivering ref events");
                let d = events::Dispatcher {
                    meta: app.meta.clone(),
                    blobs: app.blobs.clone(),
                    sink,
                    holder: holder.clone(),
                    poll: std::time::Duration::from_millis(500),
                };
                // 开始监听之前确定起点：之后的变化一定会推送
                if let Err(e) = d.init_cursor().await {
                    tracing::warn!(error = %e, "initializing the event cursor failed; retrying in the background");
                }
                tokio::spawn(d.run());
            }
        }
        Err(e) => tracing::error!(error = %e, "event delivery disabled"),
    }
    let guard = build_guard(&app.config.auth, app.meta.clone());
    if guard.required() {
        if let Err(e) = guard.reload_policy().await {
            tracing::error!(error = %e, "loading the access policy failed; denying all requests until it loads");
        }
        tokio::spawn(reload_policy(guard.clone(), app.config.policy_poll_interval));
    }
    let max_msg = usize::try_from(app.config.limits.max_chunk_bytes)
        .unwrap_or(usize::MAX)
        .saturating_add(64 * 1024);
    let info = ServerInfoServiceServer::new(ServerInfoSvc::new(app.config.clone()).with_guard(guard.clone()));
    let objects = ObjectServiceServer::new(
        ObjectSvc::new(app.config.clone(), app.meta.clone(), app.blobs.clone()).with_guard(guard.clone()),
    )
    .max_encoding_message_size(max_msg);
    // 上传的分块可能和读取分块一样大
    let limiter = app
        .config
        .repo_write_rate
        .map(|r| Arc::new(limit::RateLimiter::new(r.per_second, r.burst)));
    let mut write_svc =
        WriteSvc::new(app.config.clone(), app.meta.clone(), app.blobs.clone()).with_guard(guard.clone());
    if let Some(l) = &limiter {
        write_svc = write_svc.with_rate_limit(l.clone());
    }
    // 合入与创建提交共用同一个写入器
    let coordinator = match &app.config.cluster {
        Some(c) => scion_land::Coordinator::clustered(write_svc.writer().clone(), app.config.land, c.clone()),
        None => scion_land::Coordinator::new(write_svc.writer().clone(), app.config.land),
    };
    let hub = watch::RefHub::new(app.meta.clone(), app.config.ref_poll_interval);
    let cluster_secret: Option<Arc<str>> = app.config.cluster_secret.as_deref().map(Arc::from);
    let pre_land = match &app.config.pre_land {
        Some(cfg) => match prehook::PreLand::new(app.meta.clone(), app.blobs.clone(), cfg.clone()) {
            Ok(h) => Some(Arc::new(h)),
            // 配置了检查却无法创建：拒绝合入，而不是悄悄跳过检查
            Err(e) => {
                tracing::error!(error = %e, "pre-land check misconfigured; lands will be refused");
                Some(Arc::new(prehook::PreLand::refuse_all(
                    app.meta.clone(),
                    app.blobs.clone(),
                    cfg.clone(),
                )))
            }
        },
        None => None,
    };
    let mut land_svc = LandSvc::new(coordinator.clone())
        .with_ref_hub(hub.clone())
        .with_guard(guard.clone())
        .with_cluster_secret(cluster_secret.clone())
        .with_pre_land(pre_land.clone());
    if let Some(l) = limiter {
        land_svc = land_svc.with_rate_limit(l);
    }
    let land_svc = Arc::new(land_svc);
    let land = LandServiceServer::from_arc(land_svc.clone());
    // CreateChangeset 一次带全部改动条目：按条目数上限留足空间（每条路径最长 4 KiB，平均远小于此）
    let max_write = write_message_limit(&app.config.limits, max_msg);
    let write_svc = Arc::new(write_svc);
    let write = WriteServiceServer::from_arc(write_svc.clone()).max_decoding_message_size(max_write);
    let hub_for_shutdown = hub.clone();
    let ref_svc = Arc::new(
        RefSvc::new(app.meta.clone(), hub)
            .with_blobs(app.blobs.clone())
            .with_guard(guard.clone())
            .with_pre_land(pre_land),
    );
    let refs = RefServiceServer::from_arc(ref_svc.clone());
    let query = QueryServiceServer::new(
        QuerySvc::new(app.config.clone(), app.meta.clone(), app.blobs.clone()).with_guard(guard.clone()),
    )
    .max_encoding_message_size(max_msg);
    let auth_svc = AuthServiceServer::new(AuthSvc::new(guard.clone()));
    let event_svc = EventServiceServer::new(EventSvc::new(app.meta.clone()).with_guard(guard.clone()));
    let repo_svc = RepoServiceServer::new(
        RepoSvc::new(app.meta.clone())
            .with_guard(guard.clone())
            .with_delete_grace(app.config.repo_delete_grace),
    );
    let admin_svc =
        AdminServiceServer::new(AdminSvc::new(guard.clone()).with_redactor(redactor, app.config.purge_grace));
    let org_svc = OrgServiceServer::new(OrgSvc::new(app.meta.clone(), guard.clone()));

    let tenant = limit::TenantLimitLayer::new(
        app.config.tenant_rate.map(|r| (r.per_second, r.burst)),
        app.config.max_concurrent_per_tenant,
    )
    .with_cluster_secret(cluster_secret);
    // git's smart HTTP requests go to the git gateway before any gRPC layer
    let gateway = app.config.git.then(|| {
        let push = git::PushBackend {
            write: write_svc,
            land: land_svc,
            refs: ref_svc,
        };
        Arc::new(
            scion_git::Gateway::new(
                app.meta.clone(),
                app.blobs.clone(),
                guard.clone(),
                &app.config.server_version,
            )
            .with_push(Arc::new(push), app.config.limits.max_upload_object_bytes),
        )
    });
    // 从外到内：git → 追踪 → 指标 → 全局过载丢弃 → 认证 → 按租户限流 → deadline → 各服务（§5.3）
    let result = tonic::transport::Server::builder()
        .accept_http1(app.config.git)
        .layer(git::GitLayer::new(gateway))
        .layer(scion_obs::trace::TraceLayer)
        .layer(scion_obs::grpc::MetricsLayer::new(
            scion_proto::METHODS
                .iter()
                .chain(scion_obs::grpc::HEALTH_METHODS)
                .copied(),
        ))
        .layer(limit::ConcurrencyLimitLayer::new(app.config.max_concurrent_requests))
        .layer(auth::AuthLayer::new(guard))
        .layer(tenant)
        .layer(scion_obs::deadline::DeadlineLayer::new(app.config.max_request_timeout))
        .add_service(health_svc)
        .add_service(info)
        .add_service(objects)
        .add_service(write)
        .add_service(land)
        .add_service(refs)
        .add_service(query)
        .add_service(auth_svc)
        .add_service(admin_svc)
        .add_service(repo_svc)
        .add_service(org_svc)
        .add_service(event_svc)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.await;
            // 结束 WatchRefs 的长连接，否则优雅停机会一直等它们
            hub_for_shutdown.close();
        })
        .await;
    // 不再有请求进来：让合入任务处理完手上的批并释放租约
    coordinator.shutdown();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_store::{IdempotencyRecord, MemStore, MetaStore, RefExpect};

    fn record(id: &str, created_at_ms: i64) -> IdempotencyRecord {
        IdempotencyRecord {
            request_id: id.into(),
            fingerprint: vec![],
            response: vec![],
            created_at_ms,
        }
    }

    async fn seeded() -> Arc<MemStore> {
        let s = Arc::new(MemStore::new());
        s.create_repo("r").await.expect("repo");
        let now = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_millis(),
        )
        .expect("ms");
        let target = scion_object::ObjectId::of_blob(b"t");
        s.land_commit(
            "r",
            "main",
            RefExpect::Absent,
            target,
            &[],
            &[record("old", 0), record("fresh", now)],
        )
        .await
        .expect("seed");
        s
    }

    #[test]
    fn write_limit_fits_max_changes() {
        let l = Limits::default();
        assert_eq!(write_message_limit(&l, 1 << 20), 100_000 * 256);
        let few = Limits {
            max_changes_per_changeset: 1,
            ..Limits::default()
        };
        assert_eq!(
            write_message_limit(&few, 1 << 20),
            1 << 20,
            "never below the chunk size"
        );
        let many = Limits {
            max_changes_per_changeset: u32::MAX,
            ..Limits::default()
        };
        assert_eq!(write_message_limit(&many, 1 << 20), 256 << 20, "capped");
    }

    #[tokio::test]
    async fn expire_once_removes_only_old_results() {
        let s = seeded().await;
        let ttl = std::time::Duration::from_secs(3600);
        assert_eq!(expire_once(s.as_ref(), ttl).await, Ok(1));
        assert_eq!(s.get_idempotency("r", "old").await, Ok(None));
        assert!(s.get_idempotency("r", "fresh").await.expect("ok").is_some());
        assert_eq!(expire_once(s.as_ref(), ttl).await, Ok(0));
    }

    #[tokio::test(start_paused = true)]
    async fn janitor_expires_on_each_tick() {
        let s = seeded().await;
        s.create_repo("gone").await.expect("create");
        s.delete_repo("gone", 0).await.expect("delete long ago");
        let task = tokio::spawn(janitor(
            s.clone(),
            Arc::new(redaction::Redactor::new(
                s.clone(),
                Arc::new(scion_store::MemStore::new()),
            )),
            std::time::Duration::from_secs(3600),
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(24 * 3600),
            std::time::Duration::from_secs(3600),
        ));
        // 第一次 tick 立即触发
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(s.get_idempotency("r", "old").await, Ok(None));
        assert_eq!(
            s.get_repo("gone").await,
            Ok(None),
            "deleted repository purged after the grace period"
        );
        task.abort();
    }
}
